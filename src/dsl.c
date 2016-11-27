#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client_context.h"
#include "db_manager.h"
#include "dsl.h"
#include "scheduler.h"
#include "utils.h"

bool shutdown_initiated = false;

void dsl_create_db(char *name, Message *send_message) {
    db_create(name, send_message);
}

void dsl_create_table(char *name, char *db_name, unsigned int num_columns, Message *send_message) {
    table_create(name, db_name, num_columns, send_message);
}

void dsl_create_column(char *name, char *table_fqn, Message *send_message) {
    column_create(name, table_fqn, send_message);
}

void dsl_create_index(char *column_fqn, ColumnIndexType type, bool clustered, Message *send_message) {
    index_create(column_fqn, type, clustered, send_message);
}

void dsl_load(Vector *col_fqns, IntVector *col_vals, Message *send_message) {
    unsigned int num_columns = col_fqns->size;

    Column *columns[num_columns];
    Table *table = NULL;

    for (unsigned int i = 0; i < num_columns; i++) {
        Column *column = column_lookup(col_fqns->data[i]);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            if (table != NULL) {
                pthread_rwlock_unlock(&table->rwlock);
            }
            return;
        } else {
            columns[i] = column;

            if (i == 0) {
                table = column->table;

                pthread_rwlock_wrlock(&table->rwlock);

                if (num_columns != table->columns_capacity) {
                    send_message->status = INSERT_COLUMNS_MISMATCH;
                    pthread_rwlock_unlock(&table->rwlock);
                    return;
                } else if (table->columns_count != table->columns_capacity) {
                    send_message->status = TABLE_NOT_FULLY_INITIALIZED;
                    pthread_rwlock_unlock(&table->rwlock);
                    return;
                }
            }
        }
    }

    for (unsigned int i = 0; i < num_columns; i++) {
        IntVector *dst = &columns[i]->values;
        IntVector *src = &col_vals[i];

        if (dst->size == 0) {
            int_vector_destroy(dst);
            int_vector_shallow_copy(dst, src);
            int_vector_init(src, 0);
        } else {
            int_vector_concat(&columns[i]->values, &col_vals[i]);
        }
    }

    index_rebuild_all(table);

    pthread_rwlock_unlock(&table->rwlock);
}

static inline unsigned int dsl_select_lower(int *values, unsigned int values_count, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] < high;
    }
    return result_count;
}

static inline unsigned int dsl_select_higher(int *values, unsigned int values_count, int low,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] >= low;
    }
    return result_count;
}

static inline unsigned int dsl_select_equal(int *values, unsigned int values_count, int value,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        result_count += values[i] == value;
    }
    return result_count;
}

static inline unsigned int dsl_select_range(int *values, unsigned int values_count, int low, int high,
        unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = i;
        int value = values[i];
        result_count += (value >= low) & (value < high);
    }
    return result_count;
}

void dsl_select(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, int low,
        bool has_low, int high, bool has_high, char *pos_out_var, Message *send_message) {
    Column *source;
    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        source = column;
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        source = NULL;
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    unsigned int *result = NULL;
    unsigned int result_count = 0;
    if (values_count > 0 && (!has_low || !has_high || low < high)) {
        result = malloc(values_count * sizeof(unsigned int));

        if (index == NULL) {
            if (!has_low) {
                result_count = dsl_select_lower(values, values_count, high, result);
            } else if (!has_high) {
                result_count = dsl_select_higher(values, values_count, low, result);
            } else if (low == high - 1) {
                result_count = dsl_select_equal(values, values_count, low, result);
            } else {
                result_count = dsl_select_range(values, values_count, low, high, result);
            }
        } else {
            switch (index->type) {
            case BTREE:
                if (!has_low) {
                    result_count = btree_select_lower(&index->fields.btree, high, result);
                } else if (!has_high) {
                    result_count = btree_select_higher(&index->fields.btree, low, result);
                } else {
                    result_count = btree_select_range(&index->fields.btree, low, high, result);
                }
                break;
            case SORTED:
                if (!has_low) {
                    result_count = sorted_select_lower(&index->fields.sorted, high, result);
                } else if (!has_high) {
                    result_count = sorted_select_higher(&index->fields.sorted, low, result);
                } else {
                    result_count = sorted_select_range(&index->fields.sorted, low, high, result);
                }
                break;
            }
        }

        if (result_count == 0) {
            free(result);
            result = NULL;
        } else if (result_count < values_count) {
            result = realloc(result, result_count * sizeof(unsigned int));
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    pos_result_put(client_context, pos_out_var, source, result, result_count);
}

static inline unsigned int dsl_select_pos_lower(unsigned int *positions, int *values,
        unsigned int values_count, int high, unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] < high;
    }
    return result_count;
}

static inline unsigned int dsl_select_pos_higher(unsigned int *positions, int *values,
        unsigned int values_count, int low, unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] >= low;
    }
    return result_count;
}

static inline unsigned int dsl_select_pos_equal(unsigned int *positions, int *values,
        unsigned int values_count, int value, unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        result_count += values[i] == value;
    }
    return result_count;
}

static inline unsigned int dsl_select_pos_range(unsigned int *positions, int *values,
        unsigned int values_count, int low, int high, unsigned int *result) {
    unsigned int result_count = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result[result_count] = positions[i];
        int value = values[i];
        result_count += (value >= low) & (value < high);
    }
    return result_count;
}

void dsl_select_pos(ClientContext *client_context, char *pos_var, char *val_var, int low,
        bool has_low, int high, bool has_high, char *pos_out_var, Message *send_message) {
    Result *pos = result_lookup(client_context, pos_var);
    if (pos == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (pos->type != POS) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    Result *val = result_lookup(client_context, val_var);
    if (val == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (val->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    int *values = val->values.int_values;
    unsigned int values_count = val->num_tuples;

    if (positions_count != values_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    unsigned int *result = NULL;
    unsigned int result_count = 0;
    if (values_count > 0 && (!has_low || !has_high || low < high)) {
        result = malloc(values_count * sizeof(unsigned int));

        if (!has_low) {
            result_count = dsl_select_pos_lower(positions, values, values_count, high, result);
        } else if (!has_high) {
            result_count = dsl_select_pos_higher(positions, values, values_count, low, result);
        } else if (low == high - 1) {
            result_count = dsl_select_pos_equal(positions, values, values_count, low, result);
        } else {
            result_count = dsl_select_pos_range(positions, values, values_count, low, high, result);
        }

        if (result_count == 0) {
            free(result);
            result = NULL;
        } else if (result_count < values_count) {
            result = realloc(result, result_count * sizeof(unsigned int));
        }
    }

    pos_result_put(client_context, pos_out_var, pos->source, result, result_count);
}

void dsl_fetch(ClientContext *client_context, char *column_fqn, char *pos_var, char *val_out_var,
        Message *send_message) {
    Result *pos = result_lookup(client_context, pos_var);
    if (pos == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (pos->type != POS) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    Column *column = column_lookup(column_fqn);
    if (column == NULL) {
        send_message->status = COLUMN_NOT_FOUND;
        return;
    }

    pthread_rwlock_rdlock(&column->table->rwlock);

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    int *result = NULL;
    if (positions_count > 0) {
        int *values;
        Column *source = pos->source;
        if (source != NULL) {
            ColumnIndex *source_index = source->index;

            if (source_index != NULL && source_index->clustered) {
                values = source_index->clustered_columns[column->order].data;
            } else {
                values = column->values.data;
            }
        } else {
            values = column->values.data;
        }

        result = malloc(positions_count * sizeof(int));
        for (unsigned int i = 0; i < positions_count; i++) {
            result[i] = values[positions[i]];
        }
    }

    pthread_rwlock_unlock(&column->table->rwlock);

    int_result_put(client_context, val_out_var, result, positions_count);
}

static inline void dsl_insert(Column *column, IntVector *values) {
    int value = values->data[column->order];

    int_vector_append(&column->values, value);

    ColumnIndex *index = column->index;
    if (index != NULL) {
        if (index->clustered) {
            unsigned int position;

            switch (index->type) {
            case BTREE:
                btree_insert(&index->fields.btree, value, &position);
                break;
            case SORTED:
                sorted_insert(&index->fields.sorted, value, &position);
                break;
            }

            pos_vector_insert(index->clustered_positions, position, column->values.size - 1);

            for (unsigned int i = 0; i < values->size; i++) {
                int_vector_insert(index->clustered_columns + i, position, values->data[i]);
            }
        } else {
            unsigned int position = column->values.size - 1;

            switch (index->type) {
            case BTREE:
                btree_insert(&index->fields.btree, value, &position);
                break;
            case SORTED:
                sorted_insert(&index->fields.sorted, value, &position);
                break;
            }
        }
    }
}

void dsl_relational_insert(char *table_fqn, IntVector *values, Message *send_message) {
    Table *table = table_lookup(table_fqn);
    if (table == NULL) {
        send_message->status = TABLE_NOT_FOUND;
        return;
    }

    pthread_rwlock_wrlock(&table->rwlock);

    if (values->size != table->columns_capacity) {
        send_message->status = INSERT_COLUMNS_MISMATCH;
        pthread_rwlock_unlock(&table->rwlock);
        return;
    }
    if (table->columns_count != table->columns_capacity) {
        send_message->status = TABLE_NOT_FULLY_INITIALIZED;
        pthread_rwlock_unlock(&table->rwlock);
        return;
    }

    for (unsigned int i = 0; i < table->columns_capacity; i++) {
        dsl_insert(&table->columns[i], values);
    }

    pthread_rwlock_unlock(&table->rwlock);
}

void dsl_relational_delete(ClientContext *client_context, char *table_fqn, char *pos_var,
        Message *send_message);
void dsl_relational_update(ClientContext *client_context, char *column_fqn, char *pos_var,
        int value, Message *send_message);

void dsl_join(ClientContext *client_context, char *val_var1, char *pos_var1, char *val_var2,
        char *pos_var2, char *pos_out_var1, char *pos_out_var2, JoinType type,
        Message *send_message);

void dsl_min(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    int min_value = 0;
    if (index == NULL) {
        min_value = values[0];

        for (unsigned int i = 1; i < values_count; i++) {
            int value = values[i];
            min_value = value < min_value ? value : min_value;
        }
    } else {
        switch (index->type) {
        case BTREE:
            min_value = btree_min(&index->fields.btree, NULL);
            break;
        case SORTED:
            min_value = sorted_min(&index->fields.sorted, NULL);
            break;
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    int *value_out = malloc(sizeof(int));
    *value_out = min_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_min_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message) {
    Column *source = NULL;

    unsigned int *positions;
    unsigned int positions_count;
    if (pos_var != NULL) {
        Result *pos = result_lookup(client_context, pos_var);
        if (pos == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (pos->type != POS) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        source = pos->source;
        positions = pos->values.pos_values;
        positions_count = pos->num_tuples;
    } else {
        positions = NULL;
        positions_count = 0;
    }

    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        source = column;
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    unsigned int min_position = 0;
    int min_value = 0;
    if (index == NULL) {
        if (positions != NULL && positions_count != values_count) {
            send_message->status = TUPLE_COUNT_MISMATCH;
            if (table_rwlock != NULL) {
                pthread_rwlock_unlock(table_rwlock);
            }
            return;
        }

        min_position = 0;
        min_value = values[0];

        for (unsigned int i = 1; i < values_count; i++) {
            int value = values[i];

            bool smaller = value < min_value;

            min_position = smaller ? i : min_position;
            min_value = smaller ? value : min_value;
        }

        if (positions != NULL) {
            min_position = positions[min_position];
        }
    } else {
        switch (index->type) {
        case BTREE:
            min_value = btree_min(&index->fields.btree, &min_position);
            break;
        case SORTED:
            min_value = sorted_min(&index->fields.sorted, &min_position);
            break;
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    unsigned int *position_out = malloc(sizeof(unsigned int));
    *position_out = min_position;

    pos_result_put(client_context, pos_out_var, source, position_out, 1);

    int *value_out = malloc(sizeof(int));
    *value_out = min_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_max(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    int max_value = 0;
    if (index == NULL) {
        max_value = values[0];

        for (unsigned int i = 1; i < values_count; i++) {
            int value = values[i];
            max_value = value > max_value ? value : max_value;
        }
    } else {
        switch (index->type) {
        case BTREE:
            max_value = btree_max(&index->fields.btree, NULL);
            break;
        case SORTED:
            max_value = sorted_max(&index->fields.sorted, NULL);
            break;
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    int *value_out = malloc(sizeof(int));
    *value_out = max_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_max_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message) {
    Column *source = NULL;

    unsigned int *positions;
    unsigned int positions_count;
    if (pos_var != NULL) {
        Result *pos = result_lookup(client_context, pos_var);
        if (pos == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (pos->type != POS) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        source = pos->source;
        positions = pos->values.pos_values;
        positions_count = pos->num_tuples;
    } else {
        positions = NULL;
        positions_count = 0;
    }

    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        source = column;
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    unsigned int max_position = 0;
    int max_value = 0;
    if (index == NULL) {
        if (positions != NULL && positions_count != values_count) {
            send_message->status = TUPLE_COUNT_MISMATCH;
            if (table_rwlock != NULL) {
                pthread_rwlock_unlock(table_rwlock);
            }
            return;
        }

        max_position = 0;
        max_value = values[0];

        for (unsigned int i = 1; i < values_count; i++) {
            int value = values[i];

            bool larger = value > max_value;

            max_position = larger ? i : max_position;
            max_value = larger ? value : max_value;
        }

        if (positions != NULL) {
            max_position = positions[max_position];
        }
    } else {
        switch (index->type) {
        case BTREE:
            max_value = btree_max(&index->fields.btree, &max_position);
            break;
        case SORTED:
            max_value = sorted_max(&index->fields.sorted, &max_position);
            break;
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    unsigned int *position_out = malloc(sizeof(unsigned int));
    *position_out = max_position;

    pos_result_put(client_context, pos_out_var, source, position_out, 1);

    int *value_out = malloc(sizeof(int));
    *value_out = max_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_sum(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    long long int result = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result += values[i];
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    long long int *value_out = malloc(sizeof(long long int));
    *value_out = result;

    long_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_avg(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    pthread_rwlock_t *table_rwlock;
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        values = column->values.data;
        values_count = column->values.size;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            send_message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        table_rwlock = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    long long int sum = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        sum += values[i];
    }
    double result = (double) sum / (double) values_count;

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    double *value_out = malloc(sizeof(double));
    *value_out = result;

    float_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_add(ClientContext *client_context, char *val_var1, char *val_var2, char *val_out_var,
        Message *send_message) {
    Result *variable1 = result_lookup(client_context, val_var1);
    if (variable1 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (variable1->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }
    int *values1 = variable1->values.int_values;
    unsigned int values1_count = variable1->num_tuples;

    Result *variable2 = result_lookup(client_context, val_var2);
    if (variable2 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (variable2->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }
    int *values2 = variable2->values.int_values;
    unsigned int values2_count = variable2->num_tuples;

    if (values1_count != values2_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    int *result = NULL;
    if (values1_count > 0) {
        result = malloc(values1_count * sizeof(int));
        for (unsigned int i = 0; i < values1_count; i++) {
            result[i] = values1[i] + values2[i];
        }
    }

    int_result_put(client_context, val_out_var, result, values1_count);
}

void dsl_sub(ClientContext *client_context, char *val_var1, char *val_var2, char *val_out_var,
        Message *send_message) {
    Result *variable1 = result_lookup(client_context, val_var1);
    if (variable1 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (variable1->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }
    int *values1 = variable1->values.int_values;
    unsigned int values1_count = variable1->num_tuples;

    Result *variable2 = result_lookup(client_context, val_var2);
    if (variable2 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (variable2->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }
    int *values2 = variable2->values.int_values;
    unsigned int values2_count = variable2->num_tuples;

    if (values1_count != values2_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    int *result = NULL;
    if (values1_count > 0) {
        result = malloc(values1_count * sizeof(int));
        for (unsigned int i = 0; i < values1_count; i++) {
            result[i] = values1[i] - values2[i];
        }
    }

    int_result_put(client_context, val_out_var, result, values1_count);
}

void dsl_print(ClientContext *client_context, Vector *val_vars, Message *send_message) {
    void **vars = val_vars->data;
    unsigned int num_columns = val_vars->size;

    Result *variables[num_columns];

    unsigned int num_tuples = 0;

    size_t payload_length = 2 * sizeof(unsigned int) + num_columns * sizeof(DataType);

    for (unsigned int i = 0; i < num_columns; i++) {
        Result *variable = result_lookup(client_context, vars[i]);

        if (variable == NULL) {
            send_message->status = VARIABLE_NOT_FOUND;
            return;
        }

        if (i == 0) {
            num_tuples = variable->num_tuples;
        } else if (variable->num_tuples != num_tuples) {
            send_message->status = TUPLE_COUNT_MISMATCH;
            return;
        }

        variables[i] = variable;

        if (num_tuples > 0) {
            switch (variable->type) {
            case POS:
                payload_length += num_tuples * sizeof(unsigned int);
                break;
            case INT:
                payload_length += num_tuples * sizeof(int);
                break;
            case LONG:
                payload_length += num_tuples * sizeof(long long);
                break;
            case FLOAT:
                payload_length += num_tuples * sizeof(double);
                break;
            }
        }
    }

    send_message->status = OK_WAIT_FOR_RESPONSE;
    send_message->length = payload_length;
    send_message->payload = malloc(payload_length);

    char *payload = send_message->payload;

    memcpy(payload, &num_columns, sizeof(unsigned int));
    payload += sizeof(unsigned int);

    memcpy(payload, &num_tuples, sizeof(unsigned int));
    payload += sizeof(unsigned int);

    for (unsigned int i = 0; i < num_columns; i++) {
        Result *variable = variables[i];
        DataType type = variable->type;

        memcpy(payload, &type, sizeof(DataType));
        payload += sizeof(DataType);

        if (num_tuples > 0) {
            void *values = NULL;
            size_t values_length = 0;

            switch (variable->type) {
            case POS:
                values = variable->values.pos_values;
                values_length = num_tuples * sizeof(unsigned int);
                break;
            case INT:
                values = variable->values.int_values;
                values_length = num_tuples * sizeof(int);
                break;
            case LONG:
                values = variable->values.long_values;
                values_length = num_tuples * sizeof(long long);
                break;
            case FLOAT:
                values = variable->values.float_values;
                values_length = num_tuples * sizeof(double);
                break;
            }

            memcpy(payload, values, values_length);
            payload += values_length;
        }
    }
}

void dsl_batch_queries(ClientContext *client_context, Message *send_message) {
    if (client_context->is_batching) {
        send_message->status = ALREADY_BATCHING;
        return;
    }

    client_context->is_batching = true;
}

void dsl_batch_execute(ClientContext *client_context, Message *send_message) {
    if (!client_context->is_batching) {
        send_message->status = NOT_BATCHING;
        return;
    }

    client_context->is_batching = false;

    scheduler_execute_concurrently(client_context, send_message);
}

void dsl_shutdown() {
    shutdown_initiated = true;
}

bool is_shutdown_initiated() {
    return shutdown_initiated;
}

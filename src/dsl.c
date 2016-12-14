#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "batch.h"
#include "client_context.h"
#include "db_manager.h"
#include "dsl.h"
#include "join.h"
#include "queue.h"
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

    unsigned int rows_count = col_vals[0].size;

    for (unsigned int i = 0; i < num_columns; i++) {
        IntVector *dst = &columns[i]->values;
        IntVector *src = col_vals + i;

        if (dst->size == 0) {
            int_vector_destroy(dst);
            int_vector_shallow_copy(dst, src);
            int_vector_init(src, 0);
        } else {
            int_vector_concat(dst, src);
        }
    }

    table->rows_count += rows_count;

    BoolVector *deleted_rows = table->deleted_rows;
    if (deleted_rows != NULL) {
        unsigned int new_size = deleted_rows->size + rows_count;

        bool_vector_ensure_capacity(deleted_rows, new_size);
        memset(deleted_rows->data + deleted_rows->size, 0, rows_count * sizeof(bool));
        deleted_rows->size = new_size;
    }

    index_rebuild_all(table);

    pthread_rwlock_unlock(&table->rwlock);
}

static inline unsigned int dsl_select_lower(int *values, unsigned int values_count,
        bool *deleted_rows, int high, unsigned int *result) {
    unsigned int result_count = 0;
    if (deleted_rows == NULL) {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += values[i] < high;
        }
    } else {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += !deleted_rows[i] & (values[i] < high);
        }
    }
    return result_count;
}

static inline unsigned int dsl_select_higher(int *values, unsigned int values_count,
        bool *deleted_rows, int low, unsigned int *result) {
    unsigned int result_count = 0;
    if (deleted_rows == NULL) {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += values[i] >= low;
        }
    } else {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += !deleted_rows[i] & (values[i] >= low);
        }
    }
    return result_count;
}

static inline unsigned int dsl_select_equal(int *values, unsigned int values_count,
        bool *deleted_rows, int value, unsigned int *result) {
    unsigned int result_count = 0;
    if (deleted_rows == NULL) {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += values[i] == value;
        }
    } else {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            result_count += !deleted_rows[i] & (values[i] == value);
        }
    }
    return result_count;
}

static inline unsigned int dsl_select_range(int *values, unsigned int values_count,
        bool *deleted_rows, int low, int high, unsigned int *result) {
    unsigned int result_count = 0;
    if (deleted_rows == NULL) {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            int value = values[i];
            result_count += (value >= low) & (value < high);
        }
    } else {
        for (unsigned int i = 0; i < values_count; i++) {
            result[result_count] = i;
            int value = values[i];
            result_count += !deleted_rows[i] & (value >= low) & (value < high);
        }
    }
    return result_count;
}

void dsl_select(ClientContext *client_context, GeneralizedColumnHandle *col_hdl,
        Comparator *comparator, char *pos_out_var, Message *send_message) {
    Column *source;
    pthread_rwlock_t *table_rwlock;
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        source = column;
        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    unsigned int *result = NULL;
    unsigned int result_count = 0;
    if (rows_count > 0
            && (!comparator->has_low || !comparator->has_high || comparator->low < comparator->high)) {
        result = malloc(rows_count * sizeof(unsigned int));

        if (index == NULL) {
            if (!comparator->has_low) {
                result_count = dsl_select_lower(values, values_count, deleted_rows,
                        comparator->high, result);
            } else if (!comparator->has_high) {
                result_count = dsl_select_higher(values, values_count, deleted_rows,
                        comparator->low, result);
            } else if (comparator->low == comparator->high - 1) {
                result_count = dsl_select_equal(values, values_count, deleted_rows,
                        comparator->low, result);
            } else {
                result_count = dsl_select_range(values, values_count, deleted_rows,
                        comparator->low, comparator->high, result);
            }
        } else {
            switch (index->type) {
            case BTREE:
                if (!comparator->has_low) {
                    result_count = btree_select_lower(&index->fields.btree, comparator->high,
                            result);
                } else if (!comparator->has_high) {
                    result_count = btree_select_higher(&index->fields.btree, comparator->low,
                            result);
                } else {
                    result_count = btree_select_range(&index->fields.btree, comparator->low,
                            comparator->high, result);
                }
                break;
            case SORTED:
                if (!comparator->has_low) {
                    result_count = sorted_select_lower(&index->fields.sorted, comparator->high,
                            result);
                } else if (!comparator->has_high) {
                    result_count = sorted_select_higher(&index->fields.sorted, comparator->low,
                            result);
                } else {
                    result_count = sorted_select_range(&index->fields.sorted, comparator->low,
                            comparator->high, result);
                }
                break;
            }
        }

        if (result_count == 0) {
            free(result);
            result = NULL;
        } else if (result_count < rows_count) {
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

void dsl_select_pos(ClientContext *client_context, char *pos_var, char *val_var,
        Comparator *comparator, char *pos_out_var, Message *send_message) {
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
    if (values_count > 0
            && (!comparator->has_low || !comparator->has_high || comparator->low < comparator->high)) {
        result = malloc(values_count * sizeof(unsigned int));

        if (!comparator->has_low) {
            result_count = dsl_select_pos_lower(positions, values, values_count, comparator->high,
                    result);
        } else if (!comparator->has_high) {
            result_count = dsl_select_pos_higher(positions, values, values_count, comparator->low,
                    result);
        } else if (comparator->low == comparator->high - 1) {
            result_count = dsl_select_pos_equal(positions, values, values_count, comparator->low,
                    result);
        } else {
            result_count = dsl_select_pos_range(positions, values, values_count, comparator->low,
                    comparator->high, result);
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

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    int *result = NULL;
    if (positions_count > 0) {
        int *values;
        Column *source = pos->source;

        pthread_rwlock_rdlock(&column->table->rwlock);

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

        pthread_rwlock_unlock(&column->table->rwlock);
    }

    int_result_put(client_context, val_out_var, result, positions_count);
}

static inline void index_insert(ColumnIndex *index, int value, unsigned int position, int *values) {
    if (index->clustered) {
        pos_vector_append(index->clustered_positions, position);
        position = index->clustered_positions->size - 1;

        for (unsigned int j = 0; j < index->num_columns; j++) {
            int_vector_append(index->clustered_columns + j, values[j]);
        }

    }

    switch (index->type) {
    case BTREE:
        btree_insert(&index->fields.btree, value, position);
        break;
    case SORTED:
        sorted_insert(&index->fields.sorted, value, position);
        break;
    }
}

static void dsl_insert(Table *table, int *values) {
    bool replace;
    unsigned int insert_position = 0;

    if (table->delete_queue.size > 0) {
        replace = true;
        insert_position = queue_pop(&table->delete_queue);
    } else {
        replace = false;
    }

    for (unsigned int i = 0; i < table->columns_capacity; i++) {
        Column *column = table->columns + i;

        int value = values[i];

        if (replace) {
            column->values.data[insert_position] = value;
        } else {
            int_vector_append(&column->values, value);
            insert_position = column->values.size - 1;
        }

        ColumnIndex *index = column->index;
        if (index != NULL) {
            index_insert(index, value, insert_position, values);
        }
    }

    table->rows_count++;

    BoolVector *deleted_rows = table->deleted_rows;
    if (replace) {
        deleted_rows->data[insert_position] = false;
    } else if (deleted_rows != NULL) {
        bool_vector_append(deleted_rows, false);
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

    dsl_insert(table, values->data);

    pthread_rwlock_unlock(&table->rwlock);
}

static inline void index_remove(ColumnIndex *index, int value, unsigned int position) {
    unsigned int *positions_map = index->clustered ? index->clustered_positions->data : NULL;

    switch (index->type) {
    case BTREE:
        btree_remove(&index->fields.btree, value, position, positions_map, NULL);
        break;
    case SORTED:
        sorted_remove(&index->fields.sorted, value, position, positions_map, NULL);
        break;
    }
}

static bool dsl_delete(Table *table, unsigned int position) {
    BoolVector *deleted_rows = table->deleted_rows;

    if (deleted_rows != NULL && deleted_rows->data[position]) {
        return false;
    }

    for (unsigned int i = 0; i < table->columns_capacity; i++) {
        Column *column = table->columns + i;

        ColumnIndex *index = column->index;
        if (index != NULL) {
            int value = column->values.data[position];

            index_remove(index, value, position);
        }
    }

    table->rows_count--;

    queue_push(&table->delete_queue, position);

    if (deleted_rows == NULL) {
        deleted_rows = table->deleted_rows = malloc(sizeof(BoolVector));

        IntVector *first_column = &table->columns[0].values;

        deleted_rows->data = calloc(first_column->size, sizeof(bool));
        deleted_rows->size = first_column->size;
        deleted_rows->capacity = first_column->size;
    }

    deleted_rows->data[position] = true;

    return true;
}

void dsl_relational_delete(ClientContext *client_context, char *table_fqn, char *pos_var,
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

    Table *table = table_lookup(table_fqn);
    if (table == NULL) {
        send_message->status = TABLE_NOT_FOUND;
        return;
    }

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    if (positions_count == 0) {
        return;
    }

    pthread_rwlock_wrlock(&table->rwlock);

    if (table->columns_count != table->columns_capacity) {
        send_message->status = TABLE_NOT_FULLY_INITIALIZED;
        pthread_rwlock_unlock(&table->rwlock);
        return;
    }

    Column *source = pos->source;

    if (source != NULL) {
        ColumnIndex *source_index = source->index;

        if (source_index != NULL && source_index->clustered) {
            unsigned int *mapped_positions = malloc(positions_count * sizeof(unsigned int));

            unsigned int *clustered_positions = source_index->clustered_positions->data;
            for (unsigned int i = 0; i < positions_count; i++) {
                mapped_positions[i] = clustered_positions[positions[i]];
            }

            for (unsigned int i = 0; i < positions_count; i++) {
                dsl_delete(table, mapped_positions[i]);
            }

            free(mapped_positions);
        } else {
            for (unsigned int i = 0; i < positions_count; i++) {
                dsl_delete(table, positions[i]);
            }
        }
    } else {
        for (unsigned int i = 0; i < positions_count; i++) {
            dsl_delete(table, positions[i]);
        }
    }

    pthread_rwlock_unlock(&table->rwlock);
}

static inline void dsl_update(Table *table, Column *column, unsigned int position, int value) {
    int old_value = column->values.data[position];

    if (old_value == value) {
        return;
    }

    column->values.data[position] = value;

    // Update ColumnIndex (if any) for updated Column.
    ColumnIndex *index = column->index;
    if (index != NULL) {
        index_remove(index, old_value, position);

        if (index->clustered) {
            int row_values[table->columns_capacity];
            for (unsigned int i = 0; i < table->columns_capacity; i++) {
                row_values[i] = table->columns[i].values.data[position];
            }

            index_insert(index, value, position, row_values);
        } else {
            index_insert(index, value, position, NULL);
        }
    }

    // Update other clustered indices to reflect the updated value.
    for (unsigned int i = 0; i < table->columns_capacity; i++) {
        if (i == column->order) {
            continue;
        }

        Column *column = table->columns + i;

        ColumnIndex *index = column->index;
        if (index != NULL && index->clustered) {
            int column_value = column->values.data[position];

            bool found = false;
            unsigned int clustered_position;

            switch (index->type) {
            case BTREE:
                found = btree_search(&index->fields.btree, column_value, position,
                        index->clustered_positions->data, &clustered_position);
                break;
            case SORTED:
                found = sorted_search(&index->fields.sorted, column_value, position,
                        index->clustered_positions->data, &clustered_position);
                break;
            }

            if (found) {
                index->clustered_columns[column->order].data[clustered_position] = value;
            }
        }
    }
}

void dsl_relational_update(ClientContext *client_context, char *column_fqn, char *pos_var,
        int value, Message *send_message) {
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

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    if (positions_count == 0) {
        return;
    }

    Table *table = column->table;

    pthread_rwlock_wrlock(&table->rwlock);

    if (table->columns_count != table->columns_capacity) {
        send_message->status = TABLE_NOT_FULLY_INITIALIZED;
        pthread_rwlock_unlock(&table->rwlock);
        return;
    }

    Column *source = pos->source;

    if (source != NULL) {
        ColumnIndex *source_index = source->index;

        if (source_index != NULL && source_index->clustered) {
            unsigned int *mapped_positions = malloc(positions_count * sizeof(unsigned int));

            unsigned int *clustered_positions = source_index->clustered_positions->data;
            for (unsigned int i = 0; i < positions_count; i++) {
                mapped_positions[i] = clustered_positions[positions[i]];
            }

            for (unsigned int i = 0; i < positions_count; i++) {
                dsl_update(table, column, mapped_positions[i], value);
            }

            free(mapped_positions);
        } else {
            for (unsigned int i = 0; i < positions_count; i++) {
                dsl_update(table, column, positions[i], value);
            }
        }
    } else {
        for (unsigned int i = 0; i < positions_count; i++) {
            dsl_update(table, column, positions[i], value);
        }
    }

    pthread_rwlock_unlock(&table->rwlock);
}

void dsl_join(ClientContext *client_context, JoinType type, char *val_var1, char *pos_var1,
        char *val_var2, char *pos_var2, char *pos_out_var1, char *pos_out_var2,
        Message *send_message) {
    Result *val1 = result_lookup(client_context, val_var1);
    if (val1 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (val1->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    Result *pos1 = result_lookup(client_context, pos_var1);
    if (pos1 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (pos1->type != POS) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    int *values1 = val1->values.int_values;
    unsigned int values1_count = val1->num_tuples;

    unsigned int *positions1 = pos1->values.pos_values;
    unsigned int positions1_count = pos1->num_tuples;

    if (values1_count != positions1_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    Result *val2 = result_lookup(client_context, val_var2);
    if (val2 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (val2->type != INT) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    Result *pos2 = result_lookup(client_context, pos_var2);
    if (pos2 == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (pos2->type != POS) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    int *values2 = val2->values.int_values;
    unsigned int values2_count = val2->num_tuples;

    unsigned int *positions2 = pos2->values.pos_values;
    unsigned int positions2_count = pos2->num_tuples;

    if (values2_count != positions2_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    unsigned int *result1 = NULL;
    unsigned int result1_count = 0;

    unsigned int *result2 = NULL;
    unsigned int result2_count = 0;

    if (values1_count > 0 && values2_count > 0) {
        PosVector pos_out1;
        pos_vector_init(&pos_out1, pos1->num_tuples);

        PosVector pos_out2;
        pos_vector_init(&pos_out2, pos2->num_tuples);

        switch (type) {
        case HASH:
            join_hash(values1, positions1, values1_count, values2, positions2, values2_count,
                    &pos_out1, &pos_out2);
            break;
        case NESTED_LOOP:
            join_nested_loop(values1, positions1, values1_count, values2, positions2, values2_count,
                    &pos_out1, &pos_out2);
            break;
        case SORT_MERGE:
            join_sort_merge(values1, positions1, values1_count, values2, positions2, values2_count,
                    &pos_out1, &pos_out2);
            break;
        }

        if (pos_out1.size == 0) {
            pos_vector_destroy(&pos_out1);
        } else {
            result1 = pos_out1.data;
            result1_count = pos_out1.size;
            if (result1_count < pos_out1.capacity) {
                result1 = realloc(result1, result1_count * sizeof(unsigned int));
            }
        }

        if (pos_out2.size == 0) {
            pos_vector_destroy(&pos_out2);
        } else {
            result2 = pos_out2.data;
            result2_count = pos_out2.size;
            if (result2_count < pos_out2.capacity) {
                result2 = realloc(result2, result2_count * sizeof(unsigned int));
            }
        }
    }

    pos_result_put(client_context, pos_out_var1, pos1->source, result1, result1_count);

    pos_result_put(client_context, pos_out_var2, pos2->source, result2, result2_count);
}

void dsl_min(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    pthread_rwlock_t *table_rwlock;
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (rows_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    int min_value = 0;
    if (index == NULL) {
        if (deleted_rows == NULL) {
            min_value = values[0];

            for (unsigned int i = 1; i < values_count; i++) {
                int value = values[i];
                min_value = value < min_value ? value : min_value;
            }
        } else {
            unsigned int i = 0;

            for (; i < values_count; i++) {
                if (!deleted_rows[i]) {
                    break;
                }
            }

            min_value = values[i++];

            for (; i < values_count; i++) {
                int value = values[i];
                min_value = (!deleted_rows[i] & (value < min_value)) ? value : min_value;
            }
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
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        source = column;
        pthread_rwlock_rdlock(table_rwlock = &column->table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (rows_count == 0) {
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

        if (deleted_rows == NULL) {
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
            min_position = 0;

            for (; min_position < values_count; min_position++) {
                if (!deleted_rows[min_position]) {
                    break;
                }
            }

            min_value = values[min_position];

            for (unsigned int i = min_position + 1; i < values_count; i++) {
                int value = values[i];

                bool smaller = !deleted_rows[i] & (value < min_value);

                min_position = smaller ? i : min_position;
                min_value = smaller ? value : min_value;
            }
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
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (rows_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    int max_value = 0;
    if (index == NULL) {
        if (deleted_rows == NULL) {
            max_value = values[0];

            for (unsigned int i = 1; i < values_count; i++) {
                int value = values[i];
                max_value = value > max_value ? value : max_value;
            }
        } else {
            unsigned int i = 0;

            for (; i < values_count; i++) {
                if (!deleted_rows[i]) {
                    break;
                }
            }

            max_value = values[i++];

            for (; i < values_count; i++) {
                int value = values[i];
                max_value = (!deleted_rows[i] & (value > max_value)) ? value : max_value;
            }
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
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    ColumnIndex *index;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        source = column;
        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
        index = NULL;
    }

    if (rows_count == 0) {
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

        if (deleted_rows == NULL) {
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
            max_position = 0;

            for (; max_position < values_count; max_position++) {
                if (!deleted_rows[max_position]) {
                    break;
                }
            }

            max_value = values[max_position];

            for (unsigned int i = max_position + 1; i < values_count; i++) {
                int value = values[i];

                bool larger = !deleted_rows[i] & (value > max_value);

                max_position = larger ? i : max_position;
                max_value = larger ? value : max_value;
            }
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
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    long long int result = 0;
    if (rows_count > 0) {
        if (deleted_rows == NULL) {
            for (unsigned int i = 0; i < values_count; i++) {
                result += values[i];
            }
        } else {
            for (unsigned int i = 0; i < values_count; i++) {
                result += !deleted_rows[i] * values[i];
            }
        }
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
    unsigned int rows_count;
    bool *deleted_rows;
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->delete_queue.size > 0 ? table->deleted_rows->data : NULL;
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
        rows_count = variable->num_tuples;
        deleted_rows = NULL;
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (rows_count == 0) {
        send_message->status = EMPTY_VECTOR;
        if (table_rwlock != NULL) {
            pthread_rwlock_unlock(table_rwlock);
        }
        return;
    }

    long long int sum = 0;
    if (deleted_rows == NULL) {
        for (unsigned int i = 0; i < values_count; i++) {
            sum += values[i];
        }
    } else {
        for (unsigned int i = 0; i < values_count; i++) {
            sum += !deleted_rows[i] * values[i];
        }
    }
    double result = (double) sum / (double) rows_count;

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

    batch_execute_concurrently(client_context, send_message);
}

void dsl_shutdown() {
    shutdown_initiated = true;
}

bool is_shutdown_initiated() {
    return shutdown_initiated;
}

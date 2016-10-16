#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "client_context.h"
#include "db_manager.h"
#include "dsl.h"
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

void dsl_create_index(char *column_fqn, CreateIndexType type, bool clustered, Message *send_message) {
    (void) column_fqn;
    (void) type;
    (void) clustered;
    (void) send_message;
}

void dsl_load(Vector *col_fqns, IntVector *col_vals, Message *send_message) {
    unsigned int num_columns = col_fqns->size;

    Column *columns[num_columns];

    for (unsigned int i = 0; i < num_columns; i++) {
        Column *column = column_lookup(col_fqns->data[i]);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        } else {
            columns[i] = column;

            if (i == 0) {
                Table *table = column->table;
                if (num_columns != table->columns_capacity) {
                    send_message->status = INSERT_COLUMNS_MISMATCH;
                    return;
                } else if (table->columns_count != table->columns_capacity) {
                    send_message->status = TABLE_NOT_FULLY_INITIALIZED;
                    return;
                }
            }
        }
    }

    for (unsigned int i = 0; i < num_columns; i++) {
        int_vector_concat(&columns[i]->values, &col_vals[i]);
    }
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
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    unsigned int *result;
    unsigned int result_count;
    if (values_count == 0 || (has_low && has_high && low >= high)) {
        result = NULL;
        result_count = 0;
    } else {
        result = malloc(values_count * sizeof(unsigned int));

        if (!has_low) {
            result_count = dsl_select_lower(values, values_count, high, result);
        } else if (!has_high) {
            result_count = dsl_select_higher(values, values_count, low, result);
        } else if (low == high - 1) {
            result_count = dsl_select_equal(values, values_count, low, result);
        } else {
            result_count = dsl_select_range(values, values_count, low, high, result);
        }

        if (result_count == 0) {
            free(result);
            result = NULL;
        } else if (result_count < values_count) {
            result = realloc(result, result_count * sizeof(unsigned int));
        }
    }

    pos_result_put(client_context, pos_out_var, result, result_count);
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

    unsigned int *result;
    unsigned int result_count;
    if (values_count == 0 || (has_low && has_high && low >= high)) {
        result = NULL;
        result_count = 0;
    } else {
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

    pos_result_put(client_context, pos_out_var, result, result_count);
}

void dsl_fetch(ClientContext *client_context, char *column_fqn, char *pos_var, char *val_out_var,
        Message *send_message) {
    Column *column = column_lookup(column_fqn);
    if (column == NULL) {
        send_message->status = COLUMN_NOT_FOUND;
        return;
    }

    Result *pos = result_lookup(client_context, pos_var);
    if (pos == NULL) {
        send_message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (pos->type != POS) {
        send_message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    int *values = column->values.data;

    unsigned int *positions = pos->values.pos_values;
    unsigned int positions_count = pos->num_tuples;

    int *result;
    if (positions_count == 0) {
        result = NULL;
    } else {
        result = malloc(positions_count * sizeof(int));
        for (unsigned int i = 0; i < positions_count; i++) {
            result[i] = values[positions[i]];
        }
    }

    int_result_put(client_context, val_out_var, result, positions_count);
}

static inline void dsl_insert(Column *column, int value) {
    int_vector_append(&column->values, value);
}

void dsl_relational_insert(char *table_fqn, IntVector *values, Message *send_message) {
    Table *table = table_lookup(table_fqn);
    if (table == NULL) {
        send_message->status = TABLE_NOT_FOUND;
        return;
    }
    if (values->size != table->columns_capacity) {
        send_message->status = INSERT_COLUMNS_MISMATCH;
        return;
    }
    if (table->columns_count != table->columns_capacity) {
        send_message->status = TABLE_NOT_FULLY_INITIALIZED;
        return;
    }

    for (unsigned int i = 0; i < values->size; i++) {
        dsl_insert(&table->columns[i], values->data[i]);
    }
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
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        return;
    }

    int min_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        min_value = value < min_value ? value : min_value;
    }

    int *value_out = malloc(sizeof(int));
    *value_out = min_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_min_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message) {
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
        positions = pos->values.pos_values;
        positions_count = pos->num_tuples;
    } else {
        positions = NULL;
        positions_count = 0;
    }

    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        return;
    }

    if (positions != NULL && positions_count != values_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    unsigned int min_position = 0;
    int min_value = values[0];

    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];

        bool smaller = value < min_value;

        min_position = smaller ? i : min_position;
        min_value = smaller ? value : min_value;
    }

    if (positions != NULL) {
        min_position = positions[min_position];
    }

    unsigned int *position_out = malloc(sizeof(unsigned int));
    *position_out = min_position;

    pos_result_put(client_context, pos_out_var, position_out, 1);

    int *value_out = malloc(sizeof(int));
    *value_out = min_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_max(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        return;
    }

    int max_value = values[0];
    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];
        max_value = value > max_value ? value : max_value;
    }

    int *value_out = malloc(sizeof(int));
    *value_out = max_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_max_pos(ClientContext *client_context, char *pos_var, GeneralizedColumnHandle *col_hdl,
        char *pos_out_var, char *val_out_var, Message *send_message) {
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
        positions = pos->values.pos_values;
        positions_count = pos->num_tuples;
    } else {
        positions = NULL;
        positions_count = 0;
    }

    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        return;
    }

    if (positions != NULL && positions_count != values_count) {
        send_message->status = TUPLE_COUNT_MISMATCH;
        return;
    }

    unsigned int max_position = 0;
    int max_value = values[0];

    for (unsigned int i = 1; i < values_count; i++) {
        int value = values[i];

        bool larger = value > max_value;

        max_position = larger ? i : max_position;
        max_value = larger ? value : max_value;
    }

    if (positions != NULL) {
        max_position = positions[max_position];
    }

    unsigned int *position_out = malloc(sizeof(unsigned int));
    *position_out = max_position;

    pos_result_put(client_context, pos_out_var, position_out, 1);

    int *value_out = malloc(sizeof(int));
    *value_out = max_value;

    int_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_sum(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    long long int result = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        result += values[i];
    }

    long long int *value_out = malloc(sizeof(long long int));
    *value_out = result;

    long_result_put(client_context, val_out_var, value_out, 1);
}

void dsl_avg(ClientContext *client_context, GeneralizedColumnHandle *col_hdl, char *val_out_var,
        Message *send_message) {
    int *values;
    unsigned int values_count;
    if (col_hdl->is_column_fqn) {
        Column *column = column_lookup(col_hdl->name);
        if (column == NULL) {
            send_message->status = COLUMN_NOT_FOUND;
            return;
        }
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
        values = variable->values.int_values;
        values_count = variable->num_tuples;
    }

    if (values_count == 0) {
        send_message->status = EMPTY_VECTOR;
        return;
    }

    long long int sum = 0;
    for (unsigned int i = 0; i < values_count; i++) {
        sum += values[i];
    }
    double result = (double) sum / (double) values_count;

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

    int *result;
    if (values1_count == 0) {
        result = NULL;
    } else {
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

    int *result;
    if (values1_count == 0) {
        result = NULL;
    } else {
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

void dsl_batch_queries(Message *send_message) {
    (void) send_message;
}

void dsl_batch_execute(Message *send_message) {
    (void) send_message;
}

void dsl_shutdown(Message *send_message) {
    (void) send_message;
    shutdown_initiated = true;
}

bool is_shutdown_initiated() {
    return shutdown_initiated;
}

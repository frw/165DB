
#define _BSD_SOURCE

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "client_context.h"
#include "db_manager.h"
#include "db_operator.h"
#include "dsl.h"
#include "parser.h"
#include "utils.h"
#include "vector.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 */
static inline char *next_token(char **tokenizer, char *sep, MessageStatus *status, MessageStatus error) {
    char *token = strsep(tokenizer, sep);
    if (token == NULL) {
        *status = error;
    }
    return token;
}

DbOperator *parse_create_db(char *create_arguments, Message *message) {
    char *db_name = strsep(&create_arguments, ",");

    if (db_name == NULL) {
        // Not enough arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (create_arguments != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    char *db_name_stripped = strip_quotes(db_name);
    if (db_name_stripped == db_name) {
        // Quotes were not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }
    if (!is_valid_name(db_name_stripped)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_DB;
    dbo->fields.create_db.name = strdup(db_name_stripped);
    return dbo;
}

DbOperator *parse_create_tbl(char *create_arguments, Message *message) {
    char **create_arguments_index = &create_arguments;
    MessageStatus *status = &message->status;

    char *table_name = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *db_name = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *num_cols = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (create_arguments != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    char *table_name_stripped = strip_quotes(table_name);
    if (table_name_stripped == table_name) {
        // Quotes were not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }
    if (!is_valid_name(table_name_stripped)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    if (!is_valid_name(db_name)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *endptr;
    unsigned int column_count = strtoul(num_cols, &endptr, 10);
    if (endptr == num_cols || *endptr != '\0') {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }
    if (column_count < 1 || column_count > MAX_TABLE_LENGTH) {
        message->status = INVALID_NUMBER_OF_COLUMNS;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_TBL;
    dbo->fields.create_table.name = strdup(table_name_stripped);
    dbo->fields.create_table.db_name = strdup(db_name);
    dbo->fields.create_table.num_columns = column_count;
    return dbo;
}

DbOperator *parse_create_col(char *create_arguments, Message *message) {
    char **create_arguments_index = &create_arguments;
    MessageStatus *status = &message->status;

    char *column_name = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *table_fqn = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (create_arguments != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    char *column_name_stripped = strip_quotes(column_name);
    if (column_name_stripped == column_name) {
        // Quotes were not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }
    if (!is_valid_name(column_name_stripped)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    if (!is_valid_fqn(table_fqn, 1)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_COL;
    dbo->fields.create_column.name = strdup(column_name_stripped);
    dbo->fields.create_column.table_fqn = strdup(table_fqn);
    return dbo;
}

DbOperator *parse_create_idx(char *create_arguments, Message *message) {
    char **create_arguments_index = &create_arguments;
    MessageStatus *status = &message->status;

    char *column_fqn = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *index_type = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *clustered_param = next_token(create_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (create_arguments != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (!is_valid_fqn(column_fqn, 2)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    CreateIndexType type;
    if (strcmp(index_type, "btree") == 0) {
        type = BTREE;
    } else if (strcmp(index_type, "sorted") == 0) {
        type = SORTED;
    } else {
        message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    bool clustered;
    if (strcmp(clustered_param, "clustered") == 0) {
        clustered = true;
    } else if (strcmp(clustered_param, "unclustered") == 0) {
        clustered = false;
    } else {
        message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_IDX;
    dbo->fields.create_index.column_fqn = strdup(column_fqn);
    dbo->fields.create_index.type = type;
    dbo->fields.create_index.clustered = clustered;
    return dbo;
}

DbOperator *parse_create(char *handle, char *create_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *create_arguments_stripped = strip_parenthesis(create_arguments);
    if (create_arguments_stripped == create_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *type = strsep(&create_arguments_stripped, ",");
    if (type == NULL) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }
    if (*type == '\0') {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (strcmp(type, "db") == 0) {
        return parse_create_db(create_arguments_stripped, message);
    } else if (strcmp(type, "tbl") == 0) {
        return parse_create_tbl(create_arguments_stripped, message);
    } else if (strcmp(type, "col") == 0) {
        return parse_create_col(create_arguments_stripped, message);
    } else if (strcmp(type, "idx") == 0) {
        return parse_create_idx(create_arguments_stripped, message);
    } else {
        message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator *parse_load(char *handle, char *load_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *load_arguments_stripped = strip_parenthesis(load_arguments);
    if (load_arguments_stripped == load_arguments) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *load_arguments_stripped2 = strip_quotes(load_arguments_stripped);
    if (load_arguments_stripped2 == load_arguments_stripped) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    return dbo;
}

static inline void parse_optional_number(char *token, int *value, bool *has_value, MessageStatus *status) {
    if (strcmp(token, "null") == 0) {
        *value = 0;
        *has_value = false;
    } else {
        char *endptr;
        int val = strtol(token, &endptr, 10);
        if (endptr == token || *endptr != '\0') {
            *status = INCORRECT_FORMAT;
            return;
        }
        *value = val;
        *has_value = true;
    }
}

DbOperator *parse_select(char *pos_out_var, char *select_arguments, Message *message) {
    if (pos_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(pos_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *select_arguments_stripped = strip_parenthesis(select_arguments);
    if (select_arguments_stripped == select_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **select_arguments_index = &select_arguments_stripped;
    MessageStatus *status = &message->status;

    char *arg1 = next_token(select_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *arg2 = next_token(select_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *arg3 = next_token(select_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (select_arguments_stripped == NULL) {
        char *col_hdl = arg1;
        char *low = arg2;
        char *high = arg3;

        bool is_column_fqn;
        if (is_valid_name(col_hdl)) {
            is_column_fqn = false;
        } else if (is_valid_fqn(col_hdl, 2)) {
            is_column_fqn = true;
        } else {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        int low_val = 0;
        bool has_low = false;
        parse_optional_number(low, &low_val, &has_low, status);
        if (message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        int high_val = 0;
        bool has_high = false;
        parse_optional_number(high, &high_val, &has_high, status);
        if (message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        if (!has_low && !has_high) {
            message->status = NO_SELECT_CONDITION;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->fields.select.col_hdl.name = strdup(col_hdl);
        dbo->fields.select.col_hdl.is_column_fqn = is_column_fqn;
        dbo->fields.select.low = low_val;
        dbo->fields.select.has_low = has_low;
        dbo->fields.select.high = high_val;
        dbo->fields.select.has_high = has_high;
        dbo->fields.select.pos_out_var = strdup(pos_out_var);
        return dbo;
    } else {
        char *pos_var = arg1;
        char *val_var = arg2;
        char *low = arg3;
        char *high = select_arguments_stripped;

        if (!is_valid_name(pos_var) || !is_valid_name(val_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        int low_val = 0;
        bool has_low = false;
        parse_optional_number(low, &low_val, &has_low, status);
        if (message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        int high_val = 0;
        bool has_high = false;
        parse_optional_number(high, &high_val, &has_high, status);
        if (message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        if (!has_low && !has_high) {
            message->status = NO_SELECT_CONDITION;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT_POS;
        dbo->fields.select_pos.pos_var = strdup(pos_var);
        dbo->fields.select_pos.val_var = strdup(val_var);
        dbo->fields.select_pos.low = low_val;
        dbo->fields.select_pos.has_low = has_low;
        dbo->fields.select_pos.high = high_val;
        dbo->fields.select_pos.has_high = has_high;
        dbo->fields.select_pos.pos_out_var = strdup(pos_out_var);
        return dbo;
    }
}

DbOperator *parse_fetch(char *val_out_var, char *fetch_arguments, Message *message) {
    if (val_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(val_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *fetch_arguments_stripped = strip_parenthesis(fetch_arguments);
    if (fetch_arguments_stripped == fetch_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **fetch_arguments_index = &fetch_arguments_stripped;
    MessageStatus *status = &message->status;

    char *column_fqn = next_token(fetch_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *pos_var = next_token(fetch_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (fetch_arguments_stripped != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (!is_valid_fqn(column_fqn, 2)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    if (!is_valid_name(pos_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->fields.fetch.column_fqn = strdup(column_fqn);
    dbo->fields.fetch.pos_var = strdup(pos_var);
    dbo->fields.fetch.val_out_var = strdup(val_out_var);
    return dbo;
}

DbOperator *parse_relational_insert(char *handle, char *insert_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *insert_arguments_stripped = strip_parenthesis(insert_arguments);
    if (insert_arguments_stripped == insert_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **insert_arguments_index = &insert_arguments_stripped;

    char *table_fqn = strsep(insert_arguments_index, ",");
    if (*table_fqn == '\0') {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }
    if (!is_valid_fqn(table_fqn, 1)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    if (insert_arguments_stripped == NULL) {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    IntVector values;
    int_vector_init(&values, 4);
    char *token;
    while ((token = strsep(insert_arguments_index, ",")) != NULL) {
        char *endptr;
        int value = strtol(token, &endptr, 10);
        if (endptr == token || *endptr != '\0') {
            message->status = INCORRECT_FORMAT;
            int_vector_destroy(&values);
            return NULL;
        }
        int_vector_append(&values, value);
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = RELATIONAL_INSERT;
    dbo->fields.relational_insert.table_fqn = strdup(table_fqn);
    int_vector_shallow_copy(&dbo->fields.relational_insert.values, &values);
    return dbo;
}

DbOperator *parse_min(char *handle, char *max_arguments, Message *message) {
    if (handle == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *min_arguments_stripped = strip_parenthesis(max_arguments);
    if (min_arguments_stripped == max_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **min_arguments_index = &min_arguments_stripped;
    MessageStatus *status = &message->status;

    char *arg1 = next_token(min_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (min_arguments_stripped == NULL) {
        char *col_hdl = arg1;
        if (*col_hdl == '\0') {
            message->status = WRONG_NUMBER_OF_ARGUMENTS;
            return NULL;
        }
        bool is_column_fqn;
        if (is_valid_name(col_hdl)) {
            is_column_fqn = false;
        } else if (is_valid_fqn(col_hdl, 2)) {
            is_column_fqn = true;
        } else {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        char *val_out_var = handle;
        if (!is_valid_name(val_out_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = MIN;
        dbo->fields.min.col_hdl.name = strdup(col_hdl);
        dbo->fields.min.col_hdl.is_column_fqn = is_column_fqn;
        dbo->fields.min.val_out_var = strdup(val_out_var);
        return dbo;
    } else {
        char *pos_var = arg1;
        char *col_hdl = min_arguments_stripped;

        if (strcmp(pos_var, "null") == 0) {
            pos_var = NULL;
        } else if (!is_valid_name(pos_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        bool is_column_fqn;
        if (is_valid_name(col_hdl)) {
            is_column_fqn = false;
        } else if (is_valid_fqn(col_hdl, 2)) {
            is_column_fqn = true;
        } else {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        char *pos_out_var = next_token(&handle, ",", status, WRONG_NUMBER_OF_HANDLES);
        char *val_out_var = next_token(&handle, ",", status, WRONG_NUMBER_OF_HANDLES);

        if (message->status == WRONG_NUMBER_OF_HANDLES) {
            // Not enough handles.
            return NULL;
        }

        if (handle != NULL) {
            // Too many handles.
            message->status = WRONG_NUMBER_OF_HANDLES;
            return NULL;
        }

        if (!is_valid_name(pos_out_var) || !is_valid_name(val_out_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = MIN_POS;
        dbo->fields.min_pos.pos_var = pos_var == NULL ? NULL : strdup(pos_var);
        dbo->fields.min_pos.col_hdl.name = strdup(col_hdl);
        dbo->fields.min_pos.col_hdl.is_column_fqn = is_column_fqn;
        dbo->fields.min_pos.pos_out_var = strdup(pos_out_var);
        dbo->fields.min_pos.val_out_var = strdup(val_out_var);
        return dbo;
    }
}

DbOperator *parse_max(char *handle, char *max_arguments, Message *message) {
    if (handle == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *max_arguments_stripped = strip_parenthesis(max_arguments);
    if (max_arguments_stripped == max_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **max_arguments_index = &max_arguments_stripped;
    MessageStatus *status = &message->status;

    char *arg1 = next_token(max_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (max_arguments_stripped == NULL) {
        char *col_hdl = arg1;
        if (*col_hdl == '\0') {
            message->status = WRONG_NUMBER_OF_ARGUMENTS;
            return NULL;
        }
        bool is_column_fqn;
        if (is_valid_name(col_hdl)) {
            is_column_fqn = false;
        } else if (is_valid_fqn(col_hdl, 2)) {
            is_column_fqn = true;
        } else {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        char *val_out_var = handle;
        if (!is_valid_name(val_out_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = MAX;
        dbo->fields.max.col_hdl.name = strdup(col_hdl);
        dbo->fields.max.col_hdl.is_column_fqn = is_column_fqn;
        dbo->fields.max.val_out_var = strdup(val_out_var);
        return dbo;
    } else {
        char *pos_var = arg1;
        char *col_hdl = max_arguments_stripped;

        if (strcmp(pos_var, "null") == 0) {
            pos_var = NULL;
        } else if (!is_valid_name(pos_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        bool is_column_fqn;
        if (is_valid_name(col_hdl)) {
            is_column_fqn = false;
        } else if (is_valid_fqn(col_hdl, 2)) {
            is_column_fqn = true;
        } else {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        char *pos_out_var = next_token(&handle, ",", status, WRONG_NUMBER_OF_HANDLES);
        char *val_out_var = next_token(&handle, ",", status, WRONG_NUMBER_OF_HANDLES);

        if (message->status == WRONG_NUMBER_OF_HANDLES) {
            // Not enough handles.
            return NULL;
        }

        if (handle != NULL) {
            // Too many handles.
            message->status = WRONG_NUMBER_OF_HANDLES;
            return NULL;
        }

        if (!is_valid_name(pos_out_var) || !is_valid_name(val_out_var)) {
            message->status = INCORRECT_FORMAT;
            return NULL;
        }

        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = MAX_POS;
        dbo->fields.max_pos.pos_var = pos_var == NULL ? NULL : strdup(pos_var);
        dbo->fields.max_pos.col_hdl.name = strdup(col_hdl);
        dbo->fields.max_pos.col_hdl.is_column_fqn = is_column_fqn;
        dbo->fields.max_pos.pos_out_var = strdup(pos_out_var);
        dbo->fields.max_pos.val_out_var = strdup(val_out_var);
        return dbo;
    }
}

DbOperator *parse_sum(char *val_out_var, char *sum_arguments, Message *message) {
    if (val_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(val_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *sum_arguments_stripped = strip_parenthesis(sum_arguments);
    if (sum_arguments_stripped == sum_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *col_hdl = sum_arguments_stripped;
    if (*col_hdl == '\0') {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }
    bool is_column_fqn;
    if (is_valid_name(col_hdl)) {
        is_column_fqn = false;
    } else if (is_valid_fqn(col_hdl, 2)) {
        is_column_fqn = true;
    } else {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = SUM;
    dbo->fields.sum.col_hdl.name = strdup(col_hdl);
    dbo->fields.sum.col_hdl.is_column_fqn = is_column_fqn;
    dbo->fields.sum.val_out_var = strdup(val_out_var);
    return dbo;
}

DbOperator *parse_avg(char *val_out_var, char *avg_arguments, Message *message) {
    if (val_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(val_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *avg_arguments_stripped = strip_parenthesis(avg_arguments);
    if (avg_arguments_stripped == avg_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *col_hdl = avg_arguments_stripped;
    if (*col_hdl == '\0') {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }
    bool is_column_fqn;
    if (is_valid_name(col_hdl)) {
        is_column_fqn = false;
    } else if (is_valid_fqn(col_hdl, 2)) {
        is_column_fqn = true;
    } else {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = AVG;
    dbo->fields.avg.col_hdl.name = strdup(col_hdl);
    dbo->fields.avg.col_hdl.is_column_fqn = is_column_fqn;
    dbo->fields.avg.val_out_var = strdup(val_out_var);
    return dbo;
}

DbOperator *parse_add(char *val_out_var, char *add_arguments, Message *message) {
    if (val_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(val_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *add_arguments_stripped = strip_parenthesis(add_arguments);
    if (add_arguments_stripped == add_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **add_arguments_index = &add_arguments_stripped;
    MessageStatus *status = &message->status;

    char *val_var1 = next_token(add_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *val_var2 = next_token(add_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (add_arguments_stripped != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (!is_valid_name(val_var1) || !is_valid_name(val_var2)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = ADD;
    dbo->fields.add.val_var1 = strdup(val_var1);
    dbo->fields.add.val_var2 = strdup(val_var2);
    dbo->fields.add.val_out_var = strdup(val_out_var);
    return dbo;
}

DbOperator *parse_sub(char *val_out_var, char *sub_arguments, Message *message) {
    if (val_out_var == NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }
    if (!is_valid_name(val_out_var)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char *sub_arguments_stripped = strip_parenthesis(sub_arguments);
    if (sub_arguments_stripped == sub_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    char **sub_arguments_index = &sub_arguments_stripped;
    MessageStatus *status = &message->status;

    char *val_var1 = next_token(sub_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);
    char *val_var2 = next_token(sub_arguments_index, ",", status, WRONG_NUMBER_OF_ARGUMENTS);

    if (message->status == WRONG_NUMBER_OF_ARGUMENTS) {
        // Not enough arguments.
        return NULL;
    }

    if (sub_arguments_stripped != NULL) {
        // Too many arguments.
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    if (!is_valid_name(val_var1) || !is_valid_name(val_var2)) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = SUB;
    dbo->fields.sub.val_var1 = strdup(val_var1);
    dbo->fields.sub.val_var2 = strdup(val_var2);
    dbo->fields.sub.val_out_var = strdup(val_out_var);
    return dbo;
}

DbOperator *parse_print(char *handle, char *print_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    char *print_arguments_stripped = strip_parenthesis(print_arguments);
    if (print_arguments_stripped == print_arguments) {
        // Parenthesis was not stripped.
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    if (*print_arguments_stripped == '\0') {
        message->status = WRONG_NUMBER_OF_ARGUMENTS;
        return NULL;
    }

    char **print_arguments_index = &print_arguments_stripped;

    Vector val_vars;
    vector_init(&val_vars, 4);
    char *val_var;
    while ((val_var = strsep(print_arguments_index, ",")) != NULL) {
        if (!is_valid_name(val_var)) {
            message->status = INCORRECT_FORMAT;
            vector_destroy(&val_vars, &free);
            return NULL;
        }
        vector_append(&val_vars, strdup(val_var));
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    vector_shallow_copy(&dbo->fields.print.val_vars, &val_vars);
    return dbo;
}

DbOperator *parse_batch_queries(char *handle, char *print_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    if (*print_arguments != '\0' && strcmp(print_arguments, "()") != 0) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_QUERIES;
    return dbo;
}

DbOperator *parse_batch_execute(char *handle, char *print_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    if (*print_arguments != '\0' && strcmp(print_arguments, "()") != 0) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = BATCH_EXECUTE;
    return dbo;
}

DbOperator *parse_shutdown(char *handle, char *print_arguments, Message *message) {
    if (handle != NULL) {
        message->status = WRONG_NUMBER_OF_HANDLES;
        return NULL;
    }

    if (*print_arguments != '\0' && strcmp(print_arguments, "()") != 0) {
        message->status = INCORRECT_FORMAT;
        return NULL;
    }

    DbOperator *dbo = malloc(sizeof(DbOperator));
    dbo->type = SHUTDOWN;
    return dbo;
}

/**
 * parse_command takes as input the message from the client and then
 * parses it into the appropriate query. Stores into message the
 * status to send back.
 * Returns a db_operator.
 */
DbOperator *parse_command(char *query_command, Message *message, ClientContext *context) {
    query_command = strip_whitespace(query_command);

    char *comment_start = strstr(query_command, "--");
    if (comment_start != NULL) {
        *comment_start = '\0';
    }

    if (*query_command == '\0') {
        message->status = OK;
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle;
    if (equals_pointer != NULL) {
        // handle file table
        handle = query_command;
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    DbOperator *dbo;
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(handle, query_command, message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(handle, query_command, message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(handle, query_command, message);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(handle, query_command, message);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_relational_insert(handle, query_command, message);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_min(handle, query_command, message);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_max(handle, query_command, message);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_sum(handle, query_command, message);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_avg(handle, query_command, message);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(handle, query_command, message);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_sub(handle, query_command, message);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(handle, query_command, message);
    } else if (strncmp(query_command, "batch_queries", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_queries(handle, query_command, message);
    } else if (strncmp(query_command, "batch_execute", 13) == 0) {
        query_command += 13;
        dbo = parse_batch_execute(handle, query_command, message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 8;
        dbo = parse_shutdown(handle, query_command, message);
    } else {
        message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    if (dbo != NULL) {
        dbo->context = context;
    }

    return dbo;
}

#include <pthread.h>
#include <string.h>

#include "batch.h"
#include "client_context.h"
#include "db_operator.h"
#include "dsl.h"
#include "hash_table.h"
#include "message.h"
#include "utils.h"
#include "vector.h"

#define BATCH_MAX_SELECT 1
#define BATCH_MAX_SELECT_POS 1

#define BATCH_TABLE_INITIAL_CAPACITY 64
#define BATCH_TABLE_LOAD_FACTOR 0.75f

void batch_select(ClientContext *client_context, GeneralizedColumnHandle *col_hdl,
        Comparator *comparators, char **pos_out_vars, unsigned int batch_size, Message *message) {
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
            message->status = COLUMN_NOT_FOUND;
            return;
        }
        Table *table = column->table;

        source = column;
        pthread_rwlock_rdlock(table_rwlock = &table->rwlock);
        rows_count = table->rows_count;
        deleted_rows = table->deleted_rows != NULL ? table->deleted_rows->data : NULL;
        values = column->values.data;
        values_count = column->values.size;
        index = column->index;
    } else {
        Result *variable = result_lookup(client_context, col_hdl->name);
        if (variable == NULL) {
            message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (variable->type != INT) {
            message->status = WRONG_VARIABLE_TYPE;
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

    unsigned int *results[batch_size];
    memset(results, 0, batch_size * sizeof(unsigned int *));
    unsigned int result_counts[batch_size];
    memset(result_counts, 0, batch_size * sizeof(unsigned int));
    if (rows_count > 0) {
        for (unsigned int i = 0; i < batch_size; i++) {
            results[i] = malloc(rows_count * sizeof(unsigned int));
        }

        if (index == NULL) {
            if (deleted_rows == NULL) {
                for (unsigned int i = 0; i < values_count; i++) {
                    int value = values[i];
                    for (unsigned int j = 0; j < batch_size; j++) {
                        Comparator *comparator = comparators + j;
                        results[j][result_counts[j]] = i;
                        result_counts[j] += (!comparator->has_low || value >= comparator->low)
                                & (!comparator->has_high || value < comparator->high);
                    }
                }
            } else {
                for (unsigned int i = 0; i < values_count; i++) {
                    int value = values[i];
                    if (!deleted_rows[i]) {
                        for (unsigned int j = 0; j < batch_size; j++) {
                            Comparator *comparator = comparators + j;
                            results[j][result_counts[j]] = i;
                            result_counts[j] += (!comparator->has_low || value >= comparator->low)
                                    & (!comparator->has_high || value < comparator->high);
                        }
                    }
                }
            }
        } else {
            for (unsigned int i = 0; i < batch_size; i++) {
                Comparator *comparator = comparators + i;

                switch (index->type) {
                case BTREE:
                    if (!comparator->has_low) {
                        result_counts[i] = btree_select_lower(&index->fields.btree,
                                comparator->high, results[i]);
                    } else if (!comparator->has_high) {
                        result_counts[i] = btree_select_higher(&index->fields.btree,
                                comparator->low, results[i]);
                    } else {
                        result_counts[i] = btree_select_range(&index->fields.btree, comparator->low,
                                comparator->high, results[i]);
                    }
                    break;
                case SORTED:
                    if (!comparator->has_low) {
                        result_counts[i] = sorted_select_lower(&index->fields.sorted,
                                comparator->high, results[i]);
                    } else if (!comparator->has_high) {
                        result_counts[i] = sorted_select_higher(&index->fields.sorted,
                                comparator->low, results[i]);
                    } else {
                        result_counts[i] = sorted_select_range(&index->fields.sorted,
                                comparator->low, comparator->high, results[i]);
                    }
                    break;
                }
            }
        }

        for (unsigned int i = 0; i < batch_size; i++) {
            unsigned int result_count = result_counts[i];
            if (result_count == 0) {
                free(results[i]);
                results[i] = NULL;
            } else if (result_count < rows_count) {
                results[i] = realloc(results[i], result_count * sizeof(unsigned int));
            }
        }
    }

    if (table_rwlock != NULL) {
        pthread_rwlock_unlock(table_rwlock);
    }

    for (unsigned int i = 0; i < batch_size; i++) {
        pos_result_put(client_context, pos_out_vars[i], source, results[i], result_counts[i]);
    }
}

void batch_select_pos(ClientContext *client_context, char *val_var, char **pos_vars,
        Comparator *comparators, char **pos_out_vars, unsigned int batch_size, Message *message) {
    Result *val = result_lookup(client_context, val_var);
    if (val == NULL) {
        message->status = VARIABLE_NOT_FOUND;
        return;
    }
    if (val->type != INT) {
        message->status = WRONG_VARIABLE_TYPE;
        return;
    }

    int *values = val->values.int_values;
    unsigned int values_count = val->num_tuples;

    unsigned int *positions[batch_size];
    Column *sources[batch_size];

    for (unsigned int i = 0; i < batch_size; i++) {
        Result *pos = result_lookup(client_context, pos_vars[i]);
        if (pos == NULL) {
            message->status = VARIABLE_NOT_FOUND;
            return;
        }
        if (pos->type != POS) {
            message->status = WRONG_VARIABLE_TYPE;
            return;
        }
        if (pos->num_tuples != values_count) {
            message->status = TUPLE_COUNT_MISMATCH;
            return;
        }

        positions[i] = pos->values.pos_values;
        sources[i] = pos->source;
    }

    unsigned int *results[batch_size];
    memset(results, 0, batch_size * sizeof(unsigned int *));
    unsigned int result_counts[batch_size];
    memset(result_counts, 0, batch_size * sizeof(unsigned int));
    if (values_count > 0) {
        for (unsigned int i = 0; i < batch_size; i++) {
            results[i] = malloc(values_count * sizeof(unsigned int));
        }

        for (unsigned int i = 0; i < values_count; i++) {
            int value = values[i];
            for (unsigned int j = 0; j < batch_size; j++) {
                Comparator *comparator = comparators + j;
                results[j][result_counts[j]] = positions[j][i];
                result_counts[j] += (!comparator->has_low || value >= comparator->low)
                        & (!comparator->has_high || value < comparator->high);
            }
        }

        for (unsigned int i = 0; i < batch_size; i++) {
            unsigned int result_count = result_counts[i];
            if (result_count == 0) {
                free(results[i]);
                results[i] = NULL;
            } else if (result_count < values_count) {
                results[i] = realloc(results[i], result_count * sizeof(unsigned int));
            }
        }
    }


    for (unsigned int i = 0; i < batch_size; i++) {
        pos_result_put(client_context, pos_out_vars[i], sources[i], results[i], result_counts[i]);
    }
}

void batch_handle_operator(DbOperator **dbos, unsigned int batch_size, Message *message) {
    DbOperator *first = dbos[0];

    if (first->type == SELECT) {
        Comparator comparators[batch_size];
        char *pos_out_vars[batch_size];

        for (unsigned int i = 0; i < batch_size; i++) {
            DbOperator *dbo = dbos[i];

            comparators[i] = dbo->fields.select.comparator;
            pos_out_vars[i] = dbo->fields.select.pos_out_var;
        }

        batch_select(first->context, &first->fields.select.col_hdl, comparators, pos_out_vars,
                batch_size, message);
    } else if (first->type == SELECT_POS) {
        char *pos_vars[batch_size];
        Comparator comparators[batch_size];
        char *pos_out_vars[batch_size];

        for (unsigned int i = 0; i < batch_size; i++) {
            DbOperator *dbo = dbos[i];

            pos_vars[i] = dbo->fields.select_pos.pos_var;
            comparators[i] = dbo->fields.select_pos.comparator;
            pos_out_vars[i] = dbo->fields.select.pos_out_var;
        }

        batch_select_pos(first->context, first->fields.select_pos.val_var, pos_vars, comparators,
                pos_out_vars, batch_size, message);
    }

    for (unsigned int i = 0; i < batch_size; i++) {
        db_operator_free(dbos[i]);
    }
    free(dbos);
}

typedef struct BatchQuery {
    union {
        DbOperator *dbo;
        DbOperator **dbos;
    } operators;
    unsigned int batch_size;
    bool success;
} BatchQuery;

void *query_routine(void *data) {
    BatchQuery *query = data;

    Message message = MESSAGE_INITIALIZER;
    if (query->batch_size == 1) {
        db_operator_execute(query->operators.dbo, &message);
        db_operator_free(query->operators.dbo);
    } else if (query->batch_size > 1) {
        batch_handle_operator(query->operators.dbos, query->batch_size, &message);
    }
    query->success = message.status == OK;

    return NULL;
}

void batch_query(DbOperator *dbo, Message *message) {
    switch (dbo->type) {
    case SELECT:
    case SELECT_POS:
    case FETCH:
    case JOIN:
    case MIN:
    case MIN_POS:
    case MAX:
    case MAX_POS:
    case SUM:
    case AVG:
    case ADD:
    case SUB:
        vector_append(&dbo->context->batched_operators, dbo);
        break;

    default:
        message->status = BATCH_QUERY_UNSUPPORTED;
        db_operator_free(dbo);
        break;
    }
}

static inline bool check_dependency(DbOperator *dbo, HashTable *output_table) {
    DbOperator *query;

    switch (dbo->type) {
    case SELECT:
        if (!dbo->fields.select.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.select.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case SELECT_POS:
        query = hash_table_get(output_table, dbo->fields.select_pos.pos_var);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.select_pos.val_var);
        if (query != NULL) {
            return false;
        }
        break;

    case FETCH:
        query = hash_table_get(output_table, dbo->fields.fetch.pos_var);
        if (query != NULL) {
            return false;
        }
        break;

    case JOIN:
        query = hash_table_get(output_table, dbo->fields.join.val_var1);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.join.pos_var1);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.join.val_var2);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.join.pos_var2);
        if (query != NULL) {
            return false;
        }
        break;

    case MIN:
        if (!dbo->fields.min.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.min.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case MIN_POS:
        query = hash_table_get(output_table, dbo->fields.min_pos.pos_var);
        if (query != NULL) {
            return false;
        }

        if (!dbo->fields.min_pos.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.min_pos.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case MAX:
        if (!dbo->fields.max.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.max.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case MAX_POS:
        query = hash_table_get(output_table, dbo->fields.max_pos.pos_var);
        if (query != NULL) {
            return false;
        }

        if (!dbo->fields.max_pos.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.max_pos.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case SUM:
        if (!dbo->fields.sum.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.sum.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case AVG:
        if (!dbo->fields.avg.col_hdl.is_column_fqn) {
            query = hash_table_get(output_table, dbo->fields.avg.col_hdl.name);
            if (query != NULL) {
                return false;
            }
        }
        break;

    case ADD:
        query = hash_table_get(output_table, dbo->fields.add.val_var1);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.add.val_var2);
        if (query != NULL) {
            return false;
        }
        break;

    case SUB:
        query = hash_table_get(output_table, dbo->fields.sub.val_var1);
        if (query != NULL) {
            return false;
        }

        query = hash_table_get(output_table, dbo->fields.sub.val_var2);
        if (query != NULL) {
            return false;
        }
        break;

    default:
        break;
    }

    return true;
}

static inline void register_output(DbOperator *dbo, HashTable *output_table) {
    DbOperator *query;

    switch (dbo->type) {
    case SELECT:
        query = hash_table_put(output_table, dbo->fields.select.pos_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case SELECT_POS:
        query = hash_table_put(output_table, dbo->fields.select_pos.pos_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case FETCH:
        query = hash_table_put(output_table, dbo->fields.fetch.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case JOIN:
        query = hash_table_put(output_table, dbo->fields.join.pos_out_var1, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }

        if (strcmp(dbo->fields.join.pos_out_var2, dbo->fields.join.pos_out_var1) != 0) {
            query = hash_table_put(output_table, dbo->fields.join.pos_out_var2, dbo);
            if (query != NULL) {
                db_operator_free(query);
            }
        }
        break;

    case MIN:
        query = hash_table_put(output_table, dbo->fields.min.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case MIN_POS:
        query = hash_table_put(output_table, dbo->fields.min_pos.pos_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }

        if (strcmp(dbo->fields.min_pos.val_out_var, dbo->fields.min_pos.pos_out_var) != 0) {
            query = hash_table_put(output_table, dbo->fields.min_pos.val_out_var, dbo);
            if (query != NULL) {
                db_operator_free(query);
            }
        }
        break;

    case MAX:
        query = hash_table_put(output_table, dbo->fields.max.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case MAX_POS:
        query = hash_table_put(output_table, dbo->fields.max_pos.pos_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }

        if (strcmp(dbo->fields.max_pos.val_out_var, dbo->fields.max_pos.pos_out_var) != 0) {
            query = hash_table_put(output_table, dbo->fields.max_pos.val_out_var, dbo);
            if (query != NULL) {
                db_operator_free(query);
            }
        }
        break;

    case SUM:
        query = hash_table_put(output_table, dbo->fields.sum.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case AVG:
        query = hash_table_put(output_table, dbo->fields.avg.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case ADD:
        query = hash_table_put(output_table, dbo->fields.add.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    case SUB:
        query = hash_table_put(output_table, dbo->fields.sub.val_out_var, dbo);
        if (query != NULL) {
            db_operator_free(query);
        }
        break;

    default:
        break;
    }
}

static inline void batch_query_init(BatchQuery *query, DbOperator *dbo) {
    query->operators.dbo = dbo;
    query->batch_size = 1;
}

static inline void batch_query_attach(BatchQuery *query, DbOperator *dbo, unsigned int max_capacity) {
    if (query->batch_size == 1) {
        DbOperator *first = query->operators.dbo;
        query->operators.dbos = malloc(max_capacity * sizeof(DbOperator *));
        query->operators.dbos[0] = first;
    }

    query->operators.dbos[query->batch_size++] = dbo;
}

static inline bool execute_operators(HashTable *output_table) {
    if (output_table->num_nodes == 0) {
        return true;
    }

    if (output_table->num_nodes == 1) {
        DbOperator *dbo = output_table->nodes->value;
        Message message = MESSAGE_INITIALIZER;

        db_operator_execute(dbo, &message);
        db_operator_free(dbo);

        return message.status == OK;
    }

#if BATCH_MAX_SELECT > 1
    HashTable select_table;
    hash_table_init(&select_table, BATCH_TABLE_INITIAL_CAPACITY, BATCH_TABLE_LOAD_FACTOR);
#endif

#if BATCH_MAX_SELECT_POS > 1
    HashTable select_pos_table;
    hash_table_init(&select_pos_table, BATCH_TABLE_INITIAL_CAPACITY, BATCH_TABLE_LOAD_FACTOR);
#endif

    BatchQuery queries[output_table->num_nodes];

    unsigned int queries_count = 0;
    for (HashTableNode *node = output_table->nodes; node != NULL; node = node->nodes_next) {
        DbOperator *dbo = node->value;

        BatchQuery *query;

        switch (dbo->type) {
#if BATCH_MAX_SELECT > 1
        case SELECT:
            query = hash_table_get(&select_table, dbo->fields.select.col_hdl.name);

            if (query != NULL) {
                batch_query_attach(query, dbo, BATCH_MAX_SELECT);

                if (query->batch_size == BATCH_MAX_SELECT) {
                    hash_table_put(&select_table, dbo->fields.select.col_hdl.name, NULL);
                }
            } else {
                query = queries + queries_count++;
                batch_query_init(query, dbo);

                hash_table_put(&select_table, dbo->fields.select.col_hdl.name, query);
            }
            break;
#endif

#if BATCH_MAX_SELECT_POS > 1
        case SELECT_POS:
            query = hash_table_get(&select_pos_table, dbo->fields.select_pos.val_var);

            if (query != NULL) {
                batch_query_attach(query, dbo, BATCH_MAX_SELECT_POS);

                if (query->batch_size == BATCH_MAX_SELECT_POS) {
                    hash_table_put(&select_pos_table, dbo->fields.select_pos.val_var, NULL);
                }
            } else {
                query = queries + queries_count++;
                batch_query_init(query, dbo);

                hash_table_put(&select_pos_table, dbo->fields.select_pos.val_var, query);
            }
            break;
#endif

        default:
            query = queries + queries_count++;
            batch_query_init(query, dbo);
            break;
        }

    }

#if BATCH_MAX_SELECT > 1
    hash_table_destroy(&select_table, NULL);
#endif

#if BATCH_MAX_SELECT_POS > 1
    hash_table_destroy(&select_pos_table, NULL);
#endif

    pthread_t threads[queries_count];

    for (unsigned int i = 0; i < queries_count; i++) {
        if (pthread_create(threads + i, NULL, &query_routine, queries + i) != 0) {
            log_err("Unable to create query worker thread.");
            exit(1);
        }
    }

    bool success = true;

    for (unsigned int i = 0; i < queries_count; i++) {
        pthread_join(threads[i], NULL);

        if (!queries[i].success) {
            success = false;
        }
    }

    return success;
}

void batch_execute_concurrently(ClientContext *client_context, Message *message) {
    Vector *batched_operators = &client_context->batched_operators;

    if (batched_operators->size == 0) {
        return;
    }

    bool error = false;

    HashTable output_table;
    hash_table_init(&output_table, BATCH_TABLE_INITIAL_CAPACITY, BATCH_TABLE_LOAD_FACTOR);

    for (unsigned int i = 0; i < batched_operators->size; i++) {
        DbOperator *dbo = batched_operators->data[i];

        if (!error) {
            if (!check_dependency(dbo, &output_table)) {
                if (!execute_operators(&output_table)) {
                    error = true;
                    message->status = BATCH_EXECUTION_ERROR;
                    db_operator_free(dbo);
                    continue;
                }

                hash_table_clear(&output_table, NULL);
            }

            register_output(dbo, &output_table);
        } else {
            db_operator_free(dbo);
        }
    }

    if (!error) {
        if (!execute_operators(&output_table)) {
            message->status = BATCH_EXECUTION_ERROR;
        }
    }

    hash_table_destroy(&output_table, NULL);

    batched_operators->size = 0;
}

void batch_execute_sequentially(ClientContext *client_context, Message *message) {
    Vector *batched_operators = &client_context->batched_operators;

    if (batched_operators->size == 0) {
        return;
    }

    bool error = false;

    Message temp_message = MESSAGE_INITIALIZER;

    for (unsigned int i = 0; i < batched_operators->size; i++) {
        DbOperator *dbo = batched_operators->data[i];

        temp_message.status = OK;

        if (!error) {
            db_operator_execute(dbo, &temp_message);

            if (temp_message.status != OK) {
                message->status = BATCH_EXECUTION_ERROR;
                error = true;
            }
        }

        db_operator_free(dbo);
    }

    batched_operators->size = 0;
}

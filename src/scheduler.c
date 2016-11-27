#include <pthread.h>
#include <string.h>

#include "client_context.h"
#include "db_operator.h"
#include "dsl.h"
#include "hash_table.h"
#include "message.h"
#include "scheduler.h"
#include "utils.h"
#include "vector.h"

#define OUTPUT_TABLE_INITIAL_CAPACITY 64
#define OUTPUT_TABLE_LOAD_FACTOR 0.75f

void scheduler_handle_operator(DbOperator *dbo, Message *message) {
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
        // TODO: Implement batched joins.
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
        // TODO: Implement batched joins.
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

typedef struct Query {
    DbOperator *dbo;
    Message message;
    pthread_t query_thread;
} Query;

void *query_routine(void *data) {
    Query *query = data;
    db_operator_execute(query->dbo, &query->message);
    db_operator_free(query->dbo);
    return NULL;
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

    Query queries[output_table->num_nodes];

    unsigned int i = 0;
    for (HashTableNode *node = output_table->nodes; node != NULL; node = node->nodes_next) {
        DbOperator *dbo = node->value;

        Query *query = queries + i;
        query->dbo = dbo;
        query->message = MESSAGE_INITIALIZER;

        if (pthread_create(&query->query_thread, NULL, &query_routine, query) != 0) {
            log_err("Unable to create query worker thread.");
            exit(1);
        }

        i++;
    }

    bool success = true;

    for (i = 0; i < output_table->num_nodes; i++) {
        Query *query = queries + i;

        pthread_join(query->query_thread, NULL);

        if (query->message.status != OK) {
            success = false;
        }
    }

    return success;
}

void scheduler_execute_concurrently(ClientContext *client_context, Message *message) {
    Vector *batched_operators = &client_context->batched_operators;

    if (batched_operators->size == 0) {
        return;
    }

    bool error = false;

    HashTable output_table;
    hash_table_init(&output_table, OUTPUT_TABLE_INITIAL_CAPACITY, OUTPUT_TABLE_LOAD_FACTOR);

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

void scheduler_execute_sequentially(ClientContext *client_context, Message *message) {
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

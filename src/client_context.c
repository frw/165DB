#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>

#include "db_operator.h"
#include "client_context.h"
#include "hash_table.h"
#include "vector.h"
#include "utils.h"

#define RESULTS_TABLE_INITIAL_CAPACITY 1024
#define RESULTS_TABLE_LOAD_FACTOR 0.7f

#define BATCHED_OPERATORS_INITIAL_CAPACITY 8

void client_context_init(ClientContext *client_context, int client_socket) {
    client_context->client_socket = client_socket;
    hash_table_init(&client_context->results_table, RESULTS_TABLE_INITIAL_CAPACITY,
            RESULTS_TABLE_LOAD_FACTOR);
    pthread_mutex_init(&client_context->results_mutex, NULL);
    client_context->is_batching = false;
    vector_init(&client_context->batched_operators, BATCHED_OPERATORS_INITIAL_CAPACITY);
}

void client_context_destroy(ClientContext *client_context) {
    hash_table_destroy(&client_context->results_table, (void (*)(void *)) &result_free);
    pthread_mutex_destroy(&client_context->results_mutex);
    vector_destroy(&client_context->batched_operators, (void (*)(void *)) &db_operator_free);
}

void result_put(ClientContext *client_context, char *name, DataType type, Column *source,
        void *values, unsigned int num_tuples) {
    Result *result = malloc(sizeof(Result));
    result->type = type;
    result->source = source;
    switch (type) {
    case POS:
        result->values.pos_values = values;
        break;
    case INT:
        result->values.int_values = values;
        break;
    case LONG:
        result->values.long_values = values;
        break;
    case FLOAT:
        result->values.float_values = values;
        break;
    }
    result->num_tuples = num_tuples;

    pthread_mutex_lock(&client_context->results_mutex);
    Result *removed = hash_table_put(&client_context->results_table, name, result);
    pthread_mutex_unlock(&client_context->results_mutex);

    if (removed != NULL) {
        result_free(removed);
    }
}

Result *result_lookup(ClientContext *client_context, char *name) {
    pthread_mutex_lock(&client_context->results_mutex);
    Result *result = hash_table_get(&client_context->results_table, name);
    pthread_mutex_unlock(&client_context->results_mutex);
    return result;
}

void result_free(Result *result) {
    switch (result->type) {
    case POS:
        free(result->values.pos_values);
        break;
    case INT:
        free(result->values.int_values);
        break;
    case LONG:
        free(result->values.long_values);
        break;
    case FLOAT:
        free(result->values.float_values);
        break;
    }
    free(result);
}

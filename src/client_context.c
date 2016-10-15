#include <stdlib.h>

#include "client_context.h"
#include "hash_table.h"

#define RESULTS_TABLE_INITIAL_CAPACITY 1024
#define RESULTS_TABLE_LOAD_FACTOR 0.7f

void client_context_init(ClientContext *client_context, int client_socket) {
    hash_table_init(&client_context->results_table, RESULTS_TABLE_INITIAL_CAPACITY,
            RESULTS_TABLE_LOAD_FACTOR);
    client_context->client_socket = client_socket;
}

void client_context_destroy(ClientContext *client_context) {
    hash_table_destroy(&client_context->results_table, (void (*)(void *)) &result_free);
}

void result_put(ClientContext *client_context, char *name, DataType type, ResultValues values, unsigned int num_tuples) {
    Result *result = malloc(sizeof(Result));
    result->type = type;
    result->values = values;
    result->num_tuples = num_tuples;

    Result *removed = hash_table_put(&client_context->results_table, name, result);
    if (removed != NULL) {
        result_free(removed);
    }
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

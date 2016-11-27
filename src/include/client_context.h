#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include <pthread.h>
#include <stdbool.h>

#include "common.h"
#include "db_manager.h"
#include "hash_table.h"
#include "vector.h"

typedef union ResultValues {
    unsigned int *pos_values;
    int *int_values;
    long long int *long_values;
    double *float_values;
} ResultValues;

/**
 * Declares the type of a result column, which includes the number of tuples in
 * the result, the data type of the result, and a pointer to the result data.
 */
typedef struct Result {
    DataType type;
    Column *source;
    ResultValues values;
    unsigned int num_tuples;
} Result;

/**
 * Holds the information necessary to refer to generalized columns (results or columns).
 */
typedef struct ClientContext {
    int client_socket;
    HashTable results_table;
    pthread_mutex_t results_mutex;
    bool is_batching;
    Vector batched_operators;
} ClientContext;

void client_context_init(ClientContext *client_context, int client_socket);
void client_context_destroy(ClientContext *client_context);

void result_put(ClientContext *client_context, char *name, DataType type, Column *source,
        void *values, unsigned int num_tuples);

static inline void pos_result_put(ClientContext *client_context, char *name, Column *source,
        unsigned int *pos_values, unsigned int num_tuples) {
    result_put(client_context, name, POS, source, pos_values, num_tuples);
}

static inline void int_result_put(ClientContext *client_context, char *name,
        int *int_values, unsigned int num_tuples) {
    result_put(client_context, name, INT, NULL, int_values, num_tuples);
}

static inline void long_result_put(ClientContext *client_context, char *name,
        long long int *long_values, unsigned int num_tuples) {
    result_put(client_context, name, LONG, NULL, long_values, num_tuples);
}

static inline void float_result_put(ClientContext *client_context, char *name,
        double *float_values, unsigned int num_tuples) {
    result_put(client_context, name, FLOAT, NULL, float_values, num_tuples);
}

Result *result_lookup(ClientContext *client_context, char *name);

void result_free(Result *result);

#endif /* CLIENT_CONTEXT_H */

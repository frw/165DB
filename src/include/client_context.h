#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "common.h"
#include "db_manager.h"
#include "hash_table.h"

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
    Column *source;
    DataType type;
    ResultValues values;
    unsigned int num_tuples;
} Result;

/**
 * Holds the information necessary to refer to generalized columns (results or columns).
 */
typedef struct ClientContext {
    HashTable results_table;
    int client_socket;
} ClientContext;

void client_context_init(ClientContext *client_context, int client_socket);
void client_context_destroy(ClientContext *client_context);

void result_put(ClientContext *client_context, char *name, DataType type, ResultValues values,
        unsigned int num_tuples);

static inline void pos_result_put(ClientContext *client_context, char *name,
        unsigned int *pos_values, unsigned int num_tuples) {
    result_put(client_context, name, POS, (ResultValues) { .pos_values = pos_values }, num_tuples);
}

static inline void int_result_put(ClientContext *client_context, char *name,
        int *int_values, unsigned int num_tuples) {
    result_put(client_context, name, INT, (ResultValues) { .int_values = int_values }, num_tuples);
}

static inline void long_result_put(ClientContext *client_context, char *name,
        long long int *long_values, unsigned int num_tuples) {
    result_put(client_context, name, LONG, (ResultValues) { .long_values = long_values }, num_tuples);
}

static inline void float_result_put(ClientContext *client_context, char *name,
        double *float_values, unsigned int num_tuples) {
    result_put(client_context, name, FLOAT, (ResultValues) { .float_values = float_values }, num_tuples);
}

static inline Result *result_lookup(ClientContext *client_context, char *name) {
    return hash_table_get(&client_context->results_table, name);
}

void result_free(Result *result);

#endif /* CLIENT_CONTEXT_H */

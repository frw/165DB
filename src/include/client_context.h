#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "common.h"
#include "hash_table.h"

typedef union VariableValues {
    unsigned int *pos_values;
    int *int_values;
    long long int *long_values;
    double *float_values;
} VariableValues;

/**
 * Declares the type of a result column, which includes the number of tuples in
 * the result, the data type of the result, and a pointer to the result data.
 */
typedef struct Variable {
    DataType type;
    VariableValues values;
    unsigned int num_tuples;
} Variable;

/**
 * Holds the information necessary to refer to generalized columns (results or columns).
 */
typedef struct ClientContext {
    HashTable variables_table;
    int client_socket;
} ClientContext;

void client_context_init(ClientContext *client_context, int client_socket);
void client_context_destroy(ClientContext *client_context);

void variable_put(ClientContext *client_context, char *name, DataType type, VariableValues values,
        unsigned int num_tuples);

inline Variable *variable_lookup(ClientContext *client_context, char *name) {
    return hash_table_get(&client_context->variables_table, name);
}

void variable_free(Variable *variable);

#endif /* CLIENT_CONTEXT_H */

#include <stdlib.h>

#include "client_context.h"
#include "hash_table.h"

#define VARIABLES_TABLE_INITIAL_CAPACITY 1024
#define VARIABLES_TABLE_LOAD_FACTOR 0.7f

void client_context_init(ClientContext *client_context, int client_socket) {
    hash_table_init(&client_context->variables_table, VARIABLES_TABLE_INITIAL_CAPACITY,
            VARIABLES_TABLE_LOAD_FACTOR);
    client_context->client_socket = client_socket;
}

void client_context_destroy(ClientContext *client_context) {
    hash_table_destroy(&client_context->variables_table, (void (*)(void *)) &variable_free);
}

void variable_put(ClientContext *client_context, char *name, DataType type, VariableValues values, unsigned int num_tuples) {
    Variable *variable = malloc(sizeof(Variable));
    variable->type = type;
    variable->values = values;
    variable->num_tuples = num_tuples;

    Variable *removed = hash_table_put(&client_context->variables_table, name, variable);
    if (removed != NULL) {
        variable_free(removed);
    }
}

void variable_free(Variable *variable) {
    switch (variable->type) {
    case POS:
        free(variable->values.pos_values);
        break;
    case INT:
        free(variable->values.int_values);
        break;
    case LONG:
        free(variable->values.long_values);
        break;
    case FLOAT:
        free(variable->values.float_values);
        break;
    }
    free(variable);
}

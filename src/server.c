/** server.c
 * CS165 Fall 2016
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include "common.h"
#include "client_context.h"
#include "db_manager.h"
#include "db_operator.h"
#include "message.h"
#include "parser.h"
#include "utils.h"

#define NAME_SET_INITIAL_CAPACITY 128
#define NAME_SET_LOAD_FACTOR 0.75f

#define DEFAULT_NUM_COLUMNS 4
#define MINIMUM_NUM_TUPLES 1024
#define APPROXIMATE_CHARS_PER_VALUE 10

static inline bool load_file(DbOperator *dbo, MessageStatus *status) {
    int client_socket = dbo->context->client_socket;

    off_t file_size;
    if (recv(client_socket, &file_size, sizeof(off_t), MSG_WAITALL) == -1) {
        *status = COMMUNICATION_ERROR;
        return false;
    }

    // Duplicate client socket so that we can close it safely later.
    int client_socket_dup = dup(client_socket);
    if (client_socket_dup == -1) {
        *status = COMMUNICATION_ERROR;
        return false;
    }

    FILE *file = fdopen(client_socket_dup, "r");
    if (file == NULL) {
        *status = COMMUNICATION_ERROR;
        return false;
    }

    Vector *col_fqns = malloc(sizeof(Vector));
    vector_init(col_fqns, DEFAULT_NUM_COLUMNS);

    IntVector *col_vals = NULL;

    char dummy = 0;
    HashTable name_set;
    hash_table_init(&name_set, NAME_SET_INITIAL_CAPACITY, NAME_SET_LOAD_FACTOR);

    char *table_name = NULL;

    bool error = false;

    bool header = true;
    char read_buffer[READ_BUFFER_SIZE];
    char *output_str = NULL;

read_loop:
    while(output_str = fgets(read_buffer, READ_BUFFER_SIZE, file), !feof(file)) {
        if (output_str == NULL) {
            *status = COMMUNICATION_ERROR;
            error = true;
            break;
        }

        if (*output_str == '\0') {
            // We've finished reading until the end of the file.
            break;
        }

        if (error) {
            // Error occurred previously. Simply read until the end of the stream.
            continue;
        }

        output_str = strip_whitespace(output_str);

        if (*output_str == '\0') {
            // Ignore empty lines.
            continue;
        }

        char **output_str_index = &output_str;

        if (header) {
            char *token;

            while ((token = strsep(output_str_index, ",")) != NULL) {
                if (!is_valid_fqn(token, 2) || hash_table_get(&name_set, token) != NULL) {
                    *status = INCORRECT_FILE_FORMAT;
                    error = true;
                    goto read_loop;
                }

                int dot_count = 0;
                for (char *c = token; *c != '\0'; c++) {
                    if (*c == '.' && ++dot_count == 2) {
                        unsigned int length = c - token;
                        if (table_name == NULL) {
                            table_name = strndup(token, length);
                        } else if (strncmp(token, table_name, length)) {
                            *status = INCORRECT_FILE_FORMAT;
                            error = true;
                            goto read_loop;
                        }
                        break;
                    }
                }

                hash_table_put(&name_set, token, &dummy);

                vector_append(col_fqns, strdup(token));
            }

            unsigned int num_columns = col_fqns->size;

            unsigned int initial_capacity = file_size / num_columns / APPROXIMATE_CHARS_PER_VALUE;
            if (initial_capacity < MINIMUM_NUM_TUPLES) {
                initial_capacity = MINIMUM_NUM_TUPLES;
            }

            col_vals = malloc(num_columns * sizeof(Vector));
            for (unsigned int i = 0; i < num_columns; i++) {
                int_vector_init(&col_vals[i], initial_capacity);
            }

            header = false;
        } else {
            char *token;
            unsigned int i = 0;

            while ((token = strsep(output_str_index, ",")) != NULL) {
                if (i >= col_fqns->size) {
                    *status = INCORRECT_FILE_FORMAT;
                    error = true;
                    goto read_loop;
                }

                char *endptr;
                int value = strtol(token, &endptr, 10);
                if (endptr == token || *endptr != '\0') {
                    *status = INCORRECT_FILE_FORMAT;
                    error = true;
                    goto read_loop;
                }

                int_vector_append(&col_vals[i], value);

                i++;
            }

            if (i < col_fqns->size) {
                *status = INCORRECT_FILE_FORMAT;
                error = true;
                goto read_loop;
            }
        }
    }

    if (col_fqns->size == 0) {
        *status = INCORRECT_FILE_FORMAT;
        error = true;
    }

    if (error) {
        if (col_vals != NULL) {
            for (unsigned int i = 0; i < col_fqns->size; i++) {
                int_vector_destroy(&col_vals[i]);
            }
            free(col_vals);
        }

        vector_destroy(col_fqns, &free);
        free(col_fqns);
    } else {
        dbo->fields.load.col_fqns = col_fqns;
        dbo->fields.load.col_vals = col_vals;
    }

    hash_table_destroy(&name_set, NULL);

    if (table_name != NULL) {
        free(table_name);
    }

    fclose(file);

    return !error;
}

static inline void handle_db_operator(DbOperator *dbo, Message *message) {
    switch (dbo->type) {
    case LOAD:
        if (!load_file(dbo, &message->status)) {
            db_operator_free(dbo);
            return;
        }
        break;
    default:
        break;
    }

    log_db_operator(dbo);
    execute_db_operator(dbo, message);
    db_operator_free(dbo);
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    log_info("Connected to socket: %d.\n", client_socket);

    int length = 0;

    // Create two messages, one from which to read and one from which to receive.
    Message send_message = { 0 };
    Message recv_message = { 0 };

    // Create ClientContext.
    ClientContext client_context;
    client_context_init(&client_context, client_socket);

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message.length, sizeof(recv_message.length), MSG_WAITALL);
        if (length < 0) {
            log_err("Client connection closed!\n");
            break;
        } else if (length == 0) {
            break;
        }

        char recv_buffer[recv_message.length];
        length = recv(client_socket, recv_buffer, recv_message.length, MSG_WAITALL);
        if (length < 0) {
            log_err("Client connection closed!\n");
            break;
        } else if (length == 0) {
            break;
        }
        recv_buffer[recv_message.length] = '\0';

        send_message.status = OK;
        send_message.length = 0;
        send_message.payload = NULL;

        // 1. Parse command
        DbOperator *query = parse_command(recv_buffer, &send_message, &client_context);

        // 2. Handle request
        if (query != NULL) {
            handle_db_operator(query, &send_message);
        }

        if (is_shutdown_initiated()) {
            send_message.status |= SHUTDOWN_FLAG;
        }

        // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
        if (send(client_socket, &send_message.status, sizeof(send_message.status), 0) == -1) {
            log_err("Failed to send message.");
            break;
        }

        // 4. Send response of request
        if ((send_message.status & ~SHUTDOWN_FLAG) == OK_WAIT_FOR_RESPONSE) {
            if (send(client_socket, &send_message.length, sizeof(send_message.length), 0) == -1) {
                log_err("Failed to send message.");
                break;
            }

            if (send_message.length > 0
                    && send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                break;
            }
            free(send_message.payload);
        }
    } while(!is_shutdown_initiated());

    client_context_destroy(&client_context);
    close(client_socket);
    log_info("Connection closed at socket %d!\n", client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 */
static inline int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strcpy(local.sun_path, SOCK_PATH);
    unlink(local.sun_path);

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *) &local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    db_manager_startup();

    return server_socket;
}

void tear_down_server() {
    db_manager_shutdown();

    unlink(SOCK_PATH);
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void) {
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    atexit(&tear_down_server);

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket;

    do {
        if ((client_socket = accept(server_socket, (struct sockaddr *) &remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        handle_client(client_socket);
    } while(!is_shutdown_initiated());

    return 0;
}


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

#include "batch.h"
#include "common.h"
#include "client_context.h"
#include "db_manager.h"
#include "db_operator.h"
#include "message.h"
#include "parser.h"
#include "utils.h"
#include "vector.h"

#define CLIENTS_VECTOR_INITIAL_CAPACITY 128

#define NAME_SET_INITIAL_CAPACITY 64
#define NAME_SET_LOAD_FACTOR 0.75f

#define DEFAULT_COLUMNS_COUNT 4
#define DEFAULT_ROWS_COUNT_SMALL 1000
#define DEFAULT_ROWS_COUNT_LARGE 100000000

#define LARGE_FILE_THRESHOLD 2147483648L

typedef struct ClientNode {
    int client_socket;
    struct ClientNode *next;
    struct ClientNode *prev;
} ClientNode;

int server_socket;

ClientNode *clients_head;
ClientNode *clients_tail;
pthread_mutex_t clients_mutex;
pthread_cond_t clients_cond;

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 */
static inline bool setup_server() {
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return false;
    }

    local.sun_family = AF_UNIX;
    strcpy(local.sun_path, SOCK_PATH);
    unlink(local.sun_path);

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *) &local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return false;
    }

    if (listen(server_socket, SOMAXCONN) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return false;
    }

    clients_head = NULL;
    clients_tail = NULL;
    pthread_mutex_init(&clients_mutex, NULL);
    pthread_cond_init(&clients_cond, NULL);

    db_manager_startup();

    return true;
}

void tear_down_server() {
    db_manager_shutdown();

    pthread_mutex_destroy(&clients_mutex);
    pthread_cond_destroy(&clients_cond);

    close(server_socket);

    unlink(SOCK_PATH);
}

bool load_file(DbOperator *dbo, MessageStatus *status) {
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

    char read_buffer[READ_BUFFER_SIZE];

    register char *line;
    register bool error = false;

    Vector *col_fqns = malloc(sizeof(Vector));
    vector_init(col_fqns, DEFAULT_COLUMNS_COUNT);

    char dummy = 0;
    HashTable name_set;
    hash_table_init(&name_set, NAME_SET_INITIAL_CAPACITY, NAME_SET_LOAD_FACTOR);

    char *table_name = NULL;


    // Parse header.
    for (;;) {
        line = fgets(read_buffer, READ_BUFFER_SIZE, file);

        if (line == NULL) {
            *status = COMMUNICATION_ERROR;
            log_info("Communication error\n");
            error = true;
            break;
        }

        if (*line == '\0') {
            // We've finished reading until the end of the file.
            break;
        }
        char *line_stripped = strip_whitespace(line);

        if (*line_stripped == '\0') {
            // Ignore empty lines.
            continue;
        }

        char **line_stripped_index = &line_stripped;

        char *token;
        while ((token = strsep(line_stripped_index, ",")) != NULL) {
            if (!is_valid_fqn(token, 2) || hash_table_get(&name_set, token) != NULL) {
                *status = INCORRECT_FILE_FORMAT;
                error = true;
                goto after_header_loop;
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
                        goto after_header_loop;
                    }
                    break;
                }
            }

            hash_table_put(&name_set, token, &dummy);

            vector_append(col_fqns, strdup(token));
        }

        break;
    }

after_header_loop:
    // Free up unnecessary resources.
    hash_table_destroy(&name_set, NULL);

    if (table_name != NULL) {
        free(table_name);
    }

    // Determine number of columns.
    register unsigned int columns_count = 0;

    if (!error) {
        columns_count = col_fqns->size;

        if (columns_count == 0) {
            *status = INCORRECT_FILE_FORMAT;
            error = true;
        }
    }

    // Initialize buffers.
    int *col_vals[columns_count];
    register unsigned int col_vals_capacity = 0;

    if (!error) {
        if (file_size > LARGE_FILE_THRESHOLD) {
            col_vals_capacity = DEFAULT_ROWS_COUNT_LARGE;
        } else {
            col_vals_capacity = DEFAULT_ROWS_COUNT_SMALL;
        }

        for (unsigned int i = 0; i < columns_count; i++) {
            col_vals[i] = malloc(col_vals_capacity * sizeof(int));
        }
    }

    register unsigned int rows_count = 0;

    // Parse body.
body_loop:
    for (;;) {
        line = fgets(read_buffer, READ_BUFFER_SIZE, file);

        if (line == NULL) {
            *status = COMMUNICATION_ERROR;
            log_info("Communication error\n");
            error = true;
            break;
        }

        register char c = *line;

        if (c == '\0') {
            // We've finished reading the file.
            break;
        }

        if (error) {
            // Error occurred previously. Simply read until the end of the stream.
            continue;
        }

        if (c == '\n') {
            // Skip empty lines.
            continue;
        }

        // Check if buffers need to be expanded.
        if (rows_count == col_vals_capacity) {
            col_vals_capacity *= 2;

            for (unsigned int i = 0; i < columns_count; i++) {
                col_vals[i] = realloc(col_vals[i], col_vals_capacity * sizeof(int));
            }
        }

        register unsigned int column = 0;

        for (;;) {
            register bool parsed = false;

            register int acc = 0;
            register bool neg = false;

            // Skip any whitespace at the start.
            while (c == ' ') {
                c = *++line;
            };

            // Check if prefixed with negative sign.
            if (c == '-') {
                neg = true;
                c = *++line;
            }

            for (; c >= '0' && c <= '9'; c = *++line) {
                acc = (acc * 10) + (c - '0');
                parsed = true;
            }

            if (!parsed) {
                *status = INCORRECT_FILE_FORMAT;
                error = true;
                goto body_loop;
            }

            if (neg) {
                acc = -acc;
            }

            if (c == ',') {
                col_vals[column][rows_count] = acc;

                if (++column >= columns_count) {
                    *status = INCORRECT_FILE_FORMAT;
                    error = true;
                    goto body_loop;
                }

                c = *++line;
            } else if (c == '\n') {
                col_vals[column][rows_count] = acc;

                if (column + 1 < columns_count) {
                    *status = INCORRECT_FILE_FORMAT;
                    error = true;
                    goto body_loop;
                }

                rows_count++;
                goto body_loop;
            } else {
                *status = INCORRECT_FILE_FORMAT;
                error = true;
                goto body_loop;
            }
        }
    }

    if (error) {
        for (unsigned int i = 0; i < columns_count; i++) {
            free(col_vals[i]);
        }

        vector_destroy(col_fqns, &free);
        free(col_fqns);

        log_err("Error occurred when receiving load payload.\n");
    } else {
        dbo->fields.load.col_fqns = col_fqns;

        dbo->fields.load.col_vals = malloc(columns_count * sizeof(IntVector));
        for (unsigned int i = 0; i < columns_count; i++) {
            IntVector *v = dbo->fields.load.col_vals + i;
            v->data = col_vals[i];
            v->size = rows_count;
            v->capacity = col_vals_capacity;
        }

        log_info("Received: %u columns, %u rows.\n", columns_count, rows_count);
    }

    fclose(file);

    return !error;
}

static inline void handle_operator(DbOperator *dbo, Message *message) {
    db_operator_log(dbo);

    if (dbo->type == LOAD && !load_file(dbo, &message->status)) {
        db_operator_free(dbo);
        return;
    }

    if (dbo->context->is_batching
            && dbo->type != BATCH_QUERIES
            && dbo->type != BATCH_EXECUTE
            && dbo->type != SHUTDOWN) {
        batch_query(dbo, message);
    } else {
        db_operator_execute(dbo, message);
        db_operator_free(dbo);
    }
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
static inline void handle_client(int client_socket) {
    log_info("Connected to socket: %d.\n", client_socket);

    int length = 0;

    // Create two messages, one from which to read and one from which to receive.
    Message send_message = MESSAGE_INITIALIZER;
    Message recv_message = MESSAGE_INITIALIZER;

    // Create ClientContext.
    ClientContext client_context;
    client_context_init(&client_context, client_socket);

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message.length, sizeof(recv_message.length),
                MSG_WAITALL);
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
            handle_operator(query, &send_message);
        }

        MessageStatus status = send_message.status;

        if (is_shutdown_initiated()) {
            send_message.status |= SHUTDOWN_FLAG;
            // Shutdown server socket to stop accepting new connections
            shutdown(server_socket, SHUT_RD);
        }

        // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
        if (send(client_socket, &send_message.status, sizeof(send_message.status), 0) == -1) {
            log_err("Failed to send message.");
            free(send_message.payload);
            break;
        }

        // 4. Send response of request
        if (status == OK_WAIT_FOR_RESPONSE) {
            if (send(client_socket, &send_message.length, sizeof(send_message.length), 0) == -1) {
                log_err("Failed to send message.");
                free(send_message.payload);
                break;
            }

            if (send_message.length > 0
                    && send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                free(send_message.payload);
                break;
            }

            free(send_message.payload);
        }
    } while (!is_shutdown_initiated());

    client_context_destroy(&client_context);

    close(client_socket);
    log_info("Connection closed at socket %d!\n", client_socket);
}

void *client_thread_routine(void *data) {
    ClientNode *node = data;

    handle_client(node->client_socket);

    pthread_mutex_lock(&clients_mutex);

    if (node->next != NULL) {
        node->next->prev = node->prev;
    } else {
        clients_tail = node->prev;
    }
    if (node->prev != NULL) {
        node->prev->next = node->next;
    } else {
        clients_head = node->next;
    }

    if (clients_head == NULL) {
        pthread_cond_broadcast(&clients_cond);
    }

    pthread_mutex_unlock(&clients_mutex);

    free(node);

    return NULL;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void) {
    if (!setup_server()) {
        exit(1);
    }

    atexit(&tear_down_server);

    log_info("Bound to socket: %d.\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket;

    do {
        if ((client_socket = accept(server_socket, (struct sockaddr *) &remote, &t)) == -1) {
            // An errno of EINVAL means that the socket has been shutdown.
            if (errno != EINVAL) {
                log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            }
            break;
        }

        pthread_t client_thread;

        ClientNode *node = malloc(sizeof(ClientNode));
        node->client_socket = client_socket;

        pthread_mutex_lock(&clients_mutex);

        if (pthread_create(&client_thread, NULL, &client_thread_routine, node) == 0) {
            node->next = NULL;
            node->prev = clients_tail;
            if (clients_head == NULL) {
                clients_head = node;
            } else {
                clients_tail->next = node;
            }
            clients_tail = node;

            pthread_detach(client_thread);
        } else {
            log_err("Unable to create client worker thread.\n");
            free(node);
        }

        pthread_mutex_unlock(&clients_mutex);

    } while (!is_shutdown_initiated());

    pthread_mutex_lock(&clients_mutex);

    // Shutdown all client sockets
    for (ClientNode *node = clients_head; node != NULL; node = node->next) {
        shutdown(node->client_socket, SHUT_RD);
    }

    // Wait until all threads have terminated
    while (clients_head != NULL) {
        pthread_cond_wait(&clients_cond, &clients_mutex);
    }

    pthread_mutex_unlock(&clients_mutex);

    return 0;
}

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

bool recv_table(DbOperator *dbo, MessageStatus *status) {
    int client_socket = dbo->context->client_socket;

    unsigned int columns_count;
    if (recv(client_socket, &columns_count, sizeof(columns_count), MSG_WAITALL) <= 0) {
        *status = COMMUNICATION_ERROR;
        return false;
    }

    unsigned int rows_count;
    if (recv(client_socket, &rows_count, sizeof(rows_count), MSG_WAITALL) <= 0) {
        *status = COMMUNICATION_ERROR;
        return false;
    }

    char **col_fqns = NULL;
    unsigned int malloced_col_fqns = 0;

    int **col_vals = NULL;
    unsigned int malloced_col_vals = 0;

    HashTable name_set;
    hash_table_init(&name_set, NAME_SET_INITIAL_CAPACITY, NAME_SET_LOAD_FACTOR);

    char *table_name = NULL;

    col_fqns = malloc(columns_count * sizeof(char *));
    for (unsigned int i = 0; i < columns_count; i++) {
        unsigned int length;
        if (recv(client_socket, &length, sizeof(length), MSG_WAITALL) <= 0) {
            *status = COMMUNICATION_ERROR;
            goto ERROR;
        }

        char *col_fqn = col_fqns[malloced_col_fqns++] = malloc((length + 1) * sizeof(char));
        if (recv(client_socket, col_fqn, length * sizeof(char), MSG_WAITALL) <= 0) {
            *status = COMMUNICATION_ERROR;
            goto ERROR;
        }
        col_fqn[length] = '\0';
    }

    col_vals = malloc(columns_count * sizeof(int *));
    for (unsigned int i = 0; i < columns_count; i++) {
        int *column = col_vals[malloced_col_vals++] = malloc(rows_count * sizeof(int));
        if (recv(client_socket, column, rows_count * sizeof(int), MSG_WAITALL) <= 0) {
            *status = COMMUNICATION_ERROR;
            goto ERROR;
        }
    }

    for (unsigned int i = 0; i < columns_count; i++) {
        char *col_fqn = col_fqns[i];

        if (!is_valid_fqn(col_fqn, 2) || hash_table_get(&name_set, col_fqn) != NULL) {
            *status = INCORRECT_FILE_FORMAT;
            goto ERROR;
        }

        int dot_count = 0;
        for (char *c = col_fqn; *c != '\0'; c++) {
            if (*c == '.' && ++dot_count == 2) {
                unsigned int length = c - col_fqn;
                if (table_name == NULL) {
                    table_name = strndup(col_fqn, length);
                } else if (strncmp(col_fqn, table_name, length) != 0) {
                    *status = INCORRECT_FILE_FORMAT;
                    goto ERROR;
                }
                break;
            }
        }

        hash_table_put(&name_set, col_fqn, col_fqn);
    }

    hash_table_destroy(&name_set, NULL);

    if (table_name != NULL) {
        free(table_name);
    }

    dbo->fields.load.columns_count = columns_count;
    dbo->fields.load.col_fqns = col_fqns;
    dbo->fields.load.col_vals = malloc(columns_count * sizeof(IntVector));
    for (unsigned int i = 0; i < columns_count; i++) {
        IntVector *v = dbo->fields.load.col_vals + i;
        v->data = col_vals[i];
        v->size = rows_count;
        v->capacity = rows_count;
    }

    free(col_vals);

    return true;

ERROR:
    if (col_fqns != NULL) {
        for (unsigned int i = 0; i < malloced_col_fqns; i++) {
            free(col_fqns[i]);
        }
        free(col_fqns);
    }

    if (col_vals != NULL) {
        for (unsigned int i = 0; i < malloced_col_vals; i++) {
            free(col_vals[i]);
        }
        free(col_vals);
    }

    hash_table_destroy(&name_set, NULL);

    if (table_name != NULL) {
        free(table_name);
    }

    return false;
}

static inline void handle_operator(DbOperator *dbo, Message *message) {
    db_operator_log(dbo);

    if (dbo->type == LOAD && !recv_table(dbo, &message->status)) {
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

    int length;

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

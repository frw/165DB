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
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include "common.h"
#include "client_context.h"
#include "db_manager.h"
#include "db_operator.h"
#include "message.h"
#include "parse.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

static inline void handle_db_operator(DbOperator *dbo, Message *message) {
    if (dbo->type == LOAD) {
        int client_socket = dbo->context->client_socket;

        unsigned int num_columns;
        if (recv(client_socket, &num_columns, sizeof(num_columns), 0) == -1) {
            return;
        }

        unsigned int num_tuples;
        if (recv(client_socket, &num_tuples, sizeof(num_tuples), 0) == -1) {
            return;
        }

        Vector file_contents;
        vector_init(&file_contents, num_columns);

        bool error = false;

        for (unsigned int i = 0; i < num_columns; i++) {
            unsigned int column_fqn_length;
            if (recv(client_socket, &column_fqn_length, sizeof(column_fqn_length), 0) == -1) {
                error = true;
                break;
            }

            char *column_fqn = malloc((column_fqn_length + 1) * sizeof(char));
            if (recv(client_socket, column_fqn, column_fqn_length, 0) == -1) {
                error = true;
                free(column_fqn);
                break;
            }
            column_fqn[column_fqn_length] = '\0';

            FileColumn *column = malloc(sizeof(FileColumn));
            column->column_fqn = column_fqn;
            int_vector_init(&column->values, num_tuples);

            if (recv(client_socket, column->values.data, num_tuples * sizeof(int), 0) == -1) {
                error = true;
                file_column_free(column);
                break;
            }
            column->values.size = num_tuples;

            vector_append(&file_contents, column);
        }

        if (!error) {
            vector_shallow_copy(&dbo->fields.load.file_contents, &file_contents);
        } else {
            vector_destroy(&file_contents, (void (*)(void *)) &file_column_free);
            return;
        }
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
        length = recv(client_socket, &recv_message.length, sizeof(recv_message.length), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            break;
        } else if (length == 0) {
            break;
        }

        char recv_buffer[recv_message.length];
        length = recv(client_socket, recv_buffer, recv_message.length, 0);
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


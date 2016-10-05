/**
 * client.c
 *  CS165 Fall 2016
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include "common.h"
#include "hash_table.h"
#include "message.h"
#include "utils.h"
#include "vector.h"

#define READ_BUFFER_SIZE 1024

#define NAME_SET_INITIAL_CAPACITY 128
#define NAME_SET_LOAD_FACTOR 0.75f

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 */
static inline int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *) &remote, len) == -1) {
        perror("Client connect failed");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

static inline ssize_t recv_and_check(int sockfd, void *buf, size_t len, int flags) {
    ssize_t received = recv(sockfd, buf, len, flags);
    if (received < 0) {
        log_err("Failed to receive message.\n");
        exit(1);
    } else if (received == 0) {
        log_info("Server closed connection.\n");
        exit(1);
    }

    return received;
}

static inline char *parse_load(char *load_arguments) {
    char *load_arguments_stripped = strip_parenthesis(load_arguments);
    if (load_arguments_stripped == load_arguments) {
        return NULL;
    }

    char *load_arguments_stripped2 = strip_quotes(load_arguments_stripped);
    if (load_arguments_stripped2 == load_arguments_stripped) {
        return NULL;
    }

    return load_arguments_stripped2;
}

static inline Vector *load_file(char *file_name, MessageStatus *status) {
    FILE *file = fopen(file_name, "r");
    if (file == NULL) {
        *status = FILE_READ_ERROR;
        return NULL;
    }

    Vector *cols = malloc(sizeof(Vector));
    vector_init(cols, 4);

    char dummy = 0;
    HashTable name_set;
    hash_table_init(&name_set, NAME_SET_INITIAL_CAPACITY, NAME_SET_LOAD_FACTOR);

    char *table_name = NULL;

    bool header = true;
    char read_buffer[READ_BUFFER_SIZE];
    char *output_str = NULL;
    while(output_str = fgets(read_buffer, READ_BUFFER_SIZE, file), !feof(file)) {
        if (output_str == NULL) {
            *status = FILE_READ_ERROR;
            goto ERROR;
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
                    goto ERROR;
                }

                int dot_count = 0;
                for (char *c = token; *c != '\0'; c++) {
                    if (*c == '.' && ++dot_count == 2) {
                        unsigned int length = c - token;
                        if (table_name == NULL) {
                            table_name = strndup(token, length);
                        } else if (strncmp(token, table_name, length)) {
                            *status = INCORRECT_FILE_FORMAT;
                            goto ERROR;
                        }
                        break;
                    }
                }

                hash_table_put(&name_set, token, &dummy);

                FileColumn *col = malloc(sizeof(FileColumn));
                col->column_fqn = strdup(token);
                int_vector_init(&col->values, 1024);

                vector_append(cols, col);
            }

            header = false;
        } else {
            unsigned int i = 0;

            char *token;
            while ((token = strsep(output_str_index, ",")) != NULL) {
                if (i >= cols->size) {
                    *status = INCORRECT_FILE_FORMAT;
                    goto ERROR;
                }

                FileColumn *col = cols->data[i];

                char *endptr;
                int value = strtol(token, &endptr, 10);
                if (endptr == token || *endptr != '\0') {
                    *status = INCORRECT_FILE_FORMAT;
                    goto ERROR;
                }

                int_vector_append(&col->values, value);

                i++;
            }

            if (i < cols->size) {
                *status = INCORRECT_FILE_FORMAT;
                goto ERROR;
            }
        }
    }

    if (cols->size == 0) {
        *status = INCORRECT_FILE_FORMAT;
        goto ERROR;
    }

    goto CLEANUP;

ERROR:
    vector_destroy(cols, (void (*)(void *)) &file_column_free);
    free(cols);
    cols = NULL;

CLEANUP:
    hash_table_destroy(&name_set, NULL);

    if (table_name != NULL) {
        free(table_name);
    }

    fclose(file);

    return cols;
}

static inline bool send_file(int client_socket, Vector *columns) {
    unsigned int num_columns = columns->size;
    if (send(client_socket, &num_columns, sizeof(num_columns), 0) == -1) {
        return false;
    }

    FileColumn **file_columns = (FileColumn **) columns->data;

    unsigned int num_tuples = file_columns[0]->values.size;
    if (send(client_socket, &num_tuples, sizeof(num_tuples), 0) == -1) {
        return false;
    }

    for (unsigned int i = 0; i < num_columns; i++) {
        char *column_fqn = file_columns[i]->column_fqn;
        unsigned int column_fqn_length = strlen(column_fqn);

        if (send(client_socket, &column_fqn_length, sizeof(column_fqn_length), 0) == -1) {
            return false;
        }

        if (send(client_socket, column_fqn, column_fqn_length, 0) == -1) {
            return false;
        }

        int *values = file_columns[i]->values.data;

        if (send(client_socket, values, num_tuples * sizeof(int), 0) == -1) {
            return false;
        }
    }

    return true;
}

static inline void print_payload(char *payload) {
    unsigned int num_columns = *((unsigned int *) payload);
    payload += sizeof(unsigned int);

    unsigned int num_tuples = *((unsigned int *) payload);
    payload += sizeof(unsigned int);

    DataType column_types[num_columns];
    void *column_values[num_columns];

    for (unsigned int i = 0; i < num_columns; i++) {
        DataType type = *((DataType *) payload);
        column_types[i] = type;
        payload += sizeof(DataType);

        column_values[i] = payload;
        switch (type) {
        case POS:
            payload += num_tuples * sizeof(unsigned int);
            break;
        case INT:
            payload += num_tuples * sizeof(int);
            break;
        case LONG:
            payload += num_tuples * sizeof(long long);
            break;
        case FLOAT:
            payload += num_tuples * sizeof(double);
            break;
        default:
            return;
        }
    }

    for (unsigned int i = 0; i < num_tuples; i++) {
        for (unsigned int j = 0; j < num_columns; j++) {
            if (j > 0) {
                fputc(',', stdout);
            }
            switch (column_types[j]) {
            case POS:
                printf("%u", ((unsigned int *) column_values[j])[i]);
                break;
            case INT:
                printf("%d", ((int *) column_values[j])[i]);
                break;
            case LONG:
                printf("%lld", ((long long *) column_values[j])[i]);
                break;
            case FLOAT:
                printf("%.2f", ((double *) column_values[j])[i]);
                break;
            }
        }
        fputc('\n', stdout);
    }
}

static inline void print_error(MessageStatus status, bool interactive, unsigned int line_num) {
    char *error = message_status_to_string(status);
    if (interactive) {
        fprintf(stderr, "Error: %s\n", error);
    } else {
        fprintf(stderr, "Error on line %u: %s\n", line_num, error);
    }
}

int main(void) {
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    Message send_message = { 0 };
    Message recv_message = { 0 };

    bool interactive = isatty(fileno(stdin));

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd.
    char *prefix = interactive ? "db_client > " : "";

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. Output interactive marker.
    // 2. Read from stdin until EOF.
    char read_buffer[READ_BUFFER_SIZE];
    send_message.payload = read_buffer;

    unsigned int line_num = 0;
    while (fputs(prefix, stdout),
            output_str = fgets(read_buffer, READ_BUFFER_SIZE, stdin),
            !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        line_num++;

        if (read_buffer[0] == '\0' || strcmp(read_buffer, "\n") == 0) {
            // Ignore empty lines.
            continue;
        }

        send_message.length = strlen(read_buffer);

        char parse_buffer[send_message.length + 1];
        memcpy(parse_buffer, read_buffer, send_message.length + 1);
        char *parse_buffer_stripped = strip_whitespace(parse_buffer);

        Vector *file_contents = NULL;

        if (strncmp(parse_buffer_stripped, "load", 4) == 0) {
            char *file_name = parse_load(parse_buffer_stripped + 4);
            if (file_name == NULL) {
                print_error(INCORRECT_FORMAT, interactive, line_num);
                continue;
            }

            MessageStatus status = OK;
            file_contents = load_file(file_name, &status);

            if (status != OK) {
                print_error(status, interactive, line_num);
                continue;
            }
        }

        // Send the message_header, which tells server payload size.
        if (send(client_socket, &send_message.length, sizeof(send_message.length), 0) == -1) {
            log_err("Failed to send message header.\n");
            exit(1);
        }

        // Send the payload (query) to server.
        if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
            log_err("Failed to send query payload.\n");
            exit(1);
        }

        if (file_contents != NULL) {
            // A load query occured. Send file contents.
            if (!send_file(client_socket, file_contents)) {
                log_err("Failed to send file.\n");
                exit(1);
            }

            vector_destroy(file_contents, (void (*)(void *)) &file_column_free);
            free(file_contents);
        }

        // Always wait for server response (even if it is just an OK message).
        recv_and_check(client_socket, &recv_message.status, sizeof(recv_message.status), 0);

        bool shutdown = (recv_message.status & SHUTDOWN_FLAG) != 0;
        recv_message.status &= ~SHUTDOWN_FLAG;

        if (recv_message.status == OK_WAIT_FOR_RESPONSE) {
            recv_and_check(client_socket, &recv_message.length, sizeof(recv_message.length), 0);

            if (recv_message.length > 0) {
                // Calculate number of bytes in response package.
                char payload[recv_message.length];

                // Receive the payload and print it out.
                recv_and_check(client_socket, payload, recv_message.length, 0);
                print_payload(payload);
            }
        } else if (recv_message.status != OK) {
            print_error(recv_message.status, interactive, line_num);
        }

        if (shutdown) {
            break;
        }
    }

    close(client_socket);
    return 0;
}

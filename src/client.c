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

#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"
#include "vector.h"

#define DEFAULT_COLUMNS_COUNT 4
#define DEFAULT_ROWS_COUNT_SMALL 1000
#define DEFAULT_ROWS_COUNT_LARGE 100000000

#define LARGE_FILE_THRESHOLD 2147483648L

typedef struct Table {
    unsigned int columns_count;
    unsigned int rows_count;
    char **column_fqns;
    int **column_values;
} Table;

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

static inline bool recv_and_check(int socket, void *buffer, size_t size, int flags) {
    ssize_t received = recv(socket, buffer, size, flags);
    if (received < 0) {
        log_err("Failed to receive message.\n");
        return false;
    } else if (received == 0) {
        log_err("Server closed connection.\n");
        return false;
    }

    return true;
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

MessageStatus load_table(char *file_path, Table *table) {
    int fd = open(file_path, O_RDONLY);
    if (fd == -1) {
        return FILE_READ_ERROR;
    }

    struct stat file_stat;
    if (fstat(fd, &file_stat) == -1) {
        close(fd);
        return FILE_READ_ERROR;
    }

    off_t file_size = file_stat.st_size;

    char *addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        return FILE_READ_ERROR;
    }

    madvise(addr, file_size, MADV_SEQUENTIAL | MADV_WILLNEED);

    MessageStatus status = OK;

    char read_buffer[READ_BUFFER_SIZE];

    Vector col_fqns;
    vector_init(&col_fqns, DEFAULT_COLUMNS_COUNT);

    register int **col_vals = NULL;

    register char *ptr = addr;
    register char *end = addr + file_size;
    register char c;

    // Parse header.
    if (ptr == end) {
        goto POST_HEADER_LOOP;
    }
    c = *ptr++;

    for (;;) {
        register unsigned int len = 0;

        // Ignore any leading whitespace.
        while (c == ' ') {
            if (ptr == end) {
                goto POST_HEADER_LINE_LOOP;
            }
            c = *ptr++;
        }

        for (; c != ',' && c != '\n' && c != ' '; c = *ptr++) {
            read_buffer[len++] = c;

            if (ptr == end) {
                goto POST_HEADER_LINE_LOOP;
            }
        }

        // Ignore any trailing whitespace.
        while (c == ' ') {
            if (ptr == end) {
                goto POST_HEADER_LINE_LOOP;
            }
            c = *ptr++;
        }

POST_HEADER_LINE_LOOP:
        if (len == 0) {
            status = INCORRECT_FILE_FORMAT;
            goto ERROR;
        }

        char *col_fqn = malloc((len + 1) * sizeof(char));
        memcpy(col_fqn, read_buffer, len * sizeof(char));
        col_fqn[len] = '\0';

        vector_append(&col_fqns, col_fqn);

        if (c == ',') {
            if (ptr == end) {
                status = INCORRECT_FILE_FORMAT;
                goto ERROR;
            }

            c = *ptr++;
            continue;
        }

        break;
    }

POST_HEADER_LOOP:
    if (col_fqns.size == 0) {
        status = INCORRECT_FILE_FORMAT;
        goto ERROR;
    }

    register unsigned int columns_count = col_fqns.size;

    // Initialize buffers.
    col_vals = malloc(columns_count * sizeof(unsigned int *));
    register unsigned int col_vals_capacity;

    if (file_size > LARGE_FILE_THRESHOLD) {
        col_vals_capacity = DEFAULT_ROWS_COUNT_LARGE;
    } else {
        col_vals_capacity = DEFAULT_ROWS_COUNT_SMALL;
    }

    for (unsigned int i = 0; i < columns_count; i++) {
        col_vals[i] = malloc(col_vals_capacity * sizeof(int));
    }

    register unsigned int rows_count = 0;

    // Parse body.
    for (;;) {
        if (ptr == end) {
            goto POST_BODY_LOOP;
        }
        c = *ptr++;

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

            // Ignore any leading whitespace.
            while (c == ' ') {
                if (ptr == end) {
                    goto POST_BODY_LINE_LOOP;
                }
                c = *ptr++;
            }

            // Check if prefixed with negative sign.
            if (c == '-') {
                neg = true;

                if (ptr == end) {
                    goto POST_BODY_LINE_LOOP;
                }
                c = *ptr++;
            }

            // Parse digits.
            for (; c >= '0' && c <= '9'; c = *ptr++) {
                acc = (acc * 10) + (c - '0');
                parsed = true;

                if (ptr == end) {
                    goto POST_BODY_LINE_LOOP;
                }
            }

            // Ignore any trailing whitespace.
            while (c == ' ') {
                if (ptr == end) {
                    goto POST_BODY_LINE_LOOP;
                }
                c = *ptr++;
            }

POST_BODY_LINE_LOOP:
            if (!parsed) {
                status = INCORRECT_FILE_FORMAT;
                goto ERROR;
            }

            if (neg) {
                acc = -acc;
            }

            col_vals[column][rows_count] = acc;

            if (c == ',') {
                if (ptr == end || ++column >= columns_count) {
                    status = INCORRECT_FILE_FORMAT;
                    goto ERROR;
                }

                c = *ptr++;
                continue;
            }

            if ((ptr != end && c != '\n') || column + 1 < columns_count) {
                status = INCORRECT_FILE_FORMAT;
                goto ERROR;
            }

            rows_count++;

            break;
        }
    }

POST_BODY_LOOP:
    table->columns_count = columns_count;
    table->rows_count = rows_count;
    table->column_fqns = (char **) col_fqns.data;
    table->column_values = col_vals;

    log_info("Parsed: %u columns, %u rows\n", columns_count, rows_count);

    goto CLEANUP;

ERROR:
    vector_destroy(&col_fqns, &free);

    if (col_vals != NULL) {
        for (unsigned int i = 0; i < columns_count; i++) {
            free(col_vals[i]);
        }
        free(col_vals);
    }

CLEANUP:
    munmap(addr, file_size);

    close(fd);

    return status;
}

static inline bool send_table(int client_socket, Table *table) {
    if (send(client_socket, &table->columns_count, sizeof(table->columns_count), MSG_NOSIGNAL)
            == -1) {
        log_err("Failed to send columns count.\n");
        return false;
    }

    if (send(client_socket, &table->rows_count, sizeof(table->rows_count), MSG_NOSIGNAL) == -1) {
        log_err("Failed to send rows count.\n");
        return false;
    }

    for (unsigned int i = 0; i < table->columns_count; i++) {
        char *column_fqn = table->column_fqns[i];

        unsigned int length = strlen(column_fqn);

        if (send(client_socket, &length, sizeof(length), MSG_NOSIGNAL) == -1) {
            log_err("Failed to send columns FQN length.\n");
            return false;
        }

        if (send(client_socket, column_fqn, length * sizeof(char), MSG_NOSIGNAL) == -1) {
            log_err("Failed to send columns FQN.\n");
            return false;
        }

        free(column_fqn);
    }

    free(table->column_fqns);

    for (unsigned int i = 0; i < table->columns_count; i++) {
        int *column = table->column_values[i];

        if (send(client_socket, column, table->rows_count * sizeof(int), MSG_NOSIGNAL) == -1) {
            log_err("Failed to send columns FQN.\n");
            return false;
        }

        free(column);
    }

    free(table->column_values);

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
        return 1;
    }

    Message send_message = MESSAGE_INITIALIZER;
    Message recv_message = MESSAGE_INITIALIZER;

    Table table;

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

    bool error = false;

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

        bool loaded = false;

        if (strncmp(parse_buffer_stripped, "load", 4) == 0) {
            char *file_path = parse_load(parse_buffer_stripped + 4);
            if (file_path == NULL) {
                print_error(INCORRECT_FORMAT, interactive, line_num);
                continue;
            }

            MessageStatus load_status = load_table(file_path, &table);
            if (load_status != OK) {
                print_error(load_status, interactive, line_num);
                continue;
            }

            loaded = true;
        }

        // Send the message_header, which tells server payload size.
        if (send(client_socket, &send_message.length, sizeof(send_message.length), MSG_NOSIGNAL)
                == -1) {
            log_err("Failed to send message header.\n");
            error = true;
            break;
        }

        // Send the payload (query) to server.
        if (send(client_socket, send_message.payload, send_message.length, MSG_NOSIGNAL) == -1) {
            log_err("Failed to send query payload.\n");
            error = true;
            break;
        }

        // Send file contents if load query occured.
        if (loaded && !send_table(client_socket, &table)) {
            error = true;
            break;
        }

        // Always wait for server response (even if it is just an OK message).
        if (!recv_and_check(client_socket, &recv_message.status, sizeof(recv_message.status),
                MSG_WAITALL)) {
            error = true;
            break;
        }

        bool shutdown = (recv_message.status & SHUTDOWN_FLAG) != 0;
        recv_message.status &= ~SHUTDOWN_FLAG;

        if (recv_message.status == OK_WAIT_FOR_RESPONSE) {
            if (!recv_and_check(client_socket, &recv_message.length, sizeof(recv_message.length),
                    MSG_WAITALL)) {
                error = true;
                break;
            }

            if (recv_message.length > 0) {
                // Allocate buffer to store payload.
                char *payload = malloc(recv_message.length);

                // Receive the payload.
                if (!recv_and_check(client_socket, payload, recv_message.length, MSG_WAITALL)) {
                    error = true;
                    break;
                }

                // Print the payload.
                print_payload(payload);

                free(payload);
            }
        } else if (recv_message.status != OK) {
            print_error(recv_message.status, interactive, line_num);
        }

        if (shutdown) {
            break;
        }
    }

    close(client_socket);

    return error;
}

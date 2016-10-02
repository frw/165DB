#define _BSD_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
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

Vector *load_file(char *file_name, MessageStatus *status) {
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

int main(int argc, char *argv[]) {
    if (argc <= 1) {
        return 1;
    }

    char *file_name = argv[1];

    MessageStatus status = OK;

    Vector *columns = load_file(file_name, &status);

    if (status == OK) {
        for (unsigned int i = 0; i < columns->size; i++) {
            FileColumn *column = columns->data[i];
            printf("%s\n", column->column_fqn);
            for (unsigned int j = 0; j < column->values.size; j++) {
                printf("%d\n", column->values.data[j]);
            }
        }

        vector_destroy(columns, (void (*)(void *)) &file_column_free);
        free(columns);
    } else {
        printf("%s\n", message_status_to_string(status));
    }
}

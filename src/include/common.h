// This file includes shared constants and other values.
#ifndef COMMON_H
#define COMMON_H

#include "vector.h"

#define SOCK_PATH "cs165_unix_socket"

#define READ_BUFFER_SIZE 4096

typedef enum DataType {
    POS, INT, LONG, FLOAT
} DataType;

typedef struct FileColumn {
    char *column_fqn;
    IntVector values;
} FileColumn;

void file_column_free(FileColumn *col);

#endif  /* COMMON_H */

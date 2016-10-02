#include "common.h"
#include "vector.h"

void file_column_free(FileColumn *col) {
    free(col->column_fqn);
    int_vector_destroy(&col->values);

    free(col);
}

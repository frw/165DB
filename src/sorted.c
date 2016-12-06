#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "sorted.h"
#include "utils.h"
#include "vector.h"

void sorted_init(SortedIndex *index, bool sequential, int *values, unsigned int *positions,
        unsigned int size) {
    index->sequential = sequential;

    if (size == 0) {
        int_vector_init(&index->values, 0);
        pos_vector_init(&index->positions, 0);
        return;
    }

    int_vector_init(&index->values, size);
    memcpy(index->values.data, values, size * sizeof(int));
    index->values.size = size;

    if (sequential) {
        pos_vector_init(&index->positions, 0);
    } else {
        pos_vector_init(&index->positions, size);
        memcpy(index->positions.data, positions, size * sizeof(unsigned int));
        index->positions.size = size;
    }
}

void sorted_destroy(SortedIndex *index) {
    int_vector_destroy(&index->values);
    pos_vector_destroy(&index->positions);
}

bool sorted_save(SortedIndex *index, FILE *file) {
    if (fwrite(&index->sequential, sizeof(index->sequential), 1, file) != 1) {
        log_err("Unable to write Sorted sequential\n");
        return false;
    }

    if (!int_vector_save(&index->values, file)) {
        return false;
    }

    if (!pos_vector_save(&index->positions, file)) {
        return false;
    }

    return true;
}

bool sorted_load(SortedIndex *index, FILE *file) {
    int_vector_init(&index->values, 0);
    pos_vector_init(&index->positions, 0);

    if (fread(&index->sequential, sizeof(index->sequential), 1, file) != 1) {
        log_err("Unable to read Sorted sequential\n");
        return false;
    }

    if (!int_vector_load(&index->values, file)) {
        return false;
    }

    if (!pos_vector_load(&index->positions, file)) {
        return false;
    }

    return true;
}

void sorted_insert(SortedIndex *index, int value, unsigned int *position) {
    unsigned int idx = binary_search_right(index->values.data, index->values.size, value);

    int_vector_insert(&index->values, idx, value);

    if (index->sequential) {
        *position = idx;
    } else {
        pos_vector_insert(&index->positions, idx, *position);
    }
}

bool sorted_remove(SortedIndex *index, int value, unsigned int position,
        unsigned int *positions_map, unsigned int *position_ptr) {
    unsigned int idx = binary_search_right(index->values.data, index->values.size, value);
    if (idx == index->values.size) {
        return -1;
    }

    for (; idx < index->values.size && index->values.data[idx] == value; idx++) {
        unsigned int pos;
        if (index->sequential) {
            pos = positions_map[idx];
        } else {
            pos = index->positions.data[idx];
        }

        if (pos == position) {
            int_vector_remove(&index->values, idx);

            if (index->sequential) {
                if (position_ptr != NULL) {
                    *position_ptr = idx;
                }
            } else {
                if (position_ptr != NULL) {
                    *position_ptr = index->positions.data[idx];
                }
                pos_vector_remove(&index->positions, idx);
            }

            return true;
        }
    }

    return false;
}

static inline int sorted_search_left(IntVector *values, int value) {
    unsigned int idx = binary_search_left(values->data, values->size, value);
    if (idx == values->size) {
        return -1;
    }

    return idx;
}

static inline int sorted_search_right(IntVector *values, int value) {
    unsigned int idx = binary_search_left(values->data, values->size, value);
    if (idx == 0) {
        return -1;
    }

    return idx - 1;
}

unsigned int sorted_select_lower(SortedIndex *index, int high, unsigned int *result) {
    int idx = sorted_search_right(&index->values, high);
    if (idx == -1) {
        return 0;
    }

    if (index->sequential) {
        unsigned int position = idx;

        for (unsigned int i = 0; i <= position; i++) {
            result[i] = i;
        }
    } else {
        memcpy(result, index->positions.data, (idx + 1) * sizeof(unsigned int));
    }

    return idx + 1;
}

unsigned int sorted_select_higher(SortedIndex *index, int low, unsigned int *result) {
    int idx = sorted_search_left(&index->values, low);
    if (idx == -1) {
        return 0;
    }

    if (index->sequential) {
        unsigned int position = idx;

        for (unsigned int i = position; i < index->values.size; i++) {
            result[i - position] = i;
        }
    } else {
        memcpy(result, index->positions.data + idx,
                (index->values.size - idx) * sizeof(unsigned int));
    }

    return index->values.size - idx;
}

unsigned int sorted_select_range(SortedIndex *index, int low, int high, unsigned int *result) {
    int left_idx = sorted_search_left(&index->values, low);
    if (left_idx == -1) {
        return 0;
    }

    int right_idx = sorted_search_right(&index->values, high);
    if (right_idx == -1) {
        return 0;
    }

    if (index->sequential) {
        unsigned int left_position = left_idx;
        unsigned int right_position = right_idx;

        for (unsigned int i = left_position; i <= right_position; i++) {
            result[i - left_position] = i;
        }
    } else {
        memcpy(result, index->positions.data + left_idx,
                (right_idx - left_idx + 1) * sizeof(unsigned int));
    }

    return right_idx - left_idx + 1;
}

int sorted_min(SortedIndex *index, unsigned int *position) {
    if (position != NULL) {
        if (index->sequential) {
            *position = 0;
        } else {
            *position = index->positions.data[0];
        }
    }

    return index->values.data[0];
}

int sorted_max(SortedIndex *index, unsigned int *position) {
    unsigned int idx = index->values.size - 1;

    if (position != NULL) {
        if (index->sequential) {
            *position = idx;
        } else {
            *position = index->positions.data[idx];
        }
    }

    return index->values.data[idx];
}

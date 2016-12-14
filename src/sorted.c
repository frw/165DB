#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "sorted.h"
#include "utils.h"
#include "vector.h"

void sorted_init(SortedIndex *index, int *values, unsigned int *positions, unsigned int size) {
    if (size == 0) {
        int_vector_init(&index->values, 0);
        pos_vector_init(&index->positions, 0);
        return;
    }

    int_vector_init(&index->values, size);
    memcpy(index->values.data, values, size * sizeof(int));
    index->values.size = size;

    pos_vector_init(&index->positions, size);
    if (positions == NULL) {
        for (unsigned int i = 0; i < size; i++) {
            index->positions.data[i] = i;
        }
    } else {
        memcpy(index->positions.data, positions, size * sizeof(unsigned int));
    }
    index->positions.size = size;
}

void sorted_destroy(SortedIndex *index) {
    int_vector_destroy(&index->values);
    pos_vector_destroy(&index->positions);
}

bool sorted_save(SortedIndex *index, FILE *file) {
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

    if (!int_vector_load(&index->values, file)) {
        return false;
    }

    if (!pos_vector_load(&index->positions, file)) {
        return false;
    }

    return true;
}

void sorted_insert(SortedIndex *index, int value, unsigned int position) {
    unsigned int idx = binary_search_right(index->values.data, index->values.size, value);

    int_vector_insert(&index->values, idx, value);
    pos_vector_insert(&index->positions, idx, position);
}

bool sorted_remove(SortedIndex *index, int value, unsigned int position,
        unsigned int *positions_map, unsigned int *position_ptr) {
    for (unsigned int idx = binary_search_left(index->values.data, index->values.size, value);
            idx < index->values.size && index->values.data[idx] == value; idx++) {
        unsigned int raw_pos = index->positions.data[idx];
        unsigned int pos = positions_map != NULL ? positions_map[raw_pos] : raw_pos;

        if (pos == position) {
            if (position_ptr != NULL) {
                *position_ptr = raw_pos;
            }

            int_vector_remove(&index->values, idx);
            pos_vector_remove(&index->positions, idx);

            return true;
        }
    }

    return false;
}

bool sorted_search(SortedIndex *index, int value, unsigned int position,
        unsigned int *positions_map, unsigned int *position_ptr) {
    for (unsigned int idx = binary_search_left(index->values.data, index->values.size, value);
            idx < index->values.size && index->values.data[idx] == value; idx++) {
        unsigned int raw_pos = index->positions.data[idx];
        unsigned int pos = positions_map != NULL ? positions_map[raw_pos] : raw_pos;

        if (pos == position) {
            if (position_ptr != NULL) {
                *position_ptr = raw_pos;
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

    memcpy(result, index->positions.data, (idx + 1) * sizeof(unsigned int));

    return idx + 1;
}

unsigned int sorted_select_higher(SortedIndex *index, int low, unsigned int *result) {
    int idx = sorted_search_left(&index->values, low);
    if (idx == -1) {
        return 0;
    }

    memcpy(result, index->positions.data + idx, (index->values.size - idx) * sizeof(unsigned int));

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

    memcpy(result, index->positions.data + left_idx,
            (right_idx - left_idx + 1) * sizeof(unsigned int));

    return right_idx - left_idx + 1;
}

int sorted_min(SortedIndex *index, unsigned int *position_ptr) {
    if (position_ptr != NULL) {
        *position_ptr = index->positions.data[0];
    }

    return index->values.data[0];
}

int sorted_max(SortedIndex *index, unsigned int *position_ptr) {
    unsigned int idx = index->values.size - 1;

    if (position_ptr != NULL) {
        *position_ptr = index->positions.data[idx];
    }

    return index->values.data[idx];
}

#ifndef SORTED_H
#define SORTED_H

#include <stdbool.h>
#include <stdio.h>

#include "vector.h"

typedef struct SortedIndex {
    bool sequential;
    IntVector values;
    PosVector positions;
} SortedIndex;

void sorted_init(SortedIndex *index, bool sequential, int *values, unsigned int *positions,
        unsigned int size);
void sorted_destroy(SortedIndex *index);

bool sorted_save(SortedIndex *index, FILE *file);
bool sorted_load(SortedIndex *index, FILE *file);

void sorted_insert(SortedIndex *index, int value, unsigned int *position);
bool sorted_remove(SortedIndex *index, int value, unsigned int position,
        unsigned int *positions_map, unsigned int *position_ptr);

unsigned int sorted_select_lower(SortedIndex *index, int high, unsigned int *result);
unsigned int sorted_select_higher(SortedIndex *index, int low, unsigned int *result);
unsigned int sorted_select_range(SortedIndex *index, int low, int high, unsigned int *result);

int sorted_min(SortedIndex *index, unsigned int *position);
int sorted_max(SortedIndex *index, unsigned int *position);

#endif /* SORTED_H */

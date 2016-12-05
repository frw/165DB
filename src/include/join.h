#ifndef JOIN_H
#define JOIN_H

#include "vector.h"

void join_hash(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2);

void join_nested_loop(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2);

void join_sort_merge(int *values1, unsigned int *positions1, unsigned int count1, int *values2,
        unsigned int *positions2, unsigned int count2, PosVector *pos_out1, PosVector *pos_out2);

#endif /* JOIN_H */

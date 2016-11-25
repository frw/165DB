#include <assert.h>
#include <time.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define VALUES_COUNT 100000000

void generate_random(int *values, size_t count) {
    srand(42);

    for (size_t i = 0; i < count; i++) {
        values[i] = rand() - RAND_MAX / 2;
    }
}

void generate_ascending(int *values, size_t count) {
    for (size_t i = 0; i < count; i++) {
        values[i] = i;
    }
}

void generate_descending(int *values, size_t count) {
    for (size_t i = 0; i < count; i++) {
        values[i] = count - 1 - i;
    }
}

void generate_indices(unsigned int *indices, size_t count) {
    for (size_t i = 0; i < count; i++) {
        indices[i] = i;
    }
}

static inline bool is_ascending(int *values, size_t count) {
    if (count == 0) {
        return true;
    }

    int prev = values[0];
    for (size_t i = 1; i < count; i++) {
        int val = values[i];

        if (prev > val) {
            return false;
        }

        prev = val;
    }

    return true;
}

static inline bool is_indices_ascending(unsigned int *indices, int *values, size_t count) {
    if (count == 0) {
        return true;
    }

    int prev = values[indices[0]];
    for (size_t i = 1; i < count; i++) {
        int val = values[indices[i]];

        if (prev > val) {
            return false;
        }

        prev = val;
    }

    return true;
}

static inline void _radix_sort_lsb(int *dst, int *begin, int *end, int *begin1, unsigned maxshift) {
    size_t size = end - begin;
    if (is_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(int));
        }
        return;
    }

    int *end1 = begin1 + size;

    for (unsigned shift = 0; shift <= maxshift; shift += 8) {
        size_t count[0x100] = { 0 };
        for (int *p = begin; p != end; p++) {
            count[(*p >> shift) & 0xFF]++;
        }

        int *bucket[0x100], *q = begin1;
        for (int i = 0; i < 0x100; q += count[i++]) {
            bucket[i] = q;
        }

        for (int *p = begin; p != end; p++) {
            *bucket[(*p >> shift) & 0xFF]++ = *p;
        }

        int *tmp;

        tmp = begin;
        begin = begin1;
        begin1 = tmp;

        tmp = end;
        end = end1;
        end1 = tmp;
    }
}

static inline void _radix_sort_msb2(int *dst, int *begin, int *end, int *begin1, unsigned shift) {
    size_t size = end - begin;
    if (is_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(int));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (int *p = begin; p != end; p++) {
        count[(*p >> shift) & 0xFF]++;
    }

    int *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (int *p = begin; p != end; p++) {
        *bucket[(*p >> shift) & 0xFF]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = obucket[i] - begin1;
        _radix_sort_lsb(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

static inline void _radix_sort_msb(int *dst, int *begin, int *end, int *begin1, unsigned shift) {
    size_t size = end - begin;
    if (is_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(int));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (int *p = begin; p != end; p++) {
        count[((*p >> shift) & 0xFF) ^ (1 << 7)]++;
    }

    int *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (int *p = begin; p != end; p++) {
        *bucket[((*p >> shift) & 0xFF) ^ (1 << 7)]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = (obucket[i] - begin1);
        _radix_sort_msb2(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

void radix_sort(int *values, size_t size) {
    int *buf = malloc(size * sizeof(unsigned));
    _radix_sort_msb(values, values, values + size, buf, (sizeof(int) - 1) * 8);
    free(buf);
}

/**
 * cmp for qsort
 */

int cmp(int *a, int *b) {
    return *a - *b;
}

/**
 * radix sort indices
 */
static inline void _radix_sort_indices_lsb(unsigned int *dst, unsigned int *begin, unsigned int *end, unsigned int *begin1, int *values, unsigned maxshift) {
    size_t size = end - begin;
    if (is_indices_ascending(begin, values, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(unsigned int));
        }
        return;
    }

    unsigned int *end1 = begin1 + size;

    for (unsigned shift = 0; shift <= maxshift; shift += 8) {
        size_t count[0x100] = { 0 };
        for (unsigned int *p = begin; p != end; p++) {
            count[(values[*p] >> shift) & 0xFF]++;
        }

        unsigned int *bucket[0x100], *q = begin1;
        for (int i = 0; i < 0x100; q += count[i++]) {
            bucket[i] = q;
        }

        for (unsigned int *p = begin; p != end; p++) {
            *bucket[(values[*p] >> shift) & 0xFF]++ = *p;
        }

        unsigned int *tmp;

        tmp = begin;
        begin = begin1;
        begin1 = tmp;

        tmp = end;
        end = end1;
        end1 = tmp;
    }
}

static inline void _radix_sort_indices_msb2(unsigned int *dst, unsigned int *begin, unsigned int *end, unsigned int *begin1, int *values, unsigned shift) {
    size_t size = end - begin;
    if (is_indices_ascending(begin, values, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(unsigned int));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (unsigned int *p = begin; p != end; p++) {
        count[(values[*p] >> shift) & 0xFF]++;
    }

    unsigned int *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (unsigned int *p = begin; p != end; p++) {
        *bucket[(values[*p] >> shift) & 0xFF]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = (obucket[i] - begin1);
        _radix_sort_indices_lsb(dst + off, obucket[i], bucket[i], begin + off, values, shift - 8);
    }
}

static inline void _radix_sort_indices_msb(unsigned int *dst, unsigned int *begin, unsigned int *end, unsigned int *begin1, int *values, unsigned shift) {
    size_t size = end - begin;
    if (is_indices_ascending(begin, values, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(unsigned int));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (unsigned int *p = begin; p != end; p++) {
        count[((values[*p] >> shift) & 0xFF) ^ (1 << 7)]++;
    }

    unsigned int *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (unsigned int *p = begin; p != end; p++) {
        *bucket[((values[*p] >> shift) & 0xFF) ^ (1 << 7)]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = (obucket[i] - begin1);
        _radix_sort_indices_msb2(dst + off, obucket[i], bucket[i], begin + off, values, shift - 8);
    }
}

void radix_sort_indices(unsigned int *indices, int *values, size_t size) {
    unsigned int *buf = malloc(size * sizeof(unsigned));
    _radix_sort_indices_msb(indices, indices, indices + size, buf, values, (sizeof(int) - 1) * 8);
    free(buf);
}

/**
 * radix sort indices 2
 */

typedef struct Record {
    int value;
    unsigned int position;
} Record;

static inline bool is_records_ascending(Record *records, size_t count) {
    if (count == 0) {
        return true;
    }

    int prev = records[0].value;
    for (size_t i = 1; i < count; i++) {
        int val = records[i].value;

        if (prev > val) {
            return false;
        }

        prev = val;
    }

    return true;
}

static inline void _radix_sort_indices2_lsb(Record *dst, Record *begin, Record *end, Record *begin1, unsigned maxshift) {
    size_t size = end - begin;
    if (is_records_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(Record));
        }
        return;
    }

    Record *end1 = begin1 + size;

    for (unsigned shift = 0; shift <= maxshift; shift += 8) {
        size_t count[0x100] = { 0 };
        for (Record *p = begin; p != end; p++) {
            count[(p->value >> shift) & 0xFF]++;
        }

        Record *bucket[0x100], *q = begin1;
        for (int i = 0; i < 0x100; q += count[i++]) {
            bucket[i] = q;
        }

        for (Record *p = begin; p != end; p++) {
            *bucket[(p->value >> shift) & 0xFF]++ = *p;
        }

        Record *tmp;

        tmp = begin;
        begin = begin1;
        begin1 = tmp;

        tmp = end;
        end = end1;
        end1 = tmp;
    }
}

static inline void _radix_sort_indices2_msb2(Record *dst, Record *begin, Record *end, Record *begin1, unsigned shift) {
    size_t size = end - begin;
    if (is_records_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(Record));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (Record *p = begin; p != end; p++) {
        count[(p->value >> shift) & 0xFF]++;
    }

    Record *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (Record *p = begin; p != end; p++) {
        *bucket[(p->value >> shift) & 0xFF]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = (obucket[i] - begin1);
        _radix_sort_indices2_lsb(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

static inline void _radix_sort_indices2_msb(Record *dst, Record *begin, Record *end, Record *begin1, unsigned shift) {
    size_t size = end - begin;
    if (is_records_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(Record));
        }
        return;
    }

    size_t count[0x100] = { 0 };
    for (Record *p = begin; p != end; p++) {
        count[((p->value >> shift) & 0xFF) ^ (1 << 7)]++;
    }

    Record *bucket[0x100], *obucket[0x100], *q = begin1;
    for (int i = 0; i < 0x100; q += count[i++]) {
        obucket[i] = bucket[i] = q;
    }

    for (Record *p = begin; p != end; p++) {
        *bucket[((p->value >> shift) & 0xFF) ^ (1 << 7)]++ = *p;
    }

    for (int i = 0; i < 0x100; ++i) {
        size_t off = (obucket[i] - begin1);
        _radix_sort_indices2_msb2(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

void radix_sort_indices2(unsigned int *indices, int *values, size_t size) {
    Record *buf = malloc(size * sizeof(Record));
    Record *buf1 = malloc(size * sizeof(Record));

    for (size_t i = 0; i < size; i++) {
        Record *r = &buf[i];
        r->value = values[i];
        r->position = i;
    }

    _radix_sort_indices2_msb(buf, buf, buf + size, buf1, (sizeof(int) - 1) * 8);

    for (size_t i = 0; i < size; i++) {
        Record *r = &buf[i];
        values[i] = r->value;
        indices[i] = r->position;
    }

    free(buf);
    free(buf1);
}

int main() {
    int *values = malloc(VALUES_COUNT * sizeof(int));
    unsigned int *indices = malloc(VALUES_COUNT * sizeof(int));

    clock_t start, end;

    generate_random(values, VALUES_COUNT);
    start = clock();
    radix_sort(values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Random: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_ascending(values, VALUES_COUNT);
    start = clock();
    radix_sort(values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Ascending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_descending(values, VALUES_COUNT);
    start = clock();
    radix_sort(values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Descending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_random(values, VALUES_COUNT);
    start = clock();
    qsort(values, VALUES_COUNT, sizeof(int), (int (*)(const void *, const void *)) &cmp);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Quick Sort Random: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_ascending(values, VALUES_COUNT);
    start = clock();
    qsort(values, VALUES_COUNT, sizeof(int), (int (*)(const void *, const void *)) &cmp);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Quick Sort Ascending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_descending(values, VALUES_COUNT);
    start = clock();
    qsort(values, VALUES_COUNT, sizeof(int), (int (*)(const void *, const void *)) &cmp);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Quick Sort Descending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_random(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_indices_ascending(indices, values, VALUES_COUNT));
    printf("Radix Sort Indices Random: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_ascending(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_indices_ascending(indices, values, VALUES_COUNT));
    printf("Radix Sort Indices Ascending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_descending(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_indices_ascending(indices, values, VALUES_COUNT));
    printf("Radix Sort Indices Descending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_random(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices2(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Indices 2 Random: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_ascending(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices2(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Indices 2 Ascending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);

    generate_descending(values, VALUES_COUNT);
    generate_indices(indices, VALUES_COUNT);
    start = clock();
    radix_sort_indices2(indices, values, VALUES_COUNT);
    end = clock();
    assert(is_ascending(values, VALUES_COUNT));
    printf("Radix Sort Indices 2 Descending: %f\n", (double) (end - start) / CLOCKS_PER_SEC);
}


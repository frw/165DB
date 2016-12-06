#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

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

static inline void _radix_sort_indices_lsb(Record *dst, Record *begin, Record *end, Record *begin1,
        unsigned int maxshift) {
    size_t size = end - begin;
    if (is_records_ascending(begin, size)) {
        if (dst != begin) {
            memcpy(dst, begin, size * sizeof(Record));
        }
        return;
    }

    Record *end1 = begin1 + size;

    for (unsigned int shift = 0; shift <= maxshift; shift += 8) {
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

static inline void _radix_sort_indices_msb2(Record *dst, Record *begin, Record *end, Record *begin1,
        unsigned int shift) {
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
        _radix_sort_indices_lsb(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

static inline void _radix_sort_indices_msb(Record *dst, Record *begin, Record *end, Record *begin1,
        unsigned int shift) {
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
        _radix_sort_indices_msb2(dst + off, obucket[i], bucket[i], begin + off, shift - 8);
    }
}

void radix_sort_indices(int *values_in, unsigned int *indices_in, int *values_out,
        unsigned int *indices_out, size_t size) {
    Record *buf = malloc(size * sizeof(Record));
    Record *buf1 = malloc(size * sizeof(Record));

    if (indices_in == NULL) {
        for (size_t i = 0; i < size; i++) {
            Record *r = &buf[i];
            r->value = values_in[i];
            r->position = i;
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            Record *r = &buf[i];
            r->value = values_in[i];
            r->position = indices_in[i];
        }
    }

    _radix_sort_indices_msb(buf, buf, buf + size, buf1, (sizeof(int) - 1) * 8);

    for (size_t i = 0; i < size; i++) {
        Record *r = &buf[i];
        values_out[i] = r->value;
        indices_out[i] = r->position;
    }

    free(buf);
    free(buf1);
}

unsigned int binary_search_left(register int *values, unsigned int size, register int value) {
    register int left = 0;
    register int right = size - 1;
    while (left <= right) {
        register int mid = (left + right) / 2;
        bool smaller = values[mid] < value;
        left = smaller ? mid + 1 : left;
        right = smaller ? right : mid - 1;
    }
    return left;
}

unsigned int binary_search_right(register int *values, unsigned int size, register int value) {
    register int left = 0;
    register int right = size - 1;
    while (left <= right) {
        register int mid = (left + right) / 2;
        bool larger = values[mid] > value;
        right = larger ? mid - 1 : right;
        left = larger ? left : mid + 1;
    }
    return left;
}

// Taken from https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
unsigned int round_up_power_of_two(unsigned int v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

char *strjoin(char *s1, char *s2, char sep) {
    size_t len1 = strlen(s1);
    size_t len2 = strlen(s2);

    char *result = malloc(len1 + len2 + 2);
    if (result == NULL) {
        return NULL;
    }

    memcpy(result, s1, len1);
    result[len1] = sep;
    memcpy(result + len1 + 1, s2, len2 + 1);

    return result;
}

int strtoi(register char *str, char **endptr) {
    register int acc = 0;
    register bool neg = false;

    register char c = *str;

    if (c == '-') {
        neg = true;
        c = *++str;
    }

    for (; c >= '0' && c <= '9'; c = *++str) {
        acc = (acc * 10) + (c - '0');
    }

    if (neg) {
        acc = -acc;
    }

    if (endptr != NULL) {
        *endptr = str;
    }

    return acc;
}

unsigned int strtoui(register char *str, char **endptr) {
    register unsigned int acc = 0;

    register char c = *str;

    for (; c >= '0' && c <= '9'; c = *++str) {
        acc = (acc * 10) + (c - '0');
    }

    if (endptr != NULL) {
        *endptr = str;
    }

    return acc;
}

char *strip_newline(register char *str) {
    register size_t i = 0;
    register char c;
    register char *itr;

    for (itr = str; (c = *itr); itr++) {
        if (c != '\r' && c != '\n') {
            str[i++] = c;
        }
    }

    // Write new null terminator
    str[i] = '\0';
    return str;
}

char *strip_whitespace(register char *str) {
    register size_t i = 0;
    register char c;
    register char *itr;

    for (itr = str; (c = *itr); itr++) {
        if (!isspace(c)) {
            str[i++] = c;
        }
    }

    // Write new null terminator
    str[i] = '\0';
    return str;
}

char *strip_parenthesis(char *str) {
    if (*str != '(') {
        return str;
    }

    size_t last = strlen(str) - 1;
    if (last < 1 || str[last] != ')') {
        return str;
    }

    str[last] = '\0';
    return ++str;
}

char *strip_quotes(char *str) {
    if (*str != '"') {
        return str;
    }

    size_t last = strlen(str) - 1;
    if (last < 1 || str[last] != '"') {
        return str;
    }

    str[last] = '\0';
    return ++str;
}

static inline bool is_valid_name_char(char c) {
    return c == '_' || c == '-' || isalnum(c);
}

bool is_valid_name(char *str) {
    if (*str == '\0') {
        return false;
    }
    for (char c; (c = *str); str++) {
        if (!is_valid_name_char(c)) {
            return false;
        }
    }
    return true;
}

bool is_valid_fqn(char *str, unsigned int depth) {
    switch (*str) {
    case '\0':
    case '.':
        return false;
    }
    unsigned int dot_count = 0;
    char last = '\0';
    for (char c; (c = *str); str++) {
        if (c == '.') {
            if (last == '.') {
                return false;
            }
            dot_count++;
        } else if (!is_valid_name_char(c)) {
            return false;
        }
        last = c;
    }
    if (last == '.' || dot_count != depth) {
        return false;
    }
    return true;
}

// Implementation of Murmur hash for 32-bit and 64-bit size_t.
#if __SIZEOF_SIZE_T__ == 8

size_t hash_bytes(const void *ptr, size_t len) {
    static const size_t m = (((size_t) 0xc6a4a793UL) << 32UL) + (size_t) 0x5bd1e995UL;
    const int r = 47;
    const size_t seed = 0xc70f6907UL;

    size_t h = seed ^ (len * m);

    const size_t *data = (const size_t *) ptr;
    const size_t *end = data + (len / 8);

    while (data != end) {
        size_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char*) data;

    switch (len & 7) {
    case 7:
        h ^= ((size_t) data2[6]) << 48;
        /* no break */
    case 6:
        h ^= ((size_t) data2[5]) << 40;
        /* no break */
    case 5:
        h ^= ((size_t) data2[4]) << 32;
        /* no break */
    case 4:
        h ^= ((size_t) data2[3]) << 24;
        /* no break */
    case 3:
        h ^= ((size_t) data2[2]) << 16;
        /* no break */
    case 2:
        h ^= ((size_t) data2[1]) << 8;
        /* no break */
    case 1:
        h ^= ((size_t) data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

#else

size_t hash_bytes(const void *ptr, size_t len) {
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const size_t m = 0x5bd1e995;
    const int r = 24;
    const size_t seed = 0xc70f6907UL;

    // Initialize the hash to a 'random' value

    size_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const unsigned char *data = (const unsigned char *) ptr;

    while (len >= 4) {
        size_t k = *(size_t *) data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch (len) {
    case 3:
        h ^= data[2] << 16;
        /* no break */
    case 2:
        h ^= data[1] << 8;
        /* no break */
    case 1:
        h ^= data[0];
        h *= m;
    };

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

#endif /* __SIZEOF_SIZE_T__ */

size_t hash_string(char *key) {
    return hash_bytes(key, strlen(key));
}

void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}


/**
 * utils.h
 * CS165 Fall 2016
 *
 * Provides utility and helper functions that may be useful throughout.
 * Includes debugging tools.
 */

#ifndef UTILS_H
#define UTILS_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

typedef struct Record {
    int value;
    unsigned int position;
} Record;

/**
 * Sorts an array of integers in ascending order, and stores the indices corresponding to the original array.
 */
void radix_sort_indices(int *values_in, unsigned int *indices_in, int *values_out,
        unsigned int *indices_out, size_t size);

/**
 * Binary searches through a sorted array of values, returning the position of the left-most
 * element that is >= value.
 */
unsigned int binary_search_left(register int *values, unsigned int size, register int value);

/**
 * Binary searches through a sorted array of values, returning the position of the left-most
 * element that is > value.
 */
unsigned int binary_search_right(register int *values, unsigned int size, register int value);

/**
 * Finds the next highest power of two of a value.
 */
unsigned int round_up_power_of_two(unsigned int v);

/**
 * Joins 2 strings with a separator in between.
 */
char *strjoin(char *s1, char *s2, char sep);

/**
 * Fast, naive string to int conversion.
 */
int strtoi(register char *str, char **endptr);

/**
 * Fast, naive string to unsigned int conversion.
 */
unsigned int strtoui(register char *str, char **endptr);

/**
 * Strips newline characters from a string (in place).
 */
char *strip_newline(char *str);

/**
 * Strips parenthesis characters from a string (in place).
 */
char *strip_parenthesis(char *str);

/**
 * Strips whitespace characters from a string (in place).
 */
char *strip_whitespace(char *str);

/**
 * Strips quotations characters from a string (in place).
 */
char *strip_quotes(char *str);

/**
 * Checks if a string is a valid database/table/column/variable name.
 */
bool is_valid_name(char *str);

/**
 * Checks if a string is a valid table/column id of the form a.b.c.
 */
bool is_valid_fqn(char *str, unsigned int depth);

/**
 * Hashes any data of arbitrary length.
 */
size_t hash_bytes(const void *ptr, size_t len);

/**
 * Hashes a string of arbitrary length.
 */
size_t hash_string(char *key);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

#endif /* UTILS_H */

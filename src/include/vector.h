#ifndef VECTOR_H
#define VECTOR_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

// Vector containing void pointers.
#define STRUCT_NAME Vector
#define FUNCTION_NAME(x) vector_##x
#define TYPE void *
#define POINTER_TYPE

#include "vector_tmpl.h"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE
#undef POINTER_TYPE

// Vector containing ints.
#define STRUCT_NAME IntVector
#define FUNCTION_NAME(x) int_vector_##x
#define TYPE int

#include "vector_tmpl.h"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE

// Vector containing unsigned int positions.
#define STRUCT_NAME PosVector
#define FUNCTION_NAME(x) pos_vector_##x
#define TYPE unsigned int

#include "vector_tmpl.h"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE

// Vector containing bools.
#define STRUCT_NAME BoolVector
#define FUNCTION_NAME(x) bool_vector_##x
#define TYPE bool

#include "vector_tmpl.h"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE

#endif /* VECTOR_H */

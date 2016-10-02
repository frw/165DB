#include <string.h>

#include "vector.h"

// Vector containing void pointers.
#define STRUCT_NAME Vector
#define FUNCTION_NAME(x) vector_##x
#define TYPE void *
#define POINTER_TYPE

#include "vector_tmpl.c"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE
#undef POINTER_TYPE

// Vector containing ints.
#define STRUCT_NAME IntVector
#define FUNCTION_NAME(x) int_vector_##x
#define TYPE int

#include "vector_tmpl.c"

#undef STRUCT_NAME
#undef FUNCTION_NAME
#undef TYPE

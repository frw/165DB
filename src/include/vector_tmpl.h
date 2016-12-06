#if defined STRUCT_NAME && defined FUNCTION_NAME && defined TYPE

typedef struct STRUCT_NAME {
    TYPE *data;
    unsigned int size;
    unsigned int capacity;
} STRUCT_NAME;

void FUNCTION_NAME(init)(STRUCT_NAME *v, unsigned int initial_capacity);

void FUNCTION_NAME(ensure_capacity)(STRUCT_NAME *v, unsigned int minimum_capacity);

void FUNCTION_NAME(append)(STRUCT_NAME *v, TYPE element);

void FUNCTION_NAME(insert)(STRUCT_NAME *v, unsigned int idx, TYPE element);

void FUNCTION_NAME(concat)(STRUCT_NAME *dst, STRUCT_NAME *src);

void FUNCTION_NAME(remove)(STRUCT_NAME *v, unsigned int idx);

void FUNCTION_NAME(shallow_copy)(STRUCT_NAME *dst, STRUCT_NAME *src);

void FUNCTION_NAME(deep_copy)(STRUCT_NAME *dst, STRUCT_NAME *src);

#ifdef POINTER_TYPE

void FUNCTION_NAME(destroy)(STRUCT_NAME *v, void (*data_free)(TYPE));

bool FUNCTION_NAME(save)(STRUCT_NAME *v, bool (*data_save)(TYPE, FILE *), FILE *file);

bool FUNCTION_NAME(load)(STRUCT_NAME *v, TYPE (*data_load)(FILE *), FILE *file);

#else

void FUNCTION_NAME(destroy)(STRUCT_NAME *v);

bool FUNCTION_NAME(save)(STRUCT_NAME *v, FILE *file);

bool FUNCTION_NAME(load)(STRUCT_NAME *v, FILE *file);

#endif /* POINTER_TYPE */

#endif /* defined STRUCT_NAME && defined FUNCTION_NAME && defined TYPE */

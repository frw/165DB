#if defined STRUCT_NAME && defined FUNCTION_NAME && defined TYPE

void FUNCTION_NAME(init)(STRUCT_NAME *v, unsigned int initial_capacity) {
    if (initial_capacity == 0) {
        v->data = NULL;
    } else {
        v->data = malloc(initial_capacity * sizeof(TYPE));
    }
    v->size = 0;
    v->capacity = initial_capacity;
}

void FUNCTION_NAME(ensure_capacity)(STRUCT_NAME *v, unsigned int minimum_capacity) {
    minimum_capacity = v->size > minimum_capacity ? v->size : minimum_capacity;
    if (v->capacity < minimum_capacity) {
        if (v->data == NULL) {
            v->data = malloc(minimum_capacity * sizeof(TYPE));
        } else {
            v->data = realloc(v->data, minimum_capacity * sizeof(TYPE));
        }
        v->capacity = minimum_capacity;
    }
}

void FUNCTION_NAME(append)(STRUCT_NAME *v, TYPE element) {
    if (v->size == v->capacity) {
        if (v->capacity == 0) {
            v->capacity = 1;
            v->data = malloc(1 * sizeof(TYPE));
        } else {
            v->capacity *= 2;
            v->data = realloc(v->data, v->capacity * sizeof(TYPE));
        }
    }
    v->data[v->size++] = element;
}

void FUNCTION_NAME(concat)(STRUCT_NAME *dst, STRUCT_NAME *src) {
    unsigned int new_size = dst->size + src->size;
    FUNCTION_NAME(ensure_capacity)(dst, new_size);
    memcpy(dst->data + dst->size, src->data, src->size * sizeof(TYPE));
    dst->size = new_size;
}

void FUNCTION_NAME(shallow_copy)(STRUCT_NAME *dst, STRUCT_NAME *src) {
    dst->data = src->data;
    dst->size = src->size;
    dst->capacity = src->capacity;
}

#ifdef POINTER_TYPE

void FUNCTION_NAME(destroy)(STRUCT_NAME *v, void (*data_free)(TYPE)) {
    if (v->data != NULL) {
        if (data_free != NULL) {
            for (size_t i = 0; i < v->size; i++) {
                data_free(v->data[i]);
            }
        }
        free(v->data);
    }
}

bool FUNCTION_NAME(save)(STRUCT_NAME *v, bool (*data_save)(TYPE, FILE *), FILE *file) {
    if (fwrite(&v->size, sizeof(v->size), 1, file) != 1) {
        return false;
    }

    for (unsigned int i = 0; i < v->size; i++) {
        if (!data_save(v->data[i], file)) {
            return false;
        }
    }

    return true;
}

bool FUNCTION_NAME(load)(STRUCT_NAME *v, TYPE (*data_load)(FILE *), FILE *file) {
    unsigned int size;

    if (fread(&size, sizeof(size), 1, file) != 1) {
        return false;
    }

    FUNCTION_NAME(ensure_capacity)(v, size);

    for (unsigned int i = 0; i < size; i++) {
        TYPE element = data_load(file);
        if (element == NULL) {
            return false;
        }

        v->data[i] = element;
    }

    v->size = size;

    return true;
}

#else

void FUNCTION_NAME(destroy)(STRUCT_NAME *v) {
    free(v->data);
}

bool FUNCTION_NAME(save)(STRUCT_NAME *v, FILE *file) {
    if (fwrite(&v->size, sizeof(v->size), 1, file) != 1) {
        return false;
    }

    if (fwrite(v->data, sizeof(TYPE), v->size, file) != v->size) {
        return false;
    }

    return true;
}

bool FUNCTION_NAME(load)(STRUCT_NAME *v, FILE *file) {
    unsigned int size;

    if (fread(&size, sizeof(size), 1, file) != 1) {
        return false;
    }

    FUNCTION_NAME(ensure_capacity)(v, size);

    if (fread(v->data, sizeof(TYPE), size, file) != size) {
        return false;
    }

    v->size = size;

    return true;
}

#endif /* POINTER_TYPE */

#endif /* defined STRUCT_NAME && defined FUNCTION_NAME && defined TYPE */

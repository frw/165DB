#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "queue.h"

void queue_init(Queue *q) {
    q->head = NULL;
    q->tail = NULL;
    q->size = 0;
}

void queue_destroy(Queue *q) {
    for (QueueNode *n = q->head; n != NULL;) {
        QueueNode *node = n;
        n = node->next;
        free(node);
    }
}

bool queue_save(Queue *q, FILE *file) {
    if (fwrite(&q->size, sizeof(q->size), 1, file) != 1) {
        return false;
    }

    for (QueueNode *n = q->head; n != NULL; n = n->next) {
        if (fwrite(&n->value, sizeof(n->value), 1, file) != 1) {
            return false;
        }
    }

    return true;
}

bool queue_load(Queue *q, FILE *file) {
    if (fread(&q->size, sizeof(q->size), 1, file) != 1) {
        return false;
    }

    for (unsigned i = 0; i < q->size; i++) {
        QueueNode *node = malloc(sizeof(QueueNode));

        if (fread(&node->value, sizeof(node->value), 1, file) != 1) {
            free(node);
            return false;
        }

        node->next = NULL;

        if (q->head == NULL) {
            q->head = node;
        } else {
            q->tail->next = node;
        }
        q->tail = node;
    }

    return true;
}

void queue_push(Queue *q, unsigned int value) {
    QueueNode *node = malloc(sizeof(QueueNode));
    node->value = value;
    node->next = NULL;

    if (q->head == NULL) {
        q->head = node;
    } else {
        q->tail->next = node;
    }
    q->tail = node;

    q->size++;
}

unsigned int queue_peek(Queue *q) {
    return q->head->value;
}

unsigned int queue_pop(Queue *q) {
    QueueNode *node = q->head;

    unsigned int value = node->value;

    if (node->next != NULL) {
        q->head = node->next;
    } else {
        q->head = q->tail = NULL;
    }

    q->size--;

    free(node);

    return value;
}

#ifndef QUEUE_H
#define QUEUE_H

#include <stdbool.h>
#include <stdio.h>

typedef struct QueueNode {
    unsigned int value;
    struct QueueNode *next;
} QueueNode;

typedef struct Queue {
    QueueNode *head;
    QueueNode *tail;
    unsigned int size;
} Queue;

void queue_init(Queue *q);
void queue_destroy(Queue *q);

bool queue_save(Queue *q, FILE *file);
bool queue_load(Queue *q, FILE *file);

void queue_push(Queue *q, unsigned int value);
unsigned int queue_peek(Queue *q);
unsigned int queue_pop(Queue *q);

#endif /* QUEUE_H */

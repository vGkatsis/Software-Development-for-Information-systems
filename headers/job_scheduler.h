#ifndef job_scheduler_H_
#define job_scheduler_H_

#include "header.h"

typedef struct job{
    void   (*function)(void* argument);
    void*  argument;
	size_t size;
	int* barrier;
    struct job *next;
}Job;

typedef struct queue{	
    struct job *head;
    struct job *tail;
    int length;
}Queue;

typedef struct job_scheduler{
    int number_of_threads;
    int jobs;
    bool stop;
    pthread_t *thread_pool;
    pthread_mutex_t mutex;
    pthread_cond_t empty;
    pthread_cond_t not_empty;
    Queue *queue;
}Job_scheduler;

// queue functions
struct job *initialize_job(void (*function)(void*), void *, int*);
struct queue *initialize_queue();
void push_queue(struct queue **, void (*function)(void*), void *, size_t, int*);
struct job *pop_queue(struct queue **);
void print_queue(struct queue *);
void free_queue(struct queue **);

// thread functions
struct job_scheduler *initialize_job_scheduler(int );
void create_threads_job_scheduler();
void barrier_job_scheduler();
void dynamic_barrier_job_scheduler(int*);
void free_job_scheduler();
void stop_job_scheduler();
void schedule_job_scheduler(void (*function)(void*), void *,size_t, int*);
void *thread_function(void *);

#endif

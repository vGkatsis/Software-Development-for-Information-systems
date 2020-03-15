#include "../headers/job_scheduler.h"

struct job *initialize_job(void (*function)(void*), void *argument, int *barrier) {
    struct job *new_job = (struct job *)malloc(sizeof(struct job));
    if( new_job == NULL) {
        perror("initialize_job failed");
        return NULL;
    }

    new_job->function = function;
    new_job->argument = argument;
	new_job->barrier=barrier;
    new_job->next = NULL;
    
    return new_job;
}

struct queue *initialize_queue() {
    struct queue *new_queue = (struct queue *)malloc(sizeof(struct queue));
    if( new_queue == NULL) {
        perror("initialize_queue failed");
        return NULL;
    }

    new_queue->head = NULL;
    new_queue->tail = NULL;
    new_queue->length = 0;
    
    return new_queue;
}

void push_queue(struct queue **queue, void (*function)(void*), void *argument, size_t size, int *barrier) {
    //struct job *new_job = initialize_job(function,argument,barrier);
	Job *new_job=malloc(sizeof(struct job));
	*new_job=(struct job){function,argument,size,barrier,NULL};

    // if the queue is empty, fix head and tail
    if( (*queue)->length == 0 ) {
        (*queue)->head = new_job;
        (*queue)->tail = new_job;
    }
    // otherwise fix only tail
    else {
        (*queue)->tail->next = new_job;
        (*queue)->tail = new_job;
    }
    (*queue)->length++;
}

struct job *pop_queue(struct queue **queue){
    struct job *temp_job = (*queue)->head;
    // if the queue is empty, then there is not data to be removed
    if( (*queue)->length == 0 ) {
        printf("There is not data to be removed.\n");
        return NULL;
    }
    // if the queue has only 1 data, fix head and tail
    else if( (*queue)->length == 1 ) {
        (*queue)->head = NULL;
        (*queue)->tail = NULL;
    }
    // otherwise move only head
    else
        (*queue)->head = (*queue)->head->next;
    (*queue)->length--;
    return temp_job;
}

void print_queue(struct queue *queue) {
    printf("Queue: ");
    struct job *temp_job = queue->head;
    for( int i = 0 ; i < queue->length ; i++ ) {
    //    printf("%d ",temp_job->type);
        temp_job = temp_job->next;
    }
    printf("\n");
}

void free_queue(struct queue **queue) {
    struct job *temp_job = NULL;
    while((*queue)->head) {
        temp_job = (*queue)->head->next;
        free((*queue)->head);
        (*queue)->head = temp_job;
    }
    free(*queue);
}

struct job_scheduler *initialize_job_scheduler(int number_of_threads) {
    if( number_of_threads < 1) {
        perror("initialize_job_scheduler failed: number of threads");
        return NULL;
    }

    struct job_scheduler *new_job_scheduler = (struct job_scheduler *)malloc(sizeof(struct job_scheduler));
    new_job_scheduler->thread_pool = malloc(sizeof(pthread_t)*number_of_threads);
    if( new_job_scheduler->thread_pool == NULL) {
        perror("initialize_job_scheduler failed");
        return NULL;
    }

    new_job_scheduler->number_of_threads = number_of_threads;
    new_job_scheduler->jobs = 0;
    new_job_scheduler->stop = false;
    pthread_mutex_init(&new_job_scheduler->mutex, NULL);
    pthread_cond_init(&new_job_scheduler->empty, NULL);
    pthread_cond_init(&new_job_scheduler->not_empty, NULL);
    new_job_scheduler->queue = initialize_queue();

    return new_job_scheduler;
}

void create_threads_job_scheduler() {
    extern struct job_scheduler *job_scheduler;

    for( int i = 0 ; i < job_scheduler->number_of_threads ; i++ )
        pthread_create(&(job_scheduler->thread_pool[i]),0,thread_function,0);
}

void barrier_job_scheduler() {
    extern struct job_scheduler *job_scheduler;
	
    pthread_mutex_lock(&job_scheduler->mutex);
    while( job_scheduler->jobs > 0 )
        pthread_cond_wait(&job_scheduler->empty,&job_scheduler->mutex);
    pthread_mutex_unlock(&job_scheduler->mutex);
}
void free_job_scheduler() {
    extern struct job_scheduler *job_scheduler;

    free((job_scheduler)->thread_pool);
    pthread_mutex_destroy(&(job_scheduler)->mutex);
    pthread_cond_destroy(&(job_scheduler)->empty);
    pthread_cond_destroy(&(job_scheduler)->not_empty);
    free_queue(&(job_scheduler)->queue);
    free(job_scheduler);
}

void stop_job_scheduler() {
    extern struct job_scheduler *job_scheduler;

    job_scheduler->stop = true;
    pthread_cond_broadcast(&job_scheduler->not_empty);
    for( int i = 0 ; i < (job_scheduler)->number_of_threads ; i++ )
        pthread_join((job_scheduler)->thread_pool[i],0);
}

void schedule_job_scheduler(void (*function)(void*), void *argument, size_t size, int *barrier) {
    extern struct job_scheduler *job_scheduler;
	if(size!=0){
		void *arg=malloc(size);
		void *point_dest=arg;
		void *point_src=argument;
		for(int i=0;i<size;i++)
			*(char*)(point_dest++)=*(char*)(point_src++);
		argument=arg;
	}
    pthread_mutex_lock(&job_scheduler->mutex);
	(*barrier)++;
    push_queue(&job_scheduler->queue, function, argument, size, barrier);
    job_scheduler->jobs++;
    pthread_cond_signal(&job_scheduler->not_empty);
    pthread_mutex_unlock(&job_scheduler->mutex);
}
void dynamic_barrier_job_scheduler(int *barrier){
	extern struct job_scheduler *job_scheduler;
	
	struct job *job=NULL;
	void (*function)(void*);
	void *argument;

	while(*barrier!=0){
		pthread_mutex_lock(&job_scheduler->mutex);
		if(job_scheduler->queue->length!=0){
			job=pop_queue(&job_scheduler->queue);
			pthread_mutex_unlock(&job_scheduler->mutex);
			if(job==NULL)
				continue;
			function=job->function;
			argument=job->argument;
			function(argument);
			if(job->size!=0){
				free(argument);
			}
			pthread_mutex_lock(&job_scheduler->mutex);
			job_scheduler->jobs--;
			if(job_scheduler->jobs==0)
				pthread_cond_signal(&job_scheduler->empty);
			(*(job->barrier))--;
			free(job);
			pthread_mutex_unlock(&job_scheduler->mutex);
		}
		else{
			pthread_mutex_unlock(&job_scheduler->mutex);
		}
	}
}


void *thread_function(void *arguments) {
    extern struct job_scheduler *job_scheduler;
    struct job *job = NULL;
    void (*function)(void*);
    void*  argument;

    while( true ) {
        pthread_mutex_lock(&job_scheduler->mutex);
        while( (job_scheduler->queue->length == 0) && (job_scheduler->stop == false) )
            pthread_cond_wait(&job_scheduler->not_empty,&job_scheduler->mutex);
        if( job_scheduler->stop == true ) {
            pthread_mutex_unlock(&job_scheduler->mutex);
            pthread_exit(0);
        }
        else {
            job = pop_queue(&job_scheduler->queue);
            pthread_mutex_unlock(&job_scheduler->mutex);

            function = job->function;
            argument = job->argument;

            function(argument);
			if(job->size!=0){
				free(argument);
			}

            pthread_mutex_lock(&job_scheduler->mutex);
            job_scheduler->jobs--;
            if( job_scheduler->jobs == 0 )
                pthread_cond_signal(&job_scheduler->empty);
			(*(job->barrier))--;
            free(job);
            pthread_mutex_unlock(&job_scheduler->mutex);
        }
    }
}

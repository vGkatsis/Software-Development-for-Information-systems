#include "../headers/list_results.h"

struct list * initialize_list() {
    struct list *new_list = (struct list *)malloc(sizeof(struct list));
    if( new_list == NULL) {
        perror("initialize_list failed");
        return NULL;
    }
    new_list->head = NULL;
    new_list->tail = NULL;
    new_list->number_of_buckets = 0;
    new_list->total_size = 0;

    return new_list;
}

void free_list(struct list **list) {
    if( *list == NULL )
        return;
    while((*list)->head != NULL) {
        (*list)->tail = (*list)->head->next_bucket;
        free((*list)->head->tuples);
        free((*list)->head);
        (*list)->head = (*list)->tail;
    }
    free(*list);
}

struct bucket * initialize_bucket() {
    struct bucket *new_bucket = (struct bucket *)malloc(sizeof(struct bucket));
    if( new_bucket == NULL) {
        perror("initialize_bucket failed");
        return NULL;
    }

    new_bucket->tuples = (struct row_key_tuple *)malloc(NUMBER_OF_TUPLES_IN_BUCKET*sizeof(struct row_key_tuple));
    if( new_bucket->tuples == NULL) {
        perror("initialize_bucket failed");
        return NULL;
    }
    
    for(int i = 0 ; i < NUMBER_OF_TUPLES_IN_BUCKET ; i++) {
        new_bucket->tuples[i].row_key_1 = 0;
        new_bucket->tuples[i].row_key_2 = 0;
    }

    new_bucket->max_size = NUMBER_OF_TUPLES_IN_BUCKET;
    new_bucket->current_size = 0;
    new_bucket->next_bucket = NULL;

    return new_bucket;
}

void insert_tuple(struct list **list, uint64_t row_key_1, uint64_t row_key_2) {

    // If the list is empty, insert a new bucket and push the tuple at the first position. Also fix head and tail
    if( (*list)->number_of_buckets == 0 ) {
        // fix list data
        (*list)->head = initialize_bucket();
        (*list)->tail = (*list)->head;
        (*list)->number_of_buckets++;
        // fix bucket data
        (*list)->head->tuples[0].row_key_1 = row_key_1;
        (*list)->head->tuples[0].row_key_2 = row_key_2;
        (*list)->head->current_size++;
    }
    // If the bucket is full then insert a new bucket and push the tuple at the first position. Fix only tail
    else if( (*list)->tail->current_size == (*list)->tail->max_size ) {
        // fix list data
        (*list)->tail->next_bucket = initialize_bucket();
        (*list)->number_of_buckets++;
        (*list)->tail = (*list)->tail->next_bucket;
        // fix bucket data
        (*list)->tail->tuples[0].row_key_1 = row_key_1;
        (*list)->tail->tuples[0].row_key_2 = row_key_2;
        (*list)->tail->current_size++;
    }
    // If the bucket is not full just insert the tuple at current position
    else {
        // fix only bucket data
        (*list)->tail->tuples[(*list)->tail->current_size].row_key_1 = row_key_1;
        (*list)->tail->tuples[(*list)->tail->current_size].row_key_2 = row_key_2;
        (*list)->tail->current_size++;
    }
    (*list)->total_size++;
}

void print_list(struct list * list) {
    struct bucket * temp_bucket = list->head;
    printf("Total size: %ld buckets: %ld\n",list->total_size,list->number_of_buckets);
    printf("row_id1,row_id2\n");
    for( int i = 0 ; i< list->number_of_buckets ; i++) {
        printf("Bucket[%d]: current_size:%d max_size: %d\nRowId_1,RowId_2\n",i,temp_bucket->current_size, temp_bucket->max_size);
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ )
            printf("%"PRIu64",%"PRIu64"\n", temp_bucket->tuples[j].row_key_1, temp_bucket->tuples[j].row_key_2);
        temp_bucket = temp_bucket->next_bucket;
    }
}

struct list **initialize_2d_list_results(int rows) {
    struct list **thread_list_results = (struct list **)malloc(rows*sizeof(struct list *));
    if( thread_list_results == NULL ) {
        perror("initialize_2d_list_results failed");
        return NULL;
    }

    for ( int i = 0 ; i < rows ; i++ )
        thread_list_results[i] = initialize_list();

    return thread_list_results;
}

void print_2d_list_results(struct list **thread_list_results, int rows) {
    for( int i = 0 ; i < rows ; i ++ )
        print_list(thread_list_results[i]);
}

void free_2d_list_results(struct list **thread_list_results, int rows) {
    for( int i = 0 ; i < rows; i++ )
        free(thread_list_results[i]);
    free(thread_list_results);
}
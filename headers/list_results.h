#ifndef list_results_H_
#define list_results_H_

#include "header.h"
#define NUMBER_OF_TUPLES_IN_BUCKET ((1024*1024)-2*(sizeof(int))-sizeof(struct bucket *))/sizeof(struct row_key_tuple)

// Type definition for a row key tuple.
struct row_key_tuple {
    uint64_t row_key_1;
    uint64_t row_key_2;
};

// Type definition for a bucket.
struct bucket {
    struct row_key_tuple *tuples;
    struct bucket *next_bucket;
    int current_size;
    int max_size;
};

// Type definition for a list with head and tail.
struct list {
    struct bucket *head;
    struct bucket *tail;
    int64_t number_of_buckets;
    int64_t total_size;
};

struct list * initialize_list();
void free_list(struct list ** );
struct bucket * initialize_bucket();
void insert_tuple(struct list **, uint64_t , uint64_t );
void print_list(struct list * );
struct list **initialize_2d_list_results(int );
void print_2d_list_results(struct list **, int );
void free_2d_list_results(struct list **, int );

#endif
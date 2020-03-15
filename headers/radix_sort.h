#ifndef radix_sort_H_
#define radix_sort_H_

#include "header.h"
#include "relation.h"
#include "utilities.h"

#define CACHE_SIZE 64*1024
#define START_BYTE 6
#define MODULO 0
#define DUPLICATES CACHE_SIZE/sizeof(Tuple)

enum mode {
    RECURSIVE, ITERATIVE
};

struct sort_args {
    Relation *R;
    void *Placeholder_for_Histogram;
    struct join_partition *join_partition;
};

struct sort_loop_args {
    Relation *R;
    Relation *R_new;
    uint64_t start;
    uint64_t end;
    int byte;
    int *result_relation;
};

struct quick_sort_arguments {
    Relation *R;
    int start;
    int end;
};

struct histogram_args {
    Relation *R;
    uint64_t start;
    uint64_t end;
    int byte;
};

struct histogram_arguments {
    Relation *R;
    uint64_t start;
    uint64_t end;
    int byte;
    int64_t *histogram;
};

void break_histogram_to_jobs(Relation *, uint64_t , uint64_t , int ,struct histogram_indexing *, int64_t **, int *); 
uint64_t * sum_histograms(int64_t **, int , int , struct histogram_indexing *);
uint64_t *create_histogram_multithread(Relation *, uint64_t ,uint64_t , int , struct histogram_indexing *);
unsigned char binary_mask(uint64_t,int);
uint64_t *create_histogram(Relation*, uint64_t, uint64_t, int, struct histogram_indexing *);
uint64_t *create_prefix_sum(uint64_t*,uint64_t);
void copy_back_to_result_relation(Relation*, Relation*, uint64_t, uint64_t); //Copies data back to the result relation
void fill_new_relation(Relation*, Relation*, uint64_t*, uint64_t, uint64_t, int);
void swap_tuples(Relation **, int , int );
int partition(Relation **, int , int );
void quick_sort(Relation **, int , int );
void sort_recursive(struct sort_loop_args*);
void sort_iterative(Relation *, Relation *, uint64_t , uint64_t , int ,struct join_partition *);
void sort(Relation *, int);
void sort_multithread(struct sort_args*);

#endif

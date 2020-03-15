#ifndef relation_H_
#define relation_H_

#include "header.h"
#include "list_results.h"
#include "job_scheduler.h"

// Type definition for a tuple.
typedef struct tuple {
    uint64_t row_id;
    uint64_t value;
}Tuple;

// Type definition for a relation.
typedef struct relation {
    struct tuple *tuples;
    uint64_t num_tuples;
}Relation;

struct parallel_join_arguments {
    struct relation * R;
    struct relation * S;
    struct list * list;
    int start_R;
    int end_R;
    int start_S;
    int end_S;
};

struct relation * initialize_relation(int );
struct relation * initialize_relation_with_dataset(char *);
void free_relation(struct relation **);
void change_value(struct relation *, int , uint64_t );
void change_rowId(struct relation *, int , uint64_t );
int64_t get_value(struct relation *, int );
int64_t get_rowId(struct relation *, int );
void print_relation(struct relation *);
void get_range(struct relation *, int, int *, int);
void parallel_join(struct relation *, struct relation *, struct list **);
void break_join_to_jobs(struct relation **, struct relation **, struct list *, struct join_partition *, struct join_partition *);

#endif

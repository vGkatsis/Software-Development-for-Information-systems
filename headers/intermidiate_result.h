#include "relation.h"
#include "radix_sort.h"
#include "sql_query.h"
#include "file_array.h"
#include "header.h"
#include "vector.h"
#include "utilities.h"
#include "job_scheduler.h"
#include "join_enumaration.h"

struct execute_query_arguments {
    struct file_array file_array;
    Query *sql_query;
    int64_t **results;
    int result_index;
	stats_list *statistics_list;
};

struct intermidiate_result {
    uint64_t file_index;
    uint64_t predicate_relation;
    char field;
    vector(uint64_t) row_ids;
};

struct intermidiate_results {
    vector(struct intermidiate_result) results;
    int sorted_relations[2];                    // if we join 0.1=2.3, then we will have sorted_relations = |0,2|
    int sorted_relation_columns[2];             //                              and sorted relation columns |1,3|
};

struct relation *create_relation_from_file_array(struct file, int );
struct relation *create_relation_from_intermidiate_results_for_join(struct file , struct intermidiate_results , int , int );
void initialize_intermidiate_result(struct intermidiate_result *, uint64_t , uint64_t , char , uint64_t );
bool none_in_mid_results(struct file_array , struct intermidiate_results *, int * , int * );
bool only_one_relation_in_mid_results(struct file_array , struct intermidiate_results *, int * , int * , int );
bool both_relations_in_mid_results(struct file_array , struct intermidiate_results *, int * , int * , int , int );
bool join(struct file_array , struct intermidiate_results * , int * , int *);
void synchronize_intermidiate_results(struct intermidiate_results *, struct list *, int );
bool filter(struct file_array , struct intermidiate_results *, int *, int *);
void execute_filters(struct file_array , Query , struct intermidiate_results *);
void execute_joins(struct file_array , Query , struct intermidiate_results *);
void initialize_intermidiate_results(struct intermidiate_results *, Query );
void inform_intermidiate_sort_fields(struct intermidiate_results *, int *);
struct list * sort_join_calculation(struct relation **, struct relation **, struct intermidiate_results , int *);
void print_null(int , int64_t **, int );
void execute_query(void *);
//void read_queries(struct file_array);
void read_queries(struct file_array, stats_list *statistics_list);
void projection_sum_results(struct file_array , struct intermidiate_results , Query, int64_t **, int );
void print_intermidiate_results(struct intermidiate_results );
int search_intermidiate_results(struct intermidiate_results , int  );
void free_intermidiate_results(struct intermidiate_results *);

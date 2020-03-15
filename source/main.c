#include "../headers/header.h"
#include "../headers/relation.h"
#include "../headers/list_results.h"
#include "../headers/radix_sort.h"
#include "../headers/sort_list.h"
#include "../headers/utilities.h"
#include "../headers/file_array.h"
#include "../headers/sql_query.h"
#include "../headers/stats.h"
#include "../headers/join_enumaration.h"
#include "../headers/enumeration_utilities.h"
#include "../headers/intermidiate_result.h"
#include "../headers/job_scheduler.h"

struct job_scheduler *job_scheduler = NULL;

int main(int argc, char *argv[]) {
	int use_join_enumeration=0;
    int num_threads=NUMBER_OF_THREADS;
    if(argc>1){
        num_threads=atoi(argv[1]);
		}
	if(argc>2){
		if(!strcmp(argv[2],"--use-join")){
			use_join_enumeration=1;
		}
	}
    job_scheduler = initialize_job_scheduler(num_threads);
    create_threads_job_scheduler();

    struct timespec begin, end;
    double time_spent;
    struct file_array file_array;
    stats_list *statistics_list = NULL;

    initialize_statsList(&statistics_list);
    fix_file_array(&file_array, statistics_list);

	if(use_join_enumeration==0){
		free_statsList(statistics_list);
		statistics_list=NULL;
	}

    clock_gettime(CLOCK_MONOTONIC, &begin);
    read_queries(file_array, statistics_list);
    clock_gettime(CLOCK_MONOTONIC, &end);

    time_spent = (end.tv_sec - begin.tv_sec);
    time_spent = time_spent + (end.tv_nsec-begin.tv_nsec)/1000000000.0;
    printf("Execution time = %f\n",time_spent);

    free_file_array(file_array);
	if(statistics_list!=NULL)
	    free_statsList(statistics_list);

    stop_job_scheduler();
    free_job_scheduler();

    return 0;
}

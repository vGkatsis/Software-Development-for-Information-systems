#ifndef utilities_H_
#define utilities_H_
#include "../headers/header.h"
#include "../headers/list_results.h"
#include "../headers/radix_sort.h"

enum error_code {
	DATASET_COMPLETE, DATASET_A, DATASET_B, RANDOM, NONE
	
};

typedef struct arguments{
	int sort_method;
	//int number_of_tuples;
	//char *dataset_A;
	//char *dataset_B;
	Relation *R;
	Relation *S;
}Arguments;

Arguments parse_arguments(int argc,char *argv[]);
int64_t **allocate_and_initialize_2d_array(int , int , int);
void print_2d_array(int64_t **, int , int );
void print_2d_array_results(int64_t **, int , int );
void free_2d_array(int64_t ***, int );

#endif
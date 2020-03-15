#ifndef JOIN_ENUMARATION
#define JOIN_ENUMARATION

#include "../headers/header.h"
#include "../headers/sql_query.h"
#include "../headers/enumeration_structs.h"
#include "../headers/enumeration_utilities.h"

void join_enumeration(struct sql_query *query,stats_list *statistics_list);             //Find The Best Query Execution Order Using DP-Linear Algorithm
void join_enumeration_2(struct sql_query *query,stats_list *statistics_list);
void filter_swap(struct sql_query *sql_query, int a, int b);              //Swap Function Used By Quick Sort
int filter_partition (struct sql_query *sql_query, int low, int high, stats_list *statistics_list,struct sql_query *query);  //Partition Function Used In Quick Sort 
void filter_quickSort(struct sql_query * sql_query, int low, int high, stats_list *statistics_list,struct sql_query *query); //QuickSort Function Fro Filter Ordering 
void order_filters(struct sql_query *query,stats_list *statistics_list);  //Order The Filters By Execution Cost
long double filter_cost(struct predicate filter, stats_list *statistics_list,struct sql_query *query);
#endif
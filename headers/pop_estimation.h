#ifndef POP_ESTIMATION_H
#define POP_ESTIMATION_H

#include <math.h>
#include "header.h"
#include "stats.h"

//Filter Functions
void equal_filter(stats *result_stats, uint64_t k, relation_node *origin_relation);                      //Population Estimation For Filters Of Equality
void range_filters(stats *col_stats, uint64_t k1, int op1, uint64_t k2, int op2, relation_node *origin_relation);     //Population Estimation For Range Filters
void column_filter(stats *colA_stats, stats *colB_stats, relation_node *origin_relation);                //Population Estimation For Column Filters

//Join Functions
void diff_join(stats *colA_stats, stats *colB_stats, relation_node *origin_relationA, relation_node *origin_relationB);  //Population Estimation For Different Table Columns
void self_join(stats *col_stats, relation_node *origin_relation);                                       //Population Estimation For Same Table COlumns

#endif
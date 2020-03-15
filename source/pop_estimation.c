#include "../headers/pop_estimation.h"

//Filter Functions
void equal_filter(stats *result_stats, uint64_t k, relation_node *origin_relation){

    long double data_num = result_stats->data_num;
    long double unique_data = result_stats->unique_data;

    result_stats->min = k;
    result_stats->max = k;

    if(find_inBoolean(result_stats,k)){
        result_stats->unique_data = 1;
        result_stats->data_num = data_num / unique_data;
    }else{
        result_stats->unique_data = 0;
        result_stats->data_num = 0;        
    }

    stats_node *origin_node;

    origin_node = origin_relation->stats;
    while(origin_node != NULL){
        long double temp_data_num = origin_node->statistics->data_num;
        if(origin_node->statistics != result_stats){
            origin_node->statistics->data_num = result_stats->data_num;
            origin_node->statistics->unique_data = origin_node->statistics->unique_data * (1 - powl((long double)(1 - (result_stats->data_num/data_num)),(long double)(temp_data_num/origin_node->statistics->unique_data)));
        }
        origin_node = origin_node->next;
    }
}

void range_filters(stats *col_stats, uint64_t k1, int op1, uint64_t k2, int op2,relation_node *origin_relation){

    uint64_t min = col_stats->min;
    uint64_t max = col_stats->max;
    long double data_num = col_stats->data_num;
    long double unique_data = col_stats->unique_data;

    if((k1  < col_stats->min) || (op1 == -1)){
        k1 = col_stats->min;
    }

    if((k2 > col_stats->max) || (op2 == -1)){
        k2 = col_stats->max;
    }

    col_stats->min = k1;
    col_stats->max = k2;
    col_stats->data_num = ((k2-k1)/(max - min)) * data_num;
    col_stats->unique_data = ((k2-k1)/(max - min)) * unique_data;
    
    stats_node *origin_node;

    origin_node = origin_relation->stats;

    while(origin_node != NULL){
        long double temp_data_num = origin_node->statistics->data_num;
        if(origin_node->statistics != col_stats){
            origin_node->statistics->data_num = col_stats->data_num;
            origin_node->statistics->unique_data = origin_node->statistics->unique_data * (1 - powl((long double)(1 - (col_stats->data_num/data_num)),(long double)(temp_data_num/origin_node->statistics->unique_data)));
        }
        origin_node = origin_node->next;
    }
}

void column_filter(stats *colA_stats, stats *colB_stats, relation_node *origin_relation){

    uint64_t n;
    uint64_t minA = colA_stats->min;
    uint64_t minB = colB_stats->min;
    uint64_t maxA = colA_stats->max;
    uint64_t maxB = colB_stats->max;

    long double data_num = colA_stats->data_num;

    if(minA >= minB){
        colB_stats->min = minA;
    }else{
        colA_stats->min = minB;
    }

    if(maxA <= maxB){
        colB_stats->max = maxA;
    }else{
        colA_stats->max = maxB;
    }

    n = colA_stats->max - colA_stats->min + 1;

    colA_stats->data_num = data_num / n;
    colB_stats->data_num = data_num;

    colA_stats->unique_data = colA_stats->unique_data * (1 - powl((long double)(1 - (colA_stats->data_num/data_num)),(long double)(data_num/colA_stats->unique_data)));
    colB_stats->unique_data = colA_stats->unique_data;

    stats_node *origin_node;

    origin_node = origin_relation->stats;

    while(origin_node != NULL){
        long double temp_data_num = origin_node->statistics->data_num;
        if(origin_node->statistics != colA_stats){
            origin_node->statistics->data_num = colA_stats->data_num;
            origin_node->statistics->unique_data = origin_node->statistics->unique_data * (1 - powl((long double)(1 - (colA_stats->data_num/data_num)),(long double)(temp_data_num/origin_node->statistics->unique_data)));
        }
        origin_node = origin_node->next;
    }
}

//Join Functions
void diff_join(stats *colA_stats, stats *colB_stats, relation_node *origin_relationA, relation_node *origin_relationB){

    long double data_numA = colA_stats->data_num;
    long double data_numB = colB_stats->data_num;

    colB_stats->min = colA_stats->min;
    colB_stats->max = colA_stats->max;
    
    colA_stats->data_num = (data_numA * data_numB) / (colA_stats->max - colA_stats->min + 1);
    colB_stats->data_num = colA_stats->data_num; 

    colA_stats->unique_data = (colA_stats->unique_data * colB_stats->unique_data) / (colA_stats->max - colA_stats->min + 1);
    colB_stats->unique_data = colA_stats->unique_data;

    stats_node *origin_nodeA;
    stats_node *origin_nodeB;

    origin_nodeA = origin_relationA->stats;

    origin_nodeB = origin_relationB->stats;

    while((origin_nodeA != NULL) && (origin_nodeB != NULL)){
        long double tempA_data_num = origin_nodeA->statistics->data_num;
        if(origin_nodeA->statistics != colA_stats){
            origin_nodeA->statistics->data_num = colA_stats->data_num;    
            origin_nodeA->statistics->unique_data = origin_nodeA->statistics->unique_data * (1 - powl((long double)(1 - (colA_stats->data_num/data_numA)),(long double)(tempA_data_num/origin_nodeA->statistics->unique_data)));
        }
        long double tempB_data_num = origin_nodeB->statistics->data_num;
        if(origin_nodeB->statistics != colB_stats){
            origin_nodeB->statistics->data_num = colB_stats->data_num;    
            origin_nodeB->statistics->unique_data = origin_nodeB->statistics->unique_data * (1 - powl((long double)(1 - (colB_stats->data_num/data_numB)),(long double)(tempB_data_num/origin_nodeB->statistics->unique_data)));
        }

        origin_nodeA = origin_nodeA->next;
        origin_nodeB = origin_nodeB->next;
    }
}

void self_join(stats *col_stats, relation_node *origin_relation){

    col_stats->data_num = (col_stats->data_num * col_stats->data_num)/(col_stats->max - col_stats->min + 1);

    stats_node *origin_node;

    origin_node = origin_relation->stats;

    while(origin_node != NULL){
        if(origin_node->statistics != col_stats){
            origin_node->statistics->data_num = col_stats->data_num;
        }
        origin_node = origin_node->next;
    }
}
#include "../headers/stats.h"

void initialize_stats(stats **statistics){

    (*statistics) = (stats *) malloc(1 * sizeof(stats));

    (*statistics)->empty = 'y';
    (*statistics)->min = 0;
    (*statistics)->max = 0;
    (*statistics)->data_num = 0;
    (*statistics)->unique_data = 0;
    (*statistics)->boolean_array = NULL;
}

void initialize_bolleanArray(stats *statistics){

    uint64_t size = statistics->max - statistics->min + 1;
    if((size > BOOLEAN_SIZE) || size < 0){
        size = BOOLEAN_SIZE;
    }
    
    statistics->boolean_array = malloc(size * sizeof(int));

    for(uint64_t i = 0; i < size; i++){
        statistics->boolean_array[i] = 0;
    }
}

void initialize_statsNode(stats_node **node, stats *new_statistics){
    (*node) = (stats_node *) malloc(1 * sizeof(stats_node));

    (*node)->statistics = new_statistics;
    (*node)->next = NULL;
}

void initialize_relationNode(relation_node **node){
    (*node) = (relation_node *) malloc(1 * sizeof(relation_node));

    (*node)->size = 0;
    (*node)->stats = NULL;
    (*node)->next = NULL;
}

void initialize_statsList(stats_list **list){
    (*list) = (stats_list *) malloc(1 * sizeof(stats_list));

    (*list)->relation_number = 0;
    (*list)->head = NULL;
}

void copy_statsList(stats_list *list, stats_list **new_list){
    
    uint64_t size;
    relation_node *relation, *new_relation;
    stats_node *stats_node, *new_stats_node;
    stats *statistics;

    initialize_statsList(new_list);
    relation = list->head;
    for(int i = 0; i < list->relation_number; i++){
        initialize_relationNode(&new_relation);
        stats_node = relation->stats;
        for(int j = 0; j < relation->size; j++){
            initialize_stats(&statistics);
            statistics->max = stats_node->statistics->max;
            statistics->min = stats_node->statistics->min;
            statistics->data_num = stats_node->statistics->data_num;
            statistics->unique_data = stats_node->statistics->unique_data;
            statistics->empty = stats_node->statistics->empty;

            size = statistics->max - statistics->min + 1;
            if(size > BOOLEAN_SIZE){
                size = BOOLEAN_SIZE;
            }

            initialize_bolleanArray(statistics);
            for(uint64_t k = 0; k < size; k++){
                statistics->boolean_array[k] = stats_node->statistics->boolean_array[k];
            }

            initialize_statsNode(&new_stats_node,statistics);
            add_last_statsNode(new_relation,new_stats_node);

            stats_node = stats_node->next;
        }
    
        add_last_relationNode((*new_list), new_relation);
        relation = relation->next;
    }
}

void add_last_statsNode(relation_node *node, stats_node *new_node){

    stats_node *stats = node->stats;

    if(stats != NULL){
        while(stats->next != NULL){
            stats = stats->next;
        }

        stats->next = new_node;
        node->size++;
    }else
    {
        node->stats = new_node;
        node->size = 1;
    }
    
}

void add_last_relationNode(stats_list *list, relation_node *new_relation){

    relation_node *relations = list->head;

    if(relations != NULL){
        while(relations->next != NULL){
            relations = relations->next;
        }

        relations->next = new_relation;
        list->relation_number++;
    }else
    {
        list->head = new_relation;
        list->relation_number = 1;
    }
    
} 

void fill_booleanArray(stats *statistics, uint64_t value){

    uint64_t size = statistics->max - statistics->min + 1;
    int big_array = 0;
    
    if(size > BOOLEAN_SIZE){
        size = BOOLEAN_SIZE;
        big_array = 1;
    }

    if(big_array){
        statistics->boolean_array[(value - statistics->min) % BOOLEAN_SIZE] = 1;
    }else{
        statistics->boolean_array[value - statistics->min] = 1;
    }
}

int find_inBoolean(stats *statistics, uint64_t value){

    uint64_t size = statistics->max - statistics->min + 1;
    int big_array = 0;
    
    if(size > BOOLEAN_SIZE){
        size = BOOLEAN_SIZE;
        big_array = 1;
    }

    if(big_array){
        return statistics->boolean_array[(value - statistics->min) % BOOLEAN_SIZE];
    }else{
        return statistics->boolean_array[value - statistics->min];
    }
}

void find_Unique(stats *statistics){

    uint64_t size = statistics->max - statistics->min + 1;
    if(size > BOOLEAN_SIZE){
        size = BOOLEAN_SIZE;
    }

    for(uint64_t i = 0; i < size; i++){
        if(statistics->boolean_array[i] == 1){
            update_uniqueData(statistics);
        }
    }
}

void is_min(stats *statistics, uint64_t value){
    
    if(statistics->empty == 'y'){
        statistics->empty = 'n';
        statistics->min = value;
    }else{
        if(value < statistics->min){
            statistics->min = value;
        }
    }
}

void is_max(stats *statistics, uint64_t value){

    if(statistics->empty == 'y'){
        statistics->empty = 'n';
        statistics->max = value;
    }else{
        if(value > statistics->max){
            statistics->max = value;
        }
    }
}

void free_statsNode(stats_node *node){
        free_booleanArray(node->statistics);
        free(node->statistics);
}

void free_statsList(stats_list *list){

    stats_node *node, *next_node;
    relation_node *relation, *next_relation;

    relation = list->head;
    while(relation != NULL){
        next_relation = relation->next;
            
        node = relation->stats;
        while(node != NULL){
            next_node = node->next;
            free_statsNode(node);
            free(node);
            node = next_node;
        }

        free(relation);
        relation = next_relation;
    }

    free(list);
}

void free_booleanArray(stats *statistics){
    free(statistics->boolean_array);
}

uint64_t get_min(stats *statistics){
    return statistics->min;
}

uint64_t get_max(stats *statistics){
    return statistics->max;
}

long double get_dataNum(stats *statistics){
    return statistics->data_num;
}

long double get_uniqueData(stats *statistics){
    return statistics->unique_data;
}

void set_min(stats *statistics, uint64_t new_min){
    statistics->min = new_min;
}

void set_max(stats *statistics, uint64_t new_max){
    statistics->max = new_max;
}

void set_dataNum(stats *statistics, long double new_dataNum){
    statistics->data_num = new_dataNum;
}

void set_uniqieData(stats *statistics, long double new_uniqueData){
    statistics->unique_data = new_uniqueData;
}

void set_booleanArrayValue(stats *statistics, int value, int boolean){
    statistics->boolean_array[value - statistics->min] = boolean;
}

void update_dataNum(stats *statistics){
    statistics->data_num++;
}

void update_uniqueData(stats *statistics){
    statistics->unique_data++;
}

void print_Statslist(stats_list *list){

    int r_n = list->relation_number;
    int s_n;

    relation_node *r_node = list->head;
    stats_node *s_node;

    for(int i = 0; i < r_n; i++){
        printf("Relation %d\t",i);
        s_n = r_node->size;
        s_node = r_node->stats;
        for(int j = 0; j < s_n; j++){
            printf("Node %d\t",j);
            printf("{Min: %"PRId64" Max: %"PRId64" Mult: %Lf Boolean Array: %p  Unique Data: %Lf}\n\t\t",get_min(s_node->statistics),get_max(s_node->statistics),get_dataNum(s_node->statistics),s_node->statistics->boolean_array,get_uniqueData(s_node->statistics));
            s_node = s_node->next;
        }
        printf("\n");
        r_node = r_node->next;
    }
}
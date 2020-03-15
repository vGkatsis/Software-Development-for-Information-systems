#include "../headers/enumeration_structs.h"

void initialize_bitVector(bit_vector **vector, int relations_num, stats_list *stats){

    (*vector) =  (bit_vector *) malloc(1 * sizeof(bit_vector));

    (*vector)->relation_length = 0;
    (*vector)->relations_num = relations_num;
    (*vector)->cost = 0.0;
    (*vector)->bitVector_stats = NULL;
    (*vector)->joinTree = (bitVector_node *) malloc(relations_num * sizeof(bitVector_node));

    if(stats != NULL){
        copy_statsList(stats,&((*vector)->bitVector_stats));
    }

    for(int i = 0; i < relations_num; i++){
        initialize_bitVectorNode(&((*vector)->joinTree[i]));
    }
}

void initialize_bitVectorNode(bitVector_node *vector_node){

    vector_node->position = 0;
    vector_node->exists = 0;
}

void parse_set(relation_set *relations, relation_set *set, bit_vector *vector){

    int set_length = set->set_length;
    int set_found[set_length];
    query_relation *relation, *set_relation;

    for(int i = 0; i < set_length; i++){
        set_found[i] = 0;
    }

    relation = relations->first;
    for(int i = 0; i < vector->relations_num; i++){
        int j = 0;
        set_relation = set->first;
        while(j < set_length){
            if((relation->relation == set_relation->relation) &&
                (relation->column == set_relation->column)){
                if(!set_found[j]){
                    vector->relation_length++;
                    vector->joinTree[i].exists = 1;
                    vector->joinTree[i].position = vector->relation_length;
                    set_found[j] = 1;
                }
                break;
            }
            j++;
            set_relation = set_relation->next;
        }
        relation = relation->next;
    }
}

void parse_bitVector(relation_set *relations, bit_vector *vector, relation_set **set){

    query_relation *relation;
    
    init_relationSet(set);

    relation = relations->first;
    for(int i = 0; i < vector->relations_num; i++){
        if(vector->joinTree[i].exists){
            initialize_queryRelation(&relation,relation->relation,relation->column);
            add_atEnd((*set),relation);
        }
        relation = relation->next;
    }
}

bit_vector* joinTree_union(bit_vector *vector1, bit_vector *vector2){

    int i = 0;
    bit_vector *temp_vector;

    initialize_bitVector(&temp_vector,vector1->relations_num,vector1->bitVector_stats);

    while(i < vector1->relations_num){
        if(vector1->joinTree[i].exists){
            temp_vector->joinTree[i].exists = 1;
            temp_vector->joinTree[i].position = vector1->joinTree[i].position;
            temp_vector->relation_length++;
        }
        if(vector2->joinTree[i].exists){
            if(!vector1->joinTree[i].exists){   
                temp_vector->joinTree[i].exists = 1;
                temp_vector->relation_length++;
                temp_vector->joinTree[i].position = vector1->relation_length + 1;
            }
        }
        i++;
    }

    return temp_vector;
}

int connected(bit_vector *joinTree, bit_vector *singleTree){

    for(int i = 0; i < joinTree->relations_num; i++){
        if(joinTree->joinTree[i].exists && singleTree->joinTree[i].exists){
            return 1;
        }
    }
    return 0;
}

int bit_hash(bit_vector *vector){

    int hash = 0;
    int bit_factor = 1;

    for(int i = 0; i < vector->relations_num; i++){

        hash += (vector->joinTree[i].exists * bit_factor);
        bit_factor *= 2;
    }

    return hash;
}

long double findJoinTree_cost(hash_table *bestTree ,bit_vector *vector){

    int position = bit_hash(vector) - 1;

    return bestTree->table[position]->cost;
}

void newJoinTree_cost(bit_vector *vector,relation_set *relation_set, struct sql_query *query){

    int relation_length = vector->relations_num;
    stats_node *col_statsA, *col_statsB;
    relation_node *origin_relationA, *origin_relationB;
    query_relation *relation1 = NULL, *relation2 = NULL, *temp_relation;

    temp_relation = relation_set->first;
    for(int i = 0; i < relation_length; i++){
        if(vector->joinTree[i].position == vector->relation_length - 1){
            initialize_queryRelation(&relation1,vector_at(&(query->relations),temp_relation->relation),temp_relation->column);
        }

        if(vector->joinTree[i].position == vector->relation_length){
            initialize_queryRelation(&relation2,vector_at(&(query->relations),temp_relation->relation),temp_relation->column);
        }
        temp_relation = temp_relation->next;
    }
 
    if(relation1->relation != relation2->relation){
        uint64_t min,max;

        origin_relationA = vector->bitVector_stats->head;
        for(int i = 0; i < relation1->relation; i++){
            origin_relationA = origin_relationA->next;
        }
    
        origin_relationB = vector->bitVector_stats->head;
        for(int i = 0; i < relation2->relation; i++){
            origin_relationB = origin_relationB->next;
        }

        col_statsA = origin_relationA->stats;
        for(int i = 0; i < relation1->column; i++){
            col_statsA = col_statsA->next;
        }

        col_statsB = origin_relationB->stats;
        for(int i = 0; i < relation2->column; i++){
            col_statsB = col_statsB->next;
        }

        min = col_statsA->statistics->min;
        max = col_statsA->statistics->max;
        if(min < col_statsB->statistics->min){
            min = col_statsB->statistics->min;
        }

        if(max > col_statsB->statistics->max){
            max = col_statsB->statistics->max;
        }
    
        range_filters(col_statsA->statistics,min,'<',max,'>',origin_relationA);
        range_filters(col_statsB->statistics,min,'<',max,'>',origin_relationB);
    
        diff_join(col_statsA->statistics,col_statsB->statistics,origin_relationA,origin_relationB);

        vector->cost = col_statsA->statistics->data_num;
    }

    if((relation1->relation == relation2->relation) &&
        (relation1->column == relation2->column)){

        origin_relationA = vector->bitVector_stats->head;
        for(int i = 0; i < relation1->relation; i++){
            origin_relationA = origin_relationA->next;
        }

        col_statsA = origin_relationA->stats;
        for(int i = 0; i < relation1->column; i++){
            col_statsA = col_statsA->next;
        }

        self_join(col_statsA->statistics,origin_relationA);
    
        vector->cost = col_statsA->statistics->data_num;
    }

    free(relation1);
    free(relation2);
}

void free_bitVector(bit_vector *vector){
    if( vector->bitVector_stats != NULL)
        free_statsList(vector->bitVector_stats);
    free(vector->joinTree);
    free(vector);
}

void initialize_hashTable(hash_table **bestTree, int relations_num){

    int table_size = ((int)pow(2,relations_num)) - 1;

    (*bestTree) = (hash_table *) malloc(1 * sizeof(hash_table));

    (*bestTree)->table_size = table_size;
    (*bestTree)->table = (bit_vector **) malloc(table_size * sizeof(bit_vector *));

    for(int i = 0; i < table_size; i++){
        (*bestTree)->table[i] = NULL;
    }
}

int fill_hashTable(hash_table *bestTree, bit_vector *joinTree){

    int position = bit_hash(joinTree) - 1;
    if(bestTree->table[position]!=NULL) {
        free_bitVector(joinTree);
        return 1;
    }
    else {
        bestTree->table[position] = joinTree;
        return 0;
    }
}

bit_vector* find_Tree(hash_table *bestTree, bit_vector *joinTree){

    int position = bit_hash(joinTree) - 1;

    return bestTree->table[position];
}

void free_hashTable(hash_table *bestTree){

    if( bestTree == NULL)
        return;
    for(int i = 0; i < bestTree->table_size; i++){
        if(bestTree->table[i] != NULL){
            free(bestTree->table[i]->joinTree);
            if(bestTree->table[i]->bitVector_stats != NULL){
                free_statsList(bestTree->table[i]->bitVector_stats);
            }
        }
        free(bestTree->table[i]);
    }
    free(bestTree->table);
    free(bestTree);
    bestTree = NULL;
}

void initialize_queryRelation(query_relation **relation, int rel, int col){

    (*relation) = (query_relation *) malloc(1 * sizeof(query_relation));
    (*relation)->relation = rel;
    (*relation)->column = col;
    (*relation)->next = NULL;
}

void init_relationSet(relation_set **set){

    (*set) = malloc(1 * sizeof(relation_set));

    (*set)->set_length = 0;
    (*set)->first = NULL;
    (*set)->last = NULL;
    (*set)->next = NULL;
}

void initialize_superSet(super_set **super){

    (*super) = (super_set *) malloc(1 * sizeof(super_set));

    (*super)->number_of_sets = 0;
    (*super)->first = NULL;
    (*super)->last = NULL;
}

void add_atEnd(relation_set *set, query_relation *new_element){

    if(set->first == NULL){

        set->first = new_element;
        set->last = set->first;
    }else{

        set->last->next = new_element;
        set->last = new_element;
    }

    set->set_length++;
}

void add_lastSet(super_set *super, relation_set *new_set){
    if(super->first == NULL){

        super->first = new_set;
        super->last = new_set;
    }else{

        super->last->next = new_set;
        super->last = new_set;
    }
    super->number_of_sets++;
}

query_relation* get_first(relation_set *set){

    query_relation *tmp;

    tmp = set->first;

    if(set->first != NULL){
        set->first = set->first->next;
        set->set_length--;
    }else{
        set->first = NULL;
        set->last = NULL;
        set->set_length = 0;
    }

    return tmp;
}

relation_set * get_firstSet(super_set *super){

    relation_set * temp = super->first;

    if(temp != NULL){
        super->first = super->first->next;
        super->number_of_sets--;
        temp->next = NULL;
    }else{
        super->first = NULL;
        super->last = NULL;
        super->number_of_sets = 0;
    }
    
    return temp;
}

int element_inSet(relation_set *set, int relation, int column){

    query_relation *tmp_element = set->first;

    while((tmp_element != NULL)){
          
        if(((tmp_element->relation == relation) &&
            (tmp_element->column == column))){
        
            return 1; 
        }
        tmp_element = tmp_element->next;
    }

    return 0;
}

void free_relationSet(relation_set *set){
    if( set == NULL)
        return;
    query_relation *temp = set->first;
    query_relation *rmv = temp;
    while(temp != NULL){
        temp = temp->next;
        free(rmv);
        rmv = temp;
    }
    free(set);
    set = NULL;
}

void free_superSet(super_set *super){
    if( super == NULL)
        return;
    relation_set *temp = super->first;
    relation_set *rmv = temp;
    while(temp != NULL){
        temp = temp->next;
        free_relationSet(rmv);
        rmv = temp;
    }
    free(super);
    super = NULL;
}

void free_queryRelation(query_relation *relation){
    if( relation == NULL)
        return;

    query_relation *tmp,*rmv;

    tmp = relation;
    rmv = tmp;
    while(tmp != NULL){
        tmp = tmp->next;
        free(rmv);
        rmv = tmp;
    }
    free(relation);
    relation = NULL;
}

void print_hashTable(hash_table *hash){

    for(int i = 0; i < hash->table_size; i++){
        if(hash->table[i] != NULL){
            printf("Bit Hash Of Position %d Of Hash Table Is %d\n",i,bit_hash(hash->table[i]));
        }else{
            printf("Cell %d is NULL\n",i);
        }
    }
}

void print_superSet(super_set *super){

    relation_set *rel = super->first;
    for(int i = 0; i < super->number_of_sets; i++){
        printf("Relation %d: ",i);
        query_relation *query = rel->first;
        for(int j = 0; j < rel->set_length; j++){
            printf("Rel %d  Col %d\n",query->relation,query->column);
            query = query->next;
        }
        rel = rel->next;
    }
}

void print_relationSet(relation_set *set){

    query_relation *relation = set->first;
    for(int i = 0; i < set->set_length; i++){
        printf("Query %d, Relation %d Column %d\n",i,relation->relation,relation->column);
        relation = relation->next;
    }
}

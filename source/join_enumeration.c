#include "../headers/join_enumaration.h"

void join_enumeration(struct sql_query *query,stats_list *statistics_list){
    
    int relations_num = query->joins.length * 2, return_value;
    hash_table *bestTree;
    bit_vector *joinTree, *temp_tree, *temp_singleTree, *curr_tree, *executionTree;
    super_set *singleSuper_set, *subSuper_set;
    relation_set *relation_set, *single_set, *temp_set;
    query_relation *table, *single_table;
    query_relation data[relations_num];

    order_filters(query,statistics_list);

    init_relationSet(&relation_set);
    initialize_superSet(&singleSuper_set);

    struct predicate predicate;
    for(int i = 0; i < relations_num / 2; i++){                                 //Create A Set With All The Relations Involved In The Query
        predicate = vector_at(&(query->joins),i);                               //And A Super Set With Each Relation As A Single-Set

        init_relationSet(&single_set);
        initialize_queryRelation(&table,vector_at(&(predicate.predicate),0),vector_at(&(predicate.predicate),1));
        initialize_queryRelation(&single_table,vector_at(&(predicate.predicate),0),vector_at(&(predicate.predicate),1));    
        add_atEnd(relation_set,table);
        add_atEnd(single_set,single_table);
        add_lastSet(singleSuper_set,single_set);

        init_relationSet(&single_set);
        initialize_queryRelation(&table,vector_at(&(predicate.predicate),3),vector_at(&(predicate.predicate),4));
        initialize_queryRelation(&single_table,vector_at(&(predicate.predicate),3),vector_at(&(predicate.predicate),4));
        add_atEnd(relation_set,table);
        add_atEnd(single_set,single_table);
        add_lastSet(singleSuper_set,single_set);
    }

    initialize_hashTable(&bestTree,relations_num);

    for(int i = 1; i <= relations_num; ++i){                                    //Fill The Hash Table With JoinTrees Of Single Relations
        initialize_bitVector(&joinTree,relations_num,statistics_list);
        temp_set = get_firstSet(singleSuper_set);
        parse_set(relation_set,temp_set,joinTree);
        add_lastSet(singleSuper_set,temp_set);
        return_value = fill_hashTable(bestTree,joinTree);
        if( return_value == 1 ) {
            free_relationSet(relation_set);
            free_superSet(singleSuper_set);
            free_hashTable(bestTree);
            return;
        }
    }

    for(int i = 1; i < relations_num; i++){
        initialize_superSet(&subSuper_set);
        all_subsets(relation_set->first,subSuper_set,data,0,relations_num-1,0,i);          //Create All The SubSets Of Size i
        
        int subSuper_length = subSuper_set->number_of_sets;
        
        for(int k = 0; k < subSuper_length; k++){                             //For Each One Of The SubSets Created
            temp_set = get_firstSet(subSuper_set);
            initialize_bitVector(&temp_tree,relations_num,NULL);
            parse_set(relation_set,temp_set,temp_tree);                                    //Parse Set Into A JoinTree
            int singleSuper_length = singleSuper_set->number_of_sets;

            for(int j = 0; j < singleSuper_length; j++){                      //For Each Single Set  
                single_set = get_firstSet(singleSuper_set);
                add_lastSet(singleSuper_set,single_set);                
                initialize_bitVector(&temp_singleTree,relations_num,NULL);
                parse_set(relation_set,single_set,temp_singleTree);                        //Parse Single-Set Into A JoinTree

                if(!connected(temp_tree,temp_singleTree)){                                  //If Single-Set Does Not Exist In Set Continue
                    curr_tree = joinTree_union(find_Tree(bestTree,temp_tree),temp_singleTree);  //Union Of Best Join Tree And New SIngle Tree
                    newJoinTree_cost(curr_tree,relation_set,query);
                    
                    add_atEnd(temp_set,single_set->first);                                      //Make Union Of temp_set And single_set
                    initialize_bitVector(&joinTree,relations_num,NULL);
                    parse_set(relation_set,temp_set,joinTree);

                    if( (find_Tree(bestTree,joinTree) == NULL) || findJoinTree_cost(bestTree,find_Tree(bestTree,joinTree)) > curr_tree->cost){
                        fill_hashTable(bestTree,curr_tree);
                    }else{
                        free_bitVector(curr_tree);
                    }
                    free_bitVector(joinTree);
                }
                free_bitVector(temp_singleTree);
            }
            free_bitVector(temp_tree);
        }
        free_superSet(subSuper_set);
    }
    
    executionTree = bestTree->table[bestTree->table_size - 1];                              //Pass The Best Solution Found Into The Predicates
    for(int i = 0; i < relations_num / 2; i++){
        predicate = vector_at(&(query->joins),i);
        
        table = relation_set->first;
        for(int j = 0; j < relations_num; j++){
            if(executionTree->joinTree[j].position == (1 + (i * 2))){
                vector_set(&(predicate.predicate),0,table->relation);
                vector_set(&(predicate.predicate),1,table->column);
            }

            if(executionTree->joinTree[j].position == (2 + (i * 2))){
                vector_set(&(predicate.predicate),3,table->relation);
                vector_set(&(predicate.predicate),4,table->column);
            }
            table = table->next;
        }
    }

    free(singleSuper_set);
    free_relationSet(relation_set);
    free_relationSet(single_set);
    free_hashTable(bestTree);
}

void filter_swap(struct sql_query *sql_query, int a, int b) 
{ 
    struct predicate predicate_a, predicate_b;

    predicate_a = (vector_at(&(sql_query->filters), a));
    predicate_b = (vector_at(&(sql_query->filters), b));

    vector_set(&(sql_query->filters), b, predicate_a);
    vector_set(&(sql_query->filters), a, predicate_b);
} 

int filter_partition (struct sql_query *sql_query, int low, int high, stats_list *statistics_list,struct sql_query *query) 
{ 
    struct predicate pivot  = vector_at(&(sql_query->filters), high);
    struct predicate tmp_predicate;
    int i = (low - 1);
  
    for (int j = low; j <= high- 1; j++) 
    { 
        tmp_predicate = vector_at(&(sql_query->filters), j);
        if (filter_cost(tmp_predicate,statistics_list,query) < filter_cost(pivot,statistics_list,query)){ 
            i++; 
            filter_swap(sql_query,i,j); 
        } 
    } 
    filter_swap(sql_query, i+1, high); 
    return (i + 1); 
}

void filter_quickSort(struct sql_query * sql_query, int low, int high, stats_list *statistics_list,struct sql_query *query) 
{ 
    if (low < high) { 
        int pi = filter_partition(sql_query, low, high,statistics_list,query); 
  
        filter_quickSort(sql_query, low, pi - 1,statistics_list,query); 
        filter_quickSort(sql_query, pi + 1, high,statistics_list,query); 
    } 
} 

void order_filters(struct sql_query *query,stats_list *statistics_list){

    filter_quickSort(query,0,query->filters.length - 1,statistics_list,query);
}

long double filter_cost(struct predicate filter, stats_list *statistics_list,struct sql_query *query){

    uint64_t k;
    int relation1, column1, column2;
    char operator;

    stats_node *col_statsA, *col_statsB;
    relation_node *origin_relation;

    relation1 = vector_at(&(filter.predicate),0);
    column1 = vector_at(&(filter.predicate),1);
    operator = vector_at(&(filter.predicate),2);

    relation1 = vector_at(&(query->relations),relation1);

    origin_relation = statistics_list->head;
    for(int i = 0; i < relation1; i++){
        origin_relation = origin_relation->next;
    }

    col_statsA = origin_relation->stats;
    for(int i = 0; i < column1; i++){
        col_statsA = col_statsA->next;
    }

    if(vector_at(&(filter.predicate),4) == -1){

        k = vector_at(&(filter.predicate),3);

        switch (operator)
        {
        case '<':
            
            range_filters(col_statsA->statistics,k,'>',col_statsA->statistics->max,-1,origin_relation);
            return col_statsA->statistics->data_num;
            
            break;
        case '>':
            
            range_filters(col_statsA->statistics,col_statsA->statistics->min,-1,k,'<',origin_relation);
            return col_statsA->statistics->data_num;
            
            break;

        case '=':

            equal_filter(col_statsA->statistics,k,origin_relation);
            return col_statsA->statistics->data_num;
            
            break;   
        default:
            break;
        }
    }else{

        column2 = vector_at(&(filter.predicate),4);

        col_statsB = origin_relation->stats;
        for(int i = 0; i < column2; i++){
            col_statsB = col_statsB->next;
        }

        column_filter(col_statsA->statistics,col_statsB->statistics,origin_relation);
        return col_statsA->statistics->data_num;
    }

    return -1.0;
}

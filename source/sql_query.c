#include "../headers/sql_query.h"
#include "../headers/predicate_freq.h"

struct sql_query *initialize_sql_query() {
    struct sql_query *new_sql_query = (struct sql_query *)malloc(sizeof(struct sql_query));
    if( new_sql_query == NULL ) {
        perror("initialize_sql_query failed");
        return NULL;
    }

    vector_inititialize(&(new_sql_query->relations));
    vector_inititialize(&(new_sql_query->predicates));
    vector_inititialize(&(new_sql_query->filters));
    vector_inititialize(&(new_sql_query->joins));
    vector_inititialize(&(new_sql_query->projections));

    return new_sql_query;
}

void split_sql_query(struct sql_query *sql_query, char *query) {
    char *relations, *predicates, *projections;

    // split query string
    relations = strtok_r(query, "|", &query);
    predicates = strtok_r(query, "|", &query);
    projections = strtok_r(query, "|", &query);

    // fix queries
    parse_relation_query(sql_query,relations);
    parse_predicate_query(sql_query,predicates);
    parse_projection_query(sql_query,projections);
}

void print_sql_query(struct sql_query *sql_query) {
    print_relation_query(sql_query);
    print_predicate_query(sql_query);
    print_projection_query(sql_query);
}

void free_sql_query(struct sql_query **sql_query) {
    if( (*sql_query) == NULL )
        return;
    vector_clear(&((*sql_query)->relations));

    struct predicate predicate;
    for ( int i = 0; i < (*sql_query)->predicates.length ; i++ ) {
        predicate = vector_at(&((*sql_query)->predicates), i);
        vector_clear(&(predicate.predicate));
    }
    vector_clear(&((*sql_query)->predicates));
    vector_clear(&((*sql_query)->filters));
    vector_clear(&((*sql_query)->joins));


    struct projection projection;
    for ( int i = 0; i < (*sql_query)->projections.length ; i++ ) {
        projection = vector_at(&((*sql_query)->projections), i);
        vector_clear(&(projection.projection));
    }
    vector_clear(&((*sql_query)->projections));

    free(*sql_query);
}

void parse_relation_query(struct sql_query * sql_query, char * relation_string) {
    char *token;
    while ( (token = strtok_r(relation_string, " ", &relation_string)) != NULL ) {
        vector_push_back(&(sql_query->relations),atoi(token));
    }
}

void print_relation_query(struct sql_query * sql_query) {
    printf("Relations:\n");
    for ( int i = 0; i < sql_query->relations.length ; i++ )
        printf("%d\n", vector_at(&(sql_query->relations), i));
}

// operators: 1) '=' -> 0, 2) '>' -> 1 , 3) '<' -> 2
// if the predicate is 0.0=1.1, we will have    |0,0,0,0,0| |0,0,0,0,0|     |0,0,0,0,0|
// if next predicate is 0.1>30 we will have                 |0,1,1,30,-1|   |0,1,1,30,-1|
// if next predicate is 1.2<40 we will have                                 |1,2,2,40,-1|
void parse_predicate_query(struct sql_query * sql_query, char * predicate_string) {
    char *token_1, *token_2;
    int row_1, column_1, row_2, column_2, i, operator_int = 0;
    char operator[]=" ";
    while ( (token_1 = strtok_r(predicate_string, "&", &predicate_string)) != NULL ) {
        i = 0;
        while( token_1[i] != '\0' ) {
            if( token_1[i] == '=' ) {
                operator[0] = token_1[i];
                operator_int = EQUAL;
                break;
            }
            else if( token_1[i] == '>' ) {
                operator[0] = token_1[i];
                operator_int = GREATER;
                break;
            }
            else if( token_1[i] == '<' ) {
                operator[0] = token_1[i];
                operator_int = LESS;
                break;
            }
            i++;
        }
        token_2 = strtok_r(token_1, ".", &token_1);
        row_1 = atoi(token_2);
        token_2 = strtok_r(token_1, operator, &token_1);
        column_1 = atoi(token_2);
        token_2 = strtok_r(token_1, ".", &token_1);
        row_2 = atoi(token_2);
        token_2 = strtok_r(token_1, "", &token_1);
        // if token_2 is NULL it means that we have a filter, so the column_2 will be -1
        if( token_2 == NULL )
            column_2 = -1;
        else
            column_2 = atoi(token_2);

        struct predicate predicate;
        vector_inititialize(&(predicate.predicate));
        vector_push_back(&(predicate.predicate),row_1);
        vector_push_back(&(predicate.predicate),column_1);
        vector_push_back(&(predicate.predicate),operator_int);
        vector_push_back(&(predicate.predicate),row_2);
        vector_push_back(&(predicate.predicate),column_2);
        
        if( (column_2 == -1) || (row_1 == row_2) )
            vector_push_back(&(sql_query->filters),((struct predicate){.predicate=predicate.predicate})); 
        else
            vector_push_back(&(sql_query->joins),((struct predicate){.predicate=predicate.predicate})); 
        vector_push_back(&(sql_query->predicates),((struct predicate){.predicate=predicate.predicate})); 
    }
}

void print_predicate_query(struct sql_query * sql_query) {
    struct predicate predicate;
    printf("Predicates:\n");
    for ( int i = 0; i < sql_query->predicates.length ; i++ ) {
        predicate = vector_at(&(sql_query->predicates), i);
        printf("%d %d %d %d %d\n",  vector_at(&(predicate.predicate), ROW_A), vector_at(&(predicate.predicate), COLUMN_A),
                                    vector_at(&(predicate.predicate), OPERATOR), vector_at(&(predicate.predicate), ROW_B),
                                    vector_at(&(predicate.predicate), COLUMN_B) );
    }
    printf("Filters:\n");
    for ( int i = 0; i < sql_query->filters.length ; i++ ) {
        predicate = vector_at(&(sql_query->filters), i);
        printf("%d %d %d %d %d\n",  vector_at(&(predicate.predicate), ROW_A), vector_at(&(predicate.predicate), COLUMN_A),
                                    vector_at(&(predicate.predicate), OPERATOR), vector_at(&(predicate.predicate), ROW_B),
                                    vector_at(&(predicate.predicate), COLUMN_B) );
    }
    printf("Joins:\n");
    for ( int i = 0; i < sql_query->joins.length ; i++ ) {
        predicate = vector_at(&(sql_query->joins), i);
        printf("%d %d %d %d %d\n",  vector_at(&(predicate.predicate), ROW_A), vector_at(&(predicate.predicate), COLUMN_A),
                                    vector_at(&(predicate.predicate), OPERATOR), vector_at(&(predicate.predicate), ROW_B),
                                    vector_at(&(predicate.predicate), COLUMN_B) );
    }
}

// if the projection is 0.0, we will have   [0,0]   |0 0|
// if next projection is 1.2 we will have           |1 2|
void parse_projection_query(struct sql_query * sql_query, char * projection_string) {
    char *token_1, *token_2;
    int row, column;
    while ( (token_1 = strtok_r(projection_string, " ", &projection_string)) != NULL ) {
        token_2 = strtok_r(token_1, ".", &token_1);
        row = atoi(token_2);
        token_2 = strtok_r(token_1, ".", &token_1);
        column = atoi(token_2);
        struct projection projection;
        vector_inititialize(&(projection.projection));
        vector_push_back(&(projection.projection),row);
        vector_push_back(&(projection.projection),column);
        vector_push_back(&(sql_query->projections),((struct projection){.projection=projection.projection}));
    }
}

void print_projection_query(struct sql_query * sql_query) {
    struct projection projection;
    printf("Projections:\n");
    for ( int i = 0; i < sql_query->projections.length ; i++ ) {
        projection = vector_at(&(sql_query->projections), i);
        printf("%d %d\n", vector_at(&(projection.projection), 0),vector_at(&(projection.projection), 1));
    }
}

void swap(struct sql_query *sql_query, int a, int b) 
{ 
    struct predicate predicate_a, predicate_b;

    predicate_a = (vector_at(&(sql_query->predicates), a));
    predicate_b = (vector_at(&(sql_query->predicates), b));

    vector_set(&(sql_query->predicates), b, predicate_a);
    vector_set(&(sql_query->predicates), a, predicate_b);
} 

int partition_sql (struct sql_query *sql_query, int low, int high) 
{ 
    struct predicate pivot  = vector_at(&(sql_query->predicates), high);
    struct predicate tmp_predicate;
    int i = (low - 1);
  
    for (int j = low; j <= high- 1; j++) 
    { 
        tmp_predicate = vector_at(&(sql_query->predicates), j);
        if ((vector_at(&(tmp_predicate.predicate), 0) <= (vector_at(&(pivot.predicate), 0))) &&
             (vector_at(&(tmp_predicate.predicate), 1) <= (vector_at(&(pivot.predicate), 1))))
        { 
            i++; 
            swap(sql_query,i,j); 
        } 
    } 
    swap(sql_query, i+1, high); 
    return (i + 1); 
}

void quickSort_sql(struct sql_query * sql_query, int low, int high) 
{ 
    if (low < high) 
    { 
        int pi = partition_sql(sql_query, low, high); 
  
        quickSort_sql(sql_query, low, pi - 1); 
        quickSort_sql(sql_query, pi + 1, high); 
    } 
} 

int sort_optimization(struct sql_query *sql_query){

    int temp_rel;
    int temp_col;
    int i;
    struct predicate predicate;
    
    //Bring All Filters In The Front
    int non_filter_predicate = 0;
    for(i = 0; i < sql_query->predicates.length; i++){
        predicate = vector_at(&(sql_query->predicates), i);
        if((vector_at(&(predicate.predicate), 4) == -1) ||
            ((vector_at(&(predicate.predicate), 0) == vector_at(&(predicate.predicate), 3)))){
            swap(sql_query,non_filter_predicate,i);
            non_filter_predicate++;
        }
    }

    //Order Each Pair Of Predicates And Check Where Filters Begin
    for(i = non_filter_predicate; i < sql_query->predicates.length; i++){
        predicate = vector_at(&(sql_query->predicates), i);
        if((vector_at(&(predicate.predicate), 0) > vector_at(&(predicate.predicate), 3)) ||
            (vector_at(&(predicate.predicate), 1) > vector_at(&(predicate.predicate), 4))){
            temp_rel = vector_at(&(predicate.predicate), 0);
            temp_col = vector_at(&(predicate.predicate), 1);

            vector_set(&(predicate.predicate), 0, vector_at(&(predicate.predicate), 3));
            vector_set(&(predicate.predicate), 1, vector_at(&(predicate.predicate), 4));

            vector_set(&(predicate.predicate), 3, temp_rel);
            vector_set(&(predicate.predicate), 4, temp_col);
        }
    }

    //Sort All Sets Of Predicates
    quickSort_sql(sql_query, non_filter_predicate, sql_query->predicates.length - 1);

    return non_filter_predicate;
}

int frequency_optimization(struct sql_query *sql_query){

    int temp_rel;
    int temp_col;
    int i;
    struct predicate predicate;
    
    frequency_list * freq_list;

    init_freqList(&freq_list);

    //Bring All Filters At Front
    int non_filter_predicate = 0;
    for(i = 0; i < sql_query->predicates.length; i++){
        predicate = vector_at(&(sql_query->predicates), i);
        if((vector_at(&(predicate.predicate), 4) == -1) ||
            ((vector_at(&(predicate.predicate), 0) == vector_at(&(predicate.predicate), 3)))){
            swap(sql_query,non_filter_predicate,i);
            non_filter_predicate++;
        }
    }

    int left_relation;
    int left_column;
    int right_relation;
    int right_column;

    pred_freq *temp_element;
    //Initialize The Values Of The Struct
    for(i = non_filter_predicate; i < sql_query->predicates.length; i++){
        predicate = vector_at(&(sql_query->predicates), i);

        left_relation = vector_at(&(predicate.predicate), 0);
        left_column = vector_at(&(predicate.predicate), 1);

        right_relation = vector_at(&(predicate.predicate), 3);
        right_column = vector_at(&(predicate.predicate), 4);
       
        if((temp_element = element_exists(freq_list, left_relation, left_column)) != NULL){
            temp_element->frequency++;
        }else{

            temp_element = malloc(1 * sizeof(pred_freq));
            temp_element->relation = left_relation;
            temp_element->column = left_column;
            temp_element->frequency = 1;
            temp_element->times_swaped = 0;
            temp_element->next = NULL;

            add_last(freq_list,temp_element);
        }

        if((temp_element = element_exists(freq_list, right_relation, right_column)) != NULL){

            temp_element->frequency++;
        }else{

            temp_element = malloc(1 * sizeof(pred_freq));
            temp_element->relation = right_relation;
            temp_element->column = right_column;
            temp_element->frequency = 1;
            temp_element->times_swaped = 0;
            temp_element->next = NULL;

            add_last(freq_list,temp_element);
        }
    }

    //Sort Predicates By Frequency
    pred_freq *max_rel = max_element(freq_list);
    pred_freq *pair_pred;
    int swap_position = non_filter_predicate;
    while(max_rel!= NULL){
        
        for(i = swap_position; i < sql_query->predicates.length; i++){
            predicate = vector_at(&(sql_query->predicates), i);

            if(((vector_at(&(predicate.predicate), 0) == max_rel->relation)) &&
                ((vector_at(&(predicate.predicate), 1) == max_rel->column)) &&
                  max_rel->times_swaped < max_rel->frequency){

                max_rel->times_swaped++;
                pair_pred = element_exists(freq_list,(vector_at(&(predicate.predicate), 3)),(vector_at(&(predicate.predicate), 4)));
                pair_pred->times_swaped++;
                
                swap(sql_query, swap_position, i);
                swap_position++;
            }
            if(((vector_at(&(predicate.predicate), 3) == max_rel->relation)) &&
                ((vector_at(&(predicate.predicate), 4) == max_rel->column)) &&
                  max_rel->times_swaped < max_rel->frequency){

                max_rel->times_swaped++;
                pair_pred = element_exists(freq_list,(vector_at(&(predicate.predicate), 0)),(vector_at(&(predicate.predicate), 1)));
                pair_pred->times_swaped++;

                temp_rel = vector_at(&(predicate.predicate), 0);
                temp_col = vector_at(&(predicate.predicate), 1);

                vector_set(&(predicate.predicate), 0, vector_at(&(predicate.predicate), 3));
                vector_set(&(predicate.predicate), 1, vector_at(&(predicate.predicate), 4));

                vector_set(&(predicate.predicate), 3, temp_rel);
                vector_set(&(predicate.predicate), 4, temp_col);

                swap(sql_query, swap_position, i);

                swap_position++;
            }

        }

        max_rel->frequency = 0;
        max_rel = max_element(freq_list);
    }

    free_freqList(freq_list);

    return non_filter_predicate;
}

int bringUP_filters(struct sql_query *sql_query){

    struct predicate predicate;

    int non_filter_predicate = 0;
    for(int i = 0; i < sql_query->predicates.length; i++){
        predicate = vector_at(&(sql_query->predicates), i);
        if((vector_at(&(predicate.predicate), 4) == -1) ||
            ((vector_at(&(predicate.predicate), 0) == vector_at(&(predicate.predicate), 3)))){
            swap(sql_query,non_filter_predicate,i);
            non_filter_predicate++;
        }
    }

    return non_filter_predicate;
}
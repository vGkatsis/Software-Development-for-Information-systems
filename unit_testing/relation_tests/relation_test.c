#include "relation_test.h"

void initialize_relation_test(){
    int number_of_tuples = 5;
    Relation *R = initialize_relation(number_of_tuples);

    CU_ASSERT_PTR_NOT_NULL(R);
    
    for( int i = 0 ; i < R->num_tuples ; i++ ) {
         CU_ASSERT(R->tuples[i].row_id == i);
    }

    free_relation(&R);
}

void parallel_join_test(){
    struct relation *R = initialize_relation(4);
    struct relation *S = initialize_relation(3);
    struct list *list_results = initialize_list();

    change_value(R,0,1);
    change_value(R,1,2);
    change_value(R,2,3);
    change_value(R,3,4);

    change_value(S,0,1);
    change_value(S,1,1);
    change_value(S,2,2);

    parallel_join(R, S, &list_results);
    
    //    Result:
    //    { 0 , 0 }
    //    { 0 , 1 }
    //    { 1 , 2 }

    CU_ASSERT(list_results->head->tuples[0].row_key_1 == 0);
    CU_ASSERT(list_results->head->tuples[0].row_key_2 == 0);
    CU_ASSERT(list_results->head->tuples[1].row_key_1 == 0);
    CU_ASSERT(list_results->head->tuples[1].row_key_2 == 1);
    CU_ASSERT(list_results->head->tuples[2].row_key_1 == 1);
    CU_ASSERT(list_results->head->tuples[2].row_key_2 == 2);

    free_list(&list_results);
    free_relation(&R);
    free_relation(&S);
}

#include "list_test.h"

void initialize_list_test(){

    struct list *list = initialize_list();

    CU_ASSERT_PTR_NOT_NULL(list);
    CU_ASSERT_PTR_NULL(list->head);
    CU_ASSERT_PTR_NULL(list->tail);
    CU_ASSERT(list->number_of_buckets == 0);

    free_list(&list);
}

void initialize_bucket_test(){

    struct bucket *bucket = initialize_bucket();

    CU_ASSERT_PTR_NOT_NULL(bucket);
    CU_ASSERT_PTR_NOT_NULL(bucket->tuples);
    CU_ASSERT(bucket->max_size == NUMBER_OF_TUPLES_IN_BUCKET);
    CU_ASSERT(bucket->current_size == 0);
    CU_ASSERT_PTR_NULL(bucket->next_bucket);

    free(bucket->tuples);
    free(bucket);
}

void insert_tuple_test(){

    struct list *list = initialize_list();

    insert_tuple(&list,1,2);
    CU_ASSERT(list->head->tuples[0].row_key_1 == 1);
    CU_ASSERT(list->head->tuples[0].row_key_2 == 2);

    free_list(&list);
}
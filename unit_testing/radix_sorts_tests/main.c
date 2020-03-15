#include "../../headers/header.h"
#include "radix_sort_test.h"

int main(void){

    if(CU_initialize_registry() != CUE_SUCCESS){
        return CU_get_error();
    }

    CU_pSuite pSuite = NULL;

    pSuite = CU_add_suite("radix_sort_test_suite", 0, 0);

    if(pSuite == NULL){
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "binary_mask_test", binary_mask_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "create_histogram_test", create_histogram_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "create_prefix_sum_test", create_prefix_sum_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "fill_new_relation_test", fill_new_relation_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "swap_tuples_test", swap_tuples_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "quick_sort_test", quick_sort_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "copy_back_to_result_relation_test", copy_back_to_result_relation_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "sort_recursive_test", sort_recursive_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "sort_iterative_test", sort_iterative_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "sort_test", sort_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);

    CU_basic_run_tests();
   
    CU_cleanup_registry();

    return CU_get_error();
}
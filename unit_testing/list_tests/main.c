#include "../../headers/header.h"
#include "list_test.h"

int main(void){

    if(CU_initialize_registry() != CUE_SUCCESS){
        return CU_get_error();
    }

    CU_pSuite pSuite = NULL;

    pSuite = CU_add_suite("list_test_suite", 0, 0);

    if(pSuite == NULL){
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "initialize_list_test", initialize_list_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "initialize_bucket_test", initialize_bucket_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "insert_tuple_test", insert_tuple_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);

    CU_basic_run_tests();
   
    CU_cleanup_registry();

    return CU_get_error();
}
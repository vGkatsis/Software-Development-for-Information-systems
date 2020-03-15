#include "../../headers/header.h"
#include "relation_test.h"

int main(void){

    if(CU_initialize_registry() != CUE_SUCCESS){
        return CU_get_error();
    }

    CU_pSuite pSuite = NULL;

    pSuite = CU_add_suite("relation_test_suite", 0, 0);

    if(pSuite == NULL){
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "initialize_relation_test", initialize_relation_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    if (NULL == CU_add_test(pSuite, "parallel_join_test", parallel_join_test)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    CU_basic_set_mode(CU_BRM_VERBOSE);

    CU_basic_run_tests();

    CU_cleanup_registry();

    return CU_get_error();
}
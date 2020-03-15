#include "radix_sort_test.h"

void binary_mask_test(){                                            //Check First And Last Byte For A Few Different Numbers

    CU_ASSERT(binary_mask(0,0) == 0);

    CU_ASSERT(binary_mask(0,1) == 0);
    CU_ASSERT(binary_mask(0,8) == 0);

    CU_ASSERT(binary_mask(30000,1) == 0);
    CU_ASSERT(binary_mask(30000,8) == 48);

    CU_ASSERT(binary_mask(564123,1) == 0);
    CU_ASSERT(binary_mask(564123,8) == 155);

    CU_ASSERT(binary_mask(12345678987654321,1) == 0);
    CU_ASSERT(binary_mask(12345678987654321,8) == 177);
}

void create_histogram_test(){

    Relation *r;

    r = initialize_relation(5);
    change_value(r,0,3);
    change_value(r,1,6);
    change_value(r,2,3);
    change_value(r,3,10);
    change_value(r,4,1);

	struct histogram_indexing *histogram_indexes=calloc(BUCKET_SIZE,sizeof(struct histogram_indexing));
    uint64_t *histogram = create_histogram(r,0,5,8,histogram_indexes);

    CU_ASSERT(histogram[1] == 1);
    CU_ASSERT(histogram[3] == 2);
    CU_ASSERT(histogram[4] == 0);
    CU_ASSERT(histogram[6] == 1);
    CU_ASSERT(histogram[10] == 1);

    free_relation(&r);
    free(histogram);
}

void create_prefix_sum_test(){

     Relation *r;

    r = initialize_relation(5);
    change_value(r,0,3);
    change_value(r,1,6);
    change_value(r,2,3);
    change_value(r,3,10);
    change_value(r,4,1);

	struct histogram_indexing *histogram_indexes=calloc(BUCKET_SIZE,sizeof(struct histogram_indexing));
    uint64_t *histogram = create_histogram(r,0,5,8,histogram_indexes);

 
    uint64_t *prefix_sum = create_prefix_sum(histogram,0);

    CU_ASSERT(prefix_sum[1] == 0);
    CU_ASSERT(prefix_sum[3] == 1);
    CU_ASSERT(prefix_sum[6] == 3);
    CU_ASSERT(prefix_sum[10] == 4);

    free_relation(&r);
    free(histogram);
    free(prefix_sum);
}

void copy_back_to_result_relation_test(){

    Relation *R, *R_new;

    R = initialize_relation(5);
    R_new = initialize_relation(5);

    for(int i = 0; i < 5; i++){
        change_value(R,i,i+1);
    }

    copy_back_to_result_relation(R,R_new,0,5);

    for(int j = 0; j < 5; j++){
        CU_ASSERT(get_value(R,j) == get_value(R_new,j));
    }

    free_relation(&R);
    free_relation(&R_new);
}

void fill_new_relation_test(){

    Relation *r,*r_new;
    uint64_t counter[BUCKET_SIZE]={0};
	struct histogram_indexing *histogram_indexes=calloc(BUCKET_SIZE,sizeof(struct histogram_indexing));
    r = initialize_relation(5);
    r_new = initialize_relation(5);

    change_value(r,0,3);
    change_value(r,1,6);
    change_value(r,2,3);
    change_value(r,3,10);
    change_value(r,4,1);

    uint64_t *histogram = create_histogram(r,0,5,8,histogram_indexes);

    uint64_t *prefix_sum = create_prefix_sum(histogram,0);

    fill_new_relation(r,r_new,prefix_sum,0,5,8);

    CU_ASSERT(r_new->tuples[1].value == r->tuples[0].value);
    CU_ASSERT(r_new->tuples[1].row_id == r->tuples[0].row_id);

    CU_ASSERT(r_new->tuples[3].value == r->tuples[1].value);
    CU_ASSERT(r_new->tuples[3].row_id == r->tuples[1].row_id);

    CU_ASSERT(r_new->tuples[2].value == r->tuples[2].value);
    CU_ASSERT(r_new->tuples[2].row_id == r->tuples[2].row_id);

    CU_ASSERT(r_new->tuples[4].value == r->tuples[3].value);
    CU_ASSERT(r_new->tuples[4].row_id == r->tuples[3].row_id);

    CU_ASSERT(r_new->tuples[0].value == r->tuples[4].value);
    CU_ASSERT(r_new->tuples[0].row_id == r->tuples[4].row_id);

    free_relation(&r);
    free_relation(&r_new);
    free(histogram);
    free(prefix_sum);
}

void swap_tuples_test(){                                                //Check If Values Of Tuples Are Swaped

    Relation *r;

    r = initialize_relation(2);
    change_value(r,0,1);
    change_value(r,1,2);

    swap_tuples(&r,0,1);

    CU_ASSERT(get_value(r,0) == 2);
    CU_ASSERT(get_value(r,1) == 1);

    CU_ASSERT(get_rowId(r,0) == 1);
    CU_ASSERT(get_rowId(r,1) == 0);

    free_relation(&r);
}

void quick_sort_test(){

    Relation *r;

    r = initialize_relation(5);
    change_value(r,0,3);
    change_value(r,1,6);
    change_value(r,2,4);
    change_value(r,3,10);
    change_value(r,4,1);

    quick_sort(&r,0,4);

    CU_ASSERT(get_value(r,0) == 1);
    CU_ASSERT(get_value(r,1) == 3);
    CU_ASSERT(get_value(r,2) == 4);
    CU_ASSERT(get_value(r,3) == 6);
    CU_ASSERT(get_value(r,4) == 10);

    free_relation(&r);
}

void sort_recursive_test(){

    Relation *R;
    uint64_t value, temp_value, num_tuples;

    R = initialize_relation_with_dataset("../../Datasets/medium/relB");
    
    Relation* R_new = initialize_relation(R->num_tuples);

    CU_ASSERT_PTR_NOT_NULL_FATAL(R);
    CU_ASSERT_PTR_NOT_NULL_FATAL(R_new);

    num_tuples = R->num_tuples;

    sort_recursive(R,R_new,0,R->num_tuples,1);

    value = get_value(R,0);

    for(int i = 1; i < num_tuples; i++){

        CU_ASSERT_FATAL(get_value(R,i) >= value);
    
        value = get_value(R,i);
    }

    free_relation(&R);
    free_relation(&R_new);
}

void sort_iterative_test(){

    Relation *R;
    uint64_t value, temp_value, num_tuples;

    R = initialize_relation_with_dataset("../../Datasets/medium/relB");
    
    Relation* R_new = initialize_relation(R->num_tuples);

    CU_ASSERT_PTR_NOT_NULL_FATAL(R);
    CU_ASSERT_PTR_NOT_NULL_FATAL(R_new);

    num_tuples = R->num_tuples;

    sort_iterative(R,R_new,0,R->num_tuples,1);

    value = get_value(R,0);

    for(int i = 1; i < num_tuples; i++){

        CU_ASSERT_FATAL(get_value(R,i) >= value);
    
        value = get_value(R,i);
    }

    free_relation(&R);
    free_relation(&R_new);
}

void sort_test(){

    Relation *R,*S;
    uint64_t value, temp_value, num_tuples;

    R = initialize_relation_with_dataset("../../Datasets/medium/relB");
    S = initialize_relation_with_dataset("../../Datasets/medium/relA");
    
    CU_ASSERT_PTR_NOT_NULL_FATAL(R);
    CU_ASSERT_PTR_NOT_NULL_FATAL(S);

    num_tuples = R->num_tuples;

    sort(R, RECURSIVE);

    value = get_value(R,0);

    for(int i = 1; i < num_tuples; i++){

        CU_ASSERT_FATAL(get_value(R,i) >= value);
    
        value = get_value(R,i);
    }
    
    num_tuples = S->num_tuples;

    sort(S, ITERATIVE);
    
    value = get_value(S,0);
    
    for(int j = 1; j < num_tuples; j++){

        CU_ASSERT_FATAL(get_value(S,j) >= value);
    
        value = get_value(S,j);
    }
    
    free_relation(&R);
    free_relation(&S);
}

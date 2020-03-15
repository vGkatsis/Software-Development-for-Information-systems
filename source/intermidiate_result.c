#include "../headers/intermidiate_result.h"

#define RESULTS_ROWS 15
#define RESULTS_COLUMNS 3

struct relation *create_relation_from_file(struct file file, int column) {
    struct relation *new_relation = (struct relation *)malloc(sizeof(struct relation));
    if( new_relation == NULL) {
        perror("create_relation_from_file failed");
        return NULL;
    }

    new_relation->tuples = (struct tuple *)malloc(file.number_of_rows*sizeof(struct tuple));
    if( new_relation->tuples == NULL) {
        perror("create_relation_from_file failed");
        return NULL;
    }

    for(int i = 0 ; i < file.number_of_rows ; i++) {
        new_relation->tuples[i].row_id = i;
        new_relation->tuples[i].value = file.array[column*file.number_of_rows + i];
    }

    new_relation->num_tuples = file.number_of_rows;

    return new_relation;
}

struct relation *create_relation_from_intermidiate_results_for_join(struct file file, struct intermidiate_results intermidiate_results, int intermidiate_result_index, int column) {
    struct relation *new_relation = (struct relation *)malloc(sizeof(struct relation));
    if( new_relation == NULL) {
        perror("create_relation_from_intermidiate_results failed");
        return NULL;
    }
    
    struct intermidiate_result intermidiate_result = vector_at(&(intermidiate_results.results),intermidiate_result_index);
    
    new_relation->tuples = (struct tuple *)malloc(intermidiate_result.row_ids.length*sizeof(struct tuple));
    if( new_relation->tuples == NULL) {
        perror("create_relation_from_intermidiate_results failed");
        return NULL;
    }
    
    for(int i = 0 ; i < intermidiate_result.row_ids.length ; i++) {
        new_relation->tuples[i].row_id = i;
        new_relation->tuples[i].value = file.array[column*file.number_of_rows+intermidiate_result.row_ids.data[i]];
    }

    new_relation->num_tuples = intermidiate_result.row_ids.length;
    
    return new_relation;
}

struct list * sort_join_calculation(struct relation **R, struct relation **S, struct intermidiate_results intermidiate_results, int * predicate) {
    struct list *list_results = initialize_list();
    bool flag_r = false, flag_s = false;
    for( int i = 0 ; i < 2 ; i++ ) {
        if((intermidiate_results.sorted_relations[i] == predicate[ROW_A]) && (intermidiate_results.sorted_relation_columns[i] == predicate[COLUMN_A])) {
            flag_r = true;
            break;
        }
    }
    for( int i = 0 ; i < 2 ; i++ ) {
        if((intermidiate_results.sorted_relations[i] == predicate[ROW_B]) && (intermidiate_results.sorted_relation_columns[i] == predicate[COLUMN_B])) {
            flag_s = true;
            break;
        }
    }

    struct join_partition *join_partition_R = (struct join_partition *)malloc(sizeof(struct join_partition));
    struct join_partition *join_partition_S = (struct join_partition *)malloc(sizeof(struct join_partition));
    int job_barrier=0;
    
    if( flag_r == false ) {
        //struct sort_args *args=malloc(sizeof(struct sort_args));
        struct sort_args args={*R,NULL,join_partition_R};
        schedule_job_scheduler((void*)sort_multithread, &args,sizeof(struct sort_args), &job_barrier);
    }
    else {
        // calculate join partition for relation R
        join_partition_R->histogram_indexes.size = 0;
        join_partition_R->histogram = create_histogram_multithread((*R),0,(*R)->num_tuples,START_BYTE,&(join_partition_R->histogram_indexes));
        join_partition_R->prefix_sum = create_prefix_sum(join_partition_R->histogram,0);
    }

    if( flag_s == false ) {
        //struct sort_args *args=malloc(sizeof(struct sort_args));
        struct sort_args args={*S,NULL,join_partition_S};
        schedule_job_scheduler((void*)sort_multithread, &args, sizeof(struct sort_args), &job_barrier);
    }
    else {
        // calculate join partition for relation S
        join_partition_S->histogram_indexes.size = 0;
        join_partition_S->histogram = create_histogram_multithread((*S),0,(*S)->num_tuples,START_BYTE,&(join_partition_S->histogram_indexes));
        join_partition_S->prefix_sum = create_prefix_sum(join_partition_S->histogram,0);
    }

    dynamic_barrier_job_scheduler(&job_barrier);

    break_join_to_jobs(R,S,list_results,join_partition_R,join_partition_S);

    free(join_partition_R->histogram);
    free(join_partition_R->prefix_sum);
    free(join_partition_R);
    free(join_partition_S->histogram);
    free(join_partition_S->prefix_sum);
    free(join_partition_S);

    return list_results;
}

void initialize_intermidiate_result(struct intermidiate_result *intermidiate_result, uint64_t file_index, uint64_t predicate_relation, char field, uint64_t expand_size) {
    vector_inititialize(&(intermidiate_result->row_ids));
    vector_expand_with_size(&(intermidiate_result->row_ids),expand_size);
    intermidiate_result->file_index = file_index;
    intermidiate_result->predicate_relation = predicate_relation;
    intermidiate_result->field = field;
}

bool none_in_mid_results(struct file_array file_array, struct intermidiate_results *intermidiate_results, int * predicate, int * relations) {
    // create and initialize new intermidiate result relations
    struct intermidiate_result new_intermidiate_result_a, new_intermidiate_result_b;
    struct relation *R = NULL, *S = NULL;
    struct list *list_results = NULL;
    uint64_t file_index_a = relations[predicate[ROW_A]], file_index_b = relations[predicate[ROW_B]];
    uint64_t predicate_relation_a = predicate[ROW_A], predicate_relation_b = predicate[ROW_B];
    uint64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file file_a = vector_at(&file_array.files,file_index_a), file_b = vector_at(&file_array.files,file_index_b);
    
    // initialize structures that we need for parallel join
    R = create_relation_from_file(file_a, column_a);
    S = create_relation_from_file(file_b, column_b);
    list_results = sort_join_calculation(&R,&S,*intermidiate_results,predicate);

    // if there was no results then free structures and return false
    if( list_results->total_size == 0 ) {
        free_list(&list_results);
        free_relation(&R);
        free_relation(&S);
        return false;
    }

    initialize_intermidiate_result(&new_intermidiate_result_a,file_index_a,predicate_relation_a,'j',list_results->total_size);
    initialize_intermidiate_result(&new_intermidiate_result_b,file_index_b,predicate_relation_b,'j',list_results->total_size);
    // fix new intermidiate result relations
    struct bucket * temp_bucket = list_results->head;
    for( int i = 0 ; i < list_results->number_of_buckets ; i++) {
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
            vector_push_back(&(new_intermidiate_result_a.row_ids),temp_bucket->tuples[j].row_key_1);
            vector_push_back(&(new_intermidiate_result_b.row_ids),temp_bucket->tuples[j].row_key_2);
        }
        temp_bucket = temp_bucket->next_bucket;
    }
    // push them to intermidiate results
    vector_push_back(&(intermidiate_results->results),new_intermidiate_result_a);
    vector_push_back(&(intermidiate_results->results),new_intermidiate_result_b);

    // free structures that were used for the parallel join
    free_list(&list_results);
    free_relation(&R);
    free_relation(&S);
    return true;
}

bool only_one_relation_in_mid_results(struct file_array file_array, struct intermidiate_results *intermidiate_results, int * predicate, int * relations, int intermidiate_result_index_a) {
    // create and initialize new intermidiate result relations
    struct intermidiate_result new_intermidiate_result_a, new_intermidiate_result_b;
    struct relation *R = NULL, *S = NULL;
    struct list *list_results = NULL;
    uint64_t file_index_a = relations[predicate[ROW_A]], file_index_b = relations[predicate[ROW_B]];
    uint64_t predicate_relation_a = predicate[ROW_A], predicate_relation_b = predicate[ROW_B];
    uint64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file file_a = vector_at(&file_array.files,file_index_a), file_b = vector_at(&file_array.files,file_index_b);

    // initialize structures that we need for parallel join
    R = create_relation_from_intermidiate_results_for_join(file_a,*intermidiate_results,intermidiate_result_index_a,column_a);
    S = create_relation_from_file(file_b, column_b);
    list_results = sort_join_calculation(&R,&S,*intermidiate_results,predicate);

    // if there was no results then free structures and return false
    if( list_results->total_size == 0 ) {
        free_list(&list_results);
        free_relation(&R);
        free_relation(&S);
        return false;
    }

    // get the relation that already exists in intermidiate results
    struct intermidiate_result relation_in_intermidiate_results = vector_at(&(intermidiate_results->results),intermidiate_result_index_a);

    initialize_intermidiate_result(&new_intermidiate_result_a,file_index_a,predicate_relation_a,'j',list_results->total_size);
    initialize_intermidiate_result(&new_intermidiate_result_b,file_index_b,predicate_relation_b,'j',list_results->total_size);
    // fix new intermidiate result relations
    struct bucket * temp_bucket = list_results->head;
    for( int i = 0 ; i < list_results->number_of_buckets ; i++) {
        for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
            int index_a = temp_bucket->tuples[j].row_key_1;
            int value = vector_at(&(relation_in_intermidiate_results.row_ids),index_a);
            vector_push_back(&(new_intermidiate_result_a.row_ids),value);
            vector_push_back(&(new_intermidiate_result_b.row_ids),temp_bucket->tuples[j].row_key_2);
        }
        temp_bucket = temp_bucket->next_bucket;
    }
    // synchronize old joined relations
    synchronize_intermidiate_results(intermidiate_results,list_results,intermidiate_result_index_a);
    // delete old relation that we just joined
    vector_clear(&(relation_in_intermidiate_results.row_ids));
    vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_a+1);
    // insert the new joined relation, which was in intermidiate results
    vector_push_back(&(intermidiate_results->results),new_intermidiate_result_a);
    // insert the new joined relation, which was not in intermidiate results
    vector_push_back(&(intermidiate_results->results),new_intermidiate_result_b);

    // free structures that were used for the parallel join
    free_list(&list_results);
    free_relation(&R);
    free_relation(&S);
    return true;
}

bool both_relations_in_mid_results(struct file_array file_array, struct intermidiate_results *intermidiate_results, int * predicate, int * relations, int intermidiate_result_index_a, int intermidiate_result_index_b) {
    // create and initialize new intermidiate result relations
    struct intermidiate_result new_intermidiate_result_a, new_intermidiate_result_b;
    struct relation *R = NULL, *S = NULL;
    struct list *list_results = NULL;
    uint64_t file_index_a = relations[predicate[ROW_A]], file_index_b = relations[predicate[ROW_B]];
    uint64_t predicate_relation_a = predicate[ROW_A], predicate_relation_b = predicate[ROW_B];
    uint64_t column_a = predicate[COLUMN_A], column_b = predicate[COLUMN_B];
    struct file file_a = vector_at(&file_array.files,file_index_a), file_b = vector_at(&file_array.files,file_index_b);
    
    // get the relations that already exists in intermidiate results
    struct intermidiate_result relation_in_intermidiate_results_a = vector_at(&(intermidiate_results->results),intermidiate_result_index_a);
    struct intermidiate_result relation_in_intermidiate_results_b = vector_at(&(intermidiate_results->results),intermidiate_result_index_b);
    
    R = create_relation_from_intermidiate_results_for_join(file_a,*intermidiate_results,intermidiate_result_index_a,column_a);
    S = create_relation_from_intermidiate_results_for_join(file_b,*intermidiate_results,intermidiate_result_index_b,column_b);

    // if one of the relations is filter we should join the relations
    if((relation_in_intermidiate_results_a.field == 'j' && relation_in_intermidiate_results_b.field == 'f') || (relation_in_intermidiate_results_a.field == 'f' && relation_in_intermidiate_results_b.field == 'j')) {
        list_results = sort_join_calculation(&R,&S,*intermidiate_results,predicate);

        // if there was no results then free structures and return false
        if( list_results->total_size == 0 ) {
            free_list(&list_results);
            free_relation(&R);
            free_relation(&S);
            return false;
        }

        initialize_intermidiate_result(&new_intermidiate_result_a,file_index_a,predicate_relation_a,'j',list_results->total_size);
        initialize_intermidiate_result(&new_intermidiate_result_b,file_index_b,predicate_relation_b,'j',list_results->total_size);
        // fix new intermidiate result relations
        struct bucket * temp_bucket = list_results->head;
        for( int i = 0 ; i < list_results->number_of_buckets ; i++) {
            for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                int index_a = temp_bucket->tuples[j].row_key_1;
                int index_b = temp_bucket->tuples[j].row_key_2;
                int value_a = vector_at(&(relation_in_intermidiate_results_a.row_ids),index_a);
                int value_b = vector_at(&(relation_in_intermidiate_results_b.row_ids),index_b);
                vector_push_back(&(new_intermidiate_result_a.row_ids),value_a);
                vector_push_back(&(new_intermidiate_result_b.row_ids),value_b);
            }
            temp_bucket = temp_bucket->next_bucket;
        }

        // delete old relations and push new relations
        vector_clear(&(relation_in_intermidiate_results_a.row_ids));
        vector_clear(&(relation_in_intermidiate_results_b.row_ids));
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_a+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_a,intermidiate_result_index_a+1);
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_b+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_b,intermidiate_result_index_b+1);

        // synchronize old joined relations
        for( int i = 0 ; i < intermidiate_results->results.length ; i++ ) {
            struct intermidiate_result temp_intermidiate_result = vector_at(&(intermidiate_results->results),i);
            if( temp_intermidiate_result.field == 'f' )
                continue;
            else if( (i == intermidiate_result_index_a) || (i == intermidiate_result_index_b) )
                continue;
            else {
                struct intermidiate_result new_intermidiate_result;
                initialize_intermidiate_result(&new_intermidiate_result,temp_intermidiate_result.file_index,temp_intermidiate_result.predicate_relation,'j',list_results->total_size);
                struct bucket * temp_bucket = list_results->head;
                for( int k = 0 ; k < list_results->number_of_buckets ; k++) {
                    for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                        int index = temp_bucket->tuples[j].row_key_1;
                        int value = vector_at(&(temp_intermidiate_result.row_ids),index);
                        vector_push_back(&(new_intermidiate_result.row_ids),value);
                    }
                    temp_bucket = temp_bucket->next_bucket;
                }
                vector_clear(&(temp_intermidiate_result.row_ids));
                vector_erase_from_nth_position(&(intermidiate_results->results),i+1);
                vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result,i+1);
            }
        }

        free_list(&list_results);
    }
    // if both of the relations are filters
    else if( relation_in_intermidiate_results_a.field == 'f' && relation_in_intermidiate_results_b.field == 'f' ) {
        list_results = sort_join_calculation(&R,&S,*intermidiate_results,predicate);

        // if there was no results then free structures and return false
        if( list_results->total_size == 0 ) {
            free_list(&list_results);
            free_relation(&R);
            free_relation(&S);
            return false;
        }

        initialize_intermidiate_result(&new_intermidiate_result_a,file_index_a,predicate_relation_a,'j',list_results->total_size);
        initialize_intermidiate_result(&new_intermidiate_result_b,file_index_b,predicate_relation_b,'j',list_results->total_size);
        struct bucket * temp_bucket = list_results->head;
        for( int i = 0 ; i < list_results->number_of_buckets ; i++) {
            for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                int index_a = temp_bucket->tuples[j].row_key_1;
                int index_b = temp_bucket->tuples[j].row_key_2;
                int value_a = vector_at(&(relation_in_intermidiate_results_a.row_ids),index_a);
                int value_b = vector_at(&(relation_in_intermidiate_results_b.row_ids),index_b);
                vector_push_back(&(new_intermidiate_result_a.row_ids),value_a);
                vector_push_back(&(new_intermidiate_result_b.row_ids),value_b);
            }
            temp_bucket = temp_bucket->next_bucket;
        }

        // delete old relations and push new relations
        vector_clear(&(relation_in_intermidiate_results_a.row_ids));
        vector_clear(&(relation_in_intermidiate_results_b.row_ids));
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_a+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_a,intermidiate_result_index_a+1);
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_b+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_b,intermidiate_result_index_b+1);

        free_list(&list_results);
    }
    // if none of the relations are filters
    else {
        // calculate the indexes when R.value = S.value
        vector(uint64_t) indexes;
        vector_inititialize(&indexes);
        vector_expand_with_size(&indexes,R->num_tuples);
        for( int i = 0 ; i < R->num_tuples ; i++ ) {
            if(  R->tuples[i].value == S->tuples[i].value ) {
                vector_push_back(&indexes,R->tuples[i].row_id);
            }
        }

        // if there was no results then free structures and return false
        if( indexes.length == 0 ) {
            vector_clear(&indexes);
            free_relation(&R);
            free_relation(&S);
            return false;
        }

        initialize_intermidiate_result(&new_intermidiate_result_a,file_index_a,predicate_relation_a,'j',indexes.length);
        initialize_intermidiate_result(&new_intermidiate_result_b,file_index_b,predicate_relation_b,'j',indexes.length);
        // fix row ids for relations we want to calculate
        for( int i = 0 ; i < indexes.length ; i++ ) {
            int index = indexes.data[i];
            int value_a = vector_at(&(relation_in_intermidiate_results_a.row_ids),index);
            int value_b = vector_at(&(relation_in_intermidiate_results_b.row_ids),index);
            vector_push_back(&(new_intermidiate_result_a.row_ids),value_a);
            vector_push_back(&(new_intermidiate_result_b.row_ids),value_b);
        }

        // delete old relations and push new relations
        vector_clear(&(relation_in_intermidiate_results_a.row_ids));
        vector_clear(&(relation_in_intermidiate_results_b.row_ids));
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_a+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_a,intermidiate_result_index_a+1);
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index_b+1);
        vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result_b,intermidiate_result_index_b+1);

        // synchronize old joined relations
        for( int i = 0 ; i < intermidiate_results->results.length ; i++ ) {
            struct intermidiate_result temp_intermidiate_result = vector_at(&(intermidiate_results->results),i);
            if( temp_intermidiate_result.field == 'f' )
                continue;
            else if( (i == intermidiate_result_index_a) || (i == intermidiate_result_index_b) )
                continue;
            else {
                struct intermidiate_result new_intermidiate_result;
                initialize_intermidiate_result(&new_intermidiate_result,temp_intermidiate_result.file_index,temp_intermidiate_result.predicate_relation,'j',indexes.length);
                for( int j = 0 ; j < indexes.length ; j++ ) {
                    int index = indexes.data[j];
                    int value = vector_at(&(temp_intermidiate_result.row_ids),index);
                    vector_push_back(&(new_intermidiate_result.row_ids),value);
                }
                vector_clear(&(temp_intermidiate_result.row_ids));
                vector_erase_from_nth_position(&(intermidiate_results->results),i+1);
                vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result,i+1);
            }
        }
        vector_clear(&indexes);
    }
    // free structures that were used for the parallel join
    free_relation(&R);
    free_relation(&S);
    return true;
}

void flip_predicate(int *predicate){
    int temp;
    temp=predicate[ROW_A];
    predicate[ROW_A]=predicate[ROW_B];
    predicate[ROW_B]=temp;
    temp=predicate[COLUMN_A];
    predicate[COLUMN_A]=predicate[COLUMN_B];
    predicate[COLUMN_B]=temp;
}

bool join(struct file_array file_array, struct intermidiate_results *intermidiate_results, int * predicate, int * relations) {
    int intermidiate_result_index_a = search_intermidiate_results(*intermidiate_results,predicate[ROW_A]);
    int intermidiate_result_index_b = search_intermidiate_results(*intermidiate_results,predicate[ROW_B]);
    bool return_value;
    // if none of the relations are in mid results
    if( (intermidiate_result_index_a == -1) && (intermidiate_result_index_b == -1) )
        return_value = none_in_mid_results(file_array,intermidiate_results,predicate,relations);
    // if only the first relation is in mid results
    else if ( (intermidiate_result_index_a != -1) && (intermidiate_result_index_b == -1) )
        return_value = only_one_relation_in_mid_results(file_array,intermidiate_results,predicate,relations,intermidiate_result_index_a);
    // if only the second relation is in mid results
    else if ( (intermidiate_result_index_a == -1) && (intermidiate_result_index_b != -1) ){
        flip_predicate(predicate);
        return_value = only_one_relation_in_mid_results(file_array,intermidiate_results,predicate,relations,intermidiate_result_index_b);
    }
    // if both relations are in mid results
    else
        return_value = both_relations_in_mid_results(file_array,intermidiate_results,predicate,relations,intermidiate_result_index_a,intermidiate_result_index_b);
    
    // fix intermidiate sort fields
    inform_intermidiate_sort_fields(intermidiate_results,predicate);
    return return_value;
}

void synchronize_intermidiate_results(struct intermidiate_results *intermidiate_results, struct list *list_results, int intermidiate_result_index) {
    for( int i = 0 ; i < intermidiate_results->results.length ; i++ ) {
        struct intermidiate_result intermidiate_result = vector_at(&(intermidiate_results->results),i);
        intermidiate_result = vector_at(&(intermidiate_results->results),i);
        if( intermidiate_result.field == 'f' )
            continue;
        else if( i == intermidiate_result_index )
            continue;
        else {
            struct bucket * temp_bucket = list_results->head;
            struct intermidiate_result new_intermidiate_result;
            initialize_intermidiate_result(&new_intermidiate_result,intermidiate_result.file_index,intermidiate_result.predicate_relation,'j',list_results->total_size);
            for( int k = 0 ; k < list_results->number_of_buckets ; k++) {
                for(int j = 0 ; j < temp_bucket->current_size ; j ++ ) {
                    int index = temp_bucket->tuples[j].row_key_1;
                    int value = vector_at(&(intermidiate_result.row_ids),index);
                    vector_push_back(&(new_intermidiate_result.row_ids),value);
                }
                temp_bucket = temp_bucket->next_bucket;
            }
            vector_clear(&(intermidiate_result.row_ids));
            vector_erase_from_nth_position(&(intermidiate_results->results),i+1);
            vector_push_at_nth_position(&(intermidiate_results->results),new_intermidiate_result,i+1);
        }
    }
}

bool filter(struct file_array file_array, struct intermidiate_results * intermidiate_results, int * predicate, int * relations){
    struct intermidiate_result intermidiate_result, intermidiate_result_2;
    uint64_t file_index = relations[predicate[ROW_A]], predicate_relation = predicate[ROW_A];
    uint64_t column_a = predicate[COLUMN_A], filter_number = predicate[ROW_B], column_b = predicate[COLUMN_B], operator = predicate[OPERATOR];
    int intermidiate_result_index = search_intermidiate_results(*intermidiate_results,predicate_relation);
    struct file file = vector_at(&file_array.files,file_index);

    // if the relation does not exist in intermidiate results
    if( intermidiate_result_index == -1 ) {
        initialize_intermidiate_result(&intermidiate_result,file_index,predicate_relation,'f',file.number_of_rows);
        if( operator == EQUAL ) {
            for( int i = 0 ; i < file.number_of_rows ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows + i] == filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),i);
                else if ( column_b != -1 && file.array[column_a*file.number_of_rows + i] == file.array[column_b*file.number_of_rows + i] )
                    vector_push_back(&(intermidiate_result.row_ids),i);
        }
        else if( operator == GREATER ) {
            for( int i = 0 ; i < file.number_of_rows ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows + i] > filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),i);
                else if ( column_b != -1 && file.array[column_a*file.number_of_rows + i] > file.array[column_b*file.number_of_rows + i] )
                    vector_push_back(&(intermidiate_result.row_ids),i);
        }
        else {	// LESS
            for( int i = 0 ; i < file.number_of_rows ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows + i] < filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),i);
                else if ( column_b != -1 && file.array[column_a*file.number_of_rows + i] < file.array[column_b*file.number_of_rows + i] )
                    vector_push_back(&(intermidiate_result.row_ids),i);
        }
        vector_push_back(&(intermidiate_results->results),intermidiate_result);
    }
    // if the relation exists in intermidiate results
    else {
        intermidiate_result_2 = vector_at(&(intermidiate_results->results),intermidiate_result_index);
        initialize_intermidiate_result(&intermidiate_result,file_index,predicate_relation,'f',intermidiate_result_2.row_ids.length);
        if( operator == EQUAL ) {
            for( int i = 0 ; i < intermidiate_result_2.row_ids.length ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] == filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
                else if( column_b != -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] == file.array[column_b*file.number_of_rows+intermidiate_result_2.row_ids.data[i]])
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
        }
        else if( operator == GREATER ) {
            for( int i = 0 ; i < intermidiate_result_2.row_ids.length ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] > filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
                else if( column_b != -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] > file.array[column_b*file.number_of_rows+intermidiate_result_2.row_ids.data[i]])
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
        }
        else {	// LESS
            for( int i = 0 ; i < intermidiate_result_2.row_ids.length ; i++ )
                if( column_b == -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] < filter_number )
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
                else if( column_b != -1 && file.array[column_a*file.number_of_rows+intermidiate_result_2.row_ids.data[i]] < file.array[column_b*file.number_of_rows+intermidiate_result_2.row_ids.data[i]])
                    vector_push_back(&(intermidiate_result.row_ids),intermidiate_result_2.row_ids.data[i]);
        }
        vector_clear(&(intermidiate_result_2.row_ids));
        vector_erase_from_nth_position(&(intermidiate_results->results),intermidiate_result_index+1);
        vector_push_at_nth_position(&(intermidiate_results->results),intermidiate_result,intermidiate_result_index+1);
    }
    if( intermidiate_result.row_ids.length == 0 )
        return false;
    else
        return true;
}

void initialize_intermidiate_results(struct intermidiate_results *intermidiate_results, Query query) {
    vector_inititialize(&(intermidiate_results->results));
    vector_expand_with_size(&(intermidiate_results->results),query.relations.length);
    intermidiate_results->sorted_relations[0] = -1;
    intermidiate_results->sorted_relations[1] = -1;
    intermidiate_results->sorted_relation_columns[0] = -1;
    intermidiate_results->sorted_relation_columns[1] = -1;
}

void inform_intermidiate_sort_fields(struct intermidiate_results *intermidiate_results, int *predicates) {
    intermidiate_results->sorted_relations[0] = predicates[ROW_A];
    intermidiate_results->sorted_relations[1] = predicates[ROW_B];
    intermidiate_results->sorted_relation_columns[0] = predicates[COLUMN_A];
    intermidiate_results->sorted_relation_columns[1] = predicates[COLUMN_B];
}

void print_null(int number_of_nulls, int64_t **results, int result_index) {
    for( int i = 0 ; i < number_of_nulls ; i++)
        results[result_index][i] = -2;
}

void execute_query(void *argument) {
    struct execute_query_arguments *execute_query_arguments = (struct execute_query_arguments *)argument;
    struct file_array file_array = execute_query_arguments->file_array;
    Query *query = execute_query_arguments->sql_query;
    int64_t **results = execute_query_arguments->results;
    int result_index = execute_query_arguments->result_index;
	stats_list *statistics_list=execute_query_arguments->statistics_list;
	if(statistics_list!=NULL)
		join_enumeration(query,statistics_list);

    struct intermidiate_results intermidiate_results;
    bool return_value;
    initialize_intermidiate_results(&intermidiate_results, *query);

    int *predicate = NULL;
    int *relations = query->relations.data;
    // execute filters
    for( int i = 0 ; i < query->filters.length ; i++) {
        predicate = query->filters.data[i].predicate.data;
        return_value = filter(file_array,&intermidiate_results,predicate,relations);
        if( return_value == false) {
            print_null(query->projections.length,results, result_index);
            free_intermidiate_results(&intermidiate_results);
            free_sql_query(&execute_query_arguments->sql_query);
            return;
        }
    }
    // execute joins
    for( int i = 0 ; i < query->joins.length ; i++) {
        predicate = query->joins.data[i].predicate.data;
        return_value = join(file_array,&intermidiate_results,predicate,relations);
        if( return_value == false) {
            print_null(query->projections.length,results, result_index);
            free_intermidiate_results(&intermidiate_results);
            free_sql_query(&execute_query_arguments->sql_query);
            return;
        }
    }
    
    projection_sum_results(file_array, intermidiate_results, *query, results, result_index);

    free_intermidiate_results(&intermidiate_results);
    free_sql_query(&execute_query_arguments->sql_query);
}

void read_queries(struct file_array file_array, stats_list *statistics_list) {
    char* query=NULL;
    size_t length=0;
    int result_index;
    int64_t **results = allocate_and_initialize_2d_array(RESULTS_ROWS,RESULTS_COLUMNS,-1);
    bool stop = false;
    int job_barrier=0;
    while(1) {
        result_index = 0;
        while( getline(&query, &length, stdin) != -1 ) {
            query[strlen(query)-1]='\0';
            if( strcmp(query,"F")==0 )
                break;
            else if( strcmp(query,"Done")==0 ) {
                stop = true;
                break;
            }
            struct sql_query *sql_query = initialize_sql_query();
			struct execute_query_arguments execute_query_arguments={file_array,sql_query,results,result_index, statistics_list};
            split_sql_query(sql_query,query);
            //frequency_optimization(sql_query);
        	//join_enumeration(sql_query,statistics_list);
            schedule_job_scheduler((void*)execute_query, &execute_query_arguments, sizeof(struct execute_query_arguments), &job_barrier);

            result_index++;
        }
        // wait until the whole batch ends
        dynamic_barrier_job_scheduler(&job_barrier);
        // print batch results
        print_2d_array_results(results,result_index,RESULTS_COLUMNS);
        // if all batches were executed, leave
        if( stop == true )
            break;
    }
    free(query);
    free_2d_array(&results,RESULTS_ROWS);
}

void projection_sum_results(struct file_array file_array, struct intermidiate_results intermidiate_results, Query query, int64_t **results, int result_index) {
    for( int i = 0 ; i < query.projections.length ; i++ ) {
        uint64_t sum = 0;
        uint64_t predicate_relation = query.projections.data[i].projection.data[0];
        uint64_t file_index = query.relations.data[predicate_relation];
        uint64_t column = query.projections.data[i].projection.data[1];
        int intermidiate_result_index = search_intermidiate_results(intermidiate_results,predicate_relation);
        if ( intermidiate_result_index == -1) {
            printf("Relation %ld does not exist in mid results\n", file_index);
            continue;
        }
        struct file file = vector_at(&file_array.files,file_index);
        struct intermidiate_result intermidiate_result = vector_at(&(intermidiate_results.results),intermidiate_result_index);
        for( int j = 0 ; j < intermidiate_result.row_ids.length ; j++ )
            sum = sum + file.array[column*file.number_of_rows+intermidiate_result.row_ids.data[j]];
        results[result_index][i] = sum;
    }
}

void print_intermidiate_results(struct intermidiate_results intermidiate_results) {
    struct intermidiate_result intermidiate_result;
    printf("Intermidiate Results:\n");
    for( int i = 0 ; i < intermidiate_results.results.length ; i++ ) {
        intermidiate_result = vector_at(&(intermidiate_results.results),i);
        printf("File index=%ld, Predicate relation=%ld, Field=%c, Row ids=%d:\n",intermidiate_result.file_index, intermidiate_result.predicate_relation, intermidiate_result.field,intermidiate_result.row_ids.length);
        for( int j = 0 ; j < intermidiate_result.row_ids.length ; j++ )
            printf("%ld ",vector_at(&(intermidiate_result.row_ids),j));
        printf("\n");
    }
}

int search_intermidiate_results(struct intermidiate_results intermidiate_results, int predicate_relation ) {
    struct intermidiate_result intermidiate_result;
    for( int i = 0 ; i < intermidiate_results.results.length ; i++) {
        intermidiate_result = vector_at(&(intermidiate_results.results),i);
        if( intermidiate_result.predicate_relation == predicate_relation )
            return i;
    }
    return -1;
}

void free_intermidiate_results(struct intermidiate_results *intermidiate_results) {
    struct intermidiate_result intermidiate_result;
    for( int i = 0 ; i < intermidiate_results->results.length ; i++) {
        intermidiate_result = vector_at(&(intermidiate_results->results),i);
        vector_clear(&(intermidiate_result.row_ids));
    }
    vector_clear(&(intermidiate_results->results));
}

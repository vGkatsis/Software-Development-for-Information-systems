#include "../headers/relation.h"

struct relation * initialize_relation(int number_of_tuples) {
    struct relation *new_relation = (struct relation *)malloc(sizeof(struct relation));
    if( new_relation == NULL) {
        perror("initialize_relation failed");
        return NULL;
    }
    
    new_relation->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    if( new_relation->tuples == NULL) {
        perror("initialize_relation failed");
        return NULL;
    }
    new_relation->num_tuples = number_of_tuples;

    return new_relation;
}

struct relation * initialize_relation_with_dataset(char * filename) {
    FILE *dataset;
    char *line_buffer = NULL, *ptr;
    size_t line_buffer_size=0;
    uint64_t number_of_tuples = 0, i = 0, row_id, value;

    dataset = fopen(filename,"r");
    if( dataset == NULL ){
        perror("File doesn't exist");
        return NULL;
    }

    struct relation *new_relation = (struct relation *)malloc(sizeof(struct relation));
    if( new_relation == NULL) {
        perror("initialize_relation_with_dataset failed");
        return NULL;
    }

    // get the number of lines, so we can create the tuples
    while(getline(&line_buffer, &line_buffer_size, dataset)>=0){
        number_of_tuples++;
    }

    // create the tuples
    new_relation->tuples = (struct tuple *)malloc(number_of_tuples*sizeof(struct tuple));
    if( new_relation->tuples == NULL) {
        perror("initialize_relation_with_dataset failed");
        return NULL;
    }

    // move the file pointer at the begining
    fseek(dataset, 0, SEEK_SET);
    // initialize relation tuples with dataset
    while(getline(&line_buffer, &line_buffer_size, dataset)>=0){
        value = strtoull(line_buffer,&ptr,10);
        row_id = strtoull(ptr+1,&ptr,10);
        new_relation->tuples[i].row_id = row_id;
        new_relation->tuples[i].value = value;
        i++;
    }
    new_relation->num_tuples = number_of_tuples;

    free(line_buffer);
    fclose(dataset);

    return new_relation;
}

void free_relation(struct relation **relation) {
    if( *relation == NULL )
        return;
    free((*relation)->tuples);
    free(*relation);
}

void change_value(struct relation *R, int key, uint64_t new_value){

     R->tuples[key].value = new_value;
}

void change_rowId(struct relation *R, int key, uint64_t new_rowId){

    R->tuples[key].row_id = new_rowId;
}


int64_t get_rowId(struct relation *R, int key){

     return R->tuples[key].row_id;
}

int64_t get_value(struct relation *R, int key){

     return R->tuples[key].value;
}

void print_relation(struct relation *relation) {
    printf("Relation:\n{ RowId , Value }\n");
    for(int i = 0 ; i < relation->num_tuples ; i++)
        printf("{ %"PRIu64" , %"PRIu64" }\n", relation->tuples[i].row_id, relation->tuples[i].value);
}

void get_range(struct relation *R, int start, int *end, int finish) {
    // calculate the range
    while( *end != finish )
        if( (*end+1 != finish)&&(R->tuples[start].value == R->tuples[*end+1].value) )
            (*end)++;
        else
            return;
}

void parallel_join(struct relation * R,struct relation * S, struct list ** list) {
    int index_R_start = 0, index_S_start = 0, index_R_end = 0, index_S_end = 0, i, j;
    int finish_R = R->num_tuples, finish_S = S->num_tuples;
    while( ( index_R_start != finish_R ) && ( index_S_start != finish_S ) ){
        // calculate R's range
        get_range(R,index_R_start,&index_R_end,finish_R);
        // calculate S's range
        get_range(S,index_S_start,&index_S_end,finish_S);
        // if the value is the same then do a double loop and insert all the row id into the list
        if( R->tuples[index_R_start].value == S->tuples[index_S_start].value ) {
            for( i = index_R_start ; i <= index_R_end ; i++ )
                for( j = index_S_start ; j <= index_S_end ; j++ )
                    insert_tuple(list, R->tuples[i].row_id, S->tuples[j].row_id );
            // also move R pointer
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if R's value is less than S's value then move R pointer
        else if( R->tuples[index_R_start].value < S->tuples[index_S_start].value ) {
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if S's value is less than R's value then move R pointer
        else {
            index_S_start = index_S_end + 1;
            index_S_end = index_S_start;
        };
    }
}

void parallel_join_2(struct parallel_join_arguments *parallel_join_arguments) {
    int index_R_start = parallel_join_arguments->start_R, index_S_start = parallel_join_arguments->start_S;
    int index_R_end = parallel_join_arguments->start_R, index_S_end = parallel_join_arguments->start_S, i, j;
    int finish_R = parallel_join_arguments->end_R, finish_S = parallel_join_arguments->end_S;
    struct relation * R = parallel_join_arguments->R;
    struct relation * S = parallel_join_arguments->S;
    struct list * list = parallel_join_arguments->list;

    while( ( index_R_start != finish_R ) && ( index_S_start != finish_S ) ){
        // calculate R's range
        get_range(R,index_R_start,&index_R_end,finish_R);
        // calculate S's range
        get_range(S,index_S_start,&index_S_end,finish_S);
        // if the value is the same then do a double loop and insert all the row id into the list
        if( R->tuples[index_R_start].value == S->tuples[index_S_start].value ) {
            for( i = index_R_start ; i <= index_R_end ; i++ )
                for( j = index_S_start ; j <= index_S_end ; j++ )
                    insert_tuple(&list, R->tuples[i].row_id, S->tuples[j].row_id );
            // also move R pointer
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if R's value is less than S's value then move R pointer
        else if( R->tuples[index_R_start].value < S->tuples[index_S_start].value ) {
            index_R_start = index_R_end + 1;
            index_R_end = index_R_start;
        }
        // if S's value is less than R's value then move R pointer
        else {
            index_S_start = index_S_end + 1;
            index_S_end = index_S_start;
        };
    }
}

void fix_thread_list_results_links(struct list **thread_list_results, struct list *list_results, int number_of_rows) {
    for( int i = 0 ; i < number_of_rows ; i ++ ) {
        if( list_results->head == NULL ) {
            list_results->head = thread_list_results[i]->head;
            list_results->tail = thread_list_results[i]->tail;
        }
        else {
            list_results->tail->next_bucket = thread_list_results[i]->head;
            list_results->tail = thread_list_results[i]->tail;
        }
        list_results->total_size = list_results->total_size + thread_list_results[i]->total_size;
        list_results->number_of_buckets = list_results->number_of_buckets + thread_list_results[i]->number_of_buckets;
    }
}

void break_join_to_jobs(struct relation **R, struct relation **S, struct list *list_results, struct join_partition *join_partition_R, struct join_partition *join_partition_S) {
    struct list **thread_list_results = NULL;
    int number_of_rows = 0, index_R = 0, index_S = 0, index_list_results = 0,start_R, end_R, start_S, end_S, join_barrier = 0;

    if( join_partition_R->histogram_indexes.size <= join_partition_S->histogram_indexes.size )
        number_of_rows = join_partition_R->histogram_indexes.size;
    else
        number_of_rows = join_partition_S->histogram_indexes.size;

    thread_list_results = initialize_2d_list_results(number_of_rows);

    // break join to jobs
    while( ( index_R < join_partition_R->histogram_indexes.size ) && ( index_S < join_partition_S->histogram_indexes.size ) ) {
        if( join_partition_R->histogram_indexes.indexes[index_R] == join_partition_S->histogram_indexes.indexes[index_S] ) {
            start_R = join_partition_R->prefix_sum[join_partition_R->histogram_indexes.indexes[index_R]];
            end_R = join_partition_R->prefix_sum[join_partition_R->histogram_indexes.indexes[index_R]]+join_partition_R->histogram[join_partition_R->histogram_indexes.indexes[index_R]];
            start_S = join_partition_S->prefix_sum[join_partition_S->histogram_indexes.indexes[index_S]];
            end_S = join_partition_S->prefix_sum[join_partition_S->histogram_indexes.indexes[index_S]]+join_partition_S->histogram[join_partition_S->histogram_indexes.indexes[index_S]];

            struct parallel_join_arguments parallel_join_arguments={*R,*S,thread_list_results[index_list_results],start_R,end_R,start_S,end_S};
            schedule_job_scheduler((void*)parallel_join_2,&parallel_join_arguments,sizeof(struct parallel_join_arguments),&join_barrier);
            
            index_R++;
            index_list_results++;
        }
        else if( join_partition_R->histogram_indexes.indexes[index_R] < join_partition_S->histogram_indexes.indexes[index_S] )
            index_R++;
        else
            index_S++;
    }
    // wait all joins to end
    dynamic_barrier_job_scheduler(&join_barrier);
    
    fix_thread_list_results_links(thread_list_results,list_results,number_of_rows);

    free_2d_list_results(thread_list_results,number_of_rows);
}
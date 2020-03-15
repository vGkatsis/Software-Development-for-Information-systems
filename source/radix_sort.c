#include "../headers/header.h"
#include "../headers/radix_sort.h"
#include "../headers/sort_list.h"
#include "../headers/job_scheduler.h"
extern Job_scheduler *job_scheduler;
//Masks the most significant byte, if byte==1, masks the least significant byte if byte==8
unsigned char binary_mask(uint64_t value, int byte){
    return (value >> (8*(sizeof(value)-byte))) & 0xff; //MASKS the Most Significant Byte
}

uint64_t *create_histogram_multithread(Relation *R, uint64_t start,uint64_t end, int byte, struct histogram_indexing *histogram_indexes) {
    int job_barrier=0;
    int64_t **thread_histograms = allocate_and_initialize_2d_array(job_scheduler->number_of_threads,BUCKET_SIZE,0);
    break_histogram_to_jobs(R,start,end,byte,histogram_indexes,thread_histograms,&job_barrier);
    dynamic_barrier_job_scheduler(&job_barrier);
    uint64_t *histogram = sum_histograms(thread_histograms,job_scheduler->number_of_threads,BUCKET_SIZE,histogram_indexes);
    free_2d_array(&thread_histograms,job_scheduler->number_of_threads);
    if(histogram_indexes->size==0){
        free(histogram);
        return NULL;
    }
    return histogram;
}

uint64_t *create_histogram(Relation *R, uint64_t start, uint64_t end, int byte,struct histogram_indexing *histogram_indexes){
    uint64_t *temp=calloc(BUCKET_SIZE, sizeof(uint64_t));
    int hash_index;
    for(uint64_t i=start;i<end;i++){
        hash_index = binary_mask(R->tuples[i].value, byte);
        temp[hash_index]++;
        if(temp[hash_index] == 1) {
            histogram_indexes->indexes[histogram_indexes->size] = hash_index;
            histogram_indexes->size++;
        }
    }
    return temp;
}

uint64_t *create_prefix_sum(uint64_t *histogram, uint64_t start){
    uint64_t *temp = calloc(BUCKET_SIZE, sizeof(uint64_t));
    int sum = start;
    for(int i = 0 ; i < BUCKET_SIZE ; i++){
        if( histogram[i] != 0 ) {
            temp[i] = sum;
            sum = sum + histogram[i];
        }
    }
    return temp;
}

//Copies R to R_new (think of it as R_new = R)
void copy_back_to_result_relation(Relation *R_new, Relation *R, uint64_t start, uint64_t end){
    for(uint64_t i=start;i<end;i++){
        R_new->tuples[i]=R->tuples[i];
    }
}

//scans R and uses it to fill R_new with tuples ordered by the bit mask
void fill_new_relation(Relation *R, Relation *R_new, uint64_t *prefix_sum, uint64_t start, uint64_t end, int byte){
    uint64_t counter[BUCKET_SIZE]={0};
    int temp_value;	
    for(uint64_t i=start;i<end;i++){
        temp_value=binary_mask(R->tuples[i].value,byte);
        R_new->tuples[((counter[temp_value]++)+prefix_sum[temp_value])]=R->tuples[i];
    }	
}

void swap_tuples(Relation **R, int i, int j) {
    struct tuple temp_tuple;

    temp_tuple.row_id = (*R)->tuples[j].row_id;
    temp_tuple.value = (*R)->tuples[j].value;
    (*R)->tuples[j].row_id = (*R)->tuples[i].row_id;
    (*R)->tuples[j].value = (*R)->tuples[i].value;
    (*R)->tuples[i].row_id = temp_tuple.row_id;
    (*R)->tuples[i].value = temp_tuple.value;
}

int partition(Relation **R, int start, int end) {
    //  pivot                          , index of smaller element
    uint64_t pivot = (*R)->tuples[end].value, i = start-1;

    for( int j = start ; j < end ; j++ ) {
        // if current element is smaller than the pivot, increase index of smaller element and swap the tuples 
        if( (*R)->tuples[j].value < pivot ) {
            i++;
            swap_tuples(R,i,j);
        }
    }
    i++;
    // put pivot at correct position
    if( (*R)->tuples[i].value != (*R)->tuples[end].value )
        swap_tuples(R,i,end);
    return i;
}

void quick_sort(Relation **R, int start, int end) {
    int partition_index;
    if( start < end ) {
        partition_index = partition(R,start,end);
        quick_sort(R,start,partition_index-1);
        quick_sort(R,partition_index+1,end);
    }
}

void sort_recursive(struct sort_loop_args *args){
    Relation *R=args->R;
    Relation *R_new=args->R_new;
    uint64_t start=args->start;
    uint64_t end=args->end;
    int byte=args->byte;

    int job_barrier=0;
    struct histogram_indexing histogram_indexes;
    histogram_indexes.size = 0;
    //uint64_t *histogram=create_histogram(R,start,end,byte,&histogram_indexes);	
    uint64_t *histogram=create_histogram_multithread(R,start,end,byte,&histogram_indexes);
    if(histogram==NULL){
        //struct sort_loop_args *rec_args=malloc(sizeof(struct sort_loop_args));
        *(args->result_relation)*=-1;
        //*rec_args=(struct sort_loop_args){R, R_new, start,end, byte+1,args->result_relation};
		struct sort_loop_args rec_args={R,R_new,start,end,byte+1,args->result_relation};
        //schedule_job_scheduler((void*)sort_recursive, rec_args,sizeof(struct sort_loop_args),&job_barrier);
        schedule_job_scheduler((void*)sort_recursive, &rec_args,sizeof(struct sort_loop_args),&job_barrier);
    }
    else{
        uint64_t *prefix_sum=create_prefix_sum(histogram,start);

        fill_new_relation(R, R_new, prefix_sum, start, end, byte); //Put data ordered by their mask
        if(byte<8) 	//At this point the ids are sorted anyway. No need for further recursions
        {
            //Recursion stuff
            for(int i=0; i<BUCKET_SIZE; i++)
            {
                start=prefix_sum[i];
                end=prefix_sum[i]+histogram[i];
                if(histogram[i]==0)
                    continue;
                else if(histogram[i]*sizeof(Tuple) <= CACHE_SIZE){ //Normal sorting
                    //quick_sort_multithread(&((struct quick_sort_args){&R_new,start,end-1}));
                    quick_sort(&R_new,start,end-1);
                    if(byte%2+args->result_relation>0)
                        copy_back_to_result_relation(R, R_new, start, end); 
                }
                else{ 
                //Data too large for sorting
                    //struct sort_loop_args *rec_args=malloc(sizeof(struct sort_loop_args));
                    struct sort_loop_args rec_args=(struct sort_loop_args){R_new,R,start,end,byte+1};
                    schedule_job_scheduler((void*)sort_recursive,&rec_args,sizeof(struct sort_loop_args),&job_barrier);
                    //schedule_job_scheduler((void*)sort_recursive,&((struct sort_loop_args){R_new, R, start, end, byte+1}),&job_barrier);
                    //sort_recursive((struct sort_loop_args){R_new, R, start, end, byte+1});
                }
            }
        }
        free(histogram);
        free(prefix_sum);
    }
    dynamic_barrier_job_scheduler(&job_barrier);
}

void create_histogram_2(struct histogram_arguments *histogram_arguments) {
    int hash_index;

    for(uint64_t i = histogram_arguments->start ; i < histogram_arguments->end ; i++) {
        hash_index = binary_mask(histogram_arguments->R->tuples[i].value, histogram_arguments->byte);
        histogram_arguments->histogram[hash_index]++;
    }

    //free(histogram_arguments);
}

void break_histogram_to_jobs(Relation *R, uint64_t start, uint64_t end, int byte,struct histogram_indexing *histogram_indexes, int64_t **thread_histograms, int *job_barrier) {
	int step=(end-start)/job_scheduler->number_of_threads;
    //int step = (end-start)/job_scheduler->number_of_threads;
    for( int i = 0 ; i < job_scheduler->number_of_threads ; i++) {
        //struct histogram_arguments *histogram_arguments = (struct histogram_arguments *)malloc(sizeof(struct histogram_arguments));
		struct histogram_arguments histogram_arguments={R,start,end,byte,thread_histograms[i]};
		if(i!=job_scheduler->number_of_threads-1)
			histogram_arguments.end=start+step;
        /*histogram_arguments->R = R;
        histogram_arguments->byte = byte;
        histogram_arguments->start = start;
        histogram_arguments->histogram = thread_histograms[i];
        if( i == job_scheduler->number_of_threads-1 )
            histogram_arguments->end = end;
        else
            histogram_arguments->end = start + step;
		*/
        schedule_job_scheduler((void*)create_histogram_2,&histogram_arguments,sizeof(struct histogram_arguments),job_barrier);
        start = start + step;
    }
}

int binary_search(int a[], int item, int low, int high) { 
    if (high <= low) 
        return (item > a[low])?  (low + 1): low; 
    int mid = (low + high)/2; 
    if(item == a[mid]) 
        return mid+1; 
    if(item > a[mid]) 
        return binary_search(a, item, mid+1, high); 
    return binary_search(a, item, low, mid-1); 
}

void insertion_sort(int a[], int n) { 
    int i, loc, j, selected; 
  
    for ( i = 1 ; i < n; i++ ) { 
        j = i - 1; 
        selected = a[i]; 
        loc = binary_search(a, selected, 0, j); 
        while( j >= loc ) { 
            a[j+1] = a[j]; 
            j--; 
        } 
        a[j+1] = selected; 
    } 
} 

uint64_t * sum_histograms(int64_t **thread_histograms, int rows, int columns, struct histogram_indexing *histogram_indexes) {
    uint64_t *temp = calloc(BUCKET_SIZE,sizeof(uint64_t));
    for( int i = 0 ; i < rows ; i++ ) {
        for( int j = 0 ; j < columns ; j++ ) {
            if( thread_histograms[i][j] == 0 )
                continue;
            if( temp[j] == 0 ) {
                histogram_indexes->indexes[histogram_indexes->size] = j;
                histogram_indexes->size++;
            }
            temp[j] = temp[j] + thread_histograms[i][j];
        }
    }
    insertion_sort(histogram_indexes->indexes,histogram_indexes->size);
    return temp;
}

void thread_quick_sort(struct quick_sort_arguments *quick_sort_arguments) {

    quick_sort(&(quick_sort_arguments->R),quick_sort_arguments->start,quick_sort_arguments->end);

    //free(quick_sort_arguments);
}

void sort_iterative(Relation *R, Relation *R_new, uint64_t start, uint64_t end, int byte, struct join_partition * join_partition){ 
    int job_barrier_sorts = 0;
    struct sort_data_list *sort_data_list = initialize_sort_data_list();
    struct sort_node *sort_node;
    int get_start, get_end;
    uint64_t *histogram, *prefix_sum;
    struct histogram_indexing histogram_indexes;

    // push R 
    push_at_the_end(&sort_data_list,start,end,byte);

    while( sort_data_list->length > 0 ) {
        // pop a sort data node => {start,end,byte}
        sort_node = pop(&sort_data_list);
        // fix histogram indexes size to 0
        histogram_indexes.size = 0;

        // fix R_new sorted by current histogram and prefix sum
        if( sort_node->byte%2 == MODULO ) {
            // break histogram to jobs
            histogram = create_histogram_multithread(R,sort_node->start,sort_node->end,sort_node->byte,&histogram_indexes);
            // fix prefix sum
            prefix_sum = create_prefix_sum(histogram,sort_node->start);
            // hold first histogram and prefix_sum
            if( sort_node->byte == START_BYTE ) {
                join_partition->histogram = histogram;
                join_partition->histogram_indexes = histogram_indexes;
                join_partition->prefix_sum = prefix_sum;
            }
            // R writes to R_new
            fill_new_relation(R, R_new, prefix_sum, sort_node->start, sort_node->end, sort_node->byte);
        }
        else {
            // break histogram to jobs
            histogram = create_histogram_multithread(R_new,sort_node->start,sort_node->end,sort_node->byte,&histogram_indexes);
            // fix prefix sum
            prefix_sum = create_prefix_sum(histogram,sort_node->start);
            // R_new writes to R
            fill_new_relation(R_new, R, prefix_sum, sort_node->start, sort_node->end, sort_node->byte);
        }
        
        for( int i = 0 ; i < histogram_indexes.size ; i++ ) {
            get_start = prefix_sum[histogram_indexes.indexes[i]];
            get_end = prefix_sum[histogram_indexes.indexes[i]]+histogram[histogram_indexes.indexes[i]];
            // if the hash duplicates are less or equal to cache size then just do quick sort
            if( histogram[histogram_indexes.indexes[i]] <= DUPLICATES ) {
                // if the byte % 2 == 1 then memcpy the part we want to sort from R_new to R
                if( sort_node->byte%2 == MODULO )
                    memcpy(R->tuples+get_start,R_new->tuples+get_start,(get_end-get_start)*sizeof(Tuple));
                // add quick sort job
                /*struct quick_sort_arguments *quick_sort_arguments = (struct quick_sort_arguments *)malloc(sizeof(struct quick_sort_arguments));
                quick_sort_arguments->R = R;
                quick_sort_arguments->start = get_start;
                quick_sort_arguments->end = get_end-1;
				*/
				struct quick_sort_arguments quick_sort_arguments={R,get_start,get_end-1};
                schedule_job_scheduler((void*)thread_quick_sort,&quick_sort_arguments,sizeof(struct quick_sort_arguments),&job_barrier_sorts);
            }
            // if the hash duplicates are more than cache size then push {start,end,byte+1} to the list
            else {
                // if the next byte is not the 9th, we just push { start , end , byte + 1 } to sort data list
                if( sort_node->byte + 1 != 9 )
                    push_at_the_end(&sort_data_list,get_start,get_end,sort_node->byte+1);
            }
        }
        // free structures
        // do not free first histogram and prefix sum
        if( sort_node->byte != START_BYTE) {
            free(histogram);
            free(prefix_sum);
        }
        free(sort_node);
    }
    free_sort_data_list(&sort_data_list);
    // wait all sorts to end
    dynamic_barrier_job_scheduler(&job_barrier_sorts);
}

//This is what main() will call to start the sorting
void sort(Relation *R, int mode){
    Relation* R_new = initialize_relation(R->num_tuples); //R_new is the result relation.
    uint64_t start=0;
    uint64_t end=R->num_tuples;
    sort_recursive(&(struct sort_loop_args){R,R_new,start,end,1});
   /* 
    if( mode == RECURSIVE )
        sort_recursive(R,R_new,start,end,1);
    else 
        sort_iterative(R,R_new,start,end,1);
    */
    free_relation(&R_new);
}

#define ITER
void sort_multithread(struct sort_args *args){
    Relation* R=args->R;
    Relation* R_new = initialize_relation(R->num_tuples); //R_new is the result relation.
    uint64_t start=0;
    uint64_t end=R->num_tuples;
    struct join_partition * join_partition = args->join_partition;
    #ifdef REC
    int result_relation=1;
    sort_recursive(&(struct sort_loop_args){R,R_new,start,end,1,&result_relation});
    if(result_relation==-1){
        Relation *Temp;
        Temp=args->R;
        args->R=R_new;
        free_relation(&Temp);
        //free(args);
        return;
    }
    #endif
    #ifdef ITER
        sort_iterative(R,R_new,start,end,START_BYTE,join_partition);
    #endif
    free_relation(&R_new);
    //free(args);
}

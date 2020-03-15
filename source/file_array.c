#include "../headers/file_array.h"

struct file *initialize_file(char *file, uint64_t number_of_rows, uint64_t number_of_columns) {
    struct file *new_file = (struct file *)malloc(sizeof(struct file));
    if( new_file == NULL) {
        perror("initialize_file failed");
        return NULL;
    }

    new_file->name = (char *)malloc(strlen(file)*sizeof(char)+1);
    if( new_file == NULL) {
        perror("initialize_file failed");
        return NULL;
    }
    
    new_file->array = (uint64_t *)malloc( number_of_rows * number_of_columns * sizeof(uint64_t *));
    if( new_file->array == NULL ) {
        perror("initialize_file failed");
        return NULL;
    }
    
    strcpy(new_file->name,file);
    new_file->number_of_rows = number_of_rows;
    new_file->number_of_columns = number_of_columns;

    return new_file;
}

void print_file(struct file * file) {
    if( file == NULL)
        return;
    printf("filename = %s, number of rows = %ld, number of columns = %ld\n", file->name,file->number_of_rows,file->number_of_columns);
    for( int k = 0 ; k < file->number_of_rows ; k++ ) {
        for( int l = 0 ; l < file->number_of_columns ; l++ )
            printf("%ld ",file->array[ l*file->number_of_columns + k ]);
        printf("\n");
    }
}

void free_file(struct file * file) {
    if( file == NULL)
        return;
    free(file->name);
    free(file->array);
}

void fix_file_array(struct file_array * file_array, stats_list *statistics_list) {
    uint64_t number_of_rows, number_of_columns;
    char* filename=NULL;
    size_t length=0;
    vector_inititialize(&(file_array->files));
    while( getline(&filename, &length, stdin) != -1 ) {
        filename[strlen(filename)-1]='\0';
        if( strcmp(filename,"Done")==0 )
            break;

        int fd = open(filename, O_RDONLY);
        if (fd==-1) {
            printf("fix_relations failed: File %s does not exist\n",filename);
            return;
        }
        struct stat sb;
        if (fstat(fd,&sb)==-1) {
            perror("fix_relations failed: fstat failed");
            return;
        }

        uint64_t *value = mmap(NULL,sb.st_size,PROT_READ|PROT_EXEC,MAP_SHARED, fd,0);

        number_of_rows = value[0];
        number_of_columns = value[1];
        
        // create new file node
        struct file *new_file = initialize_file(filename,number_of_rows,number_of_columns);
        // get the values from binary file and fix the file array and create stats
        relation_node *relation;
        stats_node *stats_node;
        stats *statistics = NULL;

        uint64_t row_counter = 0;
        uint64_t col_counter = 0;
        uint64_t node_num = 0;

        int iteration_pause = 2;

        initialize_relationNode(&relation);
        initialize_stats(&statistics);
        for( int i = 2 ; i <sb.st_size/sizeof *value ; i++){
            new_file->array[i-2] = value[i];

            //Gathering Statistics For Min Max And Data Num
            is_min(statistics,value[i]);
            is_max(statistics,value[i]);
            update_dataNum(statistics);
            row_counter++;

            if(row_counter == number_of_rows){
                
                //Gather Statistics For Unique Data
                initialize_bolleanArray(statistics);
                
                while(iteration_pause <= i){
                    fill_booleanArray(statistics,new_file->array[iteration_pause-2]);
                    iteration_pause++;
                }
                find_Unique(statistics);

                initialize_statsNode(&stats_node,statistics);
                add_last_statsNode(relation,stats_node);
                
                node_num++;
                row_counter = 0;
                col_counter++;

                if(col_counter != number_of_columns){
                    initialize_stats(&statistics);
                }else{
                    col_counter = 0;
                }
            }
        }
        add_last_relationNode(statistics_list, relation);
        
        vector_push_back(&(file_array->files),*new_file);

        free(new_file);
    }
    free(filename);
}

void print_file_array(struct file_array file_array) {
    struct file file;
    printf("files: \n");
    for( int i = 0 ; i < file_array.files.length ; i++) {
        file = vector_at(&file_array.files,i);
        print_file(&file);
    }
}

void free_file_array(struct file_array file_array) {
    struct file file;
    for( int i = 0 ; i < file_array.files.length ; i++) {
        file = vector_at(&file_array.files,i);
        free_file(&file);
    }
    vector_clear(&(file_array.files));
}
#include "../headers/utilities.h"
const char *help_string=
    "PROJECT PART ONE\n"
    "USAGE: ./project [OPTIONS]\n"
    "-A dataset_A -B dataset_B The location of datasets used for join. (required)\n"
    "-n number                 The size of the randomly generated tables. This option doesn't need extra datasets. (DEBUG ONLY)\n"
    "-m                        Uses the alternative sorting method. Current default is the iterative sort. The alternative is recursive. (optional)\n"
    "EXAMPLES:\n"
    "./project -n 1000\n"
    "./project -m -n 10\n"
    "./project -m -A dataset_A -B dataset_B\n";
Arguments parse_arguments(int argc, char *argv[]){
    //Arguments arguments={ITERATIVE,-1,NULL,NULL,NO_ERROR,NULL,NULL};
    Arguments arguments={ITERATIVE,NULL,NULL};
    Relation *R=NULL,*S=NULL;
    char *dataset_A, *dataset_B;
    int number_of_tuples=0;

    int operation_mode=NONE;
    int option;
    while((option=getopt(argc,argv,"A:B:n:hm"))!=-1){
        switch(option){
            case 'm':
                arguments.sort_method=RECURSIVE;
            break;
            case 'A':
                if(operation_mode==RANDOM){
                    fprintf(stderr,"Error, conflicting options.\n");
                    exit(-1);
                }
                dataset_A=optarg;
                if(operation_mode==DATASET_B)
                    operation_mode=DATASET_COMPLETE;
                else
                    operation_mode=DATASET_A;
            break;
            case 'B':
                if(operation_mode==RANDOM){
                    fprintf(stderr,"Error, conflicting options.\n");
                    exit(-1);
                }
                dataset_B=optarg;
                if(operation_mode==DATASET_A)
                    operation_mode=DATASET_COMPLETE;
                else
                    operation_mode=DATASET_B;
            break;
            case 'n':
                number_of_tuples=atoi(optarg);
                if(operation_mode!=NONE){
                    fprintf(stderr,"Error. Conflicting options.\n");
                    exit(-1);
                }
                operation_mode=RANDOM;
                if(number_of_tuples<1){
                    fprintf(stderr,"Error. The size of the tables needs to be larger than 0.\n");
                    exit(-1);
                }
            break;
            case 'h':
                printf("%s\n", help_string);
                exit(0);
                break;

        }
    }
    switch(operation_mode){
        case DATASET_COMPLETE:	
            R = initialize_relation_with_dataset(dataset_A);
            S = initialize_relation_with_dataset(dataset_B);	
            if(R==NULL || S==NULL)
                exit(-1);
        break;
        case DATASET_A:
        case DATASET_B:
            fprintf(stderr,"Error. Both dataset files need to be specified\nTerminating...\n");
            exit(-1);
        break;
        case RANDOM:
            R = initialize_relation(number_of_tuples);
            S = initialize_relation(number_of_tuples);
            if(R==NULL||S==NULL)
                exit(-1);
        break;
        case NONE:
            fprintf(stderr,"No option specified\nHELP: ./project -h\n");
            exit(-1);
        break;
    }
    arguments.R=R;
    arguments.S=S;
    return arguments;
}

int64_t **allocate_and_initialize_2d_array(int rows, int columns, int initialize_number) {
    int64_t **array = (int64_t **)malloc(rows*sizeof(int64_t *)); 
    if( array == NULL ) {
        perror("allocate_2d_array failed");
        return NULL;
    }
    for ( int i=0 ; i < rows ; i++ ) {
        array[i] = (int64_t *)malloc(columns*sizeof(int64_t));
        if( array[i] == NULL ) {
            perror("allocate_2d_array failed");
            return NULL;
        }
    }

    for ( int i = 0 ; i <  rows ; i++ ) 
        for ( int j = 0 ; j < columns ; j++ ) 
            array[i][j] = initialize_number;

    return array;
}

void print_2d_array(int64_t **array, int rows, int columns) {
    for( int i = 0 ; i < rows ; i ++) {
        for( int j = 0 ; j < columns ; j++)
            printf("%ld ",array[i][j]);
        printf("\n");
    }
}

void print_2d_array_results(int64_t **array, int rows, int columns) {
    for( int i = 0 ; i < rows ; i ++) {
        for( int j = 0 ; j < columns ; j++) {
            if( array[i][j] == -1 )
                break;
            else if( array[i][j] == -2 )
                printf("NULL ");
            else
                printf("%ld ",array[i][j]);
            array[i][j] = -1;
        }
        printf("\n");
    }
}

void free_2d_array(int64_t ***array, int rows) {
    for( int i = 0 ; i < rows; i++ )
        free((*array)[i]);
    free(*array);
}
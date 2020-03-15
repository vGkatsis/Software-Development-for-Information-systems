#ifndef file_array_H
#define file_array_H

#include "./stats.h"
#include "./header.h"
#include "./vector.h"

typedef struct file {
    char *name;
    uint64_t number_of_rows;
    uint64_t number_of_columns;
    uint64_t *array;
}File;

typedef struct file_array {
    vector(struct file) files;
}File_array;

struct file *initialize_file(char *, uint64_t , uint64_t );
void print_file(struct file *);
void free_file(struct file *);
void fix_file_array(struct file_array *, stats_list *statistics_list);
void print_file_array(struct file_array );
void free_file_array(struct file_array );

#endif

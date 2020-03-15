#ifndef sort_list_H_
#define sort_list_H_
#include "header.h"

// Type definition for a sort data node.
struct sort_node {
    int start;
    int end;
    int byte;
    struct sort_node *next;
};

// Type definition for a sort data list.
struct sort_data_list {
    struct sort_node *head;
    struct sort_node *tail;
    int length;
};

struct sort_node *initialize_sort_node(int , int , int );
struct sort_data_list *initialize_sort_data_list();
void push_at_the_begining(struct sort_data_list **, int , int , int );
void push_at_the_end(struct sort_data_list **, int , int , int );
struct sort_node *pop(struct sort_data_list **);
void free_sort_data_list(struct sort_data_list **);
void print_sort_data_list(struct sort_data_list *);

#endif
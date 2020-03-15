#include "../headers/sort_list.h"

struct sort_node *initialize_sort_node(int start, int end, int byte) {
    struct sort_node *new_sort_node = (struct sort_node *)malloc(sizeof(struct sort_node));
    if( new_sort_node == NULL) {
        perror("initialize_sort_node failed");
        return NULL;
    }

    new_sort_node->start = start;
    new_sort_node->end = end;
    new_sort_node->byte = byte;
    new_sort_node->next = NULL;

    return new_sort_node;
}

struct sort_data_list *initialize_sort_data_list() {
    struct sort_data_list *new_sort_data_list = (struct sort_data_list *)malloc(sizeof(struct sort_data_list));
    if( new_sort_data_list == NULL) {
        perror("initialize_sort_data_list failed");
        return NULL;
    }

    new_sort_data_list->head = NULL;
    new_sort_data_list->tail = NULL;
    new_sort_data_list->length = 0;
    
    return new_sort_data_list;
}

void push_at_the_begining(struct sort_data_list **sort_data_list, int start, int end, int byte) {
    struct sort_node *new_node = initialize_sort_node(start, end, byte),*temp_node = (*sort_data_list)->head;

    // if the list is empty, fix head and tail
    if( (*sort_data_list)->length == 0 ) {
        (*sort_data_list)->head = new_node;
        (*sort_data_list)->tail = new_node;
    }
    // otherwise fix only head
    else {
        (*sort_data_list)->head = new_node;
        (*sort_data_list)->head->next = temp_node;
    }
    (*sort_data_list)->length++;
}

void push_at_the_end(struct sort_data_list **sort_data_list, int start, int end, int byte) {
    struct sort_node *new_node = initialize_sort_node(start, end, byte);

    // if the list is empty, fix head and tail
    if( (*sort_data_list)->length == 0 ) {
        (*sort_data_list)->head = new_node;
        (*sort_data_list)->tail = new_node;
    }
    // otherwise fix only tail
    else {
        (*sort_data_list)->tail->next = new_node;
        (*sort_data_list)->tail = new_node;
    }
    (*sort_data_list)->length++;
}

struct sort_node *pop(struct sort_data_list **sort_data_list){
    struct sort_node *temp_node = (*sort_data_list)->head;
    // if the list is empty, then there is not data to be removed
    if( (*sort_data_list)->length == 0 ) {
        printf("There is not data to be removed.\n");
        return NULL;
    }
    // if the list has only 1 data, fix head and tail
    else if( (*sort_data_list)->length == 1 ) {
        (*sort_data_list)->head = NULL;
        (*sort_data_list)->tail = NULL;
    }
    // otherwise move only head
    else{
        (*sort_data_list)->head = (*sort_data_list)->head->next;
    }
    (*sort_data_list)->length--;
    return temp_node;
}

void free_sort_data_list(struct sort_data_list **sort_data_list) {
    struct sort_node *temp_node;
    while((*sort_data_list)->head) {
        temp_node = (*sort_data_list)->head->next;
        free((*sort_data_list)->head);
        (*sort_data_list)->head = temp_node;
    }
    free(*sort_data_list);
}

void print_sort_data_list(struct sort_data_list *sort_data_list) {
    printf("Sort Data List:\n{ start , end , byte }\n");
    struct sort_node *temp_node = sort_data_list->head;
    for( int i = 0 ; i < sort_data_list->length ; i++ ) {
        printf("{ %d , %d , %d }\n",temp_node->start,temp_node->end,temp_node->byte);
        temp_node = temp_node->next;
    }
}
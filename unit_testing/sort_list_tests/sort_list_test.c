#include "sort_list_test.h"

void initialize_sort_node_test(){                                           //Test if a valid ptr is returned
    int start = 1, end = 2, byte = 3;
    struct sort_node *sort_node = initialize_sort_node(start,end,byte);

    CU_ASSERT_PTR_NOT_NULL(sort_node);
    CU_ASSERT(sort_node->start == start);
    CU_ASSERT(sort_node->end == end);
    CU_ASSERT(sort_node->byte == byte);
    CU_ASSERT_PTR_NULL(sort_node->next);

    free(sort_node);
}

void initialize_sort_data_list_test(){                                      //Test if a valid ptr is returned

    struct sort_data_list *sort_data_list = initialize_sort_data_list();

    CU_ASSERT_PTR_NOT_NULL(sort_data_list);
    CU_ASSERT_PTR_NULL(sort_data_list->head);
    CU_ASSERT_PTR_NULL(sort_data_list->tail);
    CU_ASSERT(sort_data_list->length == 0);

    free_sort_data_list(&sort_data_list);
}

void pop_test(){                                                            //Test if the first sort_node is returned
    int start = 1, end = 2, byte = 3;
    int start2 = 4, end2 = 5, byte2 = 6;
    struct sort_node *sort_node;
    struct sort_data_list *sort_data_list = initialize_sort_data_list();

    push_at_the_end(&sort_data_list,start,end,byte);                        // push 2 nodes, then pop them and check the values        
    push_at_the_end(&sort_data_list,start2,end2,byte2);

    sort_node = pop(&sort_data_list);
    CU_ASSERT(sort_node->start == start);
    CU_ASSERT(sort_node->end == end);
    CU_ASSERT(sort_node->byte == byte);
    free(sort_node);

    sort_node = pop(&sort_data_list);
    CU_ASSERT(sort_node->start == start2);
    CU_ASSERT(sort_node->end == end2);
    CU_ASSERT(sort_node->byte == byte2);
    free(sort_node);

    free_sort_data_list(&sort_data_list);
}

void push_at_the_begining_test(){                                           //Test if the node is pushed at the begining
    int start = 1, end = 2, byte = 3;
    int start2 = 4, end2 = 5, byte2 = 6;
    struct sort_data_list *sort_data_list = initialize_sort_data_list();

    push_at_the_begining(&sort_data_list,start,end,byte);                  // push first node at the begining and check head

    CU_ASSERT(sort_data_list->head->start == start);
    CU_ASSERT(sort_data_list->head->end == end);
    CU_ASSERT(sort_data_list->head->byte == byte);

    push_at_the_begining(&sort_data_list,start2,end2,byte2);                // push second node at the begining and check head again

    CU_ASSERT(sort_data_list->head->start == start2);
    CU_ASSERT(sort_data_list->head->end == end2);
    CU_ASSERT(sort_data_list->head->byte == byte2);

    free_sort_data_list(&sort_data_list);
}

void push_at_the_end_test(){                                                //Test if the node is pushed at the end
    int start = 1, end = 2, byte = 3;
    int start2 = 4, end2 = 5, byte2 = 6;
    struct sort_data_list *sort_data_list = initialize_sort_data_list();

    push_at_the_end(&sort_data_list,start,end,byte);                        // push first node at the begining and check tail

    CU_ASSERT(sort_data_list->tail->start == start);
    CU_ASSERT(sort_data_list->tail->end == end);
    CU_ASSERT(sort_data_list->tail->byte == byte);

    push_at_the_end(&sort_data_list,start2,end2,byte2);                     // push second node at the begining and check tail again

    CU_ASSERT(sort_data_list->tail->start == start2);
    CU_ASSERT(sort_data_list->tail->end == end2);
    CU_ASSERT(sort_data_list->tail->byte == byte2);

    free_sort_data_list(&sort_data_list);
}
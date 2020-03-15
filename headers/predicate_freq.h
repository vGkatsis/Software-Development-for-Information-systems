#ifndef PREDICATE_FREQ_H
#define PREDICATE_FREQ_H

#include "header.h"

typedef struct predicate_freq{

    int relation;
    int column;
    int frequency;
    int times_swaped;
    struct predicate_freq *next;
} pred_freq;

typedef struct frequency_list{

    pred_freq *first;
    pred_freq *last;
}frequency_list;

void init_freqList(frequency_list **list);       //INitialize Predicate Frequency List

void add_last(frequency_list *list, pred_freq *new_element);            //Add An ELement Last IN The List
void remove_element(frequency_list *list, pred_freq *element);       //Remove An Element From The List

pred_freq *element_exists(frequency_list *list, int relation, int column);           //Check If An Element Is Already In The List

pred_freq *max_element(frequency_list *list);    //Find The ELement With Max Frequency

void free_freqList(frequency_list *list);       //Free The Predicate Frequency List
#endif

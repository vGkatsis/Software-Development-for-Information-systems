#include "../headers/predicate_freq.h"

void init_freqList(frequency_list **list){

    (*list) = malloc(1 * sizeof(frequency_list));

    (*list)->first = NULL;
    (*list)->last = NULL;
}

void add_last(frequency_list *list, pred_freq *new_element){

    if(list->first == NULL){

        list->first = new_element;
        list->last = list->first;
    }else{

        list->last->next = new_element;
        list->last = new_element;
    }
    
}

void remove_element(frequency_list *list, pred_freq *element){

    if(list->first == element){

        list->first = list->first->next;
    }else{

        pred_freq *tmp = list->first;
        while(tmp->next != element){
            
            tmp = tmp->next;
        }

        tmp->next = element->next;
    
        if(tmp->next == NULL){
            list->last = tmp;
        }
    }

    free(element);
}

pred_freq *element_exists(frequency_list *list, int relation, int column){

    pred_freq *tmp_element = list->first;

    while((tmp_element != NULL)){
          
        if(((tmp_element->relation == relation) &&
            (tmp_element->column == column))){
        
            return tmp_element; 
        }
        tmp_element = tmp_element->next;
    }

    return tmp_element;
}

pred_freq *max_element(frequency_list *list){

    if(list->first == NULL){
        return NULL;
    }

    int max = list->first->frequency;
    pred_freq *max_freq = list->first;
    pred_freq *temp = list->first;

    while(temp->next != NULL){
        temp = temp->next;
        
        if(temp->frequency > max){
            max = temp->frequency;
            max_freq = temp;
        }
    }
    
    if(max_freq->frequency < 1){
        return NULL;
    }

    return max_freq;
}

void free_freqList(frequency_list *list){

    pred_freq *temp = list->first;
    pred_freq *rmv = temp;
    while(temp != NULL){
        temp = temp->next;
        free(rmv);
        rmv = temp;
    }
    free(list);
}

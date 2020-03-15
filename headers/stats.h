#ifndef STATS_H
#define STATS_H

#include "header.h"

#define BOOLEAN_SIZE 50000000

typedef struct Stats{

    char empty;                                         //Check If Any Statistics Have Been Gathered     
    uint64_t min;                                       //Minimum Value Of A Column
    uint64_t max;                                       //Maximum Value Of A Column
    long double data_num;                               //Number Of Values In A Column(fa)
    long double unique_data;                            //Number Of Unique Values In A Column(da)
    int *boolean_array;                                 //Boolean Array To Find Unique Values
}stats;

typedef struct Stats_Node{

    stats *statistics;
    struct Stats_Node *next;
}stats_node;

typedef struct Relation_Node{

    int size;
    stats_node *stats;
    struct Relation_Node *next;
}relation_node;

typedef struct Stats_List{

    int relation_number;
    relation_node *head;
}stats_list;

void print_Statslist(stats_list *list);

//Utility Functions

void initialize_statsNode(stats_node **node, stats *new_statistics);    //Initialize A Stats_Node struct
void initialize_relationNode(relation_node **node);             //Initialize A Relation Node
void initialize_statsList(stats_list **list);                   //Initialize A Stats List

void copy_statsList(stats_list *list, stats_list **new_list);            //Copy An Existing List To A New List

void add_last_statsNode(relation_node *node, stats_node *new_node);     //Add A Stats Node At The End Of The Stats List Of A Relation_Node
void add_last_relationNode(stats_list *list, relation_node *new_relation);   //Add A Relation Node At THe End Of The Relation List Of A Stats List Struct 

void free_statsNode(stats_node *node);              //Free A Stats Node
void free_relationNode(relation_node *node);        //Free A Relation Node
void free_statsList(stats_list *list);              //Free A Stats List

void initialize_stats(stats **statistics);          //Initialize Stats
void initialize_bolleanArray(stats *statistics);    //Initialize The Boolean Array Of A Stats Struct

void find_Unique(stats *statistics);                //Find All Unique Values Based On Boolean Array
int find_inBoolean(stats *statistics, uint64_t value);          //Find If A Value Exists In A Boolean Array
void fill_booleanArray(stats *statistics, uint64_t value);      //Insert A Value Into The Boolean Array

void is_min(stats *statistics, uint64_t value);     //Checks If A Value Is Less Than The Current Min Value
void is_max(stats *statistics, uint64_t value);     //Checks If A Value Is Greater Than The Current Max Value                 

void free_booleanArray(stats *statistics);          //Free A Boolean Array

//Getters And Setters

uint64_t get_min(stats *statistics);                //Getter For Min Value
uint64_t get_max(stats *statistics);                //Getter For Max Value
long double get_dataNum(stats *statistics);            //Getter For Data Number Value
long double get_uniqueData(stats *statistics);         //Getter For Unique Data Value

void set_min(stats *statistics, uint64_t new_min);          //Seτter For Min Value
void set_max(stats *statistics, uint64_t new_max);          //Seτter For Min Value
void set_dataNum(stats *statistics, long double new_dataNum);        //Seτter For Min Value
void set_uniqieData(stats *statistics, long double new_uniqueData);  //Seτter For Min Value
void set_booleanArrayValue(stats *statistics, int value, int boolean);     //Sette For Boolean Array Values

void update_dataNum(stats *statistics);             //Update Data Num By Adding One
void update_uniqueData(stats *statistics);          //Update Unique Data By Adding One

#endif 
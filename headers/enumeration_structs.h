#ifndef ENUMERATION_STRUCTS_H
#define ENUMERATION_STRUCTS_H

#include "header.h"
#include "stats.h"
#include "sql_query.h"
#include "../headers/pop_estimation.h"

//Struct Representing A Node Of A BitVector
typedef struct Bit_Vector_Node{

    int position;
    int exists;
}bitVector_node;

//Struct Representing A BitVector
typedef struct Bit_Vector{

    int relations_num;
    int relation_length;
    long double cost;
    stats_list *bitVector_stats;
    bitVector_node *joinTree;
}bit_vector;

//Struct Representing A HashTable
typedef struct Hash_Table{

    int table_size;
    bit_vector **table;
}hash_table;

//Struct Representing A Relation
typedef struct Query_Relation{

    int relation;
    int column;
    struct Query_Relation *next;
}query_relation;

//Struct Representing A Set Of Relations
typedef struct Relation_Set{

    int set_length;
    query_relation *first;
    query_relation *last;
    struct Relation_Set *next;
}relation_set;

//Struct Representing A Set Of Sets Of Relations
typedef struct Super_Set{

    int number_of_sets;
    relation_set *first;
    relation_set *last;
}super_set;

//Utility Functions For Bit Vector Structs

void initialize_bitVector(bit_vector **vector, int relations_num, stats_list *stats);          //Initialize A BitVector

void initialize_bitVectorNode(bitVector_node *vector_node);         //Initialize A BitVector Node

void parse_set(relation_set *relations, relation_set *set, bit_vector *vector);      //Transform A Set Of Relations Into A BitVector

void parse_bitVector(relation_set *relations, bit_vector *vector, relation_set **set);     //Transform A Bit Vector Into A Set

bit_vector* joinTree_union(bit_vector *vector1, bit_vector *vector2);    //Join An Existing Relation With A New Relation

int connected(
    
);          //Search If There Is A Join Of A Single Relation With A Set Of Relations

int bit_hash(bit_vector *vector);                                   //Bit Vector Hash Function Returns The Number Coresponding To The Binary Value Of The Table

void newJoinTree_cost(bit_vector *vector,relation_set *relation_set, struct sql_query *query);   //Find The Cost Of A Join Tree

void free_bitVector(bit_vector *vector);                            //Free A Bit Vector


//Utility Functions For Hash Table Struct


void initialize_hashTable(hash_table **bestTree, int relations_num);        //Initialize A Hash Table Struct

int fill_hashTable(hash_table *bestTree, bit_vector *joinTree);    //Insert A Hash Table With A Bit Vector

long double findJoinTree_cost(hash_table *bestTree ,bit_vector *vector); //Get The Cost Of An Entry Of THe Hash Table

bit_vector* find_Tree(hash_table *bestTree, bit_vector *joinTree);  //Find The Best Tree Of A Bit Vector

void free_hashTable(hash_table *bestTree);                          //Free A Hash Table


//Utility Functions For Query Relation Structs

void initialize_queryRelation(query_relation **relation, int rel, int col);    //Initialize A Query Relation
void init_relationSet(relation_set **set);       //Initialize A Relation Set
void initialize_superSet(super_set **super);     //Initialize A Super Set

void add_atEnd(relation_set *set, query_relation *new_element);       //Add An Element Last In The Set
void add_lastSet(super_set *super, relation_set *new_set);            //Add A Set Last In The Super Set

query_relation* get_first(relation_set *set);    //Get The First Relation In THe List
relation_set * get_firstSet(super_set *super);   //Get The First Set In The Super Set

int element_inSet(relation_set *set, int relation, int column);           //Check If An Element Is Already In The Set

void free_queryRelation(query_relation *relation); //Free The Relation
void free_relationSet(relation_set *list);       //Free The Relation Set
void free_superSet(super_set *super);            //Free The Super Set


//Print Functions
void print_hashTable(hash_table *hash);
void print_superSet(super_set *super);
void print_relationSet(relation_set *set);
#endif
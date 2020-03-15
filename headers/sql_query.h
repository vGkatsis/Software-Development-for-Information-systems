#ifndef sql_query_H
#define sql_query_H

#include "header.h"
#include "vector.h"
#include "file_array.h"
//#include "predicate_freq.h"

enum{
	ROW_A, COLUMN_A, OPERATOR, ROW_B, COLUMN_B
};

enum{
	EQUAL, GREATER, LESS
};

typedef struct predicate {
    vector(int) predicate;
}Predicate;

struct projection {
    vector(int) projection;
};

typedef struct sql_query {
    vector(int) relations;
    vector(struct predicate) predicates;
	vector(struct predicate) filters;
	vector(struct predicate) joins;
    vector(struct projection) projections;
}Query;

struct sql_query *initialize_sql_query();
void split_sql_query(struct sql_query *,char *);
void print_sql_query(struct sql_query *);
void free_sql_query(struct sql_query **);
void parse_relation_query(struct sql_query * , char * );
void print_relation_query(struct sql_query *);
void parse_predicate_query(struct sql_query * , char * );
void print_predicate_query(struct sql_query *);
void parse_projection_query(struct sql_query * , char * );
void print_projection_query(struct sql_query *);
int sort_optimization(struct sql_query *);
int bringUP_filters(struct sql_query *);
int frequency_optimization(struct sql_query *);

#endif

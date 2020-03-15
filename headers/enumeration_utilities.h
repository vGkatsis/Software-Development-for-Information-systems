#ifndef ENUMERATION_UTILITIES_H
#define ENUMERATION_UTILITIES_H

#include "header.h"
#include "enumeration_structs.h"

void all_subsets(query_relation *relation, super_set *subSets, query_relation data[], int start, int end, int index, int r);            //Return All Subsets Of A Set Of Relations

#endif
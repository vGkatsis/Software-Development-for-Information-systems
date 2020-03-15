#include "../headers/enumeration_utilities.h"

void all_subsets(query_relation *relations, super_set *subSet, query_relation data[], int start, int end, int index, int length){
    
    int j, i;

    if (index == length) {                                                      //Insert Set Of Requested Length In The Subset
        query_relation *temp_relation;
        relation_set *temp_set;
        init_relationSet(&temp_set);
        for (j = 0; j < length; j++){
            initialize_queryRelation(&temp_relation,data[j].relation, data[j].column);
            add_atEnd(temp_set,temp_relation);
        }
        add_lastSet(subSet,temp_set);
        return;
    }

    for (i = start; i <= end; i++)
    {
        data[index].relation = relations->relation;
        data[index].column   = relations->column;

        relations = relations->next;
        all_subsets(relations,subSet, data, i+1, end, index+1, length);
    }
}

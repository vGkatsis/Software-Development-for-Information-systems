CC = gcc
CFLAGS = -g -Wall -O3 
EXECS = project
OBJS = relation.o list_results.o radix_sort.o sort_list.o utilities.o file_array.o sql_query.o intermidiate_result.o predicate_frequency.o job_scheduler.o stats.o pop_estimation.o join_enumeration.o enumeration_utilities.o enumeration_structs.o

all: main usage

# main program
main: $(OBJS) main.c 
	$(CC) $(CFLAGS) $(OBJS) main.c -o $(EXECS) -lpthread -lm
	rm -rf $(OBJS)

relation.o: relation.c 
	$(CC) $(CFLAGS) -c relation.c

list_results.o: list_results.c 
	$(CC) $(CFLAGS) -c list_results.c

radix_sort.o: radix_sort.c
	$(CC) $(CFLAGS) -c radix_sort.c

sort_list.o: sort_list.c
	$(CC) $(CFLAGS) -c sort_list.c

utilities.o: utilities.c
	$(CC) $(CFLAGS) -c utilities.c

file_array.o: file_array.c
	$(CC) $(CFLAGS) -c file_array.c
    
sql_query.o: sql_query.c
	$(CC) $(CFLAGS) -c sql_query.c
	
predicate_frequency.o: predicate_frequency.c
	$(CC) $(CFLAGS) -c predicate_frequency.c

intermidiate_result.o: intermidiate_result.c
	$(CC) $(CFLAGS) -c intermidiate_result.c

job_scheduler.o: job_scheduler.c
	$(CC) $(CFLAGS) -c job_scheduler.c

stats.o: stats.c
	$(CC) $(CFLAGS) -c stats.c

pop_estimation.o: pop_estimation.c
	$(CC) $(CFLAGS) -c pop_estimation.c

join_enumeration.o: join_enumeration.c
	$(CC) $(CFLAGS) -c join_enumeration.c

enumeration_utilities.o: enumeration_utilities.c
	$(CC) $(CFLAGS) -c enumeration_utilities.c

enumeration_structs.o: enumeration_structs.c
	$(CC) $(CFLAGS) -c enumeration_structs.c

usage:
	@echo 'Usage of Project:'
	@echo 'valgrind ./project < ../inputfiles/inputfile_small'

clean: 
	rm -f $(OBJS) $(EXECS)

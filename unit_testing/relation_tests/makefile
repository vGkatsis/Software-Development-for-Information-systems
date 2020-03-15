CC = gcc
CFLAGS = -g -c -lm
LIBS = cunit
SRCS = main.c relation_test.c ../../source/relation.c ../../source/list_results.c ../../source/radix_sort.c ../../source/sort_list.c
PROG = test

OBJS = $(SRCS:.c=.o)

all: $(OBJS)
		$(CC) -g $(OBJS) -l $(LIBS) -o $(PROG)

main.o: main.c
		$(CC) $(CFLAGS) main.c

relation_test.o: relation_test.c
		$(CC) $(CFLAGS) relation_test.c

relation.o: relation.c
		$(CC) $(CFLAGS) relation.c

radix_sort.o: radix_sort.c
		$(CC) $(CFLAGS) radix_sort.c

sort_list.o: sort_list.c
		$(CC) $(CFLAGS) sort_list.c

clean:
		rm -f $(OBJS) $(PROG)

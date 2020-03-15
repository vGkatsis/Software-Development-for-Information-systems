CC = gcc
CFLAGS = -g -c -lm
LIBS = cunit
SRCS = main.c radix_sort_test.c ../../source/radix_sort.c ../../source/sort_list.c ../../source/relation.c ../../source/list_results.c
PROG = test

OBJS = $(SRCS:.c=.o)

all: $(OBJS)
		$(CC) -g $(OBJS) -l $(LIBS) -o $(PROG)

main.o: main.c
		$(CC) $(CFLAGS) main.c

radix_sort_test.o: radix_sort_test.c
		$(CC) $(CFLAGS) radix_sort_test.c

radix_sort.o: radix_sort.c
		$(CC) $(CFLAGS) radix_sort.c

sort_list.o: sort_list.c
		$(CC) $(CFLAGS) sort_list.c

relation.o: relation.c
		$(CC) $(CFLAGS) relation.c

list_results.o: list_results.c
		$(CC) $(CFLAGS) list_results.c

clean:
		rm -f $(OBJS) $(PROG)
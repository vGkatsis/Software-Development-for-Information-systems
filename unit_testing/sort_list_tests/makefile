CC = gcc
CFLAGS = -g -c -lm
LIBS = cunit
SRCS = main.c sort_list_test.c ../../source/sort_list.c
PROG = test

OBJS = $(SRCS:.c=.o)

all: $(OBJS)
		$(CC) -g $(OBJS) -l $(LIBS) -o $(PROG)

main.o: main.c
		$(CC) $(CFLAGS) main.c

sort_list_test.o: sort_list_test.c
		$(CC) $(CFLAGS) sort_list_test.c

sort_list.o: sort_list.c
		$(CC) $(CFLAGS) sort_list.c

clean:
		rm -f $(OBJS) $(PROG)
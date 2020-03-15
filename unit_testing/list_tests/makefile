CC = gcc
CFLAGS = -g -c -lm
LIBS = cunit
SRCS = main.c list_test.c ../../source/list_results.c
PROG = test

OBJS = $(SRCS:.c=.o)

all: $(OBJS)
		$(CC) -g $(OBJS) -l $(LIBS) -o $(PROG)

main.o: main.c
		$(CC) $(CFLAGS) main.c

list_test.o: list_test.c
		$(CC) $(CFLAGS) list_test.c

list_results.o: list_results.c
		$(CC) $(CFLAGS) list_results.c

clean:
		rm -f $(OBJS) $(PROG)
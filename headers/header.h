#ifndef header_H
#define header_H

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <pthread.h>
//#include <CUnit/CUnit.h>
//#include <CUnit/Basic.h>

#define NUMBER_OF_THREADS 4
#define BUCKET_SIZE 256

struct histogram_indexing {
    int size;
    int indexes[BUCKET_SIZE];
};

struct join_partition {
    uint64_t *histogram;
    struct histogram_indexing histogram_indexes;
    uint64_t *prefix_sum;
};

#endif

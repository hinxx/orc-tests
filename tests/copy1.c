#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <string.h>



#define SZ 1024*1024

volatile int64_t dest[SZ];
volatile int64_t src64[SZ];
volatile int32_t src32[SZ];
volatile int16_t src16[SZ];
volatile int8_t src8[SZ];

void loop(char sel) {
    struct timespec s, e;
    clock_gettime(CLOCK_MONOTONIC, &s);

    if (sel == '1') {
        for (size_t i = 0; i < SZ; i++) {
            dest[i] = src8[i];
        }
    } else if (sel == '2') {
        for (size_t i = 0; i < SZ; i++) {
            dest[i] = src16[i];
        }
    } else if (sel == '4') {
        for (size_t i = 0; i < SZ; i++) {
            dest[i] = src32[i];
        }
    } else if (sel == '8') {
        for (size_t i = 0; i < SZ; i++) {
            dest[i] = src64[i];
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &e);
    uint64_t took = (e.tv_sec - s.tv_sec) * 1e9 + (e.tv_nsec - s.tv_nsec);
    printf("loop copy of %c byte data type took %ld ns, %.3f us\n", sel, took, (double)took / 1.0e3);
}


void memcopy() {
    struct timespec s, e;
    clock_gettime(CLOCK_MONOTONIC, &s);

    memcpy((void *)dest, (void *)src64, SZ + sizeof(int64_t));

    clock_gettime(CLOCK_MONOTONIC, &e);
    uint64_t took = (e.tv_sec - s.tv_sec) * 1e9 + (e.tv_nsec - s.tv_nsec);
    printf("memcpy of 8 byte data type took %ld ns, %.3f us\n", took, (double)took / 1.0e3);
}

int main(int argc, char **argv) {

    char sel = '0';
    if (argc > 1) {
        sel = argv[1][0];
    }

    if (sel > '0') {
        loop(sel);
    } else {
        memcopy();
    }
    return 0;
}
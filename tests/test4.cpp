#include <stdio.h>
#include <stdlib.h>

struct arena {
    struct arena *next;
    char *limit;
    char *avail;
};

struct arena firsts[1] = {0};
struct arena *arenas[1] = { &firsts[0] };


void initialize(int t) {
    // initialize the first arena as empty (list head)
    firsts[t].next = NULL;
    firsts[t].avail = (char *)&firsts[t] + sizeof(&firsts[t]);
    firsts[t].limit = firsts[t].avail;
    printf("%s: first %p, next %p, limit %p, avail %p\n", __func__, arenas[t], arenas[t]->next, arenas[t]->limit, arenas[t]->avail);
}

char *allocate(int n, struct arena **p) {
    struct arena *ap;
    printf("%s: need %d\n", __func__, n);

    // iterate while there is *not* enough space found in the arena
    for (ap = *p; (ap->avail + n) > ap->limit; *p = ap) {
        printf("%s: 1 arena %p, next %p, limit %p, avail %p, free %ld\n", __func__, ap, ap->next, ap->limit, ap->avail, ap->limit - ap->avail);
        if (ap->next) {
            // move to next arena if the current is full
            ap = ap->next;
            ap->avail = (char *)ap + sizeof(*ap);
            printf("%s: 2 arena %p, next %p, limit %p, avail %p, free %ld\n", __func__, ap, ap->next, ap->limit, ap->avail, ap->limit - ap->avail);
        } else {
            // allocate new arena if no free arenas in list
            int sz = 4 * 1024;
            ap->next = (struct arena *)calloc(1, sz);
            ap = ap->next;
            ap->limit = (char *)ap + sz;
            ap->avail = (char *)ap + sizeof(*ap);
            ap->next = NULL;
            printf("%s: 3 arena %p, next %p, limit %p, avail %p, free %ld\n", __func__, ap, ap->next, ap->limit, ap->avail, ap->limit - ap->avail);
        }
    }

    // printf("return %p\n", ap->avail);
    ap->avail += n;
    return ap->avail - n;
}

void deallocate(int t) {
    printf("%s: first %p, next %p, limit %p, avail %p\n", __func__, &firsts[t], firsts[t].next, firsts[t].limit, firsts[t].avail);
    if (firsts[t].next) {
        arenas[t] = firsts[t].next;
        arenas[t]->avail = (char *)arenas[t] + sizeof(*arenas[t]);
    } else {
        arenas[t] = &firsts[t];
    }
    printf("%s: arena %p, next %p, limit %p, avail %p\n", __func__, arenas[t], arenas[t]->next, arenas[t]->limit, arenas[t]->avail);
}

void destroy(int t) {
    struct arena *head, *tmp;
    printf("%s: first %p, next %p, limit %p, avail %p\n", __func__, &firsts[t], firsts[t].next, firsts[t].limit, firsts[t].avail);
    head = firsts[t].next;
    while (head) {
        printf("%s: head %p, next %p, limit %p, avail %p\n", __func__, head, head->next, head->limit, head->avail);
        tmp = head->next;
        free(head);
        head = tmp;
    }
}

char *alloc(int k) {
    char *p = NULL;
    if ((arenas[0]->avail + k) > arenas[0]->limit) {
        // not enough space in the current arena, allocate a new one
        p = allocate(k, &arenas[0]);
    } else {
        // return the start of next available space
        p = arenas[0]->avail;
        // move the avail pointer
        arenas[0]->avail += k;
    }

    printf("p = %p\n", p);
    return p;
}

int main(int argc, char const *argv[]) {

    initialize(0);

    {
        alloc(100);
    }
    {
        alloc(1000);
    }
    {
        alloc(3000);
    }
    {
        alloc(2000);
    }
    deallocate(0);
    {
        alloc(2000);
    }
    deallocate(0);
    deallocate(0);
    deallocate(0);
    deallocate(0);
    {
        alloc(2000);
    }

    destroy(0);

    return 0;
}

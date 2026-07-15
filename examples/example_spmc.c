/**
 * Example: SPMC — single producer, multi-consumer *ring* (raw queue demo).
 *
 * The worker pool always has one consumer thread per shard. SPMC mode on the
 * pool still works (1 consumer is valid SPMC); this example shows true multi-
 * consumer drain on a standalone ring for completeness.
 */
#include "awp/awp.h"
#include "../src/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

static awp_frame_t g_frames[256];
static atomic_int g_got;

typedef struct {
    awp_ring_t *r;
    int id;
} cons_t;

static void *consumer(void *arg)
{
    cons_t *c = arg;
    for (;;) {
        awp_frame_t *f = NULL;
        if (awp_ring_pop(c->r, &f) != 0)
            break;
        if (f) {
            printf("[SPMC] consumer=%d seq=%llu symbol=%s\n",
                   c->id, (unsigned long long)f->seq, f->symbol);
            atomic_fetch_add(&g_got, 1);
        }
    }
    return NULL;
}

int main(void)
{
    awp_ring_t ring;
    cons_t ca[3];
    pthread_t th[3];
    int i;
    const int N = 30;

    atomic_init(&g_got, 0);
    if (awp_ring_init(&ring, 64, AWP_RING_SPMC) != 0)
        return 1;

    for (i = 0; i < 3; i++) {
        ca[i].r = &ring;
        ca[i].id = i;
        pthread_create(&th[i], NULL, consumer, &ca[i]);
    }

    /* Single producer. */
    for (i = 0; i < N; i++) {
        awp_frame_t *f = &g_frames[i];
        memset(f, 0, sizeof(*f));
        f->seq = (uint64_t)i;
        snprintf(f->symbol, sizeof(f->symbol), "S%d", i % 5);
        if (awp_ring_push(&ring, f, NULL) != 0) {
            fprintf(stderr, "push failed\n");
            break;
        }
    }

    {
        int spins = 0;
        while (atomic_load(&g_got) < N && spins < 100000) {
            spins++;
            sched_yield();
        }
    }
    awp_ring_close(&ring);
    for (i = 0; i < 3; i++)
        pthread_join(th[i], NULL);

    printf("SPMC example done: consumed=%d expect=%d\n", atomic_load(&g_got), N);
    awp_ring_destroy(&ring);
    return atomic_load(&g_got) == N ? 0 : 1;
}

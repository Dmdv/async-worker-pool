/**
 * Raw ring micro-benchmark for SPSC/MPSC/SPMC/MPMC (no pool overhead).
 *
 * Usage: bench_ring [ops] [spsc|mpsc|spmc|mpmc|all]
 */
#include "awp/awp.h"
#include "../src/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <sched.h>

static awp_frame_t g_slab[1]; /* dummy payload pointer target */

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static const char *mode_name(awp_ring_mode_t m)
{
    switch (m) {
    case AWP_RING_SPSC: return "SPSC";
    case AWP_RING_MPSC: return "MPSC";
    case AWP_RING_SPMC: return "SPMC";
    case AWP_RING_MPMC: return "MPMC";
    default: return "?";
    }
}

typedef struct {
    awp_ring_t *r;
    int n;
    atomic_int *done;
    atomic_int *errs;
} thr_t;

static void *prod(void *arg)
{
    thr_t *a = arg;
    int i;
    for (i = 0; i < a->n; i++) {
        if (awp_ring_push(a->r, g_slab, NULL) != 0)
            atomic_fetch_add(a->errs, 1);
    }
    return NULL;
}

static void *cons(void *arg)
{
    thr_t *a = arg;
    int got = 0;
    while (got < a->n) {
        awp_frame_t *f = NULL;
        if (awp_ring_pop(a->r, &f) != 0)
            break;
        if (f)
            got++;
    }
    atomic_fetch_add(a->done, got);
    return NULL;
}

static void run_mode(awp_ring_mode_t mode, int ops)
{
    awp_ring_t ring;
    int n_prod = (mode == AWP_RING_SPSC || mode == AWP_RING_SPMC) ? 1 : 4;
    int n_cons = (mode == AWP_RING_SPSC || mode == AWP_RING_MPSC) ? 1 : 4;
    thr_t pa[4], ca[4];
    pthread_t pt[4], ct[4];
    atomic_int done, errs;
    int i, per_p, per_c;
    uint64_t t0, t1;

    per_p = ops / n_prod;
    per_c = ops / n_cons;
    atomic_init(&done, 0);
    atomic_init(&errs, 0);

    if (awp_ring_init(&ring, 1024, mode) != 0) {
        printf("%-6s INIT_FAIL\n", mode_name(mode));
        return;
    }

    t0 = now_ns();
    for (i = 0; i < n_cons; i++) {
        ca[i].r = &ring;
        ca[i].n = (i == n_cons - 1) ? (ops - per_c * (n_cons - 1)) : per_c;
        ca[i].done = &done;
        ca[i].errs = &errs;
        pthread_create(&ct[i], NULL, cons, &ca[i]);
    }
    for (i = 0; i < n_prod; i++) {
        pa[i].r = &ring;
        pa[i].n = (i == n_prod - 1) ? (ops - per_p * (n_prod - 1)) : per_p;
        pa[i].done = &done;
        pa[i].errs = &errs;
        pthread_create(&pt[i], NULL, prod, &pa[i]);
    }
    for (i = 0; i < n_prod; i++)
        pthread_join(pt[i], NULL);

    {
        int spins = 0;
        while (atomic_load(&done) < ops && spins < 500000) {
            spins++;
            sched_yield();
        }
    }
    awp_ring_close(&ring);
    for (i = 0; i < n_cons; i++)
        pthread_join(ct[i], NULL);
    t1 = now_ns();

    printf("%-6s prods=%d cons=%d ops=%d thr=%.0f msg/s errs=%d consumed=%d %s\n",
           mode_name(mode), n_prod, n_cons, ops,
           (double)ops / ((double)(t1 - t0) / 1e9),
           atomic_load(&errs), atomic_load(&done),
           (atomic_load(&errs) == 0 && atomic_load(&done) == ops) ? "PASS"
                                                                   : "FAIL");

    awp_ring_destroy(&ring);
}

int main(int argc, char **argv)
{
    int ops = 100000;
    awp_ring_mode_t modes[] = {
        AWP_RING_SPSC, AWP_RING_MPSC, AWP_RING_SPMC, AWP_RING_MPMC
    };
    int i;
    const char *sel = "all";

    if (argc > 1)
        ops = atoi(argv[1]);
    if (argc > 2)
        sel = argv[2];

    printf("bench_ring ops=%d\n", ops);
    for (i = 0; i < 4; i++) {
        if (strcmp(sel, "all") != 0) {
            const char *want = mode_name(modes[i]);
            char lower[8];
            int j;
            for (j = 0; want[j] && j < 7; j++)
                lower[j] = (char)(want[j] | 0x20);
            lower[j] = 0;
            if (strcmp(sel, lower) != 0 && strcmp(sel, want) != 0)
                continue;
        }
        run_mode(modes[i], ops);
    }
    return 0;
}

/**
 * Exercise all ring concurrency modes: SPSC, MPSC, SPMC, MPMC.
 */
#include "awp/awp.h"
#include "../src/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <time.h>

static int g_fails;
static int g_passes;

#define CHECK(c, m) do { \
    if (!(c)) { fprintf(stderr, "  FAIL: %s (%s:%d)\n", m, __FILE__, __LINE__); g_fails++; } \
    else g_passes++; \
} while (0)

static awp_frame_t g_frames[8192];
static atomic_uint g_next_frame;

static awp_frame_t *alloc_frame(void)
{
    uint32_t i = atomic_fetch_add(&g_next_frame, 1);
    if (i >= 8192)
        return NULL;
    memset(&g_frames[i], 0, sizeof(g_frames[i]));
    g_frames[i].seq = i;
    return &g_frames[i];
}

typedef struct {
    awp_ring_t *r;
    int n_push;
    atomic_int *errors;
    atomic_int *consumed;
} thr_args_t;

static void *producer(void *arg)
{
    thr_args_t *a = arg;
    int i;
    for (i = 0; i < a->n_push; i++) {
        awp_frame_t *f = alloc_frame();
        if (!f || awp_ring_push(a->r, f, NULL) != 0)
            atomic_fetch_add(a->errors, 1);
    }
    return NULL;
}

static void *consumer(void *arg)
{
    thr_args_t *a = arg;
    for (;;) {
        awp_frame_t *f = NULL;
        if (awp_ring_pop(a->r, &f) != 0)
            break;
        if (f)
            atomic_fetch_add(a->consumed, 1);
    }
    return NULL;
}

static const char *mode_name(awp_ring_mode_t mode)
{
    switch (mode) {
    case AWP_RING_SPSC: return "SPSC";
    case AWP_RING_MPSC: return "MPSC";
    case AWP_RING_SPMC: return "SPMC";
    case AWP_RING_MPMC: return "MPMC";
    default: return "?";
    }
}

static void test_mode(awp_ring_mode_t mode, int n_prod, int n_cons, int per)
{
    awp_ring_t ring;
    thr_args_t pa[8], ca[8];
    pthread_t pt[8], ct[8];
    atomic_int errors, consumed;
    int i, total_in;

    printf("test_ring %s prods=%d cons=%d per=%d\n",
           mode_name(mode), n_prod, n_cons, per);
    atomic_store(&g_next_frame, 0);
    atomic_init(&errors, 0);
    atomic_init(&consumed, 0);
    CHECK(awp_ring_init(&ring, 128, mode) == 0, "ring init");

    total_in = n_prod * per;

    for (i = 0; i < n_cons; i++) {
        ca[i].r = &ring;
        ca[i].n_push = 0;
        ca[i].errors = &errors;
        ca[i].consumed = &consumed;
        pthread_create(&ct[i], NULL, consumer, &ca[i]);
    }
    for (i = 0; i < n_prod; i++) {
        pa[i].r = &ring;
        pa[i].n_push = per;
        pa[i].errors = &errors;
        pa[i].consumed = &consumed;
        pthread_create(&pt[i], NULL, producer, &pa[i]);
    }
    for (i = 0; i < n_prod; i++)
        pthread_join(pt[i], NULL);

    {
        int spins = 0;
        while ((int)atomic_load(&consumed) < total_in && spins < 200000) {
            spins++;
            sched_yield();
        }
    }
    awp_ring_close(&ring);
    for (i = 0; i < n_cons; i++)
        pthread_join(ct[i], NULL);

    CHECK(atomic_load(&errors) == 0, "no push errors");
    CHECK(atomic_load(&consumed) == total_in, "all frames consumed");
    CHECK((int)atomic_load(&g_next_frame) == total_in, "frame alloc count");

    awp_ring_destroy(&ring);
}

static int count_process(const awp_frame_t *f, void *u)
{
    (void)f;
    atomic_fetch_add((atomic_uint_fast64_t *)u, 1);
    return 0;
}

static void test_pool_all_modes(void)
{
    awp_ring_mode_t modes[] = {
        AWP_RING_SPSC, AWP_RING_MPSC, AWP_RING_SPMC, AWP_RING_MPMC
    };
    int m;

    for (m = 0; m < 4; m++) {
        awp_config_t cfg;
        awp_pool_t *pool = NULL;
        atomic_uint_fast64_t count;
        int i;
        const int N = 100;

        printf("test_pool ring_mode=%s\n", mode_name(modes[m]));
        atomic_store(&count, 0);
        awp_config_init(&cfg);
        cfg.n_workers = 4;
        cfg.queue_capacity = 32;
        cfg.frame_pool_size = 256;
        cfg.ring_mode = modes[m];
        cfg.enable_supervisor = 0;
        cfg.process = count_process;
        cfg.user = &count;

        CHECK(awp_pool_create(&cfg, &pool) == 0, "create");
        /* Single-threaded submit is valid under every mode. */
        for (i = 0; i < N; i++) {
            char sym[16];
            snprintf(sym, sizeof(sym), "S%d", i % 8);
            CHECK(awp_submit(pool, "trades", sym, "x", 1, 0) == 0, "submit");
        }
        {
            int w = 0;
            while (atomic_load(&count) < (uint64_t)N && w < 5000) {
                struct timespec ts = { .tv_sec = 0, .tv_nsec = 1000000L };
                nanosleep(&ts, NULL);
                w++;
            }
        }
        CHECK(atomic_load(&count) == (uint64_t)N, "all processed");
        awp_pool_shutdown(pool);
        awp_pool_destroy(pool);
    }
}

int main(void)
{
    test_mode(AWP_RING_SPSC, 1, 1, 500);
    test_mode(AWP_RING_MPSC, 4, 1, 200);
    test_mode(AWP_RING_SPMC, 1, 4, 400);
    test_mode(AWP_RING_MPMC, 4, 4, 100);

    test_pool_all_modes();

    printf("\nring_modes: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

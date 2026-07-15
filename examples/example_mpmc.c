/**
 * Example: MPMC — multi producer, multi consumer on a standalone ring.
 * Also shows pool usage with AWP_RING_MPMC (pool still SC per worker).
 */
#include "awp/awp.h"
#include "../src/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>

static awp_frame_t g_frames[1024];
static atomic_uint g_fi;
static atomic_int g_got;
static atomic_int g_errs;

typedef struct {
    awp_ring_t *r;
    int id;
    int n;
} thr_t;

static void *prod(void *arg)
{
    thr_t *t = arg;
    int i;
    for (i = 0; i < t->n; i++) {
        uint32_t idx = atomic_fetch_add(&g_fi, 1);
        awp_frame_t *f;
        if (idx >= 1024) {
            atomic_fetch_add(&g_errs, 1);
            break;
        }
        f = &g_frames[idx];
        memset(f, 0, sizeof(*f));
        f->seq = idx;
        snprintf(f->symbol, sizeof(f->symbol), "P%d", t->id);
        if (awp_ring_push(t->r, f, NULL) != 0)
            atomic_fetch_add(&g_errs, 1);
    }
    return NULL;
}

static void *cons(void *arg)
{
    thr_t *t = arg;
    for (;;) {
        awp_frame_t *f = NULL;
        if (awp_ring_pop(t->r, &f) != 0)
            break;
        if (f) {
            printf("[MPMC] c=%d seq=%llu from=%s\n", t->id,
                   (unsigned long long)f->seq, f->symbol);
            atomic_fetch_add(&g_got, 1);
        }
    }
    return NULL;
}

static int demo_ring(void)
{
    awp_ring_t ring;
    thr_t pa[3], ca[3];
    pthread_t pt[3], ct[3];
    int i;
    const int per = 20;
    const int total = 3 * per;

    atomic_store(&g_fi, 0);
    atomic_init(&g_got, 0);
    atomic_init(&g_errs, 0);

    if (awp_ring_init(&ring, 128, AWP_RING_MPMC) != 0)
        return 1;

    for (i = 0; i < 3; i++) {
        ca[i].r = &ring;
        ca[i].id = i;
        ca[i].n = 0;
        pthread_create(&ct[i], NULL, cons, &ca[i]);
    }
    for (i = 0; i < 3; i++) {
        pa[i].r = &ring;
        pa[i].id = i;
        pa[i].n = per;
        pthread_create(&pt[i], NULL, prod, &pa[i]);
    }
    for (i = 0; i < 3; i++)
        pthread_join(pt[i], NULL);

    {
        int spins = 0;
        while (atomic_load(&g_got) < total && spins < 200000) {
            spins++;
            sched_yield();
        }
    }
    awp_ring_close(&ring);
    for (i = 0; i < 3; i++)
        pthread_join(ct[i], NULL);

    printf("MPMC ring: got=%d expect=%d errs=%d\n",
           atomic_load(&g_got), total, atomic_load(&g_errs));
    awp_ring_destroy(&ring);
    return (atomic_load(&g_got) == total && atomic_load(&g_errs) == 0) ? 0 : 1;
}

static int pool_on_msg(const awp_frame_t *f, void *user)
{
    atomic_fetch_add((atomic_uint_fast64_t *)user, 1);
    (void)f;
    return 0;
}

typedef struct {
    awp_pool_t *p;
    int id;
} pool_sub_t;

static void *pool_submitter(void *a)
{
    pool_sub_t *s = a;
    int j;
    for (j = 0; j < 10; j++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "T%d", s->id);
        awp_submit(s->p, "trades", sym, "x", 1, 0);
    }
    return NULL;
}

static int demo_pool(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    atomic_uint_fast64_t n;
    pool_sub_t ps[4];
    pthread_t th[4];
    int i;

    atomic_store(&n, 0);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 256;
    cfg.ring_mode = AWP_RING_MPMC;
    cfg.enable_supervisor = 0;
    cfg.process = pool_on_msg;
    cfg.user = &n;

    if (awp_pool_create(&cfg, &pool) != 0)
        return 1;

    for (i = 0; i < 4; i++) {
        ps[i].p = pool;
        ps[i].id = i;
        pthread_create(&th[i], NULL, pool_submitter, &ps[i]);
    }
    for (i = 0; i < 4; i++)
        pthread_join(th[i], NULL);

    while (atomic_load(&n) < 40)
        sched_yield();
    printf("MPMC pool: processed=%llu\n", (unsigned long long)atomic_load(&n));
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return atomic_load(&n) == 40 ? 0 : 1;
}

int main(void)
{
    int rc = 0;
    rc |= demo_ring();
    rc |= demo_pool();
    printf("MPMC example %s\n", rc ? "FAIL" : "ok");
    return rc;
}

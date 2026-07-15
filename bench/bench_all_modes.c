/**
 * Benchmark every ring_mode; print a comparison table.
 *
 * Usage: bench_all_modes [msgs] [keys] [spsc|mpsc|spmc|mpmc|all]
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#define MAX_LAT 200000

static uint64_t g_lat_ns[MAX_LAT];
static atomic_uint_fast64_t g_lat_count;
static atomic_uint_fast64_t g_done;

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

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int bench_process(const awp_frame_t *frame, void *user)
{
    uint64_t n = atomic_fetch_add(&g_lat_count, 1);
    uint64_t dt = now_ns() - frame->submit_ns;
    (void)user;
    if (n < MAX_LAT)
        g_lat_ns[n] = dt;
    atomic_fetch_add(&g_done, 1);
    for (volatile int i = 0; i < 50; i++) {
    }
    return 0;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

typedef struct {
    awp_pool_t *pool;
    int start;
    int end;
    int n_keys;
    atomic_int *fails;
} prod_t;

static void *multi_prod(void *arg)
{
    prod_t *a = arg;
    int i;
    for (i = a->start; i < a->end; i++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "SYM%04d", i % a->n_keys);
        if (awp_submit(a->pool, "trades", sym, "x", 1, 0) != 0)
            atomic_fetch_add(a->fails, 1);
    }
    return NULL;
}

typedef struct {
    awp_ring_mode_t mode;
    int n_msgs;
    int n_keys;
    int n_prod;
    double thr;
    double p50_ms;
    double p99_ms;
    uint64_t drops;
    int ok;
} result_t;

static result_t run_one(awp_ring_mode_t mode, int n_msgs, int n_keys)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    result_t r;
    uint64_t t0, t1;
    uint64_t count;
    int n_prod;
    atomic_int fails;
    awp_pool_metrics_t m;

    memset(&r, 0, sizeof(r));
    r.mode = mode;
    r.n_msgs = n_msgs;
    r.n_keys = n_keys;

    /* Match topology: single producer for SPSC/SPMC. */
    n_prod = (mode == AWP_RING_SPSC || mode == AWP_RING_SPMC) ? 1 : 4;
    r.n_prod = n_prod;

    atomic_store(&g_lat_count, 0);
    atomic_store(&g_done, 0);
    atomic_init(&fails, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 32;
    cfg.queue_capacity = 256;
    cfg.frame_pool_size = 8192;
    cfg.ring_mode = mode;
    cfg.enable_supervisor = 0;
    cfg.process = bench_process;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "create failed for %s\n", mode_name(mode));
        return r;
    }

    t0 = now_ns();
    if (n_prod == 1) {
        int i;
        for (i = 0; i < n_msgs; i++) {
            char sym[32];
            snprintf(sym, sizeof(sym), "SYM%04d", i % n_keys);
            if (awp_submit(pool, "trades", sym, "x", 1, 0) != 0)
                atomic_fetch_add(&fails, 1);
        }
    } else {
        pthread_t th[4];
        prod_t pa[4];
        int i;
        int chunk = n_msgs / n_prod;
        for (i = 0; i < n_prod; i++) {
            pa[i].pool = pool;
            pa[i].start = i * chunk;
            pa[i].end = (i == n_prod - 1) ? n_msgs : (i + 1) * chunk;
            pa[i].n_keys = n_keys;
            pa[i].fails = &fails;
            pthread_create(&th[i], NULL, multi_prod, &pa[i]);
        }
        for (i = 0; i < n_prod; i++)
            pthread_join(th[i], NULL);
    }

    while (atomic_load(&g_done) < (uint64_t)n_msgs)
        usleep(50);
    t1 = now_ns();

    count = atomic_load(&g_lat_count);
    if (count > (uint64_t)n_msgs)
        count = (uint64_t)n_msgs;
    if (count > MAX_LAT)
        count = MAX_LAT;
    qsort(g_lat_ns, (size_t)count, sizeof(uint64_t), cmp_u64);

    r.thr = (double)n_msgs / ((double)(t1 - t0) / 1e9);
    r.p50_ms = count ? (double)g_lat_ns[count * 50 / 100] / 1e6 : 0;
    r.p99_ms = count ? (double)g_lat_ns[count * 99 / 100] / 1e6 : 0;
    awp_pool_get_metrics(pool, &m);
    r.drops = m.dropped;
    r.ok = (atomic_load(&fails) == 0 && m.dropped == 0 && r.p99_ms <= 5.0);

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return r;
}

static int parse_mode(const char *s, awp_ring_mode_t *out)
{
    if (!s)
        return 1;
    if (strcmp(s, "all") == 0)
        return 1;
    if (strcmp(s, "spsc") == 0) {
        *out = AWP_RING_SPSC;
        return 0;
    }
    if (strcmp(s, "mpsc") == 0) {
        *out = AWP_RING_MPSC;
        return 0;
    }
    if (strcmp(s, "spmc") == 0) {
        *out = AWP_RING_SPMC;
        return 0;
    }
    if (strcmp(s, "mpmc") == 0) {
        *out = AWP_RING_MPMC;
        return 0;
    }
    return -1;
}

int main(int argc, char **argv)
{
    int n_msgs = 5000;
    int n_keys = 1000;
    awp_ring_mode_t modes[] = {
        AWP_RING_SPSC, AWP_RING_MPSC, AWP_RING_SPMC, AWP_RING_MPMC
    };
    awp_ring_mode_t only;
    int all = 1;
    int i, any_fail = 0;
    result_t results[4];
    int nres = 0;

    if (argc > 1)
        n_msgs = atoi(argv[1]);
    if (argc > 2)
        n_keys = atoi(argv[2]);
    if (argc > 3) {
        int pr = parse_mode(argv[3], &only);
        if (pr < 0) {
            fprintf(stderr, "Usage: %s [msgs] [keys] [spsc|mpsc|spmc|mpmc|all]\n",
                    argv[0]);
            return 2;
        }
        all = pr;
    }
    if (n_msgs > MAX_LAT)
        n_msgs = MAX_LAT;

    printf("bench_all_modes msgs=%d keys=%d\n", n_msgs, n_keys);
    printf("%-6s %6s %12s %10s %10s %8s %s\n",
           "mode", "prods", "msg/s", "p50_ms", "p99_ms", "drops", "result");

    for (i = 0; i < 4; i++) {
        result_t r;
        if (!all && modes[i] != only)
            continue;
        r = run_one(modes[i], n_msgs, n_keys);
        results[nres++] = r;
        printf("%-6s %6d %12.0f %10.4f %10.4f %8llu %s\n",
               mode_name(r.mode), r.n_prod, r.thr, r.p50_ms, r.p99_ms,
               (unsigned long long)r.drops, r.ok ? "PASS" : "FAIL");
        if (!r.ok)
            any_fail = 1;
    }

    return any_fail ? 1 : 0;
}

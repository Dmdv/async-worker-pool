/**
 * Micro-benchmark: ~3k msg/s across ~1000 keys.
 * Reports dequeue-to-process latency p50/p99 (submit_ns → process entry).
 */
#include "awp/awp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#define N_KEYS    1000
#define N_MSGS    3000
#define MAX_LAT   (N_MSGS + 64)

static uint64_t g_lat_ns[MAX_LAT];
static atomic_uint_fast64_t g_lat_count;
static atomic_uint_fast64_t g_done;

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
    /* Simulate light publish work (~1–5 µs). */
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

int main(int argc, char **argv)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int n_msgs = N_MSGS;
    int n_keys = N_KEYS;
    int i;
    uint64_t t0, t1;
    awp_pool_metrics_t m;
    uint64_t count;
    double p50, p99, p999;

    if (argc > 1)
        n_msgs = atoi(argv[1]);
    if (argc > 2)
        n_keys = atoi(argv[2]);
    if (n_msgs > MAX_LAT)
        n_msgs = MAX_LAT;

    atomic_store(&g_lat_count, 0);
    atomic_store(&g_done, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 32;
    cfg.queue_capacity = 256;
    cfg.frame_pool_size = 4096;
    cfg.enable_supervisor = 0;
    cfg.process = bench_process;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "create failed\n");
        return 1;
    }

    t0 = now_ns();
    for (i = 0; i < n_msgs; i++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "SYM%04d", i % n_keys);
        if (awp_submit(pool, "trades", sym, "x", 1, 0) != 0) {
            fprintf(stderr, "submit fail at %d\n", i);
            return 1;
        }
    }
    while (atomic_load(&g_done) < (uint64_t)n_msgs)
        usleep(100);
    t1 = now_ns();

    count = atomic_load(&g_lat_count);
    if (count > (uint64_t)n_msgs)
        count = (uint64_t)n_msgs;
    qsort(g_lat_ns, (size_t)count, sizeof(uint64_t), cmp_u64);

    p50 = (double)g_lat_ns[count * 50 / 100] / 1e6;
    p99 = (double)g_lat_ns[count * 99 / 100] / 1e6;
    p999 = (double)g_lat_ns[count > 1000 ? count * 999 / 1000 : count - 1] / 1e6;

    awp_pool_get_metrics(pool, &m);

    printf("bench_dispatch: msgs=%d keys=%d workers=%u\n",
           n_msgs, n_keys, m.n_workers);
    printf("  throughput: %.0f msg/s  wall_ms=%.2f\n",
           (double)n_msgs / ((double)(t1 - t0) / 1e9),
           (double)(t1 - t0) / 1e6);
    printf("  latency_ms: p50=%.4f p99=%.4f p99.9=%.4f\n", p50, p99, p999);
    printf("  drops=%llu process_errors=%llu\n",
           (unsigned long long)m.dropped,
           (unsigned long long)m.process_errors);
    printf("  target: p99 <= 5.0 ms, drops == 0 → %s\n",
           (p99 <= 5.0 && m.dropped == 0) ? "PASS" : "FAIL");

    printf("  worst-worker occupancy:\n");
    {
        uint64_t worst_proc = 0, worst_hwm = 0;
        int wi = 0;
        for (i = 0; i < (int)m.n_workers; i++) {
            if (m.workers[i].processed > worst_proc) {
                worst_proc = m.workers[i].processed;
                wi = i;
            }
            if (m.workers[i].queue_hwm > worst_hwm)
                worst_hwm = m.workers[i].queue_hwm;
        }
        printf("    hottest worker=%d processed=%llu global_hwm=%llu\n",
               wi, (unsigned long long)worst_proc, (unsigned long long)worst_hwm);
    }

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);

    return (p99 <= 5.0 && m.dropped == 0) ? 0 : 1;
}

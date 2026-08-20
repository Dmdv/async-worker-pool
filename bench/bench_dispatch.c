/**
 * High-Scale Nanosecond Dispatch Benchmark
 * Measures end-to-end submit -> queue -> worker -> SIMD process callback latency.
 */
#include "awp/awp.h"
#include "bench_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

#define DEFAULT_MSGS   500000
#define DEFAULT_KEYS   2000

static uint64_t *g_lat_ns = NULL;
static size_t g_max_lat = 0;
static atomic_uint_fast64_t g_lat_count;
static atomic_uint_fast64_t g_done;
static atomic_uint_fast64_t g_checksum_acc;

static int bench_process(const awp_frame_t *frame, void *user)
{
    (void)user;
    /* Real SIMD payload checksum (64 bytes) */
    uint32_t csum = bench_fast_sum_64(frame->payload);
    atomic_fetch_add(&g_checksum_acc, csum);

    uint64_t dt = bench_now_ns() - frame->submit_ns;
    uint64_t n = atomic_fetch_add(&g_lat_count, 1);
    if (n < g_max_lat)
        g_lat_ns[n] = dt;

    atomic_fetch_add(&g_done, 1);
    return 0;
}

int main(int argc, char **argv)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int n_msgs = DEFAULT_MSGS;
    int n_keys = DEFAULT_KEYS;
    int n_workers = 32;
    int i;
    uint64_t t0, t1;
    awp_pool_metrics_t m;

    if (argc > 1)
        n_msgs = atoi(argv[1]);
    if (argc > 2)
        n_keys = atoi(argv[2]);
    if (argc > 3)
        n_workers = atoi(argv[3]);

    bench_pin_pcores();

    g_max_lat = (size_t)n_msgs;
    g_lat_ns = (uint64_t *)malloc(g_max_lat * sizeof(uint64_t));
    if (!g_lat_ns) {
        fprintf(stderr, "Failed to allocate latency buffer\n");
        return 1;
    }

    atomic_store(&g_lat_count, 0);
    atomic_store(&g_done, 0);
    atomic_store(&g_checksum_acc, 0);

    awp_config_init(&cfg);
    cfg.n_workers = n_workers;
    cfg.queue_capacity = 2048;
    cfg.frame_pool_size = 16384;
    cfg.enable_supervisor = 0;
    cfg.process = bench_process;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "Pool create failed\n");
        free(g_lat_ns);
        return 1;
    }

    /* Prepare sample 64-byte payload */
    uint8_t payload[64];
    for (i = 0; i < 64; i++)
        payload[i] = (uint8_t)(i + 1);

    t0 = bench_now_ns();
    for (i = 0; i < n_msgs; i++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "SYM%04d", i % n_keys);
        if (awp_submit(pool, "TRADES", sym, payload, sizeof(payload), 0) != 0) {
            fprintf(stderr, "Submit failed at index %d\n", i);
            break;
        }
    }

    while (atomic_load(&g_done) < (uint64_t)n_msgs)
        usleep(50);
    t1 = bench_now_ns();

    uint64_t count = atomic_load(&g_lat_count);
    if (count > (uint64_t)n_msgs)
        count = (uint64_t)n_msgs;

    bench_percentiles_t p = bench_calc_percentiles(g_lat_ns, count);
    awp_pool_get_metrics(pool, &m);

    double duration_sec = (double)(t1 - t0) / 1e9;
    double throughput = (double)n_msgs / duration_sec;

    printf("\n=== High-Scale Nanosecond Dispatch Benchmark ===\n");
    printf("Messages: %d | Keys: %d | Workers: %u | Capacity: %u\n",
           n_msgs, n_keys, m.n_workers, cfg.queue_capacity);
    printf("Throughput: %.2f M msg/sec (Wall: %.2f ms)\n",
           throughput / 1e6, (double)(t1 - t0) / 1e6);
    printf("Checksum Accumulator: %llu\n", (unsigned long long)atomic_load(&g_checksum_acc));
    printf("Latency (Submit -> SIMD Process Return):\n");
    printf("  Mean : %8.2f ns (%6.2f µs)\n", p.mean_ns, p.mean_ns / 1000.0);
    printf("  p50  : %8.2f ns (%6.2f µs)\n", p.p50_ns, p.p50_ns / 1000.0);
    printf("  p90  : %8.2f ns (%6.2f µs)\n", p.p90_ns, p.p90_ns / 1000.0);
    printf("  p99  : %8.2f ns (%6.2f µs)\n", p.p99_ns, p.p99_ns / 1000.0);
    printf("  p99.9: %8.2f ns (%6.2f µs)\n", p.p999_ns, p.p999_ns / 1000.0);
    printf("  Max  : %8.2f ns (%6.2f µs)\n", p.max_ns, p.max_ns / 1000.0);
    printf("Drops: %llu | Errors: %llu\n\n",
           (unsigned long long)m.dropped,
           (unsigned long long)m.process_errors);

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    free(g_lat_ns);

    return (p.p99_ns <= 5000000.0 && m.dropped == 0) ? 0 : 1;
}

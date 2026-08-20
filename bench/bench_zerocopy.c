/**
 * Pure Zero-Copy Claim & Commit Benchmark
 * Measures throughput and latency when writing payload directly in-place without memcpy.
 */
#include "awp/awp.h"
#include "bench_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

#include <stdalign.h>

#define TOTAL_MSGS  1000000
#define N_KEYS      2000
#define MAX_WORKERS 32
#define MAX_MSGS_PER_WORKER (TOTAL_MSGS / MAX_WORKERS * 2)

typedef struct {
    alignas(64) atomic_uint_fast64_t done;
    uint64_t checksum_acc;
    uint64_t *latencies;
    size_t count;
} bench_worker_stat_t;

static bench_worker_stat_t g_worker_stats[MAX_WORKERS];

static int bench_process(const awp_frame_t *frame, void *user)
{
    (void)user;
    uint64_t now = bench_now_ns();
    uint32_t csum = bench_fast_sum_64(frame->payload);
    uint32_t shard = frame->shard % MAX_WORKERS;
    g_worker_stats[shard].checksum_acc += csum;
    if (g_worker_stats[shard].count < MAX_MSGS_PER_WORKER && frame->submit_ns > 0 && now >= frame->submit_ns) {
        g_worker_stats[shard].latencies[g_worker_stats[shard].count++] = now - frame->submit_ns;
    }
    atomic_fetch_add_explicit(&g_worker_stats[shard].done, 1, memory_order_release);
    return 0;
}

int main(int argc, char **argv)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int n_msgs = TOTAL_MSGS;
    int n_keys = N_KEYS;
    int n_workers = 32;
    uint64_t t0, t1;
    awp_pool_metrics_t m;

    if (argc > 1)
        n_msgs = atoi(argv[1]);
    if (argc > 2)
        n_keys = atoi(argv[2]);

    bench_pin_pcores();

    for (int w = 0; w < n_workers; w++) {
        atomic_store(&g_worker_stats[w].done, 0);
        g_worker_stats[w].checksum_acc = 0;
        g_worker_stats[w].count = 0;
        g_worker_stats[w].latencies = (uint64_t *)malloc(MAX_MSGS_PER_WORKER * sizeof(uint64_t));
    }

    awp_config_init(&cfg);
    cfg.n_workers = n_workers;
    cfg.queue_capacity = 2048;
    cfg.frame_pool_size = 16384;
    cfg.ring_mode = AWP_RING_MPSC;
    cfg.enable_supervisor = 0;
    cfg.process = bench_process;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "Pool create failed\n");
        return 1;
    }

    t0 = bench_now_ns();
    for (int i = 0; i < n_msgs; i++) {
        uint32_t shard = (uint32_t)(i % n_workers);
        awp_claim_t claim;

        while (awp_claim_frame(pool, shard, &claim) != 0) {
            bench_cpu_relax();
        }

        // In-place direct write into claimed frame payload
        awp_frame_t *f = claim.frame;
        f->payload_len = 64;
        for (int b = 0; b < 64; b++)
            f->payload[b] = (uint8_t)(i + b);

        awp_commit_frame(pool, &claim);
    }

    while (1) {
        uint64_t total_done = 0;
        for (int w = 0; w < n_workers; w++) {
            total_done += atomic_load_explicit(&g_worker_stats[w].done, memory_order_acquire);
        }
        if (total_done >= (uint64_t)n_msgs) break;
        bench_cpu_relax();
    }
    t1 = bench_now_ns();

    uint64_t total_csum = 0;
    size_t total_samples = 0;
    for (int w = 0; w < n_workers; w++) {
        total_csum += g_worker_stats[w].checksum_acc;
        total_samples += g_worker_stats[w].count;
    }

    uint64_t *all_lat = (uint64_t *)malloc(total_samples * sizeof(uint64_t));
    size_t offset = 0;
    for (int w = 0; w < n_workers; w++) {
        memcpy(all_lat + offset, g_worker_stats[w].latencies, g_worker_stats[w].count * sizeof(uint64_t));
        offset += g_worker_stats[w].count;
    }

    bench_percentiles_t p = bench_calc_percentiles(all_lat, total_samples);
    awp_pool_get_metrics(pool, &m);

    double duration_ns = (double)(t1 - t0);
    double duration_sec = duration_ns / 1e9;
    double throughput = (double)n_msgs / duration_sec;

    printf("\n=== C11 Zero-Copy Claim/Commit Dispatch Benchmark ===\n");
    printf("Messages: %d | Keys: %d | Workers: %u | Capacity: %u\n",
           n_msgs, n_keys, m.n_workers, cfg.queue_capacity);
    printf("Checksum Accumulator: %llu\n", (unsigned long long)total_csum);
    printf("Throughput: %.2f M msg/sec (Wall: %.2f ms)\n",
           throughput / 1e6, duration_ns / 1e6);
    printf("Latency Percentiles (Submit -> Direct Write -> Commit -> SIMD):\n");
    printf("  Min   : %8.2f ns (%6.3f µs)\n", p.min_ns, p.min_ns / 1000.0);
    printf("  Mean  : %8.2f ns (%6.3f µs)\n", p.mean_ns, p.mean_ns / 1000.0);
    printf("  p50   : %8.2f ns (%6.3f µs)\n", p.p50_ns, p.p50_ns / 1000.0);
    printf("  p90   : %8.2f ns (%6.3f µs)\n", p.p90_ns, p.p90_ns / 1000.0);
    printf("  p99   : %8.2f ns (%6.3f µs)\n", p.p99_ns, p.p99_ns / 1000.0);
    printf("  p99.9 : %8.2f ns (%6.3f µs)\n", p.p999_ns, p.p999_ns / 1000.0);
    printf("  Max   : %8.2f ns (%6.3f µs)\n", p.max_ns, p.max_ns / 1000.0);
    printf("Drops: %llu | Errors: %llu\n\n",
           (unsigned long long)m.dropped,
           (unsigned long long)m.process_errors);

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);

    for (int w = 0; w < n_workers; w++) {
        free(g_worker_stats[w].latencies);
    }
    free(all_lat);

    return 0;
}

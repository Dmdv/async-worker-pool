/**
 * Multi-Producer Contention & Scalability Benchmark
 * Measures throughput and latency scaling across 1..32 concurrent producer threads.
 */
#include "awp/awp.h"
#include "bench_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

#define TOTAL_MSGS 1000000
#define N_KEYS     4000

typedef struct {
    awp_pool_t *pool;
    int prod_id;
    int n_msgs;
    int n_keys;
    uint8_t payload[64];
    atomic_uint_fast64_t *fails;
} prod_worker_t;

static atomic_uint_fast64_t g_done;
static atomic_uint_fast64_t g_checksum_acc;

static int bench_process(const awp_frame_t *frame, void *user)
{
    (void)user;
    uint32_t csum = bench_fast_sum_64(frame->payload);
    atomic_fetch_add(&g_checksum_acc, csum);
    atomic_fetch_add(&g_done, 1);
    return 0;
}

static void *producer_thread(void *arg)
{
    prod_worker_t *pw = (prod_worker_t *)arg;
    bench_pin_pcores();

    for (int i = 0; i < pw->n_msgs; i++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "SYM%04d", (pw->prod_id * 1000 + i) % pw->n_keys);
        if (awp_submit(pw->pool, "MARKET", sym, pw->payload, sizeof(pw->payload), 0) != 0) {
            atomic_fetch_add(pw->fails, 1);
        }
    }
    return NULL;
}

static void test_producers(int n_prods, int total_msgs)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    pthread_t threads[32];
    prod_worker_t pworkers[32];
    atomic_uint_fast64_t fails;
    awp_pool_metrics_t m;

    atomic_store(&g_done, 0);
    atomic_store(&g_checksum_acc, 0);
    atomic_store(&fails, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 32;
    cfg.queue_capacity = 2048;
    cfg.frame_pool_size = 32768;
    cfg.ring_mode = AWP_RING_MPSC;
    cfg.enable_supervisor = 0;
    cfg.process = bench_process;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "Failed to create pool for %d producers\n", n_prods);
        return;
    }

    int chunk = total_msgs / n_prods;
    for (int i = 0; i < n_prods; i++) {
        pworkers[i].pool = pool;
        pworkers[i].prod_id = i;
        pworkers[i].n_msgs = (i == n_prods - 1) ? (total_msgs - i * chunk) : chunk;
        pworkers[i].n_keys = N_KEYS;
        pworkers[i].fails = &fails;
        for (int b = 0; b < 64; b++)
            pworkers[i].payload[b] = (uint8_t)(i + b);
    }

    uint64_t t0 = bench_now_ns();
    for (int i = 0; i < n_prods; i++)
        pthread_create(&threads[i], NULL, producer_thread, &pworkers[i]);

    for (int i = 0; i < n_prods; i++)
        pthread_join(threads[i], NULL);

    while (atomic_load(&g_done) < (uint64_t)total_msgs)
        usleep(50);
    uint64_t t1 = bench_now_ns();

    double duration_sec = (double)(t1 - t0) / 1e9;
    double throughput = (double)total_msgs / duration_sec;
    double ns_per_msg = (double)(t1 - t0) / (double)total_msgs;

    awp_pool_get_metrics(pool, &m);

    printf("%2d prods  |  Throughput: %6.2f M msg/s  |  Wall: %7.2f ms  |  Avg: %6.1f ns/msg  |  Fails: %llu\n",
           n_prods, throughput / 1e6, (double)(t1 - t0) / 1e6, ns_per_msg,
           (unsigned long long)atomic_load(&fails));

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
}

int main(int argc, char **argv)
{
    int total_msgs = TOTAL_MSGS;
    if (argc > 1)
        total_msgs = atoi(argv[1]);

    bench_pin_pcores();

    printf("\n=== Multi-Producer Contention & Scalability Benchmark ===\n");
    printf("Total Messages: %d | Worker Threads: 32 | Queue Mode: MPSC\n\n", total_msgs);

    int prod_configs[] = {1, 2, 4, 8, 16, 24, 32};
    int n_configs = sizeof(prod_configs) / sizeof(prod_configs[0]);

    for (int i = 0; i < n_configs; i++) {
        test_producers(prod_configs[i], total_msgs);
    }
    printf("\n");
    return 0;
}

/**
 * End-to-end: multi-reader venue threads → sharded pool → process callback.
 * Simulates N WebSocket reader threads submitting concurrent market data.
 */
#include "test_common.h"

#define N_READERS  8
#define N_KEYS     1000
#define MSGS_PER   400   /* 8*400 = 3200 msgs ≈ 3k */

typedef struct {
    awp_pool_t *pool;
    int reader_id;
    int msgs;
    atomic_int *errors;
} reader_args_t;

static void *reader_main(void *arg)
{
    reader_args_t *a = arg;
    int i;
    for (i = 0; i < a->msgs; i++) {
        char symbol[32];
        char payload[64];
        int key = (a->reader_id * 997 + i * 13) % N_KEYS;
        snprintf(symbol, sizeof(symbol), "SYM%04d", key);
        snprintf(payload, sizeof(payload), "r%d-i%d", a->reader_id, i);
        if (awp_submit(a->pool, "trades", symbol, payload, strlen(payload), 0) != 0)
            atomic_fetch_add(a->errors, 1);
    }
    /* One broadcast-style frame per reader. */
    if (awp_submit(a->pool, "markPrice", "", "bc", 2, AWP_FRAME_BROADCAST) != 0)
        atomic_fetch_add(a->errors, 1);
    return NULL;
}

/* Per-key order tracking across all readers. */
typedef struct {
    atomic_uint_fast64_t count;
    pthread_mutex_t mu;
    uint64_t last_seq[N_KEYS];
    int seen[N_KEYS];
    int reorder;
} e2e_ctx_t;

static int e2e_process(const awp_frame_t *frame, void *user)
{
    e2e_ctx_t *c = (e2e_ctx_t *)user;
    int key = -1;

    if (strcmp(frame->feed, "trades") == 0 &&
        sscanf(frame->symbol, "SYM%d", &key) == 1 &&
        key >= 0 && key < N_KEYS) {
        pthread_mutex_lock(&c->mu);
        if (c->seen[key] && frame->seq < c->last_seq[key])
            c->reorder++;
        c->last_seq[key] = frame->seq;
        c->seen[key] = 1;
        pthread_mutex_unlock(&c->mu);
    }

    atomic_fetch_add(&c->count, 1);
    return 0;
}

int main(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    e2e_ctx_t ctx;
    pthread_t readers[N_READERS];
    reader_args_t args[N_READERS];
    atomic_int errors;
    int i;
    const char *bc_feeds[] = { "markPrice", NULL };
    uint64_t expect;
    awp_pool_metrics_t m;
    uint64_t t0, t1;
    struct timespec ts;

    printf("e2e multi-reader dispatch\n");
    memset(&ctx, 0, sizeof(ctx));
    atomic_store(&ctx.count, 0);
    pthread_mutex_init(&ctx.mu, NULL);
    atomic_init(&errors, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 32; /* skew headroom, not core count */
    cfg.n_broadcast_workers = 1;
    cfg.broadcast_feeds = bc_feeds;
    cfg.queue_capacity = 128;
    cfg.frame_pool_size = 4096;
    cfg.enable_supervisor = 1;
    cfg.process = e2e_process;
    cfg.user = &ctx;

    if (awp_pool_create(&cfg, &pool) != 0) {
        fprintf(stderr, "pool create failed\n");
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;

    for (i = 0; i < N_READERS; i++) {
        args[i].pool = pool;
        args[i].reader_id = i;
        args[i].msgs = MSGS_PER;
        args[i].errors = &errors;
        pthread_create(&readers[i], NULL, reader_main, &args[i]);
    }
    for (i = 0; i < N_READERS; i++)
        pthread_join(readers[i], NULL);

    expect = (uint64_t)N_READERS * MSGS_PER + N_READERS; /* + broadcast */
    {
        unsigned waited = 0;
        while (atomic_load(&ctx.count) < expect && waited < 15000) {
            test_sleep_ms(10);
            waited += 10;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;

    awp_pool_get_metrics(pool, &m);

    printf("  processed=%llu expect=%llu errors=%d drops=%llu reorder=%d\n",
           (unsigned long long)atomic_load(&ctx.count),
           (unsigned long long)expect,
           atomic_load(&errors),
           (unsigned long long)m.dropped,
           ctx.reorder);
    printf("  wall_ms=%llu rate=%.0f msg/s\n",
           (unsigned long long)((t1 - t0) / 1000000ull),
           (double)expect / ((double)(t1 - t0) / 1e9));

    printf("  per-worker (depth/hwm/processed):\n");
    for (i = 0; i < (int)m.n_workers; i++) {
        if (m.workers[i].processed == 0 && m.workers[i].queue_hwm == 0)
            continue;
        printf("    w%02d: proc=%llu hwm=%llu blocks=%llu\n",
               i,
               (unsigned long long)m.workers[i].processed,
               (unsigned long long)m.workers[i].queue_hwm,
               (unsigned long long)m.workers[i].enqueue_blocks);
    }

    TEST_EQ_I(atomic_load(&errors), 0, "no submit errors");
    TEST_EQ_U64(atomic_load(&ctx.count), expect, "all messages processed");
    TEST_EQ_U64(m.dropped, 0, "zero drops");
    TEST_EQ_I(ctx.reorder, 0, "per-key FIFO reorder=0");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    pthread_mutex_destroy(&ctx.mu);

    printf("\ne2e: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

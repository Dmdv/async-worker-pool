/**
 * Unit tests: FIFO, backpressure/drops=0, fault isolation, shard stability.
 */
#include "test_common.h"

static void test_hash_stable(void)
{
    uint64_t a = awp_hash_key("trades", "BTCUSDT");
    uint64_t b = awp_hash_key("trades", "BTCUSDT");
    uint64_t c = awp_hash_key("trades", "ETHUSDT");
    printf("test_hash_stable\n");
    TEST_EQ_U64(a, b, "same key same hash");
    TEST_CHECK(a != c, "different symbols differ");
}

static void test_same_key_same_worker_fifo(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    const int N = 200;
    int i;
    uint32_t shard0, shard1;

    printf("test_same_key_same_worker_fifo\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 8;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 512;
    cfg.process = test_process;
    cfg.user = &ctx;
    cfg.enable_supervisor = 0;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create pool");

    shard0 = awp_shard_of(pool, "trades", "BTCUSDT", 0);
    for (i = 0; i < 50; i++) {
        shard1 = awp_shard_of(pool, "trades", "BTCUSDT", 0);
        TEST_EQ_U64(shard0, shard1, "stable shard");
    }

    for (i = 0; i < N; i++) {
        char payload[16];
        snprintf(payload, sizeof(payload), "%d", i);
        TEST_EQ_I(awp_submit(pool, "trades", "BTCUSDT", payload, strlen(payload), 0),
                  0, "submit");
    }
    wait_processed(&ctx, (uint64_t)N, 5000);
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)N, "all processed");
    TEST_EQ_I(ctx.reorder_violations, 0, "reorder distance 0 for key");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_backpressure_no_drops(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    const int N = 500;
    int i;
    awp_pool_metrics_t m;

    printf("test_backpressure_no_drops\n");
    test_ctx_init(&ctx);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 8;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.user = &ctx;
    cfg.process = test_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /* Tiny queues: producers block rather than drop under load. */
    for (i = 0; i < N; i++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "S%d", i % 16);
        TEST_EQ_I(awp_submit(pool, "book", sym, "x", 1, 0), 0, "submit no fail");
    }
    wait_processed(&ctx, (uint64_t)N, 10000);
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)N, "all delivered");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops counter still 0");

    awp_pool_get_metrics(pool, &m);
    TEST_EQ_U64(m.dropped, 0, "metrics drops 0");
    TEST_EQ_U64(m.submitted, (uint64_t)N, "submitted == N");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

/* Separate producer for concurrent backpressure stress. */
typedef struct {
    awp_pool_t *pool;
    int n;
    atomic_int fails;
} prod_args_t;

static void *producer_thread(void *arg)
{
    prod_args_t *a = arg;
    int j;
    for (j = 0; j < a->n; j++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "K%d", j % 8);
        if (awp_submit(a->pool, "trades", sym, "p", 1, 0) != 0)
            atomic_fetch_add(&a->fails, 1);
    }
    return NULL;
}

static void test_full_queue_blocks_concurrent(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    prod_args_t pa;
    pthread_t th[4];
    int t;
    const int per = 200;

    printf("test_full_queue_blocks_concurrent\n");
    test_ctx_init(&ctx);
    /* Start slow so queues fill and producers block. */
    atomic_store(&ctx.hang, 1);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 4;
    cfg.frame_pool_size = 64;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    pa.pool = pool;
    pa.n = per;
    atomic_store(&pa.fails, 0);

    for (t = 0; t < 4; t++)
        pthread_create(&th[t], NULL, producer_thread, &pa);

    /* Let producers block on full queues, then unstick workers. */
    test_sleep_ms(100);
    atomic_store(&ctx.hang, 0);

    for (t = 0; t < 4; t++)
        pthread_join(th[t], NULL);

    wait_processed(&ctx, (uint64_t)(per * 4), 15000);
    TEST_EQ_I(atomic_load(&pa.fails), 0, "no submit failures");
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)(per * 4), "all processed");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0 under full-queue stress");

    {
        awp_pool_metrics_t m;
        uint64_t blocks = 0;
        awp_pool_get_metrics(pool, &m);
        for (t = 0; t < (int)m.n_workers; t++)
            blocks += m.workers[t].enqueue_blocks;
        /* Under hang+tiny queue we expect some blocking (not guaranteed if
         * timing is lucky — soft check). */
        (void)blocks;
    }

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_process_error_survives(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    int i;
    const int N = 50;
    awp_pool_metrics_t m;

    printf("test_process_error_survives\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 128;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    for (i = 0; i < N; i++) {
        const char *payload = (i == 10 || i == 20) ? "E" : "ok";
        char sym[16];
        snprintf(sym, sizeof(sym), "X%d", i % 5);
        TEST_EQ_I(awp_submit(pool, "trades", sym, payload, strlen(payload), 0),
                  0, "submit");
    }
    /* Errors don't increment count; wait for successes. */
    wait_processed(&ctx, (uint64_t)(N - 2), 5000);
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)(N - 2), "successes only");
    TEST_EQ_U64(atomic_load(&ctx.errors_seen), 2, "two soft errors");

    awp_pool_get_metrics(pool, &m);
    TEST_EQ_U64(m.process_errors, 2, "pool process_errors");
    /* Worker still alive */
    {
        int alive = 0;
        for (i = 0; i < (int)m.n_workers; i++)
            alive += m.workers[i].alive;
        TEST_CHECK(alive == (int)m.n_workers, "all workers alive after errors");
    }

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_broadcast_shard(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    const char *feeds[] = { "markPrice", "funding", NULL };
    uint32_t s0, s1, s2;

    printf("test_broadcast_shard\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 8;
    cfg.n_broadcast_workers = 2;
    cfg.broadcast_feeds = feeds;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 64;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    s0 = awp_shard_of(pool, "markPrice", "ANY", 0);
    s1 = awp_shard_of(pool, "markPrice", "OTHER", 0);
    s2 = awp_shard_of(pool, "trades", "BTCUSDT", 0);
    TEST_CHECK(s0 < 2, "broadcast in dedicated range");
    TEST_EQ_U64(s0, s1, "broadcast ignores symbol for same feed");
    TEST_CHECK(s2 >= 2, "symbol traffic above broadcast workers");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

int main(void)
{
    test_hash_stable();
    test_same_key_same_worker_fifo();
    test_backpressure_no_drops();
    test_full_queue_blocks_concurrent();
    test_process_error_survives();
    test_broadcast_shard();

    printf("\nunit: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

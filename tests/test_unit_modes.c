/**
 * Unit matrix: FIFO, backpressure, fault isolation for every ring_mode.
 */
#include "test_common.h"
#include "mode_util.h"

static awp_ring_mode_t g_modes[] = {
    AWP_RING_SPSC, AWP_RING_MPSC, AWP_RING_SPMC, AWP_RING_MPMC
};

static void test_fifo_mode(awp_ring_mode_t mode)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    const int N = 150;
    int i;
    uint32_t s0, s1;

    printf("  fifo [%s]\n", awp_mode_name(mode));
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 8;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 512;
    cfg.ring_mode = mode;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    s0 = awp_shard_of(pool, "trades", "BTCUSDT", 0);
    for (i = 0; i < 20; i++) {
        s1 = awp_shard_of(pool, "trades", "BTCUSDT", 0);
        TEST_EQ_U64(s0, s1, "stable shard");
    }
    for (i = 0; i < N; i++) {
        char p[16];
        snprintf(p, sizeof(p), "%d", i);
        TEST_EQ_I(awp_submit(pool, "trades", "BTCUSDT", p, strlen(p), 0), 0,
                  "submit");
    }
    wait_processed(&ctx, (uint64_t)N, 8000);
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)N, "all processed");
    TEST_EQ_I(ctx.reorder_violations, 0, "reorder 0");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0");
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

typedef struct {
    awp_pool_t *pool;
    int n;
    atomic_int *fails;
} prod_t;

static void *prod_main(void *arg)
{
    prod_t *a = arg;
    int j;
    for (j = 0; j < a->n; j++) {
        char sym[32];
        snprintf(sym, sizeof(sym), "K%d", j % 8);
        if (awp_submit(a->pool, "trades", sym, "p", 1, 0) != 0)
            atomic_fetch_add(a->fails, 1);
    }
    return NULL;
}

static void test_backpressure_mode(awp_ring_mode_t mode)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    prod_t pa[4];
    pthread_t th[4];
    atomic_int fails;
    int n_prod = awp_mode_suggest_producers(mode);
    int t;
    const int per = 150;
    int expect;

    if (n_prod > 4)
        n_prod = 4;

    printf("  backpressure [%s] producers=%d\n", awp_mode_name(mode), n_prod);
    test_ctx_init(&ctx);
    atomic_store(&ctx.hang, 1);
    atomic_init(&fails, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 4;
    cfg.frame_pool_size = 128;
    cfg.ring_mode = mode;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    for (t = 0; t < n_prod; t++) {
        pa[t].pool = pool;
        pa[t].n = per;
        pa[t].fails = &fails;
        pthread_create(&th[t], NULL, prod_main, &pa[t]);
    }
    test_sleep_ms(80);
    atomic_store(&ctx.hang, 0);
    for (t = 0; t < n_prod; t++)
        pthread_join(th[t], NULL);

    expect = per * n_prod;
    wait_processed(&ctx, (uint64_t)expect, 15000);
    TEST_EQ_I(atomic_load(&fails), 0, "no submit fails");
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)expect, "all delivered");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_fault_mode(awp_ring_mode_t mode)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    int i;
    const int N = 40;
    awp_pool_metrics_t m;

    printf("  fault-isolation [%s]\n", awp_mode_name(mode));
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 128;
    cfg.ring_mode = mode;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < N; i++) {
        const char *payload = (i == 5 || i == 15) ? "E" : "ok";
        char sym[16];
        snprintf(sym, sizeof(sym), "X%d", i % 5);
        TEST_EQ_I(awp_submit(pool, "trades", sym, payload, strlen(payload), 0),
                  0, "submit");
    }
    wait_processed(&ctx, (uint64_t)(N - 2), 5000);
    TEST_EQ_U64(atomic_load(&ctx.count), (uint64_t)(N - 2), "successes");
    TEST_EQ_U64(atomic_load(&ctx.errors_seen), 2, "two soft errors");
    awp_pool_get_metrics(pool, &m);
    TEST_EQ_U64(m.process_errors, 2, "pool errors");
    {
        int alive = 0;
        for (i = 0; i < (int)m.n_workers; i++)
            alive += m.workers[i].alive;
        TEST_CHECK(alive == (int)m.n_workers, "workers alive");
    }
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

int main(int argc, char **argv)
{
    awp_ring_mode_t only = AWP_RING_MPSC;
    int all = 1;
    size_t i;

    if (argc > 1) {
        int pr = awp_mode_parse(argv[1], &only);
        if (pr < 0) {
            awp_mode_print_usage(argv[0]);
            return 2;
        }
        all = (pr == 1);
    }

    printf("unit matrix over ring modes\n");
    for (i = 0; i < sizeof(g_modes) / sizeof(g_modes[0]); i++) {
        if (!all && g_modes[i] != only)
            continue;
        printf("mode %s\n", awp_mode_name(g_modes[i]));
        test_fifo_mode(g_modes[i]);
        test_backpressure_mode(g_modes[i]);
        test_fault_mode(g_modes[i]);
    }

    printf("\nunit_modes: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

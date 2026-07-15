/**
 * Service-level teardown drills: clean path and quarantined path.
 * Multi-stuck: absolute deadline is one budget, not N × deadline.
 */
#include "test_common.h"
#include "../src/internal.h"

static atomic_int g_sticky;

static int sticky_process(const awp_frame_t *f, void *user)
{
    (void)f;
    (void)user;
    while (atomic_load(&g_sticky))
        test_sleep_ms(20);
    return 0;
}

static int count_process(const awp_frame_t *f, void *user)
{
    (void)f;
    atomic_fetch_add((atomic_uint_fast64_t *)user, 1);
    return 0;
}

/* Clean owner protocol: shutdown 0, destroy reclaims (no quarantine). */
static void test_clean_teardown_owner_protocol(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    atomic_uint_fast64_t n;
    int i, shut;

    printf("teardown clean owner protocol\n");
    atomic_store(&n, 0);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 128;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = 5000;
    cfg.process = count_process;
    cfg.user = &n;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < 50; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "K%d", i % 8);
        TEST_EQ_I(awp_submit(pool, "trades", sym, "x", 1, 0), 0, "submit");
    }
    shut = awp_pool_shutdown(pool);
    TEST_EQ_I(shut, 0, "clean shutdown == 0");
    TEST_EQ_U64(atomic_load(&n), 50, "all processed");
    TEST_EQ_I(atomic_load(&pool->quarantined), 0, "not quarantined");
    awp_pool_destroy(pool);
}

/* Quarantine path: shutdown >0, destroy does not reclaim; submits reject. */
static void test_quarantine_teardown_process_recycle(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int shut;

    printf("teardown quarantine path\n");
    atomic_store(&g_sticky, 1);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = 300;
    cfg.process = sticky_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    TEST_EQ_I(awp_submit(pool, "t", "A", "x", 1, 0), 0, "submit stuck");
    test_sleep_ms(50);

    shut = awp_pool_shutdown(pool);
    TEST_CHECK(shut > 0, "shutdown >0 on quarantine");
    TEST_EQ_I(atomic_load(&pool->quarantined), 1, "quarantined");
    TEST_CHECK(awp_submit(pool, "t", "B", "x", 1, 0) != 0, "submit rejected");
    /* Destroy must not free storage under sticky callback (no crash). */
    awp_pool_destroy(pool);

    atomic_store(&g_sticky, 0);
    test_sleep_ms(50);
}

/* Multi-stuck workers: elapsed ~ one deadline, not N × deadline. */
static void test_multi_stuck_one_deadline(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    uint64_t t0, elapsed_ms;
    int shut, i;
    const int N = 4;
    const uint32_t deadline_ms = 400;

    printf("teardown multi-stuck absolute deadline\n");
    atomic_store(&g_sticky, 1);
    awp_config_init(&cfg);
    cfg.n_workers = (uint32_t)N;
    cfg.queue_capacity = 8;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = deadline_ms;
    cfg.process = sticky_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < N; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "W%d", i);
        /* Distinct shards: symbol hash varies */
        TEST_EQ_I(awp_submit(pool, "t", sym, "x", 1, 0), 0, "submit");
    }
    test_sleep_ms(80);

    t0 = awp_now_ns();
    shut = awp_pool_shutdown(pool);
    elapsed_ms = (awp_now_ns() - t0) / 1000000ull;

    TEST_CHECK(shut > 0, "shutdown >0");
    TEST_CHECK(elapsed_ms < (uint64_t)deadline_ms * 3 + 1500,
               "elapsed << N*deadline (one absolute budget)");
    TEST_CHECK(elapsed_ms < (uint64_t)N * deadline_ms,
               "not N times deadline");
    awp_pool_destroy(pool);
    atomic_store(&g_sticky, 0);
    test_sleep_ms(50);
}

int main(void)
{
    test_clean_teardown_owner_protocol();
    test_quarantine_teardown_process_recycle();
    test_multi_stuck_one_deadline();
    printf("\nteardown_contract: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

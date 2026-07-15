/**
 * Supervisor restarts exited workers; bounded shutdown under stuck process;
 * reentrancy and sticky quarantine safety.
 */
#include "test_common.h"
#include "../src/internal.h"
#include <errno.h>

/* --- true sticky hang (does not observe QUIESCING) --------------------- */

static atomic_int g_true_sticky;

static int true_sticky_process(const awp_frame_t *frame, void *user)
{
    (void)frame;
    (void)user;
    while (atomic_load(&g_true_sticky))
        test_sleep_ms(20);
    return 0;
}

static void test_quarantine_true_sticky_destroy_safe(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    uint64_t t0, elapsed_ms;
    int rc;

    printf("test_quarantine_true_sticky_destroy_safe\n");
    atomic_store(&g_true_sticky, 1);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.shutdown_deadline_ms = 400;
    cfg.enable_supervisor = 0;
    cfg.process = true_sticky_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    TEST_EQ_I(awp_submit(pool, "trades", "BTC", "x", 1, 0), 0, "submit stuck");
    test_sleep_ms(80); /* ensure callback entered */

    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
    }
    rc = awp_pool_shutdown(pool);
    {
        struct timespec ts;
        uint64_t t1;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
        elapsed_ms = (t1 - t0) / 1000000ull;
    }

    TEST_CHECK(elapsed_ms < 3000, "shutdown bounded with sticky callback");
    TEST_CHECK(rc >= 0, "shutdown non-negative");
    TEST_CHECK(atomic_load(&pool->quarantined) == 1, "pool sticky-quarantined");
    TEST_CHECK(rc > 0 || atomic_load(&pool->shutdown_aborts) > 0,
               "abort/quarantine counted");

    /* Must not free (would UAF sticky thread). No crash is the pass. */
    awp_pool_destroy(pool);

    atomic_store(&g_true_sticky, 0);
    /* Let stuck thread eventually exit after flag clear (leaked pool storage). */
    test_sleep_ms(50);
}

/* Cooperative sticky used for older bounded test (exits on QUIESCING). */
static atomic_int g_sticky_hang;
static awp_pool_t *g_sticky_pool;

static int sticky_process(const awp_frame_t *frame, void *user)
{
    test_ctx_t *c = (test_ctx_t *)user;
    (void)frame;
    while (atomic_load(&g_sticky_hang)) {
        if (g_sticky_pool) {
            int life = atomic_load(&g_sticky_pool->lifecycle);
            if (life >= AWP_LIFE_QUIESCING)
                break;
        }
        test_sleep_ms(10);
    }
    atomic_fetch_add(&c->count, 1);
    return 0;
}

static void test_bounded_shutdown_stuck_worker(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    uint64_t t0, elapsed_ms;
    int rc;

    printf("test_bounded_shutdown_stuck_worker\n");
    test_ctx_init(&ctx);
    atomic_store(&g_sticky_hang, 1);
    g_sticky_pool = NULL;

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.shutdown_deadline_ms = 1500;
    cfg.enable_supervisor = 0;
    cfg.process = sticky_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    g_sticky_pool = pool;
    TEST_EQ_I(awp_submit(pool, "trades", "BTC", "x", 1, 0), 0, "submit stuck");
    test_sleep_ms(50);

    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        t0 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
    }
    rc = awp_pool_shutdown(pool);
    {
        struct timespec ts;
        uint64_t t1;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        t1 = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
        elapsed_ms = (t1 - t0) / 1000000ull;
    }

    TEST_CHECK(elapsed_ms < 5000, "shutdown bounded (<5s wall)");
    TEST_CHECK(rc >= 0, "shutdown returns non-negative");

    atomic_store(&g_sticky_hang, 0);
    g_sticky_pool = NULL;
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_supervisor_restart_dead_worker(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    awp_pool_metrics_t m;
    int i;
    uint64_t restarts = 0;

    printf("test_supervisor_restart_dead_worker\n");
    test_ctx_init(&ctx);

    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 128;
    cfg.enable_supervisor = 1;
    cfg.enable_restart = 1;
    cfg.supervisor_interval_ms = 50;
    cfg.stall_threshold_ms = 5000; /* avoid stall path; use exit restart */
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /* Cooperative death: stop + close wakes blocked pop; queue storage kept. */
    atomic_store(&pool->workers[1].stop, 1);
    awp_ring_close(&pool->workers[1].queue);
    test_sleep_ms(600);

    for (i = 0; i < 100; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "S%d", i);
        awp_submit(pool, "trades", sym, "ok", 2, 0);
    }
    wait_processed(&ctx, 50, 5000);

    awp_pool_get_metrics(pool, &m);
    for (i = 0; i < (int)m.n_workers; i++)
        restarts += m.workers[i].restarts;

    TEST_CHECK(restarts >= 1, "supervisor restarted at least one worker");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0 under backpressure path");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_double_shutdown(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    int rc1, rc2;

    printf("test_double_shutdown\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    awp_submit(pool, "trades", "A", "x", 1, 0);
    wait_processed(&ctx, 1, 2000);
    rc1 = awp_pool_shutdown(pool);
    rc2 = awp_pool_shutdown(pool);
    TEST_CHECK(rc1 >= 0 && rc2 >= 0, "double shutdown ok");
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_submit_rejects_after_shutdown(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;

    printf("test_submit_rejects_after_shutdown\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    awp_pool_shutdown(pool);
    TEST_CHECK(awp_submit(pool, "trades", "A", "x", 1, 0) != 0,
               "submit rejected after shutdown");
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_input_validation(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    char big[AWP_FEED_MAX + 8];

    printf("test_input_validation\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    memset(big, 'A', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';
    TEST_CHECK(awp_submit(pool, big, "S", "x", 1, 0) == -E2BIG, "feed too long");
    TEST_CHECK(awp_submit(pool, "f", "S", NULL, 5, 0) == -EINVAL, "null payload");
    TEST_CHECK(awp_submit(pool, "f", "S", "x", AWP_PAYLOAD_MAX + 1, 0) == -E2BIG,
               "payload too big");
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

/* --- reentrancy from process / on_error -------------------------------- */

typedef struct {
    awp_pool_t *pool;
    atomic_int submit_rc;
    atomic_int shutdown_rc;
    atomic_int saw_error_cb;
} reent_t;

static int reent_process(const awp_frame_t *frame, void *user)
{
    reent_t *r = user;
    (void)frame;
    atomic_store(&r->submit_rc, awp_submit(r->pool, "t", "s", "x", 1, 0));
    atomic_store(&r->shutdown_rc, awp_pool_shutdown(r->pool));
    /* destroy must not free while we still hold TLS */
    awp_pool_destroy(r->pool);
    return 0;
}

static void test_callback_reentrancy_blocked(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    reent_t r;

    printf("test_callback_reentrancy_blocked\n");
    memset(&r, 0, sizeof(r));
    atomic_store(&r.submit_rc, 0);
    atomic_store(&r.shutdown_rc, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = 2000;
    cfg.process = reent_process;
    cfg.user = &r;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    r.pool = pool;
    TEST_EQ_I(awp_submit(pool, "trades", "R", "x", 1, 0), 0, "submit");
    test_sleep_ms(200);

    TEST_EQ_I(atomic_load(&r.submit_rc), -EDEADLK, "submit from process is EDEADLK");
    TEST_EQ_I(atomic_load(&r.shutdown_rc), -EDEADLK,
              "shutdown from process is EDEADLK");
    /* destroy from callback marked quarantine; outer destroy must not crash */
    TEST_CHECK(atomic_load(&pool->quarantined) == 1,
               "destroy-from-callback quarantined");
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
}

static void on_error_reent(const awp_frame_t *frame, int err, void *user)
{
    reent_t *r = user;
    (void)frame;
    (void)err;
    atomic_store(&r->saw_error_cb, 1);
    atomic_store(&r->submit_rc, awp_submit(r->pool, "t", "s", "x", 1, 0));
}

static int fail_process(const awp_frame_t *frame, void *user)
{
    (void)frame;
    (void)user;
    return 7;
}

static void test_on_error_reentrancy_blocked(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    reent_t r;

    printf("test_on_error_reentrancy_blocked\n");
    memset(&r, 0, sizeof(r));
    atomic_store(&r.submit_rc, 0);
    atomic_store(&r.saw_error_cb, 0);

    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = fail_process;
    cfg.on_error = on_error_reent;
    cfg.user = &r;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    r.pool = pool;
    TEST_EQ_I(awp_submit(pool, "trades", "E", "x", 1, 0), 0, "submit");
    test_sleep_ms(150);

    TEST_EQ_I(atomic_load(&r.saw_error_cb), 1, "on_error invoked");
    TEST_EQ_I(atomic_load(&r.submit_rc), -EDEADLK, "submit from on_error EDEADLK");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
}

int main(void)
{
    test_quarantine_true_sticky_destroy_safe();
    test_bounded_shutdown_stuck_worker();
    test_supervisor_restart_dead_worker();
    test_double_shutdown();
    test_submit_rejects_after_shutdown();
    test_input_validation();
    test_callback_reentrancy_blocked();
    test_on_error_reentrancy_blocked();
    printf("\nsupervisor: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

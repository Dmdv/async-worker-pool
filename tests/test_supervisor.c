/**
 * Supervisor restarts a dead worker; bounded shutdown under stuck process.
 */
#include "test_common.h"
#include "../src/internal.h"

static atomic_int g_sticky_hang;
static awp_pool_t *g_sticky_pool;

static int sticky_process(const awp_frame_t *frame, void *user)
{
    test_ctx_t *c = (test_ctx_t *)user;
    (void)frame;
    /* Simulate stuck publish: spin until hang cleared or pool shutting down. */
    while (atomic_load(&g_sticky_hang)) {
        if (g_sticky_pool && atomic_load(&g_sticky_pool->shutting_down))
            break;
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
    cfg.shutdown_deadline_ms = 1500; /* 1.5s hard bound */
    cfg.enable_supervisor = 0;       /* isolate shutdown test */
    cfg.process = sticky_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    g_sticky_pool = pool;
    TEST_EQ_I(awp_submit(pool, "trades", "BTC", "x", 1, 0), 0, "submit stuck");
    test_sleep_ms(50); /* ensure worker entered hang */

    t0 = 0;
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

    /* Shutdown must complete near deadline, not hang forever. */
    TEST_CHECK(elapsed_ms < 5000, "shutdown bounded (<5s wall)");
    /* May abort stuck worker(s). */
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
    cfg.supervisor_interval_ms = 100;
    cfg.stall_threshold_ms = 200;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /* Cooperative "death": stop flag + close queue so worker exits cleanly
     * (avoids pthread_cancel leaving mutexes held). Supervisor restarts it. */
    atomic_store(&pool->workers[1].stop, 1);
    awp_ring_close(&pool->workers[1].queue);
    /* Give supervisor time to notice alive=0 and restart. */
    test_sleep_ms(800);

    /* Submit work across keys so worker 1 may receive some. */
    for (i = 0; i < 100; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "S%d", i);
        awp_submit(pool, "trades", sym, "ok", 2, 0);
    }
    wait_processed(&ctx, 50, 5000); /* at least some progress */

    awp_pool_get_metrics(pool, &m);
    for (i = 0; i < (int)m.n_workers; i++)
        restarts += m.workers[i].restarts;

    TEST_CHECK(restarts >= 1, "supervisor restarted at least one worker");
    TEST_CHECK(m.workers[1].alive == 1 || restarts >= 1,
               "worker 1 recovered or restart counted");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "drops 0");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

int main(void)
{
    test_bounded_shutdown_stuck_worker();
    test_supervisor_restart_dead_worker();
    printf("\nsupervisor: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

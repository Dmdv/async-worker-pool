/**
 * Deterministic supervisor restart failure via AWP_TEST_HOOKS.
 */
#include "test_common.h"
#include "../src/internal.h"
#include <signal.h>

#ifndef AWP_TEST_HOOKS
#error "build with -DAWP_TEST_HOOKS"
#endif

static int noop_process(const awp_frame_t *f, void *u)
{
    (void)f;
    (void)u;
    return 0;
}

static void *worker_exit_soon(void *arg)
{
    awp_worker_t *w = arg;
    /* Force EXITED without running full worker loop — use cancel-free path:
     * set stop and close empty ring so worker_main exits if already running.
     * Here we join the real thread after pthread_kill is unavailable; instead
     * mark state EXITED only after real exit. Use cooperative stop. */
    (void)w;
    return NULL;
}

/* Kill a worker thread by requesting stop and closing its empty ring after
 * joining is not possible mid-flight — instead submit nothing and use
 * pthread_cancel is forbidden. Simulate dead worker: stop + close ring so
 * worker exits, then fail next start. */
static void test_restart_create_failure_quarantines(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    awp_worker_t *w;
    int i;

    printf("restart create failure via test hook\n");
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 1;
    cfg.enable_restart = 1;
    cfg.supervisor_interval_ms = 50;
    cfg.stall_threshold_ms = 60000;
    cfg.process = noop_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /* Arm fail-next before forcing worker exit. */
    atomic_store(&awp_test_fail_next_worker_start, 1);

    w = &pool->workers[0];
    atomic_store(&w->stop, 1);
    awp_ring_close(&w->queue);
    /* Wait for worker to exit */
    for (i = 0; i < 200 && atomic_load(&w->state) != AWP_W_EXITED; i++)
        test_sleep_ms(10);
    TEST_CHECK(atomic_load(&w->state) == AWP_W_EXITED ||
               atomic_load(&w->state) == AWP_W_JOINED ||
               atomic_load(&pool->quarantined) == 1,
               "worker exited or already quarantined");

    /* Supervisor should attempt restart and hit the hook. */
    for (i = 0; i < 100 && atomic_load(&pool->quarantined) == 0; i++)
        test_sleep_ms(20);

    TEST_EQ_I(atomic_load(&pool->quarantined), 1, "pool quarantined on restart fail");
    TEST_CHECK(atomic_load(&pool->shutdown_aborts) > 0, "shutdown_aborts set");
    TEST_CHECK(awp_submit(pool, "t", "s", "x", 1, 0) != 0, "admission closed");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    (void)worker_exit_soon;
}

int main(void)
{
    test_restart_create_failure_quarantines();
    printf("\nrestart_create_fail: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

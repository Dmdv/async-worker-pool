/**
 * E2E lifecycle: drain on shutdown, concurrent shutdown, no loss of accepted msgs.
 */
#include "test_common.h"
#include "../src/internal.h"

static int count_proc(const awp_frame_t *f, void *u)
{
    (void)f;
    atomic_fetch_add((atomic_uint_fast64_t *)u, 1);
    /* Slow enough that some remain queued at shutdown start. */
    test_sleep_ms(1);
    return 0;
}

static void test_shutdown_drains_accepted(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    atomic_uint_fast64_t n;
    const int N = 200;
    int i;
    uint64_t done;

    printf("e2e_lifecycle drain accepted frames\n");
    atomic_store(&n, 0);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 512;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = 10000;
    cfg.process = count_proc;
    cfg.user = &n;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < N; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "K%d", i % 20);
        TEST_EQ_I(awp_submit(pool, "trades", sym, "x", 1, 0), 0, "submit");
    }
    /* Shutdown while work still queued / in flight — must drain. */
    TEST_CHECK(awp_pool_shutdown(pool) >= 0, "shutdown");
    done = atomic_load(&n);
    TEST_EQ_U64(done, (uint64_t)N, "all accepted frames processed before stop");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "no abandonment under clean drain");
    awp_pool_destroy(pool);
}

typedef struct {
    awp_pool_t *pool;
    atomic_int *rc_sum;
} shut_arg_t;

static void *shut_thr(void *a)
{
    shut_arg_t *s = a;
    int rc = awp_pool_shutdown(s->pool);
    atomic_fetch_add(s->rc_sum, rc >= 0 ? 1 : 0);
    return NULL;
}

static void test_concurrent_shutdown(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    atomic_uint_fast64_t n;
    atomic_int ok;
    shut_arg_t sa[4];
    pthread_t th[4];
    int i;

    printf("e2e_lifecycle concurrent shutdown\n");
    atomic_store(&n, 0);
    atomic_init(&ok, 0);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 64;
    cfg.enable_supervisor = 0;
    cfg.process = count_proc;
    cfg.user = &n;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < 20; i++)
        awp_submit(pool, "t", "s", "x", 1, 0);

    for (i = 0; i < 4; i++) {
        sa[i].pool = pool;
        sa[i].rc_sum = &ok;
        pthread_create(&th[i], NULL, shut_thr, &sa[i]);
    }
    for (i = 0; i < 4; i++)
        pthread_join(th[i], NULL);

    TEST_EQ_I(atomic_load(&ok), 4, "all concurrent shutdowns completed");
    TEST_CHECK(atomic_load(&pool->lifecycle) == AWP_LIFE_STOPPED, "STOPPED");
    awp_pool_destroy(pool);
}

static void test_restart_preserves_progress(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    awp_pool_metrics_t m;
    int i;
    uint64_t restarts = 0;

    printf("e2e_lifecycle restart preserves ability to process\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 64;
    cfg.frame_pool_size = 256;
    cfg.enable_supervisor = 1;
    cfg.enable_restart = 1;
    cfg.supervisor_interval_ms = 50;
    cfg.stall_threshold_ms = 10000;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    atomic_store(&pool->workers[2].stop, 1);
    test_sleep_ms(500);

    for (i = 0; i < 80; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "R%d", i);
        TEST_EQ_I(awp_submit(pool, "trades", sym, "ok", 2, 0), 0, "submit");
    }
    wait_processed(&ctx, 80, 8000);
    TEST_EQ_U64(atomic_load(&ctx.count), 80, "all after restart path");

    awp_pool_get_metrics(pool, &m);
    for (i = 0; i < (int)m.n_workers; i++)
        restarts += m.workers[i].restarts;
    TEST_CHECK(restarts >= 1, "restart counted");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_restart_with_queued_work(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    int i;
    uint64_t restarts = 0;
    awp_pool_metrics_t m;

    printf("e2e_lifecycle restart with concurrent producers after exit\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 128;
    cfg.frame_pool_size = 512;
    cfg.enable_supervisor = 1;
    cfg.enable_restart = 1;
    cfg.supervisor_interval_ms = 30;
    cfg.stall_threshold_ms = 30000;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /* Fill shard-bound keys then force worker 0 exit; reopen preserves ring. */
    for (i = 0; i < 40; i++)
        TEST_EQ_I(awp_submit(pool, "trades", "W0KEY", "a", 1, 0), 0, "prefill");
    wait_processed(&ctx, 40, 5000);

    atomic_store(&pool->workers[0].stop, 1);
    awp_ring_close(&pool->workers[0].queue);
    test_sleep_ms(400);

    for (i = 0; i < 60; i++) {
        char sym[16];
        snprintf(sym, sizeof(sym), "P%d", i);
        TEST_EQ_I(awp_submit(pool, "trades", sym, "b", 1, 0), 0, "post-restart");
    }
    wait_processed(&ctx, 100, 8000);
    TEST_EQ_U64(atomic_load(&ctx.count), 100, "all pre+post restart processed");

    awp_pool_get_metrics(pool, &m);
    for (i = 0; i < (int)m.n_workers; i++)
        restarts += m.workers[i].restarts;
    TEST_CHECK(restarts >= 1, "restart observed");

    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

typedef struct {
    awp_pool_t *p;
    atomic_int done;
    atomic_int rc;
} block_submit_arg_t;

static void *block_submit_thr(void *v)
{
    block_submit_arg_t *x = v;
    int r = awp_submit(x->p, "t", "s", "y", 1, 0);
    atomic_store(&x->rc, r);
    atomic_store(&x->done, 1);
    return NULL;
}

static void test_try_push_hot_shard_no_hold(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    int i;
    pthread_t thr;
    block_submit_arg_t a;

    printf("e2e_lifecycle concurrent submit on small queue (try_push path)\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 1;
    cfg.queue_capacity = 4;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");

    /*
     * Hang the single worker after it takes one frame. Capacity is 4, so
     * after 1 in-callback + 4 queued the next submit must park on try_push.
     */
    atomic_store(&ctx.hang, 1);
    for (i = 0; i < 5; i++)
        TEST_EQ_I(awp_submit(pool, "t", "s", "x", 1, 0), 0, "fill");
    test_sleep_ms(30); /* let worker dequeue one into hang */

    atomic_store(&a.done, 0);
    atomic_store(&a.rc, -99);
    a.p = pool;
    pthread_create(&thr, NULL, block_submit_thr, &a);
    test_sleep_ms(150);
    TEST_CHECK(atomic_load(&a.done) == 0, "submit blocked on full queue");
    atomic_store(&ctx.hang, 0);
    pthread_join(thr, NULL);
    TEST_EQ_I(atomic_load(&a.rc), 0, "blocked submit eventually ok");

    wait_processed(&ctx, 6, 5000);
    TEST_EQ_U64(atomic_load(&ctx.count), 6, "all six processed");
    TEST_EQ_U64(awp_pool_drops(pool), 0, "no drops");
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

static void test_destroy_after_concurrent_shutdown_waiters(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    pthread_t th[8];
    shut_arg_t sa[8];
    atomic_int ok;
    int i;

    printf("e2e_lifecycle destroy after many concurrent shutdown waiters\n");
    test_ctx_init(&ctx);
    atomic_init(&ok, 0);
    awp_config_init(&cfg);
    cfg.n_workers = 4;
    cfg.queue_capacity = 32;
    cfg.frame_pool_size = 128;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    for (i = 0; i < 40; i++)
        awp_submit(pool, "t", "s", "x", 1, 0);

    for (i = 0; i < 8; i++) {
        sa[i].pool = pool;
        sa[i].rc_sum = &ok;
        pthread_create(&th[i], NULL, shut_thr, &sa[i]);
    }
    for (i = 0; i < 8; i++)
        pthread_join(th[i], NULL);

    TEST_EQ_I(atomic_load(&ok), 8, "all waiters completed");
    /* Must free cleanly when reclaimable (no quarantine). */
    TEST_CHECK(atomic_load(&pool->quarantined) == 0, "not quarantined");
    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

/**
 * Pass 10 regression: after terminal STOPPED, a late reopen must not leave the
 * ring open (restart_worker re-closes when lifecycle is no longer RUNNING).
 */
static void test_terminal_reopen_recloses(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    test_ctx_t ctx;
    uint32_t i;

    printf("e2e_lifecycle terminal reopen re-closes ring\n");
    test_ctx_init(&ctx);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 16;
    cfg.frame_pool_size = 32;
    cfg.enable_supervisor = 0;
    cfg.process = test_process;
    cfg.user = &ctx;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    TEST_EQ_I(awp_submit(pool, "t", "s", "x", 1, 0), 0, "submit");
    wait_processed(&ctx, 1, 2000);
    TEST_CHECK(awp_pool_shutdown(pool) >= 0, "shutdown");
    TEST_CHECK(atomic_load(&pool->lifecycle) == AWP_LIFE_STOPPED, "STOPPED");

    for (i = 0; i < pool->cfg.n_workers; i++)
        TEST_CHECK(atomic_load(&pool->workers[i].queue.closed) == 1,
                   "rings closed after shutdown");

    /* Late reopen + production helper used by restart_worker. */
    awp_ring_reopen(&pool->workers[0].queue);
    TEST_CHECK(atomic_load(&pool->workers[0].queue.closed) == 0, "reopen opens");
    TEST_EQ_I(awp_post_reopen_terminal_check(pool, 0), 1, "terminal re-close");
    TEST_CHECK(atomic_load(&pool->workers[0].queue.closed) == 1,
               "re-closed after terminal lifecycle check");

    awp_pool_destroy(pool);
    test_ctx_destroy(&ctx);
}

/** Stuck process + short deadline → quarantine; rings must be closed. */
static atomic_int g_life_hang;

static int hang_no_ctx_process(const awp_frame_t *frame, void *user)
{
    (void)frame;
    (void)user;
    while (atomic_load(&g_life_hang))
        test_sleep_ms(10);
    return 0;
}

static void test_quarantine_path_closes_rings(void)
{
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    uint32_t i;
    int rc;

    printf("e2e_lifecycle sticky hang shutdown closes rings\n");
    atomic_store(&g_life_hang, 1);
    awp_config_init(&cfg);
    cfg.n_workers = 2;
    cfg.queue_capacity = 8;
    cfg.frame_pool_size = 16;
    cfg.enable_supervisor = 0;
    cfg.shutdown_deadline_ms = 300;
    cfg.process = hang_no_ctx_process;

    TEST_EQ_I(awp_pool_create(&cfg, &pool), 0, "create");
    TEST_EQ_I(awp_submit(pool, "t", "s", "x", 1, 0), 0, "submit stuck");
    test_sleep_ms(50);
    rc = awp_pool_shutdown(pool);
    TEST_CHECK(rc > 0, "shutdown reports abort/quarantine");
    TEST_CHECK(atomic_load(&pool->quarantined) == 1, "pool quarantined");
    for (i = 0; i < pool->cfg.n_workers; i++)
        TEST_CHECK(atomic_load(&pool->workers[i].queue.closed) == 1,
                   "all rings closed on quarantine path");
    /* Destroy may leak; clear hang only after so no use-after-free of test state. */
    awp_pool_destroy(pool);
    atomic_store(&g_life_hang, 0);
    test_sleep_ms(50); /* allow leaked worker to exit if still running */
}

int main(void)
{
    test_shutdown_drains_accepted();
    test_concurrent_shutdown();
    test_restart_preserves_progress();
    test_restart_with_queued_work();
    test_try_push_hot_shard_no_hold();
    test_destroy_after_concurrent_shutdown_waiters();
    test_terminal_reopen_recloses();
    test_quarantine_path_closes_rings();
    printf("\ne2e_lifecycle: %d passed, %d failed\n", g_passes, g_fails);
    return g_fails ? 1 : 0;
}

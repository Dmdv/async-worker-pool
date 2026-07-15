#include "internal.h"
#include <strings.h> /* strcasecmp */

void awp_config_init(awp_config_t *cfg)
{
    if (!cfg)
        return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->n_workers = 8;
    cfg->queue_capacity = 256;
    cfg->frame_pool_size = 2048;
    cfg->shutdown_deadline_ms = 10000;
    cfg->supervisor_interval_ms = 500;
    cfg->stall_threshold_ms = 5000;
    cfg->enable_supervisor = 1;
    cfg->enable_restart = 1;
    cfg->n_broadcast_workers = 0;
    cfg->broadcast_feeds = NULL;
}

int awp_runtime_enabled(void)
{
    const char *e = getenv("AWP_ENABLED");
    if (!e)
        return 0;
    if (e[0] == '1' && e[1] == '\0')
        return 1;
    if (strcasecmp(e, "true") == 0 || strcasecmp(e, "yes") == 0)
        return 1;
    return 0;
}

static void free_broadcast_feeds(awp_pool_t *pool)
{
    uint32_t i;
    if (!pool->broadcast_feeds)
        return;
    for (i = 0; i < pool->n_broadcast_feeds; i++)
        free(pool->broadcast_feeds[i]);
    free(pool->broadcast_feeds);
    pool->broadcast_feeds = NULL;
    pool->n_broadcast_feeds = 0;
}

static int copy_broadcast_feeds(awp_pool_t *pool, const awp_config_t *cfg)
{
    uint32_t n = 0, i;
    if (!cfg->broadcast_feeds)
        return 0;
    while (cfg->broadcast_feeds[n])
        n++;
    if (n == 0)
        return 0;
    pool->broadcast_feeds = calloc(n, sizeof(char *));
    if (!pool->broadcast_feeds)
        return -ENOMEM;
    for (i = 0; i < n; i++) {
        pool->broadcast_feeds[i] = strdup(cfg->broadcast_feeds[i]);
        if (!pool->broadcast_feeds[i]) {
            free_broadcast_feeds(pool);
            return -ENOMEM;
        }
    }
    pool->n_broadcast_feeds = n;
    return 0;
}

int awp_pool_create(const awp_config_t *cfg, awp_pool_t **out)
{
    awp_pool_t *pool;
    uint32_t i;
    int rc;

    if (!cfg || !out || !cfg->process)
        return -EINVAL;
    if (cfg->n_workers < 1 || cfg->queue_capacity < 1 || cfg->frame_pool_size < 1)
        return -EINVAL;
    if (cfg->n_broadcast_workers > cfg->n_workers)
        return -EINVAL;

    pool = calloc(1, sizeof(*pool));
    if (!pool)
        return -ENOMEM;

    pool->cfg = *cfg;
    pool->shard_base = cfg->n_broadcast_workers;
    pool->n_shard_workers = cfg->n_workers - cfg->n_broadcast_workers;
    if (pool->n_shard_workers == 0 && cfg->n_broadcast_workers == cfg->n_workers) {
        /* all broadcast workers is ok */
    }

    atomic_store(&pool->submitted, 0);
    atomic_store(&pool->dropped, 0);
    atomic_store(&pool->process_errors, 0);
    atomic_store(&pool->shutdown_aborts, 0);
    atomic_store(&pool->seq, 0);
    atomic_store(&pool->running, 0);
    atomic_store(&pool->shutting_down, 0);
    atomic_store(&pool->supervisor_alive, 0);

    rc = copy_broadcast_feeds(pool, cfg);
    if (rc != 0) {
        free(pool);
        return rc;
    }

    rc = awp_frame_pool_init(&pool->frames, cfg->frame_pool_size);
    if (rc != 0)
        goto fail_feeds;

    pool->workers = calloc(cfg->n_workers, sizeof(awp_worker_t));
    pool->metrics_buf = calloc(cfg->n_workers, sizeof(awp_worker_metrics_t));
    if (!pool->workers || !pool->metrics_buf) {
        rc = -ENOMEM;
        goto fail_frames;
    }

    for (i = 0; i < cfg->n_workers; i++) {
        awp_worker_t *w = &pool->workers[i];
        w->id = i;
        w->pool = pool;
        atomic_store(&w->processed, 0);
        atomic_store(&w->process_errors, 0);
        atomic_store(&w->enqueue_blocks, 0);
        atomic_store(&w->blocked_ns, 0);
        atomic_store(&w->queue_hwm, 0);
        atomic_store(&w->last_progress_ns, awp_now_ns());
        atomic_store(&w->restarts, 0);
        atomic_store(&w->alive, 0);
        atomic_store(&w->stop, 0);
        rc = awp_ring_init(&w->queue, cfg->queue_capacity);
        if (rc != 0)
            goto fail_workers;
    }

    for (i = 0; i < cfg->n_workers; i++) {
        rc = awp_worker_start(&pool->workers[i]);
        if (rc != 0)
            goto fail_running;
    }

    if (cfg->enable_supervisor) {
        rc = pthread_create(&pool->supervisor, NULL, awp_supervisor_main, pool);
        if (rc != 0) {
            rc = -rc;
            goto fail_running;
        }
    }

    atomic_store(&pool->running, 1);
    *out = pool;
    return 0;

fail_running:
    atomic_store(&pool->shutting_down, 1);
    for (i = 0; i < cfg->n_workers; i++) {
        atomic_store(&pool->workers[i].stop, 1);
        awp_ring_close(&pool->workers[i].queue);
        if (atomic_load(&pool->workers[i].alive) || pool->workers[i].thread)
            pthread_join(pool->workers[i].thread, NULL);
    }
fail_workers:
    for (i = 0; i < cfg->n_workers; i++)
        awp_ring_destroy(&pool->workers[i].queue);
    free(pool->workers);
    free(pool->metrics_buf);
fail_frames:
    awp_frame_pool_destroy(&pool->frames);
fail_feeds:
    free_broadcast_feeds(pool);
    free(pool);
    return rc;
}

int awp_submit(awp_pool_t *pool,
               const char *feed,
               const char *symbol,
               const void *payload,
               size_t payload_len,
               uint32_t flags)
{
    awp_frame_t *f;
    uint32_t shard;
    uint64_t blocked = 0;
    int rc;
    size_t flen, slen;

    if (!pool || !atomic_load(&pool->running) || atomic_load(&pool->shutting_down))
        return -EINVAL;

    f = awp_frame_pool_acquire(&pool->frames);
    if (!f) {
        /* closed pool or shutdown */
        return -1;
    }

    if (!feed)
        feed = "";
    if (!symbol)
        symbol = "";

    flen = strlen(feed);
    if (flen > AWP_FEED_MAX)
        flen = AWP_FEED_MAX;
    memcpy(f->feed, feed, flen);
    f->feed[flen] = '\0';

    slen = strlen(symbol);
    if (slen > AWP_SYMBOL_MAX)
        slen = AWP_SYMBOL_MAX;
    memcpy(f->symbol, symbol, slen);
    f->symbol[slen] = '\0';

    if (payload_len > AWP_PAYLOAD_MAX)
        payload_len = AWP_PAYLOAD_MAX;
    if (payload && payload_len > 0)
        memcpy(f->payload, payload, payload_len);
    f->payload_len = payload_len;
    f->flags = flags;
    f->seq = atomic_fetch_add(&pool->seq, 1);
    f->submit_ns = awp_now_ns();

    shard = awp_compute_shard(pool, f->feed, f->symbol, flags);
    if (shard >= pool->cfg.n_workers)
        shard = shard % pool->cfg.n_workers;
    f->shard = shard;

    rc = awp_ring_push(&pool->workers[shard].queue, f, &blocked);
    if (rc != 0) {
        /* Shutdown race: recycle; do NOT count as drop from full-queue policy.
         * True full-queue path blocks inside push; rc only when closed. */
        awp_frame_pool_release(&pool->frames, f);
        return -1;
    }
    if (blocked > 0) {
        atomic_fetch_add(&pool->workers[shard].enqueue_blocks, 1);
        atomic_fetch_add(&pool->workers[shard].blocked_ns, blocked);
    }
    {
        uint32_t depth = awp_ring_depth(&pool->workers[shard].queue);
        uint64_t hwm = atomic_load(&pool->workers[shard].queue_hwm);
        while (depth > hwm &&
               !atomic_compare_exchange_weak(&pool->workers[shard].queue_hwm,
                                             &hwm, depth)) {
        }
    }
    atomic_fetch_add(&pool->submitted, 1);
    return 0;
}

int awp_pool_get_metrics(awp_pool_t *pool, awp_pool_metrics_t *out)
{
    uint32_t i;
    if (!pool || !out)
        return -EINVAL;

    /* Snapshot atomics only — no mutex on the metrics path. */
    out->submitted = atomic_load_explicit(&pool->submitted, memory_order_relaxed);
    out->dropped = atomic_load_explicit(&pool->dropped, memory_order_relaxed);
    out->process_errors =
        atomic_load_explicit(&pool->process_errors, memory_order_relaxed);
    out->shutdown_aborts =
        atomic_load_explicit(&pool->shutdown_aborts, memory_order_relaxed);
    out->n_workers = pool->cfg.n_workers;
    out->workers = pool->metrics_buf;
    for (i = 0; i < pool->cfg.n_workers; i++) {
        awp_worker_t *w = &pool->workers[i];
        awp_worker_metrics_t *m = &pool->metrics_buf[i];
        m->processed = atomic_load_explicit(&w->processed, memory_order_relaxed);
        m->process_errors =
            atomic_load_explicit(&w->process_errors, memory_order_relaxed);
        m->enqueue_blocks =
            atomic_load_explicit(&w->enqueue_blocks, memory_order_relaxed);
        m->blocked_ns = atomic_load_explicit(&w->blocked_ns, memory_order_relaxed);
        m->queue_depth = awp_ring_depth(&w->queue);
        m->queue_hwm = atomic_load_explicit(&w->queue_hwm, memory_order_relaxed);
        m->restarts = atomic_load_explicit(&w->restarts, memory_order_relaxed);
        m->last_progress_ns =
            atomic_load_explicit(&w->last_progress_ns, memory_order_relaxed);
        m->alive = atomic_load_explicit(&w->alive, memory_order_relaxed);
    }
    return 0;
}

uint64_t awp_pool_drops(const awp_pool_t *pool)
{
    if (!pool)
        return 0;
    return atomic_load(&pool->dropped);
}

int awp_pool_shutdown(awp_pool_t *pool)
{
    uint32_t i;
    uint32_t deadline;
    uint64_t t0, remaining_ms;
    int aborts = 0;
    int rc;

    if (!pool)
        return -EINVAL;

    if (atomic_exchange(&pool->shutting_down, 1))
        return 0; /* already */

    atomic_store(&pool->running, 0);
    deadline = pool->cfg.shutdown_deadline_ms
                   ? pool->cfg.shutdown_deadline_ms
                   : 10000;
    t0 = awp_now_ns();

    /* Close frame pool so new acquires fail; close rings so workers exit after drain. */
    awp_frame_pool_close(&pool->frames);
    for (i = 0; i < pool->cfg.n_workers; i++) {
        atomic_store(&pool->workers[i].stop, 0); /* allow drain first */
        awp_ring_close(&pool->workers[i].queue);
    }

    /* Join supervisor first (stops restart races). */
    if (pool->cfg.enable_supervisor) {
        /* shutting_down already set; wait for supervisor exit */
        while (atomic_load(&pool->supervisor_alive)) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 5 * 1000 * 1000 };
            nanosleep(&ts, NULL);
            if ((awp_now_ns() - t0) / 1000000ull > deadline)
                break;
        }
        pthread_join(pool->supervisor, NULL);
        atomic_store(&pool->supervisor_alive, 0);
    }

    remaining_ms = deadline;
    for (i = 0; i < pool->cfg.n_workers; i++) {
        int aborted = 0;
        uint64_t used = (awp_now_ns() - t0) / 1000000ull;
        uint32_t per = 100;
        if (used >= deadline) {
            remaining_ms = 0;
        } else {
            remaining_ms = deadline - used;
            per = (uint32_t)(remaining_ms / (pool->cfg.n_workers - i));
            if (per < 50)
                per = 50;
        }
        rc = awp_worker_join(&pool->workers[i], per, &aborted);
        if (aborted || rc > 0) {
            aborts++;
            atomic_fetch_add(&pool->shutdown_aborts, 1);
        }
        /* Drain any leftover frames back to pool (should be empty after close). */
        {
            awp_frame_t *f;
            while (awp_ring_pop(&pool->workers[i].queue, &f) == 0 && f)
                awp_frame_pool_release(&pool->frames, f);
        }
    }

    return aborts;
}

void awp_pool_destroy(awp_pool_t *pool)
{
    uint32_t i;
    if (!pool)
        return;
    if (!atomic_load(&pool->shutting_down))
        (void)awp_pool_shutdown(pool);

    for (i = 0; i < pool->cfg.n_workers; i++)
        awp_ring_destroy(&pool->workers[i].queue);
    free(pool->workers);
    free(pool->metrics_buf);
    awp_frame_pool_destroy(&pool->frames);
    free_broadcast_feeds(pool);
    free(pool);
}

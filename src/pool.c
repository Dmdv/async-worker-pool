#include "internal.h"
#include <strings.h>

void awp_config_init(awp_config_t *cfg)
{
    if (!cfg)
        return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->n_workers = 8;
    cfg->queue_capacity = 256;
    cfg->frame_pool_size = 2048;
    cfg->ring_mode = AWP_RING_MPSC;
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

static void set_life(awp_pool_t *pool, int life)
{
    atomic_store(&pool->lifecycle, life);
    pthread_mutex_lock(&pool->life_mu);
    pthread_cond_broadcast(&pool->life_cv);
    pthread_mutex_unlock(&pool->life_mu);
}

static int any_worker_quarantined(const awp_pool_t *pool)
{
    uint32_t i;
    for (i = 0; i < pool->cfg.n_workers; i++) {
        if (atomic_load(&pool->workers[i].state) == AWP_W_QUARANTINED)
            return 1;
    }
    return 0;
}

static void wait_until_stopped(awp_pool_t *pool)
{
    atomic_fetch_add(&pool->shutdown_waiters, 1);
    pthread_mutex_lock(&pool->life_mu);
    while (atomic_load(&pool->lifecycle) != AWP_LIFE_STOPPED)
        pthread_cond_wait(&pool->life_cv, &pool->life_mu);
    pthread_mutex_unlock(&pool->life_mu);
    atomic_fetch_sub(&pool->shutdown_waiters, 1);
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
    if (cfg->n_broadcast_workers >= cfg->n_workers)
        return -EINVAL; /* need ≥1 sharded worker for symbol traffic */
    if ((unsigned)cfg->ring_mode > (unsigned)AWP_RING_MPMC)
        return -EINVAL;
    if (awp_round_up_pow2(cfg->queue_capacity) == 0)
        return -EINVAL;

    pool = calloc(1, sizeof(*pool));
    if (!pool)
        return -ENOMEM;

    pool->cfg = *cfg;
    pool->shard_base = cfg->n_broadcast_workers;
    pool->n_shard_workers = cfg->n_workers - cfg->n_broadcast_workers;

    atomic_store(&pool->submitted, 0);
    atomic_store(&pool->dropped, 0);
    atomic_store(&pool->process_errors, 0);
    atomic_store(&pool->shutdown_aborts, 0);
    atomic_store(&pool->seq, 0);
    atomic_store(&pool->abandoned, 0);
    atomic_store(&pool->lifecycle, AWP_LIFE_INIT);
    atomic_store(&pool->active_submits, 0);
    atomic_store(&pool->shutdown_waiters, 0);
    atomic_store(&pool->supervisor_stop, 0);
    atomic_store(&pool->supervisor_alive, 0);
    atomic_store(&pool->supervisor_joined, 1); /* no supervisor yet */
    atomic_store(&pool->quarantined, 0);

    if (pthread_mutex_init(&pool->life_mu, NULL) != 0) {
        free(pool);
        return -ENOMEM;
    }
    if (pthread_cond_init(&pool->life_cv, NULL) != 0) {
        pthread_mutex_destroy(&pool->life_mu);
        free(pool);
        return -ENOMEM;
    }
    if (pthread_mutex_init(&pool->metrics_mu, NULL) != 0) {
        pthread_cond_destroy(&pool->life_cv);
        pthread_mutex_destroy(&pool->life_mu);
        free(pool);
        return -ENOMEM;
    }

    rc = copy_broadcast_feeds(pool, cfg);
    if (rc != 0)
        goto fail_mu;

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
        atomic_store(&w->generation, 0);
        atomic_store(&w->state, AWP_W_JOINED);
        atomic_store(&w->stop, 0);
        atomic_store(&w->joined, 1);
        rc = awp_ring_init(&w->queue, cfg->queue_capacity, cfg->ring_mode);
        if (rc != 0)
            goto fail_workers;
    }

    set_life(pool, AWP_LIFE_RUNNING);

    for (i = 0; i < cfg->n_workers; i++) {
        rc = awp_worker_start(&pool->workers[i]);
        if (rc != 0)
            goto fail_running;
    }

    if (cfg->enable_supervisor) {
        atomic_store(&pool->supervisor_joined, 0);
        rc = pthread_create(&pool->supervisor, NULL, awp_supervisor_main, pool);
        if (rc != 0) {
            atomic_store(&pool->supervisor_joined, 1);
            rc = -rc;
            goto fail_running;
        }
    }

    *out = pool;
    return 0;

fail_running:
    atomic_store(&pool->supervisor_stop, 1);
    set_life(pool, AWP_LIFE_DRAINING);
    for (i = 0; i < cfg->n_workers; i++) {
        atomic_store(&pool->workers[i].stop, 1);
        awp_ring_close(&pool->workers[i].queue);
        if (atomic_load(&pool->workers[i].state) == AWP_W_RUNNING ||
            atomic_load(&pool->workers[i].state) == AWP_W_STARTING ||
            atomic_load(&pool->workers[i].state) == AWP_W_EXITED) {
            if (!atomic_load(&pool->workers[i].joined)) {
                pthread_join(pool->workers[i].thread, NULL);
                atomic_store(&pool->workers[i].joined, 1);
            }
        }
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
fail_mu:
    pthread_mutex_destroy(&pool->metrics_mu);
    pthread_cond_destroy(&pool->life_cv);
    pthread_mutex_destroy(&pool->life_mu);
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
    uint64_t blocked_acc = 0;
    int rc;
    size_t flen, slen;
    unsigned spin = 0;

    if (!pool)
        return -EINVAL;
    if (awp_tls_in_callback)
        return -EDEADLK;

    if (!feed)
        feed = "";
    if (!symbol)
        symbol = "";
    flen = strlen(feed);
    slen = strlen(symbol);
    if (flen > AWP_FEED_MAX || slen > AWP_SYMBOL_MAX)
        return -E2BIG;
    if (payload_len > AWP_PAYLOAD_MAX)
        return -E2BIG;
    if (payload_len > 0 && !payload)
        return -EINVAL;

    /*
     * Register before lifecycle re-check so QUIESCING always sees us in
     * active_submits (closes the check-then-increment race).
     */
    atomic_fetch_add(&pool->active_submits, 1);
    if (atomic_load(&pool->lifecycle) != AWP_LIFE_RUNNING) {
        atomic_fetch_sub(&pool->active_submits, 1);
        return -EINVAL;
    }

    shard = awp_compute_shard(pool, feed, symbol, flags);
    if (shard >= pool->cfg.n_workers)
        shard = shard % pool->cfg.n_workers;

    /*
     * Hot-shard isolation: never hold a frame while blocked on a full ring.
     * Acquire → try_push; on full, release frame, park for space, retry.
     */
    for (;;) {
        if (atomic_load(&pool->lifecycle) != AWP_LIFE_RUNNING) {
            atomic_fetch_sub(&pool->active_submits, 1);
            return -EINVAL;
        }

        f = awp_frame_pool_acquire(&pool->frames);
        if (!f) {
            atomic_fetch_sub(&pool->active_submits, 1);
            return -1;
        }

        memcpy(f->feed, feed, flen);
        f->feed[flen] = '\0';
        memcpy(f->symbol, symbol, slen);
        f->symbol[slen] = '\0';
        if (payload_len > 0)
            memcpy(f->payload, payload, payload_len);
        f->payload_len = payload_len;
        f->flags = flags;
        f->seq = atomic_fetch_add(&pool->seq, 1);
        f->submit_ns = awp_now_ns();
        f->shard = shard;

        rc = awp_ring_try_push(&pool->workers[shard].queue, f);
        if (rc == 0)
            break;

        /* Push failed — release frame before waiting (no global hold on full ring). */
        awp_frame_pool_release(&pool->frames, f);

        if (rc == -1 || atomic_load(&pool->workers[shard].queue.closed) ||
            atomic_load(&pool->lifecycle) != AWP_LIFE_RUNNING) {
            atomic_fetch_add(&pool->dropped, 1);
            atomic_fetch_sub(&pool->active_submits, 1);
            return -1;
        }

        /* Full or CAS race: park / spin then retry. */
        if (spin++ < 64)
            awp_cpu_relax();
        else {
            uint64_t t0 = awp_now_ns();
            awp_ring_wait_space(&pool->workers[shard].queue);
            blocked_acc += awp_now_ns() - t0;
            spin = 0;
            atomic_fetch_add(&pool->workers[shard].enqueue_blocks, 1);
        }
    }

    if (blocked_acc > 0)
        atomic_fetch_add(&pool->workers[shard].blocked_ns, blocked_acc);
    {
        uint32_t depth = awp_ring_depth(&pool->workers[shard].queue);
        uint64_t hwm = atomic_load(&pool->workers[shard].queue_hwm);
        while (depth > hwm &&
               !atomic_compare_exchange_weak(&pool->workers[shard].queue_hwm,
                                             &hwm, depth)) {
        }
    }
    atomic_fetch_add(&pool->submitted, 1);
    atomic_fetch_sub(&pool->active_submits, 1);
    return 0;
}

int awp_pool_get_metrics(awp_pool_t *pool, awp_pool_metrics_t *out)
{
    uint32_t i;
    if (!pool || !out)
        return -EINVAL;

    pthread_mutex_lock(&pool->metrics_mu);
    out->submitted = atomic_load_explicit(&pool->submitted, memory_order_relaxed);
    out->dropped = atomic_load_explicit(&pool->dropped, memory_order_relaxed) +
                   atomic_load_explicit(&pool->abandoned, memory_order_relaxed);
    out->process_errors =
        atomic_load_explicit(&pool->process_errors, memory_order_relaxed);
    out->shutdown_aborts =
        atomic_load_explicit(&pool->shutdown_aborts, memory_order_relaxed);
    out->n_workers = pool->cfg.n_workers;
    out->workers = pool->metrics_buf;
    for (i = 0; i < pool->cfg.n_workers; i++) {
        awp_worker_t *w = &pool->workers[i];
        awp_worker_metrics_t *m = &pool->metrics_buf[i];
        int st = atomic_load(&w->state);
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
        m->alive = (st == AWP_W_RUNNING || st == AWP_W_STARTING) ? 1 : 0;
    }
    pthread_mutex_unlock(&pool->metrics_mu);
    return 0;
}

uint64_t awp_pool_drops(const awp_pool_t *pool)
{
    if (!pool)
        return 0;
    return atomic_load(&pool->dropped) + atomic_load(&pool->abandoned);
}

int awp_pool_shutdown(awp_pool_t *pool)
{
    uint32_t i;
    uint32_t deadline_ms;
    uint64_t deadline_ns, t0;
    int aborts = 0;
    int life;
    int rc;

    if (!pool)
        return -EINVAL;
    if (awp_tls_in_callback)
        return -EDEADLK;

    life = atomic_load(&pool->lifecycle);
    if (life == AWP_LIFE_STOPPED)
        return (int)atomic_load(&pool->shutdown_aborts);
    if (life == AWP_LIFE_QUIESCING || life == AWP_LIFE_DRAINING) {
        wait_until_stopped(pool);
        return (int)atomic_load(&pool->shutdown_aborts);
    }

    /* RUNNING -> QUIESCING */
    if (!atomic_compare_exchange_strong(&pool->lifecycle, &life,
                                        AWP_LIFE_QUIESCING)) {
        return awp_pool_shutdown(pool); /* race: re-enter */
    }
    /* Wake life waiters that may care about QUIESCING (none today). */
    pthread_mutex_lock(&pool->life_mu);
    pthread_cond_broadcast(&pool->life_cv);
    pthread_mutex_unlock(&pool->life_mu);

    deadline_ms = pool->cfg.shutdown_deadline_ms
                      ? pool->cfg.shutdown_deadline_ms
                      : 10000;
    t0 = awp_now_ns();
    deadline_ns = t0 + (uint64_t)deadline_ms * 1000000ull;

    /* Wait for in-flight submits. */
    while (atomic_load(&pool->active_submits) > 0 && awp_now_ns() < deadline_ns) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 1 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    if (atomic_load(&pool->active_submits) > 0) {
        /* Cannot prove submitters have left rings — sticky leak. */
        awp_pool_mark_quarantined(pool);
        fprintf(stderr,
                "[awp] shutdown: active_submits still > 0 after deadline; "
                "pool quarantined\n");
    }

    /* Stop supervisor before closing rings (no restarts during drain). */
    atomic_store(&pool->supervisor_stop, 1);
    set_life(pool, AWP_LIFE_DRAINING);

    if (pool->cfg.enable_supervisor && !atomic_load(&pool->supervisor_joined)) {
        while (atomic_load(&pool->supervisor_alive) && awp_now_ns() < deadline_ns) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
        if (!atomic_load(&pool->supervisor_alive)) {
            if (!atomic_exchange(&pool->supervisor_joined, 1))
                pthread_join(pool->supervisor, NULL);
        } else {
            /* Supervisor still running past deadline — must not free pool. */
            awp_pool_mark_quarantined(pool);
            fprintf(stderr,
                    "[awp] shutdown: supervisor not joined; pool quarantined\n");
        }
    }

    /* Close rings: workers drain then exit on closed+empty. */
    for (i = 0; i < pool->cfg.n_workers; i++)
        awp_ring_close(&pool->workers[i].queue);
    awp_frame_pool_close(&pool->frames);

    for (i = 0; i < pool->cfg.n_workers; i++) {
        int aborted = 0;
        rc = awp_worker_join_deadline(&pool->workers[i], deadline_ns, &aborted);
        if (aborted || rc > 0) {
            aborts++;
            atomic_fetch_add(&pool->shutdown_aborts, 1);
        }
        /* Drain leftovers only if not quarantined (thread may still own frames). */
        if (atomic_load(&pool->workers[i].state) != AWP_W_QUARANTINED) {
            awp_frame_t *f;
            while (awp_ring_pop(&pool->workers[i].queue, &f) == 0 && f) {
                atomic_fetch_add(&pool->abandoned, 1);
                awp_frame_pool_release(&pool->frames, f);
            }
        }
    }

    if (any_worker_quarantined(pool) || atomic_load(&pool->active_submits) > 0)
        awp_pool_mark_quarantined(pool);

    set_life(pool, AWP_LIFE_STOPPED);
    return aborts;
}

void awp_pool_destroy(awp_pool_t *pool)
{
    uint32_t i;
    if (!pool)
        return;

    /* Cannot reclaim while inside process()/on_error() on this thread. */
    if (awp_tls_in_callback) {
        awp_pool_mark_quarantined(pool);
        fprintf(stderr,
                "[awp] destroy called from callback; marking quarantine (no free)\n");
        return;
    }

    if (atomic_load(&pool->lifecycle) != AWP_LIFE_STOPPED)
        (void)awp_pool_shutdown(pool);

    /* Wait for concurrent shutdown waiters to leave life_cv before free. */
    {
        unsigned spins = 0;
        while (atomic_load(&pool->shutdown_waiters) > 0) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 1 * 1000 * 1000 };
            nanosleep(&ts, NULL);
            if (++spins > 30000) { /* ~30s safety */
                awp_pool_mark_quarantined(pool);
                break;
            }
        }
    }

    if (atomic_load(&pool->quarantined) || any_worker_quarantined(pool) ||
        atomic_load(&pool->active_submits) > 0 ||
        (pool->cfg.enable_supervisor && !atomic_load(&pool->supervisor_joined))) {
        fprintf(stderr,
                "[awp] destroy: live references possible; leaking pool memory "
                "to avoid UAF\n");
        return;
    }

    for (i = 0; i < pool->cfg.n_workers; i++)
        awp_ring_destroy(&pool->workers[i].queue);
    free(pool->workers);
    free(pool->metrics_buf);
    awp_frame_pool_destroy(&pool->frames);
    free_broadcast_feeds(pool);
    pthread_mutex_destroy(&pool->metrics_mu);
    pthread_cond_destroy(&pool->life_cv);
    pthread_mutex_destroy(&pool->life_mu);
    free(pool);
}

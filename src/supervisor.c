#include "internal.h"

static int life_is_running(awp_pool_t *pool)
{
    return atomic_load(&pool->lifecycle) == AWP_LIFE_RUNNING &&
           !atomic_load(&pool->supervisor_stop);
}

/**
 * After a reopen, if the pool is no longer RUNNING (shutdown won), re-close
 * the ring so producers cannot re-park on a full open shard. Returns 0 if
 * still running, 1 if re-closed (terminal).
 */
int awp_post_reopen_terminal_check(awp_pool_t *pool, uint32_t worker_id)
{
    awp_worker_t *w;
    if (!pool || worker_id >= pool->cfg.n_workers)
        return -1;
    w = &pool->workers[worker_id];
    if (!life_is_running(pool)) {
        awp_ring_close(&w->queue);
        return 1;
    }
    return 0;
}

static int join_worker_thread(awp_worker_t *w)
{
    int rc;
    if (atomic_load(&w->joined))
        return 0;
    rc = pthread_join(w->thread, NULL);
    if (rc != 0) {
        awp_pool_mark_quarantined(w->pool);
        return -rc;
    }
    atomic_store(&w->joined, 1);
    atomic_store(&w->state, AWP_W_JOINED);
    return 0;
}

static int restart_worker(awp_pool_t *pool, awp_worker_t *w)
{
    if (!life_is_running(pool))
        return -1;
    if (join_worker_thread(w) != 0)
        return -1;
    /* Do not reopen after shutdown has won terminal close. */
    if (!life_is_running(pool)) {
        awp_ring_close(&w->queue); /* keep closed if shutdown already closed */
        return -1;
    }
    /* Preserve backlog: reopen if closed, keep cells. */
    awp_ring_reopen(&w->queue);
    atomic_store(&w->last_progress_ns, awp_now_ns());
    if (awp_post_reopen_terminal_check(pool, w->id) != 0)
        return -1;
    if (awp_worker_start(w) == 0) {
        atomic_fetch_add(&w->restarts, 1);
        return 0;
    }
    /* Restart failed: no consumer — close shard and reject admission. */
    awp_ring_close(&w->queue);
    awp_pool_mark_quarantined(pool);
    atomic_fetch_add(&pool->shutdown_aborts, 1);
    atomic_store(&pool->supervisor_stop, 1); /* stop further restarts */
    fprintf(stderr,
            "[awp] supervisor: worker %u restart failed; shard closed, pool quarantined\n",
            w->id);
    return -1;
}

void *awp_supervisor_main(void *arg)
{
    awp_pool_t *pool = (awp_pool_t *)arg;
    uint32_t interval = pool->cfg.supervisor_interval_ms
                            ? pool->cfg.supervisor_interval_ms
                            : 500;
    uint32_t stall_ms = pool->cfg.stall_threshold_ms
                            ? pool->cfg.stall_threshold_ms
                            : 5000;

    /* Joined flag is owned by shutdown/create, not rewritten here. */
    atomic_store(&pool->supervisor_phase, 2); /* alive */
    atomic_store(&pool->supervisor_alive, 1);

    while (life_is_running(pool)) {
        uint32_t i;
        uint64_t now = awp_now_ns();
        uint64_t slice_end = now + (uint64_t)interval * 1000000ull;

        for (i = 0; i < pool->cfg.n_workers && life_is_running(pool); i++) {
            awp_worker_t *w = &pool->workers[i];
            int st = atomic_load(&w->state);
            uint64_t last = atomic_load(&w->last_progress_ns);
            uint64_t idle_ms;

            if (st == AWP_W_QUARANTINED)
                continue;

            if (st == AWP_W_EXITED && pool->cfg.enable_restart) {
                (void)restart_worker(pool, w);
                continue;
            }

            if (st != AWP_W_RUNNING)
                continue;

            idle_ms = (now > last) ? (now - last) / 1000000ull : 0;
            /*
             * Stall: queue has work but no progress (likely stuck in process).
             * Request stop; quarantine only if still no progress after grace.
             */
            if (pool->cfg.enable_restart && idle_ms > stall_ms &&
                awp_ring_depth(&w->queue) > 0) {
                uint64_t grace;
                uint64_t last_seen = last;
                fprintf(stderr,
                        "[awp] supervisor: worker %u stalled (%llums, depth=%u)\n",
                        w->id, (unsigned long long)idle_ms,
                        awp_ring_depth(&w->queue));
                atomic_store(&w->stop, 1);
                grace = awp_now_ns() + (uint64_t)stall_ms * 1000000ull;
                while (atomic_load(&w->state) == AWP_W_RUNNING &&
                       awp_now_ns() < grace && life_is_running(pool)) {
                    uint64_t prog = atomic_load(&w->last_progress_ns);
                    if (prog > last_seen) {
                        /* Made progress — cancel stop request and continue. */
                        atomic_store(&w->stop, 0);
                        last_seen = prog;
                        break;
                    }
                    {
                        struct timespec t2 = { .tv_sec = 0,
                                              .tv_nsec = 5 * 1000 * 1000 };
                        nanosleep(&t2, NULL);
                    }
                }
                if (!life_is_running(pool))
                    break;
                if (atomic_load(&w->state) == AWP_W_EXITED) {
                    (void)restart_worker(pool, w);
                } else if (atomic_load(&w->state) == AWP_W_RUNNING &&
                           atomic_load(&w->stop) &&
                           atomic_load(&w->last_progress_ns) <= last) {
                    /* Still no progress since stall detection. */
                    atomic_store(&w->state, AWP_W_QUARANTINED);
                    /* Wake producers parked on this full shard. */
                    awp_ring_close(&w->queue);
                    awp_pool_mark_quarantined(pool);
                    atomic_fetch_add(&pool->shutdown_aborts, 1);
                    fprintf(stderr,
                            "[awp] supervisor: worker %u quarantined (no progress); "
                            "shard closed\n",
                            w->id);
                } else {
                    atomic_store(&w->stop, 0); /* healthy / recovering */
                }
            }
        }

        while (life_is_running(pool) && awp_now_ns() < slice_end) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 5 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }

    atomic_store(&pool->supervisor_phase, 3); /* exited / joinable */
    atomic_store(&pool->supervisor_alive, 0);
    pthread_mutex_lock(&pool->life_mu);
    pthread_cond_broadcast(&pool->life_cv);
    pthread_mutex_unlock(&pool->life_mu);
    return NULL;
}

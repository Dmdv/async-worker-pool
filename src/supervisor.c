#include "internal.h"

static int life_is_running(awp_pool_t *pool)
{
    return atomic_load(&pool->lifecycle) == AWP_LIFE_RUNNING &&
           !atomic_load(&pool->supervisor_stop);
}

static int restart_worker(awp_pool_t *pool, awp_worker_t *w)
{
    if (!life_is_running(pool))
        return -1;
    if (!atomic_exchange(&w->joined, 1))
        pthread_join(w->thread, NULL);
    atomic_store(&w->state, AWP_W_JOINED);
    /* Preserve backlog: reopen if closed, keep cells. */
    awp_ring_reopen(&w->queue);
    atomic_store(&w->last_progress_ns, awp_now_ns());
    if (!life_is_running(pool))
        return -1;
    if (awp_worker_start(w) == 0) {
        atomic_fetch_add(&w->restarts, 1);
        return 0;
    }
    /* Restart failed: leave JOINED, mark degraded. */
    awp_pool_mark_quarantined(pool);
    fprintf(stderr,
            "[awp] supervisor: worker %u restart failed; pool quarantined\n",
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

    atomic_store(&pool->supervisor_joined, 0);
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

            /* Unexpected exit: join and restart WITHOUT destroying queue. */
            if (st == AWP_W_EXITED && pool->cfg.enable_restart) {
                (void)restart_worker(pool, w);
                continue;
            }

            if (st != AWP_W_RUNNING)
                continue;

            idle_ms = (now > last) ? (now - last) / 1000000ull : 0;
            /*
             * Stall: queue has work but no progress (likely stuck in process).
             * Do NOT start a second consumer. Request stop; if still stuck
             * after grace, quarantine (never free storage).
             */
            if (pool->cfg.enable_restart && idle_ms > stall_ms &&
                awp_ring_depth(&w->queue) > 0) {
                uint64_t grace;
                fprintf(stderr,
                        "[awp] supervisor: worker %u stalled (%llums, depth=%u)\n",
                        w->id, (unsigned long long)idle_ms,
                        awp_ring_depth(&w->queue));
                atomic_store(&w->stop, 1);
                grace = awp_now_ns() + (uint64_t)stall_ms * 1000000ull;
                while (atomic_load(&w->state) == AWP_W_RUNNING &&
                       awp_now_ns() < grace && life_is_running(pool)) {
                    struct timespec t2 = { .tv_sec = 0, .tv_nsec = 5 * 1000 * 1000 };
                    nanosleep(&t2, NULL);
                }
                if (!life_is_running(pool))
                    break;
                if (atomic_load(&w->state) == AWP_W_EXITED) {
                    (void)restart_worker(pool, w);
                } else if (atomic_load(&w->state) == AWP_W_RUNNING) {
                    atomic_store(&w->state, AWP_W_QUARANTINED);
                    awp_pool_mark_quarantined(pool);
                    fprintf(stderr,
                            "[awp] supervisor: worker %u quarantined (no second consumer)\n",
                            w->id);
                }
            }
        }

        /* Interruptible interval sleep so shutdown is not blocked on interval. */
        while (life_is_running(pool) && awp_now_ns() < slice_end) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 5 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }

    atomic_store(&pool->supervisor_alive, 0);
    pthread_mutex_lock(&pool->life_mu);
    pthread_cond_broadcast(&pool->life_cv);
    pthread_mutex_unlock(&pool->life_mu);
    return NULL;
}

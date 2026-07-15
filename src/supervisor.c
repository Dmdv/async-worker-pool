#include "internal.h"

void *awp_supervisor_main(void *arg)
{
    awp_pool_t *pool = (awp_pool_t *)arg;
    uint32_t interval = pool->cfg.supervisor_interval_ms
                            ? pool->cfg.supervisor_interval_ms
                            : 500;
    uint32_t stall_ms = pool->cfg.stall_threshold_ms
                            ? pool->cfg.stall_threshold_ms
                            : 5000;

    atomic_store(&pool->supervisor_alive, 1);

    while (!atomic_load(&pool->shutting_down)) {
        uint32_t i;
        uint64_t now = awp_now_ns();
        struct timespec ts;

        for (i = 0; i < pool->cfg.n_workers; i++) {
            awp_worker_t *w = &pool->workers[i];
            uint64_t last = atomic_load(&w->last_progress_ns);
            int alive = atomic_load(&w->alive);
            uint64_t idle_ms;

            if (!alive && pool->cfg.enable_restart &&
                !atomic_load(&pool->shutting_down)) {
                /* Worker exited unexpectedly — rejoin and restart. */
                uint32_t cap = w->queue.capacity
                                   ? w->queue.capacity
                                   : pool->cfg.queue_capacity;
                pthread_join(w->thread, NULL);
                awp_ring_destroy(&w->queue);
                if (awp_ring_init(&w->queue, cap) != 0)
                    continue;
                atomic_store(&w->last_progress_ns, awp_now_ns());
                if (awp_worker_start(w) == 0)
                    atomic_fetch_add(&w->restarts, 1);
                continue;
            }

            if (!alive)
                continue;

            idle_ms = (now > last) ? (now - last) / 1000000ull : 0;
            /*
             * Stall detection only when queue has work but no progress.
             * Prefer cooperative stop (close ring) before cancel so mutexes
             * are released via cleanup handlers at cond_wait cancel points.
             */
            if (pool->cfg.enable_restart && idle_ms > stall_ms &&
                awp_ring_depth(&w->queue) > 0) {
                uint32_t cap = w->queue.capacity
                                   ? w->queue.capacity
                                   : pool->cfg.queue_capacity;
                uint64_t wait_deadline;
                fprintf(stderr,
                        "[awp] supervisor: worker %u stalled (%llums, depth=%u) — restarting\n",
                        w->id, (unsigned long long)idle_ms,
                        awp_ring_depth(&w->queue));
                atomic_store(&w->stop, 1);
                awp_ring_close(&w->queue);
                wait_deadline = awp_now_ns() + 200000000ull; /* 200ms */
                while (atomic_load(&w->alive) && awp_now_ns() < wait_deadline) {
                    struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
                    nanosleep(&ts, NULL);
                }
                if (atomic_load(&w->alive)) {
                    pthread_cancel(w->thread);
                    pthread_join(w->thread, NULL);
                } else {
                    pthread_join(w->thread, NULL);
                }
                atomic_store(&w->alive, 0);
                awp_ring_destroy(&w->queue);
                if (awp_ring_init(&w->queue, cap) != 0)
                    continue;
                atomic_store(&w->last_progress_ns, awp_now_ns());
                if (awp_worker_start(w) == 0)
                    atomic_fetch_add(&w->restarts, 1);
            }
        }

        ts.tv_sec = interval / 1000;
        ts.tv_nsec = (long)(interval % 1000) * 1000000L;
        nanosleep(&ts, NULL);
    }

    atomic_store(&pool->supervisor_alive, 0);
    return NULL;
}

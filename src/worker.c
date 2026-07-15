#include "internal.h"

_Thread_local int awp_tls_in_callback = 0;

void *awp_worker_main(void *arg)
{
    awp_worker_t *w = (awp_worker_t *)arg;
    awp_pool_t *pool = w->pool;

    /* Never cancel inside process(); only cooperative stop between frames. */
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

    atomic_store(&w->state, AWP_W_RUNNING);
    atomic_store(&w->last_progress_ns, awp_now_ns());

    for (;;) {
        awp_frame_t *frame = NULL;
        int prc;
        uint32_t depth;

        /* Cooperative stop: exit when idle (do not block on empty ring). */
        if (atomic_load(&w->stop) && awp_ring_depth(&w->queue) == 0)
            break;

        if (awp_ring_pop(&w->queue, &frame) != 0) {
            /* closed and empty — normal drain completion */
            break;
        }
        if (!frame)
            continue;

        depth = awp_ring_depth(&w->queue);
        {
            uint64_t hwm = atomic_load(&w->queue_hwm);
            while (depth > hwm &&
                   !atomic_compare_exchange_weak(&w->queue_hwm, &hwm, depth)) {
            }
        }

        atomic_store(&w->last_progress_ns, awp_now_ns());

        prc = 0;
        if (pool->cfg.process) {
            /* Guard process + on_error against nested pool API use. */
            awp_tls_in_callback = 1;
            prc = pool->cfg.process(frame, pool->cfg.user);
            if (prc != 0) {
                atomic_fetch_add(&w->process_errors, 1);
                atomic_fetch_add(&pool->process_errors, 1);
                if (pool->cfg.on_error)
                    pool->cfg.on_error(frame, prc, pool->cfg.user);
            }
            awp_tls_in_callback = 0;
        }

        atomic_fetch_add(&w->processed, 1);
        atomic_store(&w->last_progress_ns, awp_now_ns());
        awp_frame_pool_release(&pool->frames, frame);
    }

    atomic_store(&w->state, AWP_W_EXITED);
    return NULL;
}

int awp_worker_start(awp_worker_t *w)
{
    int rc;
    if (!w)
        return -EINVAL;
    atomic_store(&w->stop, 0);
    atomic_store(&w->joined, 0);
    atomic_store(&w->state, AWP_W_STARTING);
    atomic_fetch_add(&w->generation, 1);
    rc = pthread_create(&w->thread, NULL, awp_worker_main, w);
    if (rc != 0) {
        atomic_store(&w->state, AWP_W_JOINED);
        atomic_store(&w->joined, 1);
        return -rc;
    }
    return 0;
}

int awp_worker_join_deadline(awp_worker_t *w, uint64_t deadline_ns, int *aborted)
{
    int st;

    if (aborted)
        *aborted = 0;
    if (!w)
        return -EINVAL;
    if (atomic_load(&w->joined))
        return 0;

    st = atomic_load(&w->state);
    if (st == AWP_W_QUARANTINED) {
        awp_pool_mark_quarantined(w->pool);
        if (aborted)
            *aborted = 1;
        return 1;
    }

    /* Wait for EXITED or deadline. */
    while (atomic_load(&w->state) == AWP_W_STARTING ||
           atomic_load(&w->state) == AWP_W_RUNNING) {
        if (awp_now_ns() >= deadline_ns)
            break;
        {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }

    st = atomic_load(&w->state);
    if (st == AWP_W_EXITED || st == AWP_W_JOINED) {
        if (!atomic_exchange(&w->joined, 1)) {
            int rc = pthread_join(w->thread, NULL);
            atomic_store(&w->state, AWP_W_JOINED);
            return rc == 0 ? 0 : -rc;
        }
        return 0;
    }

    /* Still running past deadline: mark quarantine — never cancel/detach/free. */
    atomic_store(&w->stop, 1);
    awp_ring_close(&w->queue);
    {
        uint64_t grace = awp_now_ns() + 200000000ull;
        while ((atomic_load(&w->state) == AWP_W_RUNNING ||
                atomic_load(&w->state) == AWP_W_STARTING) &&
               awp_now_ns() < grace) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }
    if (atomic_load(&w->state) == AWP_W_EXITED) {
        if (!atomic_exchange(&w->joined, 1))
            pthread_join(w->thread, NULL);
        atomic_store(&w->state, AWP_W_JOINED);
        if (aborted)
            *aborted = 1;
        return 1;
    }

    atomic_store(&w->state, AWP_W_QUARANTINED);
    awp_pool_mark_quarantined(w->pool);
    if (aborted)
        *aborted = 1;
    fprintf(stderr,
            "[awp] worker %u quarantined (stuck in process); pool must not free\n",
            w->id);
    return 1;
}

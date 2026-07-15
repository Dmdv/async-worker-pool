#include "internal.h"

void *awp_worker_main(void *arg)
{
    awp_worker_t *w = (awp_worker_t *)arg;
    awp_pool_t *pool = w->pool;

    /* Cancel points: pthread_testcancel in ring spin backoff. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    atomic_store(&w->alive, 1);
    atomic_store(&w->last_progress_ns, awp_now_ns());

    while (!atomic_load(&w->stop) && !atomic_load(&pool->shutting_down)) {
        awp_frame_t *frame = NULL;
        int prc;
        uint32_t depth;

        if (awp_ring_pop(&w->queue, &frame) != 0) {
            /* closed and empty */
            break;
        }
        if (!frame)
            continue;

        /* If shutdown raced after pop, recycle and exit. */
        if (atomic_load(&pool->shutting_down) || atomic_load(&w->stop)) {
            awp_frame_pool_release(&pool->frames, frame);
            break;
        }

        depth = awp_ring_depth(&w->queue);
        {
            uint64_t hwm = atomic_load(&w->queue_hwm);
            while (depth > hwm &&
                   !atomic_compare_exchange_weak(&w->queue_hwm, &hwm, depth)) {
                /* retry */
            }
        }

        atomic_store(&w->last_progress_ns, awp_now_ns());

        prc = 0;
        if (pool->cfg.process) {
            /* Soft fault isolation: never abort the worker loop on error.
             * Process callbacks should observe shutting_down on shutdown. */
            prc = pool->cfg.process(frame, pool->cfg.user);
        }
        if (prc != 0) {
            atomic_fetch_add(&w->process_errors, 1);
            atomic_fetch_add(&pool->process_errors, 1);
            if (pool->cfg.on_error)
                pool->cfg.on_error(frame, prc, pool->cfg.user);
        }

        atomic_fetch_add(&w->processed, 1);
        atomic_store(&w->last_progress_ns, awp_now_ns());
        awp_frame_pool_release(&pool->frames, frame);
    }

    atomic_store(&w->alive, 0);
    return NULL;
}

int awp_worker_start(awp_worker_t *w)
{
    int rc;
    if (!w)
        return -EINVAL;
    atomic_store(&w->stop, 0);
    atomic_store(&w->alive, 0);
    rc = pthread_create(&w->thread, NULL, awp_worker_main, w);
    return rc == 0 ? 0 : -rc;
}

int awp_worker_join(awp_worker_t *w, uint32_t deadline_ms, int *aborted)
{
    int rc;
    uint64_t deadline_ns;
    uint64_t now;

    if (aborted)
        *aborted = 0;
    if (!w)
        return -EINVAL;

    deadline_ns = awp_now_ns() + (uint64_t)deadline_ms * 1000000ull;

    /* Cooperative wait: poll alive flag with short sleeps (portable join timeout). */
    while (atomic_load(&w->alive)) {
        now = awp_now_ns();
        if (now >= deadline_ns)
            break;
        {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 }; /* 2ms */
            nanosleep(&ts, NULL);
        }
    }

    if (atomic_load(&w->alive)) {
        /* Force stop: cancel then join with a second bound. */
        atomic_store(&w->stop, 1);
        awp_ring_close(&w->queue);
        pthread_cancel(w->thread);

        /* Poll after cancel; if still alive past extra 500ms, detach. */
        {
            uint64_t extra = awp_now_ns() + 500000000ull;
            while (atomic_load(&w->alive) && awp_now_ns() < extra) {
                struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
                nanosleep(&ts, NULL);
            }
        }

        if (atomic_load(&w->alive)) {
            /* Last resort: detach so destroy can proceed. */
            pthread_detach(w->thread);
            atomic_store(&w->alive, 0);
            if (aborted)
                *aborted = 1;
            return 1;
        }

        rc = pthread_join(w->thread, NULL);
        atomic_store(&w->alive, 0);
        if (aborted)
            *aborted = 1;
        return rc == 0 ? 1 : -rc; /* 1 = aborted */
    }

    rc = pthread_join(w->thread, NULL);
    return rc == 0 ? 0 : -rc;
}

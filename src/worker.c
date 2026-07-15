#include "internal.h"

_Thread_local int awp_tls_in_callback = 0;

#ifdef AWP_TEST_HOOKS
atomic_int awp_test_fail_next_worker_start = 0;
#endif

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
#ifdef AWP_TEST_HOOKS
    /* Consume one fail-next token (test sets atomic to 1). */
    if (atomic_exchange(&awp_test_fail_next_worker_start, 0) != 0) {
        atomic_store(&w->state, AWP_W_JOINED);
        atomic_store(&w->joined, 1);
        return -EAGAIN;
    }
#endif
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

/**
 * Join thr by absolute CLOCK_MONOTONIC deadline.
 * @return 0 joined, 1 timed out (OS thread may still be reaped async), <0 error.
 *
 * Linux: pthread_timedjoin_np.
 * Elsewhere: detached heap helper performs join while this thread waits on a
 * cond with absolute budget — never detaches/cancels the target thread.
 */
typedef struct {
    pthread_t target;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int done;     /* 1 when join finished */
    int rc;       /* pthread_join result */
    int released; /* 1 if waiter timed out and freed ownership to helper */
} awp_join_box_t;

static void awp_join_box_free(awp_join_box_t *b)
{
    pthread_cond_destroy(&b->cv);
    pthread_mutex_destroy(&b->mu);
    free(b);
}

static void *awp_join_helper(void *arg)
{
    awp_join_box_t *b = (awp_join_box_t *)arg;
    int rc = pthread_join(b->target, NULL);
    int free_self = 0;

    pthread_mutex_lock(&b->mu);
    b->rc = rc;
    b->done = 1;
    if (b->released)
        free_self = 1;
    pthread_cond_broadcast(&b->cv);
    pthread_mutex_unlock(&b->mu);

    if (free_self)
        awp_join_box_free(b);
    return NULL;
}

int awp_pthread_join_deadline(pthread_t thr, uint64_t deadline_ns)
{
    /* Single portable path: pure CLOCK_MONOTONIC budget (no REALTIME). */
    awp_join_box_t *box;
    pthread_t helper;
    pthread_attr_t attr;
    int rc;
    int free_box = 1;

    if (awp_now_ns() >= deadline_ns)
        return 1;

    box = calloc(1, sizeof(*box));
    if (!box)
        return -ENOMEM;
    box->target = thr;
    if (pthread_mutex_init(&box->mu, NULL) != 0) {
        free(box);
        return -ENOMEM;
    }
    if (pthread_cond_init(&box->cv, NULL) != 0) {
        pthread_mutex_destroy(&box->mu);
        free(box);
        return -ENOMEM;
    }
    if (pthread_attr_init(&attr) != 0) {
        awp_join_box_free(box);
        return -ENOMEM;
    }
    (void)pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    rc = pthread_create(&helper, &attr, awp_join_helper, box);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        awp_join_box_free(box);
        return -rc;
    }

    pthread_mutex_lock(&box->mu);
    while (!box->done) {
        if (awp_now_ns() >= deadline_ns) {
            box->released = 1;
            free_box = 0;
            pthread_mutex_unlock(&box->mu);
            return 1;
        }
        pthread_mutex_unlock(&box->mu);
        {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
        pthread_mutex_lock(&box->mu);
    }
    rc = box->rc;
    pthread_mutex_unlock(&box->mu);
    if (free_box)
        awp_join_box_free(box);
    return rc == 0 ? 0 : -rc;
}

static int join_exited_deadline(awp_worker_t *w, uint64_t deadline_ns)
{
    int rc;
    if (atomic_load(&w->joined))
        return 0;
    rc = awp_pthread_join_deadline(w->thread, deadline_ns);
    if (rc == 0) {
        atomic_store(&w->joined, 1);
        atomic_store(&w->state, AWP_W_JOINED);
        return 0;
    }
    if (rc == 1) {
        awp_pool_mark_quarantined(w->pool);
        atomic_store(&w->state, AWP_W_QUARANTINED);
        fprintf(stderr,
                "[awp] worker %u join past deadline; quarantined (not joined)\n",
                w->id);
        return 1;
    }
    awp_pool_mark_quarantined(w->pool);
    fprintf(stderr,
            "[awp] worker %u pthread_join failed (%d); quarantined\n",
            w->id, -rc);
    return -rc;
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

    /* Wait for EXITED or absolute deadline. */
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
        int rc = join_exited_deadline(w, deadline_ns);
        if (rc != 0) {
            if (aborted)
                *aborted = 1;
            return 1;
        }
        return 0;
    }

    /* Still running past deadline: cooperative stop + close; grace within budget. */
    atomic_store(&w->stop, 1);
    awp_ring_close(&w->queue);
    {
        uint64_t grace_end = deadline_ns;
        uint64_t now = awp_now_ns();
        if (grace_end > now + 200000000ull)
            grace_end = now + 200000000ull; /* cap 200ms when budget allows */
        while ((atomic_load(&w->state) == AWP_W_RUNNING ||
                atomic_load(&w->state) == AWP_W_STARTING) &&
               awp_now_ns() < grace_end && awp_now_ns() < deadline_ns) {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 2 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }
    if (atomic_load(&w->state) == AWP_W_EXITED) {
        int rc = join_exited_deadline(w, deadline_ns);
        if (aborted)
            *aborted = 1;
        (void)rc;
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

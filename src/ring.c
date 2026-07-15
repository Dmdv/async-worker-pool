#include "internal.h"

static void awp_mutex_cleanup(void *arg)
{
    pthread_mutex_unlock((pthread_mutex_t *)arg);
}

int awp_ring_init(awp_ring_t *r, uint32_t capacity)
{
    if (!r || capacity < 1)
        return -EINVAL;
    memset(r, 0, sizeof(*r));
    r->slots = calloc(capacity, sizeof(awp_frame_t *));
    if (!r->slots)
        return -ENOMEM;
    r->capacity = capacity;
    atomic_store(&r->closed, 0);
    if (pthread_mutex_init(&r->mu, NULL) != 0) {
        free(r->slots);
        return -ENOMEM;
    }
    if (pthread_cond_init(&r->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&r->mu);
        free(r->slots);
        return -ENOMEM;
    }
    if (pthread_cond_init(&r->not_full, NULL) != 0) {
        pthread_cond_destroy(&r->not_empty);
        pthread_mutex_destroy(&r->mu);
        free(r->slots);
        return -ENOMEM;
    }
    return 0;
}

void awp_ring_destroy(awp_ring_t *r)
{
    if (!r)
        return;
    free(r->slots);
    r->slots = NULL;
    pthread_cond_destroy(&r->not_full);
    pthread_cond_destroy(&r->not_empty);
    pthread_mutex_destroy(&r->mu);
}

void awp_ring_close(awp_ring_t *r)
{
    if (!r)
        return;
    atomic_store(&r->closed, 1);
    pthread_mutex_lock(&r->mu);
    pthread_cond_broadcast(&r->not_empty);
    pthread_cond_broadcast(&r->not_full);
    pthread_mutex_unlock(&r->mu);
}

int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out)
{
    uint64_t t0 = 0;
    int did_block = 0;
    int rc = 0;

    if (!r || !frame)
        return -EINVAL;

    pthread_mutex_lock(&r->mu);
    pthread_cleanup_push(awp_mutex_cleanup, &r->mu);

    while (r->count == r->capacity && !atomic_load(&r->closed)) {
        if (!did_block) {
            t0 = awp_now_ns();
            did_block = 1;
        }
        pthread_cond_wait(&r->not_full, &r->mu);
    }
    if (atomic_load(&r->closed)) {
        rc = -1;
    } else {
        r->slots[r->tail] = frame;
        r->tail = (r->tail + 1u) % r->capacity;
        r->count++;
        pthread_cond_signal(&r->not_empty);
        rc = 0;
    }

    pthread_cleanup_pop(1); /* unlock */

    if (did_block && blocked_ns_out)
        *blocked_ns_out = awp_now_ns() - t0;
    else if (blocked_ns_out)
        *blocked_ns_out = 0;
    return rc;
}

int awp_ring_pop(awp_ring_t *r, awp_frame_t **out)
{
    int rc = 0;

    if (!r || !out)
        return -EINVAL;

    pthread_mutex_lock(&r->mu);
    pthread_cleanup_push(awp_mutex_cleanup, &r->mu);

    while (r->count == 0 && !atomic_load(&r->closed)) {
        pthread_cond_wait(&r->not_empty, &r->mu);
    }
    if (r->count == 0 && atomic_load(&r->closed)) {
        rc = -1;
    } else {
        *out = r->slots[r->head];
        r->slots[r->head] = NULL;
        r->head = (r->head + 1u) % r->capacity;
        r->count--;
        pthread_cond_signal(&r->not_full);
        rc = 0;
    }

    pthread_cleanup_pop(1); /* unlock */
    return rc;
}

uint32_t awp_ring_depth(awp_ring_t *r)
{
    uint32_t d;
    if (!r)
        return 0;
    /* Try-lock style depth for supervisor: avoid blocking forever if
     * a cancelled thread left the mutex inconsistent (should not happen
     * with cleanup handlers, but keep observability non-fatal). */
    if (pthread_mutex_trylock(&r->mu) != 0)
        return 0;
    d = r->count;
    pthread_mutex_unlock(&r->mu);
    return d;
}

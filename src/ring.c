#include "internal.h"

/*
 * Bounded atomic ring — SPSC/MPSC/SPMC/MPMC (Vyukov sequences).
 * Full/empty: spin budget then park on condvar (hybrid).
 * close/reopen: admission control without freeing storage.
 */

#define AWP_SPIN_BUDGET 64

void awp_ring_wake_all(awp_ring_t *r)
{
    if (!r)
        return;
    pthread_mutex_lock(&r->wait_mu);
    pthread_cond_broadcast(&r->wait_cv);
    pthread_mutex_unlock(&r->wait_mu);
}

void awp_ring_wait_space(awp_ring_t *r)
{
    pthread_mutex_lock(&r->wait_mu);
    while (!atomic_load_explicit(&r->closed, memory_order_acquire) &&
           awp_ring_depth(r) >= (uint32_t)r->capacity) {
        pthread_cond_wait(&r->wait_cv, &r->wait_mu);
    }
    pthread_mutex_unlock(&r->wait_mu);
}

void awp_ring_wait_data(awp_ring_t *r)
{
    pthread_mutex_lock(&r->wait_mu);
    while (!atomic_load_explicit(&r->closed, memory_order_acquire) &&
           awp_ring_depth(r) == 0) {
        pthread_cond_wait(&r->wait_cv, &r->wait_mu);
    }
    pthread_mutex_unlock(&r->wait_mu);
}

static int ring_alloc_cells(awp_ring_t *r, uint32_t capacity)
{
    size_t i;
    uint32_t cap = awp_round_up_pow2(capacity);
    void *mem = NULL;
    size_t bytes;

    if (cap == 0)
        return -EINVAL;
    bytes = (size_t)cap * sizeof(awp_cell_t);

    if (posix_memalign(&mem, AWP_CACHELINE, bytes) != 0 || !mem) {
        r->cells = calloc(cap, sizeof(awp_cell_t));
        if (!r->cells)
            return -ENOMEM;
    } else {
        r->cells = mem;
        memset(r->cells, 0, bytes);
    }

    r->capacity = cap;
    r->mask = cap - 1;
    atomic_store_explicit(&r->enqueue_pos, 0, memory_order_relaxed);
    atomic_store_explicit(&r->dequeue_pos, 0, memory_order_relaxed);
    atomic_store_explicit(&r->closed, 0, memory_order_relaxed);

    for (i = 0; i < cap; i++) {
        atomic_store_explicit(&r->cells[i].sequence, i, memory_order_relaxed);
        r->cells[i].data = NULL;
    }
    return 0;
}

int awp_ring_init(awp_ring_t *r, uint32_t capacity, awp_ring_mode_t mode)
{
    int rc;
    if (!r || capacity < 1)
        return -EINVAL;
    if ((unsigned)mode > (unsigned)AWP_RING_MPMC)
        return -EINVAL;

    memset(r, 0, sizeof(*r));
    r->mode = mode;
    if (pthread_mutex_init(&r->wait_mu, NULL) != 0)
        return -ENOMEM;
    if (pthread_cond_init(&r->wait_cv, NULL) != 0) {
        pthread_mutex_destroy(&r->wait_mu);
        return -ENOMEM;
    }
    rc = ring_alloc_cells(r, capacity);
    if (rc != 0) {
        pthread_cond_destroy(&r->wait_cv);
        pthread_mutex_destroy(&r->wait_mu);
    }
    return rc;
}

void awp_ring_destroy(awp_ring_t *r)
{
    if (!r)
        return;
    free(r->cells);
    r->cells = NULL;
    pthread_cond_destroy(&r->wait_cv);
    pthread_mutex_destroy(&r->wait_mu);
}

void awp_ring_close(awp_ring_t *r)
{
    if (!r)
        return;
    atomic_store_explicit(&r->closed, 1, memory_order_release);
    awp_ring_wake_all(r);
}

void awp_ring_reopen(awp_ring_t *r)
{
    if (!r)
        return;
    atomic_store_explicit(&r->closed, 0, memory_order_release);
    awp_ring_wake_all(r);
}

static int push_sp(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out)
{
    unsigned spin = 0;
    uint64_t t0 = 0;
    int did_block = 0;

    for (;;) {
        size_t pos;
        awp_cell_t *cell;
        size_t seq;
        intptr_t dif;

        if (atomic_load_explicit(&r->closed, memory_order_acquire)) {
            if (blocked_ns_out)
                *blocked_ns_out = did_block ? (awp_now_ns() - t0) : 0;
            return -1;
        }

        pos = atomic_load_explicit(&r->enqueue_pos, memory_order_relaxed);
        cell = &r->cells[pos & r->mask];
        seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        dif = (intptr_t)seq - (intptr_t)pos;

        if (dif == 0) {
            cell->data = frame;
            atomic_store_explicit(&cell->sequence, pos + 1, memory_order_release);
            atomic_store_explicit(&r->enqueue_pos, pos + 1, memory_order_relaxed);
            awp_ring_wake_all(r);
            if (blocked_ns_out)
                *blocked_ns_out = did_block ? (awp_now_ns() - t0) : 0;
            return 0;
        }
        if (dif < 0) {
            if (!did_block) {
                t0 = awp_now_ns();
                did_block = 1;
            }
            if (spin++ < AWP_SPIN_BUDGET)
                awp_cpu_relax();
            else {
                awp_ring_wait_space(r);
                spin = 0;
            }
            continue;
        }
        awp_cpu_relax();
    }
}

static int push_mp(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out)
{
    unsigned spin = 0;
    uint64_t t0 = 0;
    int did_block = 0;

    for (;;) {
        size_t pos;
        awp_cell_t *cell;
        size_t seq;
        intptr_t dif;

        if (atomic_load_explicit(&r->closed, memory_order_acquire)) {
            if (blocked_ns_out)
                *blocked_ns_out = did_block ? (awp_now_ns() - t0) : 0;
            return -1;
        }

        pos = atomic_load_explicit(&r->enqueue_pos, memory_order_relaxed);
        cell = &r->cells[pos & r->mask];
        seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        dif = (intptr_t)seq - (intptr_t)pos;

        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &r->enqueue_pos, &pos, pos + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                cell->data = frame;
                atomic_store_explicit(&cell->sequence, pos + 1,
                                      memory_order_release);
                awp_ring_wake_all(r);
                if (blocked_ns_out)
                    *blocked_ns_out = did_block ? (awp_now_ns() - t0) : 0;
                return 0;
            }
            continue;
        }
        if (dif < 0) {
            if (!did_block) {
                t0 = awp_now_ns();
                did_block = 1;
            }
            if (spin++ < AWP_SPIN_BUDGET)
                awp_cpu_relax();
            else {
                awp_ring_wait_space(r);
                spin = 0;
            }
            continue;
        }
        awp_cpu_relax();
    }
}

int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out)
{
    if (!r || !frame)
        return -EINVAL;
    switch (r->mode) {
    case AWP_RING_SPSC:
    case AWP_RING_SPMC:
        return push_sp(r, frame, blocked_ns_out);
    case AWP_RING_MPSC:
    case AWP_RING_MPMC:
        return push_mp(r, frame, blocked_ns_out);
    default:
        return -EINVAL;
    }
}

static int try_push_sp(awp_ring_t *r, awp_frame_t *frame)
{
    size_t pos;
    awp_cell_t *cell;
    size_t seq;
    intptr_t dif;

    if (atomic_load_explicit(&r->closed, memory_order_acquire))
        return -1;
    pos = atomic_load_explicit(&r->enqueue_pos, memory_order_relaxed);
    cell = &r->cells[pos & r->mask];
    seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
    dif = (intptr_t)seq - (intptr_t)pos;
    if (dif == 0) {
        cell->data = frame;
        atomic_store_explicit(&cell->sequence, pos + 1, memory_order_release);
        atomic_store_explicit(&r->enqueue_pos, pos + 1, memory_order_relaxed);
        awp_ring_wake_all(r);
        return 0;
    }
    if (dif < 0)
        return -EAGAIN;
    return -EAGAIN;
}

static int try_push_mp(awp_ring_t *r, awp_frame_t *frame)
{
    size_t pos;
    awp_cell_t *cell;
    size_t seq;
    intptr_t dif;

    if (atomic_load_explicit(&r->closed, memory_order_acquire))
        return -1;
    pos = atomic_load_explicit(&r->enqueue_pos, memory_order_relaxed);
    cell = &r->cells[pos & r->mask];
    seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
    dif = (intptr_t)seq - (intptr_t)pos;
    if (dif == 0) {
        if (atomic_compare_exchange_weak_explicit(
                &r->enqueue_pos, &pos, pos + 1,
                memory_order_acq_rel, memory_order_relaxed)) {
            cell->data = frame;
            atomic_store_explicit(&cell->sequence, pos + 1, memory_order_release);
            awp_ring_wake_all(r);
            return 0;
        }
        return -EAGAIN; /* lost race; caller may retry */
    }
    if (dif < 0)
        return -EAGAIN;
    return -EAGAIN;
}

int awp_ring_try_push(awp_ring_t *r, awp_frame_t *frame)
{
    if (!r || !frame)
        return -EINVAL;
    switch (r->mode) {
    case AWP_RING_SPSC:
    case AWP_RING_SPMC:
        return try_push_sp(r, frame);
    case AWP_RING_MPSC:
    case AWP_RING_MPMC:
        return try_push_mp(r, frame);
    default:
        return -EINVAL;
    }
}

static int pop_sc(awp_ring_t *r, awp_frame_t **out)
{
    unsigned spin = 0;

    for (;;) {
        size_t pos;
        awp_cell_t *cell;
        size_t seq;
        intptr_t dif;

        pos = atomic_load_explicit(&r->dequeue_pos, memory_order_relaxed);
        cell = &r->cells[pos & r->mask];
        seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        dif = (intptr_t)seq - (intptr_t)(pos + 1);

        if (dif == 0) {
            atomic_store_explicit(&r->dequeue_pos, pos + 1, memory_order_relaxed);
            *out = cell->data;
            cell->data = NULL;
            atomic_store_explicit(&cell->sequence, pos + r->capacity,
                                  memory_order_release);
            awp_ring_wake_all(r);
            return 0;
        }
        if (dif < 0) {
            if (atomic_load_explicit(&r->closed, memory_order_acquire)) {
                seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
                dif = (intptr_t)seq - (intptr_t)(pos + 1);
                if (dif == 0)
                    continue;
                return -1;
            }
            if (spin++ < AWP_SPIN_BUDGET)
                awp_cpu_relax();
            else {
                awp_ring_wait_data(r);
                spin = 0;
            }
            continue;
        }
        awp_cpu_relax();
    }
}

static int pop_mc(awp_ring_t *r, awp_frame_t **out)
{
    unsigned spin = 0;

    for (;;) {
        size_t pos;
        awp_cell_t *cell;
        size_t seq;
        intptr_t dif;

        pos = atomic_load_explicit(&r->dequeue_pos, memory_order_relaxed);
        cell = &r->cells[pos & r->mask];
        seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        dif = (intptr_t)seq - (intptr_t)(pos + 1);

        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &r->dequeue_pos, &pos, pos + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                *out = cell->data;
                cell->data = NULL;
                atomic_store_explicit(&cell->sequence, pos + r->capacity,
                                      memory_order_release);
                awp_ring_wake_all(r);
                return 0;
            }
            continue;
        }
        if (dif < 0) {
            if (atomic_load_explicit(&r->closed, memory_order_acquire)) {
                seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
                dif = (intptr_t)seq - (intptr_t)(pos + 1);
                if (dif == 0)
                    continue;
                return -1;
            }
            if (spin++ < AWP_SPIN_BUDGET)
                awp_cpu_relax();
            else {
                awp_ring_wait_data(r);
                spin = 0;
            }
            continue;
        }
        awp_cpu_relax();
    }
}

int awp_ring_pop(awp_ring_t *r, awp_frame_t **out)
{
    if (!r || !out)
        return -EINVAL;
    switch (r->mode) {
    case AWP_RING_SPSC:
    case AWP_RING_MPSC:
        return pop_sc(r, out);
    case AWP_RING_SPMC:
    case AWP_RING_MPMC:
        return pop_mc(r, out);
    default:
        return -EINVAL;
    }
}

uint32_t awp_ring_depth(awp_ring_t *r)
{
    size_t enq, deq;
    if (!r)
        return 0;
    enq = atomic_load_explicit(&r->enqueue_pos, memory_order_acquire);
    deq = atomic_load_explicit(&r->dequeue_pos, memory_order_acquire);
    if (enq < deq)
        return 0;
    {
        size_t d = enq - deq;
        if (d > r->capacity)
            d = r->capacity;
        return (uint32_t)d;
    }
}

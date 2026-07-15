#include "internal.h"

/*
 * Bounded MPSC ring — C11 atomics, sequence protocol (Dmitry Vyukov style).
 *
 * Why not mutex: Codex (and Go's runtime channel design) admit locks as a
 * throughput/latency trade-off. Hot path here is multi-reader dispatch;
 * producers CAS on enqueue_pos, single consumer advances dequeue_pos.
 *
 * Fences:
 *   - Producer: CAS enqueue_pos (acq_rel), store data, release store sequence=pos+1
 *   - Consumer: acquire load sequence, read data, release store sequence=pos+cap
 * Data is published before sequence becomes visible to the consumer.
 *
 * Full/empty: spin + backoff (pause/yield). Never drop; closed aborts waiters.
 * Capacity is rounded up to a power of two.
 */

int awp_ring_init(awp_ring_t *r, uint32_t capacity)
{
    size_t i;
    uint32_t cap;

    if (!r || capacity < 1)
        return -EINVAL;

    memset(r, 0, sizeof(*r));
    cap = awp_round_up_pow2(capacity);
    {
        void *mem = NULL;
        if (posix_memalign(&mem, AWP_CACHELINE, cap * sizeof(awp_cell_t)) != 0 ||
            !mem) {
            r->cells = calloc(cap, sizeof(awp_cell_t));
            if (!r->cells)
                return -ENOMEM;
        } else {
            r->cells = mem;
            memset(r->cells, 0, cap * sizeof(awp_cell_t));
        }
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

void awp_ring_destroy(awp_ring_t *r)
{
    if (!r)
        return;
    free(r->cells);
    r->cells = NULL;
}

void awp_ring_close(awp_ring_t *r)
{
    if (!r)
        return;
    atomic_store_explicit(&r->closed, 1, memory_order_release);
}

int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out)
{
    unsigned spin = 0;
    uint64_t t0 = 0;
    int did_block = 0;

    if (!r || !frame)
        return -EINVAL;

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
                if (blocked_ns_out)
                    *blocked_ns_out = did_block ? (awp_now_ns() - t0) : 0;
                return 0;
            }
            /* CAS lost — retry without backoff. */
            continue;
        }

        if (dif < 0) {
            /* Full: sequence not yet advanced by consumer. */
            if (!did_block) {
                t0 = awp_now_ns();
                did_block = 1;
            }
            awp_backoff(spin++);
            continue;
        }

        /* Another producer is ahead; reload enqueue_pos. */
        awp_cpu_relax();
    }
}

int awp_ring_pop(awp_ring_t *r, awp_frame_t **out)
{
    unsigned spin = 0;

    if (!r || !out)
        return -EINVAL;

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
            /* Single consumer: no CAS needed on dequeue_pos. */
            atomic_store_explicit(&r->dequeue_pos, pos + 1, memory_order_relaxed);
            *out = cell->data;
            cell->data = NULL;
            atomic_store_explicit(&cell->sequence, pos + r->capacity,
                                  memory_order_release);
            return 0;
        }

        if (dif < 0) {
            /* Empty. */
            if (atomic_load_explicit(&r->closed, memory_order_acquire)) {
                /* Re-check: a producer may have enqueued before close. */
                seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
                dif = (intptr_t)seq - (intptr_t)(pos + 1);
                if (dif == 0)
                    continue;
                return -1;
            }
            awp_backoff(spin++);
            continue;
        }

        awp_cpu_relax();
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

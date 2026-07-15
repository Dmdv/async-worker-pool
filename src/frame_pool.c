#include "internal.h"

/*
 * Lock-free freelist of slab indices with ABA tags (Treiber-style).
 * head packs (tag << 32) | idx; idx == 0xFFFFFFFFu means empty.
 */

#define AWP_POOL_EMPTY 0xFFFFFFFFu

static inline uint64_t pack_head(uint32_t idx, uint32_t tag)
{
    return ((uint64_t)tag << 32) | (uint64_t)idx;
}

static inline void unpack_head(uint64_t h, uint32_t *idx, uint32_t *tag)
{
    *idx = (uint32_t)(h & 0xFFFFFFFFu);
    *tag = (uint32_t)(h >> 32);
}

int awp_frame_pool_init(awp_frame_pool_t *p, uint32_t size)
{
    uint32_t i;
    if (!p || size < 1)
        return -EINVAL;
    memset(p, 0, sizeof(*p));
    p->slab = calloc(size, sizeof(awp_frame_t));
    p->next = calloc(size, sizeof(atomic_uint));
    if (!p->slab || !p->next) {
        free(p->slab);
        free(p->next);
        return -ENOMEM;
    }
    p->size = size;
    /* Chain 0 -> 1 -> ... -> size-1 -> EMPTY */
    for (i = 0; i + 1 < size; i++)
        atomic_store_explicit(&p->next[i], i + 1, memory_order_relaxed);
    atomic_store_explicit(&p->next[size - 1], AWP_POOL_EMPTY, memory_order_relaxed);
    atomic_store_explicit(&p->head, pack_head(0, 0), memory_order_relaxed);
    atomic_store_explicit(&p->closed, 0, memory_order_relaxed);
    return 0;
}

void awp_frame_pool_destroy(awp_frame_pool_t *p)
{
    if (!p)
        return;
    free(p->next);
    free(p->slab);
    memset(p, 0, sizeof(*p));
}

void awp_frame_pool_close(awp_frame_pool_t *p)
{
    if (!p)
        return;
    atomic_store_explicit(&p->closed, 1, memory_order_release);
}

awp_frame_t *awp_frame_pool_acquire(awp_frame_pool_t *p)
{
    unsigned spin = 0;
    if (!p)
        return NULL;

    for (;;) {
        uint64_t head;
        uint32_t idx, tag, nidx;
        uint64_t neu;

        if (atomic_load_explicit(&p->closed, memory_order_acquire)) {
            /* One last try then fail. */
        }

        head = atomic_load_explicit(&p->head, memory_order_acquire);
        unpack_head(head, &idx, &tag);
        if (idx == AWP_POOL_EMPTY) {
            if (atomic_load_explicit(&p->closed, memory_order_acquire))
                return NULL;
            awp_backoff(spin++);
            continue;
        }
        nidx = atomic_load_explicit(&p->next[idx], memory_order_relaxed);
        neu = pack_head(nidx, tag + 1);
        if (atomic_compare_exchange_weak_explicit(
                &p->head, &head, neu,
                memory_order_acq_rel, memory_order_relaxed)) {
            awp_frame_t *f = &p->slab[idx];
            memset(f, 0, sizeof(*f));
            return f;
        }
        awp_cpu_relax();
    }
}

void awp_frame_pool_release(awp_frame_pool_t *p, awp_frame_t *f)
{
    uint32_t idx;
    unsigned spin = 0;

    if (!p || !f)
        return;
    if (f < p->slab || f >= p->slab + p->size)
        return;
    idx = (uint32_t)(f - p->slab);

    for (;;) {
        uint64_t head;
        uint32_t hidx, tag;
        uint64_t neu;

        head = atomic_load_explicit(&p->head, memory_order_acquire);
        unpack_head(head, &hidx, &tag);
        atomic_store_explicit(&p->next[idx], hidx, memory_order_relaxed);
        neu = pack_head(idx, tag + 1);
        if (atomic_compare_exchange_weak_explicit(
                &p->head, &head, neu,
                memory_order_acq_rel, memory_order_relaxed))
            return;
        awp_backoff(spin++);
    }
}

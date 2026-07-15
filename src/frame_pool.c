#include "internal.h"

#define AWP_POOL_EMPTY 0xFFFFFFFFu
#define AWP_SPIN_BUDGET 64

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
    for (i = 0; i + 1 < size; i++)
        atomic_store_explicit(&p->next[i], i + 1, memory_order_relaxed);
    atomic_store_explicit(&p->next[size - 1], AWP_POOL_EMPTY, memory_order_relaxed);
    atomic_store_explicit(&p->head, pack_head(0, 0), memory_order_relaxed);
    atomic_store_explicit(&p->closed, 0, memory_order_relaxed);
    atomic_store_explicit(&p->lock_free_ok,
                          atomic_is_lock_free(&p->head) ? 1 : 0,
                          memory_order_relaxed);
    if (pthread_mutex_init(&p->wait_mu, NULL) != 0)
        goto fail;
    if (pthread_cond_init(&p->wait_cv, NULL) != 0) {
        pthread_mutex_destroy(&p->wait_mu);
        goto fail;
    }
    return 0;
fail:
    free(p->next);
    free(p->slab);
    return -ENOMEM;
}

void awp_frame_pool_destroy(awp_frame_pool_t *p)
{
    if (!p)
        return;
    pthread_cond_destroy(&p->wait_cv);
    pthread_mutex_destroy(&p->wait_mu);
    free(p->next);
    free(p->slab);
    memset(p, 0, sizeof(*p));
}

void awp_frame_pool_close(awp_frame_pool_t *p)
{
    if (!p)
        return;
    atomic_store_explicit(&p->closed, 1, memory_order_release);
    pthread_mutex_lock(&p->wait_mu);
    pthread_cond_broadcast(&p->wait_cv);
    pthread_mutex_unlock(&p->wait_mu);
}

void awp_frame_pool_reopen(awp_frame_pool_t *p)
{
    if (!p)
        return;
    atomic_store_explicit(&p->closed, 0, memory_order_release);
    pthread_mutex_lock(&p->wait_mu);
    pthread_cond_broadcast(&p->wait_cv);
    pthread_mutex_unlock(&p->wait_mu);
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

        head = atomic_load_explicit(&p->head, memory_order_acquire);
        unpack_head(head, &idx, &tag);
        if (idx == AWP_POOL_EMPTY) {
            if (atomic_load_explicit(&p->closed, memory_order_acquire))
                return NULL;
            if (spin++ < AWP_SPIN_BUDGET) {
                awp_cpu_relax();
            } else {
                pthread_mutex_lock(&p->wait_mu);
                while (!atomic_load_explicit(&p->closed, memory_order_acquire)) {
                    uint64_t h2 = atomic_load_explicit(&p->head, memory_order_acquire);
                    uint32_t i2, t2;
                    unpack_head(h2, &i2, &t2);
                    if (i2 != AWP_POOL_EMPTY)
                        break;
                    pthread_cond_wait(&p->wait_cv, &p->wait_mu);
                }
                pthread_mutex_unlock(&p->wait_mu);
                spin = 0;
            }
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
        /* Detect obvious double-free of current head. */
        if (hidx == idx) {
            fprintf(stderr, "[awp] frame_pool double-release idx=%u ignored\n", idx);
            return;
        }
        atomic_store_explicit(&p->next[idx], hidx, memory_order_relaxed);
        neu = pack_head(idx, tag + 1);
        if (atomic_compare_exchange_weak_explicit(
                &p->head, &head, neu,
                memory_order_acq_rel, memory_order_relaxed)) {
            pthread_mutex_lock(&p->wait_mu);
            pthread_cond_signal(&p->wait_cv);
            pthread_mutex_unlock(&p->wait_mu);
            return;
        }
        if (spin++ > AWP_SPIN_BUDGET)
            sched_yield();
        else
            awp_cpu_relax();
    }
}

#ifndef AWP_INTERNAL_H
#define AWP_INTERNAL_H

#include "awp/awp.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <sched.h>

#ifndef AWP_CACHELINE
#define AWP_CACHELINE 64
#endif

#define AWP_ALIGN_CACHE alignas(AWP_CACHELINE)

/* ---- time / backoff ----------------------------------------------------- */

static inline uint64_t awp_now_ns(void)
{
    struct timespec ts;
#if defined(CLOCK_MONOTONIC)
    clock_gettime(CLOCK_MONOTONIC, &ts);
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline void awp_cpu_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield" ::: "memory");
#else
    /* no-op */
#endif
}

/** Spin a bit, then yield — cancel point for force-shutdown. */
static inline void awp_backoff(unsigned spin)
{
    if (spin < 64) {
        unsigned i;
        for (i = 0; i < (1u << (spin < 6 ? spin : 6)); i++)
            awp_cpu_relax();
    } else {
        pthread_testcancel();
        sched_yield();
    }
}

static inline uint32_t awp_round_up_pow2(uint32_t v)
{
    if (v < 2)
        return 2;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

/* ---- bounded MPSC ring (atomics, Vyukov-style sequences) ---------------- */

/**
 * Cache-line padded cell: sequence + data pointer.
 * Producers CAS enqueue_pos; single consumer advances dequeue_pos.
 * Memory orders: acquire on sequence load, release on sequence store after data.
 */
typedef struct awp_cell {
    AWP_ALIGN_CACHE atomic_size_t sequence;
    awp_frame_t *data;
} awp_cell_t;

typedef struct awp_ring {
    AWP_ALIGN_CACHE atomic_size_t enqueue_pos;
    AWP_ALIGN_CACHE atomic_size_t dequeue_pos;
    awp_cell_t *cells;
    size_t capacity; /* power of two */
    size_t mask;
    atomic_int closed;
} awp_ring_t;

int  awp_ring_init(awp_ring_t *r, uint32_t capacity);
void awp_ring_destroy(awp_ring_t *r);
void awp_ring_close(awp_ring_t *r);

/**
 * Enqueue pointer. Spins while full and not closed (backpressure, never drop).
 * @return 0 ok, -1 closed/shutdown
 */
int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out);

/**
 * Dequeue. Spins while empty and not closed.
 * @return 0 ok, -1 closed and empty
 */
int awp_ring_pop(awp_ring_t *r, awp_frame_t **out);

/** Approximate depth (enqueue - dequeue); lock-free. */
uint32_t awp_ring_depth(awp_ring_t *r);

/* ---- frame object pool (lock-free Treiber stack of indices) ------------- */

typedef struct awp_frame_pool {
    awp_frame_t *slab;
    atomic_uint *next;           /* next free index chain */
    atomic_uint_fast64_t head;   /* packed: tag<<32 | idx  (idx==0xFFFFFFFF empty) */
    uint32_t size;
    atomic_int closed;
} awp_frame_pool_t;

int  awp_frame_pool_init(awp_frame_pool_t *p, uint32_t size);
void awp_frame_pool_destroy(awp_frame_pool_t *p);
void awp_frame_pool_close(awp_frame_pool_t *p);

/** Acquire a zeroed frame (spins if exhausted until recycle or close). */
awp_frame_t *awp_frame_pool_acquire(awp_frame_pool_t *p);
void awp_frame_pool_release(awp_frame_pool_t *p, awp_frame_t *f);

/* ---- worker / pool ------------------------------------------------------ */

typedef struct awp_worker {
    uint32_t id;
    pthread_t thread;
    awp_ring_t queue;
    awp_pool_t *pool;

    atomic_uint_fast64_t processed;
    atomic_uint_fast64_t process_errors;
    atomic_uint_fast64_t enqueue_blocks;
    atomic_uint_fast64_t blocked_ns;
    atomic_uint_fast64_t queue_hwm;
    atomic_uint_fast64_t last_progress_ns;
    atomic_uint_fast64_t restarts;
    atomic_int alive;
    atomic_int stop;
} awp_worker_t;

struct awp_pool {
    awp_config_t cfg;
    awp_worker_t *workers;
    awp_frame_pool_t frames;

    atomic_uint_fast64_t submitted;
    atomic_uint_fast64_t dropped;
    atomic_uint_fast64_t process_errors;
    atomic_uint_fast64_t shutdown_aborts;
    atomic_uint_fast64_t seq;

    atomic_int running;
    atomic_int shutting_down;

    pthread_t supervisor;
    atomic_int supervisor_alive;

    awp_worker_metrics_t *metrics_buf;

    char **broadcast_feeds;
    uint32_t n_broadcast_feeds;
    uint32_t shard_base;
    uint32_t n_shard_workers;
};

void *awp_worker_main(void *arg);
void *awp_supervisor_main(void *arg);
int   awp_worker_start(awp_worker_t *w);
int   awp_worker_join(awp_worker_t *w, uint32_t deadline_ms, int *aborted);

uint32_t awp_compute_shard(const awp_pool_t *pool,
                           const char *feed,
                           const char *symbol,
                           uint32_t flags);

#endif /* AWP_INTERNAL_H */

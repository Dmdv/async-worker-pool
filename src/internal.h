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

#ifndef AWP_CACHELINE
#define AWP_CACHELINE 64
#endif

#define AWP_ALIGN_CACHE alignas(AWP_CACHELINE)

/* ---- time helpers ------------------------------------------------------- */

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

static inline void awp_ms_to_timespec(uint32_t ms, struct timespec *ts)
{
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec += ms / 1000u;
    ts->tv_nsec += (long)(ms % 1000u) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

/* ---- bounded MPSC ring (mutex + condvar) -------------------------------- */

typedef struct awp_ring {
    awp_frame_t **slots;
    uint32_t capacity;
    uint32_t head; /* dequeue index */
    uint32_t tail; /* enqueue index */
    uint32_t count;
    pthread_mutex_t mu;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
    atomic_int closed; /* 1 after shutdown signal */
} awp_ring_t;

int  awp_ring_init(awp_ring_t *r, uint32_t capacity);
void awp_ring_destroy(awp_ring_t *r);
void awp_ring_close(awp_ring_t *r);

/**
 * Enqueue pointer. Blocks while full and not closed.
 * @return 0 ok, -1 closed/shutdown, -2 interrupted
 */
int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out);

/**
 * Dequeue. Blocks while empty and not closed.
 * @return 0 ok, -1 closed and empty
 */
int awp_ring_pop(awp_ring_t *r, awp_frame_t **out);

uint32_t awp_ring_depth(awp_ring_t *r);

/* ---- frame object pool -------------------------------------------------- */

typedef struct awp_frame_pool {
    awp_frame_t *slab;       /* contiguous array */
    awp_frame_t **free_list; /* stack of free pointers */
    uint32_t size;
    uint32_t free_count;
    pthread_mutex_t mu;
    pthread_cond_t  has_free;
    atomic_int closed;
} awp_frame_pool_t;

int  awp_frame_pool_init(awp_frame_pool_t *p, uint32_t size);
void awp_frame_pool_destroy(awp_frame_pool_t *p);
void awp_frame_pool_close(awp_frame_pool_t *p);

/** Acquire a zeroed frame (blocks if exhausted until recycle or close). */
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
    atomic_int stop; /* cooperative stop */
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

    /* metrics snapshot buffer */
    awp_worker_metrics_t *metrics_buf;
    pthread_mutex_t metrics_mu;

    /* broadcast feed table (copied strings) */
    char **broadcast_feeds;
    uint32_t n_broadcast_feeds;
    uint32_t shard_base; /* first worker index for symbol traffic */
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

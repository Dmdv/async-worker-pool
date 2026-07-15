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
#include <limits.h>

#ifndef AWP_CACHELINE
#define AWP_CACHELINE 64
#endif

#define AWP_ALIGN_CACHE alignas(AWP_CACHELINE)

/** Max ring capacity (power-of-two, leaves headroom in size_t positions). */
#define AWP_RING_CAP_MAX (1u << 24)

/* ---- lifecycle / worker states ------------------------------------------ */

typedef enum awp_lifecycle {
    AWP_LIFE_INIT = 0,
    AWP_LIFE_RUNNING,
    AWP_LIFE_QUIESCING, /* reject new submits; wait active_submits */
    AWP_LIFE_DRAINING,  /* rings closed; workers empty queues */
    AWP_LIFE_STOPPED
} awp_lifecycle_t;

typedef enum awp_wstate {
    AWP_W_STARTING = 0,
    AWP_W_RUNNING,
    AWP_W_EXITED,
    AWP_W_JOINED,
    AWP_W_QUARANTINED /* stuck in callback; must not free pool memory */
} awp_wstate_t;

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
#endif
}

static inline uint32_t awp_round_up_pow2(uint32_t v)
{
    if (v < 2)
        return 2;
    if (v > AWP_RING_CAP_MAX)
        return 0; /* overflow / too large */
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    if (v == 0 || v > AWP_RING_CAP_MAX)
        return 0;
    return v;
}

/* ---- bounded ring: SPSC | MPSC | SPMC | MPMC ---------------------------- */

typedef struct awp_cell {
    AWP_ALIGN_CACHE atomic_size_t sequence;
    awp_frame_t *data;
} awp_cell_t;

typedef struct awp_ring {
    AWP_ALIGN_CACHE atomic_size_t enqueue_pos;
    AWP_ALIGN_CACHE atomic_size_t dequeue_pos;
    awp_cell_t *cells;
    size_t capacity;
    size_t mask;
    awp_ring_mode_t mode;
    atomic_int closed;
    /* Hybrid park (not on data path CAS): after spin budget, wait here. */
    pthread_mutex_t wait_mu;
    pthread_cond_t wait_cv;
} awp_ring_t;

int  awp_ring_init(awp_ring_t *r, uint32_t capacity, awp_ring_mode_t mode);
void awp_ring_destroy(awp_ring_t *r);
void awp_ring_close(awp_ring_t *r);
/** Clear closed flag; keep queued cells (for worker restart without loss). */
void awp_ring_reopen(awp_ring_t *r);
void awp_ring_wait_space(awp_ring_t *r);
void awp_ring_wait_data(awp_ring_t *r);
void awp_ring_wake_all(awp_ring_t *r);

int awp_ring_push(awp_ring_t *r, awp_frame_t *frame, uint64_t *blocked_ns_out);
/** Non-blocking push: 0 ok, -1 closed/invalid, -EAGAIN full. */
int awp_ring_try_push(awp_ring_t *r, awp_frame_t *frame);
int awp_ring_pop(awp_ring_t *r, awp_frame_t **out);
uint32_t awp_ring_depth(awp_ring_t *r);

/* ---- frame object pool -------------------------------------------------- */

typedef struct awp_frame_pool {
    awp_frame_t *slab;
    atomic_uint *next;
    atomic_uint_fast64_t head;
    uint32_t size;
    atomic_int closed;
    atomic_int lock_free_ok; /* 1 if 64-bit head is lock-free */
    pthread_mutex_t wait_mu;
    pthread_cond_t wait_cv;
} awp_frame_pool_t;

int  awp_frame_pool_init(awp_frame_pool_t *p, uint32_t size);
void awp_frame_pool_destroy(awp_frame_pool_t *p);
void awp_frame_pool_close(awp_frame_pool_t *p);
void awp_frame_pool_reopen(awp_frame_pool_t *p);

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
    atomic_uint_fast64_t generation;

    atomic_int state; /* awp_wstate_t */
    atomic_int stop;  /* cooperative stop between frames (not mid-callback) */
    atomic_int joined;
} awp_worker_t;

struct awp_pool {
    awp_config_t cfg;
    awp_worker_t *workers;
    awp_frame_pool_t frames;

    atomic_uint_fast64_t submitted;
    atomic_uint_fast64_t dropped; /* abandonment / rejection (not full-queue) */
    atomic_uint_fast64_t process_errors;
    atomic_uint_fast64_t shutdown_aborts;
    atomic_uint_fast64_t seq;
    atomic_uint_fast64_t abandoned; /* frames recycled without process */

    atomic_int lifecycle; /* awp_lifecycle_t */
    atomic_int active_submits;
    atomic_int api_refs; /* live public API calls (submit/metrics/...) */
    atomic_int shutdown_waiters; /* concurrent shutdown callers waiting STOPPED */
    atomic_int supervisor_stop;
    atomic_int supervisor_alive;
    atomic_int supervisor_joined; /* 1 if no supervisor or join completed */
    atomic_int supervisor_started; /* 1 after pthread_create of supervisor succeeds */
    atomic_int quarantined; /* 1 ⇒ destroy must leak; submit rejects */
    atomic_int destroy_started; /* single destroy owner (0→1) */

    pthread_t supervisor;
    pthread_mutex_t life_mu;
    pthread_cond_t life_cv;

    pthread_mutex_t metrics_mu;
    awp_worker_metrics_t *metrics_buf;

    char **broadcast_feeds;
    uint32_t n_broadcast_feeds;
    uint32_t shard_base;
    uint32_t n_shard_workers;
};

/* TLS: reject nested submit/shutdown/destroy from process()/on_error(). */
extern _Thread_local int awp_tls_in_callback;

void *awp_worker_main(void *arg);
void *awp_supervisor_main(void *arg);
int   awp_worker_start(awp_worker_t *w);
/** Join with absolute deadline_ns (CLOCK_MONOTONIC). */
int   awp_worker_join_deadline(awp_worker_t *w, uint64_t deadline_ns, int *aborted);

/** Sticky leak flag: pool storage must not be reclaimed; wake parked submitters. */
static inline void awp_pool_mark_quarantined(awp_pool_t *pool)
{
    if (!pool)
        return;
    atomic_store(&pool->quarantined, 1);
    /* Unblock frame-pool waiters so they can observe quarantine and exit. */
    awp_frame_pool_close(&pool->frames);
}

/** Count a public API entry (must pair with leave). */
static inline void awp_api_enter(awp_pool_t *pool)
{
    if (pool)
        atomic_fetch_add_explicit(&pool->api_refs, 1, memory_order_acq_rel);
}

static inline void awp_api_leave(awp_pool_t *pool)
{
    if (pool)
        atomic_fetch_sub_explicit(&pool->api_refs, 1, memory_order_acq_rel);
}

uint32_t awp_compute_shard(const awp_pool_t *pool,
                           const char *feed,
                           const char *symbol,
                           uint32_t flags);

#endif /* AWP_INTERNAL_H */

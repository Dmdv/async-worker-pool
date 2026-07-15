/**
 * @file awp.h
 * @brief Async Worker Pool — sharded low-latency dispatch pool in C.
 *
 * Preallocated pthread workers, bounded per-worker queues, stable hash
 * sharding for per-(feed,symbol) FIFO, backpressure without drops,
 * fault-isolated process callbacks, supervisor heartbeats, bounded shutdown.
 *
 * Runtime helper: awp_runtime_enabled() reads AWP_ENABLED=1|true|yes.
 * Creation is not gated by the env var — callers decide whether to create.
 */

#ifndef AWP_AWP_H
#define AWP_AWP_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Library version (semver). */
#define AWP_VERSION_MAJOR 0
#define AWP_VERSION_MINOR 1
#define AWP_VERSION_PATCH 0

/** Max feed label length (excluding NUL). */
#define AWP_FEED_MAX 64
/** Max symbol length (excluding NUL). */
#define AWP_SYMBOL_MAX 64
/** Max payload bytes per frame (fixed-size for object pool). */
#define AWP_PAYLOAD_MAX 4096

/** Opaque pool handle. */
typedef struct awp_pool awp_pool_t;

/**
 * Frame handed to process() and recycled by the pool after the call returns.
 * Callers must not free the frame; do not retain pointers past process().
 */
typedef struct awp_frame {
    char     feed[AWP_FEED_MAX + 1];
    char     symbol[AWP_SYMBOL_MAX + 1];
    uint8_t  payload[AWP_PAYLOAD_MAX];
    size_t   payload_len;
    uint64_t seq;           /**< Monotonic per-submit sequence (for tests). */
    uint64_t submit_ns;     /**< CLOCK_MONOTONIC ns at submit (latency). */
    uint32_t shard;         /**< Worker index chosen at submit. */
    uint32_t flags;         /**< AWP_FRAME_* bits. */
} awp_frame_t;

/** Frame is a broadcast / dedicated-slot feed. */
#define AWP_FRAME_BROADCAST  (1u << 0)

/**
 * Process callback. Return 0 on success, non-zero on soft error.
 * Soft errors must not abort the worker: the pool counts and recycles.
 * Must not call awp_submit / awp_pool_shutdown / awp_pool_destroy (returns
 * -EDEADLK for submit/shutdown; destroy marks quarantine and does not free).
 */
typedef int (*awp_process_fn)(const awp_frame_t *frame, void *user);

/**
 * Called only when process() returns nonzero (soft error).
 * Frame is still recycled by the pool after this returns.
 * Same reentrancy restrictions as process().
 */
typedef void (*awp_on_error_fn)(const awp_frame_t *frame, int err, void *user);

/** Per-worker runtime metrics (snapshot). */
typedef struct awp_worker_metrics {
    uint64_t processed;
    uint64_t process_errors;
    uint64_t enqueue_blocks;   /**< Times producer blocked on this queue. */
    uint64_t blocked_ns;       /**< Cumulative producer block time. */
    uint64_t queue_depth;      /**< Current depth. */
    uint64_t queue_hwm;        /**< High-water mark. */
    uint64_t restarts;         /**< Supervisor restarts of this worker. */
    uint64_t last_progress_ns; /**< Last successful process or dequeue. */
    int      alive;            /**< 1 if thread running. */
} awp_worker_metrics_t;

/** Aggregate pool metrics. */
typedef struct awp_pool_metrics {
    uint64_t submitted;
    uint64_t dropped;          /**< Rejected/abandoned (not full-queue backpressure). */
    uint64_t process_errors;
    uint64_t shutdown_aborts;  /**< Workers quarantined / late at shutdown. */
    uint32_t n_workers;
    awp_worker_metrics_t *workers; /**< Length n_workers; valid until next get_metrics/destroy. */
} awp_pool_metrics_t;

/**
 * Per-worker ring concurrency model (C11 atomics sequence protocol; wait
 * paths may park on a condvar after a spin budget).
 * Pick the mode that matches actual producers/consumers — wrong mode is UB.
 * The pool uses one consumer thread per ring; SPMC/MPMC still run as a
 * single-consumer subset at the pool layer (multi-consumer is for raw rings).
 *
 * | Mode  | Producers | Consumers | Typical use                          |
 * |-------|-----------|-----------|--------------------------------------|
 * | SPSC  | 1         | 1         | One venue reader → one worker        |
 * | MPSC  | many      | 1         | N readers → one worker (default)     |
 * | SPMC  | 1         | many      | One feeder → multiple consumers      |
 * | MPMC  | many      | many      | Fully shared queue                   |
 */
typedef enum awp_ring_mode {
    AWP_RING_SPSC = 0,
    AWP_RING_MPSC = 1,
    AWP_RING_SPMC = 2,
    AWP_RING_MPMC = 3
} awp_ring_mode_t;

/** Pool configuration. Zero then set required fields; use awp_config_init(). */
typedef struct awp_config {
    uint32_t n_workers;           /**< Shard count; e.g. 32. Min 1. */
    uint32_t queue_capacity;      /**< Per-worker ring slots. Rounded up to power of two. */
    uint32_t frame_pool_size;     /**< Total preallocated frames (all workers). */
    awp_ring_mode_t ring_mode;    /**< Queue concurrency; default MPSC. */
    uint32_t shutdown_deadline_ms;/**< Hard shutdown deadline (default 10000). */
    uint32_t supervisor_interval_ms; /**< Heartbeat check period (default 500). */
    uint32_t stall_threshold_ms;  /**< No progress → consider stalled (default 5000). */
    int      enable_supervisor;   /**< 1 = run supervisor thread (default 1). */
    int      enable_restart;      /**< 1 = restart stalled workers (default 1). */
    awp_process_fn process;       /**< Required. */
    awp_on_error_fn on_error;     /**< Optional. */
    void    *user;                /**< Passed to process/on_error. */

    /**
     * Dedicated broadcast worker indices count. If > 0, feeds listed in
     * broadcast_feeds[] map to workers [0 .. n_broadcast_workers).
     * Symbol-sharded traffic uses workers [n_broadcast_workers .. n_workers).
     */
    uint32_t n_broadcast_workers;
    const char **broadcast_feeds; /**< NULL-terminated list, or NULL. */
} awp_config_t;

/** Initialize config with safe defaults. Caller still sets process + sizes. */
void awp_config_init(awp_config_t *cfg);

/**
 * Create and start the pool (workers + optional supervisor).
 * @return 0 on success, negative errno-style code on failure.
 */
int awp_pool_create(const awp_config_t *cfg, awp_pool_t **out);

/**
 * Submit a frame by value (copied into a pooled slot). Blocks if the target
 * worker queue is full (backpressure). Never drops on full queue.
 *
 * Ordering: FIFO is ring-reservation linearization order per shard (the CAS
 * or store that claims the enqueue slot), not wall-clock submit start.
 *
 * @param feed, symbol  labels (must fit AWP_*_MAX; oversize → -E2BIG)
 * @param payload       NULL only if payload_len == 0
 * @param payload_len   must be ≤ AWP_PAYLOAD_MAX
 * @param flags         AWP_FRAME_* or 0
 * @return 0 ok; -EINVAL not running/bad args; -E2BIG oversize;
 *         -EDEADLK from process()/on_error() callback; -1 closed/rejected.
 */
int awp_submit(awp_pool_t *pool,
               const char *feed,
               const char *symbol,
               const void *payload,
               size_t payload_len,
               uint32_t flags);

/**
 * Stable FNV-1a shard for (feed, symbol) under the pool's worker map.
 * Exposed for tests.
 */
uint32_t awp_shard_of(const awp_pool_t *pool,
                      const char *feed,
                      const char *symbol,
                      uint32_t flags);

/** Snapshot metrics (workers array valid until next snapshot/destroy). */
int awp_pool_get_metrics(awp_pool_t *pool, awp_pool_metrics_t *out);

/** Total drops counter (must remain 0 under backpressure stress). */
uint64_t awp_pool_drops(const awp_pool_t *pool);

/**
 * Bounded shutdown: quiesce submits, drain workers up to deadline.
 * Workers stuck in process() are quarantined (no cancel/detach). If live
 * references remain after the deadline, destroy will intentionally leak.
 * @return 0 if all joined cleanly, >0 if some were quarantined/late, <0 on error.
 */
int awp_pool_shutdown(awp_pool_t *pool);

/**
 * Destroy after shutdown (or call shutdown then destroy).
 * If any worker/supervisor/submitter may still reference pool memory,
 * storage is leaked intentionally to avoid UAF.
 * Not safe to call from process()/on_error() (marks quarantine, no free).
 */
void awp_pool_destroy(awp_pool_t *pool);

/** Runtime gate: returns 1 if AWP_ENABLED env is "1"/"true"/"yes". */
int awp_runtime_enabled(void);

/** FNV-1a 64-bit over feed || 0x1F || symbol (stable, for tests/tools). */
uint64_t awp_hash_key(const char *feed, const char *symbol);

#ifdef __cplusplus
}
#endif

#endif /* AWP_AWP_H */

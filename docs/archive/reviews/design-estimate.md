# Design estimate: sharded low-latency async worker pool in C

## Executive recommendation

Implement the first production version as:

- `N` permanent general-purpose `pthread` workers, plus optional dedicated workers.
- One bounded mutex/condition-variable MPSC ring per worker.
- Producer-side 64-bit FNV-1a sharding.
- Frames copied into fixed-size, preallocated ring slots.
- Callback execution outside queue locks.
- Pure condition-variable parking initially; add bounded spinning only after measurement.
- One lightweight supervisor thread for heartbeat inspection and clean-worker restart.
- Cooperative shutdown followed by process-level abort if a literal hard deadline is required.
- No live resizing, router thread, per-message allocation, asynchronous thread cancellation, or lock-free MPSC in v1.

At 1–5k messages/s, queue-lock cost should be far below the 5 ms budget. Scheduler latency, hot-shard serialization, and downstream publisher behavior are the real risks.

Three requirements need precise qualification:

1. **Zero loss:** the pool can guarantee that it never overwrites or silently drops a frame accepted by `awp_submit()`. Recycling after `process()` returns an error does not guarantee successful downstream delivery.
2. **Bounded latency:** blocking backpressure and an unconditional latency ceiling are mathematically incompatible during sustained overload. The 5 ms target applies only inside a declared arrival-rate, skew, burst, and callback-service envelope.
3. **Forced shutdown:** portable pthreads cannot safely kill arbitrary callback code. A true ≤10-second bound requires cooperative callback cancellation or process-level termination.


---

## A. Recommended module breakdown

### Files

```text
include/
  awp/
    awp.h                  Public API, configuration, status codes, metrics

src/
  awp_internal.h           Private pool, worker, ring, frame-slot types
  awp_pool.c               Creation, admission state, shutdown, destruction
  awp_queue.c              Bounded MPSC ring and condition-variable protocol
  awp_frame.c              Frame validation, slot layout, exact-byte copying
  awp_hash.c               FNV-1a and dedicated-worker mapping
  awp_worker.c             Worker loop and callback error handling
  awp_supervisor.c         Heartbeats, exit detection, safe restart
  awp_metrics.c            Atomic counters and snapshots
  awp_platform.c           Monotonic clock and portable exit-wait helpers

tests/
  unit/
  stress/
  integration/
  fault/

bench/
  dispatch_bench.c
  publisher_bench.c
```

One public header is preferable initially. Splitting the API into many headers would add packaging and ABI complexity without meaningful isolation.

### Public data model

```c
typedef struct awp_pool  awp_pool_t;
typedef struct awp_frame awp_frame_t;

typedef struct {
    const void *ptr;
    size_t len;
} awp_slice_t;

typedef struct {
    awp_slice_t feed;
    awp_slice_t symbol;
    awp_slice_t payload;

    /* Set before submission so blocking time is included in latency. */
    uint64_t ingest_time_ns;
} awp_frame_view_t;

typedef int (*awp_process_fn)(
    void *context,
    const awp_frame_t *frame);

typedef void (*awp_error_fn)(
    void *context,
    uint32_t worker_id,
    const awp_frame_t *frame,
    int process_error);
```

`awp_frame_t` remains opaque and immutable to the callback. Provide accessors:

```c
awp_slice_t awp_frame_feed(const awp_frame_t *);
awp_slice_t awp_frame_symbol(const awp_frame_t *);
awp_slice_t awp_frame_payload(const awp_frame_t *);

uint64_t awp_frame_ingest_time_ns(const awp_frame_t *);
uint64_t awp_frame_enqueue_time_ns(const awp_frame_t *);
```

The pool never parses or generates a subject. The callback receives the original feed, symbol, and normalized payload bytes.

### Configuration

```c
typedef enum {
    AWP_DEADLINE_RETURN_STUCK = 0,
    AWP_DEADLINE_ABORT_PROCESS
} awp_deadline_policy_t;

typedef struct {
    awp_slice_t feed;
    uint32_t dedicated_slot;
    bool empty_symbol_only;
} awp_dedicated_rule_t;

typedef struct {
    size_t struct_size;
    uint32_t abi_version;

    uint32_t shard_worker_count;       /* N general workers */
    uint32_t dedicated_worker_count;   /* additional threads */
    size_t total_buffer_frames;

    size_t max_feed_bytes;
    size_t max_symbol_bytes;
    size_t max_payload_bytes;

    uint64_t heartbeat_interval_ns;
    uint64_t callback_stuck_ns;
    uint64_t shutdown_timeout_ns;

    awp_process_fn process;
    void *process_context;

    awp_error_fn report_error;
    void *error_context;

    const awp_dedicated_rule_t *dedicated_rules;
    size_t dedicated_rule_count;

    awp_deadline_policy_t deadline_policy;
} awp_config_t;
```

Configuration and rule bytes are copied during creation and remain immutable. Changing `N` or dedicated mapping while frames are live is unsupported because it can place one key on two workers.

### Public operations

```c
awp_status_t awp_pool_create(
    const awp_config_t *config,
    awp_pool_t **out_pool);

awp_status_t awp_submit(
    awp_pool_t *pool,
    const awp_frame_view_t *frame);   /* blocks on full */

awp_status_t awp_shutdown(
    awp_pool_t *pool,
    uint64_t timeout_ns);

awp_status_t awp_pool_destroy(
    awp_pool_t *pool);

awp_status_t awp_metrics_get(
    const awp_pool_t *pool,
    awp_metrics_t *pool_metrics,
    awp_worker_metrics_t *workers,
    size_t worker_capacity,
    size_t *worker_count);

awp_status_t awp_shard_for_key(
    const awp_pool_t *pool,
    awp_slice_t feed,
    awp_slice_t symbol,
    uint32_t *worker_id);
```

Deliberately omit `try_submit()` from v1: it makes accidental drop-on-full behavior too easy.

`awp_submit()` returns:

- `AWP_OK`: ownership was accepted and the frame will reach `process()` unless shutdown escalates to process abort.
- `AWP_ESHUTDOWN`: admission closed before enqueue.
- `AWP_E2BIG`: a field exceeds configured fixed capacity.
- `AWP_EINVAL`: malformed arguments.
- `AWP_EDEADLK`: detected reentrant submission from a worker that could block its own shard.

### Internal types

```c
struct awp_ring {
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;

    size_t head;
    size_t tail;
    size_t count;
    size_t capacity;

    struct awp_slot *slots;
};

struct awp_worker {
    struct awp_ring queue;
    pthread_t thread;

    _Atomic uint32_t state;
    _Atomic uint32_t generation;
    _Atomic uint64_t last_heartbeat_ns;
    _Atomic uint64_t callback_started_ns;

    struct awp_worker_metrics_atomic metrics;
};

struct awp_pool {
    struct awp_config_owned config;
    _Atomic uint32_t state;

    struct awp_worker *workers;
    size_t worker_count;

    pthread_t supervisor;
    pthread_mutex_t lifecycle_mutex;
    pthread_cond_t lifecycle_changed;
};
```

---

## B. Ring queue and frame lifetime

### Recommended queue: mutex/condvar MPSC

Every reader can submit to every worker, so the safe topology is:

```text
reader[0..R-1] → hash(feed,symbol) → worker queue[0..N-1]
```

For submission:

1. Calculate the shard before taking its queue lock.
2. Lock the destination queue.
3. While full and admission remains open, wait on `not_full`.
4. Recheck pool state after every wakeup.
5. Copy the frame into the tail slot.
6. Advance `tail`, increment `count`, update HWM.
7. Signal `not_empty` and unlock.

The successful tail insertion is the FIFO linearization point.

For consumption:

1. Lock the queue.
2. While empty and running, timed-wait on `not_empty`.
3. Borrow the head slot without advancing it.
4. Unlock and invoke `process()`.
5. Relock, recycle by advancing `head`, decrement `count`.
6. Signal `not_full` and unlock.

Keeping the active head slot occupied until `process()` returns prevents producers from overwriting callback memory. Queue capacity therefore includes the currently processed frame, which is appropriate for honest backpressure.

### Why not lock-free MPSC initially?

A correct blocking MPSC ring requires:

- Per-cell sequence numbers.
- Reservation and publication states.
- Acquire/release synchronization.
- A producer-death strategy after slot reservation.
- A lost-wakeup-free parking protocol.
- Shutdown handling for blocked producers.

It still needs a condvar, semaphore, futex, or equivalent when full. At 5k messages/s, this complexity is unlikely to buy meaningful latency.

### SPSC memory ordering

If deployment wiring genuinely proves one producer per worker, a future SPSC backend can use:

- Producer writes slot contents, then release-stores `tail`.
- Consumer acquire-loads `tail` before reading the slot.
- Consumer release-stores `head` after it has finished with the slot.
- Producer acquire-loads `head` before reusing that slot.
- Thread-owned cached index reads may use relaxed ordering.

For the recommended mutex ring, `head`, `tail`, `count`, and slot contents should not be atomic. Mutex lock/unlock provides the necessary synchronization. Lifecycle state uses acquire/release; approximate metrics can use relaxed atomics.

### Padding

- Align every worker object and ring slab independently.
- Use a configurable `AWP_CACHELINE`, defaulting to 128 bytes on Apple arm64 and 64 elsewhere.
- Allocate with `posix_memalign()`.
- Ensure the worker stride is a multiple of the chosen alignment.
- Keep frequently written metrics away from queue locks and state read by producers.

Separating `head` and `tail` matters for SPSC. It offers little benefit in the mutex design because both are protected by the same lock. Isolating different workers is more important.

### Frame object pool

For v1, the ring slot slab is the frame pool:

- All slots and payload storage are allocated by `awp_pool_create()`.
- Each slot has fixed configured feed, symbol, and payload regions.
- No freelist or global allocation lock is needed.
- No `malloc()` or `free()` occurs after startup.
- Callback pointers are valid only until `process()` returns.

Copying is the right first implementation at the stated rate. Even 2 KB × 5k messages/s is only about 10 MB/s of copying.

A future acquire/fill/submit lease API is reasonable if profiling shows copy cost. It must define caller ownership on every error and limit outstanding producer-held frames to prevent pool exhaustion.

Most importantly, `process()` returning success must mean the downstream publisher has copied the buffer or explicitly assumed its lifetime. If an asynchronous publisher retains the slot pointer after returning, the next ring reuse will corrupt an in-flight publish.

---

## C. Worker loop and supervisor

### Startup

1. Validate all capacities, callbacks, mapping rules, and overflow calculations.
2. Allocate every queue, slot, metric block, and owned configuration string.
3. Create the supervisor and workers behind a startup barrier.
4. Each worker reports ready.
5. Transition to `RUNNING` only after every thread is ready.
6. If any `pthread_create()` fails, release the barrier into rollback, wake and join all created threads, then return failure.

No partially running pool is exposed.

### Worker loop

```c
for (;;) {
    publish_heartbeat();

    frame = wait_for_head_slot_with_timeout();

    if (frame == NULL) {
        if (pool_is_draining() && queue_is_empty())
            break;
        continue;
    }

    mark_callback_start(frame);
    rc = process(process_context, frame);
    clear_callback_state();
    publish_heartbeat();

    processed_total++;

    if (rc != 0) {
        process_error_total++;
        report_error(error_context, worker_id, frame, rc);
    } else {
        process_ok_total++;
    }

    advance_head_and_signal_space();
}
```

Rules:

- `process()` always runs outside queue and lifecycle locks.
- A callback error does not restart the worker.
- Error reporting must be bounded and nonblocking; otherwise the error path becomes a second callback stall.
- A TLS worker marker detects reentrant submission and shutdown calls.
- Timed empty waits let idle workers refresh heartbeats.

### Heartbeats

Track separately:

- `last_heartbeat_ns`: worker returned to library control.
- `callback_started_ns`: nonzero while inside `process()`.
- `processed_total`: actual forward progress.
- Worker generation and current frame identifier.

This distinguishes:

- Healthy idle worker: fresh heartbeat, empty queue.
- Overloaded shard: queue nonempty, growing HWM, callbacks completing.
- Slow/stuck callback: old callback start and no fresh heartbeat.
- Cleanly dead worker: exit notification received.
- Internal stall outside callback: stale heartbeat with no callback active.

A reasonable initial heartbeat interval is 100–250 ms. The stuck threshold must exceed the callback’s documented worst-case behavior; the 5 ms SLO is not a sensible death timeout.

### Restart

The supervisor may restart only after the old worker has definitively exited and been joined.

Sequence:

1. Observe exit notification.
2. Mark worker `FAILED`.
3. Join old thread.
4. Account for any current frame as `outcome_unknown/process_error` and recycle it according to the required error policy.
5. Increment generation.
6. Start one replacement against the existing queue.
7. Rate-limit restarts; after repeated failures, mark the pool fatal/degraded.

A stale heartbeat alone must never start a second consumer. The old callback may still be executing.

A callback that returns an error is isolated. A `SIGSEGV` is not. All pthreads share the same corrupted address space. Optional diagnostics may write a minimal record from an alternate signal stack to a pre-opened descriptor, then re-raise or terminate; recovery with `siglongjmp()` is unsafe.

### Shutdown

The application should consume termination signals in a `sigwait()` control thread and call `awp_shutdown()` normally. Do not call mutex or condition-variable APIs from a signal handler.

Shutdown uses one monotonic absolute deadline:

1. CAS pool state from `RUNNING` to `QUIESCING`.
2. Close admission.
3. Broadcast every `not_full` condition so blocked submitters return `AWP_ESHUTDOWN`.
4. Wait for submit paths already past their linearization point to finish.
5. Transition to `DRAINING`.
6. Broadcast `not_empty`.
7. Workers finish all accepted slots, then report exit.
8. Wait for exit notifications until the absolute deadline.
9. Join only workers known to have exited.
10. At the deadline, invoke the configured abort policy.

`pthread_timedjoin_np()` is a GNU extension and only times out the join—it does not kill the worker. macOS exposes ordinary blocking `pthread_join()`. Use a lifecycle exit latch and perform `pthread_join()` only after exit is observed. [Linux timed-join documentation](https://www.man7.org/linux/man-pages/man3/pthread_tryjoin_np.3.html), [Apple `pthread_join` documentation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/pthread_join.3.html).

Use deferred cancellation only at library-owned wait points, with cleanup handlers. Disable cancellation while inside arbitrary `process()` code. POSIX only requires a very small set of functions to be asynchronous-cancel-safe, so asynchronous cancellation can corrupt both the pool and publisher state. [POSIX cancellation rules](https://pubs.opengroup.org/onlinepubs/9799919799/functions/V2_chap02.html).

For a literal hard deadline:

- First call an optional cooperative publisher-abort hook.
- If workers still cannot exit, terminate the process or rely on an external service supervisor.
- If the application must survive, put the untrusted/stuck publisher behind a subprocess boundary.

Returning `AWP_ETIMEDOUT` while a thread continues accessing pool storage is not “force-stopped.” Such a pool must remain quarantined and cannot be destroyed.

---

## D. Hashing and dedicated workers

Use fixed 64-bit FNV-1a:

```c
uint64_t awp_hash_key(awp_slice_t feed, awp_slice_t symbol)
{
    uint64_t h = UINT64_C(14695981039346656037);

    for (size_t i = 0; i < feed.len; ++i) {
        h ^= (uint8_t)feed.ptr[i];
        h *= UINT64_C(1099511628211);
    }

    h ^= UINT8_C(0x1f);
    h *= UINT64_C(1099511628211);

    for (size_t i = 0; i < symbol.len; ++i) {
        h ^= (uint8_t)symbol.ptr[i];
        h *= UINT64_C(1099511628211);
    }

    return h;
}
```

The XOR-then-multiply order and constants match standardized FNV-1a. Unsigned overflow intentionally performs modulo-\(2^{64}\) arithmetic. [RFC 9923](https://datatracker.ietf.org/doc/html/rfc9923).

Rules:

- Hash exact bytes and lengths, excluding terminating NULs.
- Cast through `uint8_t`; signed `char` otherwise changes cross-platform results.
- No case folding, normalization, subject construction, or implicit separators.
- If `0x1F` is valid inside either key component, the concatenation is ambiguous. Either validate that the grammar forbids it or explicitly accept that additional collision class.

### Dedicated mapping

Treat `N` as the number of general shard workers. Optional dedicated workers are additional threads:

```text
general key:
    worker = hash(feed || 0x1f || symbol) % N

dedicated rule:
    worker = N + configured_dedicated_slot

generic empty-symbol fallback:
    worker = N + (hash % dedicated_worker_count)
```

This preserves the required `% N` mapping for ordinary keys.

Rules are resolved before the generic empty-symbol fallback. General keys never use dedicated slots. `N`, dedicated rules, and capacities are immutable until stop/drain/recreate.

“Stable mapping” means stable for the same configuration. Changing `N` necessarily remaps keys.

---

## E. Top eight production failure modes

| Failure | Consequence | Mitigation |
|---|---|---|
| Hot key or shard skew | One queue fills while others remain idle; reader blocks and p99 grows | Per-worker depth/HWM/blocked time; replay production key distributions; dedicated broadcast slots; size `N` for measured skew |
| SPSC used with multiple readers | Concurrent producer corruption and silent frame loss | Default to mutex MPSC; debug producer-ownership assertions for any SPSC backend |
| Sustained overload or slow publisher | Backpressure preserves frames but makes latency unbounded | Declare workload envelope; measure hottest-shard service rate; alert before sustained queue growth |
| Callback blocks or reenters pool | Entire shard stalls; same-shard recursive submit can deadlock | Async publish-accept callback; callback duration metric; TLS reentrancy rejection; cooperative abort hook |
| Publisher retains ring memory | Reused slot corrupts an in-flight publish | Define success as “buffer copied or ownership safely retained”; use a separate lease/completion API if retention is required |
| Ambiguous callback outcome | Retry may duplicate; recycle may lose delivery | Stable message IDs/deduplication; distinguish error from outcome-unknown; durable spool if end-to-end zero loss is literal |
| Supervisor restarts a merely stalled worker | Two consumers break FIFO and may duplicate processing | Restart only after definitive exit and join; generation numbers; treat stale heartbeat as `STUCK`, not `EXITED` |
| Shutdown assumes pthread kill is safe | Hung shutdown, UAF after premature destruction, or corruption from async cancellation | Cooperative drain; monotonic exit latch; quarantine on timeout; process abort or subprocess isolation for hard bounds |

Additional operational hazards worth testing include hash drift caused by signed `char`, metric/logging contention, counter wraparound, condition-variable lost wakeups, and live `N` changes.

---

## F. Test matrix

| Requirement | Concrete test | Pass gate |
|---|---|---|
| Backpressure never removed | Capacity 2, callback held by barrier, extra producer submits | Extra submit blocks, later succeeds, every accepted ID observed once, `drop_total == 0` |
| Full-queue contention | Multiple reader threads, tiny rings, randomized callback delay, millions of wraps | No overwrite, gap, duplicate, deadlock, or accepted-submit failure |
| Blocked submit during shutdown | Producer waits on full queue while shutdown closes admission | Producer wakes with `AWP_ESHUTDOWN`; no leak or permanent waiter |
| Per-key FIFO | Many keys with monotonically increasing per-key sequence, uniform and Zipf skew | Reorder distance exactly zero |
| Concurrent same-key semantics | Assign internal enqueue tickets under queue mutex | Callback order equals enqueue-ticket order |
| Stable hashing | Golden vectors with empty fields, high-bit bytes, delimiter cases, several `N` values | Identical hash and shard on macOS/Linux, Clang/GCC |
| Dedicated workers | Mix ordinary, empty-symbol, and explicitly dedicated feeds | Dedicated traffic always uses configured slots; general traffic never does |
| Publish latency | Timestamp before `awp_submit()`, finish after callback returns accepted | Absolute p99 ≤5 ms for every declared representative workload |
| Pool overhead | Randomized direct-callback versus pool A/B runs | Suggested incremental p99 ≤0.5 ms and absolute p99 ≤5 ms |
| Subject grammar untouched | Byte-edge cases, maximum lengths, checksums | Callback receives byte-identical feed, symbol, and payload |
| Callback error isolation | Reject every kth frame with several codes | One error event/counter each; slot recycled; later frames continue; no restart |
| Frame lifetime | Success, error, shutdown, wrap, clean worker exit | All slots return reusable; ASan/UBSan/leak checks clean |
| No hot-path allocation | Interpose allocator after creation and warmup | Zero allocation/free calls from submit, dispatch, and recycle |
| Fixed threads | Instrument `pthread_create()` while processing millions of frames | Exactly `N + D` workers plus one supervisor |
| Heartbeats | Idle, busy, slow, and error callbacks | Correct state classification; idle worker not falsely marked dead |
| Safe restart | Inject exit between frames | Old generation joined before one replacement; backlog remains FIFO |
| Stalled callback | Nonreturning callback in child process | Detected as stuck; no parallel replacement; configured process abort meets deadline |
| SIGSEGV diagnostics | Faulting callback in child process | Minimal diagnostic emitted and process terminates; no in-process recovery claim |
| Graceful shutdown | Concurrent submitters and adequate deadline | Admission closes; all accepted frames processed; every thread joined |
| Deadline edge | Callback releases just before and after deadline | Clean drain before deadline; documented abort policy after it |
| Hot-bucket metrics | Known skew and induced blocking | Depth ≤ capacity; exact HWM; monotonic blocked time; correct counts |
| Metrics concurrency | Snapshot continuously during stress | No races, torn values, crashes, or decreasing cumulative counters |
| Long soak | Real publisher, production sizes/skew, transient delays | 6–24 hours with zero queue drops/reorder/leaks and no unexplained restart |

### Benchmark method

- Release build only; sanitizers are correctness runs, not latency runs.
- Measure from before possible submit blocking through callback acceptance.
- Use an open-loop scheduled arrival generator to expose coordinated omission.
- Cover 1k and 5k messages/s, 500 and 2,000 keys, uniform, Zipfian, hot-key, burst, and downstream-delay scenarios.
- Run 60 seconds warmup followed by 5–10 independent runs of 5–10 minutes.
- Report p50, p90, p99, p99.9, maximum, worst one-minute p99, CPU, context switches, queue HWM, blocked time, and callback duration.
- Use preallocated per-worker sample buffers or histograms.
- Claim the SLO only if every representative run passes and the upper 95% confidence bound for p99 is below 5 ms.

“Publish accept bound unchanged” cannot literally mean zero additional latency because a thread handoff necessarily adds scheduling work. The defensible gate is a small agreed incremental p99 budget plus the absolute ≤5 ms target.

---

## G. Effort estimate

Assumptions:

- One experienced C/pthreads engineer.
- macOS and Linux support.
- Existing publisher callback or adapter.
- Mutex/condvar MPSC first.
- Tests developed with each phase.

| Phase | Work | Dependency | Person-days | Focused AI-agent hours | Exit gate |
|---|---|---|---:|---:|---|
| 0 | Contract and feasibility spike | None | 3–4 | 8–14 | Ownership, error, FIFO, retention, and abort semantics signed off |
| 1 | Public API and POSIX skeleton | Phase 0 | 2–3 | 8–12 | Strict-warning lifecycle build on macOS/Linux |
| 2 | Slot slabs and bounded MPSC rings | Phase 1 | 5–7 | 18–26 | Full-queue, wraparound, wakeup, and allocation tests pass |
| 3 | Hashing, dispatch, worker loop | Phase 2 | 4–6 | 14–22 | Hash, FIFO, routing, error-continuation tests pass |
| 4 | Metrics and error events | Phase 3 | 3–4 | 10–16 | Hot-bucket and snapshot stress pass |
| 5 | Supervisor and shutdown | Phases 3–4 | 6–9 | 22–34 | Restart, drain, blocked-submit, deadline, and stuck-child tests pass |
| 6 | Concurrency/fault qualification | Phase 5 | 6–8 | 22–32 | ASan/UBSan/TSan and randomized stress clean |
| 7 | Publisher integration/performance | Phase 6 | 6–9 | 22–36 | Real adapter meets latency envelope and soak succeeds |
| 8 | ABI and release hardening | Phase 7 | 2–3 | 8–12 | Reproducible release and operational contract |
| **Total** |  |  | **37–53 days** | **132–204 hours** | All gates above |

Unattended soak time is additional elapsed time, not focused engineering effort.

A production-grade lock-free MPSC backend would add approximately 8–12 person-days. A reader-by-worker SPSC matrix would add roughly 5–8 days for queue polling, fairness, buffer fragmentation, and cross-reader ordering semantics.

### Risks that could reorder the work

Move lifecycle/process isolation earlier if `process()` can block indefinitely.

Resolve ownership before ring layout if the publisher retains payload pointers after returning.

Resolve ordering first if multiple readers may submit the same key and FIFO means external venue order rather than queue insertion order.

Make retry/durable storage core architecture if callback errors must still satisfy business-level zero loss.

Evaluate size classes before freezing the ABI if frames cannot fit a reasonable fixed maximum.

Evaluate spin or lock-free queues only if the mutex implementation reproducibly consumes a material part of the latency budget.

### Completion criteria

Do not claim done until:

- Accepted-frame, FIFO, subject-integrity, error, shutdown, and metrics suites pass on both platforms.
- Clang and GCC strict-warning builds pass.
- ASan, UBSan, and TSan are clean.
- Hot-path allocator interception reports zero allocations.
- The real publisher’s buffer-retention contract is proven.
- A child-process test proves the hard shutdown escalation.
- Uniform, skewed, burst, and downstream-delay workloads meet the declared envelope.
- Direct-versus-pool results and raw per-run latency summaries are retained.
- A 6–24 hour soak shows zero queue drops, zero reorder, no leaks, and no unexplained worker generation changes.

### Is p99 ≤5 ms achievable?

**macOS laptop:** credible as a development target, but weak production evidence. A release build on a plugged-in, thermally stable machine should generally have substantial margin at 5k messages/s. Background services, power management, thermal changes, and waking many workers can still create tail excursions.

**Dedicated Linux:** comfortably realistic on modern non-overcommitted hardware if the callback is genuinely asynchronous, the hottest shard is below capacity, container CPU quotas are controlled, and the publisher itself leaves room in the budget. Default core pinning should not be necessary.

The target is not achievable during sustained offered load above the hottest worker’s service rate. Buffering delays that failure; it does not remove it.

---

## H. Trade-offs

| Decision | Advantages | Costs and risks | Recommendation |
|---|---|---|---|
| SPSC per reader vs shared MPSC | SPSC has minimal synchronization | Reader×worker ring matrix, polling/fairness, fragmented capacity, ambiguous cross-reader ordering | Shared mutex MPSC per worker; SPSC only with a proven single producer |
| Mutex MPSC vs lock-free MPSC | Simple blocking semantics and clear FIFO linearization | Possible mutex convoying on a hot shard | Mutex first; lock-free only after profiling |
| Condvar park vs spin-then-park | Parking saves CPU and behaves well at modest rates | Wakeup latency | Pure condvar initially; small opt-in spin only after measurements |
| Supervisor restart vs crash-only | Clean exit can recover one shard | Unsafe if old worker is merely stalled or memory is corrupted | Restart only definitively exited/joined workers; crash on memory corruption or hard-deadline breach |
| Frame copy vs pointer handoff | Copy gives simple ownership and protects against caller mutation | Extra memory bandwidth and mutex hold time | Fixed-slot copy in v1; lease API only if profiling and retention contract justify it |
| Deferred cancel vs process abort | Deferred cancel is safe at library wait points | Cannot stop arbitrary callback loops | Cooperative stop first; process abort for a literal deadline |
| Per-shard vs global frame pool | Per-shard storage isolates contention | Capacity can be stranded under skew | Ring-slot slabs per shard; total capacity sized for skew |
| Direct logging vs injected event sink | Direct logging is simple | Allocation, locks, I/O, recursive failure | Counters plus bounded caller-provided event sink |

---

## Where a naïve Rust/tokio port fails

- Tokio channel ownership does not translate into implicit C pointer lifetime.
- A Tokio task abort is not equivalent to safely terminating a pthread.
- Rust panic containment does not make `SIGSEGV` recoverable inside a shared C process.
- “Each reader is single-threaded” does not make each worker queue SPSC.
- A bounded async send preserves backpressure; replacing it with `try_enqueue()` silently deletes the zero-drop invariant.
- Same-shard execution preserves queue insertion order, not an unknowable wall-clock order between concurrent producers.
- Returning from an async publish call does not automatically prove the library has copied the payload.
- Resizing the shard count remaps keys and can violate FIFO across worker generations.
- Recycling an errored frame is not end-to-end zero loss.
- A timed join bounds waiting; it does not terminate the joined thread.


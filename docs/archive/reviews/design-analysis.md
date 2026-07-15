# async-worker-pool design analysis

## Verdict: REJECT for production dispatch as implemented

The steady-state sequence-ring algorithm is largely sound under its stated producer/consumer cardinalities. The production blockers are lifecycle and ownership failures around it:

- Ordinary shutdown silently discards successfully submitted frames.
- Supervisor restart frees queue storage while submitters may still access it.
- Forced detach permits pool destruction while a callback is still using pool memory.
- Shutdown can exceed its deadline or block forever.
- Metrics report zero drops even when shutdown/restart abandons frames.

The architecture is salvageable, but the current implementation should not be integrated with real NATS publishing until the S0 items are corrected.

This was a read-only source review; no files or build artifacts were modified. The C review method led me to separate steady-state atomic correctness from lifecycle/reclamation correctness—the latter is where the critical failures are concentrated.

> **Insight:** The sequence ring can be locally correct while the pool remains unsafe. Closing a lock-free object is not a reclamation barrier: storage may be freed only after every producer and consumer has demonstrably left it.

## A. Concurrency correctness

### Ring modes

| Mode | Implementation | Steady-state verdict |
|---|---|---|
| SPSC | `push_sp()` + `pop_sc()` | Correct with exactly one producer and one consumer. |
| MPSC | `push_mp()` + `pop_sc()` | Correct sequence protocol with many producers and one consumer. Default matches the pool. |
| SPMC | `push_sp()` + `pop_mc()` | Correct with exactly one producer and multiple consumers. The pool itself never uses multiple consumers. |
| MPMC | `push_mp()` + `pop_mc()` | Correct basic Vyukov-style claim/publication protocol. The pool exercises only its one-consumer subset. |

The payload synchronization is appropriate:

- Producer writes `cell->data`, then release-stores the published sequence in [`ring.c:146`](/Users/dima/c_lang/async-worker-pool/src/ring.c:146).
- Consumer acquire-loads the sequence before reading `data` in [`ring.c:235`](/Users/dima/c_lang/async-worker-pool/src/ring.c:235).
- Consumer release-stores the recycled sequence before a producer reuses the cell in [`ring.c:244`](/Users/dima/c_lang/async-worker-pool/src/ring.c:244).
- Relaxed position loads/stores are adequate because positions claim slots; cell sequences publish data. The position CAS is `acq_rel`, which is stronger than necessary rather than too weak.

However, this is not a lock-free progress guarantee in the strict sense. An MPSC/MPMC producer advances `enqueue_pos` before publishing its cell. If it is preempted or terminates in that interval, the consumer cannot pass the missing cell even if later producers have published later positions. That is a material tail-latency and producer-death failure mode.

### Close semantics

The acquire/release ordering on `closed` is sufficient to communicate the flag, but it does not quiesce in-flight operations. A producer can:

1. Observe `closed == 0`.
2. Claim a position.
3. Be preempted.
4. Have shutdown or the supervisor close and destroy the ring.
5. Resume and write into freed `cells`.

The window is visible in [`push_mp()`](/Users/dima/c_lang/async-worker-pool/src/ring.c:119), while restart destroys the storage in [`supervisor.c:75`](/Users/dima/c_lang/async-worker-pool/src/supervisor.c:75).

### Position wrap and capacity

The per-cell sequence design handles ordinary reuse and avoids a simple ring-index ABA. Long-term portability is weaker:

- `dif` is calculated by converting `size_t` positions to `intptr_t` and subtracting them. Conversion above `INTPTR_MAX` is implementation-defined, and signed subtraction can overflow around the signed boundary.
- On 64-bit targets, actual counter wrap is operationally irrelevant at 1–5k messages/s. On 32-bit targets, it is not.
- [`awp_round_up_pow2()`](/Users/dima/c_lang/async-worker-pool/src/internal.h:61) overflows to zero for capacities above `2^31`. Ring initialization can then succeed with `capacity == 0`, `mask == SIZE_MAX`, and unusable storage.
- `cap * sizeof(awp_cell_t)` also needs an explicit allocation-overflow check for portable 32-bit support.
- Requested capacity one is silently expanded to two. That is safe but should be explicit.

Production validation should bound capacity to a representable power of two below half the position range and check allocation multiplication.

### Frame-pool freelist

The normal acquire/release synchronization is correct: publishing `next[idx]` is ordered before the release CAS on `head`, and an acquiring thread observes it through the head’s acquire operation.

The ABA protection is not indefinite:

- The packed head has a 32-bit tag in [`frame_pool.c:10`](/Users/dima/c_lang/async-worker-pool/src/frame_pool.c:10).
- Each message normally advances it twice—acquire and release.
- At 5k messages/s, the tag wraps in roughly five days. An ABA failure still requires a CAS participant to remain stalled across that entire cycle, so this is a design risk rather than an immediate failure, but the tag is not a proof against ABA for an indefinitely running service.
- `atomic_uint_fast64_t` is not guaranteed lock-free by C11. The implementation should check `atomic_is_lock_free()` or stop claiming universal lock freedom.
- A double release is not detected. Releasing the current head again can set `next[idx] = idx`, allowing the same frame to be acquired concurrently. No normal steady-state path intentionally double-releases, but debug ownership state would catch future lifecycle mistakes.
- Actual implemented failure paths are leaks and UAF: cancellation can abandon the currently owned frame, restart loses queued frames, and detach/destroy can free the slab while a worker still uses it.

### Wrong-mode UB

“Wrong mode is UB” is a poor contract for this high-level pool API. The pool always has one consumer per queue, so SPMC and MPMC add no current capability. The safest design is:

- Internally select MPSC for the general pool.
- Keep SPSC only as a specialist option when single-producer ownership is provable, ideally with debug ownership/concurrency checks.
- Keep multi-consumer modes internal until a public topology actually uses them.
- Validate both enum bounds; negative enum values currently pass checks that only test `> AWP_RING_MPMC`.

## B. Backpressure and “never drop”

### Spin versus parking

Queue-full backpressure is correct in the narrow steady-state sense: the producer waits rather than overwriting or returning “full.”

The waiting policy is unsuitable as the only production policy:

- Empty workers spin, then call `pthread_testcancel()` and `sched_yield()` indefinitely via [`awp_backoff()`](/Users/dima/c_lang/async-worker-pool/src/internal.h:49).
- Full producers and frame-pool waiters use the same policy.
- With 8–32 workers at only 1–5k messages/s, most queues are empty most of the time. Those permanent workers still consume scheduler and CPU resources.
- On oversubscribed or CPU-quota-limited hosts, spinning can starve the worker or producer needed to resolve the wait. This creates priority inversion and worsens p99.
- CAS loops provide no producer fairness; one producer can starve under contention.

A bounded spin followed by a semaphore/condition-variable/event wait is a better default. Spin-only can remain an opt-in for dedicated, measured CPUs.

### Global frame-pool coupling

[`awp_submit()`](/Users/dima/c_lang/async-worker-pool/src/pool.c:180) acquires a global frame before it computes the shard and waits on that shard’s ring. Producers blocked on one hot queue therefore retain global frames. Once the pool is exhausted, producers targeting healthy, empty shards also stop.

This defeats soft shard isolation and makes the global frame pool an unobserved second backpressure point. Per-shard capacity or ring-owned frame slots would contain the pressure.

### Close and shutdown

Ring and frame-pool waiters normally poll the close flag and eventually return. Pool-level shutdown is nevertheless unsafe:

- It does not wait for already-entered submitters.
- Ring close does not wait for claimed-but-unpublished slots.
- Supervisor joins can block forever.
- Concurrent shutdown calls do not wait for the first shutdown to finish.
- `awp_frame_pool_close()` still allows acquisition while the freelist is non-empty; its “one last try” comment is not what the code enforces.

### Meaning of “never drop”

The honest claim is:

> A full queue does not overwrite or reject a frame while the pool remains running and its consumer continues making progress.

It is not an end-to-end delivery guarantee:

- Nonzero `process()` results are terminal and the frame is recycled.
- Normal shutdown discards accepted frames.
- Restart discards queued and possibly in-flight frames.
- Forced shutdown can abandon work.
- `dropped` is never incremented for any of those outcomes.

The design document correctly narrows this at [`DESIGN.md:134`](/Users/dima/c_lang/async-worker-pool/docs/DESIGN.md:134), but the API metrics do not make the distinction operationally visible.

## C. FIFO and sharding

### Per-key FIFO

Stable hashing plus one consumer guarantees sequential callback execution in ring-enqueue linearization order. It guarantees:

- Program order from a single producer.
- Order between non-overlapping submissions where one call completes before the next begins.
- One arbitrary, valid queue order for overlapping MPSC submissions.

It does not reconstruct venue/source order between two concurrent readers submitting the same key. Global `frame->seq` cannot provide that order: it is assigned in [`pool.c:225`](/Users/dima/c_lang/async-worker-pool/src/pool.c:225), before the MPSC reservation CAS that defines enqueue order.

If “FIFO” means source sequence across readers, the source must be serialized or carry an explicit per-key sequence upstream.

The E2E tests do not establish concurrent same-key FIFO. They check `(reader,key)` separately, and each reader sends fewer messages than the key cycle: 200 messages over 500 keys in `test_e2e_modes`, with a multiplier coprime to 500. Each `(reader,key)` is visited at most once, making the reorder check effectively vacuous.

### Broadcast mapping

For `0 < n_broadcast_workers < n_workers`, mapping is stable and dedicated ranges are disjoint:

- Broadcast feeds hash only the feed into `[0,D)`.
- Ordinary traffic hashes `(feed,symbol)` into `[D,N)`.

Caveats:

- It is feed hashing, not round-robin and not fanout. One hot broadcast feed always uses one worker.
- `n_broadcast_workers == n_workers` is accepted. Ordinary traffic then falls through to worker zero, leaving all other workers unused.
- `awp_submit()` hashes truncated feed/symbol strings, while `awp_shard_of()` hashes the caller’s untruncated strings. For labels over 64 bytes, the helper can disagree with actual submission.
- Silent label truncation can collapse distinct keys onto an identical stored key.

## D. Supervisor and shutdown

### Restart and two-consumer risk

On the happy path, the supervisor joins the old worker before starting a replacement. That ordering is correct and normally prevents two active consumers.

The implementation still lacks proof:

- Join return values are ignored.
- `alive` conflates starting, running and exited states.
- There is no worker generation or definitive `EXITED/JOINED` state.
- A newly created worker starts with `alive == 0`; the supervisor can mistake “not scheduled yet” for “exited” and block joining a healthy worker.
- Restart destroys the queue instead of preserving it, losing backlog and racing producers.
- Ring reinitialization or thread creation failure leaves the pool running with a dead shard and no degraded/fatal state.

### Cancellation

Cancellation is enabled across `process()` and `on_error()` in [`worker.c:8`](/Users/dima/c_lang/async-worker-pool/src/worker.c:8). Therefore:

- A callback containing a cancellation point can be terminated while holding application locks/resources.
- The current frame is not released.
- `alive` is not cleared because no cleanup handler exists.
- A CPU-bound callback with no cancellation point ignores deferred cancellation, leading to detach or an unbounded supervisor join.
- `awp_backoff()` also makes producer-side submit/release loops cancellation points. Canceling a producer after frame acquisition can leak that frame.

Cancellation should be disabled around arbitrary callbacks and used only at cleanup-protected library wait points.

### Bounded shutdown

The hard bound is not portable or real:

- The supervisor uses blocking `pthread_join()` after cancellation.
- Pool shutdown waits until its deadline and then unconditionally joins the supervisor.
- Each worker may receive a minimum 50–100 ms wait plus an additional 500 ms cancellation grace, even after the overall deadline is exhausted.
- Detaching a live worker does not stop it.
- Destroying a detached worker’s pool is UAF.

A timeout can safely return a quarantined, non-destroyable pool. A literal hard stop for arbitrary callback code requires process termination or subprocess isolation.

## E. Latency envelope

p99 ≤5 ms is plausible on dedicated, non-overcommitted hardware when:

- The hottest shard’s offered rate remains below its service rate.
- `process()` is predictably short and does not retain frame memory.
- No producer is stalled after ring reservation.
- CPU quotas and scheduler contention are controlled.

It is not established by the current benchmarks:

- `submit_ns` is recorded after frame-pool acquisition and feed/symbol/payload copying, so frame-pool backpressure and the front half of submission are omitted.
- Workloads are closed-loop, unpaced bursts rather than open-loop 1k/5k arrivals.
- Callback work is essentially a short volatile loop.
- There is no production skew, hot-key, burst, downstream-delay or real NATS publish case.
- There is no warmup, repeated-run confidence, worst-window tail, CPU/quota or context-switch reporting.
- `bench_all_modes` compares one producer for SPSC/SPMC with four producers for MPSC/MPMC, so it does not isolate mode overhead.

The per-call mode switch is predictable and negligible. The material cost is whether the chosen side uses a CAS, plus the operational/test complexity of four modes.

The implementation does not copy all 4KB unconditionally, but it does `memset()` the entire approximately 4.2KB frame on every acquisition, then copies the actual payload. At 5k/s this is not a raw bandwidth crisis, but it touches roughly 67 cache lines per small message and can pollute caches. Clear only metadata and used bytes unless residual-byte clearing is an explicit requirement.

## F. API and architecture

The module decomposition is understandable and appropriately small. The problematic boundary is that the supervisor owns queue destruction even though producers concurrently own queue operations. Queue storage should remain pool-lifetime stable.

Before NATS integration:

- Define callback success as “the NATS layer has copied or safely taken ownership of every frame-backed byte.” Retaining `frame` or `payload` past callback return is invalid.
- Reject `payload == NULL && payload_len > 0`.
- Reject oversize labels/payloads rather than silently truncating market data.
- Define precise statuses such as invalid input, shutdown, timeout and too large; generic `-1` is ambiguous.
- Prohibit or detect same-pool callback reentrancy. Recursive submit can deadlock on its own queue or the global frame pool.
- Make metric snapshots caller-owned or serialized.
- Add callback duration/in-flight state, frame-pool wait/exhaustion, admission rejection, forced-discard, outcome-unknown and restart-failure metrics.
- Split `processed` into attempted/succeeded/failed. It currently increments after callback errors too.
- Clarify that async NATS acceptance is not broker/durable delivery; literal zero loss requires retry/spooling/ack semantics outside this queue.
- Add `struct_size`/ABI versioning before freezing the public configuration and frame layout.

## G. Prioritized findings

| ID | Severity | Area | Finding | Evidence | Recommended fix |
|---|---|---|---|---|---|
| AWP-01 | S0 | Lifecycle/UAF | Shutdown/restart can free rings or the pool while submitters that passed the initial state check are still using them. | `pool.c:193–253, 308–380`; `ring.c:131–148`; `supervisor.c:61–76` | Close admission, track active submit calls, wait for quiescence before drain or reclamation. |
| AWP-02 | S0 | Shutdown/data loss | Setting `shutting_down` makes workers exit instead of draining; popped and queued accepted frames are silently recycled. | `worker.c:15, 27–30`; `pool.c:317–321, 355–359` | Separate QUIESCING from DRAINING; workers exit only on closed-and-empty. Account forced abandonment. |
| AWP-03 | S0 | Shutdown/UAF | Detach fallback sets `alive=0`, after which destroy frees state still reachable by the detached callback. | `worker.c:107–122`; `pool.c:366–380` | Never destroy with an unjoined worker; quarantine or terminate at process boundary. |
| AWP-04 | S0 | Cancellation | Cancellation is enabled inside arbitrary callbacks without cleanup, leaking frames and potentially corrupting application state. | `worker.c:8–10, 45–62`; `supervisor.c:68–70` | Disable cancellation around callbacks; use cleanup-protected library cancellation points and cooperative abort. |
| AWP-05 | S0 | Bounded shutdown | Supervisor joins can block forever; worker grace periods exceed the total deadline. | `supervisor.c:68–73`; `pool.c:324–350`; `worker.c:88–129` | Use one absolute deadline and exit latches; never blocking-join an unproven-live thread. |
| AWP-06 | S0 | Restart/data loss | Restart destroys the entire queue, losing queued frames and racing active producers. | `supervisor.c:26–38, 51–80` | Keep queue storage stable across worker generations; preserve and account backlog/current frame. |
| AWP-07 | S0 | Shutdown state | A second shutdown returns before the first finishes; destroy then skips waiting because `shutting_down` is already set. | `pool.c:308–309, 371–372` | Use a lifecycle state machine; concurrent shutdown callers wait for STOPPED. |
| AWP-08 | S1 | Supervisor | `alive==0` can mean “thread not scheduled yet”; supervisor may join a healthy empty worker. | `worker.c:12, 71–74`; `pool.c:141–153`; `supervisor.c:26–33` | Add startup barrier and explicit STARTING/RUNNING/EXITED/JOINED states. |
| AWP-09 | S1 | Backpressure | Hot-shard producers retain global frames while blocked, exhausting ingress for healthy shards. | `pool.c:196, 228–233`; `frame_pool.c:77–81` | Isolate capacity per shard or use ring-owned storage; measure frame-pool waits. |
| AWP-10 | S1 | CPU/latency | Empty workers and blocked producers spin/yield indefinitely, causing quota burn and possible priority inversion. | `internal.h:49–58`; `ring.c:107–115, 208–219` | Bounded spin followed by park/wake; make spin-only deployment-specific. |
| AWP-11 | S1 | Input integrity | Oversize data is silently truncated; NULL with nonzero payload length publishes zero-filled data. | `awp.h:143–150`; `pool.c:207–223` | Return `-E2BIG`/`-EINVAL`; never mutate production payloads silently. |
| AWP-12 | S1 | Metrics/UB | Concurrent metric snapshots write one shared non-atomic buffer; `dropped` is never incremented despite abandonment. | `pool.c:98, 256–285, 290–294` | Use caller-owned snapshots; expose real rejection/error/discard/outcome counters. |
| AWP-13 | S1 | Reentrancy | Callback submit can deadlock on its own queue/global frame pool; callback shutdown can self-join. | `worker.c:45–54`; `pool.c:196, 233, 337–350` | Detect callback context with TLS and reject blocking same-pool lifecycle/submission operations. |
| AWP-14 | S1 | Capacity safety | Power-of-two rounding can overflow to zero and allocation multiplication is unchecked. | `internal.h:61–71`; `ring.c:17–40` | Validate a safe maximum and checked multiplication before allocation. |
| AWP-15 | S2 | FIFO/API | FIFO is queue-linearization order, not external order between concurrent same-key readers; tests do not cover that contract. | `pool.c:225, 233`; `ring.c:142–148`; `test_e2e_modes.c:22–36, 50–66` | Specify ordering precisely; serialize or sequence upstream when source order matters. |
| AWP-16 | S2 | Mode API | SPMC/MPMC add no pool capability, wrong mode is unsafe, and negative enum values pass validation. | `awp.h:91–107`; `pool.c:83`; `ring.c:50, 167–180` | Fix pool topology to MPSC by default; retain specialist modes internally or add checks. |
| AWP-17 | S2 | Freelist | 32-bit ABA tag wraps; lock freedom is platform-dependent; double release forms a corrupt/self-linked stack. | `internal.h:116–122`; `frame_pool.c:10–18, 96–120` | Check lock freedom, add debug ownership checks, and use storage design without finite-tag ABA if indefinite proof is required. |
| AWP-18 | S2 | Latency evidence | Benchmark omits frame-pool/front-half delay and lacks representative open-loop and real-publisher workloads. | `pool.c:196–226`; `bench_all_modes.c:40–49, 130–169` | Timestamp at ingress; qualify with real NATS, skew, bursts, delays, repetitions and system metrics. |
| AWP-19 | S2 | Broadcast/API | All-workers-broadcast config sends ordinary traffic to worker zero; helper can disagree with truncated submit keys. | `pool.c:91–95, 207–217`; `shard.c:55–66` | Reject or explicitly constrain all-broadcast configuration; normalize before both helper and submit hashing. |
| AWP-20 | S2 | Qualification | Ring tests count operations but do not detect duplicate-plus-loss; FIFO and stuck-shutdown tests are weak; no sanitizer/soak targets exist. | `test_ring_modes.c:55–64, 124–126`; `test_supervisor.c:14–19`; `Makefile:97–111` | Add exact-ID stress, concurrent shutdown/restart tests, ASan/UBSan/TSan, long wraps and real-adapter soak. |
| AWP-21 | S3 | Docs/API | `on_error` is described as a recycle/drop hook but is called only for nonzero process returns. | `awp.h:62–66`; `worker.c:50–55`; `pool.c:355–359` | Correct the contract or add a separate abandonment callback. |

## Top five fixes

1. Implement a real pool lifecycle: `RUNNING → QUIESCING → DRAINING → STOPPED`, with closed admission, active-submit quiescence, deterministic drain and concurrent-shutdown waiting.
2. Remove unsafe cancellation/detach reclamation. Disable cancellation inside callbacks; on timeout quarantine the pool or escalate at a process boundary.
3. Redesign supervision around explicit worker states/generations and stable queues. Restart only after definitive exit/join and never destroy backlog during live operation.
4. Tighten the public contract: fixed safe topology, strict input validation, precise statuses, reentrancy rules, thread-safe metrics and explicit delivery outcomes.
5. Replace indefinite spin with hybrid waiting, isolate shard capacity, and requalify latency with ingress timestamps and a real NATS adapter under open-loop skew/burst/downstream-delay workloads.

## What is solid

These parts should be retained:

- The per-cell release/acquire publication protocol in the ring.
- Default MPSC for many reader threads feeding one worker per shard.
- One permanent consumer per shard and no per-message thread creation.
- Stable FNV-1a routing and disjoint normal broadcast/general ranges.
- Fixed-copy frame ownership; callers cannot mutate or retain input unexpectedly.
- No library allocation in the normal per-message path.
- Soft nonzero callback returns are counted and do not terminate the worker.
- Atomic cumulative counters on the processing hot path.
- The small, comprehensible module split.

The all-mode tests remain useful smoke tests; they are simply not production concurrency qualification.

## Mutex design versus current atomics

| Design | Wins when | Loses when |
|---|---|---|
| Mutex + condition variable | Sparse/moderate traffic, shared/cloud CPUs, CPU quotas, lifecycle simplicity, clear close/drain semantics and low operational cost | Very high sustained contention where measured mutex convoy/wakeup latency materially affects tails |
| Current atomic ring + spin | Dedicated CPUs, consistently busy queues, short callbacks, controlled scheduling, and profiling proves synchronization is significant | Mostly idle workers, oversubscribed hosts, stalled producers/callbacks, strict shutdown/restart requirements and cloud cost sensitivity |

At the specified 1–5k messages/s, mutex/condvar or bounded-spin-then-park is likely the better default unless measurements on the real publisher prove otherwise. The runtime mode switch is not the deciding cost; scheduler behavior, hot-shard skew, callback latency and lifecycle complexity dominate.

The strongest path is not necessarily to discard the atomic ring. Its steady-state core can remain after lifecycle reclamation is fixed and indefinite waiting is replaced with a measured hybrid strategy.

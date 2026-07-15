# Design: Sharded low-latency async worker pool (C)

## Goal

Preallocated, sharded dispatch stage for market-data frames:

- **N** long-lived `pthread` workers (created once)
- Producer-side shard: `hash(feed, symbol) % N` → per-key **FIFO**
- Bounded per-worker queues with **blocking backpressure** (never drop)
- p99 **submit→process-return** ≤ **5 ms** on a closed-loop microbench (includes light simulated work; not open-loop publisher-accept)
- Fault isolation, supervisor heartbeats, bounded shutdown

This is the C analog of the Rust/tokio permanent-worker “Design A” pool.

## Why not N = cores?

Little’s Law sizes **concurrency** need:

```
N_min ≈ peak_msg/s × service_time_s
```

At 5k msg/s and 50 µs service, `N_min ≈ 0.25` — tiny. The binding constraint is **hash skew**: hot symbols collide on one worker and queue latency appears even when aggregate CPU is low. Pick **N well above N_min** (e.g. 8–32) as skew headroom. Do **not** set N to core count under oversubscribed VMs (pinning is also void there).

## Module layout

| Path | Role |
|------|------|
| `include/awp/awp.h` | Public API |
| `src/ring.c` | Bounded multi-mode ring (C11 atomics, sequence protocol) |
| `src/frame_pool.c` | Preallocated frame slab (lock-free freelist) |
| `src/shard.c` | FNV-1a + broadcast dedicated workers |
| `src/worker.c` | Worker loop + portable timed join |
| `src/supervisor.c` | Heartbeat / restart |
| `src/pool.c` | Create, submit, metrics, shutdown |

Diagrams (architecture, submit path, lifecycle, ring modes, supervisor): [`DIAGRAMS.md`](DIAGRAMS.md).  
Local benchmark numbers: [`BENCHMARKS.md`](BENCHMARKS.md).

## Queue choice — all concurrency models

**Atomic bounded ring** (Vyukov-style cell sequences, C11 `<stdatomic.h>`). Mode is **not** assumed MPSC-only; configure via `awp_config.ring_mode`:

| Mode | Producers | Consumers | Claim enqueue | Claim dequeue |
|------|-----------|-----------|---------------|---------------|
| `AWP_RING_SPSC` | 1 | 1 | store | store |
| `AWP_RING_MPSC` | many | 1 | CAS | store |
| `AWP_RING_SPMC` | 1 | many | store | CAS |
| `AWP_RING_MPMC` | many | many | CAS | CAS |

- Default for the worker pool is **MPSC** (N venue readers → one thread per worker) — override when topology differs (e.g. SPSC if one reader owns a shard).
- Wrong mode for actual concurrency is **UB** (data races on pos claims).
- Full/empty: spin + `pause`/`yield` — **backpressure, never drop**
- Capacity rounded up to power of two; cells cache-line padded
- Memory orders: acquire on sequence load, release after publishing data
- Depth: lock-free `enqueue_pos - dequeue_pos`

**Why not mutex:** Codex recommended mutex+condvar as a first-cut trade-off. Hot path uses atomics instead.

**Frame pool:** lock-free freelist of slab indices with ABA tags (Treiber-style packed head).

## Frame lifecycle

1. `awp_submit` acquires a slot from the **frame pool** (blocks if exhausted)
2. Copies feed/symbol/payload into the fixed-size frame (no hot-path `malloc`)
3. Computes shard, pushes pointer into worker ring (blocks if full)
4. Worker pops → `process(frame)` → releases frame to pool

## Sharding

```
h = FNV-1a64(feed || 0x1F || symbol)
shard = shard_base + (h % n_shard_workers)
```

Broadcast feeds (config list or `AWP_FRAME_BROADCAST`) map to workers `[0, n_broadcast_workers)`.

Stable hash ⇒ same key ⇒ same worker ⇒ sequential process ⇒ **reorder distance 0**.

## Fault isolation

| Fault | Behavior |
|-------|----------|
| Soft `process()` error | Count + optional `on_error`, recycle frame, **continue loop** |
| Unexpected worker exit | Supervisor joins + restarts thread (`awp_ring_reopen`, backlog kept) |
| Stall (queue depth > 0, no progress) | Cooperative stop; if still no progress → **quarantine**, close shard + frame pool, leak on destroy |
| Shutdown deadline | Quiesce submits; join within absolute deadline; late/stuck workers **quarantined** (no `pthread_cancel` / detach) |

C has no `catch_unwind`. Soft errors **must** be error codes. Hard faults (SIGSEGV in third-party code) remain process-fatal; supervisor + metrics make silent dark buckets diagnosable when the thread simply exits or stalls. **Cancellation is disabled** inside workers.

## Bounded shutdown

1. `RUNNING → QUIESCING` (reject new admits; wait `active_submits`)
2. Stop supervisor; `DRAINING`
3. Close frame pool + **all rings** (wake every parked producer/consumer wait)
4. If supervisor joined and submits drained: join workers under absolute deadline; residual drain
5. Else (timeout / unjoined supervisor): **quarantine** — rings closed, no join of stuck callbacks; destroy will leak
6. `STOPPED`; destroy only if reclaimable (joined, no quarantine, no live API refs)

Portable: no Linux-only `pthread_timedjoin_np` — poll state + sleep. **No force-stop via cancel/detach.**

## Observability

Per-worker: `processed`, `process_errors`, `enqueue_blocks`, `blocked_ns`, `queue_depth`, `queue_hwm`, `restarts`, `last_progress_ns`, `alive`.

Decisive post-deploy signal: **worst-worker HWM / blocked time**, not total CPU.

## Runtime helper

`awp_runtime_enabled()` reads `AWP_ENABLED=1|true|yes`. **Create is not gated** — callers decide whether to construct a pool.

## Explicit non-goals

- Core pinning by default
- Router thread
- Changing publish-side subject sharding / ack windows

## Test matrix

| Invariant | Test |
|-----------|------|
| Same key → same shard → FIFO | `test_unit` / `test_same_key_same_worker_fifo` |
| Full queue blocks, drops=0 | `test_full_queue_blocks_concurrent` |
| Soft process error survives | `test_process_error_survives` |
| Broadcast dedicated range | `test_broadcast_shard` |
| Bounded shutdown | `test_bounded_shutdown_stuck_worker` |
| Supervisor restart | `test_supervisor_restart_dead_worker` |
| Multi-reader e2e, reorder=0 | `test_e2e` |
| p99 submit→process-return ≤ 5 ms, drops=0 | `bench_dispatch` (microbench) |
| Terminal reopen re-closes | `test_terminal_reopen_recloses` |

## Codex reviews (gpt-5.6-sol xhigh)

| Pass | Artifact | Scope | Verdict |
|------|----------|--------|---------|
| 1 — estimate | [`CODEX_DESIGN_ESTIMATE.md`](CODEX_DESIGN_ESTIMATE.md) | Greenfield design (mutex MPSC recommended) | plan |
| 2 — analysis | [`CODEX_DESIGN_ANALYSIS.md`](CODEX_DESIGN_ANALYSIS.md) | As-implemented atomics multi-mode design | **REJECT** (mitigations in `c11bab8`) |
| 3 — impl+specs | [`CODEX_IMPLEMENTATION_REVIEW.md`](CODEX_IMPLEMENTATION_REVIEW.md) | Post-mitigation code + docs re-review (`1e8347b`) | **REJECT** (fixes in `4b1076c`) |
| 4 — re-review | [`CODEX_PASS4_REVIEW.md`](CODEX_PASS4_REVIEW.md) | Post-fix re-review of impl + specs (`4b1076c`) | **REJECT** |
| 5 — re-review | [`CODEX_PASS5_REVIEW.md`](CODEX_PASS5_REVIEW.md) | After Pass 4 fixes (`67a7148`) | **REJECT** |
| 6 — re-review | [`CODEX_PASS6_REVIEW.md`](CODEX_PASS6_REVIEW.md) | After residual single-owner destroy (`0436973`) | **REJECT** |
| 7 — re-review | [`CODEX_PASS7_REVIEW.md`](CODEX_PASS7_REVIEW.md) | After Pass 6 contract/teardown (`5c40ee8`) | **REJECT** |
| 8 — re-review | [`CODEX_PASS8_REVIEW.md`](CODEX_PASS8_REVIEW.md) | After frame-pool wake (`b10c934`) | **REJECT** |
| 9 — re-review | [`CODEX_PASS9_REVIEW.md`](CODEX_PASS9_REVIEW.md) | After stall ring close (`92f3b07`) | **REJECT** |
| 10 — re-review | [`CODEX_PASS10_REVIEW.md`](CODEX_PASS10_REVIEW.md) | After all-rings close (`9dc1587`) | **REJECT** |
| 11 — re-review | [`CODEX_PASS11_REVIEW.md`](CODEX_PASS11_REVIEW.md) | After reopen re-close (`6c9796a`) | **ACCEPT_WITH_NITS** |
| 12 — nits | [`CODEX_PASS12_REVIEW.md`](CODEX_PASS12_REVIEW.md) | Docs/bench/tests nit cleanup (`eb19f04`) | **ACCEPT_WITH_NITS** |

### Pass 2 was **REJECT** — mitigations landed (`c11bab8`)

| Codex issue | Fix in tree |
|-------------|-------------|
| Free while submitters active | `RUNNING→QUIESCING→DRAINING→STOPPED` + `active_submits` |
| Exit without drain | Workers process until closed+empty |
| Cancel/detach UAF | No cancel in `process()`; quarantine stuck workers; leak-on-quarantine destroy |
| Concurrent shutdown races | Waiters block until `STOPPED` |
| Restart destroys queue | `awp_ring_reopen` keeps storage/backlog |
| Indefinite spin | Hybrid spin then condvar park |
| Silent truncation | `-E2BIG` / `-EINVAL` |
| Hot-shard holds frames | Wait for ring space before frame acquire |
| Callback reentrancy | TLS → `-EDEADLK` |

E2E: `test_e2e_lifecycle` (drain, concurrent shutdown, restart progress).

### Pass 3 was **REJECT** — residual reclamation / reentrancy

Pass 3 re-verified prior mitigations against `1e8347b`. Steady-state ring memory orders look fine; residual **S0** clusters (see [`CODEX_IMPLEMENTATION_REVIEW.md`](CODEX_IMPLEMENTATION_REVIEW.md)).

### Pass 3 mitigations (post-review fix pass)

| Residual theme | Fix |
|----------------|-----|
| Free while submitters active | Register `active_submits` **before** lifecycle re-check; if still >0 after deadline → sticky `quarantined` |
| Shutdown quarantine → destroy | `awp_worker_join_deadline` / supervisor always set `pool->quarantined`; destroy leaks if set |
| Concurrent shutdown / destroy | `shutdown_waiters` + destroy waits for waiters to leave `life_cv` |
| Callback reentrancy | TLS wraps `process` **and** `on_error`; destroy-from-callback marks quarantine (no free) |
| Hot-shard holds frames | `awp_ring_try_push`: release frame before parking on full ring |
| Supervisor ownership | Interruptible interval; recheck `RUNNING` before restart; failed restart → quarantine |
| Supervisor not joined | Past deadline without join → quarantine (never free under live supervisor) |

New / extended tests: true sticky quarantine + safe destroy, process/on_error reentrancy, try_push backpressure, restart with concurrent producers.



### Pass 4 was **REJECT** — residual concurrent destroy / pre-registration

Key residual S0 after `4b1076c`:

1. `STOPPED` published before shutdown finishes using `life_mu` → destroy can free under shutdown.
2. Pre-registration / untracked public readers cannot be made safe by in-object counters alone; destroy must be externally serialized or use a stable handle.

Full report: [`CODEX_PASS4_REVIEW.md`](CODEX_PASS4_REVIEW.md).



### Pass 5 was **REJECT** — lifetime ownership boundary

Pass 5 (`67a7148`) closed local reclamation bookkeeping (STOPPED under `life_mu`, waiters, no-close on active_submits, join-after-success). Residual architecture:

- First-access race before `api_enter` cannot be sealed by in-object counters alone.
- Concurrent destroy needs single-owner + external API quiescence (or a stable control block).
- Quarantine must gate admission (submit reject) — addressed in follow-up.

Follow-up on main: single-owner destroy, quarantine admission reject, create-rollback join-safe leak.



### Pass 6 was **REJECT** — contract + supervisor teardown

At `0436973`: concurrent destroy promise contradicted free-after-CAS; supervisor unjoined still raced worker teardown. Follow-up: honest exactly-once destroy contract; skip worker close/join if supervisor unjoined; close shard on restart failure; recheck quarantine in submit loop; deadline residual drain.


### Pass 7 was **REJECT** — frame-pool wait domain

Restart-failure quarantine closed the shard but not the global frame freelist wait. Follow-up: `awp_pool_mark_quarantined` closes/wakes the frame pool; submit rechecks quarantine after acquire.


### Pass 11–12 **ACCEPT_WITH_NITS**

Library-internal permanent-block/UAF findings under the exactly-once destroy contract are closed. Pass 12 cleaned docs/bench/PNG/tests; residual nits if any are test-depth only.

## Build & verify

```bash
make check   # unit + supervisor + e2e + bench + example
```

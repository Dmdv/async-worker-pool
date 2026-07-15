# Design: Sharded low-latency async worker pool (C)

## Goal

Preallocated, sharded dispatch stage for market-data frames:

- **N** long-lived `pthread` workers (created once)
- Producer-side shard: `hash(feed, symbol) % N` → per-key **FIFO**
- Bounded per-worker queues with **blocking backpressure** (never drop)
- p99 ingest→process ≤ **5 ms** at ~1–5k msg/s, ~500–2000 keys
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
| `src/ring.c` | Bounded MPSC ring (C11 atomics, sequence protocol) |
| `src/frame_pool.c` | Preallocated frame slab (lock-free freelist) |
| `src/shard.c` | FNV-1a + broadcast dedicated workers |
| `src/worker.c` | Worker loop + portable timed join |
| `src/supervisor.c` | Heartbeat / restart |
| `src/pool.c` | Create, submit, metrics, shutdown |

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
| Soft `process()` error | Log/metric, recycle frame, **continue loop** |
| Unexpected worker exit | Supervisor joins + restarts thread |
| Stall (queue depth > 0, no progress) | Cooperative stop → optional cancel → restart |
| Shutdown deadline | Per-worker join timeout → cancel/detach → return abort count |

C has no `catch_unwind`. Soft errors **must** be error codes. Hard faults (SIGSEGV in third-party code) remain process-fatal; supervisor + metrics make silent dark buckets diagnosable when the thread simply exits or stalls.

## Bounded shutdown

1. Set `shutting_down`
2. Close frame pool + all rings (wake waiters)
3. Join supervisor
4. Join each worker with a share of `shutdown_deadline_ms`
5. Force-stop leftovers (`pthread_cancel` then detach if needed)
6. Drain residual frames back to the pool

Portable: no Linux-only `pthread_timedjoin_np` — poll `alive` + sleep.

## Observability

Per-worker: `processed`, `process_errors`, `enqueue_blocks`, `blocked_ns`, `queue_depth`, `queue_hwm`, `restarts`, `last_progress_ns`, `alive`.

Decisive post-deploy signal: **worst-worker HWM / blocked time**, not total CPU.

## Runtime gate

`AWP_ENABLED=1|true|yes` via `awp_runtime_enabled()` — mirror of Rust `DISPATCH_WORKER_POOL_ENABLED` for incremental rollout.

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
| p99 ≤ 5 ms, drops=0 | `bench_dispatch` |

## Codex reviews (gpt-5.6-sol xhigh)

| Pass | Artifact | Scope |
|------|----------|--------|
| 1 — estimate | [`CODEX_DESIGN_ESTIMATE.md`](CODEX_DESIGN_ESTIMATE.md) | Greenfield design (mutex MPSC recommended) |
| 2 — analysis | [`CODEX_DESIGN_ANALYSIS.md`](CODEX_DESIGN_ANALYSIS.md) | As-implemented atomics multi-mode design |

### Pass 2 verdict (as implemented): **REJECT for production dispatch**

Codex found **S0 lifecycle/UAF/data-loss** issues around shutdown, cancel, detach, and supervisor restart — not primarily in the ring sequence protocol itself.

**Top fixes called out:**
1. Real lifecycle: `RUNNING → QUIESCING → DRAINING → STOPPED` + active-submit quiescence
2. No unsafe cancel/detach while callbacks or submitters still touch pool state
3. Supervisor: stable queues across worker generations; no destroy-backlog restart
4. Tighten API: topology contract, statuses, reentrancy, delivery outcomes
5. Hybrid wait (spin then park); requalify latency with ingress timestamps + real publisher

**What Codex said is solid:** release/acquire cell protocol, default MPSC, one consumer per shard, FNV-1a routing, fixed-copy frames, no hot-path malloc, soft error isolation, modular split.

**Mutex vs atomics (Codex):** at 1–5k msg/s, mutex/condvar or spin-then-park is likely the better *operational* default unless measured on dedicated CPUs; steady-state atomic ring can stay after lifecycle is fixed.

## Build & verify

```bash
make check   # unit + supervisor + e2e + bench + example
```

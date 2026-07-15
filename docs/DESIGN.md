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
| `src/ring.c` | Bounded MPSC ring (mutex + condvar) |
| `src/frame_pool.c` | Preallocated frame slab |
| `src/shard.c` | FNV-1a + broadcast dedicated workers |
| `src/worker.c` | Worker loop + portable timed join |
| `src/supervisor.c` | Heartbeat / restart |
| `src/pool.c` | Create, submit, metrics, shutdown |

## Queue choice

**Mutex + condvar bounded ring** (MPSC):

- Multiple venue readers enqueue to the same worker → true MPSC
- Full queue: `pthread_cond_wait(not_full)` — backpressure, zero drops
- Empty: `pthread_cond_wait(not_empty)`
- `pthread_cleanup_push` unlocks the mutex on cancel (cancel-safe)
- Depth via `pthread_mutex_trylock` so observability never deadlocks

At 5k msg/s this easily meets the 5 ms p99 budget (bench: p99 ≈ 0.01 ms on a Mac laptop). A lock-free SPSC path can be added later if profiling demands it; correctness and multi-reader fan-in come first.

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

## Codex estimate (gpt-5.6-sol xhigh)

Independent architecture estimate via Codex CLI (`gpt-5.6-sol`, `model_reasoning_effort=xhigh`). Full text: [`CODEX_DESIGN_ESTIMATE.md`](CODEX_DESIGN_ESTIMATE.md).

**Effort (Codex):** ~34–49 person-days / ~124–190 focused AI-agent hours for production-grade lifecycle through publisher integration; phased 0–8 with mutex MPSC first.

**Hard contradictions Codex called out (accepted as design truth):**

1. “Never drop” applies to **queue admission**; soft `process()` errors recycle without end-to-end delivery guarantee.
2. Portable pthreads cannot force-stop arbitrary callbacks safely — escalate cooperatively; cancel only at cleanup-protected wait points; process-level isolation for hard faults.

**Agreements applied in this implementation:**

- Mutex+condvar MPSC as the correct first queue (multi-reader fan-in)
- Frame object pool / fixed slots; no malloc on hot path
- FNV-1a stable shard; N as skew headroom
- Soft-error isolation; supervisor restart; deadline shutdown without `timedjoin_np`
- Metrics focused on worst-worker occupancy
- Cooperative stop before cancel; cleanup handlers on ring mutex

**Trade-offs accepted:**

| Option | Choice | Why |
|--------|--------|-----|
| SPSC per reader vs MPSC | MPSC mutex ring | N readers → one worker is the real topology |
| Spin vs condvar park | Condvar | Correct backpressure; latency budget still met |
| Frame copy vs pointer handoff | Copy into pool slot | Simple lifetime; fixed max payload |
| Cancel vs cooperative death | Cooperative first | Avoids mutex leaks; cancel only as last resort |

## Build & verify

```bash
make check   # unit + supervisor + e2e + bench + example
```

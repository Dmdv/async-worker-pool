# Design review pass 7

| Field | Value |
|-------|-------|
| Reviewed commit | `5c40ee8` |
| **Verdict** | **REJECT** (frame-pool waiter on quarantine) |

---

# Verdict: REJECT

HEAD is clean and matches the expected Pass 6 fix:

`5c40ee8 fix: Pass 6 residual — destroy contract, supervisor teardown race`

The destroy contract and supervisor teardown race are fixed. However, one library-internal deadlock remains in the restart-failure path, so this crosses the stated rejection boundary.

## Findings

### S1 — Restart failure can permanently block a submitter

[`awp_submit()`](src/pool.c:324) rechecks quarantine, then calls the potentially blocking [`awp_frame_pool_acquire()`](src/frame_pool.c:83). That wait only observes `frames.closed`; it cannot observe pool quarantine.

Concrete interleaving with `frame_pool_size=1`:

1. Supervisor joins an exited worker and reopens its consumerless shard before creating the replacement at [`supervisor.c:28`](src/supervisor.c:28).
2. Producer P1 consumes the sole frame and enqueues it into that shard. P2 passes the quarantine check and blocks waiting for a frame.
3. Replacement `pthread_create()` fails. The supervisor closes only the shard and quarantines the pool at [`supervisor.c:39`](src/supervisor.c:39); it never closes/wakes the global frame pool.
4. P1’s frame is stranded without a consumer. P2 cannot acquire a frame or reach the next quarantine check.
5. Shutdown times out on `active_submits`, sets `can_close=0`, and consequently skips `awp_frame_pool_close()` at [`pool.c:480`](src/pool.c:480).

P2 can remain blocked indefinitely, making the external destroy precondition—join every producer—impossible.

Relatedly, ring close is not an admission commit barrier: [`try_push_mp()`](src/ring.c:267) can read `closed=0`, race with restart failure, and publish after the close, returning success with no consumer.

### S2 — Quarantine status can still be reported as successful shutdown

Restart failure quarantines the pool but does not increment `shutdown_aborts`. Because failed worker creation restores the worker to `JOINED`, a later shutdown can return `0` despite permanent quarantine and intentional leakage. That conflicts with the return contract in [`awp.h:185`](include/awp/awp.h:185).

### Nits / evidence debt

- [`pool.c:578`](src/pool.c:578) still claims concurrent/double destroy is a no-op, contradicting the correct public exactly-once contract.
- [`DESIGN.md:80`](docs/DESIGN.md:80) still describes cancellation, detach, and force-stop behavior that no longer exists.
- The benchmark records latency at process entry before its simulated publish work at [`bench_dispatch.c:30`](bench/bench_dispatch.c:30), using a closed-loop burst. It is useful microbenchmark evidence, not real publisher-acceptance evidence.
- No test injects restart `pthread_create()` failure or exercises a quarantined frame-pool waiter.

## Pass 6 top-5 re-verification

| Item | Status | Evidence |
|---|---|---|
| 1. Exactly-once externally serialized destroy | **PASS** | Public contract is explicit at [`awp.h:201`](include/awp/awp.h:201); reclamation guards remain intact. |
| 2. `can_close=0` with unjoined supervisor | **PASS** | Both join-failure and still-alive paths clear it; all worker teardown is gated at [`pool.c:500`](src/pool.c:500). |
| 3. Restart failure closes shard; submit rechecks quarantine | **PARTIAL / FAIL** | Ring waiters are addressed, but frame-pool waiters can remain permanently blocked; close/push commit race remains. |
| 4. Residual drain deadline-bounded | **PASS** | Absolute-deadline check is present at [`pool.c:537`](src/pool.c:537). Literal whole-operation hard-deadline wording remains too strong because joins can block. |
| 5. Prior fixed items | **PASS** | Lifecycle publication, waiter sampling, no-close on active-submit timeout, join bookkeeping, rollback safe-leak, supervisor ownership, stall-progress recheck, initialized-ring cleanup, and worker grace remain intact. |

## Verification

All existing HEAD-built binaries passed without rebuilding:

- Unit: 824/0
- Unit modes: 892/0
- Ring modes: 424/0
- Supervisor: 31/0
- E2E: 4/0
- E2E modes: 16/0
- E2E lifecycle: 406/0

Total: **2,597 passed, 0 failed**. All three benchmark binaries also exited successfully. The worktree remained clean.

## Top actions

1. On terminal quarantine/restart failure, close and wake the global frame pool—or make frame acquisition cancellation-aware—and recheck quarantine immediately after acquisition.
2. Establish a real close/admission commit boundary so an in-flight `try_push` cannot publish successfully after terminal shard closure.
3. Add deterministic restart-create-failure tests with `frame_pool_size=1`, two producers, and a strict completion timeout; then correct quarantine status, stale destroy comments, DESIGN, and benchmark claims.

`★ Insight ─────────────────────────────────────`
The destroy ownership boundary is now sound. The remaining defect comes from a different lifetime layer: quarantine closes one wait domain—the shard ring—but leaves the global frame-allocation wait domain open. Terminal transitions must wake every place a public operation can park.
`─────────────────────────────────────────────────`

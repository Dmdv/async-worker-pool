# Design review pass 8

| Field | Value |
|-------|-------|
| Reviewed | `b10c934` |
| **Verdict** | **REJECT** (stall quarantine left ring waiters parked) |

---

# Verdict: REJECT

HEAD is clean and matches `b10c9344f1cb8d92ba8631460cf37b049cdf41c1`.

The exact review-round 7 restart-failure defect is fixed, but another library-internal permanent block remains: stall quarantine wakes frame-freelist waiters but not producers parked on the affected shard’s ring-space condition.

## Summary

- Restart failure closes the consumerless shard, quarantines the pool, and increments `shutdown_aborts`.
- `awp_pool_mark_quarantined()` closes/broadcasts the frame pool.
- `awp_submit()` rechecks terminal state after frame acquisition.
- No new internal UAF was found under the documented externally quiesced, exactly-once destroy contract.
- A stuck worker with a full ring can still leave `awp_submit()` blocked forever, crossing the stated rejection boundary.

## Findings

### S1 — Stall quarantine permanently blocks ring-space waiters

Concrete interleaving:

1. A worker becomes permanently stuck in `process()` and its shard fills.
2. A producer’s `try_push` returns `-EAGAIN`; it releases the frame and parks in `awp_ring_wait_space()` at [pool.c:361](src/pool.c:361).
3. That wait observes only `ring.closed` and ring depth at [ring.c:20](src/ring.c:20).
4. Stall handling marks the worker and pool quarantined at [supervisor.c:119](src/supervisor.c:119), but does not close the worker’s ring.
5. `awp_pool_mark_quarantined()` closes only the global frame pool at [internal.h:209](src/internal.h:209). Closing that condition variable cannot wake a thread waiting on the ring condition variable.
6. Shutdown sees the parked producer in `active_submits`, times out, sets `can_close=0`, and deliberately skips ring closure at [pool.c:489](src/pool.c:489).

The producer therefore cannot return, and the required producer join before destroy becomes impossible.

This differs from restart-create failure, which explicitly closes the affected shard before quarantine. Terminal quarantine must either make every ring wait cancellation-aware or close/broadcast every reachable wait domain with a safe admission boundary.

### S2 — Abort counter fixed mechanically, but shutdown can still return `0`

Restart failure now increments `pool->shutdown_aborts` at [supervisor.c:42](src/supervisor.c:42).

However, failed worker creation restores the worker to `JOINED`, and the later shutdown owner initializes a local `aborts = 0` and returns that local value at [pool.c:570](src/pool.c:570). If it encounters no new abort, it can return `0` despite quarantine and `shutdown_aborts == 1`, contrary to the `>0 if quarantined/late` contract at [awp.h:185](include/awp/awp.h:185).

This is not the rejection reason, but the review round 7 status-reporting finding is only partially resolved.

## Re-verification

| Item | Status | Evidence |
|---|---|---|
| `mark_quarantined` closes frame pool | **PASS** | [internal.h:209](src/internal.h:209) calls close; [frame_pool.c:63](src/frame_pool.c:63) sets `closed` and broadcasts. |
| Submit rechecks after frame acquire | **PASS** | [pool.c:333](src/pool.c:333) rechecks lifecycle, quarantine, and destroy state and releases any acquired frame. |
| Restart failure increments `shutdown_aborts` | **PASS literally / PARTIAL semantically** | Increment exists; first owner shutdown can still return `0`. |
| review round 6.1: externally serialized exactly-once destroy | **PASS** | Contract is explicit at [awp.h:201](include/awp/awp.h:201); reclamation guards remain. |
| review round 6.2: no teardown with unjoined supervisor | **PASS** | Both failure paths clear `can_close`; teardown is gated at [pool.c:509](src/pool.c:509). |
| review round 6.3: restart-failure wait domains | **PASS for that path** | Shard close plus frame-pool close covers restart-create failure. |
| review round 6.4: residual drain deadline | **PASS locally** | Deadline checked before each residual pop at [pool.c:544](src/pool.c:544). |
| review round 6.5: prior items remain safe | **FAIL overall** | Bookkeeping fixes remain intact, but stall quarantine leaves the separate ring-space wait domain parked. |

## Verification evidence

The up-to-date HEAD-built suite passed once: **2,597 passed, 0 failed**. The lifecycle binary passed another 8/8 sequential repetitions locally.

An independent lane observed 2/6 lifecycle failures at the timing-based `restart counted` assertion in [test_e2e_lifecycle.c:138](tests/test_e2e_lifecycle.c:138). Neither existing test covers the decisive full-ring stall-quarantine interleaving.

Permitted residual nits remain: stale cancellation/detach wording in DESIGN, benchmark/publisher-acceptance honesty, the external destroy contract, and the non-blocking theoretical `try_push` publication-after-close race.

`★ Insight ─────────────────────────────────────`
The review round 7 fix correctly handles both possible frame-acquire outcomes after closure. The rejection comes from a different condition variable: terminal state must be visible to every place where a counted public operation can sleep.
`─────────────────────────────────────────────────`

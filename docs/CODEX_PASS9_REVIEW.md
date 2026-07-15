# Codex Pass 9 — gpt-5.6-sol xhigh

| Field | Value |
|-------|-------|
| Reviewed | `92f3b07` |
| **Verdict** | **REJECT** (shutdown quarantine left rings open) |

---

## Verdict: REJECT

HEAD `92f3b07` is clean. The exact Pass 8 stall fix is correct, and no internal UAF remains under the stated externally quiesced, exactly-once destroy contract. However, a separate contract-reachable permanent block remains.

## Summary

- Stall quarantine now closes the affected worker ring before pool quarantine, correctly waking ring-space waiters.
- Quarantine correctly closes and broadcasts the frame pool.
- Exactly-once destroy is safe under the documented external-lifetime contract.
- Shutdown-triggered quarantine can still leave ring waiters permanently parked.
- Shutdown returns positive on quarantine, but the implementation is not literally the stated `max(local, prior, quarantined ? 1 : 0)`.

## Findings

### S1 — Shutdown quarantine can permanently block ring waiters

Valid interleaving:

1. A worker becomes permanently stuck in `process()`, and its ring fills.
2. A producer increments `active_submits` and parks on the full ring at [pool.c](src/pool.c:309) and [pool.c](src/pool.c:379).
3. `awp_ring_wait_space()` observes only ring closure or available capacity: [ring.c](src/ring.c:20).
4. Shutdown enters `QUIESCING` and waits for `active_submits`: [pool.c](src/pool.c:474).
5. At the deadline it quarantines but deliberately sets `can_close = 0`, leaving rings open: [pool.c](src/pool.c:493).
6. Quarantine closes only the frame pool: [internal.h](src/internal.h:210).
7. The fallback changes worker states without closing rings, then publishes `STOPPED`: [pool.c](src/pool.c:554).

With the supported `enable_supervisor=0`, or when shutdown beats stall detection, nothing can create ring space or broadcast that condition variable. The producer cannot return, making the required producer join before destroy impossible. Idle workers can likewise remain parked in `wait_data()`.

This crosses the requested rejection boundary.

### Nit — Shutdown aggregation is not literal `max(...)`

[pool.c](src/pool.c:570) consults the prior counter only when local `aborts == 0`, then consults quarantine only if still zero. It preserves the public `>0 when quarantined/late` contract, but a nonzero local count can suppress a larger prior count.

Residual DESIGN/benchmark wording nits also remain: obsolete cancel/detach shutdown descriptions and callback-entry latency presented more strongly than the benchmark supports.

## Re-verification

| Item | Result |
|---|---|
| Stall ring closes before `mark_quarantined` | **PASS** — [supervisor.c](src/supervisor.c:123) |
| Stall ring waiter broadcast | **PASS** — [ring.c](src/ring.c:107) |
| Frame-pool waiters wake on quarantine | **PASS** — [frame_pool.c](src/frame_pool.c:63) |
| Restart-failure affected ring wakes | **PASS** — [supervisor.c](src/supervisor.c:39) |
| All terminal-quarantine ring waits wake | **FAIL — REJECT** |
| Exactly-once destroy under stated contract | **PASS** — [awp.h](include/awp/awp.h:201) |
| Internal destroy UAF under that contract | **PASS — none found** |
| Shutdown returns positive on quarantine | **PASS** |
| Shutdown implements literal `max(...)` | **FAIL — non-rejecting accounting nit** |
| No library-internal permanent block | **FAIL — REJECT** |

The timestamp-aligned HEAD binaries otherwise passed; one known `restart counted` lifecycle timing flake occurred once, followed by 8/8 clean lifecycle reruns. Existing tests do not cover the rejecting full-ring plus stuck-callback plus concurrent-shutdown interleaving. No files were changed.

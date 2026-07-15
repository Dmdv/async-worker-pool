# Design review pass 10

| Field | Value |
|-------|-------|
| Reviewed | `9dc1587` |
| **Verdict** | **REJECT** (late supervisor reopen) |

---

# Verdict: REJECT

HEAD `9dc1587` fixes the direct review round 9 waiter leak, but a supervisor restart can reopen a ring after terminal shutdown closes it. This creates a public-contract-reachable permanent block. No library-internal UAF was found under the exactly-once, externally quiesced destroy contract.

## Summary

- `active_submits` timeout now closes every worker ring.
- Other `can_close == 0` paths also close every ring.
- Ring and frame-pool broadcasts correctly wake their current waiters.
- Shutdown aggregation now implements the intended maximum.
- However, `restart_worker()` can undo the terminal ring closure after shutdown publishes `STOPPED`.
- Existing tests are green but do not exercise this interleaving.

## Findings

### S1 — Late supervisor reopen can permanently re-park a producer

A valid public-API interleaving exists:

1. A callback exceeds the stall threshold, so the supervisor requests cooperative stop. The callback recovers; the worker drains and exits while `stop` remains set ([supervisor.c](src/supervisor.c:90), [worker.c](src/worker.c:21)).
2. Producers refill the still-open ring and another producer blocks in `wait_space`, keeping `active_submits > 0` ([pool.c](src/pool.c:361)).
3. The supervisor observes `EXITED`, enters `restart_worker()`, passes its first lifecycle check, and is descheduled before `awp_ring_reopen()` ([supervisor.c](src/supervisor.c:24)).
4. Shutdown times out, closes every ring, cannot join the paused supervisor, closes every ring again, and publishes `STOPPED` ([pool.c](src/pool.c:489)).
5. The supervisor resumes and reopens the ring before its second lifecycle check. It then notices shutdown and exits without starting a consumer.
6. The producer awakened by the close can reacquire the ring mutex only after that reopen. It sees `closed == 0` and a full ring, so it waits again ([ring.c](src/ring.c:20)). No consumer, supervisor, or future close remains.

A second shutdown immediately returns because lifecycle is already `STOPPED`. The destroy contract requires joining producers first, which this permanent block makes impossible ([awp.h](include/awp/awp.h:201)).

The terminal invariant must prevent any reopen after shutdown wins, or re-close if the post-reopen lifecycle check fails.

### Nits

- No direct review round 9 regression test keeps a producer blocked through deadline shutdown. The nearest test releases the callback hang before joining the producer ([test_e2e_lifecycle.c](tests/test_e2e_lifecycle.c:232)).
- DESIGN/README/diagram shutdown descriptions still mention force-stop or cancel/detach, while implementation uses quarantine and intentional leak.
- Benchmark evidence measures callback-entry latency, not the stronger ingest-to-publish-accept wording.
- Exactly-once, externally quiesced destroy remains an explicit external contract, not a rejection item.

## Re-verification

| Item | Result |
|---|---|
| HEAD `9dc1587`, clean worktree | **PASS** |
| Active-submit timeout closes all rings | **PASS** |
| Other `can_close == 0` paths close all rings | **PASS** |
| Ring close wakes space/data waiters | **PASS** |
| Frame-pool quarantine wakes waiters | **PASS** |
| Terminal rings remain closed | **FAIL — late supervisor reopen** |
| Shutdown returns `max(local, prior, quarantine)` | **PASS** |
| Exactly-once destroy under stated contract | **PASS** |
| Library-internal UAF under that contract | **PASS — none found** |
| No library-internal permanent block | **FAIL — REJECT** |
| Direct review round 9 regression coverage | **FAIL — non-rejecting test gap** |
| HEAD-aligned functional binaries | **PASS — 2,597 assertions, zero failures** |
| Diff whitespace validation | **PASS** |

No files were changed.

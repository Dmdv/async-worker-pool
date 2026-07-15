# Codex Pass 5 — Implementation & specs review (gpt-5.6-sol · xhigh)

| Field | Value |
|-------|-------|
| Model | `gpt-5.6-sol` |
| Reasoning | `xhigh` |
| Reviewed commit | `67a7148` (Pass 4 S0/S1 fix pass) |
| Date (UTC) | 2026-07-15 |
| **Verdict** | **REJECT** |

---

## Verdict: REJECT

Reviewed clean `main` at `67a71483d365904811c38bba00aa59ec5ab9f697` (`Fix Pass 4 S0/S1 reclamation, join, supervisor ownership`).

## Executive summary

The patch fixes several local defects: STOPPED publication ordering, waiter-exit sampling, active-submitter ring closure, normal join bookkeeping, supervisor flag ownership, stall-progress rechecking, ring-prefix cleanup, and per-worker grace extension.

It does not close the reclamation proof:

- Shutdown and other APIs remain invisible before their in-object registration.
- Concurrent destroy has no single-owner protocol and can double-free.
- Supervisor restart failure leaves an open consumerless shard while submit admission continues.
- Quarantine status and the advertised hard deadline remain incomplete.
- Create rollback can still reclaim storage after an uncertain join.

All checked-in HEAD-built test binaries passed, including `test_supervisor` (31/0) and `test_e2e_lifecycle` (406/0). They do not exercise the failing interleavings. A clean rebuild was unavailable because this read-only sandbox prevented Xcode from creating its temporary compiler cache.

## Spec drift

| Specification | Live implementation |
|---|---|
| Concurrent shutdown/destroy protected by waiters ([DESIGN.md:166](docs/DESIGN.md:166)) | Only already-registered waiters are protected; pre-registration and double-destroy remain unsafe. |
| Hard shutdown deadline ([awp.h:122](include/awp/awp.h:122)) | Blocking `pthread_join()` and residual draining occur without deadline enforcement. |
| Shutdown returns `>0` for quarantine/late state ([awp.h:186](include/awp/awp.h:186)) | Callback, stall, and restart-failure quarantine can remain absent from `shutdown_aborts`. |
| Workers are created once ([DESIGN.md:7](docs/DESIGN.md:7)) | Supervisor creates replacement pthread generations. |
| Stall/shutdown use cancel, detach, force-stop ([DESIGN.md:82](docs/DESIGN.md:82)) | Implementation uses cooperative stop and permanent quarantine; no cancellation/detachment. |
| Frame lifecycle is acquire/copy/blocking-push ([DESIGN.md:62](docs/DESIGN.md:62)) | Submit repeatedly acquires, copies, try-pushes, releases, parks, and retries. |
| Hot path uses atomics instead of mutexes ([DESIGN.md:58](docs/DESIGN.md:58)) | Every successful ring operation locks and broadcasts through `awp_ring_wake_all()`. |
| Frame pool is unconditionally lock-free ([DESIGN.md:31](docs/DESIGN.md:31)) | `atomic_is_lock_free()` is recorded but neither enforced nor given a fallback. |

[review-pass-04.md](docs/review-pass-04.md:24) is correctly presented as a historical review of `4b1076c`; it should remain historical.

## Findings

### S0 — API/shutdown registration still cannot protect first access

`awp_pool_shutdown()` reads lifecycle before registering as a waiter at [pool.c:445–453](src/pool.c:445). Registration occurs later under `life_mu` at [pool.c:92–102](src/pool.c:92).

One failing interleaving:

1. Thread S enters shutdown, reads `RUNNING`, then pauses.
2. Thread D calls destroy, wins shutdown ownership, reaches STOPPED, observes no waiters/API refs, and frees the pool.
3. S resumes its lifecycle CAS against freed memory.

The same race exists after S reads `QUIESCING` but before it locks `life_mu`.

The four new API-ref paths have the same fundamental first-increment window because the counter is stored in the reclaimable object itself ([internal.h:215–225](src/internal.h:215)). The header partially acknowledges external quiescence at [awp.h:201–203](include/awp/awp.h:201), but that wording conflicts with the stronger concurrent-destroy claim in DESIGN.

### S0 — concurrent destroy can double-free

[awp_pool_destroy()](src/pool.c:546) has no destroy-owner CAS, stable control block, or external-serialization requirement specific to concurrent destroy.

Two destroyers can both:

- complete or wait for shutdown;
- observe zero waiters/API refs;
- pass quarantine/join checks;
- reclaim the same rings, mutexes, and pool.

The new “destroy after concurrent shutdown waiters” test joins all shutdown threads before calling destroy at [test_e2e_lifecycle.c:284–295](tests/test_e2e_lifecycle.c:284); it does not test concurrent reclamation.

### S1 — restart failure accepts work into a consumerless shard

The supervisor reopens the queue before starting a replacement at [supervisor.c:24–35](src/supervisor.c:24). If `pthread_create()` fails, it sets quarantine and stops the supervisor but leaves that queue open at [supervisor.c:39–45](src/supervisor.c:39).

Submit checks lifecycle and ring closure, but not quarantine or worker availability ([pool.c:299–360](src/pool.c:299)). Consequences:

- frames are accepted without a consumer;
- later submissions block when the orphaned ring fills;
- shutdown abandons the accepted backlog;
- shutdown can still return `0`.

### S1 — live supervisor does not prevent worker teardown

If the supervisor remains alive past the deadline, shutdown quarantines it at [pool.c:486–504](src/pool.c:486) but leaves `can_close == 1`. It then closes and joins workers at [pool.c:507–526](src/pool.c:507).

A supervisor already inside `restart_worker()` may simultaneously join or reopen that same worker. Concurrent `pthread_join()` calls on one target have undefined behavior, and close/reopen ownership is no longer serialized.

### S1 — the hard deadline is still not hard

- `supervisor_alive == 0` means both “not scheduled yet” and “finished.” Immediate shutdown can therefore enter blocking `pthread_join()` at [pool.c:486–494](src/pool.c:486).
- Workers publish `EXITED` immediately before returning, then shutdown uses blocking join ([worker.c:61–98](src/worker.c:61)).
- Residual frames are drained without checking `deadline_ns` at [pool.c:519–524](src/pool.c:519).

The worker grace no longer extends the deadline, but these paths still can.

### S1 — create rollback frees after an uncertain join

Partial-start cleanup records `joined` only when `pthread_join()` succeeds, but ignores failure and proceeds to destroy rings, frames, mutexes, and the pool at [pool.c:228–253](src/pool.c:228). A live thread after a failed join retains pointers into reclaimed storage.

### S1 — quarantine accounting is incomplete

`note_abort()` covers active-submit timeout, supervisor join failure/timeout, and worker deadline paths. These quarantine paths do not increment the counter:

- callback destroy: [pool.c:552–556](src/pool.c:552);
- restart failure: [supervisor.c:39–45](src/supervisor.c:39);
- asynchronous stall quarantine: [supervisor.c:117–125](src/supervisor.c:117).

A later shutdown can report `0` even though destroy intentionally leaks.

### S2 findings

- Partial broadcast-feed copy leaks already duplicated strings because `n_broadcast_feeds` remains zero until the entire loop succeeds ([pool.c:34–65](src/pool.c:34)).
- Over-aligned workers and cells can fall back to ordinary `calloc()`, which need not satisfy the required 64-byte extended alignment ([internal.h:88](src/internal.h:88), [pool.c:178](src/pool.c:178), [ring.c:40–58](src/ring.c:40)).
- Frame-pool safety remains conditional on a finite 32-bit ABA tag and assumed lock-free 64-bit atomics; the recorded `lock_free_ok` is unused ([frame_pool.c:6–38](src/frame_pool.c:6)).

### S3 — unsupported flag bits are accepted

The API says flags are `AWP_FRAME_*` or zero, but submit never rejects unknown bits and copies them into the callback frame ([pool.c:280–333](src/pool.c:280)).

## Pass 4 item re-verification

| # | Claimed fix | Status |
|---:|---|---|
| 1 | Lifecycle publication/waiting under `life_mu` | **PARTIAL** — STOPPED publication and registered-waiter exit are fixed; waiter entry and double-destroy are not. |
| 2 | Waiter samples aborts before leaving | **FIXED statically** |
| 3 | `api_refs` on submit/metrics/drops/shard | **PARTIAL** — present, but cannot seal first access; shutdown/destroy are uncounted. |
| 4 | Do not close rings after active-submit timeout | **FIXED narrowly** — internal close/publish race removed; resulting quarantine remains non-terminal and untested. |
| 5 | Set `joined` only after successful join | **PARTIAL** — helpers fixed; create rollback still reclaims after join failure. |
| 6 | Supervisor joined ownership and `started` flag | **PARTIAL** — flag ownership fixed; startup-state ambiguity and live-supervisor teardown remain. |
| 7 | Recheck stall progress | **FIXED statically** |
| 8 | Destroy only `rings_ok` prefix | **FIXED statically** |
| 9 | Cap join grace by absolute deadline | **PARTIAL** — worker grace fixed; blocking joins/residual drain remain outside the bound. |
| 10 | Abort counter for quarantine paths | **PARTIAL** — several asynchronous/pre-existing quarantine paths remain uncounted. |

## Test gaps

- Barrier-controlled shutdown/destroy at lifecycle-read and waiter-registration boundaries.
- Concurrent and double destroy.
- API caller paused immediately before `awp_api_enter()`.
- Active-submitter-only timeout: status, open-ring behavior, accepted-frame completion, and callback lifetime.
- Assert all concurrent shutdown waiters inherit the same abort result; current helper records only `rc >= 0`.
- Immediate supervisor-enabled create→shutdown.
- Supervisor timeout while inside join/reopen.
- Injected worker/supervisor `pthread_create()` and `pthread_join()` failures.
- Recovered stall under sustained backlog; existing tests deliberately avoid the stall path.
- Ring-init and partial broadcast-copy allocation failure.
- Exact deadline test with multiple stuck workers; the 400 ms test currently allows 3000 ms.
- Sanitizer/Linux qualification, extended-alignment checks, ABA wrap, and non-lock-free atomics.

## Top 5 actions

1. Introduce a stable lifetime/control block with whole-call acquisition and a single destroy owner, or explicitly require complete external API quiescence and serialize destroy.
2. Make quarantine an admission state: close/reject failed shards immediately and return typed degraded/quarantine status.
3. Give supervisor and workers explicit STARTING/RUNNING/EXITED/JOINED ownership states; never tear down workers while the supervisor is unjoined.
4. Enforce one absolute deadline across polling, joins, and residual work; after expiry, quarantine without blocking operations.
5. Add deterministic barrier/fault-injection tests, then update DESIGN and the public contract to match the chosen lifetime model.

`★ Insight ─────────────────────────────────────`
The patch repaired bookkeeping after registration, but safe reclamation requires controlling who may begin registration. That ownership boundary—not another counter inside `awp_pool_t`—is the remaining architectural issue.
`─────────────────────────────────────────────────`

# Design review pass 6

| Field | Value |
|-------|-------|
| Tooling | Independent external review |

| Reviewed commit | `0436973` |
| **Verdict** | **REJECT** |

---

### Verdict: REJECT

Reviewed clean `main` at `0436973`:

```text
0436973 fix: Pass 5 residual — single-owner destroy, quarantine admission
67a7148 Fix Pass 4 S0/S1 reclamation, join, supervisor ownership
a6d3616 docs: review external high-depth review Pass 4 review (REJECT)
```

### Executive summary

Pass 5’s local lifetime repairs remain intact: lifecycle publication is under `life_mu`, registered shutdown waiters are protected through result sampling, rings are not closed while `active_submits` remain, and join state is recorded only after successful joins. Create rollback now safely leaks after an uncertain join.

However, HEAD is not ready for `ACCEPT WITH NITS`:

- The public contract promises concurrent/double destroy safety, but an in-object CAS cannot protect a destroyer that reaches the CAS after another destroyer has reclaimed the object. As documented, this remains S0.
- An unjoined supervisor can still join/reopen a worker while shutdown closes/joins/drains it. This is a library-internal S1 race independent of external quiescence.
- Quarantine is checked only before the submit retry loop; an overlapping submit can still enqueue into the consumerless shard or wait forever after restart failure.

Under a corrected contract requiring exactly one externally serialized destroy after every possible caller has finished, the architectural first-access limitation itself would be acceptable as a documented boundary. Even under that contract, the supervisor race prevents acceptance at this HEAD.

### Findings with severity

#### S0 — The advertised concurrent/double-destroy guarantee still permits UAF

The header promises that concurrent/double destroy is a no-op at [awp.h:201](include/awp/awp.h:201). Ownership is elected using an in-object CAS at [pool.c:572](src/pool.c:572), after which the winner can free the object at [pool.c:618](src/pool.c:618).

Failing interleaving:

1. D2 enters `awp_pool_destroy()` and pauses immediately before the CAS.
2. D1 wins the CAS, observes a reclaimable pool, and frees it.
3. D2 resumes and executes the CAS against freed storage.

A sequential second destroy is likewise necessarily a dangling-pointer access. The CAS prevents two owners only when every contender reaches it while storage remains live.

The external-quiescence language at [awp.h:204](include/awp/awp.h:204) does not resolve the contradiction: it says threads that have “entered” an API are distinguished, but shutdown has no whole-call registration. A shutdown caller can pause between its lifecycle load and CAS at [pool.c:457](src/pool.c:457) and still be invisible to both `api_refs` and `shutdown_waiters`.

Judgment on item 4: the first-access race is `ACCEPT WITH NITS` only under a stricter contract stating:

> Destroy exactly once, externally serialized, only after joining every thread that can hold, use, or begin using the handle.

That is not the contract currently advertised.

#### S1 — Shutdown manipulates workers while the supervisor remains unjoined

When the supervisor remains alive past the deadline, shutdown quarantines the pool but leaves `can_close == 1` at [pool.c:498](src/pool.c:498). It then closes, joins, and drains workers at [pool.c:519](src/pool.c:519).

Meanwhile, `restart_worker()` independently joins and reopens the same worker at [supervisor.c:24](src/supervisor.c:24).

One concrete deadlock:

1. Supervisor passes its initial lifecycle check inside `restart_worker()`.
2. Shutdown sets `DRAINING`, reaches its deadline, and continues worker teardown despite the live supervisor.
3. Shutdown closes the worker queue.
4. Supervisor finishes its join and reopens the queue, then declines to start a replacement after its second lifecycle check.
5. Shutdown sees the worker joined and calls residual `awp_ring_pop()`.
6. The queue is empty but open, so `awp_ring_pop()` waits indefinitely.

Shutdown and supervisor can also concurrently call `pthread_join()` on the same worker, which is undefined. Quarantine prevents subsequent reclamation, so this does not become an internal UAF, but bounded shutdown and join ownership are broken.

#### S1 — Quarantine admission remains a check-then-act race

`awp_submit()` checks `quarantined` and `destroy_started` once at [pool.c:309](src/pool.c:309). Its retry loop at [pool.c:324](src/pool.c:324) rechecks only lifecycle.

The supervisor reopens the queue before replacement creation at [supervisor.c:30](src/supervisor.c:30). If creation fails, it quarantines the pool but leaves that queue open at [supervisor.c:39](src/supervisor.c:39).

A submit that passed the initial check can therefore:

- enqueue after quarantine and return `0` with no consumer; or
- park indefinitely when the consumerless queue fills.

New calls are rejected correctly, but the original consumerless-shard defect is narrowed rather than fully closed.

#### S1 — The advertised hard deadline remains unenforced globally

The public configuration calls this a hard deadline at [awp.h:122](include/awp/awp.h:122), but shutdown can exceed it through:

- blocking `pthread_join()` after observing `supervisor_alive == 0`, which also means “not scheduled yet,” at [pool.c:498](src/pool.c:498);
- blocking worker join after `EXITED` is published immediately before actual thread return at [worker.c:61](src/worker.c:61);
- residual draining without deadline checks at [pool.c:531](src/pool.c:531);
- the supervisor reopen/deadlock interleaving above.

The worker grace cap is locally fixed, but it does not make the whole shutdown operation hard-bounded.

#### S2 — Pool-level quarantine can still yield shutdown return `0`

Restart failure marks `pool->quarantined` but does not increment `shutdown_aborts`. Because failed `awp_worker_start()` restores the worker to `JOINED`, a later shutdown can join everything and return `0` despite permanent quarantine and intentional leakage. A destroy-from-callback that completes before shutdown can produce the same mismatch.

This conflicts with the return contract at [awp.h:185](include/awp/awp.h:185).

#### S2 — Specifications and tests remain materially behind the implementation

[DESIGN.md:80](docs/DESIGN.md:80) still describes cancel/detach/force-stop behavior, while [worker.c:10](src/worker.c:10) explicitly disables cancellation and implements quarantine/leak instead.

Commit `0436973` adds no tests. The lifecycle test joins every shutdown thread before the sole destroy call at [test_e2e_lifecycle.c:284](tests/test_e2e_lifecycle.c:284), so it cannot exercise concurrent reclamation. There is also no restart-create failure, join failure, active-submit timeout, or direct quarantine-admission fault injection.

### Prior themes re-verification

| Theme | Status |
|---|---|
| `destroy_started` single owner | **PARTIAL** — one live-object owner; concurrent/double caller lifetime remains unsafe |
| Quarantine gates submit | **PARTIAL** — new calls reject; overlapping/retrying calls can still publish or block |
| `set_life` under `life_mu` | **FIXED** |
| Waiter result sampling and wakeup | **FIXED after registration**; pre-registration remains externally constrained |
| `api_refs` on ordinary public APIs | **FIXED after entry**; first increment remains contract-bound |
| No ring close with `active_submits` | **FIXED** |
| `joined` only after successful join | **FIXED locally** in worker, supervisor, and rollback paths |
| Create rollback after join failure | **FIXED safely** through intentional leak |
| Supervisor joined/start flag ownership | **Local flag fix holds**; exclusive worker-teardown ownership remains broken |
| Stall-progress recheck | **FIXED statically** |
| Destroy only initialized ring prefix | **FIXED statically** |
| Worker grace bounded by absolute deadline | **FIXED locally**; whole shutdown remains unbounded |
| Quarantine abort accounting | **PARTIAL** |

### Verification

All existing HEAD-timestamped binaries passed:

- unit: 824/0
- unit modes: 892/0
- ring modes: 424/0
- supervisor: 31/0
- e2e: 4/0
- e2e modes: 16/0
- e2e lifecycle: 406/0

A clean rebuild was unavailable because the read-only environment prevented Xcode from creating its temporary cache. The passing suite is functional smoke evidence, not adversarial lifetime proof.

### Top 5

1. Either remove the concurrent/double-destroy promise and require exactly-once externally serialized destroy, or introduce a stable out-of-object lifetime control block.
2. If the supervisor is not joined, set `can_close = 0` and perform no worker close/join/drain operations it may still race.
3. Close and wake a shard on restart failure, and define/enforce quarantine at the submit commit point or explicitly document overlapping-submit semantics.
4. Enforce one absolute deadline across supervisor join, worker join, and residual draining.
5. Add deterministic barriers and fault injection for concurrent destroy, pre-registration shutdown, restart `pthread_create` failure, join failure, active-submit timeout, and supervisor reopen during shutdown.

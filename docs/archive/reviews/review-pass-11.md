# Design review pass 11

| Field | Value |
|-------|-------|
| Reviewed | `6c9796a` |
| **Verdict** | **ACCEPT WITH NITS** |

---

## Verdict: ACCEPT WITH NITS

### Summary

review-round 10’s rejecting race is fixed at HEAD `6c9796a`.

`restart_worker()` now:

- Rechecks lifecycle after joining and before reopening the ring.
- Re-closes the ring if shutdown wins after reopen.
- Leaves the remaining start window protected by supervisor join or sticky quarantine.

No library-internal UAF or contract-valid permanent block was found under the documented exactly-once, externally quiesced destroy contract.

### Findings

No rejecting findings.

- **Nit — missing deterministic review round 10 regression test.** Existing tests cover restart progress and blocked submission, but do not barrier-control shutdown immediately before/after `awp_ring_reopen()` ([test_e2e_lifecycle.c:101](tests/test_e2e_lifecycle.c:101), [test_e2e_lifecycle.c:145](tests/test_e2e_lifecycle.c:145), [test_e2e_lifecycle.c:211](tests/test_e2e_lifecycle.c:211)).

- **Nit — stale lifecycle documentation.** DESIGN/README/diagram still mention cancellation, detach, or force-stop, while the implementation quarantines and intentionally leaks ([DESIGN.md:86](docs/DESIGN.md:86), [README.md:77](README.md:77), [DIAGRAMS.md:146](docs/DIAGRAMS.md:146)).

- **Nit — benchmark claim mismatch.** README claims ingest-to-publish-accept latency, but measurement ends at callback entry before simulated publishing ([README.md:5](README.md:5), [bench_dispatch.c:30](bench/bench_dispatch.c:30)).

- **Accepted external boundary.** Destroy remains exactly-once and externally quiesced by contract ([awp.h:201](include/awp/awp.h:201)); per guidance, this is not a rejection item.

### Re-verification

| Item | Result |
|---|---|
| HEAD `6c9796a`, clean worktree | **PASS** |
| Lifecycle checked after join and before reopen | **PASS** — [supervisor.c:24](src/supervisor.c:24) |
| Failed pre-reopen check keeps ring closed | **PASS** — [supervisor.c:30](src/supervisor.c:30) |
| Shutdown after reopen triggers re-close | **PASS** — [supervisor.c:35](src/supervisor.c:35) |
| Ring close wakes parked producers | **PASS** — [ring.c:20](src/ring.c:20), [ring.c:107](src/ring.c:107) |
| Race after final lifecycle check remains safe | **PASS** — supervisor join or quarantine/terminal close at [pool.c:508](src/pool.c:508) |
| `STOPPED` published after terminal closure work | **PASS** — [pool.c:569](src/pool.c:569) |
| Unjoined supervisor cannot lead to reclamation | **PASS** — [pool.c:625](src/pool.c:625) |
| Library-internal UAF | **PASS — none found** |
| Contract-valid permanent block | **PASS — none found** |
| Direct review round 10 regression coverage | **NIT — absent** |
| Functional binaries | **PASS — 2,597 assertions, 0 failures** |
| Focused supervisor/lifecycle repetitions | **PASS — 5/5 each** |
| Diff whitespace validation | **PASS** |
| Read-only execution | **PASS — no files changed** |

A clean rebuild was intentionally not performed in the read-only sandbox; the relevant checked-in binaries were confirmed up-to-date against the patched source.

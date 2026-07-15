# Design review pass 12

| Field | Value |
|-------|-------|
| Reviewed | `eb19f04` (Pass 12 + follow-up); post-review `eef90f5` aligned `bench_all_modes` |
| Prior | Pass 11 ACCEPT WITH NITS |
| **Verdict** | **ACCEPT WITH NITS** (all listed Pass 12 nits cleared; remaining bar is external destroy contract only) |

---

## Verdict

**ACCEPT WITH NITS**

All four Pass 12 nits are cleared at clean HEAD `eb19f04974d628fb8cad9abc3f6b689c737ef598`. No new library-internal UAF or deadlock was found under the documented lifetime contract.

## Summary

The quarantine test now keeps callback state alive safely, the reopen regression exercises the production helper, and the supervisor PNG/documentation match current quarantine behavior. One trivial benchmark-description inconsistency remains; it does not affect library correctness.

## Findings

- **Nit — `bench_all_modes` does not match the shared benchmark note.** [BENCHMARKS.md:47](docs/BENCHMARKS.md:47) says latency includes simulated work through process return, but [bench_all_modes.c:43](bench/bench_all_modes.c:43) samples latency and signals completion before the simulated work at line 48. Scope that note to `bench_dispatch`, or move the all-modes sample after the work.

## Re-verification

| Pass 12 nit | Result | Evidence |
|---|---|---|
| Quarantine test lifetime | **Cleared** | Static atomic callback state at [test_e2e_lifecycle.c:342](tests/test_e2e_lifecycle.c:342); no `ctx`; destroy precedes hang clear at [lines 380–381](tests/test_e2e_lifecycle.c:380). Quarantined destroy refuses reclamation at [pool.c:625](src/pool.c:625). |
| Production reopen helper | **Cleared** | Helper implemented at [supervisor.c:14](src/supervisor.c:14), used by `restart_worker()` at [line 56](src/supervisor.c:56), and called directly by the regression at [test_e2e_lifecycle.c:333](tests/test_e2e_lifecycle.c:333). |
| Supervisor PNG | **Cleared** | [05-supervisor.png](docs/diagrams/05-supervisor.png) was regenerated and visually shows cooperative stop, quarantine, guarded reopen/re-close, and no cancel/detach action. It matches [DIAGRAMS.md:137](docs/DIAGRAMS.md:137). |
| Benchmark wording | **Cleared for `bench_dispatch`** | Correct scope appears in [README.md:5](README.md:5), [DESIGN.md:10](docs/DESIGN.md:10), and [BENCHMARKS.md:47](docs/BENCHMARKS.md:47). `bench_dispatch` samples after simulated work and labels itself closed-loop. See the residual nit above. |

Verification on the current HEAD-built binaries:

- Lifecycle: **421 passed, 0 failed**
- Supervisor: **31 passed, 0 failed**
- Unit: **824 passed, 0 failed**
- E2E: **4 passed, 0 failed**
- Dispatch benchmark: p99 **0.307 ms**, zero drops, PASS
- `git diff --check`: clean
- Worktree: clean

A fresh rebuild was unavailable because the read-only sandbox denied Xcode’s temporary cache write; the exercised binaries were newer than the changed sources and contained the new helper symbol.

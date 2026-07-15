# Design review pass 4

| Field | Value |
|-------|-------|
| Tooling | Independent external review |

| Reviewed commit | `4b1076c` (Pass 3 S0 fix pass + adversarial tests) |
| Date (UTC) | 2026-07-15 |
| Prior passes | 1 estimate · 2 analysis REJECT · 3 impl review REJECT · fix `4b1076c` |
| This pass | Post-fix re-review of code + specs |
| **Verdict** | **REJECT** |

Raw log (local): `(local review log; not in repo)`

---

### Verdict

**REJECT**

### Executive summary

I reviewed clean HEAD `4b1076c6c3c448475647bad2d060acb1b16a8554`. Pass 3’s sticky worker/supervisor quarantine, callback TLS coverage, release-before-park submit path, queue reopen, and no-cancel policy are real improvements. However, concurrent shutdown/destroy remains S0-unsafe: shutdown publishes `STOPPED` before it finishes using the pool’s mutex, allowing destroy to reclaim that mutex and pool underneath it. Submitters and shutdown waiters also remain invisible before their in-object counters are incremented, so the claimed concurrent lifetime protection is incomplete. The steady-state SPSC/MPSC/SPMC/MPMC sequence protocol appears locally correct under its declared cardinalities; I found no payload memory-order defect or classic condition-variable lost wakeup. Additional S1 issues remain in timeout delivery semantics, termination proof, supervisor ownership, create rollback, and shutdown status/deadline reporting. All existing HEAD-built test binaries passed, but several adversarial tests do not exercise what their names claim, and raw-ring tests can miss duplicate-plus-loss. The benchmark still excludes front-half/backpressure latency and ends before publishing, so it does not qualify the 5 ms ingest-to-publish-accept target.

Executed binaries:

- `test_unit`: 824/824
- `test_unit_modes`: 892/892
- `test_ring_modes`: 424/424
- `test_supervisor`: 30/30
- `test_e2e`: 4/4
- `test_e2e_modes`: 16/16
- `test_e2e_lifecycle`: 403/403

The binaries are newer than the reviewed sources. A fresh build/sanitizer run was blocked because Apple tooling could not create its `/tmp/xcrun_db-*` cache in the read-only sandbox.

### Spec vs implementation drift

| Claim | Code reality | Severity |
|---|---|---:|
| Concurrent shutdown/destroy is fixed | `STOPPED` is stored before `set_life()` locks/broadcasts on `life_mu`; destroy can free the pool in that interval. Waiter registration/exit also has uncovered windows. | **S0** |
| `active_submits` prevents free under live submitters | Submission validates strings before registration; other public pool readers are entirely untracked. In-object counters cannot protect callers paused before their increment. | **S0** |
| Shutdown returns `>0` when quarantined/late | Active-submitter and supervisor timeouts set `quarantined` without incrementing the returned abort count. | **S1** |
| Hard shutdown deadline | Each late worker receives a new 200 ms grace after the shared deadline; destroy may additionally wait about 30 seconds. | **S1** |
| Accepted work drains on shutdown | After submit-quiescence timeout, a producer can publish after ring close, return success, and have its frame abandoned or stranded. | **S1** |
| p99 ingest→process / publish-accept | Timestamping occurs after frame acquisition/copy and is reset after every failed push. Sampling ends at callback entry. | **S1** |
| Benchmark qualifies 1–5k msg/s target | Workload is an unpaced roughly 300k–585k msg/s burst, not open-loop 1–5k traffic with sparse wakeups or downstream acceptance. | **S1** |
| Benchmark represents `4b1076c` | `BENCHMARKS.md` predates the submit-path rewrite in `4b1076c`. | **S2** |
| Frame lifecycle is acquire → block on push | Current code repeatedly acquires/copies/tries, releases on failure, parks, and retries. | **S2** |
| Stall/deadline uses cancel/detach or force-stop | Current code uses neither; it cooperatively stops or permanently leaks a quarantined pool. | **S2** |
| Frame pool / hot path is lock-free | Lock freedom is only recorded, not required. Every push/pop broadcasts while holding `wait_mu`; every frame release also takes a mutex. | **S2** |
| SPMC/MPMC offer public multi-consumer operation | The pool always has one consumer per ring. True multi-consumer examples include private `src/internal.h`; no public raw-ring API exists. | **S2** |
| `AWP_ENABLED` is a runtime gate | The header correctly calls it caller-owned, but README/DESIGN still call it a gate; creation never checks it. | **S2** |
| N workers are created once | Worker slots are fixed, but replacement pthread generations can be created by the supervisor. | **S3** |

### Residual / new findings

1. **Concurrent shutdown/destroy still permits UAF — S0**

   - **Evidence:** [`set_life()`](src/pool.c:69) stores the new lifecycle at line 71, then accesses `life_mu` at line 72. Destroy trusts `STOPPED` at [`awp_pool_destroy()`](src/pool.c:513) and frees the synchronization objects at line 545.
   - **Failure mode:** Shutdown stores `STOPPED` and is preempted before locking `life_mu`. Concurrent destroy observes `STOPPED`, sees no waiters, destroys the mutex/CV, and frees the pool. Shutdown resumes into freed memory.
   - A waiter also decrements `shutdown_waiters` at [`wait_until_stopped()`](src/pool.c:94) before its caller’s final `shutdown_aborts` load at line 418. Destroy can free between those operations.
   - **Fix:** Publish reclaimability only after the shutdown owner has completed all pool accesses. If destroy must be concurrent, use a stable out-of-object control block with whole-call references and single shutdown/destroy ownership. The simpler contract is to require destroy only after all API-caller threads have been joined.

2. **Pre-registration callers remain invisible to destroy — S0**

   - **Evidence:** [`awp_submit()`](src/pool.c:240) validates and calls `strlen()` at lines 259–270 before incrementing `active_submits` at line 276. Metrics, drops, and sharding helpers are untracked at [`awp_pool_get_metrics()`](src/pool.c:355), [`awp_pool_drops()`](src/pool.c:392), and [`awp_shard_of()`](src/shard.c:69).
   - **Failure mode:** A submitter paused before registration—or an untracked reader already executing—can resume after concurrent destroy and access freed pool storage.
   - **Fix:** Either explicitly prohibit destroy from overlapping any public operation and require external quiescence, or redesign around a stable handle/reference mechanism. Moving the counter earlier inside the same raw-pointer object cannot eliminate the first-access race.

3. **Submit timeout can publish after ring close and return success — S1**

   - **Evidence:** Shutdown times out and closes rings at [`awp_pool_shutdown()`](src/pool.c:438). `try_push_mp()` checks `closed` at [`ring.c:274`](src/ring.c:274), then reserves and publishes later at line 281 without a post-reservation close check.
   - **Failure mode:** A counted producer passes the close check and pauses. Shutdown times out, quarantines, closes the ring, and the worker exits closed+empty. The producer resumes, publishes, and [`awp_submit()`](src/pool.c:314) returns `0`. Shutdown may count the frame as abandoned or miss it after residual draining, leaving it stranded.
   - **Fix:** If submit quiescence misses the deadline, do not close/drain the referenced rings. Return an explicit timed-out/non-reclaimable state and allow later quiescence completion, or wait for all registered submissions before closing.

4. **Join failure is treated as proof of termination — S1**

   - **Evidence:** [`awp_worker_join_deadline()`](src/worker.c:113) sets `joined` before `pthread_join()` and sets state `JOINED` even if join fails. Shutdown ignores negative results at [`pool.c:477`](src/pool.c:477). Supervisor restart ignores join results at [`supervisor.c:13`](src/supervisor.c:13).
   - **Failure mode:** A join error leaves termination unproven, but destroy can still free storage or the supervisor can create a replacement consumer.
   - **Fix:** Set `joined=1` only after `pthread_join()` succeeds. Any join error must set sticky quarantine, prevent restart/reclamation, and be surfaced in status.

5. **Supervisor ownership remains incomplete — S1**

   - **Evidence:** The supervisor writes `supervisor_joined=0` on entry at [`supervisor.c:43`](src/supervisor.c:43), while shutdown can set it before joining at [`pool.c:454`](src/pool.c:454).
   - **Failure mode:** Immediate shutdown can observe `supervisor_alive=0`, set joined to one, then block in `pthread_join()`. The newly scheduled supervisor overwrites joined back to zero and exits. Join succeeds, but destroy later sees zero and permanently leaks the pool.
   - **Fix:** Only the creator/joiner should own the joined flag; store one after a successful join.

6. **Stall recovery can falsely quarantine a healthy draining worker — S1**

   - **Evidence:** Stop is honored only when queue depth reaches zero at [`worker.c:21`](src/worker.c:21). The supervisor waits one grace interval and quarantines any still-`RUNNING` worker at [`supervisor.c:75`](src/supervisor.c:75).
   - **Failure mode:** A callback recovers and resumes processing, but continuous backlog prevents depth reaching zero before grace. The supervisor marks a progressing worker and pool quarantined, causing a permanent leak/degraded shard.
   - A replacement `pthread_create()` failure is also unsafe operationally: [`restart_worker()`](src/supervisor.c:16) has already reopened the queue, but lifecycle remains `RUNNING`; producers can fill the consumerless shard and block indefinitely.
   - **Fix:** Use a distinct between-frame restart request, re-evaluate forward progress during grace, and transition admission to an explicit degraded/rejected state after restart failure.

7. **Shutdown deadline and status are not truthful — S1**

   - **Evidence:** Every late worker gets `now + 200 ms` at [`worker.c:127`](src/worker.c:127), called serially at [`pool.c:475`](src/pool.c:475). Destroy waits up to roughly 30 seconds at [`pool.c:519`](src/pool.c:519).
   - Active-submitter and supervisor timeouts only mark quarantine at [`pool.c:442`](src/pool.c:442) and [`pool.c:463`](src/pool.c:463); the returned counter is updated only for workers.
   - **Failure mode:** Thirty-two stuck workers can add about 6.4 seconds after the configured deadline. Shutdown can return `0` even though subsequent destroy intentionally leaks the entire pool.
   - **Fix:** Use one absolute deadline throughout and return a status bitmask/query covering worker, submitter, supervisor, and callback quarantine. Document the lifetime required for `cfg.user` when a callback remains live.

8. **Partial-create rollback destroys invalid pthread objects — S1**

   - **Evidence:** Ring initialization failure jumps from [`pool.c:182`](src/pool.c:182) to cleanup of all configured rings at line 223. The failed ring may already have destroyed its own mutex/CV at [`ring.c:83`](src/ring.c:83).
   - **Failure mode:** Resource-exhaustion or injected initialization failure causes double-destroy of the failed ring and destruction of never-initialized later rings.
   - **Fix:** Track the successfully initialized prefix and destroy only that prefix.

9. **The retained latency PASS does not measure the claimed SLA — S1**

   - **Evidence:** Frame acquisition/copy occurs at [`pool.c:296`](src/pool.c:296); `submit_ns` is assigned at line 311 inside the retry loop. Failed pushes release/wait/retry at line 314, resetting the timestamp. [`bench_process()`](bench/bench_dispatch.c:30) samples before its simulated publish work.
   - **Failure mode:** Frame-pool blocking, queue backpressure, copying, retry delay, and publisher acceptance are omitted. The burst workload does not exercise 1–5k msg/s sparse-wakeup tails or coordinated omission.
   - **Fix:** Timestamp immediately before entering `awp_submit`, retain that timestamp across retries, complete after real publisher-buffer acceptance, and use repeated open-loop 1k/5k workloads with skew, bursts, downstream delay, and confidence bounds.

10. **Over-aligned allocations are formally non-portable — S2**

   - **Evidence:** `awp_cell_t` and `awp_ring_t` require 64-byte alignment at [`internal.h:88`](src/internal.h:88). Cell allocation falls back to ordinary `calloc()` at [`ring.c:51`](src/ring.c:51); worker arrays containing aligned rings use `calloc()` at [`pool.c:160`](src/pool.c:160).
   - **Failure mode:** Ordinary allocation need not satisfy extended 64-byte alignment, making accesses formally undefined and invalidating cache-line isolation on some implementations.
   - **Fix:** Use checked aligned allocation for both cells and worker arrays; do not fall back to ordinary `calloc` for over-aligned types.

11. **Freelist safety/progress claims remain conditional — S2**

   - **Evidence:** The head packs a 32-bit tag at [`frame_pool.c:6`](src/frame_pool.c:6). Double release detects only the current head at [`frame_pool.c:145`](src/frame_pool.c:145). `lock_free_ok` is recorded at line 36 but never enforced.
   - **Failure mode:** At 5k messages/s, the tag wraps after roughly five days of acquire/release mutations; exploitation requires a CAS participant stalled across that full interval, so this is possible but low probability. A non-head double release can immediately create a freelist cycle, though no valid current path should double-release.
   - **Fix:** Prefer a mutex-backed fallback or redesign without finite-tag ABA exposure. Add per-frame ownership state in debug/testing builds and reject unsupported non-lock-free platforms if lock freedom is required.

12. **32-bit counter-wrap behavior is undefined — S2**

   - **Evidence:** Ring comparisons cast to `intptr_t` and subtract at [`ring.c:144`](src/ring.c:144) and analogous push/pop sites. [`awp_ring_depth()`](src/ring.c:420) treats `enqueue_pos < dequeue_pos` as empty.
   - **Failure mode:** On 32-bit targets, signed subtraction can overflow near `2^31`; full wrap can make live backlog appear empty. At 5k operations/s the signed boundary is operationally reachable in roughly five days.
   - **Fix:** Declare and enforce 64-bit-only support with a static assertion, or implement defined modular comparisons/reset handling.

13. **Mode and hot-path API claims exceed the supported surface — S2**

   - **Evidence:** The public header admits the pool is single-consumer at [`awp.h:99`](include/awp/awp.h:99), but refers to raw rings that exist only in [`internal.h:86`](src/internal.h:86). True SPMC/MPMC examples include the private header.
   - Every successful operation invokes [`awp_ring_wake_all()`](src/ring.c:11), which locks and broadcasts.
   - **Failure mode:** Installed users cannot use the topology that justifies SPMC/MPMC, and “atomics instead of mutex” obscures hot-path serialization/wake storms.
   - **Fix:** Either publish a supported raw-ring API or keep SPMC/MPMC internal. Describe synchronization as atomic reservation plus mutex/condvar notification.

14. **Unsupported frame-flag bits are silently accepted — S3**

   - **Evidence:** The header restricts flags to `AWP_FRAME_*` at [`awp.h:159`](include/awp/awp.h:159), but [`awp_submit()`](src/pool.c:240) never validates unknown bits.
   - **Failure mode:** Unsupported bits pass through to callbacks, making future compatibility semantics ambiguous.
   - **Fix:** Reject `flags & ~AWP_FRAME_BROADCAST` with `-EINVAL`, or explicitly document unknown-bit pass-through.

### Prior S0 re-verification — Pass 3 themes

| Issue | Status | Evidence |
|---|---|---|
| Free while submitters active | **PARTIAL** | Registered submitters now force sticky quarantine after timeout, but pre-registration and raw-handle lifetime remain open. |
| Shutdown quarantine sticky for destroy | **FIXED** | Worker, supervisor, active-submit, and callback paths mark `pool->quarantined`; destroy checks it. |
| Concurrent shutdown/destroy | **STILL OPEN** | `STOPPED` publication, waiter registration, and waiter-exit windows still permit reclamation under live shutdown callers. |
| Callback reentrancy | **FIXED** statically | TLS spans `process` and `on_error`; callback destroy marks quarantine. `on_error` shutdown/destroy tests are missing. |
| Hot-shard holds frames | **FIXED** in code | Failed `try_push` releases its frame before park/retry. The new test does not reproduce the original producer-herd race. |
| Supervisor ownership | **PARTIAL** | Interruptible polling, lifecycle recheck, and restart-failure quarantine landed; joined ownership and recovered-backlog handling remain flawed. |
| Supervisor not joined past deadline | **FIXED** for UAF | A live unjoined supervisor forces quarantine/leak. Immediate startup can instead produce a false permanent leak. |

### Pass 2 table

| Pass 2 issue | Status | Current evidence |
|---|---|---|
| AWP-01 submitter/reclamation race | **PARTIAL** | Registered path improved; finding 2 remains. |
| AWP-02 exit without drain | **FIXED** | Worker drains until closed+empty at [`worker.c:16`](src/worker.c:16). |
| AWP-03 detach UAF | **FIXED** | No detach; sticky quarantine prevents reclamation. |
| AWP-04 cancellation inside callback | **FIXED** | Worker disables cancellation at [`worker.c:10`](src/worker.c:10); no cancel path remains. |
| AWP-05 bounded shutdown | **PARTIAL** | Unsafe force-stop removed, but deadline/status findings remain. |
| AWP-06 restart destroys queue | **FIXED** in code | [`awp_ring_reopen()`](src/supervisor.c:16) preserves cells/backlog. |
| AWP-07 concurrent shutdown | **PARTIAL** | Waiters block for `STOPPED`, but destroy synchronization remains unsafe. |
| AWP-08 STARTING mistaken for exited | **FIXED** | Explicit STARTING/RUNNING/EXITED/JOINED states exist. |
| AWP-09 hot shard retains frames | **FIXED** in code | Frame released before parking; cross-shard regression missing. |
| AWP-10 indefinite spin | **FIXED** for normal waits | Bounded spin then condition-variable park. |
| AWP-11 truncation/null payload | **FIXED** | `-E2BIG`/`-EINVAL` validation at [`pool.c:263`](src/pool.c:263). |
| AWP-12 metrics/drop accounting | **PARTIAL** | Residual abandonment is counted; quarantined backlog and shutdown status remain incomplete. |
| AWP-13 callback reentrancy | **FIXED** | Process/on-error submit/shutdown are rejected; destroy quarantines. |
| AWP-14 capacity/enum validation | **FIXED** for configured capacity | Maximum capacity and unsigned enum validation are present; long-term 32-bit position wrap remains separate. |
| AWP-15 FIFO contract/tests | **PARTIAL** | Header now says reservation-linearization order; E2E order coverage remains largely vacuous. |
| AWP-16 mode API/topology | **PARTIAL** | Header discloses pool single-consumer behavior, but true raw-ring API remains private. |
| AWP-17 finite ABA/lock freedom | **STILL OPEN** | Finding 11. |
| AWP-18 SLA evidence | **STILL OPEN** | Finding 9. |
| AWP-19 broadcast all-workers/truncation | **FIXED** | All-workers broadcast config and oversize labels are rejected. |
| AWP-20 qualification | **PARTIAL** | More lifecycle tests landed; exact-ID, fault injection, sanitizers, and Linux CI remain absent. |
| AWP-21 `on_error` contract | **FIXED** | Public header now says it is called only on nonzero `process()` return. |

### Test matrix gaps

- Barrier-controlled destroy races at:
  - before `active_submits++`;
  - after lifecycle read but before `shutdown_waiters++`;
  - after waiter decrement but before shutdown returns;
  - between `STOPPED` store and `set_life()`’s mutex access.
- Concurrent/double destroy, or an explicit public contract proving it forbidden.
- Pause between `try_push`’s close check and reservation/publication.
- Active-submitter-only and supervisor-only quarantine must produce nonzero/typed shutdown status.
- Immediate supervisor-enabled create→shutdown and injected join/thread-create failures.
- Multiple simultaneous stuck workers with an assertion against the configured absolute deadline.
- True backlog restart: the current test processes all 40 prefill frames before forcing exit and submits the next 60 only after a 400 ms restart delay.
- True hot-shard isolation: the current one-worker/one-blocked-producer test would also pass the pre-fix depth-check implementation.
- Raw-ring exact-ID bitsets and payload integrity; current total counts allow one duplicate plus one loss to pass.
- Repeated-key per-producer FIFO: current E2E matrices visit each `(reader,key)` at most once.
- Concurrent close/reopen, long wrap, frame ownership/double-release, and allocation/pthread fault injection.
- ASan, UBSan, TSan, strict GCC/Linux CI, and non-lock-free 64-bit atomic/`-latomic` qualification.
- `bench_ring` must return nonzero on `INIT_FAIL` or invariant failure; it currently always exits zero.
- Benchmark timeout/error handling: `bench_all_modes` can wait indefinitely after thread/submit failure.

### Spec/doc recommendations

- Define destroy’s synchronization precondition explicitly unless the lifetime design is changed.
- Model `STOPPED_RECLAIMABLE` separately from `QUARANTINED_NONRECLAIMABLE`, or document lifecycle and quarantine as independent dimensions.
- Rewrite `DESIGN.md` shutdown, supervisor, and frame lifecycle sections; remove cancel/detach/force-stop wording.
- Redraw `DIAGRAMS.md` around registration-before-recheck, acquire/try/release/wait/retry, deadline quarantine, and real concurrent-shutdown behavior; regenerate PNGs.
- Narrow “never drop” to full-queue backpressure while the consumer continues making progress.
- Describe worker slots as fixed, with restartable pthread generations.
- Consistently call `awp_runtime_enabled()` a caller-owned helper; the example should skip creation when disabled rather than setting the environment variable itself.
- State that true SPMC/MPMC operation is private/internal unless a public raw-ring API is intended.
- Describe the implementation as atomic queue reservation plus mutex/condvar notification, and qualify freelist lock freedom by platform.
- Preserve review reports as historical snapshots. Pass 3 is correctly tied to `1e8347b`; Pass 2 should gain an explicit reviewed commit/banner and replace absolute local links.
- Regenerate benchmark evidence at the reviewed commit with raw samples, open-loop schedules, real publisher acceptance, multiple runs, and system telemetry.

### Top 5 actions ranked by risk reduction

1. **Close the reclamation contract:** prevent `STOPPED` from becoming observable until shutdown has finished every pool access, and either externally serialize destroy or introduce a stable lifetime control block.
2. **Repair timeout semantics:** never close/drain while active submissions can still publish; return machine-readable quarantine status and use one absolute deadline.
3. **Make termination ownership provable:** record joined only after successful joins, fix supervisor startup ownership, define restart-failure admission behavior, and quarantine every uncertain join.
4. **Add adversarial qualification:** exact barriers, exact IDs, real backlog restart, hot-shard cross-isolation, fault injection, and sanitizer/Linux CI.
5. **Correct the evidence layer:** rewrite DESIGN/DIAGRAMS/README and rerun an honest ingress-to-real-publisher benchmark at `4b1076c`.

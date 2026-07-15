# Codex Pass 3 — Implementation & specs review (gpt-5.6-sol · xhigh)

| Field | Value |
|-------|-------|
| Model | `gpt-5.6-sol` |
| Reasoning | `xhigh` (Extra-High) |
| Mode | `codex exec` read-only |
| Reviewed commit | `1e8347b` (main after lifecycle fixes + diagrams/benches) |
| Date (UTC) | 2026-07-15 |
| Prior passes | Pass 1 estimate → `design-estimate.md`; Pass 2 as-implemented → `design-analysis.md` (**REJECT**, mitigations in `c11bab8`) |
| This pass | Adversarial re-review of **code + specs** after mitigations |
| **Verdict** | **REJECT** |

Raw run log (local scratch, not committed): `~/.scratch` equivalent under `/Users/dima/c_lang/.scratch/codex-review/run.log`.

---

### Verdict

**REJECT**

### Executive summary

The steady-state sequence-ring algorithms appear locally correct under their declared producer/consumer cardinalities; I found no release/acquire defect around `cell->data` and no classic condvar lost wakeup. The lifecycle fixes, however, do not establish safe reclamation: `STOPPED` can coexist with an active submitter, an unjoined supervisor, or a shutdown-quarantined worker. Four independently confirmed S0 paths can therefore free pool memory while another thread still references it. Callback reentrancy is also only partially guarded: `process()` can invoke `awp_pool_destroy()`, and `on_error()` runs outside TLS protection. Supervisor recovery, deadline behavior, and hot-shard isolation remain incomplete. All existing functional test binaries passed, but the dangerous paths are absent or deliberately cooperative. The benchmark is a useful local microbenchmark, but it excludes submit-side blocking/copying, stops at callback entry, and does not qualify the advertised ingest-to-publish-accept SLA. The committed design text and rendered diagrams still describe obsolete acquisition, cancellation, detach, and force-stop behavior.

`★ Insight ─────────────────────────────────────`
The key defect is semantic: `STOPPED` currently means “shutdown finished executing,” while destruction requires the stronger condition “no thread can reference the pool.” A safe design must either prove the latter or enter a permanently non-reclaimable quarantine state.
`─────────────────────────────────────────────────`

Validation performed against clean `main` at `1e8347b`. Existing binaries passed:

- `test_unit`: 824/824
- `test_unit_modes`: 892/892
- `test_ring_modes`: 424/424
- `test_supervisor`: 15/15
- `test_e2e`: 4/4
- `test_e2e_modes`: 16/16
- `test_e2e_lifecycle`: 290/290

A fresh compiler/sanitizer build was not possible because the read-only sandbox prevented Apple tooling from creating its `/tmp/xcrun_db-*` cache. No files were modified.

### Spec vs implementation drift

| Claim | Code reality | Severity |
|---|---|---:|
| `active_submits` makes destruction safe | Shutdown stops waiting at the deadline and enters `DRAINING` even when the counter is nonzero. API calls paused before registration are also invisible. | **S0** |
| Quarantined workers cause destroy to leak safely | Shutdown sets only `w->state`; destroy checks a different `pool->quarantined` flag that this path never sets. | **S0** |
| `STOPPED` is safe to destroy | `STOPPED` may coexist with active submits, an unjoined supervisor, or shutdown waiters. | **S0** |
| Callback reentrancy returns `-EDEADLK` | Submit/shutdown from `process()` are rejected, but destroy ignores shutdown’s error and `on_error()` runs outside TLS. | **S0** |
| Hard shutdown deadline / force-stop | Late workers get an additional 200 ms each and are quarantined, not force-stopped. | **S1** |
| Supervisor restarts stalled workers | A recovered backlogged worker can be falsely quarantined; a restart `pthread_create()` failure leaves an open queue without a consumer. | **S1** |
| Hot-shard producers no longer retain frames | The depth check does not reserve queue capacity; many producers can pass it and acquire global frames for one available cell. | **S1** |
| p99 ingest→publish-accept ≤5 ms | Timestamping occurs after ring-space wait, frame acquisition, and copying, and ends at callback entry before simulated publishing. | **S1** |
| Atomic rings use “no mutex” | Every successful push/pop locks `wait_mu` and broadcasts; frame release also takes a mutex. | **S2** |
| Pool exposes useful SPMC/MPMC topologies | The pool always has one consumer per ring. True multi-consumer demonstrations include private `src/internal.h`. | **S2** |
| Concurrent FIFO follows successful push completion | MPSC order is determined by reservation CAS; completion can occur in the opposite order. | **S2** |
| `AWP_ENABLED` / `AWP_COMPILE_ENABLED` gates the library | `AWP_ENABLED` is only queried by a helper; creation does not enforce it. `AWP_COMPILE_ENABLED` is not implemented. | **S2** |
| DESIGN/diagrams show current submit path | They show frame acquisition before ring-space waiting; code now performs the precheck first. | **S2** |
| Quarantine uses cancel/detach | Current code does neither, but DESIGN and the rendered supervisor diagram still say it does. | **S2** |
| Soft errors are logged, counted, and recycled | They are counted/recycled; logging occurs only if the optional user callback logs. | **S3** |

### Residual / new findings

#### 1. Shutdown quarantine does not activate destroy’s leak guard

- **Severity:** **S0**
- **Evidence:** [worker.c:120](/Users/dima/c_lang/async-worker-pool/src/worker.c:120) sets `AWP_W_QUARANTINED`; [pool.c:469](/Users/dima/c_lang/async-worker-pool/src/pool.c:469) checks only `pool->quarantined`. Only the supervisor path sets that flag at [supervisor.c:79](/Users/dima/c_lang/async-worker-pool/src/supervisor.c:79).
- **Failure mode:** With the supervisor disabled, a genuinely stuck `process()` exceeds the deadline. Shutdown returns and publishes `STOPPED`; destroy frees the frame, worker, rings, and pool. If the callback later returns, the worker updates freed atomics and releases into the freed frame pool.
- **Suggested fix:** Centralize quarantine in one helper that atomically makes the pool non-reclaimable before publishing worker quarantine. Destroy should defensively verify that every worker and the supervisor are joined, not trust one drift-prone flag.

#### 2. Submit quiescence is deadline-limited, but reclamation is not

- **Severity:** **S0**
- **Evidence:** Admission tracking is at [pool.c:248](/Users/dima/c_lang/async-worker-pool/src/pool.c:248). Shutdown waits only until its deadline at [pool.c:406](/Users/dima/c_lang/async-worker-pool/src/pool.c:406), then unconditionally reaches `STOPPED` at [pool.c:456](/Users/dima/c_lang/async-worker-pool/src/pool.c:456).
- **Failure mode:** A counted submitter paused while acquiring/filling/pushing resumes after destroy and touches freed slab, ring, worker metrics, or counters. A caller paused in validation/`strlen()` before incrementing `active_submits` is not tracked at all.
- **Suggested fix:** Never make a pool reclaimable while `active_submits != 0`. Deadline expiry must enter a quarantined/non-destroyable state. If concurrent destroy is supported, use an external stable handle/reference scheme; an in-object counter cannot protect callers paused before their first protected access. Otherwise explicitly require all producer threads to be joined before destroy.

#### 3. An unjoined supervisor may outlive the pool

- **Severity:** **S0**
- **Evidence:** Supervisor interval and stall-grace waits are not interruptible at [supervisor.c:64](/Users/dima/c_lang/async-worker-pool/src/supervisor.c:64) and [supervisor.c:88](/Users/dima/c_lang/async-worker-pool/src/supervisor.c:88). Shutdown skips `pthread_join()` if it remains alive after the deadline at [pool.c:415](/Users/dima/c_lang/async-worker-pool/src/pool.c:415), but still permits destruction.
- **Failure mode:** Set `supervisor_interval_ms > shutdown_deadline_ms`. Destroy frees the pool while the supervisor sleeps; it later wakes and reads freed lifecycle fields or broadcasts through destroyed synchronization objects.
- **Suggested fix:** Use an interruptible condition-variable wait. An unjoined supervisor must quarantine the pool. `STOPPED` must not be reclaimable until the supervisor has definitively exited and joined.

#### 4. Callback reentrancy can directly free the running pool

- **Severity:** **S0**
- **Evidence:** TLS covers only `process()` at [worker.c:43](/Users/dima/c_lang/async-worker-pool/src/worker.c:43), and is cleared before `on_error()`. Destroy calls shutdown but ignores its result at [pool.c:466](/Users/dima/c_lang/async-worker-pool/src/pool.c:466).
- **Failure mode:** `process()` calls `awp_pool_destroy(pool)`. Nested shutdown returns `-EDEADLK`; destroy ignores it and frees the pool beneath the callback. `on_error()` can also submit to its own full queue, self-shutdown, or destroy because TLS is already clear.
- **Suggested fix:** Keep callback context active through both callbacks. Reject/no-op destroy in callback context and never reclaim unless shutdown successfully established a joined, reclaimable state. Prefer a status-returning destroy API.

#### 5. Supervisor restart and stall handling are not lifecycle-safe

- **Severity:** **S1**
- **Evidence:** Lifecycle is checked before restart work at [supervisor.c:31](/Users/dima/c_lang/async-worker-pool/src/supervisor.c:31), but not immediately before reopen/start at lines 37–40 or 70–77. The worker honors `stop` only when its queue is empty at [worker.c:21](/Users/dima/c_lang/async-worker-pool/src/worker.c:21).
- **Failure mode:** Shutdown can begin while the supervisor joins or waits in grace, after which it reopens and starts a new generation during drain. A recovered worker with continuous backlog remains `RUNNING`, is falsely quarantined, and continues processing. If replacement `pthread_create()` fails, the reopened queue remains live with no consumer.
- **Suggested fix:** Recheck lifecycle immediately before every reopen/start; make supervisor waits interruptible. Use an unconditional between-frame restart-stop distinct from shutdown drain. Report/retry restart failures or mark the pool degraded and reject that shard.

#### 6. The configured hard deadline is exceeded per late worker

- **Severity:** **S1**
- **Evidence:** Each worker receives a fresh 200 ms grace at [worker.c:120](/Users/dima/c_lang/async-worker-pool/src/worker.c:120), called serially from [pool.c:439](/Users/dima/c_lang/async-worker-pool/src/pool.c:439).
- **Failure mode:** Thirty-two stuck workers can add about 6.4 seconds after the configured absolute deadline.
- **Suggested fix:** Use the original absolute deadline everywhere with no post-deadline per-worker extension, or rename the setting and document the additional per-worker quarantine budget. Remove “hard” and “force-stop” unless process-level isolation is added.

#### 7. Hot-shard frame isolation is still racy

- **Severity:** **S1**
- **Evidence:** [pool.c:263](/Users/dima/c_lang/async-worker-pool/src/pool.c:263) checks depth before frame acquisition, but the actual queue reservation occurs later at [ring.c:190](/Users/dima/c_lang/async-worker-pool/src/ring.c:190).
- **Failure mode:** After one cell becomes available, many awakened MPSC producers all pass the depth check and acquire global frames. One gets the cell; the others block in push while retaining frames, starving healthy shards.
- **Suggested fix:** Atomically reserve per-shard capacity before acquiring a frame, partition frames per shard, or use ring-owned frame slots.

#### 8. Partial-create rollback destroys invalid pthread objects

- **Severity:** **S1**
- **Evidence:** Ring initialization failure jumps from [pool.c:145](/Users/dima/c_lang/async-worker-pool/src/pool.c:145) to cleanup of all `n_workers` rings at lines 199–201. [ring.c:89](/Users/dima/c_lang/async-worker-pool/src/ring.c:89) already cleans its own partially initialized mutex/condvar.
- **Failure mode:** Allocation or pthread initialization failure causes double-destroy of the failed ring and destruction of zero-filled, never-initialized rings—undefined behavior in the error path.
- **Suggested fix:** Track the exact successfully initialized ring count and destroy only that prefix.

#### 9. Over-aligned types are allocated with ordinary `calloc`

- **Severity:** **S2**
- **Evidence:** `awp_ring_t`, `awp_cell_t`, and therefore `awp_worker_t` have 64-byte extended alignment via [internal.h:23](/Users/dima/c_lang/async-worker-pool/src/internal.h:23). Workers use plain `calloc` at [pool.c:138](/Users/dima/c_lang/async-worker-pool/src/pool.c:138); cell allocation falls back to `calloc` at [ring.c:51](/Users/dima/c_lang/async-worker-pool/src/ring.c:51).
- **Failure mode:** Ordinary allocation is not required to satisfy user-requested extended alignment, making accesses formally undefined and invalidating cache-line isolation on some platforms.
- **Suggested fix:** Use aligned allocation for workers and cells; do not fall back to ordinary `calloc` for an over-aligned type.

#### 10. The queue and freelist are not mutex-free operations

- **Severity:** **S2**
- **Evidence:** Every successful push/pop calls `awp_ring_wake_all()`, which locks and broadcasts at [ring.c:11](/Users/dima/c_lang/async-worker-pool/src/ring.c:11). Every frame release locks `wait_mu` at [frame_pool.c:157](/Users/dima/c_lang/async-worker-pool/src/frame_pool.c:157).
- **Failure mode:** Producers and consumers serialize on the wake mutex and can block behind a descheduled mutex owner. Broadcast on every operation can create unnecessary wake storms.
- **Suggested fix:** Describe the design as “atomic reservation plus mutex/condvar wakeup.” If mutex-free progress is required, redesign notification so successful operations do not take a contended mutex.

#### 11. Freelist ABA protection is finite and lock freedom is unchecked

- **Severity:** **S2**
- **Evidence:** The head uses a 32-bit tag at [frame_pool.c:6](/Users/dima/c_lang/async-worker-pool/src/frame_pool.c:6). `atomic_is_lock_free()` is merely recorded at line 36 and never enforced or surfaced.
- **Failure mode:** A CAS participant stalled across \(2^{32}\) head mutations can observe a repeated packed head and reinsert an in-use node. Non-head double release can also form a cycle; the current check catches only a node already at the head.
- **Suggested fix:** Use a design without finite-tag ABA exposure, a wider generation where supported, or a mutex fallback. Add debug ownership state and explicitly document/enforce supported lock-free platforms.

#### 12. Ring/API ordering and topology contracts are overstated

- **Severity:** **S2**
- **Evidence:** The header says concurrent same-key producers are ordered by successful push completion at [awp.h:146](/Users/dima/c_lang/async-worker-pool/include/awp/awp.h:146), but MPSC order is determined by the earlier CAS at [ring.c:196](/Users/dima/c_lang/async-worker-pool/src/ring.c:196). The pool provides only one consumer per ring.
- **Failure mode:** Producer A can reserve first and complete after producer B, while callbacks still observe A then B. Users relying on documented completion order receive different behavior. SPMC/MPMC pool configurations do not provide the advertised multi-consumer topology.
- **Suggested fix:** Document FIFO as ring-reservation linearization order. Label pool SPMC/MPMC execution as their single-consumer subsets, or keep multi-consumer modes internal until a public topology uses them.

Related: ring close is not a quiescence barrier—a producer can reserve before close and publish afterward. Clean shutdown avoids this only when submit quiescence actually succeeds.

#### 13. Benchmark PASS does not qualify the advertised SLA

- **Severity:** **S1**
- **Evidence:** `submit_ns` is assigned after pre-wait, acquisition, and copies at [pool.c:263](/Users/dima/c_lang/async-worker-pool/src/pool.c:263). Latency ends at callback entry at [bench_dispatch.c:30](/Users/dima/c_lang/async-worker-pool/bench/bench_dispatch.c:30). The workload is an unpaced closed-loop burst at lines 85–96.
- **Failure mode:** Frame-pool/ring backpressure and callback/publisher work are excluded. The retained 585k msg/s, 5.23 ms run does not exercise sparse wakeup tails at the claimed 1–5k msg/s workload.
- **Suggested fix:** Timestamp before entering `awp_submit`, complete samples after real publisher buffer acceptance, and use open-loop 1k/5k schedules with skew, bursts, delays, warmup, repetitions, and confidence bounds.

#### 14. The validation suite gives false confidence around lifecycle and identity

- **Severity:** **S2**
- **Evidence:** The “stuck” callback exits on `QUIESCING` at [test_supervisor.c:14](/Users/dima/c_lang/async-worker-pool/tests/test_supervisor.c:14). Restart tests stop an empty worker and submit only after restart. Direct ring tests count pops but do not validate exact frame IDs. `bench_ring` prints failure but always returns zero from [bench_ring.c:131](/Users/dima/c_lang/async-worker-pool/bench/bench_ring.c:131).
- **Failure mode:** Quarantine UAF, duplicate-plus-loss, backlog loss, and benchmark failure can all escape a green `make check`.
- **Suggested fix:** Add the matrix below and make every validation executable return nonzero on failed invariants.

#### 15. The advertised runtime/compile-time gate is not a gate

- **Severity:** **S2**
- **Evidence:** [pool.c:22](/Users/dima/c_lang/async-worker-pool/src/pool.c:22) only provides a helper; [pool.c:77](/Users/dima/c_lang/async-worker-pool/src/pool.c:77) creates a pool regardless. `AWP_COMPILE_ENABLED` appears only in the header comment.
- **Failure mode:** Applications believing creation is disabled unless the gate is enabled can start the pool unintentionally.
- **Suggested fix:** Either enforce the gate in creation/build configuration or clearly document it as a caller-owned query helper and remove the nonexistent compile-time switch.

### Prior S0 re-verification

| Prior issue | Status | Evidence |
|---|---|---|
| Free while submitters active | **PARTIAL** | Counter added, but shutdown ignores it after the deadline and callers before registration remain invisible. |
| Exit without drain | **FIXED** for clean shutdown | Workers consume until closed and empty at [worker.c:16](/Users/dima/c_lang/async-worker-pool/src/worker.c:16). Timeout/quarantine still breaks the wider delivery guarantee. |
| Cancel/detach UAF | **STILL OPEN** | Cancel/detach were removed, but shutdown-created quarantine does not set destroy’s leak guard. |
| Concurrent shutdown races | **PARTIAL** | Callers wait for `STOPPED`, but immediate destroy can race a waiter still leaving `pthread_cond_wait`. |
| Restart destroys queue | **FIXED** for the exact storage issue | `awp_ring_reopen()` preserves storage/backlog. Lifecycle-safe restart and backlog preservation remain untested. |
| Indefinite spin | **FIXED** for normal waits | Ring/frame exhaustion paths park after bounded spinning; no classic lost wakeup found. |
| Silent truncation | **FIXED** | Oversize values return `-E2BIG`; invalid payloads return `-EINVAL`. |
| Hot-shard holds frames | **PARTIAL** | The non-reserving depth precheck still permits a thundering herd to acquire frames. |
| Callback reentrancy | **PARTIAL** | Submit/shutdown from `process()` are rejected; destroy and all `on_error()` reentrancy remain open. |
| `n_broadcast_workers >= n_workers` | **FIXED** | Rejected at [pool.c:87](/Users/dima/c_lang/async-worker-pool/src/pool.c:87). |

### Test matrix gaps

Add at minimum:

- A truly nonreturning callback in a child process: verify shutdown result, sticky quarantine, and no reclamation/UAF.
- Barrier-controlled submitters paused before registration, after registration, after frame acquisition, and after ring reservation while shutdown/destroy runs.
- Supervisor interval greater than shutdown deadline and shutdown during stall grace.
- `process()` and `on_error()` attempts to submit, shutdown, and destroy.
- Restart with a populated backlog and concurrent producers, validating each ID exactly once and FIFO.
- Fault injection for ring allocation, mutex/condvar initialization, worker creation, and restart creation.
- Hot-shard producer herd while a healthy shard continues submitting.
- Exact-ID bitsets in all multi-producer/consumer ring tests; millions of wraps plus concurrent close/reopen.
- Concurrent metrics snapshots and lifecycle tests under TSan.
- ASan, UBSan, and TSan targets; strict Clang/macOS and GCC/Linux CI. Either test 32-bit wrap behavior or declare 64-bit-only support.
- Open-loop SLA qualification with real publisher acceptance, representative skew/bursts, repetitions, and retained raw results.

### Spec/doc recommendations

- Rewrite `DESIGN.md` lifecycle and fault sections: remove cancellation, detach, force-stop, and unconditional hard-deadline wording.
- Define distinct states for clean reclaimable stop versus timed-out quarantine. Document whether destroy may run concurrently with any API call.
- Update `DIAGRAMS.md` and regenerate PNGs: current submit order is stale, supervisor artwork still says cancel/detach, and several rendered labels contain literal `\n`.
- State that the pool has one consumer per ring; true SPMC/MPMC behavior is internal raw-ring functionality.
- Change “no mutex” and “lock-free freelist” to precise progress-language that includes wakeup locks and platform dependence.
- Correct FIFO to reservation-linearization order.
- State explicitly that zero full-queue drops is not downstream delivery, and process errors/forced abandonment are separate outcomes.
- Correct benchmark labels to the interval actually measured until methodology is fixed.
- Remove `AWP_COMPILE_ENABLED` or implement it; clarify whether `AWP_ENABLED` is enforced or caller-owned.
- Mark `design-analysis.md` as a historical Pass 2 snapshot with its reviewed commit; replace absolute `/Users/dima/...` links with relative repository links.
- Correct the header’s “pool logs” and force-stop comments and document callback restrictions, quarantine behavior, metrics-buffer concurrency, and status semantics.

### Top 5 actions ranked by risk reduction

1. **Make reclamation provable:** no reclaimable terminal state until `active_submits == 0`, all workers are joined, the supervisor is joined, and no shutdown waiter remains; otherwise enter sticky quarantine.
2. **Close reentrancy and API-lifetime holes:** protect `process`, `on_error`, shutdown, and destroy consistently; define and enforce destroy synchronization preconditions.
3. **Repair supervisor lifecycle ownership:** interruptible waits, lifecycle rechecks before restart, unconditional between-frame restart stop, and explicit restart-failure/degraded handling.
4. **Add adversarial lifecycle qualification:** true stuck callback, paused submit phases, late supervisor, backlog restart, exact-ID ring stress, fault injection, and sanitizers on macOS/Linux.
5. **Fix capacity isolation and evidence:** reserve ring capacity before global frame acquisition, then rewrite docs/diagrams and rerun an honest open-loop publisher benchmark.

# HFT & Low-Latency Industrial-Grade Evolution Plan

This document outlines the systematic engineering roadmap to evolve `async-worker-pool` from a generic POSIX sharded pool into a production-grade, sub-microsecond HFT dispatch and processing engine.

---

## Table of Contents

- [Phase 1: Hot-Path Lock-Free & Syscall Elimination](#phase-1-hot-path-lock-free--syscall-elimination-target-sub-microsecond-p99)
  - [1.1 Remove Condvar Signals from Enqueue/Dequeue Hot Path](#11-remove-condvar-signals-from-enqueuedequeue-hot-path)
  - [1.2 Eliminate Over-Alignment in Ring Cells](#12-eliminate-over-alignment-in-ring-cells)
  - [1.3 Zero-Copy Claim & Commit API](#13-zero-copy-claim--commit-api)
- [Phase 2: Hardware, Memory & OS Optimization](#phase-2-hardware-memory--os-optimization)
  - [2.1 P-Core Pinning & Cache Cluster Affinity](#21-p-core-pinning--cache-cluster-affinity)
  - [2.2 HugePages & Slab Allocation](#22-hugepages--slab-allocation)
  - [2.3 Cycle-Accurate Nanosecond Profiling (RDTSC / Mach Timebase)](#23-cycle-accurate-nanosecond-profiling-rdtsc--mach-timebase)
- [Phase 3: Industrial Safety, Load-Shedding & Tooling](#phase-3-industrial-safety-load-shedding--tooling)
  - [3.1 Configurable Backpressure Policies](#31-configurable-backpressure-policies)
  - [3.2 Verification & Sanitizer Suite](#32-verification--sanitizer-suite)
- [Phase 4: Cross-Language Integration & Parallel Zig Engine](#phase-4-cross-language-integration--parallel-zig-engine)
  - [4.1 C ABI Stability & Packaging](#41-c-abi-stability--packaging)
  - [4.2 Parallel Zig Engine (`async-worker-pool_zig`)](#42-parallel-zig-engine-async-worker-pool_zig)

---

## Phase 1: Hot-Path Lock-Free & Syscall Elimination (Target: Sub-microsecond P99)

### 1.1 Remove Condvar Signals from Enqueue/Dequeue Hot Path
* **Current State:** `try_push_sp` ([`src/ring.c:259`](file:///Users/dima/c_lang/async-worker-pool/src/ring.c#L259)) and `try_push_mp` ([`src/ring.c:286`](file:///Users/dima/c_lang/async-worker-pool/src/ring.c#L286)) unconditionally invoke `awp_ring_wake_all()`, performing `pthread_mutex_lock` + `pthread_cond_broadcast` on every single message.
* **Target Architecture:** Pure lock-free atomic sequence protocol. Implement an atomic `waiters` counter. Syscall / parking is invoked **only** when `waiters > 0` after a configurable spin budget.
* **Expected Impact:** Eliminates 2–15 µs kernel transition jitter on every push.

### 1.2 Eliminate Over-Alignment in Ring Cells
* **Current State:** `awp_cell_t` ([`src/internal.h:89`](file:///Users/dima/c_lang/async-worker-pool/src/internal.h#L89)) applies `alignas(64)` to every cell sequence, inflating each cell to 64 bytes and causing guaranteed L1 cache misses per ring slot advancement.
* **Target Architecture:** Remove `alignas(64)` from individual cells. Align the entire cell buffer to 64 bytes (`AWP_ALIGN_CACHE`) so hardware prefetchers load adjacent sequence counters in a single cache line.
* **Expected Impact:** Improves sequential throughput and L1/L2 data cache hit rate.

### 1.3 Zero-Copy Claim & Commit API
* **Current State:** `awp_submit` unconditionally executes `memcpy` into a pooled `awp_frame_t`.
* **Target Architecture:** Add a two-phase Zero-Copy API:
  ```c
  awp_frame_t *awp_claim_frame(awp_pool_t *pool, uint32_t shard);
  int awp_commit_frame(awp_pool_t *pool, awp_frame_t *frame);
  ```
  Allows direct zero-copy parsing from network/socket buffers straight into the pre-allocated frame memory.

---

## Phase 2: Hardware, Memory & OS Optimization

### 2.1 P-Core Pinning & Cache Cluster Affinity
* **Apple Silicon:** Use `pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0)` and Mach `THREAD_AFFINITY_POLICY` to bind reader and worker threads to the same shared L2/L3 cache cluster.
* **Linux / x86_64:** Support `pthread_setaffinity_np` with user-configurable core masks and `isolcpus` integration.

### 2.2 HugePages & Slab Allocation
* Allocate the `awp_frame_pool_t` slab and ring buffers via `mmap` with `MAP_HUGETLB` (or `madvise(MADV_HUGEPAGE)`).
* Prevents TLB misses when cycling through multi-megabyte frame slabs.

### 2.3 Cycle-Accurate Nanosecond Profiling (RDTSC / Mach Timebase)
* Replace `clock_gettime(CLOCK_MONOTONIC)` in hot loops with:
  * x86_64: `__builtin_ia32_rdtsc()` / `_mm_lfence()`.
  * ARM64: `cntvct_el0` / `mach_absolute_time()`.
* Calibrate CPU cycle frequency at initialization; convert to nanoseconds only in metrics/telemetry reporting.

---

## Phase 3: Industrial Safety, Load-Shedding & Tooling

### 3.1 Configurable Backpressure Policies
* `AWP_OVERFLOW_BLOCK`: Standard blocking backpressure (batch / back-office).
* `AWP_OVERFLOW_DROP_OLDEST`: Ring overwrite (ideal for L2/L3 OrderBook snapshots and market data).
* `AWP_OVERFLOW_REJECT`: Immediate `-EAGAIN` non-blocking return (fast-fail for order routing).

### 3.2 Verification & Sanitizer Suite
* Add `make check-tsan`, `make check-asan`, and `make check-ubsan` targets to `Makefile`.
* Integrate concurrency model checkers (CDSChecker / Loom / CBMC) to mathematically verify C11 atomic ordering.

---

## Phase 4: Cross-Language Integration & Parallel Zig Engine

### 4.1 C ABI Stability & Packaging
* Split headers: `include/awp/awp_types.h`, `include/awp/awp_version.h`, `include/awp/awp.h`.
* Support `-flto` (Link-Time Optimization) in `libawp.a` for cross-language inlining into Rust, C++, and Zig.
* Structure-size initialization (`sizeof(awp_config_t)`) to preserve ABI compatibility.

### 4.2 Parallel Zig Engine (`async-worker-pool_zig`)
* Build a native Zig 0.16 parallel implementation utilizing:
  * `@Vector(16, u8)` and SIMD reductions for ultra-fast payload parsing and checksums.
  * Comptime-specialized ring buffer queues (eliminating runtime branching).
  * Native thread pinning and zero-allocation semantics.
* Benchmark side-by-side against C11 `libawp.a`.

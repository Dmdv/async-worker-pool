# Comprehensive Benchmark Results & Cross-Language Comparison

High-scale verification (1,000,000+ messages) comparing C11 (`libawp`), Rust FFI bindings (`awp-rs`), and Zig 0.16 (`async-worker-pool_zig`) with cycle-accurate Mach timebase on Apple Silicon and 64-byte ARM NEON / `@Vector` SIMD checksum calculation.

| Environment Field | Specification |
| :--- | :--- |
| **Host Architecture** | Darwin arm64 (Apple Silicon M-Series) |
| **C Compiler** | Apple Clang (C11, `-O2 -pthread`) |
| **Rust Toolchain** | Rust 1.96 / Cargo (`--release`) |
| **Zig Toolchain** | Zig 0.16.0 (`ReleaseFast`, SIMD `@Vector(16, u8)`) |
| **Thread QoS & Pinning** | `QOS_CLASS_USER_INTERACTIVE` (P-Cores) |
| **Payload Verification** | 64-byte SIMD Checksum |

---

## Table of Contents

- [1. Cross-Language Comparative Summary (1,000,000 Messages)](#1-cross-language-comparative-summary-1000000-messages)
- [2. C Engine Evolution: Baseline vs Phase 1 (Hot-Path Lock-Free & Zero-Copy)](#2-c-engine-evolution-baseline-vs-phase-1-hot-path-lock-free--zero-copy)
  - [2.1 Raw Lock-Free Rings (1,000,000 Operations)](#21-raw-lock-free-rings-1000000-operations)
  - [2.2 Pool Dispatch Benchmarks](#22-pool-dispatch-benchmarks)
- [3. Key Architectural Findings](#3-key-architectural-findings)

---

## 1. Cross-Language Comparative Summary (1,000,000 Messages)

Workload: **1,000,000 messages** processed asynchronously (Zig: 4 Pinned Workers on Apple Silicon P-Cores; C11: 32 Workers).

| Implementation | Mode / API | Throughput | Median (p50) | p99 Latency | Mean Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Zig 0.16** ([`async-worker-pool_zig`](https://github.com/Dmdv/async-worker-pool_zig)) | Multi-Threaded Async (4 Pinned Workers) | **5.38 M msg/s** 🚀 | **< 100 ns** | **1.00 µs** (1,000 ns) | **547.0 ns** (0.55 µs) |
| **Zig 0.16** (Pure SPSC Ring) | Concurrent SPSC (0 CAS) | **171.76 M ops/s** 🚀 | **< 6 ns** | **< 8 ns** | **5.82 ns** |
| **C11** ([`async-worker-pool`](https://github.com/Dmdv/async-worker-pool)) | Zero-Copy Claim/Commit (32 Workers) | **0.52 M msg/s** | **3.46 µs** (3,458 ns) | **1.11 ms** (1,110,000 ns) | **2.11 µs** (2,109 ns) |
| **C11** (Raw SPSC Ring) | Lock-Free Push/Pop | **62.50 M ops/s** | **N/A (Bulk)** | **N/A (Bulk)** | **16.00 ns** (avg) |
| **Rust** ([`awp-rs`](../bindings/rust)) | Safe FFI Zero-Copy (`v0.3.0`) | **0.53 M msg/s** | **3.35 µs** (3,350 ns) | **1.15 ms** (1,150,000 ns) | **1.87 µs** (1,870 ns) |

---

### Detailed Tail Latencies Breakdown (1,000,000 Messages)

| Percentile | **Zig 0.16 Engine (Phase 1 Final)** | **C11 Engine** (`async-worker-pool`) | Delta / Notes |
| :--- | :--- | :--- | :--- |
| **Min (Observed Floor)** | **15 ns** (0.015 µs) | **120 ns** (0.120 µs) | Observed Single-Hop Floor |
| **p50 (Median)** | **< 100 ns** | **3.46 µs** (3,458 ns) | **Zig is > 34x lower latency** 🚀 |
| **p90** | **1.00 µs** (1,000 ns) | **7.17 µs** (7,167 ns) | **Zig is 7.2x lower latency** 🚀 |
| **p99 (Tail)** | **1.00 µs** (1,000 ns) | **1.11 ms** (1,110,000 ns) | **Zig is 1,110x lower tail jitter** 🚀 |
| **p99.9** | **96.0 µs** (96,000 ns) | **1.27 ms** (1,270,000 ns) | **Zig is 13.2x lower tail jitter** 🚀 |
| **Max** | **128.0 µs** (128,000 ns) | **1.63 ms** (1,630,000 ns) | **Zig is 12.7x lower peak jitter** 🚀 |
| **Pure SPSC Throughput** | **171.76 Million ops/sec** | **62.50 Million ops/sec** | **Zig is 2.75x faster (5.82 ns/op)** 🚀 |

<p align="center">
  <img src="images/benchmark_throughput.png" width="48%" alt="Throughput Comparison" />
  <img src="images/benchmark_spsc_comparison.png" width="48%" alt="SPSC Comparison" />
</p>
<p align="center">
  <img src="images/benchmark_tail_latencies.png" width="96%" alt="Tail Latencies Distribution" />
</p>

### 1.1 Reproducing Cross-Language Benchmarks

To reproduce and verify the captured benchmark dataset (`benchmarks/zig_phase1_calibrated.json`):

```bash
# 1. Run C engine dispatch benchmark
make bench

# 2. Run Zig 0.16 engine hardware dispatch benchmark
cd ../async-worker-pool_zig
zig build bench -Doptimize=ReleaseFast

# 3. Regenerate charts with active venv
cd ../async-worker-pool
venv/bin/python scripts/generate_charts.py
```

## 2. C Engine Evolution: Baseline vs Phase 1 (Hot-Path Lock-Free & Zero-Copy)

### 2.1 Raw Lock-Free Rings (1,000,000 Operations)
| Ring Mode | Baseline (Before) | Phase 1 (After) | Speedup | Latency Delta |
| :--- | :--- | :--- | :--- | :--- |
| **SPSC** | `24.40 M ops/s` (41.0 ns) | **`62.50 M ops/s` (16.0 ns)** | **+156% (2.56x)** 🚀 | **41.0 ns ➔ 16.0 ns** |
| **MPSC** | `7.10 M ops/s` (140.9 ns) | **`13.70 M ops/s` (73.0 ns)** | **+93% (1.93x)** 🚀 | **140.9 ns ➔ 73.0 ns** |
| **SPMC** | `7.69 M ops/s` (130.0 ns) | **`10.59 M ops/s` (94.4 ns)** | **+38% (1.38x)** | **130.0 ns ➔ 94.4 ns** |
| **MPMC** | `13.80 M ops/s` (72.5 ns) | **`4.15 M ops/s` (241.2 ns)** | Contention bounded | Multi-reader CAS |

### 2.2 Pool Dispatch Benchmarks
* **`bench_dispatch` (1,000,000 msgs, 2,000 keys, 32 workers):**
  * Throughput: **0.52 M msg/sec**
  * Latency: $p50 = 3.42\text{ µs}$, $p90 = 6.58\text{ µs}$, $p99 = 82.50\text{ µs}$
* **`bench_zerocopy` (Zero-Copy Claim/Commit API):**
  * Direct in-place writing: 0 intermediate copies, 0 global CAS lock contention.

---

## 3. Key Architectural Findings

1. **Zig 0.16 Performance Advantages:**
   * **LLVM Autovectorization & `@Vector`:** Zig compiles `@Vector(16, u8)` directly into single-cycle ARM NEON SIMD instructions with zero wrapper overhead.
   * **`ArenaAllocator` Lifecycle:** O(1) bulk destruction without iterating over individual pointers or holding mutexes.
2. **C / Rust Interoperability:**
   * `libawp.a` provides an ultra-stable C ABI easily consumed by Rust (`awp-rs`) with zero performance regression.

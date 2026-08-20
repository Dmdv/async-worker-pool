# awp-rs: Safe & Idiomatic Rust FFI Bindings for Async Worker Pool

High-performance, zero-allocation Rust bindings for the `libawp` C11 asynchronous worker pool engine.

[![Crate](https://img.shields.io/badge/crate-awp--rs-orange.svg)](bindings/rust)
[![Version](https://img.shields.io/badge/version-0.3.0-green.svg)](bindings/rust)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](../../LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Key Features (v0.3.0)](#key-features-v030)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [1. Fluent PoolBuilder Configuration](#1-fluent-poolbuilder-configuration)
  - [2. Zero-Allocation Message Submission](#2-zero-allocation-message-submission)
  - [3. Two-Phase Zero-Copy Claim & Commit API](#3-two-phase-zero-copy-claim--commit-api)
  - [4. Zero-Copy Typed Struct Serialization](#4-zero-copy-typed-struct-serialization)
- [Performance Benchmarks](#performance-benchmarks)
- [Error Handling](#error-handling)
- [FFI Architecture & Safety](#ffi-architecture--safety)

---

## Overview

`awp-rs` provides safe, idiomatic Rust ergonomics over the ultra-low-latency `libawp` engine:
- **Zero dynamic allocations** on the hot path (stack-buffered string parsing, in-place zero-copy slabs).
- **Safe RAII lifecycle**: Guaranteed teardown with quarantine detection on drop.
- **`Send + Sync` thread safety**: Designed for multi-producer market data dispatch.

---

## Key Features (v0.3.0)

- **`PoolBuilder`**: Fine-grained configuration of worker counts, ring capacity, supervisor heartbeats, and broadcast slots.
- **Zero-Alloc Submissions**: Fast-path stack buffers avoid heap allocations (`malloc`) on every message.
- **Typed Zero-Copy Serialization**: Direct reads/writes of plain-old-data (POD) structs via `guard.write_struct(&item)` and `frame.payload_as::<T>()`.
- **Typed `AwpError`**: Idiomatic error enum implementing `std::error::Error` and `Display`.
- **Safe Discard on Drop**: Abandoned `ClaimGuard`s do not publish corrupt frames to worker queues.

---

## Quick Start

### 1. Run Unit Tests

```bash
cd bindings/rust
cargo test
```

### 2. Run Rust FFI Benchmark (1,000,000 Messages)

```bash
cargo run --release --example bench_throughput
```

---

## Usage Examples

### 1. Fluent `PoolBuilder` Configuration

```rust
use awp_rs::{AwpRingMode, PoolBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PoolBuilder::new()
        .workers(32)
        .queue_capacity(4096)
        .ring_mode(AwpRingMode::Mpsc)
        .supervisor(true)
        .shutdown_deadline_ms(5_000)
        .build(|frame| {
            println!("Received seq {} on feed {}: {:?}", 
                     frame.seq(), frame.feed(), frame.payload());
            0 // 0 = success, non-zero = soft error counted by telemetry
        })?;

    pool.submit("trades", "BTCUSDT", b"{\"price\": 65000.5}", 0)?;
    Ok(())
}
```

### 2. Zero-Allocation Message Submission

```rust
use awp_rs::{AsyncWorkerPool, AwpRingMode};
use std::ffi::CStr;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = AsyncWorkerPool::new(16, 1024, AwpRingMode::Mpsc, |_| 0)?;

    // 1. Standard zero-allocation stack submission (&str)
    pool.submit("binance_depth", "BTCUSDT", b"depth_snapshot", 0)?;

    // 2. Ultra-fast pre-formatted CStr submission (0 parsing overhead)
    let feed = CStr::from_bytes_with_nul(b"quotes\0")?;
    let symbol = CStr::from_bytes_with_nul(b"ETHUSDT\0")?;
    pool.submit_cstr(feed, symbol, b"quote_tick", 0)?;

    // 3. Keyed submission (bypasses hash computation)
    pool.submit_keyed(123456789, "quotes", "ETHUSDT", b"quote_tick", 0)?;

    Ok(())
}
```

### 3. Two-Phase Zero-Copy Claim & Commit API

```rust
use awp_rs::{AsyncWorkerPool, AwpRingMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = AsyncWorkerPool::new(16, 2048, AwpRingMode::Mpsc, |frame| {
        assert_eq!(frame.feed(), "okx_trades");
        assert_eq!(frame.symbol(), "BTCUSDT");
        println!("Received {} bytes in-place", frame.payload().len());
        0
    })?;

    let target_shard = 0;

    // 1. Claim a slot directly in the ring slab without copying
    let mut guard = loop {
        match pool.claim(target_shard) {
            Ok(g) => break g,
            Err(_) => std::thread::yield_now(),
        }
    };

    // 2. Set metadata in-place
    guard.set_feed("okx_trades")?;
    guard.set_symbol("BTCUSDT")?;

    // 3. Write directly into ring payload memory
    let buf = guard.payload_mut();
    buf[..32].fill(0xAA);
    guard.set_payload_len(32);

    // 4. Commit to make available to the worker thread
    guard.commit()?;

    Ok(())
}
```

### 4. Zero-Copy Typed Struct Serialization

```rust
use awp_rs::{AsyncWorkerPool, AwpRingMode};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
struct MarketTick {
    timestamp_ns: u64,
    bid: f64,
    ask: f64,
    volume: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = AsyncWorkerPool::new(16, 2048, AwpRingMode::Mpsc, |frame| {
        if let Some(tick) = frame.payload_as::<MarketTick>() {
            println!("Tick: bid={:.2}, ask={:.2}", tick.bid, tick.ask);
        }
        0
    })?;

    let mut guard = loop {
        match pool.claim(0) {
            Ok(g) => break g,
            Err(_) => std::thread::yield_now(),
        }
    };

    let tick = MarketTick {
        timestamp_ns: 1724140800000000000,
        bid: 65000.10,
        ask: 65000.20,
        volume: 12.5,
    };

    // Direct binary write into slab buffer
    guard.write_struct(&tick)?;
    guard.commit()?;

    Ok(())
}
```

---

## Performance Benchmarks

Measured on Apple Silicon (M-series, 1,000,000 messages, 32 workers):

| Metric | C11 Native (`libawp`) | Rust FFI (`awp-rs` v0.3.0) | Zig 0.16 Native (Phase 1) |
| :--- | :--- | :--- | :--- |
| **Throughput** | **0.52 M msg/s** | **0.53 M msg/s** | **5.38 M msg/s** 🚀 |
| **Median Latency (p50)** | **3,458 ns** (3.46 µs) | **3,350 ns** (3.35 µs) | **< 100 ns** |
| **p99 Tail Latency** | **1,110,000 ns** (1.11 ms) | **1,150,000 ns** (1.15 ms) | **1,000 ns** (1.00 µs) 🚀 |
| **Mean Latency** | **2,109 ns** (2.11 µs) | **1,870 ns** (1.87 µs) | **547 ns** (0.55 µs) 🚀 |

---

## Error Handling

`awp-rs` returns typed [`AwpError`](src/error.rs) variants:

```rust
use awp_rs::AwpError;

match pool.submit("feed", "symbol", payload, 0) {
    Ok(()) => (),
    Err(AwpError::InvalidArg) => eprintln!("Invalid configuration or arguments"),
    Err(AwpError::TooBig) => eprintln!("Payload or feed exceeds maximum buffer limit"),
    Err(AwpError::Deadlock) => eprintln!("Reentrancy detected: cannot submit inside worker callback"),
    Err(AwpError::PoolClosed) => eprintln!("Pool is shutting down"),
    Err(AwpError::Failed(code)) => eprintln!("System error code: {}", code),
}
```

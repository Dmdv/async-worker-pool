# Performance comparison: AWP vs market solutions

Reference note for later reading. **Not** a same-host head-to-head suite against
external libraries; category-level positioning using AWP local numbers and
publicly reported ranges for market tools.

| Field | Value |
|-------|--------|
| Repo tip (at write) | `main` (post-ACCEPT gate) |
| AWP local benches | See also [`BENCHMARKS.md`](BENCHMARKS.md) |
| Host (published benches) | Darwin arm64 · Apple M3 Ultra |
| Intent | Reference for product positioning / integration choices |

## What AWP is (and is not)

AWP is a **sharded permanent-worker dispatch stage** for market-data frames:

- Fixed `pthread` workers (not per-message spawn)
- Hash shard → **per-key FIFO**
- Bounded rings + **blocking backpressure** (no drops)
- Preallocated frames (no hot-path `malloc`)
- Supervisor, quarantine, bounded shutdown

It is **not**:

- An exchange matching engine
- Kernel-bypass / FPGA market data
- A pure lock-free queue (moodycamel, Folly MPMC, Disruptor)
- A general work-stealing pool (TBB, `std::execution`, Tokio work-stealing runtime)

Design intent: C analog of a **permanent-worker “Design A”** dispatch pool, not
max raw enqueue/dequeue RPS. Product latency claim is closed-loop
**submit→process-return p99 ≤ 5 ms** with light simulated work — **not** an
open-loop publisher-accept SLA.

## AWP numbers (local)

From [`BENCHMARKS.md`](BENCHMARKS.md) (M3 Ultra, larger runs) and a
`make check-bench` sample around the same period:

| Path | Metric | Published (larger run) | Sample local run |
|------|--------|------------------------|------------------|
| Full pool `bench_dispatch` | p50 / p99 latency (submit→process return) | **3 / 9 µs** | **5 / 16 µs** |
| Full pool | throughput | **~586k msg/s** | **~362k msg/s** |
| SPSC ring only | throughput | **~24.7M ops/s** | **~25.4M ops/s** |
| MPSC ring only | throughput | **~6.3M ops/s** | **~5.9M ops/s** |
| Open-loop mock 1k/s | submit→accept p99 | — | **~54 µs** |
| Gate | pool p99 ≤ 5 ms, drops=0 | **PASS** (large margin) | **PASS** |

### Caveats (always restate when quoting)

- Latency includes **light simulated callback work**.
- Workload is mostly **closed-loop burst**, not open-loop sparse 1–5k msg/s with cold wakeups.
- **Not** real publisher-accept / wire SLA evidence (`bench_openloop` is mock only).
- Numbers are single-host / single-run; use for relative mode cost, not cloud SLAs.

So AWP’s practical claim is: **microsecond-class dispatch** on a laptop-class
closed loop, with a **5 ms p99 safety bar** for the intended market-data rate
regime (order of thousands of msg/s per process path).

## Market landscape (by category)

### 1. Pure queues (not a full pool)

| Solution | Typical public range | vs AWP ring |
|----------|----------------------|-------------|
| **moodycamel** ConcurrentQueue / ReaderWriterQueue | Often **tens of M** ops/s; SPSC variants cited much higher on favorable hardware | AWP SPSC ~**25M** is **same order**, usually not “queue champion” |
| **Folly** / **TBB** concurrent queues | Competitive multi-producer; often lower than best SPSC specialty queues | AWP MP* modes are solid mid-pack, not record-setting |
| **LMAX Disruptor** (Java) | Classic hop latencies **~tens of ns** mean; high millions msg/s in favorable setups | Pure hop latency can beat AWP ring; **different product** (pipeline pattern, JVM, different workload) |
| Mutex + condvar queues | Often **µs–ms** under contention | AWP atomics are expected **faster** on hot path |

**Takeaway:** On raw queue thr, AWP is a **credible custom ring**, not the
market’s absolute fastest queue. That is fine: the pool path is dominated by
**frame copy + callback + sharding**, not empty push/pop.

### 2. General thread pools / executors

| Solution | Performance character | vs AWP |
|----------|----------------------|--------|
| **Boost.Asio** `thread_pool`, **libuv** pool | Good general async I/O; scheduling overhead often **higher** for tiny tasks | AWP should win **small, hot market-data callbacks** when sharded correctly |
| **Folly** CPUThreadPoolExecutor | Mature, work-stealing, production-proven | Often better **load balance**; can **break key-local FIFO** unless you shard externally |
| **Intel TBB** | Excellent parallel algorithms / work-stealing | Same: great compute fan-out, **wrong default** for per-symbol FIFO |
| **Tokio / Rayon / std executors** | Strong ecosystems | AWP’s edge is **C + permanent shard + zero-drop backpressure + quarantine contract**, not peak steal efficiency |

**Takeaway:** General pools often **match or beat AWP on bulk CPU work**. AWP
is aiming at **ordering + isolation + predictable lifecycle**, with latency
“good enough” (µs) for MD dispatch, not work-stealing peak FLOPS.

### 3. Market-data / trading-oriented systems

| Class | Typical latency / thr | Relation to AWP |
|-------|----------------------|-----------------|
| **Venue / kernel-bypass stacks** (Solarflare, DPDK, Aeron, Chronicle, custom C++) | Wire / IPC often **ns–low µs** end-to-end on tuned hosts | **Out of AWP’s league** if the bottleneck is NIC→app |
| **Sharded permanent workers** (many in-house “Design A” pools) | Same shape as AWP: **µs–low ms** p99 under load | **Same product category**; AWP is an open C packaging of that pattern |
| **Bus + drop-on-full** MD buses | Can show higher thr by **dropping** | AWP deliberately **blocks instead of drop** — thr under overload is lower by design |

**Takeaway:** Against “real market” HFT plumbing, AWP is a **middle-layer
dispatch library**, not the low-latency path. Against the common internal
**symbol-sharded worker pool**, AWP’s numbers look **in the expected band** for
software-only, non-pinned laptop benches.

## Side-by-side (conceptual)

| Dimension | AWP (measured / design) | Typical general pool | Typical HFT/MD stack | Fastest pure queue |
|-----------|-------------------------|----------------------|----------------------|--------------------|
| Empty queue thr | ~6–25M ops/s | N/A (tasks heavier) | Often custom rings | Often **higher** SPSC |
| Full dispatch latency | **~3–20 µs** p50–p99 closed loop | Often higher for tiny tasks | Can be **ns–low µs** e2e | N/A |
| Per-key FIFO | **Built-in** (hash shard) | You build it | Often built-in | You build it |
| Overload policy | **Block, never drop** | Varies | Often drop / coalesce | Drop or block |
| Stuck callback | **Quarantine + process recycle** | Cancel/detach or hang | Process isolation | N/A |
| Production maturity | Young library, strong contract tests | High (Boost/Folly/TBB) | High in shops that own it | High (moodycamel etc.) |
| Target rate | Comfortable at **1–5k+ msg/s** with huge p99 margin to 5 ms | Same or higher | Wire rates much higher | Millions of empty ops |

## Where AWP is strong (performance-relevant)

1. **Latency budget for its use case** — p99 ~10–20 µs vs a **5 ms** product bar → roughly **250–500× headroom** on local microbench.
2. **Throughput vs design load** — hundreds of k msg/s closed-loop vs typical MD aggregate of thousands–tens of thousands/s for many adapters → **not queue-bound** at that rate if callbacks stay light.
3. **Zero-drop backpressure** — avoids “fast but lossy” results that look good on thr charts and fail correctness.
4. **Sharding preserves FIFO** without a global lock — the usual correctness win that also avoids a contended single queue.

## Where market solutions are stronger

1. **Raw queue speed** — moodycamel / Disruptor-class designs often win empty SPSC/MPMC thr and hop latency.
2. **Ecosystem + battle history** — Folly, TBB, Asio, Aeron/Chronicle.
3. **Absolute latency to market** — kernel bypass, shared-memory IPC, language-specific rings.
4. **Load balancing** — work-stealing pools better for **uneven CPU work** that is **not** key-ordered.
5. **Fair comparison gap** — no published AWP vs X on the **same** box, pinning, message size, and callback cost.

## Bottom line

| Question | Answer |
|----------|--------|
| Is AWP “faster than market thread pools”? | **Often for tiny sharded MD dispatch**; **not** for general compute or work-stealing. |
| Is AWP “faster than market queues”? | **Usually no** on empty thr; **yes enough** that the queue is not the bottleneck once you copy a frame and run `process()`. |
| Is AWP competitive with HFT stacks? | **No** — wrong layer. |
| Does AWP meet its own performance product claim? | **Yes, comfortably** (µs vs 5 ms p99 gate, drops=0). |
| Fair market ranking today? | **Mid-tier custom C dispatch pool**: solid µs-class path, strong lifecycle/correctness story; **not** a published latency-leader library. |

## Fair head-to-head (if revisited later)

A useful competitive scorecard would be a small harness: same host, pinned
cores, same payload size, same “no-op vs ~1 µs work” callbacks, comparing:

1. AWP (default MPSC, fixed workers)
2. Mutex + condvar pool (baseline)
3. Folly/Asio-style pool with **external** shard for key FIFO
4. moodycamel (or equivalent) + permanent worker threads

Until that exists, treat absolute cross-library thr/latency numbers as
**order-of-magnitude guidance only**.

## Related docs

- [`BENCHMARKS.md`](BENCHMARKS.md) — captured AWP microbench dumps
- [`DESIGN.md`](DESIGN.md) — architecture and lifecycle contract
- [`KNOWN_ISSUES.md`](KNOWN_ISSUES.md) — residual S3 nits (non-performance gate)
- [`../README.md`](../README.md) — product overview and build targets

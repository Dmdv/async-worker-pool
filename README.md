# async-worker-pool (AWP)

**Sharded low-latency dispatch worker pool in C** — preallocated `pthread` workers, bounded per-worker queues, stable hash sharding for per-`(feed, symbol)` FIFO, blocking backpressure (zero drops), fault-isolated process callbacks, supervisor heartbeats, and bounded shutdown.

Designed as the C equivalent of a permanent-worker market-data dispatch stage (target: p99 ingest→publish-accept ≤ 5 ms at ~1–5k msg/s).

## Features

- **N fixed workers** created once (never per message)
- **Producer-side shard**: FNV-1a(`feed || 0x1F || symbol`) → worker index
- **Bounded atomic rings** — **SPSC / MPSC / SPMC / MPMC** (`ring_mode`), sequence protocol, spin/yield backpressure, **never drop**
- **Preallocated frame pool** (lock-free freelist) — no `malloc`/`free` on the hot path
- **Dedicated broadcast workers** for mark-price / funding-style feeds
- **Soft fault isolation**: `process()` errors recycle the frame and continue
- **Supervisor**: restarts dead/stalled workers; per-worker metrics
- **Bounded shutdown** with hard deadline (portable, no Linux-only APIs)
- **Runtime gate**: `AWP_ENABLED=1`

## Quick start

```bash
make          # libawp.a + tests + bench + example
make check    # full suite
```

```c
#include "awp/awp.h"

static int on_frame(const awp_frame_t *f, void *user) {
    /* e.g. natsConnection_PublishAsync(...) */
    (void)user; (void)f;
    return 0; /* non-zero = soft error; worker continues */
}

int main(void) {
    awp_config_t cfg;
    awp_pool_t *pool = NULL;

    awp_config_init(&cfg);
    cfg.n_workers = 32;           /* skew headroom, not core count */
    cfg.queue_capacity = 256;
    cfg.frame_pool_size = 4096;
    cfg.process = on_frame;

    awp_pool_create(&cfg, &pool);
    awp_submit(pool, "trades", "BTCUSDT", payload, len, 0);
    awp_pool_shutdown(pool);
    awp_pool_destroy(pool);
    return 0;
}
```

See `examples/simple_publish.c` for multi-reader usage.

## Layout

```
include/awp/awp.h     Public API
src/                  ring, frame pool, shard, worker, supervisor, pool
tests/                unit + supervisor + e2e + lifecycle
bench/                latency/throughput micro-benchmark
examples/             mock publish demo + per-mode demos
docs/DESIGN.md        Architecture, sizing, test matrix
docs/DIAGRAMS.md      Architecture / lifecycle / ring / supervisor diagrams
docs/BENCHMARKS.md    Local latency & throughput results
docs/diagrams/        Rendered PNG diagrams
```

## Design notes (short)

| Topic | Choice |
|-------|--------|
| Queue | Atomic sequence ring — SPSC/MPSC/SPMC/MPMC via `ring_mode` |
| N workers | Config knob for **hash skew** headroom (e.g. 32), not `#cores` |
| Ordering | Stable hash ⇒ one worker per key ⇒ FIFO by construction |
| Backpressure | Block producer when full; `drops` must stay 0 |
| Shutdown | Signal → drain with deadline → force-stop stuck workers |

Full write-up: [`docs/DESIGN.md`](docs/DESIGN.md) · diagrams: [`docs/DIAGRAMS.md`](docs/DIAGRAMS.md) · benches: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) · Codex Pass 3: [`docs/CODEX_IMPLEMENTATION_REVIEW.md`](docs/CODEX_IMPLEMENTATION_REVIEW.md) (**REJECT**).

## Tests, benchmarks, examples (all ring modes)

| Artifact | Covers |
|----------|--------|
| `test_unit` | Baseline FIFO / backpressure / faults (default MPSC) |
| `test_unit_modes [mode\|all]` | FIFO + backpressure + faults × **SPSC/MPSC/SPMC/MPMC** |
| `test_ring_modes` | Raw ring multi-thread stress × all modes |
| `test_e2e` | Multi-reader e2e (MPSC default) |
| `test_e2e_modes [mode\|all]` | E2E × all modes (producer count matches topology) |
| `test_supervisor` | Restart + bounded shutdown |
| `bench_dispatch` | Latency bench (default MPSC) |
| `bench_all_modes` | Pool throughput/p50/p99 table × all modes |
| `bench_ring` | Raw ring ops/s × all modes |
| `example_spsc` / `example_mpsc` / `example_spmc` / `example_mpmc` | Per-mode demos |

```bash
make check                          # full matrix
./build/test_unit_modes spsc        # one mode
./build/test_e2e_modes all
./build/bench_all_modes 5000 1000 all
./build/bench_ring 100000 mpmc
./build/example_spsc && ./build/example_mpsc
./build/example_spmc && ./build/example_mpmc
```

`cfg.ring_mode = AWP_RING_SPSC | MPSC | SPMC | MPMC` — pick the mode that matches **actual** producer/consumer counts.

## License

MIT — see [LICENSE](LICENSE).

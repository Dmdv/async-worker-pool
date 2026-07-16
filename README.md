# async-worker-pool (AWP)

**Sharded low-latency dispatch worker pool in C** — preallocated `pthread` workers, bounded per-worker queues, stable hash sharding for per-`(feed, symbol)` FIFO, blocking backpressure (zero drops), fault-isolated process callbacks, supervisor heartbeats, and bounded shutdown.

Designed as the C equivalent of a permanent-worker market-data dispatch stage. Local microbench target: **p99 submit→process-return ≤ 5 ms** (closed-loop burst with light simulated work; **not** open-loop publisher-accept SLA).

## Features

- **N fixed workers** created once (never per message)
- **Producer-side shard**: FNV-1a(`feed || 0x1F || symbol`) → worker index
- **Bounded atomic rings** — **SPSC / MPSC / SPMC / MPMC** (`ring_mode`), sequence protocol, spin/yield backpressure, **never drop**
- **Preallocated frame pool** (lock-free freelist where supported; 32-bit ABA tag; **64-bit hosts**) — no `malloc`/`free` on the hot path
- **Dedicated broadcast workers** for mark-price / funding-style feeds
- **Soft fault isolation**: `process()` errors recycle the frame and continue
- **Supervisor**: restarts dead/stalled workers; per-worker metrics
- **Bounded shutdown wait** then **quarantine** stuck callbacks (no cancel/detach)
- **Runtime helper**: `awp_runtime_enabled()` / `AWP_ENABLED` (caller-owned; create is not gated)

## Lifetime contract (read this before production use)

| Rule | Meaning |
|------|---------|
| Destroy **exactly once** | One owner thread; after every other handle user has finished |
| External quiescence | Join producers and concurrent shutdown waiters **before** destroy |
| Shutdown deadline | Absolute **wait budget** only — does **not** kill `process()` |
| Quarantine | Sticky; pool storage may leak; treat as **process recycle** |
| `cfg.user` lifetime | Must remain valid until process exit if any callback may still run |

Canonical owner sequence:

1. Stop publishing the handle to new work; set the producer-stop condition.
2. Call `awp_pool_shutdown()` from the designated owner; record its return value and metrics.
3. Join every producer, metrics reader, and concurrent shutdown caller.
4. Call `awp_pool_destroy()` exactly once.
5. If shutdown returned `> 0`, preserve callback-owned state and normally **terminate/recycle the process**. Do not create replacement pools indefinitely in the same process.

## Quick start

```bash
make          # libawp.a + tests + bench + examples
make check    # functional tests only (no latency gates)
make check-all # functional + benches + examples
```

```c
#include "awp/awp.h"

static int on_frame(const awp_frame_t *f, void *user) {
    /* e.g. local publisher enqueue — do not retain f after return */
    (void)user; (void)f;
    return 0; /* non-zero = soft error; worker continues */
}

int main(void) {
    awp_config_t cfg;
    awp_pool_t *pool = NULL;
    int shut;

    awp_config_init(&cfg);
    cfg.n_workers = 32;           /* skew headroom, not core count */
    cfg.queue_capacity = 256;
    cfg.frame_pool_size = 4096;
    cfg.process = on_frame;

    awp_pool_create(&cfg, &pool);
    awp_submit(pool, "trades", "BTCUSDT", payload, len, 0);

    /* Real services: stop producers, shutdown (wakes blocked submits), then join. */
    shut = awp_pool_shutdown(pool);
    /* join producers / metrics threads here if any */
    awp_pool_destroy(pool);
    if (shut > 0)
        return 2; /* quarantined: recycle process in production */
    return 0;
}
```

See `examples/simple_publish.c` for multi-reader usage.

## Layout

```
include/awp/awp.h     Public API
src/                  ring, frame pool, shard, worker, supervisor, pool
tests/                unit + supervisor + e2e + lifecycle + contract drills
bench/                closed-loop microbench + open-loop mock harness
examples/             mock publish demo + per-mode demos
docs/DESIGN.md        Architecture, lifecycle contract, test matrix
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
| Shutdown | Quiesce → close rings/pool → join under wait budget → **quarantine** stuck callbacks |

Full write-up: [`docs/DESIGN.md`](docs/DESIGN.md) · diagrams: [`docs/DIAGRAMS.md`](docs/DIAGRAMS.md) · benches: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) · market positioning: [`docs/PERFORMANCE_COMPARISON.md`](docs/PERFORMANCE_COMPARISON.md).

Non-blocking residual nits (S3): [`docs/KNOWN_ISSUES.md`](docs/KNOWN_ISSUES.md) · [GitHub issues](https://github.com/Dmdv/async-worker-pool/issues).

Historical design-review dumps are **not** published in the repository (local-only under `docs/archive/reviews/` if present).

## Build, test, install

```bash
make lib
make check                 # functional correctness
make check-sanitize        # ASan+UBSan (Clang/GCC)
make check-bench           # optional microbench (not CI gate)
make install PREFIX=/usr/local
pkg-config --cflags --libs awp
```

| Artifact | Covers |
|----------|--------|
| `test_unit` / `test_unit_modes` | FIFO, backpressure, faults × ring modes |
| `test_ring_modes` | Raw ring stress with **exact ID** accounting |
| `test_e2e*` / `test_supervisor` | Multi-reader, restart, sticky quarantine |
| `test_e2e_lifecycle` | Drain + concurrent shutdown |
| `test_teardown_contract` | Clean vs quarantined teardown drills |
| `test_restart_create_fail` | Deterministic restart `pthread_create` failure |
| `bench_dispatch` / `bench_all_modes` / `bench_ring` | Closed-loop microbench |
| `bench_openloop` | Open-loop schedule + mock accept (not a real-publisher SLA) |

`cfg.ring_mode = AWP_RING_SPSC | MPSC | SPMC | MPMC` — match **actual** producer/consumer counts.

## License

MIT — see [LICENSE](LICENSE).

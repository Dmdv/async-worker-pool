# async-worker-pool (AWP)

**Sharded low-latency dispatch worker pool in C** — preallocated `pthread` workers, bounded per-worker queues, stable hash sharding for per-`(feed, symbol)` FIFO, blocking backpressure (zero drops), fault-isolated process callbacks, supervisor heartbeats, and bounded shutdown.

Designed as the C equivalent of a permanent-worker market-data dispatch stage (target: p99 ingest→publish-accept ≤ 5 ms at ~1–5k msg/s).

## Features

- **N fixed workers** created once (never per message)
- **Producer-side shard**: FNV-1a(`feed || 0x1F || symbol`) → worker index
- **Bounded MPSC rings** with **C11 atomics** (sequence protocol) — spin/yield backpressure, **never drop** on full
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
tests/                unit + supervisor + e2e
bench/                latency/throughput micro-benchmark
examples/             mock publish demo
docs/DESIGN.md        Architecture, sizing, test matrix
```

## Design notes (short)

| Topic | Choice |
|-------|--------|
| Queue | Atomic sequence ring (MPSC, CAS enqueue / single-consumer dequeue) |
| N workers | Config knob for **hash skew** headroom (e.g. 32), not `#cores` |
| Ordering | Stable hash ⇒ one worker per key ⇒ FIFO by construction |
| Backpressure | Block producer when full; `drops` must stay 0 |
| Shutdown | Signal → drain with deadline → force-stop stuck workers |

Full write-up: [`docs/DESIGN.md`](docs/DESIGN.md).

## Tests & benchmarks

```bash
./build/test_unit
./build/test_supervisor
./build/test_e2e
./build/bench_dispatch 3000 1000   # msgs keys
AWP_ENABLED=1 ./build/simple_publish
```

Typical laptop result (illustrative):

```
bench_dispatch: msgs=3000 keys=1000 workers=32
  latency_ms: p50≈0.003 p99≈0.01   target p99 ≤ 5.0 → PASS
  drops=0
```

## License

MIT — see [LICENSE](LICENSE).

# Benchmark results

Captured after `make check` (full unit / e2e / lifecycle / examples green).

| Field | Value |
|-------|-------|
| Host | Darwin arm64 · Apple M3 Ultra |
| Date (UTC) | 2026-07-15T12:38:55Z |
| Compiler | Apple clang 21.0.0 (clang-2100.1.1.101) |
| Suite | `make check` + larger bench runs below |

## Dispatch (pool) — bench_dispatch

```
bench_dispatch: msgs=3064 keys=2000 workers=32
  throughput: 585627 msg/s  wall_ms=5.23
  latency_ms: p50=0.0030 p99=0.0090 p99.9=0.0170
  drops=0 process_errors=0
  target: p99 <= 5.0 ms, drops == 0 → PASS
  worst-worker occupancy:
    hottest worker=11 processed=103 global_hwm=2
```

## All ring modes (pool) — bench_all_modes

```
bench_all_modes msgs=20000 keys=2000
mode    prods        msg/s     p50_ms     p99_ms    drops result
SPSC        1       417502     0.0040     0.0130        0 PASS
MPSC        4       300521     0.0160     0.0350        0 PASS
SPMC        1       441755     0.0040     0.0110        0 PASS
MPMC        4       326936     0.0170     2.2200        0 PASS
```

## Ring microbench — bench_ring

```
bench_ring ops=500000
SPSC   prods=1 cons=1 ops=500000 thr=24743901 msg/s errs=0 consumed=500000 PASS
MPSC   prods=4 cons=1 ops=500000 thr=6293583 msg/s errs=0 consumed=500000 PASS
SPMC   prods=1 cons=4 ops=500000 thr=7038685 msg/s errs=0 consumed=500000 PASS
MPMC   prods=4 cons=4 ops=500000 thr=8764088 msg/s errs=0 consumed=500000 PASS
```

## Notes

- Latency is ingest→process wall time (ns → ms).
- PASS criteria: pool p99 ≤ 5 ms, drops == 0.
- Ring thr is pure queue push/pop (no process callback).
- Numbers are single-run local; compare relative mode cost, not absolute cloud SLAs.

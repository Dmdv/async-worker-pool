# Architecture diagrams

Rendered PNGs live in [`diagrams/`](diagrams/). Mermaid sources below also render on GitHub.

## 1. Pool architecture

![Pool architecture](diagrams/01-architecture.png)

```mermaid
flowchart LR
  subgraph Producers
    R1[Venue reader 1]
    R2[Venue reader 2]
    Rn[Venue reader N]
  end

  subgraph Pool["awp_pool"]
    SUB[awp_submit]
    FP[Frame freelist slab]
    SH[FNV-1a shard + broadcast map]
    subgraph Workers
      W0[Worker 0 ring + thread]
      W1[Worker 1 ring + thread]
      Wm[Worker M-1 ring + thread]
    end
    SUP[Supervisor heartbeat]
  end

  CB[process callback e.g. publish]

  R1 --> SUB
  R2 --> SUB
  Rn --> SUB
  SUB --> FP
  FP --> SH
  SH --> W0
  SH --> W1
  SH --> Wm
  W0 --> CB
  W1 --> CB
  Wm --> CB
  SUP -. restart / metrics .-> W0
  SUP -. restart / metrics .-> W1
  SUP -. restart / metrics .-> Wm
```

## 2. Submit → process path

![Submit path](diagrams/02-submit-path.png)

```mermaid
sequenceDiagram
  participant P as Producer
  participant Pool as awp_pool
  participant FP as Frame pool
  participant Ring as Worker ring
  participant W as Worker thread
  participant CB as process()

  P->>Pool: awp_submit(feed, symbol, payload)
  Pool->>Pool: lifecycle==RUNNING?
  Pool->>Pool: active_submits++
  Pool->>FP: acquire frame (block if empty)
  FP-->>Pool: frame*
  Pool->>Pool: copy feed/symbol/payload
  Pool->>Pool: shard = FNV-1a % N
  Pool->>Ring: enqueue(frame*) block if full
  Ring-->>Pool: ok
  Pool->>Pool: active_submits--
  Pool-->>P: 0

  W->>Ring: dequeue
  Ring-->>W: frame*
  W->>CB: process(frame)
  CB-->>W: 0 or soft error
  W->>FP: release frame
  W->>W: heartbeat / metrics
```

## 3. Lifecycle state machine

![Lifecycle](diagrams/03-lifecycle.png)

```mermaid
stateDiagram-v2
  [*] --> RUNNING: awp_pool_create
  RUNNING --> QUIESCING: awp_pool_shutdown
  QUIESCING --> DRAINING: active_submits == 0
  note right of QUIESCING: reject new submits
  DRAINING --> STOPPED: workers drained / joined
  note right of DRAINING: close rings + frame pool
  STOPPED --> [*]: awp_pool_destroy
  RUNNING --> RUNNING: concurrent shutdown waits for STOPPED
```

**States (post lifecycle S0 fixes):**

| State | Meaning |
|-------|---------|
| `RUNNING` | Accepts submits |
| `QUIESCING` | Rejects new work; waits for in-flight `active_submits` |
| `DRAINING` | Rings/pool closed; workers drain to empty; joins |
| `STOPPED` | Safe to destroy (or leak quarantined workers) |

## 4. Ring concurrency modes

![Ring modes](diagrams/04-ring-modes.png)

```mermaid
flowchart TB
  subgraph Modes["Ring modes (Vyukov sequence cells)"]
    direction LR
    SPSC["SPSC — 1 prod → 1 cons — enq store / deq store"]
    MPSC["MPSC default — many → 1 — enq CAS / deq store"]
    SPMC["SPMC — 1 → many — enq store / deq CAS"]
    MPMC["MPMC — many → many — enq CAS / deq CAS"]
  end

  subgraph Cell["Per cell"]
    SEQ[sequence atomic]
    DATA[data pointer]
  end

  Modes --> Cell
  FULL[Full: spin then condvar park — never drop]
  EMPTY[Empty: spin then park consumer]
  Cell --> FULL
  Cell --> EMPTY
```

Wrong mode for actual concurrency is **UB**. Pool default is **MPSC**.

## 5. Supervisor restart

![Supervisor](diagrams/05-supervisor.png)

```mermaid
flowchart TD
  SUP[Supervisor tick]
  SUP --> HB{Worker alive and progressing?}
  HB -->|yes| MET[Update metrics]
  HB -->|thread exited| RESTART[Join thread]
  HB -->|stall: depth>0 no progress| STOP[Cooperative stop request]
  STOP --> PROG{Progress resumed?}
  PROG -->|yes| MET
  PROG -->|no| QUAR[Quarantine: close shard + frame pool — leak on destroy]
  RESTART --> LIFE{lifecycle still RUNNING?}
  LIFE -->|no| CLOSE[Keep/re-close ring]
  LIFE -->|yes| REOPEN[awp_ring_reopen — keep storage + backlog]
  REOPEN --> LIFE2{still RUNNING?}
  LIFE2 -->|no| CLOSE
  LIFE2 -->|yes| SPAWN[pthread_create worker]
  SPAWN -->|ok| MET
  SPAWN -->|fail| QUAR
  CLOSE --> MET
  QUAR --> MET
```

Restart **reopens** the ring only while `RUNNING`; if shutdown wins mid-restart, the ring is **re-closed** so producers wake. Stuck callbacks are never cancelled/detached.

## Related docs

- [`DESIGN.md`](DESIGN.md) — full architecture write-up
- [`BENCHMARKS.md`](BENCHMARKS.md) — measured latency/throughput
- Historical S0/S1 dumps: local-only under `docs/archive/reviews/` if present (untracked)

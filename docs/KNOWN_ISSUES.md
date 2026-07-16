# Known residual issues

Tracked **non-blocking** residuals after the final formal gate (**ACCEPT** at
`71395f6` / re-review6). These are **S3** hygiene and coverage items — not
library-internal UAF/deadlock under the lifetime contract.

Live tracker: [Dmdv/async-worker-pool issues](https://github.com/Dmdv/async-worker-pool/issues).

| ID | Severity | Summary | GitHub |
|----|----------|---------|--------|
| AWP-NIT-01 | S3 docs | Join-deadline wording vs `awp_now_ns` clock fallback | [#1](https://github.com/Dmdv/async-worker-pool/issues/1) |
| AWP-NIT-02 | S3 diagnostics | Join-helper setup failures logged generically | [#2](https://github.com/Dmdv/async-worker-pool/issues/2) |
| AWP-NIT-03 | S3 coverage | No fault injection for `setdetachstate` failure | [#3](https://github.com/Dmdv/async-worker-pool/issues/3) |

## AWP-NIT-01 — Clock wording precision

**Issue:** [#1](https://github.com/Dmdv/async-worker-pool/issues/1)

Join-path comments describe pure `CLOCK_MONOTONIC` budgets on all platforms.
`awp_now_ns()` prefers monotonic time and falls back to `CLOCK_REALTIME` only
when `CLOCK_MONOTONIC` is absent (exotic / non-POSIX). Supported Linux/macOS
qualification targets use monotonic clocks.

**Not a bug** on supported hosts; docs should match the code’s fallback policy.

## AWP-NIT-02 — Join helper diagnostics

**Issue:** [#2](https://github.com/Dmdv/async-worker-pool/issues/2)

Deadline join uses a detached helper. Failures while setting attributes or
creating the helper may still surface through a generic join-failure log path
rather than a setup-specific message. Fail-closed behavior (no joinable helper
leak) is already in place after `71395f6`.

## AWP-NIT-03 — `setdetachstate` fault injection

**Issue:** [#3](https://github.com/Dmdv/async-worker-pool/issues/3)

`pthread_attr_setdetachstate(..., PTHREAD_CREATE_DETACHED)` failure aborts
helper spawn and frees the join box. There is no deterministic test that forces
that branch (unlike restart `pthread_create` injection).

## Out of scope for this list

- Process-recycle after quarantine (product/ops, already in the lifetime contract)
- Open-loop publisher-accept SLA (not claimed; see `docs/BENCHMARKS.md`)
- Historical REJECT rounds (local-only under `docs/archive/reviews/` if present; untracked)

## Gate reference

| Review | HEAD | Verdict |
|--------|------|---------|
| Final re-review5 | `2f29094` | ACCEPT — F1–F12 FIXED |
| Final re-review6 | `71395f6` | ACCEPT — named S3 residuals closed as scoped; this file tracks leftovers |

Scratch review bodies are local operator artifacts (not committed).

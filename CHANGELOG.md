# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-07-15

### Added

- Lifecycle contract productization: owner stop/join/destroy protocol; quarantine as process-recycle; `cfg.user` lifetime rules in `awp.h` and README.
- Functional vs bench separation: `make check` (correctness), `make check-bench`, `make check-sanitize`.
- Exact-ID ring accounting in `test_ring_modes`.
- Teardown contract drills (`test_teardown_contract`) and restart-create failure injection (`test_restart_create_fail`, test-only hooks).
- Open-loop mock harness `bench_openloop` (not real-publisher SLA evidence).
- Install + pkg-config: `make install`, `awp.pc`.
- GitHub Actions CI: Linux GCC functional + ASan/UBSan.

### Changed

- `shutdown_deadline_ms` documentation: absolute wait budget, not forced callback termination.
- DESIGN.md: canonical contract/assurance; historical Pass tables demoted to index.

### Notes

- ABI 0.x: static library first; shared SONAME deferred.
- Public qualification target: 64-bit hosts; finite freelist ABA tag assumed.

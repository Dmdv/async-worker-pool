## Summary of Changes

<!-- Provide a brief, concise summary of the changes introduced in this PR. -->

---

## Key Invariants Checked

- [ ] **Zero-Allocation:** No dynamic heap allocations on the critical hot path.
- [ ] **Memory Safety:** Tested clean under AddressSanitizer (ASan), UBSan, and LSan (`make check-sanitize`).
- [ ] **Concurrency & Ordering:** Explicit `acquire`/`release` memory orderings verified; no data races.
- [ ] **Rust FFI:** `awp-rs` unit tests (`make check-rust`) and benchmark (`make bench-rust`) pass.
- [ ] **Documentation:** Updated relevant markdown docs and tables in English.

---

## Test Verification

```bash
# Functional tests
make check

# Sanitizers suite
make check-sanitize

# Rust FFI bindings
make check-rust
```

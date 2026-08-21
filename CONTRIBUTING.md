# Contributing

Contributions are welcome. Behavioural changes should include a focused regression test
and update `docs/upstream-compatibility.md` when they change compatibility status.

## Local checks

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo doc --workspace --all-features --no-deps
```

Use small, reviewable commits. Public APIs require rustdoc and must not use `unsafe` unless
the safety invariant is documented and a safe design is demonstrably insufficient.

Bug reports should include the Rust version, operating system, minimal reproduction, actual
behaviour, expected upstream behaviour, and the relevant upstream commit or test.

# Testing strategy

The frozen upstream target contains 163 Vitest `it()` scenarios across core, timer,
logger-console, loader, include, and HMR. Rust tests do not mirror that count one-for-one:
one integration test often covers several upstream assertions, while additional tests exercise
Rust-only cancellation, concurrency, ownership, rollback, and filesystem safety constraints.

The current workspace has 114 named test scenarios across these layers:

| Layer | Scenarios | Focus |
| --- | ---: | --- |
| `cordis-core` | 37 | root contexts, events, effects, fibers, cancellation-safe lifecycle, services, readiness, isolation, layered intercepts, logging, reflection, association, invocation |
| loader/config/HMR | 56 | started creation, self-update/dispose persistence, cancellation, delayed readiness barriers, partial updates, recursive interpolation, reconciliation, weak observer ownership, realm permutations and garbage collection, group moves and locate, file watching and drop cleanup, JSON/YAML tags, anchors, duplicate-key recovery, dependency batches, serialized HMR transactions, events and content detection |
| timer/utils/logger-console | 17 | one-shot self-cleanup, cancellation, streams, schedulers, effect ownership, rendering and level selection |
| facade/scaffolder | 4 | core and full-feature exports, safe project creation |

## Required checks

Start with the smallest affected test target, then run the workspace gate for shared runtime or
cross-crate changes:

```sh
cargo fmt --all --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

CI runs the test and lint gates on Linux, macOS, and Windows, and separately checks the declared
Rust 1.85 minimum supported version.

## High-risk invariants

- Effects dispose once, in reverse ownership order, and rollback if setup or registration fails.
- A provided service is externally visible only while its owner fiber is active.
- Concurrent provider/config updates never expose mixed dependency snapshots.
- Loader identity is stable by entry id; unchanged entries are not restarted.
- Include and HMR failures restore the last active graph without partial replacements.
- Timer and watcher tasks terminate on explicit or owning-context disposal.
- Dropping a started public lifecycle/effect future does not strand a partial transition.

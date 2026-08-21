# Changelog

All notable changes are documented here. This project follows Semantic Versioning and uses
the structure from Keep a Changelog.

## [Unreleased]

### Added

- Rust workspace and public `cordis` facade.
- Conflict-free crates.io package namespace under `cordis-rs-*` while preserving concise Rust
  library names; the facade uses `cordis-rs-framework` because `cordis-rs` is already occupied.
- Fiber-scoped effects, plugins, events, active-provider injection, isolation, tracked services,
  typed accessors, callable extensions, and logging.
- Incremental loader groups, JSON/YAML includes with recovery, transactional HMR with a debounced
  polling watcher, timers, utilities, and console logs.
- Typed loader partial updates, host-provided recursive expression evaluation, stable entry-local
  realms, content-aware HMR polling, and evaluator propagation through includes.
- Cancellation-safe fiber/effect transitions and self-cleaning timeout/interval-stream effects.
- Loader started creation, readiness barriers, self-update/self-dispose persistence, and
  cancellation-safe owner callbacks.
- Debounced Include file watching and Windows `ReplaceFileW` atomic replacement.
- YAML `!!js` expression round trips, watcher-independent Include lifecycle persistence, exact
  delayed loader readiness barriers, fiber state subscriptions, and serialized concurrent HMR
  transactions.
- Weak loader snapshots and Include drop cleanup that prevent persistence observers from retaining
  a loader/runtime cycle.
- `create-cordis` application scaffolder.
- Compatibility tests, strict CI, docs.rs/readme package metadata, documentation, licensing,
  security, and contribution policy.

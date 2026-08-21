# Releasing

## Required gates

```sh
cargo fmt --all --check
cargo test --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

All workspace crates share one version and are published in dependency order:

1. `cordis-rs-core`
2. `cordis-rs-group`, `cordis-rs-loader`, `cordis-rs-logger-console`, `cordis-rs-timer`,
   `cordis-rs-utils`
3. `cordis-rs-include`, `cordis-rs-hmr`
4. `cordis-rs-framework`, `cordis-rs-create`

`cargo package -p cordis-rs-core --registry crates-io` can validate the first artifact before any
release exists. Dependent package validation requires the matching `cordis-rs-core` version to be
present in the configured registry; this is Cargo's registry dependency rule, not a local path
resolution failure.

Verify the workspace repository URL resolves to the public release commit; never substitute an
upstream-owned repository URL.

Never publish from a dirty tree. Verify that `NOTICE`, `LICENSE`, the frozen upstream commit,
the compatibility matrix, and the changelog all match the release tag before publishing.

# Cordis for Rust

This workspace is an independent Rust port of
[`cordiverse/cordis`](https://github.com/cordiverse/cordis), frozen at upstream commit
`8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`.

The compatibility target is behavioural: every public upstream module and observable
test behaviour is tracked in [`docs/upstream-compatibility.md`](docs/upstream-compatibility.md).
The frozen 163-scenario inventory and closure gate are in
[`docs/upstream-test-map.md`](docs/upstream-test-map.md).
The scenario-by-scenario mapping to named Rust tests is in
[`docs/upstream-scenario-evidence.md`](docs/upstream-scenario-evidence.md).
The complete public-export mapping, including typed and host-runtime boundaries, is in
[`docs/upstream-api-map.md`](docs/upstream-api-map.md).
Rust APIs use explicit types and futures in places where the TypeScript implementation
relies on proxies, symbols, or structural typing.

## Crates

- `cordis-rs-framework` (`cordis`): public facade with opt-in integration features
- `cordis-rs-core` (`cordis_core`): contexts, fibers, effects, events, services, plugins, and logging
- `cordis-rs-loader`, `cordis-rs-group`, `cordis-rs-include`: hierarchical configuration
- `cordis-rs-hmr`: transactional dependency-aware hot replacement
- `cordis-rs-timer`, `cordis-rs-utils`, `cordis-rs-logger-console`: standard services
- `cordis-rs-create`: safe application scaffolding (`create-cordis`)

The crates.io package names use the `cordis-rs` namespace; the facade is published as
`cordis-rs-framework` because `cordis-rs` itself is already owned by another publisher. Rust
library names remain concise. For example:

```toml
[dependencies]
cordis = { package = "cordis-rs-framework", version = "0.1", features = ["full"] }
```

## Example

```rust
use cordis::{Context, Plugin};
use serde_json::Value;

# async fn example() -> cordis::Result<()> {
let context = Context::new();
let plugin = Plugin::new("hello", |context, _| async move {
    context.logger().info("hello from Cordis", vec![])?;
    Ok(None)
});
let fiber = context.plugin(plugin, Value::Null).await?;
fiber.dispose().await?;
# Ok(())
# }
```

## Development

```sh
cargo test -p cordis-core
cargo fmt --all --check
cargo clippy -p cordis-core --all-targets -- -D warnings
```

For release validation and publication order, see
[`docs/releasing.md`](docs/releasing.md), and see [`docs/testing.md`](docs/testing.md) for the
cross-platform test strategy and high-risk invariants. Local behavioural closure against the
frozen upstream target is complete; the GitHub Actions platform matrix remains the release gate.

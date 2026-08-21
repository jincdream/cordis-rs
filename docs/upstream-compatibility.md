# Upstream compatibility

- Upstream: `cordiverse/cordis`
- Frozen commit: `8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`
- Compatibility definition: matching public behaviour and ported upstream tests.

| Upstream package | Rust crate/module | Status |
| --- | --- | --- |
| `cordis` core | `cordis-rs-core` (`cordis_core`) | Public surface mapped; local behavioural closure complete |
| `@cordisjs/utils` | `cordis-rs-utils` (`cordis_utils`) | Implemented; parity tests passing |
| `@cordisjs/timer` | `cordis-rs-timer` (`cordis_timer`) | Callback/promise-equivalent/stream timers, schedulers, cancellation, one-shot self-cleanup, and fiber disposal implemented |
| `@cordisjs/logger-console` | `cordis-rs-logger-console` (`cordis_logger_console`) | Implemented; parity tests passing |
| `@cordisjs/loader` | `cordis-rs-loader` (`cordis_loader`) | Started creation, readiness predicates, self-update/dispose persistence, typed partial updates, host-evaluated recursive config interpolation, incremental reconciliation, moves, stable realms, inherited contexts, and lifecycle implemented |
| `@cordisjs/include` | `cordis-rs-include` (`cordis_include`) | JSON/YAML including `!!js`, patches, automatic lifecycle persistence, debounced file watching, recovery, and cross-platform atomic replacement implemented |
| `@cordisjs/group` | `cordis-rs-group` (`cordis_group`) | Implemented |
| `@cordisjs/hmr` | `cordis-rs-hmr` (`cordis_hmr`) | Dependency propagation, reload/change events, content-aware polling watcher, serialized batched replacement, linked lookup, pre-mutation build rollback, observable apply failure, and later recovery implemented |
| `create-cordis` | `cordis-rs-create` (`cordis_create`) | Rust application scaffolding implemented |

## Core behaviour matrix

| Area | Upstream test files | Rust status |
| --- | --- | --- |
| Events and filtering | `events.spec.ts` | Core paths implemented |
| Effects and disposal | `dispose.spec.ts` | Sync/async disposer paths, cancellation-time rollback, and detached completion after callers stop waiting implemented |
| Plugin registry | `plugin.spec.ts` | Function-style plugins implemented |
| Dynamic injection | `service.spec.ts` | Active-provider visibility, self-access while loading, multi-provider update batching, and lifecycle implemented |
| Context isolation | `isolate.spec.ts` | Implemented |
| Association and shadow contexts | `associate.spec.ts`, `shadow.spec.ts` | Typed association, caller/provider tracking, callable propagation, and child-plugin scope stripping implemented without runtime proxies |
| Reflection/accessors | `reflect.spec.ts` | Explicit typed accessors implemented and tested |
| Invocation/decorators | `invoke.spec.ts`, `decorator.spec.ts` | Context-aware invocation and tracked extensions implemented; `Context::inject` is the explicit Rust lifecycle equivalent of `@Inject` |
| Logger | `logger.spec.ts` | Core paths implemented |
| Fiber transitions | `fiber.spec.ts` | Loading/unloading serialization, cancellation-safe public transitions, failure cleanup, disposer-error handling, and concurrent dependency/config updates implemented and tested |

The TypeScript API uses runtime proxies and decorators. The Rust port preserves their
semantics through typed handles (`Context`, `ServiceRef<T>`, `TrackedService<T>`,
`Associated<T>`, `Plugin`, `FiberHandle`) and explicit operations:

- `Context::property` and `Context::set_property` perform typed accessor reads and writes;
- `Context::intercept_chain` preserves outer-to-inner override layers while
  `Context::intercept_value` returns the effective innermost value;
- `Context::associated` resolves both `base.member` services and receiver-aware accessors;
- `Context::get_tracked` retains the caller context and the provider fiber context;
- `Context::invoke` passes the call-site context to `CallableService`;
- `TrackedService::extend` creates an unregistered `ExtendableService` value while preserving
  the caller metadata;
- `Context::inject` owns dependency-bound method effects in the same lifecycle shape as
  upstream `@Inject`.

This intentionally avoids emulating unrestricted JavaScript property access in Rust.

Serialized loader values preserve upstream recursive `__jsExpr` interpolation, including YAML
`!!js`/`!js` scalar round trips. Because the Rust
framework is runtime-neutral, applications inject a `ConfigEvaluator`; `IncludeConfig` forwards
the same evaluator into its loader. Raw expressions remain in persisted entry options while
plugins receive evaluated values. Native JavaScript evaluation is therefore a host capability,
not a hard dependency of the framework.

## Verification boundary

Local behavioural closure is complete against the frozen commit: every public export is either
implemented or assigned an explicit language/host boundary, and all 163 upstream scenarios have
named Rust evidence. This is behavioural compatibility, not JavaScript source or reflection-shape
identity: automatic proxy shadow propagation is represented through tracked caller/origin
contexts, while native JavaScript evaluation and dynamic-module compilation are tested host
boundaries through `ConfigEvaluator` and `PluginFactory`. The public-export audit is recorded in
[`upstream-api-map.md`](upstream-api-map.md), and scenario-level evidence is recorded in
[`upstream-scenario-evidence.md`](upstream-scenario-evidence.md). The full GitHub Actions platform
matrix is still required before publishing a release.

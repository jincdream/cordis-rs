# Upstream public API map

This inventory covers the public exports at frozen upstream commit
`8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`. Compatibility means equivalent observable
behaviour in an idiomatic Rust contract; JavaScript reflection, decorators, proxies, Node module
internals, and package-manager bootstrapping are represented by explicit typed or host-provided
boundaries instead of being emulated.

## `cordis` core

| Upstream export | Rust mapping | Evidence or language mapping |
| --- | --- | --- |
| `Context` | `Context`, `IsolationKey` | Context creation, `root()`, base URL, intercept layers, isolation, events, effects, plugins, services, registry inspection, typed reflection and invocation are public methods and covered across `cordis-core/tests` |
| `EventsService`, `Events`, `DispatchMode`, `EventOptions`, `Hook`, `isBailed` | `Context::{on, once, emit, parallel, serial, bail, waterfall}`, `EventOptions`, `EventInvocation`, `EventOutcome`, `DispatchTarget` | Dispatch is expressed as typed methods and `EventOutcome`; `core.rs` covers every upstream dispatch mode, filtering, ordering and disposal |
| `Fiber`, `FiberState`, `Effect`, `Disposable`, `EffectMeta` | `FiberHandle`, `FiberState`, `FiberSnapshot`, `EffectHandle`, `EffectScope`, `Disposer`, `EffectMeta` | `fiber_state.rs` and `core.rs` cover transitions, restart/update, async cleanup, error continuation, cancellation and introspection; `subscribe_state` exposes ordered lifecycle observation |
| `ValidationError`, `CordisError`, `resolveConfig` | `CordisError`, `PluginBuilder::validate_config`, `FiberHandle::update` | Rust validators return typed errors before mutation; config update and rollback paths are tested |
| `Logger`, `LoggerService`, `LoggerLevel`, `Message`, `Exporter`, formatters and color helpers | `Logger`, `LoggerService`, `LogLevel`, `LogType`, `Message`, `Exporter`, `LogValue`, `format_message`, `color_code`, `color` | `logger.rs`, `reflect.rs`, and `cordis-logger-console` tests cover buffering, names, caller tracking, levels, formatting, colors and exporter lifetime |
| `Plugin`, `Plugin.Runtime`, `RegistryService` | `Plugin`, `PluginBuilder`, `PluginId`, `Context::{start_plugin, plugin, has_plugin, plugin_fibers, plugin_ids, delete_plugin}` | Function/object plugin shapes collapse into a builder; stable identity, nested fibers, inspection and deletion are tested |
| `Inject` decorator and injection metadata | `PluginBuilder::{inject, inject_with}`, `Plugin::with_injects`, `Context::inject` | Explicit typed registration replaces decorators; dependency activation and method-effect ownership are tested |
| `Service` and service symbols | `provide`, `provide_shared`, `ServiceRef<T>`, `ServiceGuard`, `TrackedService<T>`, `CallableService`, `ExtendableService` | Rust traits replace symbol-keyed virtual methods; service lifecycle, shadow/caller metadata, callable and extension behaviour are tested |
| Reflection `Property`, `Impl`, `ReflectService` | `register_accessor`, `property`, `set_property`, `associated`, `Associated<T>` | Typed accessor/service association replaces runtime property descriptors and is covered by `reflect.rs` |
| `DisposableList` | Fiber-owned `EffectHandle` collections and `cordis_utils::List<T>` | Rust ownership replaces weak-key cleanup; reverse cleanup and list ownership are tested |
| `Tracker`, `symbols`, `isConstructor`, `joinPrototype`, `isObject`, `getPropertyDescriptor`, `getTraceable`, `withProps`, `createCallable` | `TrackedService<T>`, `Associated<T>`, `CallableService`, `ExtendableService`, Rust type system | These exports implement JavaScript proxy/prototype mechanics. Their observable caller/origin/property/call semantics are exposed through typed handles and tested; raw JS reflection shapes have no Rust runtime meaning |
| `composeError`, `buildOuterStack` | `CordisError` propagation and Rust backtraces | JavaScript stack rewriting is runtime-specific; errors remain typed and preserve their causal operation without fabricating JS frames |

## Loader, groups, and includes

| Upstream export | Rust mapping | Evidence or host boundary |
| --- | --- | --- |
| `EntryOptions`, `Entry` | `EntryOptions`, `EntryUpdate`, `EntryPosition`, `EntrySnapshot`, `Loader` entry methods | Create/update/move/remove/disable/config/self-update/self-dispose/locate paths and compact omission of default serialized fields are covered in loader/include tests |
| `EntryTree`, `EntryGroup`, `Group` | `Loader`, `Loader::wait`, nested `EntryOptions { group: true }`, `cordis_group::plugin` | Stable recursive IDs, all-settled waiting, reconciliation, moves and enable/disable inheritance are tested |
| `Realm`, `LocalRealm`, `GlobalRealm`, `isolate` | `IsolationRule`, `IsolationKey`, loader context rebuilding | Local/global realm identity, shielding, relevant changes, transfer directions, reference-preserving reuse and last-reference garbage collection are tested |
| `Loader`, `Loader.Config` | `Loader`, `PluginResolver`, `RegistryResolver` | Strict `create` and upstream-style `create_started` are both explicit; readiness includes fibers that enter `Loading` after initially being `Pending` |
| `Loader.locate`, `write`, `exit`, `unwrapExports`, `import` | `locate`, `loader/config-update`, `PluginResolver`, `PluginFactory`, host process control | Fiber ancestry and config update events are native; dynamic import/export unwrapping and process exit remain host responsibilities |
| `evaluate`, `interpolate`, `isJsExpr`, `JsExpr` | `ConfigEvaluator`, `interpolate`, `is_expression`, `EXPRESSION_KEY` | Recursive evaluation is tested while raw expressions remain persisted; no JavaScript engine is embedded |
| `ModuleLoaderV1/V2`, `ModuleJob`, `ModuleWrap`, resolve/load request types | `PluginResolver` and HMR `PluginFactory`/dependency registration | Node 22–24 private loader APIs are an explicit host boundary; Rust HMR consumes stable factories and dependency URLs rather than exposing Node internals |
| `PatchOptions`, `Include.Config`, `Include` | `PatchOptions`, `IncludeConfig`, `Include`, `IncludeWatchHandle` | JSON/YAML, patches, initial creation, readonly checks, rollback, atomic write, persistent EntryTree mutations, lifecycle persistence and debounced external refresh are tested |
| YAML `!!js` schema | YAML event parser plus `ConfigEvaluator` | Standard `!!js` input/output round-trips through the host evaluator; anchors, aliases, ordinary YAML scalar typing and duplicate-key rejection are parser-level tested contracts |

## HMR and standard services

| Upstream export | Rust mapping | Evidence or host boundary |
| --- | --- | --- |
| `Hmr`, `Hmr.Config` | `Hmr`, `WatchTarget`, `WatchHandle`, `WatchEvent`, `ChangeOutcome`, `HmrReload` | Dependency propagation, cycles, content-aware debounce, reload events, service/effect replacement, pre-mutation factory rollback, observable apply failure and recovery, and concurrent registry-transaction serialization are tested |
| `TimerService` | `TimerService`, `Interval`, `Scheduled<T>` | `timeout`, `set_timeout`, `sleep`, `interval`, `set_interval`, `interval_stream`, `debounce`, and `throttle` cover callback, future and stream equivalents |
| `ConsoleExporter`, `ConsoleExporter.Config`, `LabelStyle`, `ColorSupportLevel` | `ConsoleExporter`, `ConsoleConfig`, `LabelStyle`, `LabelAlign` | Native/browser console variants share one sink-injected Rust exporter; formatting, levels, labels and custom specifiers are tested |
| `@cordisjs/utils::List<T>` | `cordis_utils::List<T>` | Length, push, values, filter, map and context-owned cleanup are covered |
| `@cordisjs/group` default plugin | `cordis_group::plugin`, `BUILTIN_NAME` | Loader group behaviour is tested through nested configuration |

## Facade and scaffolder

| Upstream export | Rust mapping | Evidence or ecosystem mapping |
| --- | --- | --- |
| package default and named exports | `cordis` facade and feature-gated modules | Facade compile tests verify the public re-exports |
| `create-cordis` default `scaffold` | `cordis_create::scaffold`, `ScaffoldOptions`, `create-cordis` binary | Generates a complete Rust application, validates Cargo names, refuses destructive overwrite and rolls back partial writes |
| `YarnRc`, `StageYarnAgent`, `StageYarnOptions`, `stageYarnBin` | No Rust package-manager API | These configure a JavaScript template's Yarn binary and are outside the Rust framework/toolchain. The Rust scaffolder emits Cargo metadata and never downloads or mutates Yarn state |

No upstream public export is omitted from this inventory. A row marked as a language or host
boundary is not silently unsupported: it identifies the stable Rust contract that carries the
observable framework behaviour and the runtime-specific mechanism deliberately left to the host.

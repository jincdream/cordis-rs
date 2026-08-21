# Upstream test traceability

This table inventories the 163 `it()` scenarios at upstream commit
`8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`. A Rust scenario may consolidate several upstream
assertions, so raw test counts are not used as a parity claim. Completion requires every row to
have scenario-level evidence and no open behavioural difference. The individual scenario mapping
is recorded in [`upstream-scenario-evidence.md`](upstream-scenario-evidence.md).

| Upstream suite | Cases | Rust evidence | Current audit |
| --- | ---: | --- | --- |
| `core/associate.spec.ts` | 5 | `cordis-core/tests/reflect.rs` | Typed service/property association covered; JS inspection shape is represented by typed metadata |
| `core/decorator.spec.ts` | 1 | `cordis-core/tests/reflect.rs::inject_owns_method_effects_for_the_dependency_lifecycle` | Explicit `Context::inject` mapping covered |
| `core/dispose.spec.ts` | 13 | `cordis-core/tests/core.rs::effects_are_nested_reversed_idempotent_and_rolled_back_on_failure`, `fiber_state.rs::{dropping_effect_future_still_finishes_registration_and_cleanup,effect_setup_that_finishes_after_cancellation_rolls_back_its_resources,dropping_manual_effect_disposal_future_does_not_cancel_cleanup}` | Sync return/yield map to registered disposers; async completion, abort, awaited disposal, setup errors and disposer errors are covered by the named lifecycle tests |
| `core/events.spec.ts` | 7 | `cordis-core/tests/core.rs` | Registration, once, filtering, ordering, aggregate failure, bail, waterfall covered |
| `core/fiber.spec.ts` | 8 | `cordis-core/tests/fiber_state.rs`, `core.rs` | Inertia, update, restart, failure and concurrent provider update covered, plus Rust future-cancellation cases |
| `core/invoke.spec.ts` | 2 | `cordis-core/tests/reflect.rs` | Callable service and extension caller context covered |
| `core/isolate.spec.ts` | 3 | `cordis-core/tests/core.rs` | Local/shared service and event isolation covered |
| `core/logger.spec.ts` | 9 | `cordis-core/tests/logger.rs`, `reflect.rs` | Buffer, exporter lifetime, naming, nested tracked caller and formatting covered |
| `core/plugin.spec.ts` | 10 | `cordis-core/tests/core.rs`, `fiber_state.rs` | Functional plugin lifecycle, registry, nesting, inspection, deletion and root disposal covered; JS object-plugin shape is mapped to `PluginBuilder` |
| `core/reflect.spec.ts` | 4 | `cordis-core/tests/reflect.rs` | Typed access, scoped service injection and no-leak lifecycle covered |
| `core/service.spec.ts` | 5 | `cordis-core/tests/core.rs`, `fiber_state.rs`, `reflect.rs` | Pending/multiple inject and traceable effects covered |
| `core/shadow.spec.ts` | 4 | `cordis-core/tests/reflect.rs` | Caller/provider separation, callable context, and child-plugin scope stripping are covered through explicit tracked handles; raw JS proxy mutation has no corresponding Rust operation |
| `hmr/index.spec.ts` | 26 | `cordis-hmr/tests/hmr.rs`, `cordis-hmr/src/lib.rs`, `cordis-include/tests/include.rs::watcher_debounces_config_changes_and_disables_the_live_plugin` | Dependency propagation, serialized concurrent batches, cycles, linked lookup, reload/change events, config-file refresh, build rollback, failed-apply observation and recovery, repeated debounce batches and equal-length file changes covered; native compilation uses tested `PluginFactory` factories |
| `include/patch.spec.ts` | 10 | `cordis-include/tests/include.rs` | Override, disable, insert, name/id/group warnings, JSON/YAML `!!js`, recovery, evaluator propagation, and persistence without a watcher covered |
| `loader/group.spec.ts` | 11 | `cordis-loader/tests/loader.rs` | Nested enable/disable, transfer, intercept inheritance and stable recursive ids covered |
| `loader/index.spec.ts` | 6 | `cordis-loader/tests/loader.rs`, `config.rs` | Started Loading/Pending/Failed observation, exact loader-await readiness including delayed Pending-to-Loading transitions, reconciliation, self-update/self-dispose persistence, cancellation, typed partial updates, locate, and recursive expression interpolation covered |
| `loader/isolate.spec.ts` | 20 | `cordis-loader/tests/loader.rs` | Local/global realms, relevant/irrelevant changes, nested shielding, provider/injector switches and all transfer directions are covered in consolidated sequences |
| `logger-console/index.spec.ts` | 5 | `cordis-logger-console/tests/console.rs` | Error/object/custom formatter/levels/label/diff paths covered |
| `timer/index.spec.ts` | 14 | `cordis-timer/tests/timer.rs` | Timeout, sleep, callback interval, explicit `Stream` return/error/context-disposal semantics, debounce and throttle covered |

## Completion gate

- [x] Replace every provisional compatibility statement with a named Rust test or a documented
  language-level mapping whose observable behaviour is tested.
- [x] Run all tests and strict linting on the declared Rust 1.85 minimum and current stable.
- [ ] Run the CI matrix on Linux, macOS and Windows before release; local success and Windows
  cross-compilation are not equivalent to remote execution.
- [x] Audit all public upstream exports, not only the tests, and record host-provided boundaries such
  as `ConfigEvaluator` and `PluginFactory`; the inventory is maintained in
  [`upstream-api-map.md`](upstream-api-map.md).

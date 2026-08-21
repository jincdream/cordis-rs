# Upstream scenario evidence

This document maps every one of the 163 Vitest `it()` scenarios at upstream commit
`8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4` to named Rust evidence. Several upstream scenarios
share one Rust test when a single deterministic sequence proves the same state transitions.
Paths below are relative to `crates/`.

## Core: effects, fibers, events, and plugins

| Upstream scenario | Rust evidence |
| --- | --- |
| `dispose`: dispose by plugin; dispose manually; yield dispose | `cordis-core/tests/core.rs::effects_are_nested_reversed_idempotent_and_rolled_back_on_failure` |
| `dispose`: async return 1; async return 2 | `cordis-core/tests/fiber_state.rs::dropping_effect_future_still_finishes_registration_and_cleanup`, `dropping_manual_effect_disposal_future_does_not_cancel_cleanup` |
| `dispose`: async yield 1; async yield 4 (await dispose) | `core.rs::effects_are_nested_reversed_idempotent_and_rolled_back_on_failure` and `fiber_state.rs::dropping_effect_future_still_finishes_registration_and_cleanup` |
| `dispose`: async yield 2 (aborted); async yield 3 (aborted) | `fiber_state.rs::effect_setup_that_finishes_after_cancellation_rolls_back_its_resources`, `dropping_effect_future_still_finishes_registration_and_cleanup` |
| `dispose`: return with error; yield with error; async return with error; async yield with error | `core.rs::effects_are_nested_reversed_idempotent_and_rolled_back_on_failure`, including reverse cleanup continuing after the first error |
| `events`: ctx.on(); ctx.once() | `core.rs::event_on_once_prepend_and_manual_disposal` |
| `events`: ctx.parallel(); ctx.emit(); ctx.serial(); ctx.bail() | `core.rs::parallel_waits_for_all_failures_and_serial_bails`, `event_filter_and_global_listener_match_upstream_rules` |
| `events`: ctx.waterfall() | `core.rs::waterfall_composes_in_registration_order_and_can_short_circuit` |
| `fiber`: inertia lock 1; inertia lock 2; inertia lock 3 | `fiber_state.rs::loading_unloading_and_reloading_are_serialized` |
| `fiber`: plugin error | `fiber_state.rs::plugin_failure_cleans_effects_but_preserves_observable_failed_fiber` |
| `fiber`: dispose error | `fiber_state.rs::disposer_failure_is_logged_and_does_not_reject_fiber_disposal` |
| `fiber`: update config on wrapped fiber; restart wrapped fiber | `core.rs::update_restarts_plugin_and_root_dispose_is_idempotent` |
| `fiber`: update config while injected service reloads | `fiber_state.rs::concurrent_provider_and_consumer_updates_never_apply_mixed_config` |
| `plugin`: apply functional plugin; apply object plugin; apply invalid plugin | `core.rs::plugin_lifecycle_nested_cleanup_and_inactive_context`, `injected_plugin_variants_preserve_registry_identity`; the structurally invalid JS shapes are excluded by the Rust `Plugin` type |
| `plugin`: inactive context; nested plugins; root dispose | `core.rs::plugin_lifecycle_nested_cleanup_and_inactive_context`, `update_restarts_plugin_and_root_dispose_is_idempotent` |
| `plugin`: context inspect | `FiberSnapshot::name`, exercised by `plugin_lifecycle_nested_cleanup_and_inactive_context` and `reflect.rs::tracked_services_preserve_caller_and_provider_contexts` |
| `plugin`: ctx.registry; compare snapshot | `core.rs::injected_plugin_variants_preserve_registry_identity` |
| `plugin`: Service.init | Plugin setup/disposer contract in `plugin_lifecycle_nested_cleanup_and_inactive_context` |

## Core: services, reflection, association, and shadowing

| Upstream scenario | Rust evidence |
| --- | --- |
| `associate`: service injection | `cordis-core/tests/reflect.rs::tracked_services_preserve_caller_and_provider_contexts` |
| `associate`: property injection | `reflect.rs::accessors_are_typed_scoped_and_support_receivers` |
| `associate`: associated type - service injection | `reflect.rs::associated_members_can_be_independent_services` |
| `associate`: associated type - accessor injection | `reflect.rs::accessors_are_typed_scoped_and_support_receivers` |
| `associate`: inspect | `TrackedService::{name,caller,origin}` assertions in `tracked_services_preserve_caller_and_provider_contexts` |
| `decorator`: @Inject on class method | `reflect.rs::inject_owns_method_effects_for_the_dependency_lifecycle` |
| `invoke`: functional service | `reflect.rs::callable_services_receive_the_call_site_context` |
| `invoke`: uses the service shadow for callable extensions | `reflect.rs::callable_extensions_preserve_caller_context_and_merge_config` |
| `reflect`: Context.is() | Compile-time `Context` typing plus facade/core construction tests; no fallible runtime downcast is needed |
| `reflect`: access check | `reflect.rs::read_only_accessors_reject_assignment` |
| `reflect`: service injection | `reflect.rs::accessors_are_typed_scoped_and_support_receivers` and `tracked_services_preserve_caller_and_provider_contexts` |
| `reflect`: service inject leak | `reflect.rs::tracked_service_calls_do_not_leak_the_caller_scope_into_child_plugins` |
| `service`: pending inject | `core.rs::service_injection_activates_and_deactivates_fiber` |
| `service`: traceable effect (with inject); traceable effect (without inject) | `reflect.rs::tracked_services_preserve_caller_and_provider_contexts`, `tracked_service_logger_uses_service_name_unless_caller_overrides_it` |
| `service`: compare snapshot | `core.rs::duplicate_service_and_wrong_type_are_safe`, `injected_plugin_variants_preserve_registry_identity` |
| `service`: multiple injects | `fiber_state.rs::concurrent_multi_provider_update_is_applied_as_one_dependency_batch` |
| `shadow`: keeps caller metadata separate from the service shadow | `reflect.rs::tracked_services_preserve_caller_and_provider_contexts` |
| `shadow`: exposes the caller without preserving shadow for noShadow services | `reflect.rs::tracked_service_logger_uses_service_name_unless_caller_overrides_it` |
| `shadow`: exposes the caller to callable services | `reflect.rs::callable_services_receive_the_call_site_context` |
| `shadow`: strips service shadow before creating plugins | `reflect.rs::tracked_service_calls_do_not_leak_the_caller_scope_into_child_plugins` |
| `isolate`: isolated context; shared label | `core.rs::isolated_services_and_shared_labels_are_independent` |
| `isolate`: isolated event | `core.rs::event_filter_and_global_listener_match_upstream_rules` |
| `logger`: keeps the bounded buffer in place and chronological | `cordis-core/tests/logger.rs::bounded_buffer_stays_chronological` |
| `logger`: disposes the exporter that registered the disposer | `logger.rs::exporter_lifetime_levels_and_names_follow_context` |
| `logger`: uses fiber name when called from outside any service; honours explicit name argument; honours intercept name | `logger.rs::exporter_lifetime_levels_and_names_follow_context`, `nested_logger_intercepts_merge_from_outer_to_inner` |
| `logger`: uses service name when called from inside a Service method (regression); still lets outer caller intercept override the service-derived name; uses the innermost service name and restores the outer service; uses service name when called from inside [Service.init] (unchanged behaviour) | `reflect.rs::tracked_service_logger_uses_service_name_unless_caller_overrides_it` |

## Loader and include

| Upstream scenario | Rust evidence |
| --- | --- |
| `loader/index`: loader initiate; loader update | `cordis-loader/tests/loader.rs::replace_reconciles_stable_ids_without_restarting_unchanged_entries`, `group_config_update_reconciles_children` |
| `loader/index`: plugin self-update; plugin self-dispose | `loader.rs::self_updated_entry_persists_the_validated_configuration`, `self_disposed_entry_is_persisted_as_disabled_and_can_be_reenabled`, plus both cancellation variants |
| `loader/index`: pending; resolved | `loader.rs::started_creation_exposes_loading_pending_and_failed_fibers`, `loader_idle_barrier_tracks_a_pending_entry_that_starts_loading_later` |
| `loader/group` basic: initialize; disable inner; disable outer; enable inner; enable outer | `loader.rs::nested_groups_propagate_disable_and_enable` |
| `loader/group` transfer: initialize; enabled -> enabled | `loader.rs::enabled_to_enabled_move_preserves_fiber_and_recursive_ids_remain_consistent` |
| `loader/group` transfer: enabled -> disabled; disabled -> disabled; disabled -> enabled | `loader.rs::moving_entry_across_enabled_boundary_restarts_it` |
| `loader/group` intercept: initialize | `loader.rs::nested_intercepts_preserve_every_override_layer`, `intercept_and_global_isolation_are_inherited` |
| `loader/isolate` basic: initiate | `loader.rs::intercept_and_global_isolation_are_inherited` |
| `loader/isolate` basic: add isolate on injector (relevant); remove isolate on injector (relevant); add isolate on provider (relevant); remove isolate on provider (relevant) | `loader.rs::replacing_local_isolation_reconciles_only_relevant_service_realms` |
| `loader/isolate` basic: add isolate on injector (irrelevant); remove isolate on injector (irrelevant); add isolate on provider (irrelevant); remove isolate on provider (irrelevant) | `loader.rs::irrelevant_isolation_changes_do_not_restart_active_plugins` |
| `loader/isolate` realm: add isolate group; update isolate group (no change) | `loader.rs::changing_group_realm_switches_the_consumer_to_an_existing_provider`, `local_realm_identity_survives_moves_and_context_rebuilds` |
| `loader/isolate` realm: realm reference | `loader.rs::matching_global_realm_connects_provider_and_consumer_across_entries` |
| `loader/isolate` realm: special case: nested realms | `loader.rs::an_inner_explicit_realm_shields_plugins_from_outer_realm_changes` |
| `loader/isolate` realm: special case: change provider | `loader.rs::changing_provider_realm_hands_service_to_the_matching_injector` |
| `loader/isolate` realm: special case: change injector | `loader.rs::changing_group_realm_switches_the_consumer_to_an_existing_provider` |
| `loader/isolate` transfer: initiate; transfer injector into group; transfer provider into group; transfer injector out of group; transfer provider out of group | `loader.rs::provider_and_injector_transfers_reconcile_their_effective_realm` |
| `include`: should load without patches | `cordis-include/tests/include.rs::yaml_and_root_insert_are_supported` |
| `include`: should disable an entry via patch; should override config via patch | `include.rs::patches_cover_nested_override_insert_and_warnings` |
| `include`: should warn on name mismatch and skip patch; should warn on non-existent id | `include.rs::patches_report_all_structural_warnings_and_skip_name_mismatches` |
| `include`: should insert entries into root group; should insert entries into a specific group | `include.rs::yaml_and_root_insert_are_supported`, `patches_cover_nested_override_insert_and_warnings` |
| `include`: should warn when inserting into a non-group entry | `include.rs::patches_report_all_structural_warnings_and_skip_name_mismatches` |
| `include`: should apply multiple patches; should validate name consistency (matching name is ok) | `include.rs::patches_cover_nested_override_insert_and_warnings`, `patches_report_all_structural_warnings_and_skip_name_mismatches` |

## HMR

| Upstream scenario | Rust evidence |
| --- | --- |
| basic single plugin: should load plugin and respond to events | Initial loader assertions in `cordis-hmr/tests/hmr.rs::dependency_change_reloads_all_plugin_instances` |
| basic single plugin: should reload plugin when file changes; should handle reverting file back to original | `hmr.rs::dependency_change_reloads_all_plugin_instances`, `polling_watcher_debounces_rapid_changes_into_one_transaction` (two successive batches) |
| basic single plugin: should emit hmr/reload event on reload; should properly dispose old plugin effects on reload | `hmr.rs::dependency_change_reloads_all_plugin_instances` |
| multiple plugins: should load multiple plugins; should reload changed plugin without affecting others; should handle simultaneous changes to both plugins | `hmr.rs::batched_changes_reload_each_affected_plugin_once`, `multi_plugin_apply_failure_does_not_undo_successful_siblings` |
| dependency chain: should load plugin with dependency; should reload plugin when its dependency changes; should reload plugin when plugin file itself changes | `hmr.rs::dependency_change_reloads_all_plugin_instances`, `dependency_cycles_terminate_and_still_propagate_changes` |
| import error rollback: should load plugin normally; should rollback on syntax error and keep old plugin working; should recover after fixing the error | `hmr.rs::compile_failure_rolls_back_but_apply_failure_remains_recoverable` |
| debounce: should batch rapid changes within debounce window | `hmr.rs::polling_watcher_debounces_rapid_changes_into_one_transaction` |
| config file changes: should handle config file update to disable plugin | `cordis-include/tests/include.rs::watcher_debounces_config_changes_and_disables_the_live_plugin` |
| plugin with service registration: should load service plugin; should reload service plugin on change | `hmr.rs::reload_replaces_registered_services` |
| fiber and entry re-association: should maintain entry association after reload | `cordis-loader/tests/loader.rs::locate_follows_descendant_fibers_and_entry_moves`, `hmr.rs::dependency_change_reloads_all_plugin_instances` |
| rapid successive reloads: should handle many rapid reloads without crashing | `hmr.rs::polling_watcher_debounces_rapid_changes_into_one_transaction`, `concurrent_change_batches_do_not_interleave_registry_mutation` |
| event handler changes: should register new event handlers after reload; should remove old event handlers after reload | `hmr.rs::reload_removes_old_event_handlers_before_installing_new_ones` |
| getLinked: should return linked dependencies for a loaded file; should return empty array for unknown file | `hmr.rs::linked_dependencies_are_sorted_and_unknown_modules_are_empty` |
| runtime error in plugin apply: should handle apply error gracefully without crashing | `hmr.rs::compile_failure_rolls_back_but_apply_failure_remains_recoverable` |
| stash management: should clear stashed files after successful reload | The second independent event in `hmr.rs::polling_watcher_debounces_rapid_changes_into_one_transaction` |

## Timer and console logger

| Upstream scenario | Rust evidence |
| --- | --- |
| `timer/timeout`: basic support | `cordis-timer/tests/timer.rs::timeout_interval_and_disposal_follow_fiber`, `fired_timeout_unregisters_its_effect_before_callback` |
| `timer/timeout`: dispose | `timer.rs::timeout_can_be_cancelled_before_it_fires` |
| `timer/timeout`: promise | `timer.rs::sleep_resolves_at_deadline_and_fails_when_its_context_is_disposed` |
| `timer/interval`: basic support | `timer.rs::timeout_interval_and_disposal_follow_fiber` |
| `timer/interval`: async iterator (manual return); async iterator (break return) | `timer.rs::interval_implements_stream_return_and_context_error_semantics`, `interval_stream_ticks_and_manual_disposal_closes_it` |
| `timer/interval`: async iterator (manual throw); async iterator (break throw) | `timer.rs::interval_implements_stream_return_and_context_error_semantics` (Rust `Stream` returns `Err` then terminates) |
| `timer/interval`: async iterator (context dispose) | `timer.rs::interval_stream_closes_when_its_context_is_disposed` |
| `timer/throttle`: basic support; trailing mode | `timer.rs::debounce_and_throttle_keep_latest_expected_call`, `throttle_supports_trailing_and_no_trailing_modes` |
| `timer/throttle`: disposed | `timer.rs::disposed_schedulers_match_upstream_call_semantics` |
| `timer/debounce`: basic support | `timer.rs::debounce_and_throttle_keep_latest_expected_call` |
| `timer/debounce`: disposed | `timer.rs::disposed_schedulers_match_upstream_call_semantics` |
| `logger-console`: format error | `cordis-logger-console/tests/console.rs::renders_errors_and_applies_per_name_level_overrides` |
| `logger-console`: format object; custom formatter; log levels | `console.rs::renders_objects_custom_formatters_levels_and_multiline_labels`, `renders_errors_and_applies_per_name_level_overrides` |
| `logger-console`: label style | `console.rs::renders_objects_custom_formatters_levels_and_multiline_labels` |

## Rust-only strengthening scenarios

The 114 Rust scenarios also cover contracts without a direct JavaScript test: dropped-future
cancellation, multi-provider update convergence, services hidden during provider loading, invalid
group rollback, self-lifecycle cancellation, delayed Pending-to-Loading readiness, YAML `!!js`
round trips, Windows-safe atomic replacement, repeated watcher batches, concurrent HMR callers,
weak persistence ownership and drop cleanup, compact serialization, safe scaffolder rollback, and
full-feature facade exports.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use cordis_core::{
    Context, CordisError, DispatchTarget, Disposer, EffectMeta, EventCallback, EventOptions,
    EventOutcome, FiberState, Plugin, shared,
};
use parking_lot::Mutex;
use serde_json::{Value, json};

fn ok_listener(counter: Arc<AtomicUsize>) -> EventCallback {
    Arc::new(move |_| {
        let counter = Arc::clone(&counter);
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(EventOutcome::Continue)
        })
    })
}

fn record_disposer(sequence: Arc<Mutex<Vec<usize>>>, value: usize) -> Disposer {
    Arc::new(move || {
        let sequence = Arc::clone(&sequence);
        Box::pin(async move {
            sequence.lock().push(value);
            Ok(())
        })
    })
}

#[tokio::test]
async fn event_on_once_prepend_and_manual_disposal() {
    let context = Context::new();
    let sequence = Arc::new(Mutex::new(Vec::new()));

    let last = Arc::clone(&sequence);
    let normal: EventCallback = Arc::new(move |_| {
        let last = Arc::clone(&last);
        Box::pin(async move {
            last.lock().push(2);
            Ok(EventOutcome::Continue)
        })
    });
    let first = Arc::clone(&sequence);
    let prepended: EventCallback = Arc::new(move |_| {
        let first = Arc::clone(&first);
        Box::pin(async move {
            first.lock().push(1);
            Ok(EventOutcome::Continue)
        })
    });
    let once_count = Arc::new(AtomicUsize::new(0));

    let normal_effect = context
        .on("event", normal, EventOptions::default())
        .unwrap();
    context
        .on(
            "event",
            prepended,
            EventOptions {
                prepend: true,
                global: false,
            },
        )
        .unwrap();
    context
        .once(
            "event",
            ok_listener(Arc::clone(&once_count)),
            EventOptions::default(),
        )
        .unwrap();

    context.emit("event", vec![]).await.unwrap();
    context.emit("event", vec![]).await.unwrap();
    assert_eq!(&*sequence.lock(), &[1, 2, 1, 2]);
    assert_eq!(once_count.load(Ordering::SeqCst), 1);

    normal_effect.dispose().await.unwrap();
    context.emit("event", vec![]).await.unwrap();
    assert_eq!(&*sequence.lock(), &[1, 2, 1, 2, 1]);
}

#[tokio::test]
async fn event_filter_and_global_listener_match_upstream_rules() {
    let root = Context::new();
    let enabled = root.intercept("enabled", json!(true));
    let disabled = root.intercept("enabled", json!(false));
    let enabled_count = Arc::new(AtomicUsize::new(0));
    let disabled_count = Arc::new(AtomicUsize::new(0));
    let global_count = Arc::new(AtomicUsize::new(0));

    enabled
        .on(
            "filtered",
            ok_listener(Arc::clone(&enabled_count)),
            EventOptions::default(),
        )
        .unwrap();
    disabled
        .on(
            "filtered",
            ok_listener(Arc::clone(&disabled_count)),
            EventOptions::default(),
        )
        .unwrap();
    root.on(
        "filtered",
        ok_listener(Arc::clone(&global_count)),
        EventOptions {
            prepend: false,
            global: true,
        },
    )
    .unwrap();

    let target = DispatchTarget::new(None, |listener| {
        listener.intercept_value("enabled") == Some(&json!(true))
    });
    root.emit_to(Some(target), "filtered", vec![])
        .await
        .unwrap();
    assert_eq!(enabled_count.load(Ordering::SeqCst), 1);
    assert_eq!(disabled_count.load(Ordering::SeqCst), 0);
    assert_eq!(global_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_waits_for_all_failures_and_serial_bails() {
    let context = Context::new();
    let completed = Arc::new(AtomicUsize::new(0));
    let slow_completed = Arc::clone(&completed);
    let slow: EventCallback = Arc::new(move |_| {
        let slow_completed = Arc::clone(&slow_completed);
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            slow_completed.fetch_add(1, Ordering::SeqCst);
            Err(CordisError::InvalidConfig("slow".to_owned()))
        })
    });
    let fast: EventCallback =
        Arc::new(|_| Box::pin(async { Err(CordisError::InvalidConfig("fast".to_owned())) }));
    context
        .on("parallel", slow, EventOptions::default())
        .unwrap();
    context
        .on("parallel", fast, EventOptions::default())
        .unwrap();

    let error = context.parallel("parallel", vec![]).await.unwrap_err();
    assert_eq!(error.errors().len(), 2);
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    let skipped = Arc::new(AtomicUsize::new(0));
    let bail: EventCallback =
        Arc::new(|_| Box::pin(async { Ok(EventOutcome::Bail(shared(42_i32))) }));
    context.on("serial", bail, EventOptions::default()).unwrap();
    context
        .on(
            "serial",
            ok_listener(Arc::clone(&skipped)),
            EventOptions::default(),
        )
        .unwrap();
    let EventOutcome::Bail(value) = context.serial("serial", vec![]).await.unwrap() else {
        panic!("listener should bail");
    };
    assert_eq!(value.downcast_ref::<i32>(), Some(&42));
    assert_eq!(skipped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn waterfall_composes_in_registration_order_and_can_short_circuit() {
    let context = Context::new();
    for _ in 0..2 {
        let middleware: EventCallback = Arc::new(|invocation| {
            Box::pin(async move {
                let value = *invocation.arg::<i32>(0).unwrap();
                let outcome = invocation.next(vec![shared(value)]).unwrap().await?;
                let EventOutcome::Bail(next_value) = outcome else {
                    return Ok(EventOutcome::Continue);
                };
                let next_value = *next_value.downcast_ref::<i32>().unwrap();
                Ok(EventOutcome::Bail(shared(value + next_value)))
            })
        });
        context
            .on("waterfall", middleware, EventOptions::default())
            .unwrap();
    }
    let terminal =
        Arc::new(|_: Vec<_>| Box::pin(async { Ok(EventOutcome::Bail(shared(2_i32))) }) as _);
    let EventOutcome::Bail(value) = context
        .waterfall("waterfall", vec![shared(1_i32)], terminal)
        .await
        .unwrap()
    else {
        panic!("waterfall should return a value");
    };
    assert_eq!(value.downcast_ref::<i32>(), Some(&4));

    let skipped = Arc::new(AtomicUsize::new(0));
    let stop: EventCallback =
        Arc::new(|_| Box::pin(async { Ok(EventOutcome::Bail(shared(9_i32))) }));
    context.on("short", stop, EventOptions::default()).unwrap();
    context
        .on(
            "short",
            ok_listener(Arc::clone(&skipped)),
            EventOptions::default(),
        )
        .unwrap();
    let terminal =
        Arc::new(|_: Vec<_>| Box::pin(async { Ok(EventOutcome::Bail(shared(0_i32))) }) as _);
    context.waterfall("short", vec![], terminal).await.unwrap();
    assert_eq!(skipped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn effects_are_nested_reversed_idempotent_and_rolled_back_on_failure() {
    let context = Context::new();
    let sequence = Arc::new(Mutex::new(Vec::new()));
    let sequence_for_setup = Arc::clone(&sequence);
    let effect = context
        .effect("outer", move |scope| {
            let sequence = Arc::clone(&sequence_for_setup);
            async move {
                scope.defer(record_disposer(Arc::clone(&sequence), 1));
                scope.defer(record_disposer(Arc::clone(&sequence), 2));
                Ok(Some(record_disposer(sequence, 3)))
            }
        })
        .await
        .unwrap();
    assert_eq!(
        context.fiber().get_effects(),
        vec![EffectMeta {
            label: "outer".to_owned(),
            children: vec![],
        }]
    );
    effect.dispose().await.unwrap();
    effect.dispose().await.unwrap();
    assert_eq!(&*sequence.lock(), &[3, 2, 1]);
    assert!(context.fiber().get_effects().is_empty());

    let rollback = Arc::new(AtomicUsize::new(0));
    let rollback_setup = Arc::clone(&rollback);
    let error = context
        .effect("failing", move |scope| {
            let rollback = Arc::clone(&rollback_setup);
            async move {
                let disposer: Disposer = Arc::new(move || {
                    let rollback = Arc::clone(&rollback);
                    Box::pin(async move {
                        rollback.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                });
                scope.defer(disposer);
                Err(CordisError::InvalidConfig("setup".to_owned()))
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, CordisError::InvalidConfig(_)));
    assert_eq!(rollback.load(Ordering::SeqCst), 1);

    let sequence = Arc::new(Mutex::new(Vec::new()));
    let setup_sequence = Arc::clone(&sequence);
    let effect = context
        .effect("fallible cleanup", move |scope| {
            let sequence = Arc::clone(&setup_sequence);
            async move {
                scope.defer(record_disposer(Arc::clone(&sequence), 1));
                let failing: Disposer = Arc::new(move || {
                    let sequence = Arc::clone(&sequence);
                    Box::pin(async move {
                        sequence.lock().push(2);
                        Err(CordisError::InvalidConfig("cleanup".to_owned()))
                    })
                });
                scope.defer(failing);
                scope.defer(record_disposer(Arc::clone(&setup_sequence), 3));
                Ok(None)
            }
        })
        .await
        .unwrap();
    assert!(effect.dispose().await.is_err());
    assert_eq!(&*sequence.lock(), &[3, 2, 1]);
}

#[tokio::test]
async fn plugin_lifecycle_nested_cleanup_and_inactive_context() {
    let root = Context::new();
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let child_starts = Arc::clone(&starts);
    let child_stops = Arc::clone(&stops);
    let child = Plugin::new("child", move |_, _| {
        let starts = Arc::clone(&child_starts);
        let stops = Arc::clone(&child_stops);
        async move {
            starts.fetch_add(1, Ordering::SeqCst);
            let disposer: Disposer = Arc::new(move || {
                let stops = Arc::clone(&stops);
                Box::pin(async move {
                    stops.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
            Ok(Some(disposer))
        }
    });
    let nested_child = child.clone();
    let parent = Plugin::new("parent", move |context, _| {
        let child = nested_child.clone();
        async move {
            context.plugin(child, json!(null)).await?;
            Ok(None)
        }
    });

    let fiber = root.plugin(parent, json!(null)).await.unwrap();
    let plugin_context = fiber.context().unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(root.registry_size(), 2);
    fiber.dispose().await.unwrap();
    fiber.dispose().await.unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert_eq!(root.registry_size(), 0);
    assert!(matches!(
        plugin_context.on(
            "late",
            ok_listener(Arc::new(AtomicUsize::new(0))),
            EventOptions::default()
        ),
        Err(CordisError::InactiveEffect)
    ));
}

#[tokio::test]
async fn child_plugins_can_address_the_unshadowed_root_context() {
    let root = Context::new();
    let root_events = Arc::new(AtomicUsize::new(0));
    root.on(
        "root-event",
        ok_listener(Arc::clone(&root_events)),
        EventOptions::default(),
    )
    .unwrap();
    let child = Plugin::new("child", |context, _| async move {
        let shadowed = context
            .isolate("service")
            .intercept("logger", json!({"name": "child"}));
        let root = shadowed.root();
        assert_eq!(root.fiber().identity().0, 0);
        assert!(root.intercept_value("logger").is_none());
        assert!(root.same_isolation(&context.root(), "service"));
        root.emit("root-event", vec![]).await?;
        Ok(None)
    });

    root.plugin(child, Value::Null).await.unwrap();
    assert_eq!(root_events.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn service_injection_activates_and_deactivates_fiber() {
    let root = Context::new();
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let plugin_starts = Arc::clone(&starts);
    let plugin_stops = Arc::clone(&stops);
    let plugin = Plugin::builder("dependent", move |context, _| {
        let starts = Arc::clone(&plugin_starts);
        let stops = Arc::clone(&plugin_stops);
        async move {
            assert_eq!(*context.get::<usize>("foo").unwrap(), 7);
            starts.fetch_add(1, Ordering::SeqCst);
            let disposer: Disposer = Arc::new(move || {
                let stops = Arc::clone(&stops);
                Box::pin(async move {
                    stops.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
            Ok(Some(disposer))
        }
    })
    .inject("foo")
    .build();

    let fiber = root.plugin(plugin, Value::Null).await.unwrap();
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Pending);
    assert_eq!(starts.load(Ordering::SeqCst), 0);

    let service = root.provide("foo", 7_usize).await.unwrap();
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Active);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    service.dispose().await.unwrap();
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Pending);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn isolated_services_and_shared_labels_are_independent() {
    let root = Context::new();
    let first = root.isolate("foo");
    let second = root.isolate("foo");
    let shared_key = root.fresh_isolation_key();
    let shared_one = root.isolate_with("foo", shared_key);
    let shared_two = root.isolate_with("foo", shared_key);

    let root_service = root.provide("foo", 100_i32).await.unwrap();
    let first_service = first.provide("foo", 200_i32).await.unwrap();
    let shared_service = shared_one.provide("foo", 300_i32).await.unwrap();

    assert_eq!(*root.get::<i32>("foo").unwrap(), 100);
    assert_eq!(*first.get::<i32>("foo").unwrap(), 200);
    assert!(second.get::<i32>("foo").is_none());
    assert_eq!(*shared_two.get::<i32>("foo").unwrap(), 300);

    shared_service.dispose().await.unwrap();
    assert!(shared_one.get::<i32>("foo").is_none());
    assert!(shared_two.get::<i32>("foo").is_none());
    first_service.dispose().await.unwrap();
    root_service.dispose().await.unwrap();
}

#[tokio::test]
async fn duplicate_service_and_wrong_type_are_safe() {
    let root = Context::new();
    let service = root.provide("value", 1_i32).await.unwrap();
    let duplicate = root.provide("value", 2_i32).await.unwrap_err();
    assert!(matches!(
        duplicate,
        CordisError::ServiceAlreadyRegistered { .. }
    ));
    assert!(root.get::<String>("value").is_none());
    assert_eq!(*root.get::<i32>("value").unwrap(), 1);
    service.dispose().await.unwrap();
}

#[tokio::test]
async fn update_restarts_plugin_and_root_dispose_is_idempotent() {
    let root = Context::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stopped = Arc::new(AtomicUsize::new(0));
    let plugin_seen = Arc::clone(&seen);
    let plugin_stopped = Arc::clone(&stopped);
    let plugin = Plugin::new("configurable", move |_, config| {
        let seen = Arc::clone(&plugin_seen);
        let stopped = Arc::clone(&plugin_stopped);
        async move {
            seen.lock().push(config);
            let disposer: Disposer = Arc::new(move || {
                let stopped = Arc::clone(&stopped);
                Box::pin(async move {
                    stopped.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            });
            Ok(Some(disposer))
        }
    });
    let fiber = root.plugin(plugin, json!({ "version": 1 })).await.unwrap();
    fiber.update(json!({ "version": 2 })).await.unwrap();
    assert_eq!(seen.lock().len(), 2);
    assert_eq!(stopped.load(Ordering::SeqCst), 1);

    root.fiber().dispose().await.unwrap();
    root.fiber().dispose().await.unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Disposed);
    assert_eq!(root.fiber().snapshot().unwrap().state, FiberState::Active);
}

#[tokio::test]
async fn injected_plugin_variants_preserve_registry_identity() {
    let root = Context::new();
    root.provide("dependency", 1_u8).await.unwrap();
    let plugin = Plugin::new("shared", |_, _| async { Ok(None) });
    let first = root
        .plugin(plugin.with_injects(["dependency"]), Value::Null)
        .await
        .unwrap();
    let second = root
        .plugin(plugin.with_injects(["dependency"]), Value::Null)
        .await
        .unwrap();

    assert_eq!(root.registry_size(), 1);
    assert!(root.has_plugin(&plugin));
    let mut fibers = root
        .plugin_fibers(&plugin)
        .into_iter()
        .map(|fiber| fiber.identity())
        .collect::<Vec<_>>();
    fibers.sort();
    assert_eq!(fibers, vec![first.identity(), second.identity()]);
    assert_eq!(root.plugin_ids(), vec![plugin.id()]);
    assert!(root.delete_plugin(&plugin).await.unwrap());
    assert_eq!(first.snapshot().unwrap().state, FiberState::Disposed);
    assert_eq!(second.snapshot().unwrap().state, FiberState::Disposed);
    assert_eq!(root.registry_size(), 0);
    assert!(!root.has_plugin(&plugin));
    assert!(root.plugin_fibers(&plugin).is_empty());
    assert!(root.plugin_ids().is_empty());
}

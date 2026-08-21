use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use cordis_core::{
    Context, CordisError, Disposer, EventCallback, EventOptions, EventOutcome, FiberState, LogType,
    Plugin,
};
use parking_lot::Mutex;
use serde_json::{Value, json};

#[tokio::test(start_paused = true)]
async fn loading_unloading_and_reloading_are_serialized() {
    let root = Context::new();
    let service = root.provide("foo", 1_u8).await.unwrap();
    let plugin = Plugin::builder("consumer", |_, _| async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let disposer: Disposer = Arc::new(|| {
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                Ok(())
            })
        });
        Ok(Some(disposer))
    })
    .inject("foo")
    .build();
    let fiber = root.start_plugin(plugin, Value::Null).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Loading);

    let removal = tokio::spawn(async move { service.dispose().await });
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Unloading);

    let provider = root.clone();
    let addition = tokio::spawn(async move { provider.provide("foo", 2_u8).await });
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(removal.await.unwrap().is_ok());
    tokio::task::yield_now().await;
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Loading);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    fiber.wait().await.unwrap();
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Active);
    assert!(addition.await.unwrap().is_ok());
}

#[tokio::test]
async fn plugin_failure_cleans_effects_but_preserves_observable_failed_fiber() {
    let root = Context::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let listener_calls = Arc::clone(&calls);
    let plugin = Plugin::new("fallible", move |context, config| {
        let listener_calls = Arc::clone(&listener_calls);
        async move {
            let callback: EventCallback = Arc::new(move |_| {
                let listener_calls = Arc::clone(&listener_calls);
                Box::pin(async move {
                    listener_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(EventOutcome::Continue)
                })
            });
            context.on("event", callback, EventOptions::default())?;
            if !config.get("ok").and_then(Value::as_bool).unwrap_or(false) {
                return Err(CordisError::InvalidConfig("plugin error".into()));
            }
            Ok(None)
        }
    });
    let failed = root
        .start_plugin(plugin.clone(), json!({"ok": false}))
        .unwrap();
    assert!(failed.wait().await.is_err());
    assert_eq!(failed.snapshot().unwrap().state, FiberState::Failed);
    let active = root.plugin(plugin, json!({"ok": true})).await.unwrap();
    root.emit("event", vec![]).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(active.snapshot().unwrap().state, FiberState::Active);
    assert!(
        root.logger_service()
            .buffer()
            .unwrap()
            .iter()
            .any(|message| message.kind == LogType::Error)
    );
}

#[tokio::test]
async fn disposer_failure_is_logged_and_does_not_reject_fiber_disposal() {
    let root = Context::new();
    let plugin = Plugin::new("bad-disposer", |_, _| async {
        let disposer: Disposer =
            Arc::new(|| Box::pin(async { Err(CordisError::InvalidConfig("dispose".into())) }));
        Ok(Some(disposer))
    });
    let fiber = root.plugin(plugin, Value::Null).await.unwrap();
    fiber.dispose().await.unwrap();
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Disposed);
    assert!(
        root.logger_service()
            .buffer()
            .unwrap()
            .iter()
            .any(|message| message.kind == LogType::Error)
    );
}

#[tokio::test]
async fn concurrent_provider_and_consumer_updates_never_apply_mixed_config() {
    let root = Context::new();
    let provider = Plugin::new("provider", |context, config| async move {
        let value = config["value"].as_i64().unwrap();
        context.provide("provider", value).await?;
        Ok(None)
    });
    let applied = Arc::new(Mutex::new(Vec::new()));
    let consumer_applied = Arc::clone(&applied);
    let consumer = Plugin::builder("consumer", move |context, config| {
        let applied = Arc::clone(&consumer_applied);
        async move {
            applied.lock().push((
                *context.get::<i64>("provider").unwrap(),
                config["mode"].as_str().unwrap().to_owned(),
            ));
            Ok(None)
        }
    })
    .inject("provider")
    .build();
    let provider_fiber = root.plugin(provider, json!({"value": 1})).await.unwrap();
    let consumer_fiber = root.plugin(consumer, json!({"mode": "old"})).await.unwrap();
    let (provider_result, consumer_result) = tokio::join!(
        provider_fiber.update(json!({"value": 2})),
        consumer_fiber.update(json!({"mode": "new"}))
    );
    provider_result.unwrap();
    consumer_result.unwrap();
    assert_eq!(
        &*applied.lock(),
        &[(1, "old".to_owned()), (2, "new".to_owned())]
    );
}

#[tokio::test]
async fn concurrent_multi_provider_update_is_applied_as_one_dependency_batch() {
    let root = Context::new();
    let provider = |name: &'static str| {
        Plugin::new(name, move |context, config| async move {
            context
                .provide(name, config["value"].as_i64().unwrap())
                .await?;
            Ok(None)
        })
    };
    let applied = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&applied);
    let consumer = Plugin::builder("consumer", move |context, config| {
        let capture = Arc::clone(&capture);
        async move {
            capture.lock().push((
                *context.get::<i64>("a").unwrap(),
                *context.get::<i64>("b").unwrap(),
                config["mode"].as_str().unwrap().to_owned(),
            ));
            Ok(None)
        }
    })
    .inject("a")
    .inject("b")
    .build();
    let a = root
        .plugin(provider("a"), json!({"value": 1}))
        .await
        .unwrap();
    let b = root
        .plugin(provider("b"), json!({"value": 1}))
        .await
        .unwrap();
    let consumer = root.plugin(consumer, json!({"mode": "old"})).await.unwrap();

    let (a_result, b_result, consumer_result) = tokio::join!(
        a.update(json!({"value": 2})),
        b.update(json!({"value": 2})),
        consumer.update(json!({"mode": "new"})),
    );
    a_result.unwrap();
    b_result.unwrap();
    consumer_result.unwrap();
    assert_eq!(
        &*applied.lock(),
        &[(1, 1, "old".to_owned()), (2, 2, "new".to_owned())]
    );
}

#[tokio::test]
async fn loading_provider_service_is_private_until_activation_finishes() {
    let root = Context::new();
    let consumer_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&consumer_calls);
    let consumer = Plugin::builder("consumer", move |context, _| {
        let calls = Arc::clone(&calls);
        async move {
            assert_eq!(*context.get::<u32>("foo").unwrap(), 42);
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    })
    .inject("foo")
    .build();
    let consumer_fiber = root.plugin(consumer, Value::Null).await.unwrap();
    assert_eq!(
        consumer_fiber.snapshot().unwrap().state,
        FiberState::Pending
    );

    let provided = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let self_access = Arc::new(AtomicBool::new(false));
    let provider = Plugin::new("provider", {
        let provided = Arc::clone(&provided);
        let release = Arc::clone(&release);
        let self_access = Arc::clone(&self_access);
        move |context, _| {
            let provided = Arc::clone(&provided);
            let release = Arc::clone(&release);
            let self_access = Arc::clone(&self_access);
            async move {
                context.provide("foo", 42_u32).await?;
                self_access.store(context.get::<u32>("foo").is_some(), Ordering::SeqCst);
                provided.notify_one();
                release.notified().await;
                Ok(None)
            }
        }
    });
    let provider_fiber = root.start_plugin(provider, Value::Null).unwrap();
    provided.notified().await;

    assert!(self_access.load(Ordering::SeqCst));
    assert!(root.get::<u32>("foo").is_none());
    assert_eq!(consumer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider_fiber.snapshot().unwrap().state,
        FiberState::Loading
    );

    release.notify_one();
    provider_fiber.wait().await.unwrap();
    assert_eq!(provider_fiber.snapshot().unwrap().state, FiberState::Active);
    assert_eq!(consumer_fiber.snapshot().unwrap().state, FiberState::Active);
    assert_eq!(consumer_calls.load(Ordering::SeqCst), 1);
    assert_eq!(*root.get::<u32>("foo").unwrap(), 42);
}

#[tokio::test]
async fn effect_setup_that_finishes_after_cancellation_rolls_back_its_resources() {
    let root = Context::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cleanups = Arc::new(AtomicUsize::new(0));
    let plugin = Plugin::new("slow-effect", {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let cleanups = Arc::clone(&cleanups);
        move |context, _| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let cleanups = Arc::clone(&cleanups);
            async move {
                context
                    .effect("slow", move |scope| async move {
                        started.notify_one();
                        release.notified().await;
                        let disposer: Disposer = Arc::new(move || {
                            let cleanups = Arc::clone(&cleanups);
                            Box::pin(async move {
                                cleanups.fetch_add(1, Ordering::SeqCst);
                                Ok(())
                            })
                        });
                        scope.defer(disposer);
                        Ok(None)
                    })
                    .await?;
                Ok(None)
            }
        }
    });
    let fiber = root.start_plugin(plugin, Value::Null).unwrap();
    started.notified().await;

    let disposal = tokio::spawn({
        let fiber = fiber.clone();
        async move { fiber.dispose().await }
    });
    tokio::task::yield_now().await;
    assert!(!disposal.is_finished());
    release.notify_one();
    disposal.await.unwrap().unwrap();

    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Disposed);
}

#[tokio::test]
async fn dropping_dispose_future_does_not_cancel_the_lifecycle_transition() {
    let root = Context::new();
    let disposing = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let disposed = Arc::new(tokio::sync::Notify::new());
    let plugin = Plugin::new("slow-dispose", {
        let disposing = Arc::clone(&disposing);
        let release = Arc::clone(&release);
        let disposed = Arc::clone(&disposed);
        move |_, _| {
            let disposing = Arc::clone(&disposing);
            let release = Arc::clone(&release);
            let disposed = Arc::clone(&disposed);
            async move {
                let disposer: Disposer = Arc::new(move || {
                    let disposing = Arc::clone(&disposing);
                    let release = Arc::clone(&release);
                    let disposed = Arc::clone(&disposed);
                    Box::pin(async move {
                        disposing.notify_one();
                        release.notified().await;
                        disposed.notify_one();
                        Ok(())
                    })
                });
                Ok(Some(disposer))
            }
        }
    });
    let fiber = root.plugin(plugin, Value::Null).await.unwrap();
    let disposal = tokio::spawn({
        let fiber = fiber.clone();
        async move { fiber.dispose().await }
    });
    disposing.notified().await;
    disposal.abort();
    release.notify_one();
    disposed.notified().await;
    tokio::task::yield_now().await;

    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Disposed);
}

#[tokio::test]
async fn dropping_update_future_still_applies_the_new_configuration() {
    let root = Context::new();
    let disposing = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let reapplied = Arc::new(tokio::sync::Notify::new());
    let starts = Arc::new(AtomicUsize::new(0));
    let plugin = Plugin::new("slow-update", {
        let disposing = Arc::clone(&disposing);
        let release = Arc::clone(&release);
        let reapplied = Arc::clone(&reapplied);
        let starts = Arc::clone(&starts);
        move |_, config| {
            let disposing = Arc::clone(&disposing);
            let release = Arc::clone(&release);
            let reapplied = Arc::clone(&reapplied);
            let starts = Arc::clone(&starts);
            async move {
                let invocation = starts.fetch_add(1, Ordering::SeqCst);
                if invocation > 0 {
                    assert_eq!(config, json!({"version": 2}));
                    reapplied.notify_one();
                    return Ok(None);
                }
                let disposer: Disposer = Arc::new(move || {
                    let disposing = Arc::clone(&disposing);
                    let release = Arc::clone(&release);
                    Box::pin(async move {
                        disposing.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                });
                Ok(Some(disposer))
            }
        }
    });
    let fiber = root.plugin(plugin, json!({"version": 1})).await.unwrap();
    let update = tokio::spawn({
        let fiber = fiber.clone();
        async move { fiber.update(json!({"version": 2})).await }
    });
    disposing.notified().await;
    update.abort();
    release.notify_one();
    reapplied.notified().await;
    fiber.wait().await.unwrap();

    assert_eq!(fiber.snapshot().unwrap().state, FiberState::Active);
    assert_eq!(fiber.snapshot().unwrap().config, json!({"version": 2}));
}

#[tokio::test]
async fn dropping_effect_future_still_finishes_registration_and_cleanup() {
    let root = Context::new();
    let setup_started = Arc::new(tokio::sync::Notify::new());
    let release_setup = Arc::new(tokio::sync::Notify::new());
    let cleaned = Arc::new(tokio::sync::Notify::new());
    let effect_task = tokio::spawn({
        let root = root.clone();
        let setup_started = Arc::clone(&setup_started);
        let release_setup = Arc::clone(&release_setup);
        let cleaned = Arc::clone(&cleaned);
        async move {
            root.effect("detached", move |scope| async move {
                let disposer: Disposer = Arc::new(move || {
                    let cleaned = Arc::clone(&cleaned);
                    Box::pin(async move {
                        cleaned.notify_one();
                        Ok(())
                    })
                });
                scope.defer(disposer);
                setup_started.notify_one();
                release_setup.notified().await;
                Ok(None)
            })
            .await
        }
    });
    setup_started.notified().await;
    effect_task.abort();
    release_setup.notify_one();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert_eq!(root.fiber().get_effects().len(), 1);

    root.fiber().dispose().await.unwrap();
    cleaned.notified().await;
    assert!(root.fiber().get_effects().is_empty());
}

#[tokio::test]
async fn dropping_manual_effect_disposal_future_does_not_cancel_cleanup() {
    let root = Context::new();
    let disposing = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cleaned = Arc::new(tokio::sync::Notify::new());
    let effect = root
        .effect("manual", {
            let disposing = Arc::clone(&disposing);
            let release = Arc::clone(&release);
            let cleaned = Arc::clone(&cleaned);
            move |scope| async move {
                let disposer: Disposer = Arc::new(move || {
                    let disposing = Arc::clone(&disposing);
                    let release = Arc::clone(&release);
                    let cleaned = Arc::clone(&cleaned);
                    Box::pin(async move {
                        disposing.notify_one();
                        release.notified().await;
                        cleaned.notify_one();
                        Ok(())
                    })
                });
                scope.defer(disposer);
                Ok(None)
            }
        })
        .await
        .unwrap();
    let disposal = tokio::spawn({
        let effect = effect.clone();
        async move { effect.dispose().await }
    });
    disposing.notified().await;
    disposal.abort();
    release.notify_one();
    cleaned.notified().await;

    assert!(effect.is_disposed());
    assert!(root.fiber().get_effects().is_empty());
}

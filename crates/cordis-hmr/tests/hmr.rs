use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use cordis_core::{
    Context, CordisError, Disposer, EventCallback, EventFuture, EventInvocation, EventOptions,
    EventOutcome, Plugin,
};
use cordis_hmr::{ChangeOutcome, Hmr, HmrReload, PluginFactory, WatchTarget};
use cordis_loader::{EntryOptions, Loader, RegistryResolver};
use parking_lot::Mutex;

fn counted_plugin(name: &'static str, starts: Arc<AtomicUsize>, stops: Arc<AtomicUsize>) -> Plugin {
    Plugin::new(name, move |_, _| {
        let starts = Arc::clone(&starts);
        let stops = Arc::clone(&stops);
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
    })
}

#[tokio::test]
async fn dependency_change_reloads_all_plugin_instances() {
    let resolver = Arc::new(RegistryResolver::default());
    let old_starts = Arc::new(AtomicUsize::new(0));
    let old_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("old", Arc::clone(&old_starts), Arc::clone(&old_stops)),
    );
    let root = Context::new();
    let reload_events = Arc::new(Mutex::new(Vec::<Vec<HmrReload>>::new()));
    root.on(
        "hmr/reload",
        {
            let reload_events = Arc::clone(&reload_events);
            Arc::new(move |invocation: EventInvocation| -> EventFuture {
                let reload_events = Arc::clone(&reload_events);
                Box::pin(async move {
                    reload_events
                        .lock()
                        .push(invocation.arg::<Vec<HmrReload>>(0).unwrap().clone());
                    Ok(EventOutcome::Continue)
                })
            }) as EventCallback
        },
        EventOptions::default(),
    )
    .unwrap();
    let loader = Loader::new(root, resolver.clone());
    for id in ["one", "two"] {
        loader
            .create(
                EntryOptions {
                    id: Some(id.into()),
                    name: "foo".into(),
                    ..Default::default()
                },
                None,
                usize::MAX,
            )
            .await
            .unwrap();
    }
    let hmr = Hmr::new(loader, resolver);
    let new_starts = Arc::new(AtomicUsize::new(0));
    let new_stops = Arc::new(AtomicUsize::new(0));
    let factory: PluginFactory = {
        let starts = Arc::clone(&new_starts);
        let stops = Arc::clone(&new_stops);
        Arc::new(move || {
            Ok(counted_plugin(
                "new",
                Arc::clone(&starts),
                Arc::clone(&stops),
            ))
        })
    };
    hmr.register_module(
        "plugin.rs",
        ["dependency.rs"],
        Some("foo".into()),
        Some(factory),
        false,
    );
    hmr.register_module("dependency.rs", Vec::<String>::new(), None, None, false);
    assert_eq!(
        hmr.change("dependency.rs").await.unwrap(),
        ChangeOutcome::Reloaded(vec!["foo".into()])
    );
    assert_eq!(old_starts.load(Ordering::SeqCst), 2);
    assert_eq!(old_stops.load(Ordering::SeqCst), 2);
    assert_eq!(new_starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        &*reload_events.lock(),
        &[vec![HmrReload {
            plugin_name: "foo".into(),
            module_url: "plugin.rs".into(),
        }]]
    );
}

#[tokio::test]
async fn compile_failure_rolls_back_but_apply_failure_remains_recoverable() {
    let resolver = Arc::new(RegistryResolver::default());
    let old_starts = Arc::new(AtomicUsize::new(0));
    let old_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("old", Arc::clone(&old_starts), Arc::clone(&old_stops)),
    );
    let loader = Loader::new(Context::new(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("one".into()),
                name: "foo".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let hmr = Hmr::new(loader.clone(), resolver.clone());
    hmr.register_module(
        "foo.rs",
        Vec::<String>::new(),
        Some("foo".into()),
        Some(Arc::new(|| {
            Err(CordisError::InvalidConfig("compile".into()))
        })),
        false,
    );
    assert!(hmr.change("foo.rs").await.is_err());
    assert_eq!(old_stops.load(Ordering::SeqCst), 0);

    let failing: PluginFactory = Arc::new(|| {
        Ok(Plugin::new("broken", |_, _| async {
            Err(CordisError::InvalidConfig("activate".into()))
        }))
    });
    hmr.register_module(
        "foo.rs",
        Vec::<String>::new(),
        Some("foo".into()),
        Some(failing),
        false,
    );
    assert_eq!(
        hmr.change("foo.rs").await.unwrap(),
        ChangeOutcome::Reloaded(vec!["foo".into()])
    );
    assert_eq!(old_stops.load(Ordering::SeqCst), 1);
    assert_eq!(old_starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        loader.snapshots().await[0].state,
        Some(cordis_core::FiberState::Failed)
    );

    hmr.register_module(
        "foo.rs",
        Vec::<String>::new(),
        Some("foo".into()),
        Some(Arc::new(|| {
            Ok(Plugin::new("recovered", |_, _| async { Ok(None) }))
        })),
        false,
    );
    assert!(matches!(
        hmr.change("foo.rs").await.unwrap(),
        ChangeOutcome::Reloaded(_)
    ));
    assert_eq!(
        loader.snapshots().await[0].state,
        Some(cordis_core::FiberState::Active)
    );
}

#[tokio::test]
async fn external_and_unknown_changes_are_classified() {
    let resolver = Arc::new(RegistryResolver::default());
    let root = Context::new();
    let changed = Arc::new(Mutex::new(Vec::new()));
    root.on(
        "hmr/change",
        {
            let changed = Arc::clone(&changed);
            Arc::new(move |invocation: EventInvocation| -> EventFuture {
                let changed = Arc::clone(&changed);
                Box::pin(async move {
                    changed
                        .lock()
                        .push(invocation.arg::<String>(0).unwrap().clone());
                    Ok(EventOutcome::Continue)
                })
            }) as EventCallback
        },
        EventOptions::default(),
    )
    .unwrap();
    let loader = Loader::new(root, resolver.clone());
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module("runtime.rs", Vec::<String>::new(), None, None, true);
    assert_eq!(
        hmr.change("runtime.rs").await.unwrap(),
        ChangeOutcome::FullReload
    );
    assert_eq!(
        hmr.change("unknown.rs").await.unwrap(),
        ChangeOutcome::Ignored
    );
    assert_eq!(&*changed.lock(), &["unknown.rs"]);
}

#[test]
fn linked_dependencies_are_sorted_and_unknown_modules_are_empty() {
    let resolver = Arc::new(RegistryResolver::default());
    let loader = Loader::new(Context::new(), resolver.clone());
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "plugin.rs",
        ["z.rs", "a.rs"],
        Some("plugin".into()),
        None,
        false,
    );

    assert_eq!(
        hmr.linked_dependencies("plugin.rs"),
        vec!["a.rs".to_owned(), "z.rs".to_owned()]
    );
    assert!(hmr.linked_dependencies("unknown.rs").is_empty());
}

#[tokio::test]
async fn batched_changes_reload_each_affected_plugin_once() {
    let resolver = Arc::new(RegistryResolver::default());
    let old_starts = Arc::new(AtomicUsize::new(0));
    let old_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("old", Arc::clone(&old_starts), Arc::clone(&old_stops)),
    );
    let loader = Loader::new(Context::new(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("foo".into()),
                name: "foo".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let new_starts = Arc::new(AtomicUsize::new(0));
    let new_stops = Arc::new(AtomicUsize::new(0));
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "foo.rs",
        ["a.rs", "b.rs"],
        Some("foo".into()),
        Some({
            let factory_calls = Arc::clone(&factory_calls);
            let starts = Arc::clone(&new_starts);
            let stops = Arc::clone(&new_stops);
            Arc::new(move || {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(counted_plugin(
                    "new",
                    Arc::clone(&starts),
                    Arc::clone(&stops),
                ))
            })
        }),
        false,
    );
    hmr.register_module("a.rs", Vec::<String>::new(), None, None, false);
    hmr.register_module("b.rs", Vec::<String>::new(), None, None, false);

    assert_eq!(
        hmr.change_many(["a.rs", "b.rs", "a.rs"]).await.unwrap(),
        ChangeOutcome::Reloaded(vec!["foo".into()])
    );
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    assert_eq!(old_stops.load(Ordering::SeqCst), 1);
    assert_eq!(new_starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_change_batches_do_not_interleave_registry_mutation() {
    let resolver = Arc::new(RegistryResolver::default());
    let disposal_started = Arc::new(tokio::sync::Notify::new());
    let release_disposal = Arc::new(tokio::sync::Notify::new());
    resolver.register("foo", {
        let disposal_started = Arc::clone(&disposal_started);
        let release_disposal = Arc::clone(&release_disposal);
        Plugin::new("old", move |_, _| {
            let disposal_started = Arc::clone(&disposal_started);
            let release_disposal = Arc::clone(&release_disposal);
            async move {
                let disposer: Disposer = Arc::new(move || {
                    let disposal_started = Arc::clone(&disposal_started);
                    let release_disposal = Arc::clone(&release_disposal);
                    Box::pin(async move {
                        disposal_started.notify_one();
                        release_disposal.notified().await;
                        Ok(())
                    })
                });
                Ok(Some(disposer))
            }
        })
    });
    let loader = Loader::new(Context::new(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("foo".into()),
                name: "foo".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();

    let factory_calls = Arc::new(AtomicUsize::new(0));
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "foo.rs",
        Vec::<String>::new(),
        Some("foo".into()),
        Some({
            let factory_calls = Arc::clone(&factory_calls);
            Arc::new(move || {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Plugin::new("replacement", |_, _| async { Ok(None) }))
            })
        }),
        false,
    );

    let first = tokio::spawn({
        let hmr = hmr.clone();
        async move { hmr.change("foo.rs").await }
    });
    tokio::time::timeout(Duration::from_secs(1), disposal_started.notified())
        .await
        .unwrap();
    let second = tokio::spawn({
        let hmr = hmr.clone();
        async move { hmr.change("foo.rs").await }
    });
    tokio::task::yield_now().await;
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    release_disposal.notify_one();
    assert!(matches!(
        first.await.unwrap().unwrap(),
        ChangeOutcome::Reloaded(_)
    ));
    assert!(matches!(
        second.await.unwrap().unwrap(),
        ChangeOutcome::Reloaded(_)
    ));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dependency_cycles_terminate_and_still_propagate_changes() {
    let resolver = Arc::new(RegistryResolver::default());
    let old_starts = Arc::new(AtomicUsize::new(0));
    let old_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("old", Arc::clone(&old_starts), Arc::clone(&old_stops)),
    );
    let loader = Loader::new(Context::new(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("foo".into()),
                name: "foo".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "foo.rs",
        ["cycle.rs"],
        Some("foo".into()),
        Some(Arc::new(|| {
            Ok(Plugin::new("new", |_, _| async { Ok(None) }))
        })),
        false,
    );
    hmr.register_module("cycle.rs", ["foo.rs"], None, None, false);

    assert_eq!(
        hmr.change("cycle.rs").await.unwrap(),
        ChangeOutcome::Reloaded(vec!["foo".into()])
    );
    assert_eq!(old_stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn multi_plugin_apply_failure_does_not_undo_successful_siblings() {
    let resolver = Arc::new(RegistryResolver::default());
    let a_starts = Arc::new(AtomicUsize::new(0));
    let a_stops = Arc::new(AtomicUsize::new(0));
    let b_starts = Arc::new(AtomicUsize::new(0));
    let b_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "a",
        counted_plugin("old-a", Arc::clone(&a_starts), Arc::clone(&a_stops)),
    );
    resolver.register(
        "b",
        counted_plugin("old-b", Arc::clone(&b_starts), Arc::clone(&b_stops)),
    );
    let loader = Loader::new(Context::new(), resolver.clone());
    for name in ["a", "b"] {
        loader
            .create(
                EntryOptions {
                    id: Some(name.into()),
                    name: name.into(),
                    ..Default::default()
                },
                None,
                usize::MAX,
            )
            .await
            .unwrap();
    }
    let replacement_a_stops = Arc::new(AtomicUsize::new(0));
    let hmr = Hmr::new(loader.clone(), resolver);
    hmr.register_module(
        "a.rs",
        ["shared.rs"],
        Some("a".into()),
        Some({
            let stops = Arc::clone(&replacement_a_stops);
            Arc::new(move || {
                Ok(counted_plugin(
                    "new-a",
                    Arc::new(AtomicUsize::new(0)),
                    Arc::clone(&stops),
                ))
            })
        }),
        false,
    );
    hmr.register_module(
        "b.rs",
        ["shared.rs"],
        Some("b".into()),
        Some(Arc::new(|| {
            Ok(Plugin::new("broken-b", |_, _| async {
                Err(CordisError::InvalidConfig("activation".into()))
            }))
        })),
        false,
    );
    hmr.register_module("shared.rs", Vec::<String>::new(), None, None, false);

    assert_eq!(
        hmr.change("shared.rs").await.unwrap(),
        ChangeOutcome::Reloaded(vec!["a".into(), "b".into()])
    );
    assert_eq!(a_starts.load(Ordering::SeqCst), 1);
    assert_eq!(b_starts.load(Ordering::SeqCst), 1);
    assert_eq!(a_stops.load(Ordering::SeqCst), 1);
    assert_eq!(b_stops.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_a_stops.load(Ordering::SeqCst), 0);
    let states = loader
        .snapshots()
        .await
        .into_iter()
        .map(|snapshot| (snapshot.id, snapshot.state.unwrap()))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(states["a"], cordis_core::FiberState::Active);
    assert_eq!(states["b"], cordis_core::FiberState::Failed);
}

#[tokio::test]
async fn polling_watcher_debounces_rapid_changes_into_one_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foo.rs");
    tokio::fs::write(&path, "version-one").await.unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let old_starts = Arc::new(AtomicUsize::new(0));
    let old_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("old", Arc::clone(&old_starts), Arc::clone(&old_stops)),
    );
    let loader = Loader::new(Context::new(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("foo".into()),
                name: "foo".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "foo.rs",
        Vec::<String>::new(),
        Some("foo".into()),
        Some({
            let factory_calls = Arc::clone(&factory_calls);
            Arc::new(move || {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Plugin::new("new", |_, _| async { Ok(None) }))
            })
        }),
        false,
    );
    let mut watcher = hmr
        .watch(
            [WatchTarget::new(&path, "foo.rs")],
            Duration::from_millis(10),
            Duration::from_millis(50),
        )
        .await
        .unwrap();

    tokio::fs::write(&path, "version-two-longer").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::fs::write(&path, "version-three-even-longer")
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), watcher.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.changed, vec!["foo.rs"]);
    assert_eq!(
        event.outcome.unwrap(),
        ChangeOutcome::Reloaded(vec!["foo".into()])
    );
    assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    assert_eq!(old_stops.load(Ordering::SeqCst), 1);

    tokio::fs::write(&path, "version-four").await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), watcher.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.changed, vec!["foo.rs"]);
    assert!(matches!(
        second.outcome.unwrap(),
        ChangeOutcome::Reloaded(_)
    ));
    assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
    watcher.stop().await;
}

#[tokio::test]
async fn reload_replaces_registered_services() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register(
        "service",
        Plugin::new("service-v1", |context, _| async move {
            context.provide("version", 1_u32).await?;
            Ok(None)
        }),
    );
    let loader = Loader::new(root.clone(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("service".into()),
                name: "service".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    assert_eq!(*root.get::<u32>("version").unwrap(), 1);
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "service.rs",
        Vec::<String>::new(),
        Some("service".into()),
        Some(Arc::new(|| {
            Ok(Plugin::new("service-v2", |context, _| async move {
                context.provide("version", 2_u32).await?;
                Ok(None)
            }))
        })),
        false,
    );

    hmr.change("service.rs").await.unwrap();
    assert_eq!(*root.get::<u32>("version").unwrap(), 2);
}

#[tokio::test]
async fn reload_removes_old_event_handlers_before_installing_new_ones() {
    fn event_plugin(name: &'static str, event: &'static str, calls: Arc<AtomicUsize>) -> Plugin {
        Plugin::new(name, move |context, _| {
            let calls = Arc::clone(&calls);
            async move {
                let callback: EventCallback = Arc::new(move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(EventOutcome::Continue)
                    })
                });
                context.on(event, callback, EventOptions::default())?;
                Ok(None)
            }
        })
    }

    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let old_calls = Arc::new(AtomicUsize::new(0));
    let new_calls = Arc::new(AtomicUsize::new(0));
    let final_calls = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "events",
        event_plugin("old", "old-only", Arc::clone(&old_calls)),
    );
    let loader = Loader::new(root.clone(), resolver.clone());
    loader
        .create(
            EntryOptions {
                id: Some("events".into()),
                name: "events".into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let hmr = Hmr::new(loader, resolver);
    hmr.register_module(
        "events.rs",
        Vec::<String>::new(),
        Some("events".into()),
        Some({
            let factory_calls = Arc::clone(&factory_calls);
            let new_calls = Arc::clone(&new_calls);
            let final_calls = Arc::clone(&final_calls);
            Arc::new(move || {
                if factory_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(event_plugin("new", "new-only", Arc::clone(&new_calls)))
                } else {
                    Ok(event_plugin(
                        "final",
                        "final-only",
                        Arc::clone(&final_calls),
                    ))
                }
            })
        }),
        false,
    );

    root.emit("old-only", vec![]).await.unwrap();
    hmr.change("events.rs").await.unwrap();
    root.emit("old-only", vec![]).await.unwrap();
    root.emit("new-only", vec![]).await.unwrap();
    hmr.change("events.rs").await.unwrap();
    root.emit("new-only", vec![]).await.unwrap();
    root.emit("final-only", vec![]).await.unwrap();

    assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(new_calls.load(Ordering::SeqCst), 1);
    assert_eq!(final_calls.load(Ordering::SeqCst), 1);
}

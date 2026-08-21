use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use cordis_core::{Context, Disposer, FiberState, Plugin};
use cordis_loader::{
    EntryOptions, EntryPosition, EntryUpdate, IsolationRule, Loader, RegistryResolver,
};
use parking_lot::Mutex;
use serde_json::json;

fn entry(id: &str, name: &str) -> EntryOptions {
    EntryOptions {
        id: Some(id.to_owned()),
        name: name.to_owned(),
        ..Default::default()
    }
}

#[tokio::test]
async fn weak_loader_view_does_not_keep_loader_state_alive() {
    let loader = Loader::new(Context::new(), Arc::new(RegistryResolver::default()));
    let weak = loader.downgrade();

    assert_eq!(weak.root_config().await.unwrap(), Some(Vec::new()));
    drop(loader);
    assert_eq!(weak.root_config().await.unwrap(), None);
}

#[tokio::test]
async fn started_creation_exposes_loading_pending_and_failed_fibers() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let release = Arc::new(tokio::sync::Notify::new());
    resolver.register("loading", {
        let release = Arc::clone(&release);
        Plugin::new("loading", move |_, _| {
            let release = Arc::clone(&release);
            async move {
                release.notified().await;
                Ok(None)
            }
        })
    });
    resolver.register(
        "pending",
        Plugin::builder("pending", |_, _| async { Ok(None) })
            .inject("missing")
            .build(),
    );
    resolver.register(
        "failed",
        Plugin::new("failed", |_, _| async {
            Err(cordis_core::CordisError::InvalidConfig(
                "activation failed".into(),
            ))
        }),
    );
    let idle_starts = Arc::new(AtomicUsize::new(0));
    resolver.register("after-idle", {
        let idle_starts = Arc::clone(&idle_starts);
        Plugin::new("after-idle", move |_, _| {
            let idle_starts = Arc::clone(&idle_starts);
            async move {
                idle_starts.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
        })
    });
    let loader = Loader::new(root, resolver);

    let loading = loader
        .create_started(entry("loading", "loading"), None, 0)
        .await
        .unwrap();
    let pending = loader
        .create_started(entry("pending", "pending"), None, 1)
        .await
        .unwrap();
    let failed = loader
        .create_started(entry("failed", "failed"), None, 2)
        .await
        .unwrap();
    let after_idle = loader
        .create_started(
            EntryOptions {
                id: Some("after-idle".into()),
                name: "after-idle".into(),
                inject: vec!["loader".into()],
                intercept: [("loader".into(), json!({"await": true}))].into(),
                ..Default::default()
            },
            None,
            3,
        )
        .await
        .unwrap();

    assert_eq!(
        loader
            .fiber(&loading)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Loading
    );
    assert_eq!(
        loader
            .fiber(&pending)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Pending
    );
    let failed_fiber = loader.fiber(&failed).await.unwrap().unwrap();
    assert!(failed_fiber.wait().await.is_err());
    assert_eq!(failed_fiber.snapshot().unwrap().state, FiberState::Failed);
    assert_eq!(
        loader
            .fiber(&after_idle)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Pending
    );
    assert_eq!(idle_starts.load(Ordering::SeqCst), 0);

    release.notify_one();
    let loading_fiber = loader.fiber(&loading).await.unwrap().unwrap();
    loader.wait().await;
    assert_eq!(loading_fiber.snapshot().unwrap().state, FiberState::Active);
    assert_eq!(
        loader
            .fiber(&pending)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Pending
    );
    assert_eq!(
        loader
            .fiber(&after_idle)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Active
    );
    assert_eq!(idle_starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn loader_idle_barrier_tracks_a_pending_entry_that_starts_loading_later() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    resolver.register("delayed", {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        Plugin::builder("delayed", move |_, _| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            async move {
                started.notify_one();
                release.notified().await;
                Ok(None)
            }
        })
        .inject("trigger")
        .build()
    });
    let waiter_starts = Arc::new(AtomicUsize::new(0));
    resolver.register("waiter", {
        let waiter_starts = Arc::clone(&waiter_starts);
        Plugin::new("waiter", move |_, _| {
            let waiter_starts = Arc::clone(&waiter_starts);
            async move {
                waiter_starts.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
        })
    });
    let loader = Loader::new(root.clone(), resolver);
    let delayed = loader
        .create_started(entry("delayed", "delayed"), None, 0)
        .await
        .unwrap();
    assert_eq!(
        loader
            .fiber(&delayed)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Pending
    );

    let provision = tokio::spawn(async move { root.provide("trigger", ()).await });
    tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    let waiter = loader
        .create_started(
            EntryOptions {
                id: Some("waiter".into()),
                name: "waiter".into(),
                inject: vec!["loader".into()],
                intercept: [("loader".into(), json!({"await": true}))].into(),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        loader
            .fiber(&waiter)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Pending
    );
    assert_eq!(waiter_starts.load(Ordering::SeqCst), 0);

    release.notify_one();
    let _trigger = provision.await.unwrap().unwrap();
    loader.wait().await;
    assert_eq!(
        loader
            .fiber(&waiter)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Active
    );
    assert_eq!(waiter_starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn self_updated_entry_persists_the_validated_configuration() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("foo", Arc::clone(&starts), Arc::clone(&stops)),
    );
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("item", "foo"), None, 0).await.unwrap();

    loader
        .fiber(&id)
        .await
        .unwrap()
        .unwrap()
        .update(json!({"version": 2}))
        .await
        .unwrap();

    let snapshot = loader
        .snapshots()
        .await
        .into_iter()
        .find(|snapshot| snapshot.id == id)
        .unwrap();
    assert_eq!(snapshot.options.config, json!({"version": 2}));
    assert_eq!(snapshot.state, Some(FiberState::Active));
    assert_eq!(
        loader.root_config().await.unwrap()[0].config,
        json!({"version": 2})
    );
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn self_disposed_entry_is_persisted_as_disabled_and_can_be_reenabled() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("foo", Arc::clone(&starts), Arc::clone(&stops)),
    );
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("item", "foo"), None, 0).await.unwrap();

    loader
        .fiber(&id)
        .await
        .unwrap()
        .unwrap()
        .dispose()
        .await
        .unwrap();

    let snapshot = loader
        .snapshots()
        .await
        .into_iter()
        .find(|snapshot| snapshot.id == id)
        .unwrap();
    assert!(snapshot.options.disabled);
    assert_eq!(snapshot.state, None);
    assert!(loader.root_config().await.unwrap()[0].disabled);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    loader.set_disabled(&id, false).await.unwrap();

    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        loader
            .snapshots()
            .await
            .into_iter()
            .find(|snapshot| snapshot.id == id)
            .unwrap()
            .state,
        Some(FiberState::Active)
    );
}

#[tokio::test]
async fn cancelling_self_dispose_wait_still_persists_the_disabled_entry() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let disposing = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    resolver.register("foo", {
        let disposing = Arc::clone(&disposing);
        let release = Arc::clone(&release);
        Plugin::new("foo", move |_, _| {
            let disposing = Arc::clone(&disposing);
            let release = Arc::clone(&release);
            async move {
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
        })
    });
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("item", "foo"), None, 0).await.unwrap();
    let dispose = tokio::spawn({
        let fiber = loader.fiber(&id).await.unwrap().unwrap();
        async move { fiber.dispose().await }
    });

    disposing.notified().await;
    dispose.abort();
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if loader.root_config().await.unwrap()[0].disabled {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(loader.fiber(&id).await.unwrap().is_none());
}

#[tokio::test]
async fn cancelling_self_update_wait_still_persists_the_new_configuration() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let disposing = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let reapplied = Arc::new(tokio::sync::Notify::new());
    resolver.register("foo", {
        let starts = Arc::clone(&starts);
        let disposing = Arc::clone(&disposing);
        let release = Arc::clone(&release);
        let reapplied = Arc::clone(&reapplied);
        Plugin::new("foo", move |_, config| {
            let starts = Arc::clone(&starts);
            let disposing = Arc::clone(&disposing);
            let release = Arc::clone(&release);
            let reapplied = Arc::clone(&reapplied);
            async move {
                if starts.fetch_add(1, Ordering::SeqCst) > 0 {
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
        })
    });
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("item", "foo"), None, 0).await.unwrap();
    let update = tokio::spawn({
        let fiber = loader.fiber(&id).await.unwrap().unwrap();
        async move { fiber.update(json!({"version": 2})).await }
    });

    disposing.notified().await;
    update.abort();
    release.notify_one();
    reapplied.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if loader.root_config().await.unwrap()[0].config == json!({"version": 2}) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        loader
            .fiber(&id)
            .await
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap()
            .state,
        FiberState::Active
    );
}

#[tokio::test]
async fn self_lifecycle_observers_follow_entry_across_id_changes() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    resolver.register("foo", Plugin::new("foo", |_, _| async { Ok(None) }));
    let loader = Loader::new(root, resolver);
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let original = loader.create(entry("item", "foo"), None, 1).await.unwrap();
    let fiber = loader.fiber(&original).await.unwrap().unwrap();

    let moved = loader.move_entry(&original, Some(&group), 0).await.unwrap();
    assert_eq!(moved, "group:item");
    assert_eq!(
        loader.fiber(&moved).await.unwrap().unwrap().identity(),
        fiber.identity()
    );

    fiber.update(json!({"version": 2})).await.unwrap();
    let roots = loader.root_config().await.unwrap();
    let children: Vec<EntryOptions> = serde_json::from_value(roots[0].config.clone()).unwrap();
    assert_eq!(children[0].config, json!({"version": 2}));

    fiber.dispose().await.unwrap();
    let roots = loader.root_config().await.unwrap();
    let children: Vec<EntryOptions> = serde_json::from_value(roots[0].config.clone()).unwrap();
    assert!(children[0].disabled);
    assert!(loader.fiber(&moved).await.unwrap().is_none());
}

#[tokio::test]
async fn locate_follows_descendant_fibers_and_entry_moves() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let child = Arc::new(Mutex::new(None));
    resolver.register("owner", {
        let child = Arc::clone(&child);
        Plugin::new("owner", move |context, _| {
            let child = Arc::clone(&child);
            async move {
                let fiber = context
                    .plugin(Plugin::new("child", |_, _| async { Ok(None) }), json!(null))
                    .await?;
                *child.lock() = Some(fiber);
                Ok(None)
            }
        })
    });
    let loader = Loader::new(root.clone(), resolver);
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let owner = loader
        .create(entry("owner", "owner"), None, 1)
        .await
        .unwrap();
    let owner_fiber = loader.fiber(&owner).await.unwrap().unwrap();
    let child_fiber = child.lock().clone().unwrap();

    assert_eq!(loader.locate(&owner_fiber).await.as_deref(), Some("owner"));
    assert_eq!(loader.locate(&child_fiber).await.as_deref(), Some("owner"));
    let unrelated = root
        .plugin(
            Plugin::new("unrelated", |_, _| async { Ok(None) }),
            json!(null),
        )
        .await
        .unwrap();
    assert_eq!(loader.locate(&unrelated).await, None);

    let moved = loader.move_entry(&owner, Some(&group), 0).await.unwrap();
    assert_eq!(moved, "group:owner");
    assert_eq!(
        loader.locate(&child_fiber).await.as_deref(),
        Some("group:owner")
    );
}

#[tokio::test]
async fn nested_groups_propagate_disable_and_enable() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let plugin_starts = Arc::clone(&starts);
    let plugin_stops = Arc::clone(&stops);
    resolver.register(
        "foo",
        Plugin::new("foo", move |_, _| {
            let starts = Arc::clone(&plugin_starts);
            let stops = Arc::clone(&plugin_stops);
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
        }),
    );
    let loader = Loader::new(root, resolver);
    let outer = loader
        .create(
            EntryOptions {
                id: Some("outer".into()),
                name: "group".into(),
                group: true,
                config: json!([{ "id": "first", "name": "foo" }]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    loader
        .create(
            EntryOptions {
                id: Some("inner".into()),
                name: "group".into(),
                group: true,
                config: json!([{ "id": "second", "name": "foo" }]),
                ..Default::default()
            },
            Some(&outer),
            1,
        )
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    loader.set_disabled(&outer, true).await.unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 2);
    loader.set_disabled(&outer, false).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 4);
    assert_eq!(loader.snapshots().await.len(), 4);
}

#[tokio::test]
async fn moving_entry_across_enabled_boundary_restarts_it() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let starts = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&starts);
    resolver.register(
        "foo",
        Plugin::new("foo", move |_, _| {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
        }),
    );
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("item", "foo"), None, 0).await.unwrap();
    let disabled = loader
        .create(
            EntryOptions {
                id: Some("disabled".into()),
                name: "group".into(),
                group: true,
                disabled: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    let nested = loader.move_entry(&id, Some(&disabled), 0).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    let root_id = loader.move_entry(&nested, None, 0).await.unwrap();
    assert_eq!(root_id, "item");
    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn intercept_and_global_isolation_are_inherited() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&contexts);
    resolver.register(
        "capture",
        Plugin::new("capture", move |context, _| {
            let captured = Arc::clone(&captured);
            async move {
                captured.lock().push(context);
                Ok(None)
            }
        }),
    );
    let loader = Loader::new(root, resolver);
    let parent = loader
        .create(
            EntryOptions {
                id: Some("parent".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                intercept: [("foo".into(), json!({"a": 1}))].into(),
                isolate: [("db".into(), IsolationRule::Global("shared".into()))].into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    loader
        .create(
            EntryOptions {
                id: Some("one".into()),
                name: "capture".into(),
                ..Default::default()
            },
            Some(&parent),
            0,
        )
        .await
        .unwrap();
    loader
        .create(
            EntryOptions {
                id: Some("two".into()),
                name: "capture".into(),
                ..Default::default()
            },
            Some(&parent),
            1,
        )
        .await
        .unwrap();
    let contexts = contexts.lock();
    assert_eq!(contexts[0].intercept_value("foo"), Some(&json!({"a": 1})));
    assert!(contexts[0].same_isolation(&contexts[1], "db"));
}

#[tokio::test]
async fn nested_intercepts_preserve_every_override_layer() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let captured = Arc::new(Mutex::new(None));
    resolver.register("capture", {
        let captured = Arc::clone(&captured);
        Plugin::new("capture", move |context, _| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock() = Some(context);
                Ok(None)
            }
        })
    });
    let loader = Loader::new(root, resolver);
    let outer = loader
        .create(
            EntryOptions {
                id: Some("outer".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                intercept: [("foo".into(), json!({"a": 1}))].into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let inner = loader
        .create(
            EntryOptions {
                id: Some("inner".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                intercept: [("foo".into(), json!({"b": 2}))].into(),
                ..Default::default()
            },
            Some(&outer),
            0,
        )
        .await
        .unwrap();
    let mut leaf = entry("leaf", "capture");
    leaf.intercept.insert("foo".into(), json!({"c": 3}));
    loader.create(leaf, Some(&inner), 0).await.unwrap();

    let context = captured.lock().take().unwrap();
    assert_eq!(context.intercept_value("foo"), Some(&json!({"c": 3})));
    assert_eq!(
        context.intercept_chain("foo"),
        vec![json!({"a": 1}), json!({"b": 2}), json!({"c": 3})]
    );
}

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
async fn replace_reconciles_stable_ids_without_restarting_unchanged_entries() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let foo_starts = Arc::new(AtomicUsize::new(0));
    let foo_stops = Arc::new(AtomicUsize::new(0));
    let bar_starts = Arc::new(AtomicUsize::new(0));
    let bar_stops = Arc::new(AtomicUsize::new(0));
    let qux_starts = Arc::new(AtomicUsize::new(0));
    let qux_stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("foo", Arc::clone(&foo_starts), Arc::clone(&foo_stops)),
    );
    resolver.register(
        "bar",
        counted_plugin("bar", Arc::clone(&bar_starts), Arc::clone(&bar_stops)),
    );
    resolver.register(
        "qux",
        counted_plugin("qux", Arc::clone(&qux_starts), Arc::clone(&qux_stops)),
    );
    let loader = Loader::new(root, resolver);

    loader
        .replace(vec![entry("1", "foo"), entry("2", "bar")])
        .await
        .unwrap();
    loader
        .replace(vec![entry("1", "foo"), entry("3", "qux")])
        .await
        .unwrap();

    assert_eq!(foo_starts.load(Ordering::SeqCst), 1);
    assert_eq!(foo_stops.load(Ordering::SeqCst), 0);
    assert_eq!(bar_starts.load(Ordering::SeqCst), 1);
    assert_eq!(bar_stops.load(Ordering::SeqCst), 1);
    assert_eq!(qux_starts.load(Ordering::SeqCst), 1);
    assert_eq!(qux_stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn group_config_update_reconciles_children() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let configs = Arc::new(Mutex::new(Vec::new()));
    let stops = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&configs);
    let captured_stops = Arc::clone(&stops);
    resolver.register(
        "capture",
        Plugin::new("capture", move |_, config| {
            let captured = Arc::clone(&captured);
            let stops = Arc::clone(&captured_stops);
            async move {
                captured.lock().push(config);
                let disposer: Disposer = Arc::new(move || {
                    let stops = Arc::clone(&stops);
                    Box::pin(async move {
                        stops.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                });
                Ok(Some(disposer))
            }
        }),
    );
    let loader = Loader::new(root, resolver);
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([{ "id": "one", "name": "capture", "config": { "v": 1 } }]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();

    loader
        .update_config(
            &group,
            json!([
                { "id": "one", "name": "capture", "config": { "v": 2 } },
                { "id": "two", "name": "capture", "config": { "v": 3 } }
            ]),
        )
        .await
        .unwrap();
    assert_eq!(
        &*configs.lock(),
        &[json!({"v": 1}), json!({"v": 2}), json!({"v": 3})]
    );
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    loader
        .update_config(
            &group,
            json!([{ "id": "two", "name": "capture", "config": { "v": 3 } }]),
        )
        .await
        .unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 2);
    assert_eq!(loader.snapshots().await.len(), 2);
}

#[tokio::test]
async fn enabled_to_enabled_move_preserves_fiber_and_recursive_ids_remain_consistent() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("foo", Arc::clone(&starts), Arc::clone(&stops)),
    );
    let loader = Loader::new(root, resolver);
    let leaf = loader.create(entry("leaf", "foo"), None, 0).await.unwrap();
    let host = loader
        .create(
            EntryOptions {
                id: Some("host".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    let nested_leaf = loader.move_entry(&leaf, Some(&host), 0).await.unwrap();
    assert_eq!(nested_leaf, "host:leaf");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 0);

    let outer = loader
        .create(
            EntryOptions {
                id: Some("outer".into()),
                name: "group".into(),
                group: true,
                config: json!([{ "id": "inner", "name": "group", "group": true, "config": [
                    { "id": "deep", "name": "foo" }
                ] }]),
                ..Default::default()
            },
            None,
            2,
        )
        .await
        .unwrap();
    let moved = loader.move_entry(&outer, Some(&host), 1).await.unwrap();
    assert_eq!(moved, "host:outer");
    let ids = loader
        .snapshots()
        .await
        .into_iter()
        .map(|snapshot| snapshot.id)
        .collect::<Vec<_>>();
    assert!(ids.contains(&"host:outer:inner:deep".to_owned()));
    assert!(!ids.iter().any(|id| id.starts_with("outer:")));

    loader.remove(&moved).await.unwrap();
    assert!(
        !loader
            .snapshots()
            .await
            .iter()
            .any(|snapshot| snapshot.id.contains("outer"))
    );
}

#[tokio::test]
async fn invalid_group_configuration_fails_without_leaving_a_live_entry() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "group",
        counted_plugin("group", Arc::clone(&starts), Arc::clone(&stops)),
    );
    let loader = Loader::new(root, resolver);

    let result = loader
        .create(
            EntryOptions {
                id: Some("broken".into()),
                name: "group".into(),
                group: true,
                config: json!({"not": "an array"}),
                ..Default::default()
            },
            None,
            0,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert_eq!(stops.load(Ordering::SeqCst), 0);
    assert!(loader.snapshots().await.is_empty());
}

#[tokio::test]
async fn replacing_local_isolation_reconciles_only_relevant_service_realms() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", 42_u32).await?;
            Ok(None)
        }),
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let consumer_starts = Arc::clone(&starts);
    let consumer_stops = Arc::clone(&stops);
    resolver.register(
        "consumer",
        Plugin::builder("consumer", move |context, _| {
            let starts = Arc::clone(&consumer_starts);
            let stops = Arc::clone(&consumer_stops);
            async move {
                assert_eq!(*context.get::<u32>("bar").unwrap(), 42);
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
        .inject("bar")
        .build(),
    );
    let loader = Loader::new(root, resolver);
    let provider = entry("provider", "provider");
    let consumer = entry("consumer", "consumer");
    loader
        .replace(vec![provider.clone(), consumer.clone()])
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    let mut isolated = consumer.clone();
    isolated
        .isolate
        .insert("bar".into(), IsolationRule::Local(true));
    loader
        .replace(vec![provider.clone(), isolated.clone()])
        .await
        .unwrap();
    let snapshot = loader
        .snapshots()
        .await
        .into_iter()
        .find(|snapshot| snapshot.id == "consumer")
        .unwrap();
    assert_eq!(snapshot.state, Some(FiberState::Pending));
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    isolated
        .isolate
        .insert("irrelevant".into(), IsolationRule::Local(true));
    loader
        .replace(vec![provider.clone(), isolated.clone()])
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    isolated.isolate.remove("bar");
    loader.replace(vec![provider, isolated]).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn matching_global_realm_connects_provider_and_consumer_across_entries() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", String::from("shared")).await?;
            Ok(None)
        }),
    );
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&observed);
    resolver.register(
        "consumer",
        Plugin::builder("consumer", move |context, _| {
            let capture = Arc::clone(&capture);
            async move {
                capture
                    .lock()
                    .push(context.get::<String>("bar").unwrap().to_string());
                Ok(None)
            }
        })
        .inject("bar")
        .build(),
    );
    let loader = Loader::new(root, resolver);
    let mut provider = entry("provider", "provider");
    provider
        .isolate
        .insert("bar".into(), IsolationRule::Global("shared".into()));
    let mut consumer = entry("consumer", "consumer");
    consumer
        .isolate
        .insert("bar".into(), IsolationRule::Global("shared".into()));
    loader.replace(vec![provider, consumer]).await.unwrap();

    assert_eq!(&*observed.lock(), &["shared"]);
}

#[tokio::test]
async fn global_realms_are_reused_while_referenced_and_collected_after_the_last_entry() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let contexts = Arc::new(Mutex::new(Vec::<Context>::new()));
    resolver.register("capture", {
        let contexts = Arc::clone(&contexts);
        Plugin::new("capture", move |context, _| {
            let contexts = Arc::clone(&contexts);
            async move {
                contexts.lock().push(context);
                Ok(None)
            }
        })
    });
    let loader = Loader::new(root, resolver);
    let isolated = |id: &str| EntryOptions {
        id: Some(id.into()),
        name: "capture".into(),
        isolate: [("service".into(), IsolationRule::Global("shared".into()))].into(),
        ..Default::default()
    };

    loader.create(isolated("a"), None, 0).await.unwrap();
    loader.create(isolated("b"), None, 1).await.unwrap();
    let first = contexts.lock()[0].clone();
    let second = contexts.lock()[1].clone();
    assert!(first.same_isolation(&second, "service"));

    loader.remove("a").await.unwrap();
    loader.create(isolated("c"), None, 1).await.unwrap();
    let third = contexts.lock()[2].clone();
    assert!(second.same_isolation(&third, "service"));

    loader.remove("b").await.unwrap();
    loader.remove("c").await.unwrap();
    loader.create(isolated("d"), None, 0).await.unwrap();
    let fourth = contexts.lock()[3].clone();
    assert!(!third.same_isolation(&fourth, "service"));
}

#[tokio::test]
async fn typed_partial_update_covers_context_lifecycle_and_move_fields() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&contexts);
    resolver.register(
        "capture",
        Plugin::new("capture", move |context, _| {
            let captured = Arc::clone(&captured);
            async move {
                captured.lock().push(context);
                Ok(None)
            }
        }),
    );
    let loader = Loader::new(root, resolver);
    let item = loader
        .create(entry("item", "capture"), None, 0)
        .await
        .unwrap();
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();

    let moved = loader
        .update(
            &item,
            EntryUpdate {
                intercept: Some([("request".into(), json!({"v": 1}))].into()),
                isolate: Some([("db".into(), IsolationRule::Local(true))].into()),
                ..Default::default()
            },
            Some(EntryPosition {
                parent: Some(group),
                position: 0,
            }),
        )
        .await
        .unwrap();
    assert_eq!(moved, "group:item");
    assert_eq!(
        contexts.lock().last().unwrap().intercept_value("request"),
        Some(&json!({"v": 1}))
    );

    loader
        .update(
            &moved,
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let snapshot = loader
        .snapshots()
        .await
        .into_iter()
        .find(|snapshot| snapshot.id == moved)
        .unwrap();
    assert_eq!(snapshot.state, None);
}

#[tokio::test]
async fn invalid_group_partial_update_preserves_the_active_entry() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "foo",
        counted_plugin("foo", Arc::clone(&starts), Arc::clone(&stops)),
    );
    let loader = Loader::new(root, resolver);
    let id = loader.create(entry("foo", "foo"), None, 0).await.unwrap();
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();

    let result = loader
        .update(
            &id,
            EntryUpdate {
                group: Some(true),
                config: Some(json!({"not": "children"})),
                ..Default::default()
            },
            Some(EntryPosition {
                parent: Some(group),
                position: 0,
            }),
        )
        .await;
    assert!(result.is_err());
    let snapshots = loader.snapshots().await;
    assert!(snapshots.iter().all(|snapshot| snapshot.id != "group:foo"));
    let snapshot = snapshots
        .into_iter()
        .find(|snapshot| snapshot.id == id)
        .unwrap();
    assert!(!snapshot.options.group);
    assert_eq!(snapshot.state, Some(FiberState::Active));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_realm_identity_survives_moves_and_context_rebuilds() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("capture", {
        let contexts = Arc::clone(&contexts);
        let stops = Arc::clone(&stops);
        Plugin::new("capture", move |context, _| {
            let contexts = Arc::clone(&contexts);
            let stops = Arc::clone(&stops);
            async move {
                contexts.lock().push(context);
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
    });
    let loader = Loader::new(root, resolver);
    let first_group = loader
        .create(
            EntryOptions {
                id: Some("first".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let second_group = loader
        .create(
            EntryOptions {
                id: Some("second".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            1,
        )
        .await
        .unwrap();
    let mut options = entry("item", "capture");
    options
        .isolate
        .insert("db".into(), IsolationRule::Local(true));
    let item = loader.create(options, Some(&first_group), 0).await.unwrap();
    let moved = loader
        .move_entry(&item, Some(&second_group), 0)
        .await
        .unwrap();

    assert_eq!(contexts.lock().len(), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 0);

    loader
        .update(
            &moved,
            EntryUpdate {
                intercept: Some([("feature".into(), json!(true))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let contexts = contexts.lock();
    assert_eq!(contexts.len(), 2);
    assert!(contexts[0].same_isolation(&contexts[1], "db"));
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn irrelevant_isolation_changes_do_not_restart_active_plugins() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", 42_u32).await?;
            Ok(None)
        }),
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "consumer",
        counted_plugin("consumer", Arc::clone(&starts), Arc::clone(&stops)).with_injects(["bar"]),
    );
    let loader = Loader::new(root, resolver);
    loader
        .replace(vec![
            entry("provider", "provider"),
            entry("consumer", "consumer"),
        ])
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    loader
        .update(
            "consumer",
            EntryUpdate {
                isolate: Some([("qux".into(), IsolationRule::Local(true))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    loader
        .update(
            "provider",
            EntryUpdate {
                isolate: Some([("qux".into(), IsolationRule::Local(true))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 0);

    loader
        .update(
            "consumer",
            EntryUpdate {
                isolate: Some(
                    [
                        ("bar".into(), IsolationRule::Local(true)),
                        ("qux".into(), IsolationRule::Local(true)),
                    ]
                    .into(),
                ),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    loader
        .update(
            "consumer",
            EntryUpdate {
                isolate: Some([("qux".into(), IsolationRule::Local(true))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_inner_explicit_realm_shields_plugins_from_outer_realm_changes() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", 42_u32).await?;
            Ok(None)
        }),
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "consumer",
        counted_plugin("consumer", Arc::clone(&starts), Arc::clone(&stops)).with_injects(["bar"]),
    );
    let loader = Loader::new(root, resolver);
    let outer = loader
        .create(
            EntryOptions {
                id: Some("outer".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let inner = loader
        .create(
            EntryOptions {
                id: Some("inner".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                isolate: [("bar".into(), IsolationRule::Global("custom".into()))].into(),
                ..Default::default()
            },
            Some(&outer),
            0,
        )
        .await
        .unwrap();
    loader
        .create(entry("provider", "provider"), Some(&inner), 0)
        .await
        .unwrap();
    loader
        .create(entry("nested", "consumer"), Some(&inner), 1)
        .await
        .unwrap();
    let mut root_consumer = entry("root-consumer", "consumer");
    root_consumer
        .isolate
        .insert("bar".into(), IsolationRule::Global("custom".into()));
    loader.create(root_consumer, None, 1).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    for isolate in [
        [("bar".into(), IsolationRule::Global("custom".into()))].into(),
        Default::default(),
    ] {
        loader
            .update(
                &outer,
                EntryUpdate {
                    isolate: Some(isolate),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
    }
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn provider_and_injector_transfers_reconcile_their_effective_realm() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", 42_u32).await?;
            Ok(None)
        }),
    );
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register(
        "consumer",
        counted_plugin("consumer", Arc::clone(&starts), Arc::clone(&stops)).with_injects(["bar"]),
    );
    let loader = Loader::new(root, resolver);
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                isolate: [("bar".into(), IsolationRule::Local(true))].into(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();
    let provider = loader
        .create(entry("provider", "provider"), None, 1)
        .await
        .unwrap();
    let consumer = loader
        .create(entry("consumer", "consumer"), None, 2)
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    let consumer = loader.move_entry(&consumer, Some(&group), 0).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    let provider = loader.move_entry(&provider, Some(&group), 1).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);

    let consumer = loader.move_entry(&consumer, None, 0).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 2);

    loader.move_entry(&provider, None, 1).await.unwrap();
    assert_eq!(consumer, "consumer");
    assert_eq!(starts.load(Ordering::SeqCst), 3);
    assert_eq!(stops.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn changing_group_realm_switches_the_consumer_to_an_existing_provider() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    resolver.register(
        "provider",
        Plugin::new("provider", |context, config| async move {
            context
                .provide("bar", config["value"].as_str().unwrap().to_owned())
                .await?;
            Ok(None)
        }),
    );
    let observed = Arc::new(Mutex::new(Vec::new()));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("consumer", {
        let observed = Arc::clone(&observed);
        let stops = Arc::clone(&stops);
        Plugin::builder("consumer", move |context, _| {
            let observed = Arc::clone(&observed);
            let stops = Arc::clone(&stops);
            async move {
                observed
                    .lock()
                    .push(context.get::<String>("bar").unwrap().to_string());
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
        .inject("bar")
        .build()
    });
    let loader = Loader::new(root, resolver);
    for (position, label) in ["alpha", "beta"].into_iter().enumerate() {
        let mut provider = entry(label, "provider");
        provider.config = json!({"value": label});
        provider
            .isolate
            .insert("bar".into(), IsolationRule::Global(label.into()));
        loader.create(provider, None, position).await.unwrap();
    }
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                isolate: [("bar".into(), IsolationRule::Global("alpha".into()))].into(),
                ..Default::default()
            },
            None,
            2,
        )
        .await
        .unwrap();
    loader
        .create(entry("consumer", "consumer"), Some(&group), 0)
        .await
        .unwrap();

    loader
        .update(
            &group,
            EntryUpdate {
                isolate: Some([("bar".into(), IsolationRule::Global("beta".into()))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(&*observed.lock(), &["alpha", "beta"]);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn changing_provider_realm_hands_service_to_the_matching_injector() {
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("group", Plugin::new("group", |_, _| async { Ok(None) }));
    resolver.register(
        "provider",
        Plugin::new("provider", |context, _| async move {
            context.provide("bar", 42_u32).await?;
            Ok(None)
        }),
    );
    let activated = Arc::new(Mutex::new(Vec::new()));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("consumer", {
        let activated = Arc::clone(&activated);
        let stops = Arc::clone(&stops);
        Plugin::builder("consumer", move |_, config| {
            let activated = Arc::clone(&activated);
            let stops = Arc::clone(&stops);
            async move {
                activated
                    .lock()
                    .push(config["label"].as_str().unwrap().to_owned());
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
        .inject("bar")
        .build()
    });
    let loader = Loader::new(root, resolver);
    for (position, label) in ["alpha", "beta"].into_iter().enumerate() {
        let mut consumer = entry(label, "consumer");
        consumer.config = json!({"label": label});
        consumer
            .isolate
            .insert("bar".into(), IsolationRule::Global(label.into()));
        loader.create(consumer, None, position).await.unwrap();
    }
    let group = loader
        .create(
            EntryOptions {
                id: Some("group".into()),
                name: "group".into(),
                group: true,
                config: json!([]),
                isolate: [("bar".into(), IsolationRule::Global("alpha".into()))].into(),
                ..Default::default()
            },
            None,
            2,
        )
        .await
        .unwrap();
    loader
        .create(entry("provider", "provider"), Some(&group), 0)
        .await
        .unwrap();
    assert_eq!(&*activated.lock(), &["alpha"]);

    loader
        .update(
            &group,
            EntryUpdate {
                isolate: Some([("bar".into(), IsolationRule::Global("beta".into()))].into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(&*activated.lock(), &["alpha", "beta"]);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}

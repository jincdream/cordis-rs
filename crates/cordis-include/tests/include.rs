use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use cordis_core::{Context, CordisError, Disposer, Plugin, Result};
use cordis_include::{Include, IncludeConfig, PatchOptions, PatchWarning, apply_patches};
use cordis_loader::{EXPRESSION_KEY, EntryOptions, RegistryResolver};
use serde_json::{Value, json};

fn entry(id: &str, name: &str) -> EntryOptions {
    EntryOptions {
        id: Some(id.to_owned()),
        name: name.to_owned(),
        ..Default::default()
    }
}

#[test]
fn patches_cover_nested_override_insert_and_warnings() {
    let data = vec![EntryOptions {
        id: Some("group".into()),
        name: "group".into(),
        group: true,
        config: json!([{ "id": "inner", "name": "foo" }]),
        ..Default::default()
    }];
    let patches = vec![
        PatchOptions {
            id: Some("inner".into()),
            name: Some("foo".into()),
            disabled: Some(true),
            ..Default::default()
        },
        PatchOptions {
            id: Some("group".into()),
            insert: Some(vec![entry("extra", "bar")]),
            ..Default::default()
        },
        PatchOptions {
            id: Some("missing".into()),
            disabled: Some(true),
            ..Default::default()
        },
    ];
    let (patched, warnings) = apply_patches(data, &patches);
    let children: Vec<EntryOptions> = serde_json::from_value(patched[0].config.clone()).unwrap();
    assert!(children[0].disabled);
    assert_eq!(children[1].id.as_deref(), Some("extra"));
    assert_eq!(
        warnings,
        vec![PatchWarning::EntryNotFound("missing".into())]
    );
}

#[test]
fn patches_report_all_structural_warnings_and_skip_name_mismatches() {
    let data = vec![entry("plain", "foo")];
    let patches = vec![
        PatchOptions {
            disabled: Some(true),
            ..Default::default()
        },
        PatchOptions {
            id: Some("plain".into()),
            insert: Some(vec![entry("nested", "bar")]),
            ..Default::default()
        },
        PatchOptions {
            id: Some("plain".into()),
            name: Some("wrong".into()),
            disabled: Some(true),
            ..Default::default()
        },
    ];

    let (patched, warnings) = apply_patches(data, &patches);
    assert!(!patched[0].disabled);
    assert_eq!(
        warnings,
        vec![
            PatchWarning::MissingId,
            PatchWarning::NotAGroup("plain".into()),
            PatchWarning::NameMismatch {
                id: "plain".into(),
                expected: "foo".into(),
                actual: "wrong".into(),
            },
        ]
    );
}

#[tokio::test]
async fn initial_file_refresh_failure_and_atomic_write_preserve_state() {
    let directory = tempfile::tempdir().unwrap();
    let resolver = Arc::new(RegistryResolver::default());
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
    let mut config = IncludeConfig::new("config.json");
    config.initial = Some(vec![entry("one", "foo")]);
    let mut include = Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    tokio::fs::write(include.filename(), "not json")
        .await
        .unwrap();
    assert!(include.refresh().await.is_err());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 0);

    tokio::fs::write(
        include.filename(),
        r#"[{"id":"one","name":"foo","config":{"v":2}}]"#,
    )
    .await
    .unwrap();
    assert!(include.refresh().await.unwrap());
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    include
        .loader()
        .update_config("one", json!({"v": 3}))
        .await
        .unwrap();
    include.write().await.unwrap();
    let saved: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(include.filename()).await.unwrap())
            .unwrap();
    assert_eq!(saved[0].config, json!({"v": 3}));
    assert!(!include.filename().with_extension("json.tmp").exists());
}

#[tokio::test]
async fn yaml_and_root_insert_are_supported() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yml");
    tokio::fs::write(&path, "- id: one\n  name: foo\n")
        .await
        .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    for name in ["foo", "bar"] {
        let starts = Arc::clone(&starts);
        resolver.register(
            name,
            Plugin::new(name, move |_, _| {
                let starts = Arc::clone(&starts);
                async move {
                    starts.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                }
            }),
        );
    }
    let mut config = IncludeConfig::new("config.yml");
    config.patches.push(PatchOptions {
        insert: Some(vec![entry("two", "bar")]),
        ..Default::default()
    });
    let include = Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(include.data().len(), 2);
}

#[tokio::test]
async fn unchanged_refresh_is_a_noop_and_stop_disposes_the_tree() {
    let directory = tempfile::tempdir().unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("foo", {
        let starts = Arc::clone(&starts);
        let stops = Arc::clone(&stops);
        Plugin::new("foo", move |_, _| {
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
    });
    let mut config = IncludeConfig::new("config.json");
    config.initial = Some(vec![entry("one", "foo")]);
    let mut include = Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();

    assert!(!include.refresh().await.unwrap());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    include.stop().await.unwrap();
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert!(include.loader().snapshots().await.is_empty());
}

#[tokio::test]
async fn dropping_include_unregisters_persistence_and_disposes_the_tree() {
    let directory = tempfile::tempdir().unwrap();
    let root = Context::new();
    let resolver = Arc::new(RegistryResolver::default());
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("foo", {
        let stops = Arc::clone(&stops);
        Plugin::new("foo", move |_, _| {
            let stops = Arc::clone(&stops);
            async move {
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
    let mut config = IncludeConfig::new("config.json");
    config.initial = Some(vec![entry("one", "foo")]);
    let include = Include::open(root.clone(), resolver, config, directory.path())
        .await
        .unwrap();

    drop(include);
    tokio::time::timeout(Duration::from_secs(1), async {
        while stops.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping Include should dispose its loader tree");
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    root.emit("loader/config-update", Vec::new()).await.unwrap();
}

#[tokio::test]
async fn activation_failure_rolls_back_then_a_fixed_file_can_refresh() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    tokio::fs::write(&path, r#"[{"id":"one","name":"foo","config":{"v":1}}]"#)
        .await
        .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let configs = Arc::new(Mutex::new(Vec::new()));
    resolver.register("foo", {
        let configs = Arc::clone(&configs);
        Plugin::new("foo", move |_, config| {
            let configs = Arc::clone(&configs);
            async move {
                configs.lock().unwrap().push(config);
                Ok(None)
            }
        })
    });
    resolver.register(
        "broken",
        Plugin::new("broken", |_, _| async {
            Err(cordis_core::CordisError::InvalidConfig("broken".into()))
        }),
    );
    let config = IncludeConfig::new("config.json");
    let mut include = Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();

    tokio::fs::write(
        &path,
        r#"[
            {"id":"one","name":"foo","config":{"v":2}},
            {"id":"two","name":"broken"}
        ]"#,
    )
    .await
    .unwrap();
    assert!(include.refresh().await.is_err());
    assert_eq!(include.data()[0].config, json!({"v": 1}));
    assert_eq!(
        &*configs.lock().unwrap(),
        &[json!({"v": 1}), json!({"v": 2}), json!({"v": 1})]
    );
    let snapshots = include.loader().snapshots().await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].id, "one");
    assert_eq!(snapshots[0].state, Some(cordis_core::FiberState::Active));

    tokio::fs::write(&path, r#"[{"id":"one","name":"foo","config":{"v":3}}]"#)
        .await
        .unwrap();
    assert!(include.refresh().await.unwrap());
    assert_eq!(include.data()[0].config, json!({"v": 3}));
}

#[tokio::test]
async fn unsupported_extensions_and_missing_files_fail_without_side_effects() {
    let directory = tempfile::tempdir().unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let unsupported = Include::open(
        Context::new(),
        resolver.clone(),
        IncludeConfig::new("config.toml"),
        directory.path(),
    )
    .await;
    assert!(unsupported.is_err());
    assert!(!directory.path().join("config.toml").exists());

    let missing = Include::open(
        Context::new(),
        resolver,
        IncludeConfig::new("missing.yml"),
        directory.path(),
    )
    .await;
    assert!(missing.is_err());
    assert!(!directory.path().join("missing.yml").exists());
}

#[tokio::test]
async fn include_passes_dynamic_configuration_through_its_host_evaluator() {
    let directory = tempfile::tempdir().unwrap();
    let source = json!([{
        "id": "dynamic",
        "name": "capture",
        "config": { "value": { (EXPRESSION_KEY): "answer" } }
    }]);
    tokio::fs::write(
        directory.path().join("config.json"),
        serde_json::to_vec_pretty(&source).unwrap(),
    )
    .await
    .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let observed = Arc::new(Mutex::new(Vec::new()));
    resolver.register("capture", {
        let observed = Arc::clone(&observed);
        Plugin::new("capture", move |_, config| {
            let observed = Arc::clone(&observed);
            async move {
                observed.lock().unwrap().push(config);
                Ok(None)
            }
        })
    });
    let mut config = IncludeConfig::new("config.json");
    config.evaluator = Some(Arc::new(|_: &Context, expression: &str| -> Result<Value> {
        match expression {
            "answer" => Ok(json!(42)),
            other => Err(CordisError::InvalidConfig(other.to_owned())),
        }
    }));

    let include = Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();
    assert_eq!(&*observed.lock().unwrap(), &[json!({"value": 42})]);
    assert_eq!(
        include.data()[0].config,
        json!({"value": { (EXPRESSION_KEY): "answer" }})
    );
}

#[tokio::test]
async fn yaml_js_tags_round_trip_through_the_host_evaluator() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yml");
    tokio::fs::write(
        &path,
        "- id: dynamic\n  name: capture\n  config:\n    value: !!js answer\n",
    )
    .await
    .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let observed = Arc::new(Mutex::new(Vec::new()));
    resolver.register("capture", {
        let observed = Arc::clone(&observed);
        Plugin::new("capture", move |_, config| {
            let observed = Arc::clone(&observed);
            async move {
                observed.lock().unwrap().push(config);
                Ok(None)
            }
        })
    });
    let mut config = IncludeConfig::new("config.yml");
    config.evaluator = Some(Arc::new(|_: &Context, expression: &str| -> Result<Value> {
        assert_eq!(expression, "answer");
        Ok(json!(42))
    }));
    let mut include = Include::open(
        Context::new(),
        resolver.clone(),
        config.clone(),
        directory.path(),
    )
    .await
    .unwrap();
    assert_eq!(&*observed.lock().unwrap(), &[json!({"value": 42})]);

    include.write().await.unwrap();
    let saved = tokio::fs::read_to_string(&path).await.unwrap();
    assert!(!saved.contains(EXPRESSION_KEY));
    assert!(saved.contains("!!js"));

    Include::open(Context::new(), resolver, config, directory.path())
        .await
        .unwrap();
    assert_eq!(observed.lock().unwrap().last(), Some(&json!({"value": 42})));
}

#[tokio::test]
async fn yaml_anchors_aliases_and_duplicate_key_rejection_are_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.yml");
    tokio::fs::write(
        &path,
        "- id: one\n  name: capture\n  config: &shared\n    value: 1\n- id: two\n  name: capture\n  config: *shared\n",
    )
    .await
    .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let observed = Arc::new(Mutex::new(Vec::new()));
    resolver.register("capture", {
        let observed = Arc::clone(&observed);
        Plugin::new("capture", move |_, config| {
            let observed = Arc::clone(&observed);
            async move {
                observed.lock().unwrap().push(config);
                Ok(None)
            }
        })
    });
    Include::open(
        Context::new(),
        resolver.clone(),
        IncludeConfig::new("config.yml"),
        directory.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        &*observed.lock().unwrap(),
        &[json!({"value": 1}), json!({"value": 1})]
    );

    tokio::fs::write(&path, "- id: one\n  id: duplicate\n  name: capture\n")
        .await
        .unwrap();
    let error = Include::open(
        Context::new(),
        resolver,
        IncludeConfig::new("config.yml"),
        directory.path(),
    )
    .await
    .err()
    .expect("duplicate keys must be rejected");
    assert!(error.to_string().contains("duplicated YAML mapping key"));
    assert_eq!(observed.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn self_lifecycle_updates_persist_without_starting_a_file_watcher() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    tokio::fs::write(
        &path,
        serde_json::to_vec_pretty(&[EntryOptions {
            id: Some("item".into()),
            name: "capture".into(),
            config: json!({"version": 1}),
            ..Default::default()
        }])
        .unwrap(),
    )
    .await
    .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("capture", Plugin::new("capture", |_, _| async { Ok(None) }));
    let mut include = Include::open(
        Context::new(),
        resolver,
        IncludeConfig::new("config.json"),
        directory.path(),
    )
    .await
    .unwrap();
    let fiber = include.loader().fiber("item").await.unwrap().unwrap();

    fiber.update(json!({"version": 2})).await.unwrap();
    let updated: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert_eq!(updated[0].config, json!({"version": 2}));

    fiber.dispose().await.unwrap();
    let disposed: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert!(disposed[0].disabled);

    include.set_disabled("item", false).await.unwrap();
    include
        .update_config("item", json!({"version": 3}))
        .await
        .unwrap();
    let reenabled: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert!(!reenabled[0].disabled);
    assert_eq!(reenabled[0].config, json!({"version": 3}));
    let compact: Value =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert_eq!(
        compact,
        json!([{"id": "item", "name": "capture", "config": {"version": 3}}])
    );

    include.remove("item").await.unwrap();
    let removed: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert!(removed.is_empty());
}

#[tokio::test]
async fn watcher_debounces_config_changes_and_disables_the_live_plugin() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    tokio::fs::write(&path, r#"[{"id":"one","name":"foo"}]"#)
        .await
        .unwrap();
    let resolver = Arc::new(RegistryResolver::default());
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    resolver.register("foo", {
        let starts = Arc::clone(&starts);
        let stops = Arc::clone(&stops);
        Plugin::new("foo", move |_, _| {
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
    });
    let include = Include::open(
        Context::new(),
        resolver,
        IncludeConfig::new("config.json"),
        directory.path(),
    )
    .await
    .unwrap();
    let include = Arc::new(tokio::sync::Mutex::new(include));
    let mut watcher = Include::watch(
        Arc::clone(&include),
        Duration::from_millis(5),
        Duration::from_millis(20),
    )
    .await
    .unwrap();

    tokio::fs::write(&path, r#"[{"id":"one","name":"foo","disabled":true}]"#)
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), watcher.next())
        .await
        .unwrap()
        .unwrap();

    assert!(event.outcome.unwrap());
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    let snapshot = include.lock().await.loader().snapshots().await.remove(0);
    assert!(snapshot.options.disabled);
    assert_eq!(snapshot.state, None);

    tokio::fs::write(&path, r#"[{"id":"one","name":"foo"}]"#)
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), watcher.next())
        .await
        .unwrap()
        .unwrap();
    assert!(event.outcome.unwrap());
    let fiber = include
        .lock()
        .await
        .loader()
        .fiber("one")
        .await
        .unwrap()
        .unwrap();

    fiber.update(json!({"version": 2})).await.unwrap();
    let saved: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert_eq!(saved[0].config, json!({"version": 2}));

    fiber.dispose().await.unwrap();
    let saved: Vec<EntryOptions> =
        serde_json::from_str(&tokio::fs::read_to_string(&path).await.unwrap()).unwrap();
    assert!(saved[0].disabled);
    watcher.stop().await;
}

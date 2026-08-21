use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicUsize, Ordering},
};

use cordis_core::{
    AccessorGetter, AccessorSetter, CallableService, Context, CordisError, Disposer,
    ExtendableService, Plugin, Result,
};
use serde_json::{Map, Value, json};

#[derive(Debug)]
struct Session {
    answer: AtomicU32,
}

#[tokio::test]
async fn accessors_are_typed_scoped_and_support_receivers() {
    let context = Context::new();
    let getter: AccessorGetter = Arc::new(|_, receiver| {
        let session = receiver
            .and_then(|value| Arc::downcast::<Session>(value).ok())
            .ok_or_else(|| CordisError::InvalidConfig("missing session receiver".to_owned()))?;
        Ok(Some(Arc::new(session.answer.load(Ordering::Acquire))))
    });
    let setter: AccessorSetter = Arc::new(|_, value, receiver| {
        let answer =
            Arc::downcast::<u32>(value).map_err(|_| CordisError::PropertyTypeMismatch {
                name: "session.answer".to_owned(),
                expected: "u32",
            })?;
        let session = receiver
            .and_then(|value| Arc::downcast::<Session>(value).ok())
            .ok_or_else(|| CordisError::InvalidConfig("missing session receiver".to_owned()))?;
        session.answer.store(*answer, Ordering::Release);
        Ok(true)
    });
    let accessor = context
        .register_accessor("session.answer", getter, Some(setter))
        .unwrap();

    let duplicate = context
        .register_accessor("session.answer", Arc::new(|_, _| Ok(None)), None)
        .unwrap_err();
    assert!(matches!(
        duplicate,
        CordisError::PropertyAlreadyDeclared(name) if name == "session.answer"
    ));

    let session = Session {
        answer: AtomicU32::new(42),
    };
    let service = context.provide("session", session).await.unwrap();
    let associated = context.associated::<Session>("session").unwrap().unwrap();
    assert_eq!(*associated.member::<u32>("answer").unwrap().unwrap(), 42);
    assert!(associated.set_member("answer", 100_u32).unwrap());
    assert_eq!(*associated.member::<u32>("answer").unwrap().unwrap(), 100);

    let wrong_type = associated.member::<String>("answer").unwrap_err();
    assert!(matches!(
        wrong_type,
        CordisError::PropertyTypeMismatch { name, .. } if name == "session.answer"
    ));

    accessor.dispose().await.unwrap();
    assert!(associated.member::<u32>("answer").unwrap().is_none());
    service.dispose().await.unwrap();
}

#[tokio::test]
async fn read_only_accessors_reject_assignment() {
    let context = Context::new();
    let accessor = context
        .register_accessor("version", Arc::new(|_, _| Ok(Some(Arc::new(1_u32)))), None)
        .unwrap();

    assert_eq!(
        *context.property::<u32>("version", None).unwrap().unwrap(),
        1
    );
    let error = context
        .set_property("version", Arc::new(2_u32), None)
        .unwrap_err();
    assert!(matches!(error, CordisError::ReadOnlyProperty(name) if name == "version"));
    accessor.dispose().await.unwrap();
}

#[derive(Debug)]
struct ProviderService(&'static str);

#[tokio::test]
async fn tracked_services_preserve_caller_and_provider_contexts() {
    let root = Context::new();
    let plugin = Plugin::new("provider", |context, _| async move {
        context
            .provide("provider", ProviderService("ready"))
            .await?;
        Ok(None)
    });
    let fiber = root.plugin(plugin, Value::Null).await.unwrap();
    let caller = root.intercept("request-id", json!("req-1"));

    let tracked = caller
        .get_tracked::<ProviderService>("provider")
        .unwrap()
        .unwrap();
    assert_eq!(tracked.0, "ready");
    assert_eq!(
        tracked.caller().intercept_value("request-id"),
        Some(&json!("req-1"))
    );
    assert_eq!(
        tracked.origin().fiber().snapshot().unwrap().name,
        "provider"
    );

    fiber.dispose().await.unwrap();
    assert!(
        caller
            .get_tracked::<ProviderService>("provider")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn tracked_service_logger_uses_service_name_unless_caller_overrides_it() {
    let root = Context::new();
    let service = root
        .provide("provider", ProviderService("ready"))
        .await
        .unwrap();
    root.get_tracked::<ProviderService>("provider")
        .unwrap()
        .unwrap()
        .logger()
        .info("service", vec![])
        .unwrap();
    root.intercept("logger", json!({"name": "request"}))
        .get_tracked::<ProviderService>("provider")
        .unwrap()
        .unwrap()
        .logger()
        .info("caller", vec![])
        .unwrap();

    let buffer = root.logger_service().buffer().unwrap();
    assert_eq!(buffer[0].name, "provider");
    assert_eq!(buffer[1].name, "request");
    service.dispose().await.unwrap();
}

struct Formatter {
    prefix: &'static str,
}

impl CallableService<&str> for Formatter {
    type Output = String;

    fn invoke(&self, caller: &Context, suffix: &str) -> Self::Output {
        let request = caller
            .intercept_value("request")
            .and_then(Value::as_str)
            .unwrap_or("none");
        format!("{}:{request}:{suffix}", self.prefix)
    }
}

#[tokio::test]
async fn callable_services_receive_the_call_site_context() -> Result<()> {
    let root = Context::new();
    let service = root
        .provide("formatter", Formatter { prefix: "base" })
        .await?;

    assert_eq!(
        root.invoke::<Formatter, _>("formatter", "plain")?,
        "base:none:plain"
    );
    let caller = root.intercept("request", json!("overridden"));
    assert_eq!(
        caller.invoke::<Formatter, _>("formatter", "tail")?,
        "base:overridden:tail"
    );

    service.dispose().await?;
    Ok(())
}

#[derive(Clone)]
struct ConfigService {
    config: Map<String, Value>,
}

impl CallableService<Option<Map<String, Value>>> for ConfigService {
    type Output = Value;

    fn invoke(&self, caller: &Context, head: Option<Map<String, Value>>) -> Self::Output {
        let mut merged = self.config.clone();
        if let Some(intercept) = caller.intercept_value("config").and_then(Value::as_object) {
            merged.extend(intercept.clone());
        }
        if let Some(head) = head {
            merged.extend(head);
        }
        Value::Object(merged)
    }
}

impl ExtendableService<Map<String, Value>> for ConfigService {
    fn extend(&self, _: &Context, config: Map<String, Value>) -> Self {
        let mut merged = self.config.clone();
        merged.extend(config);
        Self { config: merged }
    }
}

#[tokio::test]
async fn callable_extensions_preserve_caller_context_and_merge_config() -> Result<()> {
    let root = Context::new();
    let service = root
        .provide(
            "config",
            ConfigService {
                config: json!({"a": 1}).as_object().unwrap().clone(),
            },
        )
        .await?;
    let caller = root.intercept("config", json!({"b": 2}));
    let tracked = caller
        .get_tracked::<ConfigService>("config")?
        .expect("service is active");
    assert_eq!(tracked.invoke(None), json!({"a": 1, "b": 2}));

    let extended = tracked.extend(json!({"c": 3}).as_object().unwrap().clone());
    assert_eq!(
        extended.invoke(Some(json!({"d": 4}).as_object().unwrap().clone())),
        json!({"a": 1, "b": 2, "c": 3, "d": 4})
    );
    assert_eq!(
        extended.caller().intercept_value("config"),
        Some(&json!({"b": 2}))
    );
    service.dispose().await?;
    Ok(())
}

#[tokio::test]
async fn associated_members_can_be_independent_services() {
    let context = Context::new();
    let base = context
        .provide(
            "session",
            Session {
                answer: AtomicU32::new(0),
            },
        )
        .await
        .unwrap();
    let member = context
        .provide("session.status", String::from("active"))
        .await
        .unwrap();
    let associated = context.associated::<Session>("session").unwrap().unwrap();

    assert_eq!(
        associated
            .member::<String>("status")
            .unwrap()
            .as_deref()
            .map(String::as_str),
        Some("active")
    );
    member.dispose().await.unwrap();
    assert!(associated.member::<String>("status").unwrap().is_none());
    base.dispose().await.unwrap();
}

struct DependencyBoundMethod {
    calls: Arc<AtomicUsize>,
    disposals: Arc<AtomicUsize>,
}

impl DependencyBoundMethod {
    async fn install(&self, context: &Context) -> Result<cordis_core::FiberHandle> {
        let calls = Arc::clone(&self.calls);
        let disposals = Arc::clone(&self.disposals);
        context
            .inject(["foo"], move |_| {
                let calls = Arc::clone(&calls);
                let disposals = Arc::clone(&disposals);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let disposer: Disposer = Arc::new(move || {
                        let disposals = Arc::clone(&disposals);
                        Box::pin(async move {
                            disposals.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                    });
                    Ok(Some(disposer))
                }
            })
            .await
    }
}

#[tokio::test]
async fn inject_owns_method_effects_for_the_dependency_lifecycle() {
    let context = Context::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let disposals = Arc::new(AtomicUsize::new(0));
    let method = DependencyBoundMethod {
        calls: Arc::clone(&calls),
        disposals: Arc::clone(&disposals),
    };
    let binding = method.install(&context).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let dependency = context.provide("foo", ()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(disposals.load(Ordering::SeqCst), 0);

    dependency.dispose().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
    binding.dispose().await.unwrap();
}

struct PluginLoader {
    origin: Context,
}

impl PluginLoader {
    async fn load(&self, plugin: Plugin) -> Result<cordis_core::FiberHandle> {
        self.origin.plugin(plugin, Value::Null).await
    }
}

#[tokio::test]
async fn tracked_service_calls_do_not_leak_the_caller_scope_into_child_plugins() {
    let root = Context::new();
    let loader_guard = root
        .provide(
            "loader",
            PluginLoader {
                origin: root.clone(),
            },
        )
        .await
        .unwrap();
    let caller = root.isolate("server");
    let loader = caller
        .get_tracked::<PluginLoader>("loader")
        .unwrap()
        .unwrap();

    let provider = loader
        .load(Plugin::new("server", |context, _| async move {
            context.provide("server", 42_u32).await?;
            Ok(None)
        }))
        .await
        .unwrap();
    let injected = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&injected);
    let consumer = loader
        .load(
            Plugin::builder("consumer", move |context, _| {
                let calls = Arc::clone(&calls);
                async move {
                    assert_eq!(*context.get::<u32>("server").unwrap(), 42);
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                }
            })
            .inject("server")
            .build(),
        )
        .await
        .unwrap();

    assert_eq!(injected.load(Ordering::SeqCst), 1);
    assert!(provider.context().unwrap().same_isolation(&root, "server"));
    assert!(!caller.same_isolation(&root, "server"));
    consumer.dispose().await.unwrap();
    provider.dispose().await.unwrap();
    loader_guard.dispose().await.unwrap();
}

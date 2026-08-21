use std::sync::Arc;

use cordis_core::{Context, CordisError, Plugin, Result};
use cordis_loader::{
    EXPRESSION_KEY, EntryOptions, Loader, RegistryResolver, interpolate, is_expression,
};
use parking_lot::Mutex;
use serde_json::{Value, json};

#[test]
fn recursive_interpolation_matches_expression_object_semantics() {
    let context = Context::new().intercept("answer", json!(42));
    let evaluator = |context: &Context, expression: &str| -> Result<Value> {
        match expression {
            "answer" => Ok(context.intercept_value("answer").unwrap().clone()),
            "object" => Ok(json!({"resolved": true})),
            other => Err(CordisError::InvalidConfig(format!(
                "unknown expression `{other}`"
            ))),
        }
    };
    let source = json!({
        "plain": 1,
        "nested": [
            { (EXPRESSION_KEY): "answer" },
            { "deep": { (EXPRESSION_KEY): "object", "ignored": true } }
        ]
    });

    assert!(is_expression(&json!({ (EXPRESSION_KEY): "answer" })));
    assert!(!is_expression(&source));
    assert_eq!(
        interpolate(&context, source, &evaluator).unwrap(),
        json!({
            "plain": 1,
            "nested": [42, { "deep": { "resolved": true } }]
        })
    );
    assert!(interpolate(&context, json!({ (EXPRESSION_KEY): 42 }), &evaluator).is_err());
}

#[tokio::test]
async fn loader_resolves_dynamic_config_on_create_and_update_but_stores_source() {
    let root = Context::new().intercept("answer", json!(42));
    let resolver = Arc::new(RegistryResolver::default());
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&seen);
    resolver.register(
        "capture",
        Plugin::new("capture", move |_, config| {
            let captured = Arc::clone(&captured);
            async move {
                captured.lock().push(config);
                Ok(None)
            }
        }),
    );
    let evaluator = Arc::new(|context: &Context, expression: &str| -> Result<Value> {
        context
            .intercept_value(expression)
            .cloned()
            .ok_or_else(|| CordisError::InvalidConfig(expression.to_owned()))
    });
    let loader = Loader::new(root, resolver).with_evaluator(evaluator);
    let source = json!({"value": { (EXPRESSION_KEY): "answer" }});
    let id = loader
        .create(
            EntryOptions {
                id: Some("capture".into()),
                name: "capture".into(),
                config: source.clone(),
                ..Default::default()
            },
            None,
            0,
        )
        .await
        .unwrap();

    assert_eq!(&*seen.lock(), &[json!({"value": 42})]);
    assert_eq!(loader.snapshots().await[0].options.config, source);

    loader
        .update_config(&id, json!({"items": [{ (EXPRESSION_KEY): "answer" }]}))
        .await
        .unwrap();
    assert_eq!(seen.lock().last(), Some(&json!({"items": [42]})));
}

#[tokio::test]
async fn expression_without_host_evaluator_fails_without_leaking_an_entry() {
    let resolver = Arc::new(RegistryResolver::default());
    resolver.register("noop", Plugin::new("noop", |_, _| async { Ok(None) }));
    let loader = Loader::new(Context::new(), resolver);

    let result = loader
        .create(
            EntryOptions {
                id: Some("dynamic".into()),
                name: "noop".into(),
                config: json!({ (EXPRESSION_KEY): "answer" }),
                ..Default::default()
            },
            None,
            0,
        )
        .await;

    assert!(matches!(result, Err(CordisError::InvalidConfig(_))));
    assert!(loader.snapshots().await.is_empty());
}

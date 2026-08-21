use std::sync::Arc;

use cordis_core::{Context, Exporter, LogLevel, LogValue, Message, Plugin, format_message};
use parking_lot::Mutex;
use serde_json::json;

struct Capture {
    messages: Arc<Mutex<Vec<Message>>>,
    level: LogLevel,
    max_length: usize,
}

impl Exporter for Capture {
    fn level(&self, _: &str) -> Option<LogLevel> {
        Some(self.level)
    }
    fn max_length(&self) -> usize {
        self.max_length
    }
    fn format(&self, specifier: char, _: &LogValue, _: &Message) -> Option<String> {
        (specifier == 'x').then(|| "custom".to_owned())
    }
    fn export(&self, message: &Message) {
        self.messages.lock().push(message.clone());
    }
}

#[tokio::test]
async fn bounded_buffer_stays_chronological() {
    let context = Context::new();
    let service = context.logger_service();
    service.set_buffer_size(2).unwrap();
    context.logger().info("one", vec![]).unwrap();
    context.logger().info("two", vec![]).unwrap();
    context.logger().info("three", vec![]).unwrap();
    let buffer = service.buffer().unwrap();
    assert_eq!(buffer.len(), 2);
    assert_eq!(buffer[0].args[0], LogValue::from("two"));
    assert_eq!(buffer[1].args[0], LogValue::from("three"));
    service.set_buffer_size(0).unwrap();
    assert!(service.buffer().unwrap().is_empty());
}

#[test]
fn built_in_buffer_honours_the_logger_level() {
    let context = Context::new();
    context
        .logger_named("test")
        .debug("hidden", vec![])
        .unwrap();
    assert!(context.logger_service().buffer().unwrap().is_empty());

    context
        .logger_named("test")
        .with_level(LogLevel::Debug)
        .debug("visible", vec![])
        .unwrap();
    let buffer = context.logger_service().buffer().unwrap();
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer[0].args, vec![LogValue::from("visible")]);
}

#[test]
fn nested_logger_intercepts_merge_from_outer_to_inner() {
    let context = Context::new()
        .intercept("logger", json!({"level": 3}))
        .intercept("logger", json!({"name": "nested"}));
    context.logger().debug("visible", vec![]).unwrap();

    let buffer = context.logger_service().buffer().unwrap();
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer[0].name, "nested");
    assert_eq!(
        context.intercept_chain("logger"),
        vec![json!({"level": 3}), json!({"name": "nested"})]
    );
}

#[tokio::test]
async fn exporter_lifetime_levels_and_names_follow_context() {
    let context = Context::new();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let effect = context
        .logger_service()
        .exporter(Arc::new(Capture {
            messages: Arc::clone(&messages),
            level: LogLevel::Debug,
            max_length: 100,
        }))
        .unwrap();
    context.logger().debug("root", vec![]).unwrap();
    context
        .intercept("logger", json!({ "name": "override", "level": 3 }))
        .logger()
        .debug("intercept", vec![])
        .unwrap();
    let plugin = Plugin::new("driver", {
        let messages = Arc::clone(&messages);
        move |plugin_context, _| {
            let messages = Arc::clone(&messages);
            async move {
                plugin_context.logger().debug("plugin", vec![])?;
                assert_eq!(messages.lock().last().unwrap().name, "driver");
                Ok(None)
            }
        }
    });
    context.plugin(plugin, json!(null)).await.unwrap();
    assert_eq!(
        messages
            .lock()
            .iter()
            .map(|message| message.name.as_str())
            .collect::<Vec<_>>(),
        vec!["root", "override", "driver"]
    );
    effect.dispose().await.unwrap();
    context.logger().info("ignored", vec![]).unwrap();
    assert_eq!(messages.lock().len(), 3);
}

#[test]
fn formatter_supports_specifiers_customization_unicode_and_truncation() {
    let exporter = Capture {
        messages: Arc::new(Mutex::new(Vec::new())),
        level: LogLevel::Debug,
        max_length: 3,
    };
    let message = Message {
        sequence: 1,
        timestamp_ms: 0,
        name: "test".to_owned(),
        kind: cordis_core::LogType::Info,
        level: LogLevel::Info,
        args: vec![
            LogValue::from("%s %d %f %o %x %%"),
            LogValue::from("世界abc"),
            LogValue::from(2_i32),
            LogValue::from(2.5),
            LogValue::from(json!({"x": 1})),
            LogValue::Null,
        ],
        fiber: None,
    };
    assert_eq!(format_message(&exporter, &message), "世界a...");
}

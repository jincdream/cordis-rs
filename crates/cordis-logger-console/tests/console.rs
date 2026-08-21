use std::sync::Arc;

use cordis_core::{Context, LogLevel, LogType, LogValue, Message};
use cordis_logger_console::{ConsoleConfig, ConsoleExporter, LabelAlign, LabelStyle};
use parking_lot::Mutex;
use serde_json::json;

#[tokio::test]
async fn renders_objects_custom_formatters_levels_and_multiline_labels() {
    let context = Context::new();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&lines);
    let config = ConsoleConfig {
        show_diff: true,
        show_time: String::new(),
        levels: [("default".to_owned(), LogLevel::Debug)].into(),
        ..Default::default()
    };
    let (exporter, _) =
        ConsoleExporter::install_with(&context, config, move |line| captured.lock().push(line))
            .unwrap();
    context
        .logger_named("test")
        .info(LogValue::from(json!({"foo": "bar"})), vec![])
        .unwrap();
    assert_eq!(lines.lock()[0], "[I] test { foo: 'bar' } +0ms");
    exporter.set_formatter('x', |_, _| "custom".to_owned());
    context
        .logger_named("test")
        .info("%x%%x", vec![LogValue::Null])
        .unwrap();
    assert!(lines.lock()[1].contains("custom%x"));

    let styled = ConsoleConfig {
        show_diff: false,
        show_time: String::new(),
        label: LabelStyle {
            width: 10,
            margin: 2,
            align: LabelAlign::Right,
        },
        ..Default::default()
    };
    let rendered = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::clone(&rendered);
    let (_, _) =
        ConsoleExporter::install_with(&context, styled, move |line| target.lock().push(line))
            .unwrap();
    context
        .logger_named("test")
        .info("message\nmessage", vec![])
        .unwrap();
    assert_eq!(
        rendered.lock()[0],
        "      test  [I]  message\n                 message"
    );
}

#[tokio::test]
async fn renders_errors_and_applies_per_name_level_overrides() {
    let context = Context::new();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&lines);
    let config = ConsoleConfig {
        show_time: String::new(),
        levels: [
            ("default".to_owned(), LogLevel::Debug),
            ("quiet".to_owned(), LogLevel::Error),
        ]
        .into(),
        ..Default::default()
    };
    let (_, _) =
        ConsoleExporter::install_with(&context, config, move |line| captured.lock().push(line))
            .unwrap();

    context
        .logger_named("quiet")
        .debug("hidden", vec![])
        .unwrap();
    context
        .logger_named("loud")
        .debug("visible", vec![])
        .unwrap();
    context
        .logger_named("quiet")
        .error(LogValue::Error("message".into()), vec![])
        .unwrap();

    assert_eq!(
        &*lines.lock(),
        &[
            "[D] loud visible".to_owned(),
            "[E] quiet message".to_owned()
        ]
    );
}

#[tokio::test]
async fn diff_uses_message_timestamps() {
    let context = Context::new();
    let (exporter, _) = ConsoleExporter::install_with(
        &context,
        ConsoleConfig {
            show_time: String::new(),
            show_diff: true,
            ..Default::default()
        },
        |_| {},
    )
    .unwrap();
    let message = |timestamp_ms| Message {
        sequence: timestamp_ms,
        timestamp_ms,
        name: "test".into(),
        kind: LogType::Info,
        level: LogLevel::Info,
        args: vec![LogValue::from("message")],
        fiber: None,
    };

    assert_eq!(exporter.render(&message(100)), "[I] test message +0ms");
    assert_eq!(exporter.render(&message(102)), "[I] test message +2ms");
}

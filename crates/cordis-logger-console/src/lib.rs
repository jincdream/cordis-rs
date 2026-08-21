//! Console rendering for Cordis log messages.

use std::{collections::HashMap, sync::Arc};

use cordis_core::{
    Context, EffectHandle, Exporter, LogLevel, LogValue, Message, Result, color, color_code,
    format_message,
};
use parking_lot::{Mutex, RwLock};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LabelAlign {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct LabelStyle {
    pub width: usize,
    pub margin: usize,
    pub align: LabelAlign,
}

impl Default for LabelStyle {
    fn default() -> Self {
        Self {
            width: 0,
            margin: 1,
            align: LabelAlign::Left,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsoleConfig {
    pub colors: Option<u8>,
    pub max_length: usize,
    pub levels: HashMap<String, LogLevel>,
    pub show_diff: bool,
    pub show_time: String,
    pub label: LabelStyle,
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        Self {
            colors: None,
            max_length: 10_240,
            levels: HashMap::new(),
            show_diff: false,
            show_time: "%Y-%m-%d %H:%M:%S ".to_owned(),
            label: LabelStyle::default(),
        }
    }
}

type Formatter = Arc<dyn Fn(&LogValue, &Message) -> String + Send + Sync>;
type Sink = Arc<dyn Fn(String) + Send + Sync>;

pub struct ConsoleExporter {
    config: ConsoleConfig,
    timestamp: Mutex<Option<u64>>,
    formatters: RwLock<HashMap<char, Formatter>>,
    sink: Sink,
}

impl ConsoleExporter {
    pub fn install(context: &Context, config: ConsoleConfig) -> Result<(Arc<Self>, EffectHandle)> {
        Self::install_with(context, config, |line| println!("{line}"))
    }

    pub fn install_with<F>(
        context: &Context,
        config: ConsoleConfig,
        sink: F,
    ) -> Result<(Arc<Self>, EffectHandle)>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let exporter = Arc::new(Self {
            config,
            timestamp: Mutex::new(None),
            formatters: RwLock::new(HashMap::new()),
            sink: Arc::new(sink),
        });
        let effect = context.logger_service().exporter(exporter.clone())?;
        Ok((exporter, effect))
    }

    pub fn set_formatter<F>(&self, specifier: char, formatter: F)
    where
        F: Fn(&LogValue, &Message) -> String + Send + Sync + 'static,
    {
        self.formatters
            .write()
            .insert(specifier, Arc::new(formatter));
    }

    pub fn render(&self, message: &Message) -> String {
        let prefix = format!("[{}]", message.kind.initial());
        let space = " ".repeat(self.config.label.margin);
        let mut indent = 3 + space.len();
        let mut output = String::new();
        if !self.config.show_time.is_empty() {
            let now = chrono::Local::now()
                .format(&self.config.show_time)
                .to_string();
            output.push_str(&color(self.config.colors, 8, &now, ""));
            indent += now.chars().count();
        }
        let code = color_code(&message.name, self.config.colors);
        let label = color(self.config.colors, code, &message.name, ";1");
        let width = self.config.label.width.max(message.name.chars().count());
        match self.config.label.align {
            LabelAlign::Right => {
                output.push_str(&" ".repeat(width - message.name.chars().count()));
                output.push_str(&label);
                output.push_str(&space);
                output.push_str(&prefix);
                output.push_str(&space);
                indent += width + space.len();
            }
            LabelAlign::Left => {
                output.push_str(&prefix);
                output.push_str(&space);
                output.push_str(&label);
                output.push_str(&" ".repeat(width - message.name.chars().count()));
                output.push_str(&space);
            }
        }
        output.push_str(
            &format_message(self, message).replace('\n', &format!("\n{}", " ".repeat(indent))),
        );
        let mut timestamp = self.timestamp.lock();
        if self.config.show_diff {
            let diff =
                timestamp.map_or(0, |previous| message.timestamp_ms.saturating_sub(previous));
            output.push_str(&color(
                self.config.colors,
                code,
                &format!(" +{}ms", diff),
                "",
            ));
        }
        *timestamp = Some(message.timestamp_ms);
        output
    }
}

impl Exporter for ConsoleExporter {
    fn colors(&self) -> Option<u8> {
        self.config.colors
    }
    fn max_length(&self) -> usize {
        self.config.max_length
    }
    fn level(&self, name: &str) -> Option<LogLevel> {
        self.config
            .levels
            .get(name)
            .or_else(|| self.config.levels.get("default"))
            .copied()
    }
    fn format(&self, specifier: char, value: &LogValue, message: &Message) -> Option<String> {
        if let Some(formatter) = self.formatters.read().get(&specifier) {
            return Some(formatter(value, message));
        }
        matches!(specifier, 'o' | 'O').then(|| inspect(value))
    }
    fn export(&self, message: &Message) {
        (self.sink)(self.render(message));
    }
}

fn inspect(value: &LogValue) -> String {
    match value {
        LogValue::Json(serde_json::Value::Object(map)) => format!(
            "{{ {} }}",
            map.iter()
                .map(|(key, value)| format!("{key}: {}", inspect_json(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        LogValue::Json(value) => inspect_json(value),
        other => format!("{other:?}"),
    }
}
fn inspect_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => format!("'{}'", value.replace('\'', "\\'")),
        serde_json::Value::Object(map) => format!(
            "{{ {} }}",
            map.iter()
                .map(|(key, value)| format!("{key}: {}", inspect_json(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        serde_json::Value::Array(values) => format!(
            "[ {} ]",
            values
                .iter()
                .map(inspect_json)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        value => value.to_string(),
    }
}

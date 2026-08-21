use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::{Mutex, RwLock};
use serde_json::Value;

use crate::{Context, EffectHandle, FiberId, Result, effect::infallible_disposer};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LogType {
    Error,
    Warn,
    Info,
    Debug,
}

impl LogType {
    pub fn initial(self) -> char {
        match self {
            Self::Error => 'E',
            Self::Warn => 'W',
            Self::Info => 'I',
            Self::Debug => 'D',
        }
    }
    fn level(self) -> LogLevel {
        match self {
            Self::Error => LogLevel::Error,
            Self::Warn => LogLevel::Warn,
            Self::Info => LogLevel::Info,
            Self::Debug => LogLevel::Debug,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Json(Value),
    Error(String),
    Null,
}

impl From<&str> for LogValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}
impl From<String> for LogValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<i64> for LogValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl From<i32> for LogValue {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}
impl From<f64> for LogValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}
impl From<bool> for LogValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
impl From<Value> for LogValue {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub name: String,
    pub kind: LogType,
    pub level: LogLevel,
    pub args: Vec<LogValue>,
    pub fiber: Option<FiberId>,
}

pub trait Exporter: Send + Sync + 'static {
    fn colors(&self) -> Option<u8> {
        None
    }
    fn max_length(&self) -> usize {
        10_240
    }
    fn level(&self, _name: &str) -> Option<LogLevel> {
        None
    }
    fn format(&self, _specifier: char, _value: &LogValue, _message: &Message) -> Option<String> {
        None
    }
    fn export(&self, message: &Message);
}

pub(crate) struct LoggerServiceInner {
    sequence: std::sync::atomic::AtomicU64,
    exporter_sequence: std::sync::atomic::AtomicU64,
    buffer_size: std::sync::atomic::AtomicUsize,
    buffer: Mutex<VecDeque<Message>>,
    exporters: RwLock<HashMap<u64, Arc<dyn Exporter>>>,
}

impl LoggerServiceInner {
    pub(crate) fn new() -> Self {
        Self {
            sequence: 0.into(),
            exporter_sequence: 0.into(),
            buffer_size: 1000.into(),
            buffer: Mutex::new(VecDeque::new()),
            exporters: RwLock::new(HashMap::new()),
        }
    }
    fn push_buffer(&self, message: Message) {
        let capacity = self.buffer_size.load(Ordering::Relaxed);
        let mut buffer = self.buffer.lock();
        if capacity == 0 {
            buffer.clear();
            return;
        }
        buffer.push_back(message);
        while buffer.len() > capacity {
            buffer.pop_front();
        }
    }
}

#[derive(Clone)]
pub struct LoggerService {
    context: Context,
}

impl LoggerService {
    pub(crate) fn new(context: Context) -> Self {
        Self { context }
    }

    pub fn logger(&self, explicit_name: Option<String>) -> Logger {
        let mut intercept = serde_json::Map::new();
        for layer in self.context.intercept_chain("logger") {
            if let Value::Object(layer) = layer {
                intercept.extend(layer);
            }
        }
        let intercept_name = intercept
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let level = intercept
            .get("level")
            .and_then(Value::as_u64)
            .and_then(level_from_u64);
        let fiber = self.context.fiber().snapshot();
        Logger {
            service: self.clone(),
            name: explicit_name
                .or(intercept_name)
                .or_else(|| fiber.as_ref().map(|fiber| fiber.name.clone()))
                .unwrap_or_else(|| "root".to_owned()),
            level,
            fiber: fiber.and_then(|fiber| fiber.id),
        }
    }

    pub fn set_buffer_size(&self, size: usize) -> Result<()> {
        let inner = self.context.logger_inner()?;
        inner.buffer_size.store(size, Ordering::Relaxed);
        let mut buffer = inner.buffer.lock();
        while buffer.len() > size {
            buffer.pop_front();
        }
        Ok(())
    }
    pub fn buffer(&self) -> Result<Vec<Message>> {
        Ok(self
            .context
            .logger_inner()?
            .buffer
            .lock()
            .iter()
            .cloned()
            .collect())
    }
    pub fn exporter(&self, exporter: Arc<dyn Exporter>) -> Result<EffectHandle> {
        let inner = self.context.logger_inner()?;
        let id = inner.exporter_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        inner.exporters.write().insert(id, exporter);
        let weak = Arc::downgrade(&inner);
        let disposer = infallible_disposer(move || {
            if let Some(inner) = weak.upgrade() {
                inner.exporters.write().remove(&id);
            }
        });
        self.context.register_effect(EffectHandle::collection(
            "ctx.logger.exporter()",
            vec![EffectHandle::disposer(disposer)],
        ))
    }
}

#[derive(Clone)]
pub struct Logger {
    service: LoggerService,
    pub name: String,
    pub level: Option<LogLevel>,
    fiber: Option<FiberId>,
}

impl Logger {
    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.level = Some(level);
        self
    }
    pub fn error(&self, format: impl Into<LogValue>, args: Vec<LogValue>) -> Result<()> {
        self.log(LogType::Error, format, args)
    }
    pub fn warn(&self, format: impl Into<LogValue>, args: Vec<LogValue>) -> Result<()> {
        self.log(LogType::Warn, format, args)
    }
    pub fn info(&self, format: impl Into<LogValue>, args: Vec<LogValue>) -> Result<()> {
        self.log(LogType::Info, format, args)
    }
    pub fn debug(&self, format: impl Into<LogValue>, args: Vec<LogValue>) -> Result<()> {
        self.log(LogType::Debug, format, args)
    }
    pub fn log(
        &self,
        kind: LogType,
        format: impl Into<LogValue>,
        mut args: Vec<LogValue>,
    ) -> Result<()> {
        args.insert(0, format.into());
        let inner = self.service.context.logger_inner()?;
        let message = Message {
            sequence: inner.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            name: self.name.clone(),
            kind,
            level: kind.level(),
            args,
            fiber: self.fiber,
        };
        let buffer_level = self.level.unwrap_or(LogLevel::Info);
        if buffer_level as u8 >= message.level as u8 {
            inner.push_buffer(message.clone());
        }
        for exporter in inner.exporters.read().values() {
            let target = exporter
                .level(&self.name)
                .or(self.level)
                .unwrap_or(LogLevel::Info);
            if target as u8 >= message.level as u8 {
                exporter.export(&message);
            }
        }
        Ok(())
    }
}

fn level_from_u64(level: u64) -> Option<LogLevel> {
    match level {
        0 => Some(LogLevel::Error),
        1 => Some(LogLevel::Warn),
        2 => Some(LogLevel::Info),
        3 => Some(LogLevel::Debug),
        _ => None,
    }
}

pub fn format_message(exporter: &dyn Exporter, message: &Message) -> String {
    let mut args = message.args.clone().into_iter();
    let first = args.next().unwrap_or(LogValue::Null);
    let mut remaining = args.collect::<VecDeque<_>>();
    let format = match first {
        LogValue::String(value) => value,
        LogValue::Error(value) => {
            remaining.push_front(LogValue::String(value));
            "%s".to_owned()
        }
        value => {
            remaining.push_front(value);
            "%o".to_owned()
        }
    };
    let mut output = String::new();
    let mut chars = format.chars();
    while let Some(character) = chars.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        let Some(specifier) = chars.next() else {
            output.push('%');
            break;
        };
        if specifier == '%' {
            output.push('%');
            continue;
        }
        let Some(value) = remaining.pop_front() else {
            output.push('%');
            output.push(specifier);
            continue;
        };
        output.push_str(
            &exporter
                .format(specifier, &value, message)
                .unwrap_or_else(|| default_format(specifier, &value, exporter, message)),
        );
    }
    for value in remaining {
        output.push(' ');
        output.push_str(&default_format('o', &value, exporter, message));
    }
    output
        .split('\n')
        .map(|line| truncate(line, exporter.max_length()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate(line: &str, limit: usize) -> String {
    let mut chars = line.chars();
    let prefix = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}...")
    } else {
        prefix
    }
}

fn default_format(
    specifier: char,
    value: &LogValue,
    exporter: &dyn Exporter,
    message: &Message,
) -> String {
    match specifier {
        's' => value_string(value),
        'd' | 'i' => value_number(value).trunc().to_string(),
        'f' => value_number(value).to_string(),
        'o' | 'O' => value_json(value),
        'c' => String::new(),
        'C' => color(
            exporter.colors(),
            color_code(&message.name, exporter.colors()),
            &value_string(value),
            "",
        ),
        _ => format!("%{specifier}"),
    }
}
fn value_string(value: &LogValue) -> String {
    match value {
        LogValue::String(v) | LogValue::Error(v) => v.clone(),
        LogValue::Integer(v) => v.to_string(),
        LogValue::Float(v) => v.to_string(),
        LogValue::Bool(v) => v.to_string(),
        LogValue::Json(v) => v.to_string(),
        LogValue::Null => "null".to_owned(),
    }
}
fn value_number(value: &LogValue) -> f64 {
    match value {
        LogValue::Integer(v) => *v as f64,
        LogValue::Float(v) => *v,
        LogValue::Bool(v) => u8::from(*v).into(),
        LogValue::String(v) => v.parse().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}
fn value_json(value: &LogValue) -> String {
    match value {
        LogValue::Json(v) => v.to_string(),
        _ => value_string(value),
    }
}

const C16: [u8; 6] = [6, 2, 3, 4, 5, 1];
const C256: [u8; 73] = [
    20, 21, 26, 27, 32, 33, 38, 39, 40, 41, 42, 43, 44, 45, 56, 57, 62, 63, 68, 69, 74, 75, 76, 77,
    78, 79, 80, 81, 92, 93, 98, 99, 112, 113, 129, 134, 135, 148, 149, 160, 161, 162, 163, 164,
    165, 166, 167, 168, 169, 170, 171, 172, 173, 178, 179, 184, 185, 196, 197, 198, 199, 200, 201,
    202, 203, 204, 205, 206, 207, 208, 209, 214, 215,
];
pub fn color_code(name: &str, colors: Option<u8>) -> u8 {
    let mut hash = 0_i32;
    for character in name.encode_utf16() {
        hash = hash
            .wrapping_shl(3)
            .wrapping_sub(hash)
            .wrapping_add(i32::from(character))
            .wrapping_add(13);
    }
    let palette: &[u8] = match colors {
        Some(level) if level >= 2 => &C256,
        Some(1) => &C16,
        _ => return 0,
    };
    palette[hash.unsigned_abs() as usize % palette.len()]
}
pub fn color(colors: Option<u8>, code: u8, value: &str, decoration: &str) -> String {
    let Some(level) = colors.filter(|level| *level > 0) else {
        return value.to_owned();
    };
    let code = if code < 8 {
        code.to_string()
    } else {
        format!("8;5;{code}")
    };
    let decoration = if level >= 2 { decoration } else { "" };
    format!("\u{1b}[3{code}{decoration}m{value}\u{1b}[0m")
}

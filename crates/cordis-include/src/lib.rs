//! JSON/YAML-backed loader subtrees with declarative patches and atomic writes.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cordis_core::{
    Context, CordisError, EffectHandle, EventCallback, EventOptions, EventOutcome, Result,
};
use cordis_loader::{
    ConfigEvaluator, EXPRESSION_KEY, EntryOptions, EntryPosition, EntryUpdate, IsolationRule,
    Loader, PluginResolver,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PatchOptions {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub insert: Option<Vec<EntryOptions>>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub group: Option<bool>,
    #[serde(default)]
    pub disabled: Option<bool>,
    #[serde(default)]
    pub inject: Option<Vec<String>>,
    #[serde(default)]
    pub intercept: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub isolate: Option<BTreeMap<String, IsolationRule>>,
}

#[derive(Clone)]
pub struct IncludeConfig {
    pub path: PathBuf,
    pub initial: Option<Vec<EntryOptions>>,
    pub patches: Vec<PatchOptions>,
    pub enable_logs: bool,
    pub evaluator: Option<Arc<dyn ConfigEvaluator>>,
}

impl std::fmt::Debug for IncludeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IncludeConfig")
            .field("path", &self.path)
            .field("initial", &self.initial)
            .field("patches", &self.patches)
            .field("enable_logs", &self.enable_logs)
            .field("has_evaluator", &self.evaluator.is_some())
            .finish()
    }
}

impl IncludeConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            initial: None,
            patches: Vec::new(),
            enable_logs: false,
            evaluator: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PatchWarning {
    MissingId,
    EntryNotFound(String),
    NotAGroup(String),
    NameMismatch {
        id: String,
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone)]
pub struct IncludeWatchEvent {
    pub outcome: Result<bool>,
}

pub struct IncludeWatchHandle {
    stop: tokio::sync::watch::Sender<bool>,
    events: tokio::sync::mpsc::UnboundedReceiver<IncludeWatchEvent>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl IncludeWatchHandle {
    pub async fn next(&mut self) -> Option<IncludeWatchEvent> {
        self.events.recv().await
    }

    pub async fn stop(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for IncludeWatchHandle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct Include {
    filename: PathBuf,
    config: IncludeConfig,
    loader: Loader,
    content: Option<String>,
    data: Vec<EntryOptions>,
    warnings: Vec<PatchWarning>,
    readonly: Arc<AtomicBool>,
    persistence: Option<EffectHandle>,
}

impl Include {
    pub async fn open(
        context: Context,
        resolver: Arc<dyn PluginResolver>,
        config: IncludeConfig,
        base_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let filename = if config.path.is_absolute() {
            config.path.clone()
        } else {
            base_dir.as_ref().join(&config.path)
        };
        validate_extension(&filename)?;
        if tokio::fs::metadata(&filename).await.is_err() {
            let initial = config.initial.as_ref().ok_or_else(|| {
                CordisError::InvalidConfig(format!("config file not found: {}", filename.display()))
            })?;
            write_atomic(&filename, initial).await?;
        }
        let loader = match config.evaluator.clone() {
            Some(evaluator) => Loader::new(context, resolver).with_evaluator(evaluator),
            None => Loader::new(context, resolver),
        };
        let readonly = Arc::new(AtomicBool::new(false));
        let mut include = Self {
            filename,
            config,
            loader,
            content: None,
            data: Vec::new(),
            warnings: Vec::new(),
            readonly,
            persistence: None,
        };
        include.refresh_forced().await?;
        include.install_persistence()?;
        Ok(include)
    }

    pub fn loader(&self) -> &Loader {
        &self.loader
    }
    pub fn filename(&self) -> &Path {
        &self.filename
    }
    pub fn data(&self) -> &[EntryOptions] {
        &self.data
    }
    pub fn warnings(&self) -> &[PatchWarning] {
        &self.warnings
    }
    pub fn is_readonly(&self) -> bool {
        self.readonly.load(Ordering::Acquire)
    }

    fn install_persistence(&mut self) -> Result<()> {
        let loader = self.loader.downgrade();
        let filename = self.filename.clone();
        let readonly = Arc::clone(&self.readonly);
        let write_lock = Arc::new(tokio::sync::Mutex::new(()));
        let persistence: EventCallback = Arc::new(move |_| {
            let loader = loader.clone();
            let filename = filename.clone();
            let readonly = Arc::clone(&readonly);
            let write_lock = Arc::clone(&write_lock);
            Box::pin(async move {
                let _write = write_lock.lock().await;
                if readonly.load(Ordering::Acquire) {
                    return Err(CordisError::InvalidConfig(
                        "cannot overwrite readonly config".to_owned(),
                    ));
                }
                if let Some(config) = loader.root_config().await? {
                    write_atomic(&filename, &config).await?;
                }
                Ok(EventOutcome::Continue)
            })
        });
        self.persistence = Some(self.loader.context().on(
            "loader/config-update",
            persistence,
            EventOptions::default(),
        )?);
        Ok(())
    }

    /// Watches a shared include and applies debounced file changes transactionally.
    pub async fn watch(
        include: Arc<tokio::sync::Mutex<Self>>,
        poll_interval: Duration,
        debounce: Duration,
    ) -> Result<IncludeWatchHandle> {
        if poll_interval.is_zero() {
            return Err(CordisError::InvalidConfig(
                "include poll interval must be greater than zero".to_owned(),
            ));
        }
        let filename = {
            let include = include.lock().await;
            include.filename.clone()
        };
        let mut observed = tokio::fs::read_to_string(&filename)
            .await
            .map_err(io_error)?;
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let (event_sender, events) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.tick().await;
            let mut last_change = None;
            loop {
                tokio::select! {
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let Ok(content) = tokio::fs::read_to_string(&filename).await else {
                            continue;
                        };
                        if content != observed {
                            observed = content;
                            last_change = Some(tokio::time::Instant::now());
                        }
                        if last_change.is_none_or(|changed| changed.elapsed() < debounce) {
                            continue;
                        }
                        last_change = None;
                        let outcome = include.lock().await.refresh().await;
                        if event_sender.send(IncludeWatchEvent { outcome }).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Ok(IncludeWatchHandle {
            stop,
            events,
            task: Some(task),
        })
    }

    pub async fn refresh(&mut self) -> Result<bool> {
        let content = tokio::fs::read_to_string(&self.filename)
            .await
            .map_err(io_error)?;
        if self.content.as_deref() == Some(&content) {
            return Ok(false);
        }
        self.load_content(content).await?;
        Ok(true)
    }

    /// Creates an entry and persists the updated include tree atomically.
    pub async fn create(
        &mut self,
        options: EntryOptions,
        parent: Option<&str>,
        position: usize,
    ) -> Result<String> {
        let id = self.loader.create(options, parent, position).await?;
        self.write().await?;
        Ok(id)
    }

    /// Applies a partial update or move and persists the updated include tree atomically.
    pub async fn update(
        &mut self,
        id: &str,
        update: EntryUpdate,
        target: Option<EntryPosition>,
    ) -> Result<String> {
        let id = self.loader.update(id, update, target).await?;
        self.write().await?;
        Ok(id)
    }

    pub async fn set_disabled(&mut self, id: &str, disabled: bool) -> Result<()> {
        self.loader.set_disabled(id, disabled).await?;
        self.write().await
    }

    pub async fn update_config(&mut self, id: &str, config: Value) -> Result<()> {
        self.loader.update_config(id, config).await?;
        self.write().await
    }

    pub async fn move_entry(
        &mut self,
        id: &str,
        parent: Option<&str>,
        position: usize,
    ) -> Result<String> {
        let id = self.loader.move_entry(id, parent, position).await?;
        self.write().await?;
        Ok(id)
    }

    pub async fn remove(&mut self, id: &str) -> Result<()> {
        self.loader.remove(id).await?;
        self.write().await
    }

    async fn refresh_forced(&mut self) -> Result<()> {
        let content = tokio::fs::read_to_string(&self.filename)
            .await
            .map_err(io_error)?;
        self.load_content(content).await
    }

    async fn load_content(&mut self, content: String) -> Result<()> {
        let parsed = parse(&self.filename, &content)?;
        let (patched, warnings) = apply_patches(parsed, &self.config.patches);
        // Parsing and patching finish before existing fibers are touched.
        if let Err(error) = self.loader.replace(patched.clone()).await {
            if let Err(rollback) = self.loader.replace(self.data.clone()).await {
                return Err(CordisError::Internal(format!(
                    "include refresh failed ({error}); rollback also failed ({rollback})"
                )));
            }
            return Err(error);
        }
        self.readonly.store(
            tokio::fs::metadata(&self.filename)
                .await
                .map(|metadata| metadata.permissions().readonly())
                .unwrap_or(true),
            Ordering::Release,
        );
        self.content = Some(content);
        self.data = patched;
        self.warnings = warnings;
        Ok(())
    }

    pub async fn write(&mut self) -> Result<()> {
        if self.readonly.load(Ordering::Acquire) {
            return Err(CordisError::InvalidConfig(
                "cannot overwrite readonly config".to_owned(),
            ));
        }
        let config = self.loader.root_config().await?;
        write_atomic(&self.filename, &config).await?;
        self.content = Some(
            tokio::fs::read_to_string(&self.filename)
                .await
                .map_err(io_error)?,
        );
        self.data = config;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.dispose().await?;
        }
        self.loader.stop().await
    }
}

impl Drop for Include {
    fn drop(&mut self) {
        let Some(persistence) = self.persistence.take() else {
            return;
        };
        let loader = self.loader.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = persistence.dispose().await;
                let _ = loader.stop().await;
            });
        }
    }
}

pub fn apply_patches(
    mut data: Vec<EntryOptions>,
    patches: &[PatchOptions],
) -> (Vec<EntryOptions>, Vec<PatchWarning>) {
    let mut warnings = Vec::new();
    for patch in patches {
        if let Some(insert) = &patch.insert {
            match &patch.id {
                None => data.extend(insert.clone()),
                Some(id) => {
                    let mut was_group = false;
                    if !with_entry_mut(&mut data, id, &mut |target| {
                        was_group = target.group;
                        if !target.group {
                            return;
                        }
                        let mut children: Vec<EntryOptions> =
                            serde_json::from_value(target.config.clone()).unwrap_or_default();
                        children.extend(insert.clone());
                        target.config =
                            serde_json::to_value(children).expect("entry options serialize");
                    }) {
                        warnings.push(PatchWarning::EntryNotFound(id.clone()));
                    } else if !was_group {
                        warnings.push(PatchWarning::NotAGroup(id.clone()));
                    }
                }
            }
            continue;
        }
        let Some(id) = &patch.id else {
            warnings.push(PatchWarning::MissingId);
            continue;
        };
        let mut mismatch = None;
        if !with_entry_mut(&mut data, id, &mut |target| {
            if let Some(name) = &patch.name {
                if name != &target.name {
                    mismatch = Some(PatchWarning::NameMismatch {
                        id: id.clone(),
                        expected: target.name.clone(),
                        actual: name.clone(),
                    });
                    return;
                }
            }
            if let Some(value) = &patch.config {
                target.config = value.clone();
            }
            if let Some(value) = patch.group {
                target.group = value;
            }
            if let Some(value) = patch.disabled {
                target.disabled = value;
            }
            if let Some(value) = &patch.inject {
                target.inject = value.clone();
            }
            if let Some(value) = &patch.intercept {
                target.intercept = value.clone();
            }
            if let Some(value) = &patch.isolate {
                target.isolate = value.clone();
            }
        }) {
            warnings.push(PatchWarning::EntryNotFound(id.clone()));
        } else if let Some(warning) = mismatch {
            warnings.push(warning);
        }
    }
    (data, warnings)
}

fn with_entry_mut(
    entries: &mut [EntryOptions],
    id: &str,
    callback: &mut impl FnMut(&mut EntryOptions),
) -> bool {
    for entry in entries {
        if entry.id.as_deref() == Some(id) {
            callback(entry);
            return true;
        }
        if entry.group {
            let mut children: Vec<EntryOptions> =
                serde_json::from_value(entry.config.clone()).unwrap_or_default();
            if with_entry_mut(&mut children, id, callback) {
                entry.config = serde_json::to_value(children).expect("entry options serialize");
                return true;
            }
        }
    }
    false
}

fn parse(path: &Path, content: &str) -> Result<Vec<EntryOptions>> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("json") => serde_json::from_str(content)
            .map_err(|error| CordisError::InvalidConfig(error.to_string())),
        Some("yaml" | "yml") => serde_yaml::from_value(parse_yaml_with_expressions(content)?)
            .map_err(|error| CordisError::InvalidConfig(error.to_string())),
        _ => Err(CordisError::InvalidConfig(
            "unsupported config extension".to_owned(),
        )),
    }
}

fn parse_yaml_with_expressions(content: &str) -> Result<serde_yaml::Value> {
    use yaml_rust2::parser::Parser;

    let mut parser = Parser::new(content.chars());
    let mut receiver = ExpressionYamlReceiver::default();
    parser
        .load(&mut receiver, true)
        .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
    if let Some(error) = receiver.error {
        return Err(CordisError::InvalidConfig(error));
    }
    match receiver.documents.as_slice() {
        [document] => Ok(document.clone()),
        [] => Err(CordisError::InvalidConfig("empty YAML stream".to_owned())),
        _ => Err(CordisError::InvalidConfig(
            "multiple YAML documents are not supported".to_owned(),
        )),
    }
}

#[derive(Debug)]
enum YamlContainer {
    Sequence {
        values: Vec<serde_yaml::Value>,
        anchor: usize,
    },
    Mapping {
        values: serde_yaml::Mapping,
        key: Option<serde_yaml::Value>,
        anchor: usize,
    },
}

#[derive(Default)]
struct ExpressionYamlReceiver {
    documents: Vec<serde_yaml::Value>,
    document: Option<serde_yaml::Value>,
    stack: Vec<YamlContainer>,
    anchors: std::collections::BTreeMap<usize, serde_yaml::Value>,
    error: Option<String>,
}

impl yaml_rust2::parser::MarkedEventReceiver for ExpressionYamlReceiver {
    fn on_event(&mut self, event: yaml_rust2::parser::Event, _marker: yaml_rust2::scanner::Marker) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) = self.process_event(event) {
            self.error = Some(error);
        }
    }
}

impl ExpressionYamlReceiver {
    fn process_event(
        &mut self,
        event: yaml_rust2::parser::Event,
    ) -> std::result::Result<(), String> {
        use yaml_rust2::parser::Event;

        match event {
            Event::Nothing | Event::StreamStart | Event::StreamEnd => {}
            Event::DocumentStart => {
                self.document = None;
                self.anchors.clear();
            }
            Event::DocumentEnd => {
                if !self.stack.is_empty() {
                    return Err("invalid YAML container nesting".to_owned());
                }
                self.documents
                    .push(self.document.take().unwrap_or(serde_yaml::Value::Null));
            }
            Event::SequenceStart(anchor, tag) => {
                reject_container_tag(tag)?;
                self.stack.push(YamlContainer::Sequence {
                    values: Vec::new(),
                    anchor,
                });
            }
            Event::SequenceEnd => {
                let Some(YamlContainer::Sequence { values, anchor }) = self.stack.pop() else {
                    return Err("unexpected YAML sequence end".to_owned());
                };
                self.insert(serde_yaml::Value::Sequence(values), anchor)?;
            }
            Event::MappingStart(anchor, tag) => {
                reject_container_tag(tag)?;
                self.stack.push(YamlContainer::Mapping {
                    values: serde_yaml::Mapping::new(),
                    key: None,
                    anchor,
                });
            }
            Event::MappingEnd => {
                let Some(YamlContainer::Mapping {
                    values,
                    key,
                    anchor,
                }) = self.stack.pop()
                else {
                    return Err("unexpected YAML mapping end".to_owned());
                };
                if key.is_some() {
                    return Err("YAML mapping key is missing a value".to_owned());
                }
                self.insert(serde_yaml::Value::Mapping(values), anchor)?;
            }
            Event::Scalar(value, style, anchor, tag) => {
                self.insert(parse_yaml_scalar(value, style, tag)?, anchor)?;
            }
            Event::Alias(anchor) => {
                let value = self
                    .anchors
                    .get(&anchor)
                    .cloned()
                    .ok_or_else(|| format!("unknown YAML anchor {anchor}"))?;
                self.insert(value, 0)?;
            }
        }
        Ok(())
    }

    fn insert(
        &mut self,
        value: serde_yaml::Value,
        anchor: usize,
    ) -> std::result::Result<(), String> {
        if anchor > 0 {
            self.anchors.insert(anchor, value.clone());
        }
        match self.stack.last_mut() {
            Some(YamlContainer::Sequence { values, .. }) => values.push(value),
            Some(YamlContainer::Mapping { values, key, .. }) => {
                if let Some(key) = key.take() {
                    if values.insert(key.clone(), value).is_some() {
                        return Err(format!("duplicated YAML mapping key {key:?}"));
                    }
                } else {
                    *key = Some(value);
                }
            }
            None if self.document.is_none() => self.document = Some(value),
            None => return Err("multiple YAML roots in one document".to_owned()),
        }
        Ok(())
    }
}

fn reject_container_tag(tag: Option<yaml_rust2::parser::Tag>) -> std::result::Result<(), String> {
    match tag {
        Some(tag) => Err(format!("unsupported YAML tag {}{}", tag.handle, tag.suffix)),
        None => Ok(()),
    }
}

fn parse_yaml_scalar(
    value: String,
    style: yaml_rust2::scanner::TScalarStyle,
    tag: Option<yaml_rust2::parser::Tag>,
) -> std::result::Result<serde_yaml::Value, String> {
    use serde_yaml::Value as YamlValue;
    use yaml_rust2::scanner::TScalarStyle;

    if let Some(tag) = tag {
        if matches!(tag.handle.as_str(), "" | "!" | "tag:yaml.org,2002:") && tag.suffix == "js" {
            return Ok(YamlValue::Mapping(
                [(
                    YamlValue::String(EXPRESSION_KEY.to_owned()),
                    YamlValue::String(value),
                )]
                .into_iter()
                .collect(),
            ));
        }
        if tag.handle != "tag:yaml.org,2002:" {
            return Err(format!("unsupported YAML tag {}{}", tag.handle, tag.suffix));
        }
        return parse_standard_yaml_scalar(&tag.suffix, value);
    }
    if style != TScalarStyle::Plain {
        return Ok(YamlValue::String(value));
    }
    serde_yaml::from_str(&value).map_err(|error| error.to_string())
}

fn parse_standard_yaml_scalar(
    tag: &str,
    value: String,
) -> std::result::Result<serde_yaml::Value, String> {
    use serde_yaml::Value as YamlValue;

    match tag {
        "str" => Ok(YamlValue::String(value)),
        "null" if matches!(value.as_str(), "~" | "null" | "Null" | "NULL") => Ok(YamlValue::Null),
        "bool" => serde_yaml::from_str::<bool>(&value)
            .map(YamlValue::Bool)
            .map_err(|error| error.to_string()),
        "int" => serde_yaml::from_str::<i64>(&value)
            .map(serde_yaml::Number::from)
            .map(YamlValue::Number)
            .map_err(|error| error.to_string()),
        "float" => serde_yaml::from_str::<f64>(&value)
            .and_then(serde_yaml::to_value)
            .map_err(|error| error.to_string()),
        "null" => Err(format!("invalid YAML null scalar {value:?}")),
        _ => Ok(YamlValue::String(value)),
    }
}

fn encode_yaml_expressions(value: serde_yaml::Value) -> serde_yaml::Value {
    use serde_yaml::{Value as YamlValue, value::TaggedValue};

    match value {
        YamlValue::Mapping(mut values)
            if values.len() == 1
                && values
                    .get(YamlValue::String(EXPRESSION_KEY.to_owned()))
                    .is_some_and(|value| matches!(value, YamlValue::String(_))) =>
        {
            let expression = values
                .remove(YamlValue::String(EXPRESSION_KEY.to_owned()))
                .expect("expression key was checked above");
            YamlValue::Tagged(Box::new(TaggedValue {
                tag: serde_yaml::value::Tag::new("tag:yaml.org,2002:js"),
                value: expression,
            }))
        }
        YamlValue::Sequence(values) => {
            YamlValue::Sequence(values.into_iter().map(encode_yaml_expressions).collect())
        }
        YamlValue::Mapping(values) => YamlValue::Mapping(
            values
                .into_iter()
                .map(|(key, value)| (encode_yaml_expressions(key), encode_yaml_expressions(value)))
                .collect(),
        ),
        value => value,
    }
}

fn validate_extension(path: &Path) -> Result<()> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("json" | "yaml" | "yml") => Ok(()),
        extension => Err(CordisError::InvalidConfig(format!(
            "extension `{:?}` not supported",
            extension
        ))),
    }
}

async fn write_atomic(path: &Path, config: &[EntryOptions]) -> Result<()> {
    let content = match path.extension().and_then(|value| value.to_str()) {
        Some("json") => serde_json::to_string_pretty(config)
            .map_err(|error| CordisError::InvalidConfig(error.to_string()))?,
        Some("yaml" | "yml") => {
            let value = serde_yaml::to_value(config)
                .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
            let content = serde_yaml::to_string(&encode_yaml_expressions(value))
                .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
            normalize_emitted_js_tags(content)
        }
        _ => {
            return Err(CordisError::InvalidConfig(
                "unsupported config extension".to_owned(),
            ));
        }
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(io_error)?;
    }
    static NEXT_TEMPORARY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nonce = NEXT_TEMPORARY.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let temporary = path.with_extension(format!(
        "{}.{}.{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("config"),
        std::process::id(),
        nonce,
    ));
    tokio::fs::write(&temporary, content)
        .await
        .map_err(io_error)?;
    if let Err(error) = replace_atomic(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(io_error(error));
    }
    Ok(())
}

fn normalize_emitted_js_tags(content: String) -> String {
    const EMITTED: &str = "!tag:yaml.org,2002:js";
    content
        .replace(&format!(": {EMITTED}"), ": !!js")
        .replace(&format!("- {EMITTED}"), "- !!js")
}

#[cfg(not(windows))]
async fn replace_atomic(source: &Path, destination: &Path) -> std::io::Result<()> {
    tokio::fs::rename(source, destination).await
}

#[cfg(windows)]
async fn replace_atomic(source: &Path, destination: &Path) -> std::io::Result<()> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    tokio::task::spawn_blocking(move || replace_file_windows(&source, &destination))
        .await
        .map_err(std::io::Error::other)?
}

#[cfg(windows)]
fn replace_file_windows(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::ReplaceFileW;

    if !destination.exists() {
        return std::fs::rename(source, destination);
    }
    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both UTF-16 buffers are NUL-terminated, remain alive for the call, and the
    // optional backup/exclusion/reserved pointers are null as permitted by ReplaceFileW.
    let replaced = unsafe {
        ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if replaced == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn io_error(error: std::io::Error) -> CordisError {
    CordisError::InvalidConfig(error.to_string())
}

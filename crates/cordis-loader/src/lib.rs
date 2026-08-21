//! Hierarchical, dynamically reloadable Cordis plugin configuration.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use cordis_core::{
    Context, CordisError, FiberHandle, FiberId, FiberState, IsolationKey, Plugin, Result,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

pub const ENTRY_SEPARATOR: char = ':';
pub const EXPRESSION_KEY: &str = "__jsExpr";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum IsolationRule {
    Local(bool),
    Global(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EntryOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub config: Value,
    #[serde(default, skip_serializing_if = "is_false")]
    pub group: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inject: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub intercept: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub isolate: BTreeMap<String, IsolationRule>,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntryUpdate {
    pub config: Option<Value>,
    pub group: Option<bool>,
    pub disabled: Option<bool>,
    pub inject: Option<Vec<String>>,
    pub intercept: Option<BTreeMap<String, Value>>,
    pub isolate: Option<BTreeMap<String, IsolationRule>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPosition {
    pub parent: Option<String>,
    pub position: usize,
}

impl EntryUpdate {
    fn apply_to(self, options: &mut EntryOptions) {
        if let Some(config) = self.config {
            options.config = config;
        }
        if let Some(group) = self.group {
            options.group = group;
        }
        if let Some(disabled) = self.disabled {
            options.disabled = disabled;
        }
        if let Some(inject) = self.inject {
            options.inject = inject;
        }
        if let Some(intercept) = self.intercept {
            options.intercept = intercept;
        }
        if let Some(isolate) = self.isolate {
            options.isolate = isolate;
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntrySnapshot {
    pub id: String,
    pub parent: Option<String>,
    pub options: EntryOptions,
    pub state: Option<FiberState>,
    pub children: Vec<String>,
}

#[async_trait]
pub trait PluginResolver: Send + Sync + 'static {
    async fn resolve(&self, name: &str) -> Result<Plugin>;
}

/// Host-language bridge for dynamic values in serialized loader configuration.
///
/// Upstream evaluates `__jsExpr` with JavaScript. Rust hosts supply an evaluator instead of
/// embedding a JavaScript runtime in the framework.
pub trait ConfigEvaluator: Send + Sync + 'static {
    fn evaluate(&self, context: &Context, expression: &str) -> Result<Value>;
}

impl<F> ConfigEvaluator for F
where
    F: Fn(&Context, &str) -> Result<Value> + Send + Sync + 'static,
{
    fn evaluate(&self, context: &Context, expression: &str) -> Result<Value> {
        self(context, expression)
    }
}

pub fn is_expression(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.contains_key(EXPRESSION_KEY))
}

fn contains_expression(value: &Value) -> bool {
    if is_expression(value) {
        return true;
    }
    match value {
        Value::Array(values) => values.iter().any(contains_expression),
        Value::Object(object) => object.values().any(contains_expression),
        _ => false,
    }
}

pub fn interpolate(
    context: &Context,
    value: Value,
    evaluator: &dyn ConfigEvaluator,
) -> Result<Value> {
    match value {
        Value::Object(mut object) if object.contains_key(EXPRESSION_KEY) => {
            let expression = object
                .remove(EXPRESSION_KEY)
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| {
                    CordisError::InvalidConfig(format!(
                        "`{EXPRESSION_KEY}` must contain a string expression"
                    ))
                })?;
            evaluator.evaluate(context, &expression)
        }
        Value::Array(values) => values
            .into_iter()
            .map(|value| interpolate(context, value, evaluator))
            .collect(),
        Value::Object(object) => object
            .into_iter()
            .map(|(key, value)| interpolate(context, value, evaluator).map(|value| (key, value)))
            .collect(),
        value => Ok(value),
    }
}

#[derive(Default)]
pub struct RegistryResolver {
    plugins: RwLock<HashMap<String, Plugin>>,
}
impl RegistryResolver {
    pub fn register(&self, name: impl Into<String>, plugin: Plugin) {
        self.plugins.write().insert(name.into(), plugin);
    }
    pub fn unregister(&self, name: &str) -> Option<Plugin> {
        self.plugins.write().remove(name)
    }

    pub fn get(&self, name: &str) -> Option<Plugin> {
        self.plugins.read().get(name).cloned()
    }
}
#[async_trait]
impl PluginResolver for RegistryResolver {
    async fn resolve(&self, name: &str) -> Result<Plugin> {
        self.plugins
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| CordisError::InvalidConfig(format!("cannot resolve plugin `{name}`")))
    }
}

struct EntryRecord {
    parent: Option<String>,
    options: EntryOptions,
    context: Context,
    local_realms: Arc<RwLock<HashMap<String, IsolationKey>>>,
    fiber: Option<FiberHandle>,
    children: Vec<String>,
}

type EntryMap = HashMap<String, Arc<Mutex<EntryRecord>>>;

#[derive(Clone)]
pub struct Loader {
    root: Context,
    resolver: Arc<dyn PluginResolver>,
    evaluator: Option<Arc<dyn ConfigEvaluator>>,
    entries: Arc<RwLock<EntryMap>>,
    roots: Arc<RwLock<Vec<String>>>,
    sequence: Arc<AtomicU64>,
    global_realms: Arc<RwLock<HashMap<(String, String), IsolationKey>>>,
    managed_fibers: Arc<RwLock<HashMap<FiberId, FiberHandle>>>,
}

/// A read-only weak view of the serialized loader tree.
///
/// This is intended for observers such as Include persistence hooks. It can snapshot the
/// configuration while the loader exists without extending the lifetime of the loader, its
/// fibers, or the root runtime.
#[derive(Clone)]
pub struct WeakLoader {
    entries: Weak<RwLock<EntryMap>>,
    roots: Weak<RwLock<Vec<String>>>,
}

type BoxTask<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

impl Loader {
    pub fn new(root: Context, resolver: Arc<dyn PluginResolver>) -> Self {
        Self {
            root,
            resolver,
            evaluator: None,
            entries: Arc::new(RwLock::new(HashMap::new())),
            roots: Arc::new(RwLock::new(Vec::new())),
            sequence: Arc::new(AtomicU64::new(0)),
            global_realms: Arc::new(RwLock::new(HashMap::new())),
            managed_fibers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_evaluator(mut self, evaluator: Arc<dyn ConfigEvaluator>) -> Self {
        self.evaluator = Some(evaluator);
        self
    }

    pub fn context(&self) -> Context {
        self.root.clone()
    }

    /// Returns a weak configuration view that does not keep this loader alive.
    pub fn downgrade(&self) -> WeakLoader {
        WeakLoader {
            entries: Arc::downgrade(&self.entries),
            roots: Arc::downgrade(&self.roots),
        }
    }

    fn resolve_config(&self, context: &Context, options: &EntryOptions) -> Result<Value> {
        if options.group || !contains_expression(&options.config) {
            return Ok(options.config.clone());
        }
        let evaluator = self.evaluator.as_deref().ok_or_else(|| {
            CordisError::InvalidConfig(
                "dynamic configuration requires a ConfigEvaluator".to_owned(),
            )
        })?;
        interpolate(context, options.config.clone(), evaluator)
    }

    fn waits_for_loader_idle(options: &EntryOptions) -> bool {
        options.inject.iter().any(|name| name == "loader")
            && options
                .intercept
                .get("loader")
                .and_then(Value::as_object)
                .and_then(|config| config.get("await"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }

    fn track_managed_fiber(&self, fiber: &FiberHandle) {
        self.managed_fibers
            .write()
            .insert(fiber.identity(), fiber.clone());
        let Some(mut states) = fiber.subscribe_state() else {
            return;
        };
        let entries = Arc::downgrade(&self.entries);
        let managed_fibers = Arc::downgrade(&self.managed_fibers);
        let identity = fiber.identity();
        tokio::spawn(async move {
            loop {
                if states.changed().await.is_err() {
                    break;
                }
                let state = *states.borrow_and_update();
                if matches!(
                    state,
                    FiberState::Active | FiberState::Failed | FiberState::Pending
                ) {
                    if let Some(entries) = entries.upgrade() {
                        Self::wake_idle_waiters_in(&entries).await;
                    }
                }
                if state == FiberState::Disposed {
                    break;
                }
            }
            if let Some(managed_fibers) = managed_fibers.upgrade() {
                managed_fibers.write().remove(&identity);
            }
        });
    }

    fn loader_is_idle(&self) -> bool {
        !self.managed_fibers.read().values().any(|fiber| {
            fiber.snapshot().map(|snapshot| snapshot.state) == Some(FiberState::Loading)
        })
    }

    async fn wake_idle_waiters(&self) -> bool {
        Self::wake_idle_waiters_in(&self.entries).await
    }

    async fn wake_idle_waiters_in(entries: &RwLock<EntryMap>) -> bool {
        let ids = entries.read().keys().cloned().collect::<Vec<_>>();
        let mut changed = false;
        for id in ids {
            let Some(record) = entries.read().get(&id).cloned() else {
                continue;
            };
            let Some(fiber) = record.lock().await.fiber.clone() else {
                continue;
            };
            if fiber.snapshot().map(|snapshot| snapshot.state) == Some(FiberState::Pending) {
                let _ = fiber.refresh().await;
                changed |=
                    fiber.snapshot().map(|snapshot| snapshot.state) != Some(FiberState::Pending);
            }
        }
        changed
    }

    fn observe_self_dispose(&self, id: &str, fiber: &FiberHandle) -> Result<()> {
        let record = self
            .entries
            .read()
            .get(id)
            .map(Arc::downgrade)
            .ok_or_else(|| CordisError::InvalidConfig(format!("cannot resolve entry `{id}`")))?;
        let context = self.root.clone();
        let fiber_id = fiber.identity();
        fiber.on_dispose(move || {
            let record = record.clone();
            let context = context.clone();
            async move {
                let mut changed = false;
                if let Some(record) = record.upgrade() {
                    let mut record = record.lock().await;
                    if record
                        .fiber
                        .as_ref()
                        .is_some_and(|current| current.identity() == fiber_id)
                    {
                        record.fiber = None;
                        record.options.disabled = true;
                        changed = true;
                    }
                }
                if changed {
                    Self::notify_config_update(&context).await;
                }
            }
        })
    }

    fn observe_self_update(&self, id: &str, fiber: &FiberHandle) -> Result<()> {
        let record = self
            .entries
            .read()
            .get(id)
            .map(Arc::downgrade)
            .ok_or_else(|| CordisError::InvalidConfig(format!("cannot resolve entry `{id}`")))?;
        let context = self.root.clone();
        let fiber_id = fiber.identity();
        fiber.on_update(move |config| {
            let record = record.clone();
            let context = context.clone();
            async move {
                let mut changed = false;
                if let Some(record) = record.upgrade() {
                    let mut record = record.lock().await;
                    if record
                        .fiber
                        .as_ref()
                        .is_some_and(|current| current.identity() == fiber_id)
                    {
                        record.options.config = config;
                        changed = true;
                    }
                }
                if changed {
                    Self::notify_config_update(&context).await;
                }
            }
        })
    }

    async fn notify_config_update(context: &Context) {
        if let Err(error) = context.emit("loader/config-update", vec![]).await {
            let _ = context
                .logger_named("loader")
                .error(error.to_string(), vec![]);
        }
    }

    /// Creates an entry and waits until its plugin reaches an active or failed terminal state.
    pub async fn create(
        &self,
        options: EntryOptions,
        parent: Option<&str>,
        position: usize,
    ) -> Result<String> {
        self.create_mode(options, parent.map(str::to_owned), position, true)
            .await
    }

    /// Creates an entry and returns after activation has started.
    ///
    /// This matches upstream loader creation semantics and makes `Pending`, `Loading`, and
    /// `Failed` fibers observable through [`Loader::fiber`] without blocking on dependencies or
    /// asynchronous plugin setup.
    pub async fn create_started(
        &self,
        options: EntryOptions,
        parent: Option<&str>,
        position: usize,
    ) -> Result<String> {
        self.create_mode(options, parent.map(str::to_owned), position, false)
            .await
    }

    fn create_mode(
        &self,
        mut options: EntryOptions,
        parent: Option<String>,
        position: usize,
        wait: bool,
    ) -> BoxTask<'_, String> {
        Box::pin(async move {
            let group_children = if options.group {
                Some(
                    serde_json::from_value::<Vec<EntryOptions>>(options.config.clone())
                        .map_err(|error| CordisError::InvalidConfig(error.to_string()))?,
                )
            } else {
                None
            };
            let local = options.id.clone().unwrap_or_else(|| {
                format!("{:08x}", self.sequence.fetch_add(1, Ordering::Relaxed) + 1)
            });
            options.id = Some(local.clone());
            let id = parent
                .as_ref()
                .map_or_else(|| local.clone(), |parent| format!("{parent}:{local}"));
            if self.entries.read().contains_key(&id) {
                return Err(CordisError::InvalidConfig(format!(
                    "duplicate entry `{id}`"
                )));
            }
            let inherited = match &parent {
                Some(parent) => self.record(parent).await?.context.clone(),
                None => self.root.clone(),
            };
            let local_realms = Arc::new(RwLock::new(HashMap::new()));
            let context = self.patch_context(inherited, &options, &local_realms);
            self.entries.write().insert(
                id.clone(),
                Arc::new(Mutex::new(EntryRecord {
                    parent: parent.clone(),
                    options: options.clone(),
                    context,
                    local_realms,
                    fiber: None,
                    children: vec![],
                })),
            );
            self.link(parent.as_deref(), id.clone(), position).await?;
            let refresh = if wait {
                self.refresh(&id).await
            } else {
                self.refresh_started(&id).await.map(drop)
            };
            if let Err(error) = refresh {
                self.remove(&id).await?;
                return Err(error);
            }
            if let Some(children) = group_children {
                for (index, child) in children.into_iter().enumerate() {
                    if let Err(error) = self.create_mode(child, Some(id.clone()), index, wait).await
                    {
                        if let Err(rollback) = self.remove(&id).await {
                            return Err(CordisError::Internal(format!(
                                "group creation failed ({error}); rollback also failed ({rollback})"
                            )));
                        }
                        return Err(error);
                    }
                }
            }
            if !wait {
                tokio::task::yield_now().await;
            }
            Ok(id)
        })
    }

    async fn record(&self, id: &str) -> Result<tokio::sync::OwnedMutexGuard<EntryRecord>> {
        let record =
            self.entries.read().get(id).cloned().ok_or_else(|| {
                CordisError::InvalidConfig(format!("cannot resolve entry `{id}`"))
            })?;
        Ok(record.lock_owned().await)
    }

    async fn link(&self, parent: Option<&str>, id: String, position: usize) -> Result<()> {
        if let Some(parent) = parent {
            let mut record = self.record(parent).await?;
            let at = position.min(record.children.len());
            record.children.insert(at, id);
        } else {
            let mut roots = self.roots.write();
            let at = position.min(roots.len());
            roots.insert(at, id);
        }
        Ok(())
    }

    async fn unlink(&self, parent: Option<&str>, id: &str) -> Result<()> {
        if let Some(parent) = parent {
            self.record(parent)
                .await?
                .children
                .retain(|child| child != id);
        } else {
            self.roots.write().retain(|entry| entry != id);
        }
        Ok(())
    }

    fn patch_context(
        &self,
        mut context: Context,
        options: &EntryOptions,
        local_realms: &RwLock<HashMap<String, IsolationKey>>,
    ) -> Context {
        for (name, value) in &options.intercept {
            context = context.intercept(name, value.clone());
        }
        for (name, rule) in &options.isolate {
            let key = match rule {
                IsolationRule::Local(false) => continue,
                IsolationRule::Local(true) => *local_realms
                    .write()
                    .entry(name.clone())
                    .or_insert_with(|| context.fresh_isolation_key()),
                IsolationRule::Global(label) => *self
                    .global_realms
                    .write()
                    .entry((label.clone(), name.clone()))
                    .or_insert_with(|| context.fresh_isolation_key()),
            };
            context = context.isolate_with(name, key);
        }
        context
    }

    async fn disabled(&self, id: &str) -> Result<bool> {
        let mut current = Some(id.to_owned());
        while let Some(id) = current {
            let record = self.record(&id).await?;
            if record.options.disabled {
                return Ok(true);
            }
            current = record.parent.clone();
        }
        Ok(false)
    }

    async fn refresh_started(&self, id: &str) -> Result<Option<FiberHandle>> {
        let disabled = self.disabled(id).await?;
        let (old, context, options) = {
            let mut record = self.record(id).await?;
            if disabled {
                (
                    record.fiber.take(),
                    record.context.clone(),
                    record.options.clone(),
                )
            } else if let Some(fiber) = &record.fiber {
                return Ok(Some(fiber.clone()));
            } else {
                (None, record.context.clone(), record.options.clone())
            }
        };
        if let Some(fiber) = old {
            fiber.dispose_managed().await?;
        }
        if disabled {
            return Ok(None);
        }
        let waits_for_loader = Self::waits_for_loader_idle(&options);
        let inject = options
            .inject
            .iter()
            .filter(|name| !(waits_for_loader && name.as_str() == "loader"))
            .cloned()
            .collect::<Vec<_>>();
        let plugin = self
            .resolver
            .resolve(&options.name)
            .await?
            .with_injects(inject);
        let plugin = if waits_for_loader {
            let loader = self.clone();
            plugin.with_readiness(move |_| loader.loader_is_idle())
        } else {
            plugin
        };
        let config = self.resolve_config(&context, &options)?;
        let fiber = context.start_plugin(plugin, config)?;
        self.observe_self_dispose(id, &fiber)?;
        self.observe_self_update(id, &fiber)?;
        self.record(id).await?.fiber = Some(fiber.clone());
        if !waits_for_loader {
            self.track_managed_fiber(&fiber);
        }
        Ok(Some(fiber))
    }

    async fn refresh(&self, id: &str) -> Result<()> {
        let Some(fiber) = self.refresh_started(id).await? else {
            return Ok(());
        };
        if let Err(error) = fiber.wait().await {
            fiber.dispose_managed().await?;
            let mut record = self.record(id).await?;
            if record
                .fiber
                .as_ref()
                .is_some_and(|current| current.identity() == fiber.identity())
            {
                record.fiber = None;
            }
            return Err(error);
        }
        Ok(())
    }

    async fn refresh_tree(&self, id: &str, restart: bool) -> Result<()> {
        let (fiber, children) = {
            let mut record = self.record(id).await?;
            (
                if restart { record.fiber.take() } else { None },
                record.children.clone(),
            )
        };
        if let Some(fiber) = fiber {
            fiber.dispose_managed().await?;
        }
        self.refresh(id).await?;
        for child in children {
            self.refresh_tree_owned(child, restart).await?;
        }
        Ok(())
    }
    fn refresh_tree_owned(&self, id: String, restart: bool) -> BoxTask<'_, ()> {
        Box::pin(async move { self.refresh_tree(&id, restart).await })
    }

    fn rebuild_context_tree(&self, id: String, inherited: Context) -> BoxTask<'_, ()> {
        Box::pin(async move {
            let (context, children) = {
                let mut record = self.record(&id).await?;
                let context = self.patch_context(inherited, &record.options, &record.local_realms);
                record.context = context.clone();
                (context, record.children.clone())
            };
            for child in children {
                self.rebuild_context_tree(child, context.clone()).await?;
            }
            Ok(())
        })
    }

    async fn contexts_equivalent(&self, left: &Context, right: &Context) -> bool {
        if left.base_url() != right.base_url() {
            return false;
        }
        let records = self.entries.read().values().cloned().collect::<Vec<_>>();
        let mut intercepts = std::collections::BTreeSet::new();
        let mut isolates = std::collections::BTreeSet::new();
        for record in records {
            let record = record.lock().await;
            intercepts.extend(record.options.intercept.keys().cloned());
            isolates.extend(record.options.isolate.keys().cloned());
        }
        intercepts
            .iter()
            .all(|name| left.intercept_chain(name) == right.intercept_chain(name))
            && isolates.iter().all(|name| left.same_isolation(right, name))
    }

    fn isolation_change_relevant(
        &self,
        id: String,
        inherited: Context,
        changed: Arc<BTreeSet<String>>,
        next_options: Option<EntryOptions>,
    ) -> BoxTask<'_, bool> {
        Box::pin(async move {
            let (previous, next, fiber, children) = {
                let record = self.record(&id).await?;
                let options = next_options.as_ref().unwrap_or(&record.options);
                (
                    record.context.clone(),
                    self.patch_context(inherited, options, &record.local_realms),
                    record.fiber.clone(),
                    record.children.clone(),
                )
            };
            if let Some(fiber) = fiber {
                let used = fiber
                    .dependencies()
                    .into_iter()
                    .chain(fiber.provided_services())
                    .collect::<BTreeSet<_>>();
                if changed
                    .iter()
                    .any(|name| used.contains(name) && !previous.same_isolation(&next, name))
                {
                    return Ok(true);
                }
            }
            for child in children {
                if self
                    .isolation_change_relevant(child, next.clone(), Arc::clone(&changed), None)
                    .await?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    fn update_entry(&self, id: String, mut options: EntryOptions) -> BoxTask<'_, ()> {
        Box::pin(async move {
            let (legacy, parent) = {
                let record = self.record(&id).await?;
                (record.options.clone(), record.parent.clone())
            };
            options.id = legacy.id.clone();
            if options == legacy {
                return Ok(());
            }

            let context_changed =
                legacy.intercept != options.intercept || legacy.isolate != options.isolate;
            let changed_isolations = legacy
                .isolate
                .keys()
                .chain(options.isolate.keys())
                .filter(|name| legacy.isolate.get(*name) != options.isolate.get(*name))
                .cloned()
                .collect::<BTreeSet<_>>();
            let context_restart_required = if legacy.intercept != options.intercept {
                true
            } else if changed_isolations.is_empty() {
                false
            } else {
                let inherited = match &parent {
                    Some(parent) => self.record(parent).await?.context.clone(),
                    None => self.root.clone(),
                };
                self.isolation_change_relevant(
                    id.clone(),
                    inherited,
                    Arc::new(changed_isolations),
                    Some(options.clone()),
                )
                .await?
            };
            let restart_self = legacy.name != options.name
                || legacy.inject != options.inject
                || legacy.group != options.group;
            let disabled_changed = legacy.disabled != options.disabled;
            let config_changed = legacy.config != options.config;
            let resolved_config =
                if config_changed && !context_changed && !restart_self && !disabled_changed {
                    let context = self.record(&id).await?.context.clone();
                    Some(self.resolve_config(&context, &options)?)
                } else {
                    None
                };
            let group_children = if options.group && (config_changed || !legacy.group) {
                Some(
                    serde_json::from_value::<Vec<EntryOptions>>(options.config.clone())
                        .map_err(|error| CordisError::InvalidConfig(error.to_string()))?,
                )
            } else {
                None
            };
            self.record(&id).await?.options = options.clone();
            self.prune_global_realms().await;

            if legacy.group && !options.group {
                let children = self.record(&id).await?.children.clone();
                for child in children {
                    self.remove(&child).await?;
                }
            }

            if context_changed {
                let inherited = match parent {
                    Some(parent) => self.record(&parent).await?.context.clone(),
                    None => self.root.clone(),
                };
                self.rebuild_context_tree(id.clone(), inherited).await?;
                if context_restart_required {
                    self.refresh_tree(&id, true).await?;
                }
            } else if restart_self {
                let fiber = self.record(&id).await?.fiber.take();
                if let Some(fiber) = fiber {
                    fiber.dispose_managed().await?;
                }
                self.refresh(&id).await?;
            } else if disabled_changed {
                self.refresh_tree(&id, false).await?;
            } else if config_changed {
                let fiber = self.record(&id).await?.fiber.clone();
                match fiber {
                    Some(fiber) => {
                        fiber
                            .update_managed(
                                resolved_config.expect("active config updates are resolved"),
                            )
                            .await?
                    }
                    None => self.refresh(&id).await?,
                }
            }

            if let Some(children) = group_children {
                self.reconcile_children(Some(id), children).await?;
            }
            Ok(())
        })
    }

    async fn prune_global_realms(&self) {
        let ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
        let mut referenced = BTreeSet::new();
        for id in ids {
            let Ok(record) = self.record(&id).await else {
                continue;
            };
            referenced.extend(record.options.isolate.iter().filter_map(|(name, rule)| {
                let IsolationRule::Global(label) = rule else {
                    return None;
                };
                Some((label.clone(), name.clone()))
            }));
        }
        self.global_realms
            .write()
            .retain(|realm, _| referenced.contains(realm));
    }

    fn reconcile_children(
        &self,
        parent: Option<String>,
        mut config: Vec<EntryOptions>,
    ) -> BoxTask<'_, ()> {
        Box::pin(async move {
            for options in &mut config {
                if options.id.is_none() {
                    options.id = Some(format!(
                        "{:08x}",
                        self.sequence.fetch_add(1, Ordering::Relaxed) + 1
                    ));
                }
            }
            let mut desired = Vec::with_capacity(config.len());
            for options in &config {
                let local = options.id.as_ref().expect("ids are assigned above");
                desired.push(
                    parent
                        .as_ref()
                        .map_or_else(|| local.clone(), |parent| format!("{parent}:{local}")),
                );
            }
            let mut unique = desired.clone();
            unique.sort();
            unique.dedup();
            if unique.len() != desired.len() {
                return Err(CordisError::InvalidConfig(
                    "duplicate entry id in configuration".to_owned(),
                ));
            }

            let current = match &parent {
                Some(parent) => self.record(parent).await?.children.clone(),
                None => self.roots.read().clone(),
            };
            for id in current {
                if !desired.contains(&id) {
                    self.remove(&id).await?;
                }
            }
            for (position, (id, options)) in desired.iter().zip(config).enumerate() {
                if self.entries.read().contains_key(id) {
                    self.update_entry(id.clone(), options).await?;
                } else {
                    self.create_mode(options, parent.clone(), position, true)
                        .await?;
                }
            }
            match parent {
                Some(parent) => self.record(&parent).await?.children = desired,
                None => *self.roots.write() = desired,
            }
            Ok(())
        })
    }

    pub async fn set_disabled(&self, id: &str, disabled: bool) -> Result<()> {
        self.record(id).await?.options.disabled = disabled;
        self.refresh_tree(id, false).await
    }

    pub async fn update_config(&self, id: &str, config: Value) -> Result<()> {
        let mut options = self.record(id).await?.options.clone();
        options.config = config;
        self.update_entry(id.to_owned(), options).await
    }

    /// Applies a typed partial update and optionally moves the entry in the same operation.
    pub async fn update(
        &self,
        id: &str,
        update: EntryUpdate,
        target: Option<EntryPosition>,
    ) -> Result<String> {
        let mut options = self.record(id).await?.options.clone();
        update.apply_to(&mut options);
        if options.group {
            serde_json::from_value::<Vec<EntryOptions>>(options.config.clone())
                .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
        }
        let id = match target {
            Some(target) => {
                self.move_entry(id, target.parent.as_deref(), target.position)
                    .await?
            }
            None => id.to_owned(),
        };
        self.update_entry(id.clone(), options).await?;
        Ok(id)
    }

    pub async fn move_entry(
        &self,
        id: &str,
        parent: Option<&str>,
        position: usize,
    ) -> Result<String> {
        if parent == Some(id) || parent.is_some_and(|value| value.starts_with(&format!("{id}:"))) {
            return Err(CordisError::InvalidConfig(
                "cannot move entry into its descendant".to_owned(),
            ));
        }
        let (old_parent, local, options, old_context, local_realms) = {
            let record = self.record(id).await?;
            (
                record.parent.clone(),
                record.options.id.clone().unwrap(),
                record.options.clone(),
                record.context.clone(),
                Arc::clone(&record.local_realms),
            )
        };
        let next = parent.map_or_else(|| local.clone(), |parent| format!("{parent}:{local}"));
        if next != id && self.entries.read().contains_key(&next) {
            return Err(CordisError::InvalidConfig(format!(
                "duplicate entry `{next}`"
            )));
        }
        let inherited = match parent {
            Some(parent) => self.record(parent).await?.context.clone(),
            None => self.root.clone(),
        };
        let next_context = self.patch_context(inherited.clone(), &options, &local_realms);
        let context_changed = !self.contexts_equivalent(&old_context, &next_context).await;

        self.unlink(old_parent.as_deref(), id).await?;
        {
            let mut record = self.record(id).await?;
            record.parent = parent.map(str::to_owned);
            record.context = next_context;
        }
        if next != id {
            let ids = self
                .entries
                .read()
                .keys()
                .filter(|candidate| {
                    candidate.as_str() == id
                        || candidate.starts_with(&format!("{id}{ENTRY_SEPARATOR}"))
                })
                .cloned()
                .collect::<Vec<_>>();
            let mapping = ids
                .iter()
                .map(|old| {
                    let renamed = format!("{}{}", next, old.strip_prefix(id).unwrap());
                    (old.clone(), renamed)
                })
                .collect::<HashMap<_, _>>();
            {
                let mut entries = self.entries.write();
                let records = ids
                    .iter()
                    .filter_map(|old| entries.remove(old).map(|record| (old.clone(), record)))
                    .collect::<Vec<_>>();
                for (old, record) in records {
                    entries.insert(mapping[&old].clone(), record);
                }
            }
            for (old, renamed) in &mapping {
                let mut record = self.record(renamed).await?;
                if old == id {
                    record.parent = parent.map(str::to_owned);
                } else if let Some(previous) = &record.parent {
                    if let Some(mapped) = mapping.get(previous) {
                        record.parent = Some(mapped.clone());
                    }
                }
                record.children = record
                    .children
                    .iter()
                    .map(|child| mapping.get(child).cloned().unwrap_or_else(|| child.clone()))
                    .collect();
            }
        }
        self.link(parent, next.clone(), position).await?;
        self.rebuild_context_tree(next.clone(), inherited).await?;
        let is_disabled = self.disabled(&next).await?;
        let fiber_active = self.record(&next).await?.fiber.is_some();
        if fiber_active == is_disabled {
            self.refresh_tree(&next, false).await?;
        } else if context_changed {
            self.refresh_tree(&next, true).await?;
        }
        Ok(next)
    }

    pub async fn remove(&self, id: &str) -> Result<()> {
        let (parent, children, fiber) = {
            let mut record = self.record(id).await?;
            (
                record.parent.clone(),
                record.children.clone(),
                record.fiber.take(),
            )
        };
        for child in children {
            self.remove_owned(child).await?;
        }
        if let Some(fiber) = fiber {
            fiber.dispose_managed().await?;
        }
        self.unlink(parent.as_deref(), id).await?;
        self.entries.write().remove(id);
        self.prune_global_realms().await;
        Ok(())
    }
    fn remove_owned(&self, id: String) -> BoxTask<'_, ()> {
        Box::pin(async move { self.remove(&id).await })
    }

    pub async fn snapshots(&self) -> Vec<EntrySnapshot> {
        let ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
        let mut output = Vec::new();
        for id in ids {
            if let Ok(record) = self.record(&id).await {
                output.push(EntrySnapshot {
                    id,
                    parent: record.parent.clone(),
                    options: record.options.clone(),
                    state: record
                        .fiber
                        .as_ref()
                        .and_then(FiberHandle::snapshot)
                        .map(|fiber| fiber.state),
                    children: record.children.clone(),
                });
            }
        }
        output.sort_by(|a, b| a.id.cmp(&b.id));
        output
    }

    /// Waits until all currently transitioning entries have settled.
    ///
    /// Failed fibers remain inspectable and permanently pending dependencies do not block this
    /// method, matching upstream `EntryTree.await()` and its `allSettled` loop.
    pub async fn wait(&self) {
        loop {
            let ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
            let mut transitioning = Vec::new();
            for id in ids {
                let Ok(Some(fiber)) = self.fiber(&id).await else {
                    continue;
                };
                if fiber.snapshot().is_some_and(|snapshot| {
                    matches!(snapshot.state, FiberState::Loading | FiberState::Unloading)
                }) {
                    transitioning.push(fiber);
                }
            }
            if transitioning.is_empty() {
                if self.wake_idle_waiters().await {
                    continue;
                }
                return;
            }
            for fiber in transitioning {
                let _ = fiber.wait().await;
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn fiber(&self, id: &str) -> Result<Option<FiberHandle>> {
        Ok(self.record(id).await?.fiber.clone())
    }

    /// Locates the loader entry that owns a fiber or any of its descendant plugin fibers.
    pub async fn locate(&self, fiber: &FiberHandle) -> Option<String> {
        let mut ancestors = BTreeSet::<FiberId>::new();
        let mut current = Some(fiber.clone());
        while let Some(fiber) = current {
            ancestors.insert(fiber.identity());
            current = fiber.parent();
        }
        let ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
        for id in ids {
            let Ok(record) = self.record(&id).await else {
                continue;
            };
            if record
                .fiber
                .as_ref()
                .is_some_and(|fiber| ancestors.contains(&fiber.identity()))
            {
                return Some(id);
            }
        }
        None
    }

    /// Replaces the root configuration and all descendant entries.
    ///
    /// Replacement is intentionally transactional at the parsed configuration boundary:
    /// callers validate and patch the complete new value before this method mutates fibers.
    pub async fn replace(&self, config: Vec<EntryOptions>) -> Result<()> {
        self.reconcile_children(None, config).await
    }

    pub async fn stop(&self) -> Result<()> {
        self.replace(Vec::new()).await
    }

    pub async fn root_config(&self) -> Result<Vec<EntryOptions>> {
        let roots = self.roots.read().clone();
        let mut config = Vec::with_capacity(roots.len());
        for id in roots {
            let mut options = self.record(&id).await?.options.clone();
            if options.group {
                options.config = serde_json::to_value(self.child_config(&id).await?)
                    .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
            }
            config.push(options);
        }
        Ok(config)
    }

    pub async fn entry_ids_by_name(&self, name: &str) -> Vec<String> {
        let ids = self.entries.read().keys().cloned().collect::<Vec<_>>();
        let mut matches = Vec::new();
        for id in ids {
            if self
                .record(&id)
                .await
                .is_ok_and(|record| record.options.name == name)
            {
                matches.push(id);
            }
        }
        matches.sort();
        matches
    }

    /// Restarts every entry resolved by the given module name.
    pub async fn reload_name(&self, name: &str) -> Result<usize> {
        let ids = self.entry_ids_by_name(name).await;
        for id in &ids {
            let fiber = self.record(id).await?.fiber.take();
            if let Some(fiber) = fiber {
                fiber.dispose_managed().await?;
            }
            self.refresh(id).await?;
        }
        Ok(ids.len())
    }

    /// Restarts every matching entry without awaiting plugin activation.
    ///
    /// This preserves upstream HMR semantics: module construction errors are returned before
    /// mutation, while an asynchronous plugin-apply error remains observable as a failed fiber
    /// that a later reload can replace.
    pub async fn reload_name_started(&self, name: &str) -> Result<usize> {
        let ids = self.entry_ids_by_name(name).await;
        for id in &ids {
            let fiber = self.record(id).await?.fiber.take();
            if let Some(fiber) = fiber {
                fiber.dispose_managed().await?;
            }
            self.refresh_started(id).await?;
        }
        tokio::task::yield_now().await;
        Ok(ids.len())
    }

    fn child_config<'a>(&'a self, id: &'a str) -> BoxTask<'a, Vec<EntryOptions>> {
        Box::pin(async move {
            let children = self.record(id).await?.children.clone();
            let mut config = Vec::with_capacity(children.len());
            for child in children {
                let mut options = self.record(&child).await?.options.clone();
                if options.group {
                    options.config = serde_json::to_value(self.child_config(&child).await?)
                        .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
                }
                config.push(options);
            }
            Ok(config)
        })
    }
}

impl WeakLoader {
    /// Snapshots the root configuration, or returns `None` after the loader has been dropped.
    pub async fn root_config(&self) -> Result<Option<Vec<EntryOptions>>> {
        let Some(entries) = self.entries.upgrade() else {
            return Ok(None);
        };
        let Some(roots) = self.roots.upgrade() else {
            return Ok(None);
        };
        let roots = roots.read().clone();
        let mut config = Vec::with_capacity(roots.len());
        for id in roots {
            let (mut options, _) = entry_config_parts(&entries, &id).await?;
            if options.group {
                options.config = serde_json::to_value(weak_child_config(&entries, &id).await?)
                    .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
            }
            config.push(options);
        }
        Ok(Some(config))
    }
}

async fn entry_config_parts(
    entries: &RwLock<EntryMap>,
    id: &str,
) -> Result<(EntryOptions, Vec<String>)> {
    let record = entries
        .read()
        .get(id)
        .cloned()
        .ok_or_else(|| CordisError::InvalidConfig(format!("cannot resolve entry `{id}`")))?;
    let record = record.lock().await;
    Ok((record.options.clone(), record.children.clone()))
}

fn weak_child_config<'a>(
    entries: &'a Arc<RwLock<EntryMap>>,
    id: &'a str,
) -> BoxTask<'a, Vec<EntryOptions>> {
    Box::pin(async move {
        let (_, children) = entry_config_parts(entries, id).await?;
        let mut config = Vec::with_capacity(children.len());
        for child in children {
            let (mut options, _) = entry_config_parts(entries, &child).await?;
            if options.group {
                options.config = serde_json::to_value(weak_child_config(entries, &child).await?)
                    .map_err(|error| CordisError::InvalidConfig(error.to_string()))?;
            }
            config.push(options);
        }
        Ok(config)
    })
}

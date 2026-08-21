use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{Arc, Weak, atomic::Ordering},
};

use parking_lot::{Mutex as ParkingMutex, RwLock};
use serde_json::Value;

use crate::{
    AggregateError, CordisError, DispatchTarget, Disposer, EffectHandle, EventArgs, EventCallback,
    EventOptions, EventOutcome, FiberHandle, FiberId, FiberState, Next, Plugin, PluginId, Result,
    ServiceGuard, ServiceRef, SharedValue,
    effect::{EffectScope, infallible_disposer},
    events::EventsRegistry,
    fiber::{FiberInner, dispose_effects},
    logger::LoggerServiceInner,
    reflect::AccessorEntry,
    service::ServiceEntry,
};

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IsolationKey(u64);

type ServiceKey = (IsolationKey, String);

pub(crate) struct Runtime {
    next_isolation: std::sync::atomic::AtomicU64,
    next_fiber: std::sync::atomic::AtomicU64,
    next_service: std::sync::atomic::AtomicU64,
    root_isolations: RwLock<HashMap<String, IsolationKey>>,
    pub(crate) fibers: RwLock<HashMap<FiberId, Arc<FiberInner>>>,
    plugin_fibers: RwLock<HashMap<PluginId, Vec<FiberId>>>,
    pub(crate) services: RwLock<HashMap<ServiceKey, ServiceEntry>>,
    pub(crate) next_accessor: std::sync::atomic::AtomicU64,
    pub(crate) accessors: RwLock<HashMap<String, AccessorEntry>>,
    events: EventsRegistry,
    logger: Arc<LoggerServiceInner>,
    pending_updates: ParkingMutex<HashMap<FiberId, Arc<tokio::sync::Notify>>>,
}

impl Runtime {
    fn new() -> Self {
        Self {
            next_isolation: std::sync::atomic::AtomicU64::new(0),
            next_fiber: std::sync::atomic::AtomicU64::new(0),
            next_service: std::sync::atomic::AtomicU64::new(0),
            root_isolations: RwLock::new(HashMap::new()),
            fibers: RwLock::new(HashMap::new()),
            plugin_fibers: RwLock::new(HashMap::new()),
            services: RwLock::new(HashMap::new()),
            next_accessor: std::sync::atomic::AtomicU64::new(0),
            accessors: RwLock::new(HashMap::new()),
            events: EventsRegistry::default(),
            logger: Arc::new(LoggerServiceInner::new()),
            pending_updates: ParkingMutex::new(HashMap::new()),
        }
    }

    fn next_isolation(&self) -> IsolationKey {
        IsolationKey(self.next_isolation.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

struct PendingUpdateGuard {
    runtime: Arc<Runtime>,
    fiber: FiberId,
    completion: Arc<tokio::sync::Notify>,
    completed: bool,
}

impl PendingUpdateGuard {
    fn new(runtime: Arc<Runtime>, fiber: FiberId) -> Self {
        let completion = Arc::new(tokio::sync::Notify::new());
        runtime
            .pending_updates
            .lock()
            .insert(fiber, Arc::clone(&completion));
        Self {
            runtime,
            fiber,
            completion,
            completed: false,
        }
    }

    fn remove(&self) {
        let mut pending = self.runtime.pending_updates.lock();
        if pending
            .get(&self.fiber)
            .is_some_and(|current| Arc::ptr_eq(current, &self.completion))
        {
            pending.remove(&self.fiber);
        }
    }

    fn complete(&mut self) {
        self.remove();
        self.completion.notify_waiters();
        self.completed = true;
    }
}

impl Drop for PendingUpdateGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.remove();
            self.completion.notify_waiters();
        }
    }
}

/// A cloneable view into a Cordis runtime and one of its fibers.
#[derive(Clone)]
pub struct Context {
    runtime: Weak<Runtime>,
    owner: Option<Arc<Runtime>>,
    fiber_id: FiberId,
    isolation: Arc<HashMap<String, IsolationKey>>,
    intercepts: Arc<BTreeMap<String, Value>>,
    intercept_chains: Arc<BTreeMap<String, Vec<Value>>>,
    base_url: Option<Arc<str>>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Context")
            .field("fiber", &self.fiber().snapshot())
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    pub fn new() -> Self {
        let runtime = Arc::new(Runtime::new());
        let context = Self {
            runtime: Arc::downgrade(&runtime),
            owner: Some(Arc::clone(&runtime)),
            fiber_id: FiberId(0),
            isolation: Arc::new(HashMap::new()),
            intercepts: Arc::new(BTreeMap::new()),
            intercept_chains: Arc::new(BTreeMap::new()),
            base_url: None,
        };
        runtime
            .fibers
            .write()
            .insert(FiberId(0), Arc::new(FiberInner::root(context.detached())));
        context
    }

    fn runtime(&self) -> Result<Arc<Runtime>> {
        self.owner
            .clone()
            .or_else(|| self.runtime.upgrade())
            .ok_or_else(|| CordisError::Internal("runtime has been dropped".to_owned()))
    }

    pub(crate) fn runtime_arc(&self) -> Result<Arc<Runtime>> {
        self.runtime()
    }

    pub(crate) fn detached(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            owner: None,
            fiber_id: self.fiber_id,
            isolation: Arc::clone(&self.isolation),
            intercepts: Arc::clone(&self.intercepts),
            intercept_chains: Arc::clone(&self.intercept_chains),
            base_url: self.base_url.clone(),
        }
    }

    pub(crate) fn attached_from(&self, context: &Self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            owner: self.owner.clone().or_else(|| self.runtime.upgrade()),
            fiber_id: context.fiber_id,
            isolation: Arc::clone(&context.isolation),
            intercepts: Arc::clone(&context.intercepts),
            intercept_chains: Arc::clone(&context.intercept_chains),
            base_url: context.base_url.clone(),
        }
    }

    pub fn fiber(&self) -> FiberHandle {
        FiberHandle {
            runtime: self.clone(),
            id: self.fiber_id,
        }
    }

    /// Returns the root context for this runtime.
    pub fn root(&self) -> Self {
        let root = self
            .fiber_inner(FiberId(0))
            .expect("an attached context has a root fiber");
        self.attached_from(&root.context)
    }

    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    pub fn with_base_url(&self, base_url: impl Into<Arc<str>>) -> Self {
        let mut context = self.clone();
        context.base_url = Some(base_url.into());
        context
    }

    pub fn intercept(&self, name: impl Into<String>, value: Value) -> Self {
        let name = name.into();
        let mut intercepts = (*self.intercepts).clone();
        intercepts.insert(name.clone(), value.clone());
        let mut intercept_chains = (*self.intercept_chains).clone();
        intercept_chains.entry(name).or_default().push(value);
        let mut context = self.clone();
        context.intercepts = Arc::new(intercepts);
        context.intercept_chains = Arc::new(intercept_chains);
        context
    }

    pub fn intercept_value(&self, name: &str) -> Option<&Value> {
        self.intercepts.get(name)
    }

    /// Returns intercept layers from the outermost context to the innermost override.
    pub fn intercept_chain(&self, name: &str) -> Vec<Value> {
        self.intercept_chains.get(name).cloned().unwrap_or_default()
    }

    pub fn isolate(&self, name: impl Into<String>) -> Self {
        let runtime = self
            .runtime()
            .expect("an attached context has a live runtime");
        self.isolate_with(name, runtime.next_isolation())
    }

    pub fn isolate_with(&self, name: impl Into<String>, key: IsolationKey) -> Self {
        let mut isolation = (*self.isolation).clone();
        isolation.insert(name.into(), key);
        let mut context = self.clone();
        context.isolation = Arc::new(isolation);
        context
    }

    pub fn fresh_isolation_key(&self) -> IsolationKey {
        self.runtime()
            .expect("an attached context has a live runtime")
            .next_isolation()
    }

    pub fn isolation_key(&self, name: &str) -> IsolationKey {
        if let Some(key) = self.isolation.get(name) {
            return *key;
        }
        let runtime = self.runtime().expect("a live context has a runtime");
        let mut keys = runtime.root_isolations.write();
        *keys
            .entry(name.to_owned())
            .or_insert_with(|| runtime.next_isolation())
    }

    pub fn same_isolation(&self, other: &Self, name: &str) -> bool {
        self.isolation_key(name) == other.isolation_key(name)
    }

    pub fn service_target<T>(&self, name: impl Into<String>, value: T) -> DispatchTarget
    where
        T: Any + Send + Sync,
    {
        let name = name.into();
        let key = self.isolation_key(&name);
        DispatchTarget::new(Some(Arc::new(value)), move |context| {
            context.isolation_key(&name) == key
        })
    }

    fn assert_active(&self) -> Result<Arc<FiberInner>> {
        let fiber = self
            .fiber_inner(self.fiber_id)
            .ok_or(CordisError::InactiveEffect)?;
        if fiber.is_disposed() {
            Err(CordisError::InactiveEffect)
        } else {
            Ok(fiber)
        }
    }

    pub(crate) fn fiber_inner(&self, id: FiberId) -> Option<Arc<FiberInner>> {
        self.runtime().ok()?.fibers.read().get(&id).cloned()
    }

    pub(crate) fn register_effect(&self, effect: EffectHandle) -> Result<EffectHandle> {
        self.assert_active()?.register_effect(effect.clone())?;
        Ok(effect)
    }

    pub fn logger(&self) -> crate::Logger {
        crate::LoggerService::new(self.clone()).logger(None)
    }

    pub fn logger_named(&self, name: impl Into<String>) -> crate::Logger {
        crate::LoggerService::new(self.clone()).logger(Some(name.into()))
    }

    pub fn logger_service(&self) -> crate::LoggerService {
        crate::LoggerService::new(self.clone())
    }

    pub(crate) fn logger_inner(&self) -> Result<Arc<LoggerServiceInner>> {
        Ok(Arc::clone(&self.runtime()?.logger))
    }

    pub async fn effect<F, Fut>(&self, label: impl Into<String>, setup: F) -> Result<EffectHandle>
    where
        F: FnOnce(EffectScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<Disposer>>> + Send + 'static,
    {
        let context = self.clone();
        let label = label.into();
        tokio::spawn(async move { context.run_effect(label, setup).await })
            .await
            .map_err(|error| CordisError::Internal(format!("effect setup task failed: {error}")))?
    }

    async fn run_effect<F, Fut>(&self, label: String, setup: F) -> Result<EffectHandle>
    where
        F: FnOnce(EffectScope) -> Fut,
        Fut: Future<Output = Result<Option<Disposer>>>,
    {
        self.assert_active()?;
        let scope = EffectScope::default();
        let disposer = match setup(scope.clone()).await {
            Ok(disposer) => disposer,
            Err(error) => {
                let _ = scope.rollback().await;
                return Err(error);
            }
        };
        if let Some(disposer) = disposer {
            scope.defer(disposer);
        }
        let effect = EffectHandle::collection(label, scope.finish());
        if let Err(error) = self.register_effect(effect.clone()) {
            let _ = effect.dispose().await;
            return Err(error);
        }
        Ok(effect)
    }

    pub fn on(
        &self,
        name: impl Into<String>,
        callback: EventCallback,
        options: EventOptions,
    ) -> Result<EffectHandle> {
        self.assert_active()?;
        let name = name.into();
        let runtime = self.runtime()?;
        let id = runtime
            .events
            .register(self.detached(), name.clone(), callback, options);
        let weak = Arc::downgrade(&runtime);
        let dispose_name = name.clone();
        let disposer = infallible_disposer(move || {
            if let Some(runtime) = weak.upgrade() {
                runtime.events.unregister(&dispose_name, id);
            }
        });
        self.register_effect(EffectHandle::collection(
            format!("ctx.on({name:?})"),
            vec![EffectHandle::disposer(disposer)],
        ))
    }

    pub fn once(
        &self,
        name: impl Into<String>,
        callback: EventCallback,
        options: EventOptions,
    ) -> Result<EffectHandle> {
        let slot = Arc::new(parking_lot::Mutex::new(None::<EffectHandle>));
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_slot = Arc::clone(&slot);
        let wrapped: EventCallback = Arc::new(move |invocation| {
            let callback = Arc::clone(&callback);
            let effect = callback_slot.lock().clone();
            let fired = Arc::clone(&fired);
            Box::pin(async move {
                if fired.swap(true, Ordering::AcqRel) {
                    return Ok(EventOutcome::Continue);
                }
                if let Some(effect) = effect {
                    effect.dispose().await?;
                }
                callback(invocation).await
            })
        });
        let effect = self.on(name, wrapped, options)?;
        *slot.lock() = Some(effect.clone());
        Ok(effect)
    }

    pub async fn emit(&self, name: &str, args: EventArgs) -> Result<()> {
        self.emit_to(None, name, args).await
    }

    pub async fn emit_to(
        &self,
        target: Option<DispatchTarget>,
        name: &str,
        args: EventArgs,
    ) -> Result<()> {
        self.runtime()?.events.emit(name, args, target).await
    }

    pub async fn parallel(
        &self,
        name: &str,
        args: EventArgs,
    ) -> std::result::Result<(), AggregateError> {
        self.parallel_to(None, name, args).await
    }

    pub async fn parallel_to(
        &self,
        target: Option<DispatchTarget>,
        name: &str,
        args: EventArgs,
    ) -> std::result::Result<(), AggregateError> {
        match self.runtime() {
            Ok(runtime) => runtime.events.parallel(name, args, target).await,
            Err(error) => Err(AggregateError::new(vec![error])),
        }
    }

    pub async fn serial(&self, name: &str, args: EventArgs) -> Result<EventOutcome> {
        self.serial_to(None, name, args).await
    }

    pub async fn serial_to(
        &self,
        target: Option<DispatchTarget>,
        name: &str,
        args: EventArgs,
    ) -> Result<EventOutcome> {
        self.runtime()?.events.serial(name, args, target).await
    }

    pub async fn bail(&self, name: &str, args: EventArgs) -> Result<EventOutcome> {
        self.serial(name, args).await
    }

    pub async fn waterfall(
        &self,
        name: &str,
        args: EventArgs,
        terminal: Next,
    ) -> Result<EventOutcome> {
        self.runtime()?
            .events
            .waterfall(name, args, None, terminal)
            .await
    }

    fn create_plugin_fiber(&self, plugin: Plugin, config: Value) -> Result<FiberHandle> {
        self.assert_active()?;
        let config = plugin.validate(config)?;
        let runtime = self.runtime()?;
        let id = FiberId(runtime.next_fiber.fetch_add(1, Ordering::Relaxed) + 1);
        let mut intercepts = (*self.intercepts).clone();
        let mut intercept_chains = (*self.intercept_chains).clone();
        for (name, value) in &plugin.inner.inject {
            if let Some(value) = value {
                intercepts.insert(name.clone(), value.clone());
                intercept_chains
                    .entry(name.clone())
                    .or_default()
                    .push(value.clone());
            }
        }
        let context = Self {
            runtime: Arc::downgrade(&runtime),
            owner: None,
            fiber_id: id,
            isolation: Arc::clone(&self.isolation),
            intercepts: Arc::new(intercepts),
            intercept_chains: Arc::new(intercept_chains),
            base_url: self.base_url.clone(),
        };
        let fiber = Arc::new(FiberInner::plugin(
            id,
            self.fiber_id,
            context,
            plugin.clone(),
            config,
        ));
        runtime.fibers.write().insert(id, fiber);
        runtime
            .plugin_fibers
            .write()
            .entry(plugin.id())
            .or_default()
            .push(id);

        let weak = Arc::downgrade(&runtime);
        let disposer: Disposer = Arc::new(move || {
            let weak = weak.clone();
            Box::pin(async move {
                let Some(runtime) = weak.upgrade() else {
                    return Ok(());
                };
                let context = Context::runtime_context(runtime);
                context.dispose_fiber(id).await
            })
        });
        if let Err(error) = self.register_effect(EffectHandle::collection(
            "ctx.plugin()",
            vec![EffectHandle::disposer(disposer)],
        )) {
            runtime.fibers.write().remove(&id);
            let mut registry = runtime.plugin_fibers.write();
            if let Some(ids) = registry.get_mut(&plugin.id()) {
                ids.retain(|fiber_id| *fiber_id != id);
                if ids.is_empty() {
                    registry.remove(&plugin.id());
                }
            }
            return Err(error);
        }

        Ok(FiberHandle {
            runtime: self.clone(),
            id,
        })
    }

    /// Starts a plugin and returns its fiber before asynchronous activation settles.
    pub fn start_plugin(&self, plugin: Plugin, config: Value) -> Result<FiberHandle> {
        let handle = self.create_plugin_fiber(plugin, config)?;
        let context = self.clone();
        let id = handle.id;
        tokio::spawn(async move {
            let _ = context.refresh_fiber(id).await;
        });
        Ok(handle)
    }

    pub async fn plugin(&self, plugin: Plugin, config: Value) -> Result<FiberHandle> {
        let handle = self.start_plugin(plugin, config)?;
        let id = handle.id;
        if let Err(error) = self.wait_fiber(id).await {
            let _ = self.dispose_fiber(id).await;
            return Err(error);
        }
        Ok(handle)
    }

    pub async fn inject<F, Fut>(
        &self,
        dependencies: impl IntoIterator<Item = impl Into<String>>,
        callback: F,
    ) -> Result<FiberHandle>
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Disposer>>> + Send + 'static,
    {
        let mut builder = Plugin::builder("inject", move |context, _| callback(context));
        for dependency in dependencies {
            builder = builder.inject(dependency);
        }
        self.plugin(builder.build(), Value::Null).await
    }

    pub fn registry_size(&self) -> usize {
        self.runtime()
            .map(|runtime| runtime.plugin_fibers.read().len())
            .unwrap_or_default()
    }

    pub fn has_plugin(&self, plugin: &Plugin) -> bool {
        self.runtime().is_ok_and(|runtime| {
            runtime
                .plugin_fibers
                .read()
                .get(&plugin.id())
                .is_some_and(|fibers| !fibers.is_empty())
        })
    }

    pub fn plugin_fibers(&self, plugin: &Plugin) -> Vec<FiberHandle> {
        let Ok(runtime) = self.runtime() else {
            return Vec::new();
        };
        runtime
            .plugin_fibers
            .read()
            .get(&plugin.id())
            .into_iter()
            .flatten()
            .map(|id| FiberHandle {
                runtime: self.clone(),
                id: *id,
            })
            .collect()
    }

    pub fn plugin_ids(&self) -> Vec<crate::PluginId> {
        let Ok(runtime) = self.runtime() else {
            return Vec::new();
        };
        let mut ids = runtime
            .plugin_fibers
            .read()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub async fn delete_plugin(&self, plugin: &Plugin) -> Result<bool> {
        let runtime = self.runtime()?;
        let ids = runtime
            .plugin_fibers
            .read()
            .get(&plugin.id())
            .cloned()
            .unwrap_or_default();
        if ids.is_empty() {
            return Ok(false);
        }
        for id in ids {
            self.dispose_fiber(id).await?;
        }
        Ok(true)
    }

    pub async fn provide<T>(&self, name: impl Into<String>, value: T) -> Result<ServiceGuard>
    where
        T: Any + Send + Sync,
    {
        self.provide_shared(name, Arc::new(value)).await
    }

    pub async fn provide_shared(
        &self,
        name: impl Into<String>,
        value: SharedValue,
    ) -> Result<ServiceGuard> {
        let fiber = self.assert_active()?;
        let runtime = self.runtime()?;
        let name = name.into();
        let isolation = self.isolation_key(&name);
        let key = (isolation, name.clone());
        let service_id = runtime.next_service.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut services = runtime.services.write();
            if let Some(existing) = services.get(&key) {
                return Err(CordisError::ServiceAlreadyRegistered {
                    name,
                    fiber_id: existing.owner.0,
                });
            }
            services.insert(
                key.clone(),
                ServiceEntry {
                    id: service_id,
                    owner: self.fiber_id,
                    value,
                },
            );
        }

        let weak = Arc::downgrade(&runtime);
        let dispose_key = key.clone();
        let dispose_name = name.clone();
        let owner = self.fiber_id;
        let disposer: Disposer = Arc::new(move || {
            let weak = weak.clone();
            let key = dispose_key.clone();
            let name = dispose_name.clone();
            Box::pin(async move {
                let Some(runtime) = weak.upgrade() else {
                    return Ok(());
                };
                let removed = {
                    let mut services = runtime.services.write();
                    if services.get(&key).map(|entry| entry.id) == Some(service_id) {
                        services.remove(&key);
                        true
                    } else {
                        false
                    }
                };
                if removed {
                    Context::runtime_context(Arc::clone(&runtime))
                        .refresh_dependents(&name, key.0, Some(owner))
                        .await?;
                }
                Ok(())
            })
        });
        let effect = EffectHandle::collection(
            format!("ctx.provide({name:?})"),
            vec![EffectHandle::disposer(disposer)],
        );
        if let Err(error) = fiber.register_effect(effect.clone()) {
            runtime.services.write().remove(&key);
            return Err(error);
        }
        if *fiber.state.lock() == FiberState::Active {
            self.refresh_dependents(&name, isolation, Some(self.fiber_id))
                .await?;
        }
        Ok(ServiceGuard::new(self.clone(), name, isolation, effect))
    }

    pub fn get<T>(&self, name: &str) -> Option<ServiceRef<T>>
    where
        T: Any + Send + Sync,
    {
        let value = self.get_shared(name)?;
        Arc::downcast::<T>(value).ok().map(ServiceRef::new)
    }

    pub fn get_shared(&self, name: &str) -> Option<SharedValue> {
        self.visible_service_entry(name)
            .map(|entry| Arc::clone(&entry.value))
    }

    pub(crate) fn visible_service_entry(&self, name: &str) -> Option<ServiceEntry> {
        let runtime = self.runtime().ok()?;
        let key = (self.isolation_key(name), name.to_owned());
        let entry = runtime.services.read().get(&key).cloned()?;
        if entry.owner == self.fiber_id {
            return Some(entry);
        }
        let owner = runtime.fibers.read().get(&entry.owner).cloned()?;
        (*owner.state.lock() == FiberState::Active).then_some(entry)
    }

    fn runtime_context(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime: Arc::downgrade(&runtime),
            owner: Some(runtime),
            fiber_id: FiberId(0),
            isolation: Arc::new(HashMap::new()),
            intercepts: Arc::new(BTreeMap::new()),
            intercept_chains: Arc::new(BTreeMap::new()),
            base_url: None,
        }
    }

    async fn refresh_dependents(
        &self,
        name: &str,
        isolation: IsolationKey,
        skip: Option<FiberId>,
    ) -> Result<()> {
        let runtime = self.runtime()?;
        let fibers = runtime.fibers.read().values().cloned().collect::<Vec<_>>();
        for fiber in fibers {
            if Some(fiber.id) == skip || fiber.is_disposed() {
                continue;
            }
            let Some(plugin) = &fiber.plugin else {
                continue;
            };
            if !plugin.inner.inject.contains_key(name)
                || fiber.context.isolation_key(name) != isolation
            {
                continue;
            }
            Box::pin(self.refresh_fiber(fiber.id)).await?;
        }
        Ok(())
    }

    fn dependencies_ready(&self, fiber: &FiberInner) -> bool {
        let Some(plugin) = &fiber.plugin else {
            return true;
        };
        if plugin
            .inner
            .readiness
            .as_ref()
            .is_some_and(|readiness| !readiness(&fiber.context))
        {
            return false;
        }
        let Ok(runtime) = self.runtime() else {
            return false;
        };
        let pending = runtime.pending_updates.lock();
        plugin.inner.inject.keys().all(|name| {
            let key = (fiber.context.isolation_key(name), name.clone());
            let Some(entry) = runtime.services.read().get(&key).cloned() else {
                return false;
            };
            if entry.owner != fiber.id && pending.contains_key(&entry.owner) {
                return false;
            }
            fiber.context.get_shared(name).is_some()
        })
    }

    pub(crate) async fn refresh_fiber(&self, id: FiberId) -> Result<()> {
        let Some(fiber) = self.fiber_inner(id) else {
            return Ok(());
        };
        if !fiber.started.swap(true, Ordering::AcqRel) {
            fiber.started_notify.notify_one();
        }
        let guard = fiber.lifecycle.lock().await;
        if fiber.is_disposed() {
            return Ok(());
        }
        let ready = self.dependencies_ready(&fiber);
        let state = *fiber.state.lock();
        let mut became_active = false;
        if ready {
            if matches!(
                state,
                FiberState::Active | FiberState::Loading | FiberState::Failed
            ) {
                return Ok(());
            }
            fiber.set_state(FiberState::Loading);
            let plugin = fiber
                .plugin
                .as_ref()
                .expect("non-root fibers always have a plugin")
                .clone();
            let config = fiber.config.lock().clone();
            match (plugin.inner.callback)(fiber.context.clone(), config).await {
                Ok(disposer) => {
                    if let Some(disposer) = disposer {
                        fiber.register_effect(EffectHandle::disposer(disposer))?;
                    }
                    *fiber.error.lock() = None;
                    fiber.set_state(FiberState::Active);
                    became_active = true;
                }
                Err(error) => {
                    dispose_effects(&fiber).await;
                    let _ = self
                        .attached_from(&fiber.context)
                        .logger()
                        .error(crate::LogValue::Error(error.to_string()), vec![]);
                    *fiber.error.lock() = Some(error.clone());
                    fiber.set_state(FiberState::Failed);
                }
            }
        } else if matches!(state, FiberState::Active | FiberState::Loading) {
            fiber.set_state(FiberState::Unloading);
            let errors = dispose_effects(&fiber).await;
            fiber.set_state(FiberState::Pending);
            self.log_dispose_errors(&fiber, errors);
        }
        drop(guard);
        if became_active {
            self.publish_services(&fiber).await?;
        }
        Ok(())
    }

    async fn publish_services(&self, fiber: &FiberInner) -> Result<()> {
        let runtime = self.runtime()?;
        let services = runtime
            .services
            .read()
            .iter()
            .filter(|(_, entry)| entry.owner == fiber.id)
            .map(|((isolation, name), _)| (name.clone(), *isolation))
            .collect::<Vec<_>>();
        for (name, isolation) in services {
            self.refresh_dependents(&name, isolation, Some(fiber.id))
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn wait_fiber(&self, id: FiberId) -> Result<()> {
        let Some(fiber) = self.fiber_inner(id) else {
            return Ok(());
        };
        while !fiber.started.load(Ordering::Acquire) {
            fiber.started_notify.notified().await;
        }
        let _guard = fiber.lifecycle.lock().await;
        let error = fiber.error.lock().clone();
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn dispose_fiber(&self, id: FiberId) -> Result<()> {
        let Some(fiber) = self.fiber_inner(id) else {
            return Ok(());
        };
        if id == FiberId(0) {
            let _guard = fiber.lifecycle.lock().await;
            let errors = dispose_effects(&fiber).await;
            self.log_dispose_errors(&fiber, errors);
            return Ok(());
        }
        if fiber.disposed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let _guard = fiber.lifecycle.lock().await;
        fiber.set_state(FiberState::Unloading);
        let errors = dispose_effects(&fiber).await;
        self.log_dispose_errors(&fiber, errors);
        fiber.set_state(FiberState::Disposed);
        if let Some(plugin) = &fiber.plugin {
            let runtime = self.runtime()?;
            let mut registry = runtime.plugin_fibers.write();
            if let Some(ids) = registry.get_mut(&plugin.id()) {
                ids.retain(|fiber_id| *fiber_id != id);
                if ids.is_empty() {
                    registry.remove(&plugin.id());
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn restart_fiber(&self, id: FiberId) -> Result<()> {
        let Some(fiber) = self.fiber_inner(id) else {
            return Ok(());
        };
        if fiber.is_disposed() {
            return Err(CordisError::InactiveEffect);
        }
        {
            let _guard = fiber.lifecycle.lock().await;
            fiber.set_state(FiberState::Unloading);
            let errors = dispose_effects(&fiber).await;
            self.log_dispose_errors(&fiber, errors);
            fiber.set_state(FiberState::Pending);
            *fiber.error.lock() = None;
        }
        self.refresh_fiber(id).await
    }

    pub(crate) async fn update_fiber(&self, id: FiberId, config: Value) -> Result<Value> {
        let fiber = self.fiber_inner(id).ok_or(CordisError::InactiveEffect)?;
        if fiber.is_disposed() {
            return Err(CordisError::InactiveEffect);
        }
        let plugin = fiber
            .plugin
            .as_ref()
            .ok_or_else(|| CordisError::Internal("root fiber has no config".to_owned()))?;
        let config = plugin.validate(config)?;
        *fiber.config.lock() = config.clone();
        let runtime = self.runtime()?;
        let mut pending_update = PendingUpdateGuard::new(Arc::clone(&runtime), id);
        // Let every update in this scheduler turn publish its config before choosing which
        // fibers drive the dependency cascade.
        tokio::task::yield_now().await;

        let provider_completions = {
            let pending = runtime.pending_updates.lock();
            let mut completions = Vec::new();
            for name in plugin.inner.inject.keys() {
                let completion = (|| {
                    let key = (fiber.context.isolation_key(name), name.clone());
                    let owner = runtime.services.read().get(&key)?.owner;
                    (owner != id)
                        .then(|| pending.get(&owner).cloned())
                        .flatten()
                })();
                if let Some(completion) = completion {
                    if !completions
                        .iter()
                        .any(|current| Arc::ptr_eq(current, &completion))
                    {
                        completions.push(completion);
                    }
                }
            }
            completions
        };
        let result = if provider_completions.is_empty() {
            self.restart_fiber(id).await
        } else {
            for provider_completion in provider_completions {
                let notified = provider_completion.notified();
                let provider_still_pending = {
                    let pending = runtime.pending_updates.lock();
                    pending
                        .values()
                        .any(|value| Arc::ptr_eq(value, &provider_completion))
                };
                if provider_still_pending {
                    notified.await;
                }
            }
            self.wait_fiber(id).await
        };
        pending_update.remove();
        if result.is_ok() && *fiber.state.lock() == FiberState::Active {
            self.publish_services(&fiber).await?;
        }
        pending_update.complete();
        result.map(|()| config)
    }

    fn log_dispose_errors(&self, fiber: &FiberInner, errors: Vec<CordisError>) {
        let logger = self.attached_from(&fiber.context).logger();
        for error in errors {
            let _ = logger.error(crate::LogValue::Error(error.to_string()), vec![]);
        }
    }
}

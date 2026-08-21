use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use serde_json::Value;

use crate::{Context, CordisError, EffectHandle, EffectMeta, Plugin, Result};

type DisposeObserver =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static>;
type UpdateObserver = Arc<
    dyn Fn(Value) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static,
>;

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FiberId(pub u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Disposed,
    Unloading,
}

#[derive(Debug, Clone)]
pub struct FiberSnapshot {
    pub id: Option<FiberId>,
    pub name: String,
    pub state: FiberState,
    pub config: Value,
    pub effects: Vec<EffectMeta>,
    pub error: Option<CordisError>,
}

pub(crate) struct FiberInner {
    pub(crate) id: FiberId,
    pub(crate) parent: FiberId,
    pub(crate) context: Context,
    pub(crate) plugin: Option<Plugin>,
    pub(crate) config: Mutex<Value>,
    pub(crate) state: Mutex<FiberState>,
    pub(crate) effects: Mutex<Vec<EffectHandle>>,
    pub(crate) disposed: AtomicBool,
    pub(crate) error: Mutex<Option<CordisError>>,
    pub(crate) lifecycle: tokio::sync::Mutex<()>,
    pub(crate) started: AtomicBool,
    pub(crate) started_notify: tokio::sync::Notify,
    dispose_observers: Mutex<Vec<DisposeObserver>>,
    dispose_observers_notified: AtomicBool,
    update_observers: Mutex<Vec<UpdateObserver>>,
    state_changes: tokio::sync::watch::Sender<FiberState>,
}

impl FiberInner {
    pub(crate) fn root(context: Context) -> Self {
        let (state_changes, _) = tokio::sync::watch::channel(FiberState::Active);
        Self {
            id: FiberId(0),
            parent: FiberId(0),
            context,
            plugin: None,
            config: Mutex::new(Value::Null),
            state: Mutex::new(FiberState::Active),
            effects: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            error: Mutex::new(None),
            lifecycle: tokio::sync::Mutex::new(()),
            started: AtomicBool::new(true),
            started_notify: tokio::sync::Notify::new(),
            dispose_observers: Mutex::new(Vec::new()),
            dispose_observers_notified: AtomicBool::new(false),
            update_observers: Mutex::new(Vec::new()),
            state_changes,
        }
    }

    pub(crate) fn plugin(
        id: FiberId,
        parent: FiberId,
        context: Context,
        plugin: Plugin,
        config: Value,
    ) -> Self {
        let (state_changes, _) = tokio::sync::watch::channel(FiberState::Pending);
        Self {
            id,
            parent,
            context,
            plugin: Some(plugin),
            config: Mutex::new(config),
            state: Mutex::new(FiberState::Pending),
            effects: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            error: Mutex::new(None),
            lifecycle: tokio::sync::Mutex::new(()),
            started: AtomicBool::new(false),
            started_notify: tokio::sync::Notify::new(),
            dispose_observers: Mutex::new(Vec::new()),
            dispose_observers_notified: AtomicBool::new(false),
            update_observers: Mutex::new(Vec::new()),
            state_changes,
        }
    }

    pub(crate) fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }

    pub(crate) fn register_effect(&self, effect: EffectHandle) -> Result<()> {
        if self.is_disposed() {
            return Err(CordisError::InactiveEffect);
        }
        self.effects.lock().push(effect);
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> FiberSnapshot {
        FiberSnapshot {
            id: (!self.is_disposed()).then_some(self.id),
            name: self
                .plugin
                .as_ref()
                .map_or_else(|| "root".to_owned(), |plugin| plugin.name().to_owned()),
            state: *self.state.lock(),
            config: self.config.lock().clone(),
            effects: self
                .effects
                .lock()
                .iter()
                .filter_map(EffectHandle::meta)
                .collect(),
            error: self.error.lock().clone(),
        }
    }

    pub(crate) fn set_state(&self, state: FiberState) {
        *self.state.lock() = state;
        self.state_changes.send_replace(state);
    }
}

#[derive(Clone)]
pub struct FiberHandle {
    pub(crate) runtime: Context,
    pub(crate) id: FiberId,
}

impl std::fmt::Debug for FiberHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FiberHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl FiberHandle {
    pub fn identity(&self) -> FiberId {
        self.id
    }

    pub fn id(&self) -> Option<FiberId> {
        self.runtime
            .fiber_inner(self.id)
            .filter(|fiber| !fiber.is_disposed())
            .map(|fiber| fiber.id)
    }

    pub fn parent(&self) -> Option<Self> {
        let fiber = self.runtime.fiber_inner(self.id)?;
        (fiber.parent != fiber.id).then(|| Self {
            runtime: self.runtime.clone(),
            id: fiber.parent,
        })
    }

    pub fn context(&self) -> Option<Context> {
        self.runtime
            .fiber_inner(self.id)
            .map(|fiber| self.runtime.attached_from(&fiber.context))
    }

    pub fn snapshot(&self) -> Option<FiberSnapshot> {
        self.runtime
            .fiber_inner(self.id)
            .map(|fiber| fiber.snapshot())
    }

    /// Subscribes to lifecycle state changes, starting with the current state.
    pub fn subscribe_state(&self) -> Option<tokio::sync::watch::Receiver<FiberState>> {
        self.runtime
            .fiber_inner(self.id)
            .map(|fiber| fiber.state_changes.subscribe())
    }

    pub fn get_effects(&self) -> Vec<EffectMeta> {
        self.snapshot().map_or_else(Vec::new, |state| state.effects)
    }

    pub fn dependencies(&self) -> Vec<String> {
        let mut names = self
            .runtime
            .fiber_inner(self.id)
            .and_then(|fiber| fiber.plugin.clone())
            .map(|plugin| plugin.inner.inject.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        names.sort();
        names
    }

    pub fn provided_services(&self) -> Vec<String> {
        let Ok(runtime) = self.runtime.runtime_arc() else {
            return Vec::new();
        };
        let mut names = runtime
            .services
            .read()
            .iter()
            .filter(|(_, service)| service.owner == self.id)
            .map(|((_, name), _)| name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }

    pub async fn wait(&self) -> Result<Self> {
        self.runtime.wait_fiber(self.id).await?;
        Ok(self.clone())
    }

    pub async fn dispose(&self) -> Result<()> {
        let handle = self.clone();
        tokio::spawn(async move { handle.dispose_observed().await })
            .await
            .map_err(|error| {
                CordisError::Internal(format!("fiber disposal task failed: {error}"))
            })?
    }

    async fn dispose_observed(&self) -> Result<()> {
        let fiber = self.runtime.fiber_inner(self.id);
        let should_notify = fiber.as_ref().is_some_and(|fiber| !fiber.is_disposed());
        self.runtime.dispose_fiber(self.id).await?;
        if !should_notify {
            return Ok(());
        }
        let Some(fiber) = fiber else {
            return Ok(());
        };
        if fiber
            .dispose_observers_notified
            .swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }
        let observers = fiber.dispose_observers.lock().clone();
        for observer in observers {
            observer().await;
        }
        Ok(())
    }

    /// Disposes without invoking owner callbacks such as loader self-disable handling.
    pub async fn dispose_managed(&self) -> Result<()> {
        let runtime = self.runtime.clone();
        let id = self.id;
        tokio::spawn(async move { runtime.dispose_fiber(id).await })
            .await
            .map_err(|error| {
                CordisError::Internal(format!("fiber disposal task failed: {error}"))
            })?
    }

    pub fn on_dispose<F, Fut>(&self, observer: F) -> Result<()>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let fiber = self
            .runtime
            .fiber_inner(self.id)
            .ok_or(CordisError::InactiveEffect)?;
        if fiber.is_disposed() {
            return Err(CordisError::InactiveEffect);
        }
        fiber
            .dispose_observers
            .lock()
            .push(Arc::new(move || Box::pin(observer())));
        Ok(())
    }

    pub fn on_update<F, Fut>(&self, observer: F) -> Result<()>
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let fiber = self
            .runtime
            .fiber_inner(self.id)
            .ok_or(CordisError::InactiveEffect)?;
        if fiber.is_disposed() {
            return Err(CordisError::InactiveEffect);
        }
        fiber
            .update_observers
            .lock()
            .push(Arc::new(move |config| Box::pin(observer(config))));
        Ok(())
    }

    pub async fn restart(&self) -> Result<()> {
        let runtime = self.runtime.clone();
        let id = self.id;
        tokio::spawn(async move { runtime.restart_fiber(id).await })
            .await
            .map_err(|error| CordisError::Internal(format!("fiber restart task failed: {error}")))?
    }

    /// Re-evaluates dependencies and readiness without forcing an active plugin to restart.
    pub async fn refresh(&self) -> Result<()> {
        let runtime = self.runtime.clone();
        let id = self.id;
        tokio::spawn(async move { runtime.refresh_fiber(id).await })
            .await
            .map_err(|error| CordisError::Internal(format!("fiber refresh task failed: {error}")))?
    }

    pub async fn update(&self, config: Value) -> Result<()> {
        let handle = self.clone();
        tokio::spawn(async move {
            let config = handle.runtime.update_fiber(handle.id, config).await?;
            let observers = handle
                .runtime
                .fiber_inner(handle.id)
                .map(|fiber| fiber.update_observers.lock().clone())
                .unwrap_or_default();
            for observer in observers {
                observer(config.clone()).await;
            }
            Ok(())
        })
        .await
        .map_err(|error| CordisError::Internal(format!("fiber update task failed: {error}")))?
    }

    /// Updates without invoking owner callbacks such as loader self-persistence handling.
    pub async fn update_managed(&self, config: Value) -> Result<()> {
        let runtime = self.runtime.clone();
        let id = self.id;
        tokio::spawn(async move { runtime.update_fiber(id, config).await.map(drop) })
            .await
            .map_err(|error| CordisError::Internal(format!("fiber update task failed: {error}")))?
    }
}

pub(crate) async fn dispose_effects(fiber: &Arc<FiberInner>) -> Vec<CordisError> {
    let effects = std::mem::take(&mut *fiber.effects.lock());
    let mut errors = Vec::new();
    for effect in effects.into_iter().rev() {
        if let Err(error) = effect.dispose().await {
            errors.push(error);
        }
    }
    errors
}

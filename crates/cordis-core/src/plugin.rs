use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use serde_json::Value;

use crate::{Context, Disposer, Result};

static NEXT_PLUGIN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PluginId(pub(crate) u64);

pub(crate) type PluginFuture =
    Pin<Box<dyn Future<Output = Result<Option<Disposer>>> + Send + 'static>>;
pub(crate) type PluginCallback =
    Arc<dyn Fn(Context, Value) -> PluginFuture + Send + Sync + 'static>;
type ConfigValidator = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync + 'static>;
type ReadinessCheck = Arc<dyn Fn(&Context) -> bool + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Plugin {
    pub(crate) inner: Arc<PluginDefinition>,
}

pub(crate) struct PluginDefinition {
    pub(crate) id: PluginId,
    pub(crate) name: String,
    pub(crate) inject: BTreeMap<String, Option<Value>>,
    pub(crate) callback: PluginCallback,
    validator: Option<ConfigValidator>,
    pub(crate) readiness: Option<ReadinessCheck>,
}

impl std::fmt::Debug for Plugin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Plugin")
            .field("id", &self.inner.id)
            .field("name", &self.inner.name)
            .field("inject", &self.inner.inject)
            .finish_non_exhaustive()
    }
}

impl Plugin {
    pub fn builder<F, Fut>(name: impl Into<String>, callback: F) -> PluginBuilder
    where
        F: Fn(Context, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Disposer>>> + Send + 'static,
    {
        PluginBuilder {
            name: name.into(),
            inject: BTreeMap::new(),
            callback: Arc::new(move |context, config| Box::pin(callback(context, config))),
            validator: None,
            readiness: None,
        }
    }

    pub fn new<F, Fut>(name: impl Into<String>, callback: F) -> Self
    where
        F: Fn(Context, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<Disposer>>> + Send + 'static,
    {
        Self::builder(name, callback).build()
    }

    pub fn id(&self) -> PluginId {
        self.inner.id
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns an equivalent plugin with additional required services.
    pub fn with_injects(&self, dependencies: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut inject = self.inner.inject.clone();
        for dependency in dependencies {
            inject.entry(dependency.into()).or_insert(None);
        }
        Self {
            inner: Arc::new(PluginDefinition {
                id: self.inner.id,
                name: self.inner.name.clone(),
                inject,
                callback: Arc::clone(&self.inner.callback),
                validator: self.inner.validator.clone(),
                readiness: self.inner.readiness.clone(),
            }),
        }
    }

    /// Returns an equivalent plugin that remains pending until the predicate is true.
    pub fn with_readiness<F>(&self, readiness: F) -> Self
    where
        F: Fn(&Context) -> bool + Send + Sync + 'static,
    {
        let readiness: ReadinessCheck = Arc::new(readiness);
        let readiness = match &self.inner.readiness {
            None => readiness,
            Some(current) => {
                let current = Arc::clone(current);
                let next = Arc::clone(&readiness);
                Arc::new(move |context: &Context| current(context) && next(context))
                    as ReadinessCheck
            }
        };
        Self {
            inner: Arc::new(PluginDefinition {
                id: self.inner.id,
                name: self.inner.name.clone(),
                inject: self.inner.inject.clone(),
                callback: Arc::clone(&self.inner.callback),
                validator: self.inner.validator.clone(),
                readiness: Some(readiness),
            }),
        }
    }

    pub(crate) fn validate(&self, config: Value) -> Result<Value> {
        match &self.inner.validator {
            Some(validator) => validator(config),
            None => Ok(config),
        }
    }
}

pub struct PluginBuilder {
    name: String,
    inject: BTreeMap<String, Option<Value>>,
    callback: PluginCallback,
    validator: Option<ConfigValidator>,
    readiness: Option<ReadinessCheck>,
}

impl PluginBuilder {
    pub fn inject(mut self, name: impl Into<String>) -> Self {
        self.inject.insert(name.into(), None);
        self
    }

    pub fn inject_with(mut self, name: impl Into<String>, intercept: Value) -> Self {
        self.inject.insert(name.into(), Some(intercept));
        self
    }

    pub fn validate_config<F>(mut self, validator: F) -> Self
    where
        F: Fn(Value) -> Result<Value> + Send + Sync + 'static,
    {
        self.validator = Some(Arc::new(validator));
        self
    }

    pub fn ready_when<F>(mut self, readiness: F) -> Self
    where
        F: Fn(&Context) -> bool + Send + Sync + 'static,
    {
        self.readiness = Some(Arc::new(readiness));
        self
    }

    pub fn build(self) -> Plugin {
        Plugin {
            inner: Arc::new(PluginDefinition {
                id: PluginId(NEXT_PLUGIN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1),
                name: self.name,
                inject: self.inject,
                callback: self.callback,
                validator: self.validator,
                readiness: self.readiness,
            }),
        }
    }
}

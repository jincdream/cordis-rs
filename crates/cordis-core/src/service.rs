use std::{any::Any, sync::Arc};

use crate::{Context, EffectHandle, IsolationKey, Result, SharedValue};

#[derive(Clone)]
pub struct ServiceRef<T>
where
    T: Any + Send + Sync,
{
    inner: Arc<T>,
}

impl<T> ServiceRef<T>
where
    T: Any + Send + Sync,
{
    pub(crate) fn new(inner: Arc<T>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Arc<T> {
        self.inner
    }
}

impl<T> std::ops::Deref for ServiceRef<T>
where
    T: Any + Send + Sync,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::fmt::Debug for ServiceRef<T>
where
    T: Any + Send + Sync + std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[derive(Clone)]
pub(crate) struct ServiceEntry {
    pub(crate) id: u64,
    pub(crate) owner: crate::FiberId,
    pub(crate) value: SharedValue,
}

#[derive(Clone)]
pub struct ServiceGuard {
    context: Context,
    name: String,
    isolation: IsolationKey,
    effect: EffectHandle,
}

impl std::fmt::Debug for ServiceGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceGuard")
            .field("name", &self.name)
            .field("isolation", &self.isolation)
            .field("disposed", &self.effect.is_disposed())
            .finish()
    }
}

impl ServiceGuard {
    pub(crate) fn new(
        context: Context,
        name: String,
        isolation: IsolationKey,
        effect: EffectHandle,
    ) -> Self {
        Self {
            context,
            name,
            isolation,
            effect,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn isolation(&self) -> IsolationKey {
        self.isolation
    }

    pub fn is_disposed(&self) -> bool {
        self.effect.is_disposed()
    }

    pub async fn dispose(&self) -> Result<()> {
        self.effect.dispose().await
    }

    pub fn context(&self) -> &Context {
        &self.context
    }
}

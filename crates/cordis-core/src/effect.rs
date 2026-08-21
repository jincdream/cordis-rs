use std::{future::Future, pin::Pin, sync::Arc};

use parking_lot::Mutex;

use crate::{CordisError, Result};

/// A boxed asynchronous cleanup callback.
pub type Disposer = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> + Send + Sync + 'static,
>;

/// User-facing effect tree metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EffectMeta {
    pub label: String,
    pub children: Vec<EffectMeta>,
}

#[derive(Clone)]
pub struct EffectHandle {
    inner: Arc<EffectInner>,
}

struct EffectInner {
    label: Option<String>,
    entries: Mutex<Option<Vec<EffectHandle>>>,
    callback: Mutex<Option<Disposer>>,
}

impl std::fmt::Debug for EffectHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectHandle")
            .field("meta", &self.meta())
            .field("disposed", &self.is_disposed())
            .finish()
    }
}

impl EffectHandle {
    pub(crate) fn collection(label: impl Into<String>, entries: Vec<Self>) -> Self {
        Self {
            inner: Arc::new(EffectInner {
                label: Some(label.into()),
                entries: Mutex::new(Some(entries)),
                callback: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn disposer(disposer: Disposer) -> Self {
        Self {
            inner: Arc::new(EffectInner {
                label: None,
                entries: Mutex::new(Some(Vec::new())),
                callback: Mutex::new(Some(disposer)),
            }),
        }
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.entries.lock().is_none()
    }

    pub fn meta(&self) -> Option<EffectMeta> {
        let label = self.inner.label.clone()?;
        let entries = self.inner.entries.lock();
        let entries = entries.as_ref()?;
        let children = entries.iter().filter_map(Self::meta).collect();
        Some(EffectMeta { label, children })
    }

    /// Disposes this effect once. Entries are reversed to match Cordis generator cleanup.
    pub async fn dispose(&self) -> Result<()> {
        let effect = self.clone();
        tokio::spawn(async move { effect.dispose_inner().await })
            .await
            .map_err(|error| {
                CordisError::Internal(format!("effect disposal task failed: {error}"))
            })?
    }

    async fn dispose_inner(&self) -> Result<()> {
        let Some(entries) = self.inner.entries.lock().take() else {
            return Ok(());
        };
        let mut first_error = None;
        for entry in entries.into_iter().rev() {
            if let Err(error) = Box::pin(entry.dispose_inner()).await {
                first_error.get_or_insert(error);
            }
        }
        let callback = self.inner.callback.lock().take();
        if let Some(callback) = callback {
            if let Err(error) = callback().await {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Collector passed to an effect setup callback.
#[derive(Clone, Default)]
pub struct EffectScope {
    entries: Arc<Mutex<Vec<EffectHandle>>>,
}

impl EffectScope {
    pub fn defer(&self, disposer: Disposer) {
        self.entries.lock().push(EffectHandle::disposer(disposer));
    }

    pub fn child(&self, effect: EffectHandle) {
        self.entries.lock().push(effect);
    }

    pub(crate) fn finish(self) -> Vec<EffectHandle> {
        std::mem::take(&mut *self.entries.lock())
    }

    pub(crate) async fn rollback(self) -> Result<()> {
        let entries = self.finish();
        let mut first_error = None;
        for entry in entries.into_iter().rev() {
            if let Err(error) = entry.dispose().await {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub(crate) fn sync_disposer<F>(callback: F) -> Disposer
where
    F: Fn() -> Result<()> + Send + Sync + 'static,
{
    Arc::new(move || {
        let result = callback();
        Box::pin(async move { result })
    })
}

pub(crate) fn infallible_disposer<F>(callback: F) -> Disposer
where
    F: Fn() + Send + Sync + 'static,
{
    sync_disposer(move || {
        callback();
        Ok(())
    })
}

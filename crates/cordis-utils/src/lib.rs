//! Effect-aware utility collections.

use std::{collections::BTreeMap, sync::Arc};

use cordis_core::{Context, EffectHandle, Result};
use parking_lot::RwLock;

/// An insertion-ordered collection whose entries follow the current fiber lifetime.
///
/// Dropping the returned [`EffectHandle`] removes the inserted value. Disposing the owning
/// fiber removes all values it inserted.
pub struct List<T> {
    context: Context,
    trace: Arc<str>,
    next_id: std::sync::atomic::AtomicU64,
    values: Arc<RwLock<BTreeMap<u64, T>>>,
}

impl<T> List<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(context: Context, trace: impl Into<Arc<str>>) -> Self {
        Self {
            context,
            trace: trace.into(),
            next_id: std::sync::atomic::AtomicU64::new(0),
            values: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.values.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.read().is_empty()
    }

    pub async fn push(&self, value: T) -> Result<EffectHandle> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let values = Arc::clone(&self.values);
        self.context
            .effect(format!("{}.push()", self.trace), move |scope| async move {
                values.write().insert(id, value);
                let dispose_values = Arc::clone(&values);
                let disposer = Arc::new(move || {
                    let values = Arc::clone(&dispose_values);
                    Box::pin(async move {
                        values.write().remove(&id);
                        Ok(())
                    }) as _
                });
                scope.defer(disposer);
                Ok(None)
            })
            .await
    }
}

impl<T> List<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn values(&self) -> Vec<T> {
        self.values.read().values().cloned().collect()
    }

    pub fn filter<F>(&self, predicate: F) -> Vec<T>
    where
        F: Fn(&T) -> bool,
    {
        self.values
            .read()
            .values()
            .filter(|value| predicate(value))
            .cloned()
            .collect()
    }

    pub fn map<U, F>(&self, mapper: F) -> Vec<U>
    where
        F: Fn(&T) -> U,
    {
        self.values.read().values().map(mapper).collect()
    }
}

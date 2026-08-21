use std::{any::Any, sync::Arc};

/// A type-erased, thread-safe value used at dynamic Cordis boundaries.
pub type SharedValue = Arc<dyn Any + Send + Sync>;

/// Erases a concrete value while retaining shared ownership.
pub fn shared<T>(value: T) -> SharedValue
where
    T: Any + Send + Sync,
{
    Arc::new(value)
}

/// Borrows a concrete value from a type-erased value.
pub fn value_ref<T>(value: &SharedValue) -> Option<&T>
where
    T: Any + Send + Sync,
{
    value.downcast_ref::<T>()
}

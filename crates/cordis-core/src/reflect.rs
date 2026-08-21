use std::{
    any::{Any, type_name},
    ops::Deref,
    sync::Arc,
};

use crate::{Context, CordisError, EffectHandle, Result, SharedValue};

pub type AccessorGetter = Arc<
    dyn Fn(Context, Option<SharedValue>) -> Result<Option<SharedValue>> + Send + Sync + 'static,
>;
pub type AccessorSetter =
    Arc<dyn Fn(Context, SharedValue, Option<SharedValue>) -> Result<bool> + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct AccessorEntry {
    pub(crate) id: u64,
    pub(crate) get: AccessorGetter,
    pub(crate) set: Option<AccessorSetter>,
}

/// A service value together with the caller context and its provider context.
#[derive(Clone)]
pub struct TrackedService<T: Any + Send + Sync> {
    value: Arc<T>,
    name: String,
    caller: Context,
    origin: Context,
}

impl<T: Any + Send + Sync> TrackedService<T> {
    pub(crate) fn new(value: Arc<T>, name: String, caller: Context, origin: Context) -> Self {
        Self {
            value,
            name,
            caller,
            origin,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn caller(&self) -> &Context {
        &self.caller
    }
    pub fn origin(&self) -> &Context {
        &self.origin
    }
    pub fn into_inner(self) -> Arc<T> {
        self.value
    }
    pub fn logger(&self) -> crate::Logger {
        if self.caller.intercept_value("logger").is_some() {
            self.caller.logger()
        } else {
            self.caller.logger_named(self.name.clone())
        }
    }

    pub fn invoke<Args>(&self, args: Args) -> <T as CallableService<Args>>::Output
    where
        T: CallableService<Args>,
    {
        self.value.invoke(&self.caller, args)
    }

    pub fn extend<Config>(&self, config: Config) -> TrackedService<T>
    where
        T: ExtendableService<Config>,
    {
        TrackedService::new(
            Arc::new(self.value.extend(&self.caller, config)),
            self.name.clone(),
            self.caller.clone(),
            self.origin.clone(),
        )
    }
}
impl<T: Any + Send + Sync> Deref for TrackedService<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// A typed base service with dynamically associated `base.member` properties.
#[derive(Clone)]
pub struct Associated<T: Any + Send + Sync> {
    service: TrackedService<T>,
    prefix: String,
}

impl<T: Any + Send + Sync> Associated<T> {
    pub(crate) fn new(service: TrackedService<T>, prefix: String) -> Self {
        Self { service, prefix }
    }
    pub fn service(&self) -> &TrackedService<T> {
        &self.service
    }
    pub fn member<U: Any + Send + Sync>(&self, name: &str) -> Result<Option<Arc<U>>> {
        let property = format!("{}.{}", self.prefix, name);
        let receiver: SharedValue = self.service.value.clone();
        self.service.caller.property(&property, Some(receiver))
    }
    pub fn set_member<U: Any + Send + Sync>(&self, name: &str, value: U) -> Result<bool> {
        let property = format!("{}.{}", self.prefix, name);
        let receiver: SharedValue = self.service.value.clone();
        self.service
            .caller
            .set_property(&property, Arc::new(value), Some(receiver))
    }
}
impl<T: Any + Send + Sync> Deref for Associated<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

/// Trait for services whose invocation depends on the caller context.
pub trait CallableService<Args> {
    type Output;
    fn invoke(&self, caller: &Context, args: Args) -> Self::Output;
}

/// Trait for creating unregistered service extensions that retain call-site metadata.
pub trait ExtendableService<Config>: Sized {
    fn extend(&self, caller: &Context, config: Config) -> Self;
}

impl Context {
    pub fn register_accessor(
        &self,
        name: impl Into<String>,
        get: AccessorGetter,
        set: Option<AccessorSetter>,
    ) -> Result<EffectHandle> {
        let name = name.into();
        let runtime = self.runtime_arc()?;
        let id = runtime
            .next_accessor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        {
            let mut accessors = runtime.accessors.write();
            if accessors.contains_key(&name) {
                return Err(CordisError::PropertyAlreadyDeclared(name));
            }
            accessors.insert(name.clone(), AccessorEntry { id, get, set });
        }
        let weak = Arc::downgrade(&runtime);
        let dispose_name = name.clone();
        let disposer = crate::effect::infallible_disposer(move || {
            if let Some(runtime) = weak.upgrade() {
                let mut accessors = runtime.accessors.write();
                if accessors
                    .get(&dispose_name)
                    .is_some_and(|entry| entry.id == id)
                {
                    accessors.remove(&dispose_name);
                }
            }
        });
        self.register_effect(EffectHandle::collection(
            format!("ctx.accessor({name:?})"),
            vec![EffectHandle::disposer(disposer)],
        ))
    }

    pub fn property<T: Any + Send + Sync>(
        &self,
        name: &str,
        receiver: Option<SharedValue>,
    ) -> Result<Option<Arc<T>>> {
        if let Some(value) = self.get_shared(name) {
            return Arc::downcast::<T>(value).map(Some).map_err(|_| {
                CordisError::PropertyTypeMismatch {
                    name: name.to_owned(),
                    expected: type_name::<T>(),
                }
            });
        }
        let accessor = self.runtime_arc()?.accessors.read().get(name).cloned();
        let Some(accessor) = accessor else {
            return Ok(None);
        };
        match (accessor.get)(self.clone(), receiver)? {
            None => Ok(None),
            Some(value) => {
                Arc::downcast::<T>(value)
                    .map(Some)
                    .map_err(|_| CordisError::PropertyTypeMismatch {
                        name: name.to_owned(),
                        expected: type_name::<T>(),
                    })
            }
        }
    }

    pub fn set_property(
        &self,
        name: &str,
        value: SharedValue,
        receiver: Option<SharedValue>,
    ) -> Result<bool> {
        let accessor = self.runtime_arc()?.accessors.read().get(name).cloned();
        let Some(accessor) = accessor else {
            return Err(CordisError::ServiceNotFound(name.to_owned()));
        };
        let setter = accessor
            .set
            .ok_or_else(|| CordisError::ReadOnlyProperty(name.to_owned()))?;
        setter(self.clone(), value, receiver)
    }

    pub fn get_tracked<T: Any + Send + Sync>(
        &self,
        name: &str,
    ) -> Result<Option<TrackedService<T>>> {
        let runtime = self.runtime_arc()?;
        let Some(entry) = self.visible_service_entry(name) else {
            return Ok(None);
        };
        let value =
            Arc::downcast::<T>(entry.value).map_err(|_| CordisError::PropertyTypeMismatch {
                name: name.to_owned(),
                expected: type_name::<T>(),
            })?;
        let origin = runtime
            .fibers
            .read()
            .get(&entry.owner)
            .map(|fiber| self.attached_from(&fiber.context))
            .ok_or(CordisError::InactiveEffect)?;
        Ok(Some(TrackedService::new(
            value,
            name.to_owned(),
            self.clone(),
            origin,
        )))
    }

    pub fn associated<T: Any + Send + Sync>(&self, name: &str) -> Result<Option<Associated<T>>> {
        Ok(self
            .get_tracked(name)?
            .map(|service| Associated::new(service, name.to_owned())))
    }

    pub fn invoke<T, Args>(
        &self,
        name: &str,
        args: Args,
    ) -> Result<<T as CallableService<Args>>::Output>
    where
        T: Any + Send + Sync + CallableService<Args>,
    {
        let service = self
            .get_tracked::<T>(name)?
            .ok_or_else(|| CordisError::ServiceNotFound(name.to_owned()))?;
        Ok(service.invoke(args))
    }
}

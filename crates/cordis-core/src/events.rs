use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use futures::future::join_all;
use parking_lot::RwLock;

use crate::{AggregateError, Context, Result, SharedValue};

pub type EventValue = SharedValue;
pub type EventArgs = Vec<EventValue>;
pub type EventFuture = Pin<Box<dyn Future<Output = Result<EventOutcome>> + Send + 'static>>;
pub type EventCallback = Arc<dyn Fn(EventInvocation) -> EventFuture + Send + Sync + 'static>;
pub type Next = Arc<dyn Fn(EventArgs) -> EventFuture + Send + Sync + 'static>;

#[derive(Clone)]
pub enum EventOutcome {
    Continue,
    Bail(EventValue),
}

impl std::fmt::Debug for EventOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue => formatter.write_str("Continue"),
            Self::Bail(_) => formatter.write_str("Bail(..)"),
        }
    }
}

#[derive(Clone)]
pub struct DispatchTarget {
    value: Option<SharedValue>,
    filter: Arc<dyn Fn(&Context) -> bool + Send + Sync>,
}

impl DispatchTarget {
    pub fn new<F>(value: Option<SharedValue>, filter: F) -> Self
    where
        F: Fn(&Context) -> bool + Send + Sync + 'static,
    {
        Self {
            value,
            filter: Arc::new(filter),
        }
    }

    pub fn unfiltered(value: SharedValue) -> Self {
        Self::new(Some(value), |_| true)
    }

    pub fn value(&self) -> Option<&SharedValue> {
        self.value.as_ref()
    }

    fn accepts(&self, context: &Context) -> bool {
        (self.filter)(context)
    }
}

#[derive(Clone)]
pub struct EventInvocation {
    args: Arc<[EventValue]>,
    target: Option<DispatchTarget>,
    next: Option<Next>,
}

impl EventInvocation {
    pub(crate) fn new(args: EventArgs, target: Option<DispatchTarget>, next: Option<Next>) -> Self {
        Self {
            args: args.into(),
            target,
            next,
        }
    }

    pub fn args(&self) -> &[EventValue] {
        &self.args
    }

    pub fn arg<T>(&self, index: usize) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.args.get(index)?.downcast_ref::<T>()
    }

    pub fn target(&self) -> Option<&SharedValue> {
        self.target.as_ref()?.value()
    }

    pub fn next(&self, args: EventArgs) -> Option<EventFuture> {
        Some((self.next.as_ref()?)(args))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EventOptions {
    pub prepend: bool,
    pub global: bool,
}

#[derive(Clone)]
struct Hook {
    id: u64,
    context: Context,
    callback: EventCallback,
    global: bool,
}

#[derive(Default)]
pub(crate) struct EventsRegistry {
    next_id: std::sync::atomic::AtomicU64,
    hooks: RwLock<HashMap<String, Vec<Hook>>>,
}

impl EventsRegistry {
    pub(crate) fn register(
        &self,
        context: Context,
        name: String,
        callback: EventCallback,
        options: EventOptions,
    ) -> u64 {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let hook = Hook {
            id,
            context,
            callback,
            global: options.global,
        };
        let mut hooks = self.hooks.write();
        let hooks = hooks.entry(name).or_default();
        if options.prepend {
            hooks.insert(0, hook);
        } else {
            hooks.push(hook);
        }
        id
    }

    pub(crate) fn unregister(&self, name: &str, id: u64) -> bool {
        let mut all_hooks = self.hooks.write();
        let Some(hooks) = all_hooks.get_mut(name) else {
            return false;
        };
        let Some(index) = hooks.iter().position(|hook| hook.id == id) else {
            return false;
        };
        hooks.remove(index);
        if hooks.is_empty() {
            all_hooks.remove(name);
        }
        true
    }

    fn resolve(&self, name: &str, target: Option<&DispatchTarget>) -> Vec<EventCallback> {
        self.hooks
            .read()
            .get(name)
            .into_iter()
            .flatten()
            .filter(|hook| {
                hook.global
                    || target
                        .map(|target| target.accepts(&hook.context))
                        .unwrap_or(true)
            })
            .map(|hook| Arc::clone(&hook.callback))
            .collect()
    }

    pub(crate) async fn emit(
        &self,
        name: &str,
        args: EventArgs,
        target: Option<DispatchTarget>,
    ) -> Result<()> {
        for callback in self.resolve(name, target.as_ref()) {
            callback(EventInvocation::new(args.clone(), target.clone(), None)).await?;
        }
        Ok(())
    }

    pub(crate) async fn parallel(
        &self,
        name: &str,
        args: EventArgs,
        target: Option<DispatchTarget>,
    ) -> std::result::Result<(), AggregateError> {
        let tasks = self
            .resolve(name, target.as_ref())
            .into_iter()
            .map(|callback| callback(EventInvocation::new(args.clone(), target.clone(), None)));
        let errors = join_all(tasks)
            .await
            .into_iter()
            .filter_map(std::result::Result::err)
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(AggregateError::new(errors))
        }
    }

    pub(crate) async fn serial(
        &self,
        name: &str,
        args: EventArgs,
        target: Option<DispatchTarget>,
    ) -> Result<EventOutcome> {
        for callback in self.resolve(name, target.as_ref()) {
            let outcome =
                callback(EventInvocation::new(args.clone(), target.clone(), None)).await?;
            if matches!(outcome, EventOutcome::Bail(_)) {
                return Ok(outcome);
            }
        }
        Ok(EventOutcome::Continue)
    }

    pub(crate) async fn waterfall(
        &self,
        name: &str,
        args: EventArgs,
        target: Option<DispatchTarget>,
        terminal: Next,
    ) -> Result<EventOutcome> {
        let callbacks = Arc::new(self.resolve(name, target.as_ref()));
        waterfall_at(callbacks, 0, args, target, terminal).await
    }
}

fn waterfall_at(
    callbacks: Arc<Vec<EventCallback>>,
    index: usize,
    args: EventArgs,
    target: Option<DispatchTarget>,
    terminal: Next,
) -> EventFuture {
    Box::pin(async move {
        let Some(callback) = callbacks.get(index).cloned() else {
            return terminal(args).await;
        };
        let next_callbacks = Arc::clone(&callbacks);
        let next_target = target.clone();
        let next_terminal = Arc::clone(&terminal);
        let next: Next = Arc::new(move |next_args| {
            waterfall_at(
                Arc::clone(&next_callbacks),
                index + 1,
                next_args,
                next_target.clone(),
                Arc::clone(&next_terminal),
            )
        });
        callback(EventInvocation::new(args, target, Some(next))).await
    })
}

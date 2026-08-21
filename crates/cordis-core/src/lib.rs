//! Rust implementation of the Cordis core runtime.
//!
//! Cordis models plugin lifetime as fibers. Services activate dependent fibers when they
//! become available and deactivate them when they disappear. Every registration is an
//! effect, so unloading a fiber reverses its side effects deterministically.

mod context;
mod effect;
mod error;
mod events;
mod fiber;
mod logger;
mod plugin;
mod reflect;
mod service;
mod value;

pub use context::{Context, IsolationKey};
pub use effect::{Disposer, EffectHandle, EffectMeta, EffectScope};
pub use error::{AggregateError, CordisError, Result};
pub use events::{
    DispatchTarget, EventArgs, EventCallback, EventFuture, EventInvocation, EventOptions,
    EventOutcome, EventValue, Next,
};
pub use fiber::{FiberHandle, FiberId, FiberSnapshot, FiberState};
pub use logger::{
    Exporter, LogLevel, LogType, LogValue, Logger, LoggerService, Message, color, color_code,
    format_message,
};
pub use plugin::{Plugin, PluginBuilder, PluginId};
pub use reflect::{
    AccessorGetter, AccessorSetter, Associated, CallableService, ExtendableService, TrackedService,
};
pub use service::{ServiceGuard, ServiceRef};
pub use value::{SharedValue, shared, value_ref};

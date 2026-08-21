use std::{error::Error as StdError, fmt, sync::Arc};

use thiserror::Error;

/// The common Cordis result type.
pub type Result<T, E = CordisError> = std::result::Result<T, E>;

/// Errors produced by the core runtime.
#[derive(Debug, Error, Clone)]
pub enum CordisError {
    #[error("cannot create effect on inactive context")]
    InactiveEffect,
    #[error("service `{0}` has not been provided in this isolation realm")]
    ServiceNotFound(String),
    #[error("service `{name}` has already been registered by fiber {fiber_id}")]
    ServiceAlreadyRegistered { name: String, fiber_id: u64 },
    #[error("service `{name}` cannot be removed by fiber {actual}; owner is fiber {owner}")]
    ServiceOwnerMismatch {
        name: String,
        owner: u64,
        actual: u64,
    },
    #[error("property `{0}` has already been declared")]
    PropertyAlreadyDeclared(String),
    #[error("property `{0}` is read-only")]
    ReadOnlyProperty(String),
    #[error("property `{name}` has a different concrete type; expected `{expected}`")]
    PropertyTypeMismatch {
        name: String,
        expected: &'static str,
    },
    #[error("plugin `{0}` failed: {1}")]
    Plugin(String, Arc<dyn StdError + Send + Sync>),
    #[error("event listener failed: {0}")]
    Listener(Arc<dyn StdError + Send + Sync>),
    #[error("effect failed: {0}")]
    Effect(Arc<dyn StdError + Send + Sync>),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("internal runtime invariant failed: {0}")]
    Internal(String),
}

/// The `parallel` dispatcher waits for every listener and reports all failures.
#[derive(Debug, Clone)]
pub struct AggregateError {
    errors: Arc<[CordisError]>,
}

impl AggregateError {
    pub(crate) fn new(errors: Vec<CordisError>) -> Self {
        Self {
            errors: errors.into(),
        }
    }

    pub fn errors(&self) -> &[CordisError] {
        &self.errors
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} event listener(s) failed", self.errors.len())
    }
}

impl StdError for AggregateError {}

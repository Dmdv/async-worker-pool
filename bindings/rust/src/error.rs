//! Typed error types for awp-rs.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwpError {
    /// Invalid configuration or argument passed to libawp (-EINVAL).
    InvalidArg,
    /// Feed, symbol, or payload exceeds maximum allowed size (-E2BIG).
    TooBig,
    /// Deadlock detected or submit called from inside worker callback (-EDEADLK).
    Deadlock,
    /// Pool is closed or shutdown is in progress (-1).
    PoolClosed,
    /// Memory allocation failed during pool initialization.
    AllocationFailed,
    /// Generic error code returned by libawp.
    Failed(i32),
}

impl fmt::Display for AwpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AwpError::InvalidArg => write!(f, "Invalid argument or configuration (EINVAL)"),
            AwpError::TooBig => write!(f, "Payload or identifier exceeds buffer capacity (E2BIG)"),
            AwpError::Deadlock => write!(f, "Deadlock or reentrant callback detected (EDEADLK)"),
            AwpError::PoolClosed => write!(f, "AsyncWorkerPool is closed or shutting down"),
            AwpError::AllocationFailed => write!(f, "Memory allocation failed"),
            AwpError::Failed(rc) => write!(f, "libawp operation failed with error code: {}", rc),
        }
    }
}

impl std::error::Error for AwpError {}

impl From<i32> for AwpError {
    fn from(rc: i32) -> Self {
        match rc {
            -22 => AwpError::InvalidArg,
            -7 => AwpError::TooBig,
            -35 => AwpError::Deadlock,
            -1 => AwpError::PoolClosed,
            code => AwpError::Failed(code),
        }
    }
}

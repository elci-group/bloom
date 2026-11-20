#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("harness unavailable: {0}")]
    Unavailable(String),
    #[error("model not found: {0}")]
    ModelNotFound(String),
    #[error("execution failed: {0}")]
    Execution(String),
    #[error("operation cancelled")]
    Cancelled,
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("harness configuration error: {0}")]
    Config(String),
}

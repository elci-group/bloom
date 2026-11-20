#[derive(Debug, thiserror::Error)]
pub enum BloomError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("harness error: {0}")]
    Harness(String),
    #[error("api error: {0}")]
    Api(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = core::result::Result<T, BloomError>;

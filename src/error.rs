use thiserror::Error;

#[derive(Debug, Error)]
pub enum KbctlError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("notion error: {0}")]
    Notion(String),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("state error: {0}")]
    State(String),
}

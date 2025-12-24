use thiserror::Error;

/// Archive error types
#[derive(Error, Debug)]
pub enum ArchiveError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// Invalid archive format
    #[error("Invalid archive format: {0}")]
    InvalidFormat(String),
}

/// Result type alias for archive operations
pub type Result<T> = std::result::Result<T, ArchiveError>;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("source error: {0}")]
    Source(#[from] SourceError),

    #[error("invalid generation manifest: {0}")]
    InvalidManifest(String),

    #[error("generation is not complete")]
    IncompleteGeneration,

    #[error("required generation file is missing from the manifest: {0}")]
    MissingFile(String),

    #[error("generation file {name} has size {actual}, expected {expected}")]
    FileSize {
        name: String,
        expected: u64,
        actual: u64,
    },

    #[error("generation file {name} has SHA-256 {actual}, expected {expected}")]
    FileHash {
        name: String,
        expected: String,
        actual: String,
    },

    #[error("invalid Archive V2 hot-block index: {0}")]
    InvalidIndex(String),

    #[error("invalid Archive V2 registry: {0}")]
    InvalidRegistry(String),

    #[error("invalid Archive V2 metadata: {0}")]
    InvalidMetadata(String),

    #[error("invalid Archive V2 block at slot {slot}: {message}")]
    InvalidBlock { slot: u64, message: String },

    #[error("cannot decode Archive V2 block at slot {slot}: {message}")]
    DecodeBlock { slot: u64, message: String },

    #[error("filter belongs to a different archive generation or registry")]
    FilterBindingMismatch,

    #[error("signatures.bin is not present in this generation")]
    SignaturesUnavailable,

    #[error("integer overflow while reading {0}")]
    Overflow(&'static str),
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("invalid object name: {0}")]
    InvalidName(String),

    #[error("I/O error for {object}: {source}")]
    Io {
        object: String,
        #[source]
        source: std::io::Error,
    },

    #[error("object {0} does not exist")]
    NotFound(String),

    #[error("short range read for {object}: got {actual} bytes, expected {expected}")]
    ShortRead {
        object: String,
        expected: usize,
        actual: usize,
    },

    #[error(
        "range for {object} is outside the object: offset={offset}, length={length}, size={size}"
    )]
    OutOfBounds {
        object: String,
        offset: u64,
        length: usize,
        size: u64,
    },

    #[error("remote source protocol error: {0}")]
    Protocol(String),
}

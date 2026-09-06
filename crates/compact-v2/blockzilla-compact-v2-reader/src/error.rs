use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("source error: {0}")]
    Source(#[from] SourceError),

    #[error("invalid generation manifest: {0}")]
    InvalidManifest(String),

    #[error("invalid operator-trusted local descriptor: {0}")]
    InvalidLocalDescriptor(String),

    #[error("Compact V2 message schema error: {0}")]
    MessageSchema(#[from] crate::message_schema::CompactV2MessageSchemaError),

    #[error("Compact V2 metadata schema error: {0}")]
    MetadataSchema(#[from] crate::metadata_schema::CompactV2MetadataSchemaError),

    #[error("generation is not complete")]
    IncompleteGeneration,

    #[error("required generation file is missing from the manifest: {0}")]
    MissingFile(String),

    #[error("required Compact V2 file is missing from the operator-trusted local source: {0}")]
    MissingLocalFile(String),

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

pub use blockzilla_source::SourceError;

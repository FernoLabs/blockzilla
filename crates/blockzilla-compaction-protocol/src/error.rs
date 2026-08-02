use thiserror::Error;

/// Failure to construct, encode, decode, or validate a V1 protocol value.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("invalid embedded Hivezilla primitive: {0}")]
    Hivezilla(#[from] hivezilla_protocol::ProtocolError),

    #[error("{field} length {actual} is outside {min}..={max}")]
    LengthOutOfBounds {
        field: &'static str,
        min: usize,
        max: usize,
        actual: usize,
    },

    #[error("{field} count {actual} exceeds {max}")]
    CountOutOfBounds {
        field: &'static str,
        max: usize,
        actual: u64,
    },

    #[error("{context} is truncated: need {needed} bytes, have {remaining}")]
    Truncated {
        context: &'static str,
        needed: usize,
        remaining: usize,
    },

    #[error("{context} has {count} trailing bytes")]
    TrailingBytes { context: &'static str, count: usize },

    #[error("invalid option tag {value} in {field}")]
    InvalidOptionTag { field: &'static str, value: u8 },

    #[error("unknown {field} value {value}")]
    UnknownEnum { field: &'static str, value: u8 },

    #[error("finality manifest version {value} is not supported")]
    UnknownFinalityManifestVersion { value: u16 },

    #[error("invalid {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },

    #[error("{field} is not in strict canonical order")]
    NonCanonicalOrder { field: &'static str },

    #[error("{field} digest does not match its exact bytes")]
    DigestMismatch { field: &'static str },

    #[error("integer overflow while processing {field}")]
    IntegerOverflow { field: &'static str },

    #[error("invalid domain prefix for {context}")]
    InvalidDomain { context: &'static str },
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

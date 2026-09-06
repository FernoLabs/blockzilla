use crate::ArchiveFormat;

/// Errors returned by the source-neutral query API.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid scan request: {0}")]
    InvalidRequest(String),
    #[error("invalid canonical transaction: {0}")]
    InvalidTransaction(String),
    #[error("invalid canonical stream: {0}")]
    InvalidStream(String),
    #[error("{format} source error")]
    Source {
        format: ArchiveFormat,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("application sink error")]
    Sink {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    /// Convert a format adapter error without exposing adapter-specific types.
    pub fn source(
        format: ArchiveFormat,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Source {
            format,
            source: Box::new(error),
        }
    }

    /// Convert an application sink error without changing the source API.
    pub fn sink(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Sink {
            source: Box::new(error),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

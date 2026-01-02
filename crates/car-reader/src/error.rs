use std::{error::Error as StdError, fmt, io};

#[derive(Debug, Clone)]
pub enum CarReadError {
    Io(String),
    UnexpectedEof(String),
    InvalidData(String),
    VarintOverflow(String),
    Cid(String),
}
pub type CarReadResult<T> = std::result::Result<T, CarReadError>;

impl fmt::Display for CarReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CarReadError::Io(s) => write!(f, "io error: {s}"),
            CarReadError::UnexpectedEof(s) => write!(f, "unexpected eof: {s}"),
            CarReadError::InvalidData(s) => write!(f, "invalid data: {s}"),
            CarReadError::VarintOverflow(s) => write!(f, "varint overflow: {s}"),
            CarReadError::Cid(s) => write!(f, "cid error: {s}"),
        }
    }
}
impl StdError for CarReadError {}
impl From<io::Error> for CarReadError {
    fn from(e: io::Error) -> Self {
        CarReadError::Io(e.to_string())
    }
}

#[derive(Debug)]
pub enum GroupError {
    /// Error while decoding a CBOR node
    Node(crate::node::NodeDecodeError),

    /// CID not found in `cid_map`
    MissingCid,

    /// `block_payload` did not decode to a `BlockNode`
    WrongRootKind,

    /// Error while decoding a Solana transaction with wincode
    TxDecode,
    IteratorStateBug,
    TxMetaDecode,
    Io,
    Other(String),
}

impl core::fmt::Display for GroupError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GroupError::Node(e) => write!(f, "{e}"),
            GroupError::MissingCid => write!(f, "missing cid payload in group"),
            GroupError::WrongRootKind => write!(f, "block_payload is not a Block node"),
            GroupError::TxDecode => write!(f, "transaction decode error"),
            GroupError::IteratorStateBug => write!(f, "iterator state bug"),
            GroupError::TxMetaDecode => write!(f, "transaction metadata decode error"),
            GroupError::Io => write!(f, "io error"),
            GroupError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for GroupError {}

impl From<crate::node::NodeDecodeError> for GroupError {
    #[inline]
    fn from(e: crate::node::NodeDecodeError) -> Self {
        GroupError::Node(e)
    }
}

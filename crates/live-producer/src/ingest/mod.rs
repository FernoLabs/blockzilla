//! Durable, source-independent primitives for the redundant live-ingest pipeline.

pub mod config;
pub mod dedup;
pub mod receipt_crypto;
pub mod replication;
pub mod spool;

pub use config::*;
pub use dedup::*;
pub use receipt_crypto::*;
pub use replication::*;
pub use spool::*;

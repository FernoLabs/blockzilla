//! A deliberately small, in-repo compatibility crate that exposes only the
//! `solana-gossip` surface currently needed by Blockzilla.
//!
//! This keeps dependency surface small while preserving the API shape we use for
//! shred repair bootstrapping and gossip/repair peer accounting.
#![forbid(unsafe_code)]

pub mod cluster_info;
pub mod contact_info;
pub mod gossip_service;
pub mod ping_pong;

pub use cluster_info::ClusterInfo;
pub use contact_info::{ContactInfo, Protocol};
pub use gossip_service::GossipService;
pub use ping_pong::{Ping, Pong};

//! Leaf primitives shared by the record model, the registry, and log parsing.
//!
//! These carry no dependency on any of them, which is what keeps
//! compact/registry/program_logs free of dependency cycles when they are
//! split into separate crates.

pub mod pubkey;
pub mod string_table;

pub use pubkey::*;
pub use string_table::*;

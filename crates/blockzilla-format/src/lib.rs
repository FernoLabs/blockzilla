//! Facade over the split format crates.
//!
//! Kept so consumers can migrate to the individual crates one at a time. Once
//! nothing imports `blockzilla_format`, delete this crate: the flat glob
//! surface is what let consumers reach past the read SDK in the first place.

pub use blockzilla_archive_v2 as archive_v2;
pub use blockzilla_compact as compact_crate;
pub use blockzilla_live_format as live_format;
pub use blockzilla_primitives as primitives_crate;
pub use blockzilla_program_logs as program_logs_crate;
pub use blockzilla_registry as registry_crate;

pub use blockzilla_archive_v2::*;
pub use blockzilla_compact::*;
pub use blockzilla_live_format::*;
pub use blockzilla_primitives::*;
pub use blockzilla_program_logs::*;
pub use blockzilla_registry::*;

// Module paths consumers already use.
pub use blockzilla_compact::compact;
pub use blockzilla_compact::split_compact;
pub use blockzilla_primitives::framed;
pub use blockzilla_primitives::primitives;
pub use blockzilla_program_logs::program_logs;
pub use blockzilla_archive_v2::v2;
pub use blockzilla_registry::registry;
pub use blockzilla_registry::blockhash_registry;

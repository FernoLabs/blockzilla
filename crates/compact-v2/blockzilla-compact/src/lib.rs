pub mod compact;
pub mod split_compact;

pub use compact::*;
pub use split_compact::*;

// The `Known` variant is gated in blockzilla-program-logs while the matches over
// it are gated here. If the two manifests ever disagree, fail with this message
// instead of a non-exhaustive-match error pointing at the wrong crate.
#[cfg(feature = "known-program-logs")]
const _: () = assert!(
    blockzilla_program_logs::KNOWN_PROGRAM_LOGS_ENABLED,
    "known-program-logs must forward to blockzilla-program-logs/known-program-logs"
);

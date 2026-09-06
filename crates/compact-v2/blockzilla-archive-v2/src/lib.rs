pub mod skipped_slots;
pub mod v2;

pub use skipped_slots::*;
pub use v2::*;

// The `Known` variant is gated in blockzilla-program-logs while the matches over
// it are gated here. If the two manifests ever disagree, fail with this message
// instead of a non-exhaustive-match error pointing at the wrong crate.
#[cfg(feature = "known-program-logs")]
const _: () = assert!(
    blockzilla_program_logs::KNOWN_PROGRAM_LOGS_ENABLED,
    "known-program-logs must forward to blockzilla-program-logs/known-program-logs"
);

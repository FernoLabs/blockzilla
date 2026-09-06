//! Build and query an immutable per-epoch signer user to reached-program index.
//!
//! A user is any required transaction signer. The relation covers top-level
//! and recorded inner/CPI program invocations from successful transactions.
//! Existing `wallet` field and file names remain unchanged for format
//! compatibility.

pub mod build;
pub mod decode;
pub mod dense_accumulator;
pub mod format;
pub mod query;
pub mod signer_rank;

// Historical wire-profile operations are opt-in so index library consumers do
// not depend on the retired read SDK or the operational HTTP/process tooling.
#[cfg(feature = "developer-tools")]
pub mod firewatch_controller_cgroup;
#[cfg(feature = "developer-tools")]
pub mod firewatch_controller_eta;
#[cfg(feature = "developer-tools")]
pub mod firewatch_wire_profile_attestation;
#[cfg(feature = "developer-tools")]
pub mod firewatch_wire_profile_transition;

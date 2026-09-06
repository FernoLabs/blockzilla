//! Add one authenticated wire-profile marker to an exact direct generation.

use std::path::PathBuf;

use anyhow::Result;
use blockzilla_user_program_index::firewatch_wire_profile_transition::{
    MarkerTransitionLocks, MarkerTransitionOptions, transition_marker_free_direct_generation,
};
use clap::Parser;

const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "firewatch-wire-profile-marker-transition",
    about = "Publish an audited marker into a manifest-free Archive V2 direct generation without changing block bytes"
)]
struct Args {
    /// Existing canonical direct Archive V2 epoch directory.
    #[arg(long)]
    archive: PathBuf,
    /// Existing canonical Firewatch controller state root. The controller must be stopped.
    #[arg(long)]
    controller_state_root: PathBuf,
    #[arg(long)]
    epoch: u64,
    #[arg(long, value_parser = ["first_seen", "usage_sorted"])]
    registry_order: String,
    /// Marker-free direct identity bound by the protected neutral attestation.
    #[arg(long)]
    old_content_generation_sha256: String,
    #[arg(long, default_value_t = 432_000)]
    slots_per_epoch: u64,
    #[arg(long, default_value_t = MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut options = MarkerTransitionOptions::new(
        args.archive,
        args.controller_state_root,
        args.epoch,
        args.registry_order,
        args.old_content_generation_sha256,
    );
    options.slots_per_epoch = args.slots_per_epoch;
    options.max_message_bytes = args.max_message_bytes;
    let locks = MarkerTransitionLocks::acquire(&options.archive, &options.controller_state_root)?;
    let outcome = transition_marker_free_direct_generation(&options, &locks)?;
    println!("wire_profile={}", outcome.wire_profile);
    println!(
        "old_content_generation_sha256={}",
        outcome.old_content_generation_sha256
    );
    println!(
        "new_content_generation_sha256={}",
        outcome.new_content_generation_sha256
    );
    println!("marker={}", outcome.marker_path.display());
    println!("intent={}", outcome.intent_path.display());
    println!("receipt={}", outcome.receipt_path.display());
    println!("new_attestation={}", outcome.new_attestation_path.display());
    Ok(())
}

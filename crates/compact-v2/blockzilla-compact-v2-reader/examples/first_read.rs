//! Run from the workspace root:
//! cargo run --locked -p blockzilla-compact-v2-reader --features http \
//!   --example first_read -- <archive-directory> <epoch>

use std::{error::Error, num::NonZeroU32};

use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSourceExt, CompactV2Archive, CompactV2LocalDescriptor, ScanRequest,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let usage = "usage: first_read <archive-directory> <epoch>";
    let root = std::fs::canonicalize(args.next().ok_or(usage)?)?;
    let epoch: u64 = args.next().ok_or(usage)?.to_str().ok_or(usage)?.parse()?;
    if args.next().is_some() {
        return Err(usage.into());
    }

    // START first-read
    let descriptor = CompactV2LocalDescriptor::mainnet(epoch, "first-read-v1")?;
    let mut archive = CompactV2Archive::open_local(root, descriptor)?;
    let range = archive.bounded_range(0, NonZeroU32::new(10).unwrap())?;

    // This first read needs block and transaction identities only.
    let request = ScanRequest::bounded(range)
        .without_instructions()
        .without_primary_signatures()
        .without_required_signers()
        .without_execution_status();

    let receipt = archive.for_each_block(&request, |block| {
        println!(
            "slot={} transactions={}",
            block.header.slot,
            block.transactions.len(),
        );
        for tx in block.transaction_views().take(5) {
            println!("  transaction={}", tx.header.tx_index);
        }
        Ok(())
    })?;

    println!(
        "Read complete: {} blocks, {} transactions",
        receipt.blocks, receipt.transactions,
    );
    // END first-read
    Ok(())
}

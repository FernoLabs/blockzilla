use std::{error::Error, io, time::Instant};

use blockzilla_compact_v2_read_sdk::{
    ArchiveInstructionSource, BlockSink, BlockView, CompactV2Archive, CompactV2LocalDescriptor,
    CompactV2ParallelScanConfig, QueryError, QueryResult, ScanRequest,
};
use blockzilla_read_compact_v2::{RunTiming, Source, count_arguments, finish_count};

const SLOTS_PER_APPROXIMATE_HOUR: u64 = 9_000;

fn main() -> Result<(), Box<dyn Error>> {
    let args = count_arguments("read-compact-v2-slot-hours")?;
    let started = Instant::now();
    let mut archive = match &args.source {
        Source::Network { origin, cache_root } => {
            CompactV2Archive::open(origin, args.epoch, cache_root)?
        }
        Source::Local {
            epoch_root,
            candidate_id,
        } => CompactV2Archive::open_local(
            epoch_root,
            CompactV2LocalDescriptor::mainnet(args.epoch, candidate_id.clone())?,
        )?,
    };
    let timing = RunTiming::after_open(started, &archive);

    let identity = archive.identity();
    let first_slot = identity.first_slot;
    let slots_per_epoch = identity.slots_per_epoch;
    let expected_blocks = u64::from(identity.block_count);
    let mut counts = ApproximateHourCounts::new(first_slot, slots_per_epoch)?;

    // Keep instruction coordinates, but skip payloads and account lists.
    let request = ScanRequest::all()
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .without_primary_signatures()
        .without_instruction_data()
        .without_instruction_accounts()
        .without_required_signers()
        .without_execution_status();
    let config = CompactV2ParallelScanConfig::new(args.threads);
    let scan = Instant::now();
    let parallel = archive.scan_ordered_parallel(&request, &mut counts, config)?;
    let scan_seconds = scan.elapsed().as_secs_f64();
    let receipt = parallel.scan;

    if receipt.blocks != expected_blocks || receipt.blocks != counts.total_blocks() {
        return Err("the scan did not visit every block in the epoch".into());
    }
    if receipt.transactions != counts.total_transactions() {
        return Err("the transaction count does not match the SDK receipt".into());
    }
    if counts.total_recorded_inner_instructions() > receipt.instructions {
        return Err("the inner-instruction count exceeds the SDK instruction count".into());
    }
    finish_count(
        &args,
        archive,
        timing,
        parallel,
        scan_seconds,
        counts.total_recorded_inner_instructions(),
    )?;
    counts.print();
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    blocks: u64,
    transactions: u64,
    recorded_inner_instructions: u64,
}

struct ApproximateHourCounts {
    first_slot: u64,
    end_slot: u64,
    last_slot: Option<u64>,
    buckets: Vec<Bucket>,
}

impl ApproximateHourCounts {
    fn new(first_slot: u64, slots_per_epoch: u64) -> Result<Self, Box<dyn Error>> {
        let end_slot = first_slot
            .checked_add(slots_per_epoch)
            .ok_or("epoch slot range overflows u64")?;
        let bucket_count = slots_per_epoch.div_ceil(SLOTS_PER_APPROXIMATE_HOUR);
        Ok(Self {
            first_slot,
            end_slot,
            last_slot: None,
            buckets: vec![Bucket::default(); usize::try_from(bucket_count)?],
        })
    }

    fn total_blocks(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.blocks).sum()
    }

    fn total_transactions(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.transactions).sum()
    }

    fn total_recorded_inner_instructions(&self) -> u64 {
        self.buckets
            .iter()
            .map(|bucket| bucket.recorded_inner_instructions)
            .sum()
    }

    fn print(&self) {
        println!(
            "bucket_basis=slot slots_per_approximate_hour=9000 assumed_slot_time_ms=400 utc_clock_hours=false"
        );
        for (index, bucket) in self.buckets.iter().enumerate() {
            let start_slot = self.first_slot + index as u64 * SLOTS_PER_APPROXIMATE_HOUR;
            let end_slot = (start_slot + SLOTS_PER_APPROXIMATE_HOUR).min(self.end_slot);
            println!(
                "approximate_hour={index} start_slot={start_slot} end_slot_exclusive={end_slot} blocks={} transactions={} recorded_inner_instructions={}",
                bucket.blocks, bucket.transactions, bucket.recorded_inner_instructions,
            );
        }
        println!(
            "total blocks={} transactions={} recorded_inner_instructions={}",
            self.total_blocks(),
            self.total_transactions(),
            self.total_recorded_inner_instructions(),
        );
    }
}

impl BlockSink for ApproximateHourCounts {
    fn visit_block(&mut self, block: BlockView<'_>) -> QueryResult<()> {
        let slot = block.header.slot;
        // The ordered SDK callback must always move forward in ledger slot order.
        if self.last_slot.is_some_and(|last_slot| slot <= last_slot) {
            return Err(QueryError::sink(io::Error::other(format!(
                "block slot {slot} is not after the prior slot"
            ))));
        }
        if slot >= self.end_slot {
            return Err(QueryError::sink(io::Error::other(
                "block is after the epoch slot range",
            )));
        }
        let offset = slot.checked_sub(self.first_slot).ok_or_else(|| {
            QueryError::sink(io::Error::other("block is before the epoch slot range"))
        })?;
        let index = usize::try_from(offset / SLOTS_PER_APPROXIMATE_HOUR)
            .map_err(|error| QueryError::sink(io::Error::other(error)))?;
        let bucket = self.buckets.get_mut(index).ok_or_else(|| {
            QueryError::sink(io::Error::other("block is after the epoch slot range"))
        })?;

        bucket.blocks += 1;
        for transaction in block.transaction_views() {
            bucket.transactions += 1;
            bucket.recorded_inner_instructions += transaction
                .instructions
                .iter()
                .filter(|instruction| instruction.coordinate.inner_index.is_some())
                .count() as u64;
        }
        self.last_slot = Some(slot);
        Ok(())
    }
}

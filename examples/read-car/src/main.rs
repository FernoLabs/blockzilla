use blockzilla_example_workloads::ProgressSink;

use std::{error::Error, io, time::Instant};

use blockzilla_read_car::{CountSource, RunFacts, count_arguments};
use of_car_reader::archive::{
    ArchiveInstructionSource, BlockSink, BlockView, CarArchive, QueryError, QueryResult,
    ScanRequest,
};

const SLOTS_PER_APPROXIMATE_HOUR: u64 = 9_000;

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = count_arguments("read-car")?;
    let total_started = Instant::now();
    let mut archive = open_archive(&arguments)?;
    let setup_seconds = total_started.elapsed().as_secs_f64();

    let identity = archive.identity();
    let expected_blocks = u64::from(identity.block_count);
    let requested_blocks = identity.block_count;
    let mut counts = ApproximateHourCounts::new(identity.first_slot, identity.slots_per_epoch)?;

    // Keep instruction coordinates, but skip payloads and unused projections.
    let request = ScanRequest::all()
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .without_primary_signatures()
        .without_instruction_data()
        .without_instruction_accounts()
        .without_instruction_programs()
        .without_required_signers()
        .without_execution_status()
        .count_instructions_only();

    let verification = archive.identity().verification;
    let bound_source_size_bytes = archive.bound_source_size_bytes();
    let setup_io = archive.io_snapshot();
    let scan_started = Instant::now();
    let receipt = archive.scan_ordered(
        &request,
        &mut ProgressSink::new(&mut counts, expected_blocks),
    )?;
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let total_io = archive.finish_io();
    let total_seconds = total_started.elapsed().as_secs_f64();
    let scan_io = total_io.saturating_sub(setup_io);

    if receipt.blocks != expected_blocks || receipt.blocks != counts.total_blocks() {
        return Err("the scan did not visit every block in the epoch".into());
    }
    if receipt.transactions != counts.total_transactions() {
        return Err("the transaction count does not match the SDK receipt".into());
    }
    if counts.total_recorded_inner_instructions() > receipt.instructions {
        return Err("the inner-instruction count exceeds the SDK instruction count".into());
    }

    let run = RunFacts {
        epoch: arguments.epoch,
        verification,
        requested_blocks,
        bound_source_size_bytes,
        receipt,
        setup_seconds,
        scan_seconds,
        total_seconds,
        setup_io,
        scan_io,
        total_io,
    };
    println!(
        "{run} recorded_inner_instructions={} transactions_with_incomplete_instructions={} transactions_with_incomplete_cpi={}",
        counts.total_recorded_inner_instructions(),
        receipt.transactions_with_incomplete_instructions,
        receipt.transactions_with_incomplete_cpi,
    );
    counts.print();
    Ok(())
}

fn open_archive(
    arguments: &blockzilla_read_car::CountArguments,
) -> of_car_reader::archive::Result<CarArchive> {
    match &arguments.source {
        CountSource::Network { origin } => {
            CarArchive::open(origin, arguments.epoch, arguments.expected_blocks)
        }
        CountSource::Local { archive_root } => {
            CarArchive::open_local(archive_root, arguments.epoch, arguments.expected_blocks)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

    fn add(
        &mut self,
        slot: u64,
        transactions: u64,
        recorded_inner_instructions: u64,
    ) -> QueryResult<()> {
        if self.last_slot.is_some_and(|last_slot| slot <= last_slot) {
            return Err(sink_error(format!(
                "block slot {slot} is not after the prior slot"
            )));
        }
        let offset = slot
            .checked_sub(self.first_slot)
            .ok_or_else(|| sink_error("block is before the epoch slot range"))?;
        if slot >= self.end_slot {
            return Err(sink_error("block is after the epoch slot range"));
        }
        let index = usize::try_from(offset / SLOTS_PER_APPROXIMATE_HOUR)
            .map_err(|error| sink_error(error.to_string()))?;
        let bucket = self
            .buckets
            .get_mut(index)
            .ok_or_else(|| sink_error("block is after the epoch slot range"))?;

        increment(&mut bucket.blocks, 1)?;
        increment(&mut bucket.transactions, transactions)?;
        increment(
            &mut bucket.recorded_inner_instructions,
            recorded_inner_instructions,
        )?;
        self.last_slot = Some(slot);
        Ok(())
    }

    fn print(&self) {
        println!(
            "bucket_basis=slot slots_per_approximate_hour=9000 assumed_slot_time_ms=400 utc_clock_hours=false"
        );
        for (index, bucket) in self.buckets.iter().enumerate() {
            let index = u64::try_from(index).expect("bucket index fits in u64");
            let start_slot = self.first_slot + index * SLOTS_PER_APPROXIMATE_HOUR;
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
        if let Some(counts) = block.counts {
            return self.add(
                block.header.slot,
                counts.transactions,
                counts.recorded_inner_instructions,
            );
        }
        let transactions = u64::try_from(block.transactions.len())
            .map_err(|error| sink_error(error.to_string()))?;
        let recorded_inner_instructions =
            block
                .transaction_views()
                .try_fold(0u64, |total, transaction| {
                    let inner = transaction
                        .instructions
                        .iter()
                        .filter(|instruction| instruction.coordinate.inner_index.is_some())
                        .count();
                    let inner =
                        u64::try_from(inner).map_err(|error| sink_error(error.to_string()))?;
                    total
                        .checked_add(inner)
                        .ok_or_else(|| sink_error("inner-instruction count overflow"))
                })?;
        self.add(block.header.slot, transactions, recorded_inner_instructions)
    }
}

fn increment(count: &mut u64, amount: u64) -> QueryResult<()> {
    *count = count
        .checked_add(amount)
        .ok_or_else(|| sink_error("count overflow"))?;
    Ok(())
}

fn sink_error(message: impl Into<String>) -> QueryError {
    QueryError::sink(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_fixed_slot_windows() {
        let mut counts = ApproximateHourCounts::new(1_000, 18_001).unwrap();
        counts.add(1_000, 2, 3).unwrap();
        counts.add(9_999, 5, 7).unwrap();
        counts.add(10_000, 11, 13).unwrap();
        counts.add(19_000, 17, 19).unwrap();

        assert_eq!(
            counts.buckets,
            vec![
                Bucket {
                    blocks: 2,
                    transactions: 7,
                    recorded_inner_instructions: 10,
                },
                Bucket {
                    blocks: 1,
                    transactions: 11,
                    recorded_inner_instructions: 13,
                },
                Bucket {
                    blocks: 1,
                    transactions: 17,
                    recorded_inner_instructions: 19,
                },
            ]
        );
        assert_eq!(counts.total_blocks(), 4);
        assert_eq!(counts.total_transactions(), 35);
        assert_eq!(counts.total_recorded_inner_instructions(), 42);
    }

    #[test]
    fn rejects_duplicate_or_decreasing_slots() {
        let mut duplicate = ApproximateHourCounts::new(1_000, 9_000).unwrap();
        duplicate.add(1_100, 1, 0).unwrap();
        assert!(duplicate.add(1_100, 1, 0).is_err());

        let mut decreasing = ApproximateHourCounts::new(1_000, 9_000).unwrap();
        decreasing.add(1_100, 1, 0).unwrap();
        assert!(decreasing.add(1_099, 1, 0).is_err());
    }

    #[test]
    fn rejects_slots_outside_the_epoch() {
        let mut before = ApproximateHourCounts::new(1_000, 9_000).unwrap();
        assert!(before.add(999, 1, 0).is_err());

        let mut after = ApproximateHourCounts::new(1_000, 9_000).unwrap();
        assert!(after.add(10_000, 1, 0).is_err());
    }
}

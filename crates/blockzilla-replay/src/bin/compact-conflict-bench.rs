//! Structural transaction-concurrency analysis for Blockzilla Compact V2.
//!
//! The benchmark keeps the canonical transaction payload in ledger order and
//! constructs one bounded account-conflict graph per produced slot.  It never
//! opens a CAR file, never executes a transaction, and never mutates replay
//! state.  Unit and instruction-count weights describe structural scheduling
//! opportunity only; they are not predictions of wall-clock SBF execution.

use std::{
    cmp::Reverse,
    collections::BTreeSet,
    fmt::Write as _,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use blockzilla_format::{ARCHIVE_V2_PUBKEY_REGISTRY_FILE, KeyIndex, KeyStore};
use blockzilla_read_sdk::{ArchiveReader, HashVerification, LocalRangeSource, OpenOptions};
use blockzilla_replay::{
    BPF_LOADER_PROGRAM_ID, CONFIG_PROGRAM_ID, CompactArchivedTransactionOutcome,
    CompactMessageVersion, CompactProbeError, CompactSlotProbe, CompactTransactionProbe,
    CompactVisitConfig, CompactVisitControl, CompactVisitEvent, STAKE_PROGRAM_ID,
    SYSTEM_PROGRAM_ID, VOTE_PROGRAM_ID,
    conflict_schedule::{
        AccountAccess, ConflictPlan, ScheduleSimulation, TransactionPlanInput, plan_slot,
    },
    read_compact_generation_context, visit_compact_generation_without_program_counts,
};
use clap::{Parser, ValueEnum};
use hashbrown::{HashMap, hash_map::Entry};
use smallvec::SmallVec;

const DEFAULT_WORKERS: &str = "1,2,4,8,12,16";
const DEFAULT_PROGRESS_ROWS: usize = 10_000;
const DEFAULT_TOP_ACCOUNTS: usize = 20;
const DEFAULT_MAX_RAW_KEYS: usize = 100_000;
const GLOBAL_BARRIER_ACCOUNT: u32 = 0;
const FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CostModel {
    /// Every transaction has weight one.  This measures graph shape only.
    Unit,
    /// Weight is the number of top-level instructions, with a minimum of one.
    InstructionCount,
}

impl CostModel {
    fn label(self) -> &'static str {
        match self {
            Self::Unit => "unit",
            Self::InstructionCount => "instruction-count",
        }
    }

    fn weight(self, transaction: &CompactTransactionProbe) -> u64 {
        match self {
            Self::Unit => 1,
            Self::InstructionCount => transaction.instructions.len().max(1) as u64,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "compact-conflict-bench",
    about = "Measure structural transaction concurrency in Blockzilla Compact V2 (never CAR)"
)]
struct Args {
    /// Root of one sealed Blockzilla Compact Archive V2 generation.
    generation: PathBuf,

    /// Zero-based hot-index row at which analysis starts.
    #[arg(long, default_value_t = 0)]
    start_row: usize,

    /// Maximum number of present block rows.  Omit for the complete remainder.
    #[arg(long)]
    rows: Option<usize>,

    /// Comma-separated deterministic worker counts to simulate.
    #[arg(long, default_value = DEFAULT_WORKERS)]
    workers: String,

    /// Structural transaction weight model.
    #[arg(long, value_enum, default_value_t = CostModel::Unit)]
    cost_model: CostModel,

    /// Report progress after this many rows; zero disables progress output.
    #[arg(long, default_value_t = DEFAULT_PROGRESS_ROWS)]
    progress_rows: usize,

    /// Number of exact hottest writable accounts to report.
    #[arg(long, default_value_t = DEFAULT_TOP_ACCOUNTS)]
    top_accounts: usize,

    /// Abort if decoded keys absent from the sealed registry exceed this count.
    #[arg(long, default_value_t = DEFAULT_MAX_RAW_KEYS)]
    max_raw_keys: usize,
}

#[derive(Debug, Clone)]
struct Selection {
    start_row: usize,
    end_row: usize,
    start_slot: u64,
    end_slot_exclusive: Option<u64>,
    rows: u64,
    transactions: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

impl Selection {
    fn open(root: &Path, start_row: usize, requested_rows: Option<usize>) -> Result<Self> {
        let archive = ArchiveReader::open_with_options(
            LocalRangeSource::new(root),
            OpenOptions {
                hash_verification: HashVerification::ControlFiles,
                ..OpenOptions::default()
            },
        )
        .with_context(|| format!("open Compact V2 generation {}", root.display()))?;
        let rows = &archive.index().rows;
        ensure!(
            !rows.is_empty(),
            "Compact generation has an empty hot index"
        );
        ensure!(
            start_row < rows.len(),
            "--start-row {start_row} is outside the hot index (rows={})",
            rows.len()
        );
        let requested_rows = requested_rows.unwrap_or_else(|| rows.len() - start_row);
        ensure!(requested_rows > 0, "--rows must be greater than zero");
        let end_row = start_row.saturating_add(requested_rows).min(rows.len());
        let selected = &rows[start_row..end_row];
        let transactions = checked_sum(selected.iter().map(|row| u64::from(row.tx_count)))?;
        let compressed_bytes =
            checked_sum(selected.iter().map(|row| u64::from(row.compressed_len)))?;
        let uncompressed_bytes =
            checked_sum(selected.iter().map(|row| u64::from(row.uncompressed_len)))?;
        Ok(Self {
            start_row,
            end_row,
            start_slot: selected[0].slot,
            end_slot_exclusive: rows.get(end_row).map(|row| row.slot),
            rows: selected.len() as u64,
            transactions,
            compressed_bytes,
            uncompressed_bytes,
        })
    }

    fn visit_config(&self) -> CompactVisitConfig {
        CompactVisitConfig {
            start_slot: Some(self.start_slot),
            end_slot_exclusive: self.end_slot_exclusive,
            max_slots: Some(self.rows as usize),
        }
    }
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| anyhow!("selection counter overflow"))
    })
}

/// Maps already-resolved pubkeys back to generation-registry ids, then maps
/// only the accounts touched by one slot into the planner's dense id space.
struct RegistryIndexer {
    index: KeyIndex,
    global_keys: Vec<[u8; 32]>,
    raw_ids: HashMap<[u8; 32], u32>,
    slot_generation: u32,
    slot_stamps: Vec<u32>,
    slot_local_ids: Vec<u32>,
    next_local_id: u32,
    access_counts: Vec<u64>,
    writable_counts: Vec<u64>,
    raw_lookup_count: u64,
}

impl RegistryIndexer {
    fn load(root: &Path) -> Result<(Self, Duration)> {
        let path = root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let started = Instant::now();
        let store = KeyStore::load(&path)
            .with_context(|| format!("load Compact registry {}", path.display()))?;
        ensure!(
            store.keys.len() < u32::MAX as usize,
            "registry is too large for dense u32 ids"
        );
        let index = KeyIndex::build_from_slice_low_memory(&store.keys);
        let len = store.keys.len();
        let indexer = Self {
            index,
            global_keys: store.keys,
            raw_ids: HashMap::new(),
            slot_generation: 0,
            slot_stamps: vec![0; len],
            slot_local_ids: vec![0; len],
            next_local_id: 1,
            access_counts: vec![0; len],
            writable_counts: vec![0; len],
            raw_lookup_count: 0,
        };
        Ok((indexer, started.elapsed()))
    }

    fn begin_slot(&mut self) {
        self.slot_generation = self.slot_generation.wrapping_add(1);
        if self.slot_generation == 0 {
            self.slot_stamps.fill(0);
            self.slot_generation = 1;
        }
        // Account zero is reserved for an optional global serialization point.
        self.next_local_id = 1;
    }

    fn resolve(&mut self, pubkey: [u8; 32]) -> Result<(u32, u32)> {
        let global_id = if let Some(id) = self.index.lookup(&pubkey) {
            id
        } else {
            self.raw_lookup_count = self
                .raw_lookup_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("raw lookup counter overflow"))?;
            match self.raw_ids.entry(pubkey) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let id = u32::try_from(self.global_keys.len())
                        .ok()
                        .and_then(|length| length.checked_add(1))
                        .ok_or_else(|| anyhow!("raw pubkey id space exhausted"))?;
                    entry.insert(id);
                    self.global_keys.push(pubkey);
                    self.slot_stamps.push(0);
                    self.slot_local_ids.push(0);
                    self.access_counts.push(0);
                    self.writable_counts.push(0);
                    id
                }
            }
        };
        let global_index = global_id
            .checked_sub(1)
            .ok_or_else(|| anyhow!("registry returned reserved id zero"))?
            as usize;
        let local_id = if self.slot_stamps[global_index] == self.slot_generation {
            self.slot_local_ids[global_index]
        } else {
            let id = self.next_local_id;
            self.next_local_id = self
                .next_local_id
                .checked_add(1)
                .ok_or_else(|| anyhow!("slot-local account id overflow"))?;
            self.slot_stamps[global_index] = self.slot_generation;
            self.slot_local_ids[global_index] = id;
            id
        };
        Ok((global_id, local_id))
    }

    fn account_count(&self) -> usize {
        self.next_local_id as usize
    }

    fn record_access(&mut self, global_id: u32, writable: bool) -> Result<()> {
        let index = global_id
            .checked_sub(1)
            .ok_or_else(|| anyhow!("cannot record pseudo-account access"))?
            as usize;
        self.access_counts[index] = self.access_counts[index]
            .checked_add(1)
            .ok_or_else(|| anyhow!("account access counter overflow"))?;
        if writable {
            self.writable_counts[index] = self.writable_counts[index]
                .checked_add(1)
                .ok_or_else(|| anyhow!("writable account counter overflow"))?;
        }
        Ok(())
    }

    fn hottest_writable(&self, limit: usize) -> Vec<(u32, [u8; 32], u64, u64)> {
        let mut indices = self
            .writable_counts
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, writes)| *writes != 0)
            .collect::<Vec<_>>();
        indices.sort_unstable_by_key(|(index, writes)| {
            Reverse((*writes, self.access_counts[*index], Reverse(*index)))
        });
        indices
            .into_iter()
            .take(limit)
            .map(|(index, writes)| {
                (
                    index as u32 + 1,
                    self.global_keys[index],
                    self.access_counts[index],
                    writes,
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedAccess {
    global_id: Option<u32>,
    access: AccountAccess,
}

#[derive(Debug, Clone, Copy)]
struct TransactionRange {
    canonical_index: u32,
    start: usize,
    end: usize,
    weight: u64,
}

#[derive(Debug, Default)]
struct WorkerAggregate {
    workers: usize,
    makespan: u128,
    lower_bound: u128,
    peak_running: usize,
    max_ready: usize,
    ready_width: Vec<u64>,
    schedule_fingerprint: u64,
}

impl WorkerAggregate {
    fn new(workers: usize, slot_capacity: usize) -> Self {
        Self {
            workers,
            ready_width: Vec::with_capacity(slot_capacity),
            schedule_fingerprint: FINGERPRINT_OFFSET,
            ..Self::default()
        }
    }

    fn add(&mut self, slot: u64, simulation: ScheduleSimulation) -> Result<()> {
        ensure!(
            self.workers == simulation.workers,
            "worker aggregate mismatch"
        );
        self.makespan = self
            .makespan
            .checked_add(u128::from(simulation.makespan))
            .ok_or_else(|| anyhow!("simulated makespan overflow"))?;
        let work_bound = simulation.total_weight.div_ceil(self.workers as u64);
        let lower_bound = simulation.critical_path_weight.max(work_bound);
        ensure!(
            simulation.makespan >= lower_bound,
            "simulated makespan is below its scheduling lower bound"
        );
        self.lower_bound = self
            .lower_bound
            .checked_add(u128::from(lower_bound))
            .ok_or_else(|| anyhow!("simulated lower-bound overflow"))?;
        self.peak_running = self.peak_running.max(simulation.peak_running);
        self.max_ready = self.max_ready.max(simulation.max_ready);
        self.ready_width.push(simulation.max_ready as u64);
        self.schedule_fingerprint = fold_fingerprint(
            self.schedule_fingerprint,
            [slot, simulation.schedule_fingerprint],
        );
        Ok(())
    }
}

#[derive(Debug)]
struct Aggregate {
    workers: Vec<WorkerAggregate>,
    slots: u64,
    transactions: u64,
    failed_transactions: u64,
    succeeded_transactions: u64,
    unknown_transactions: u64,
    loader_barrier_transactions: u64,
    instructions: u64,
    readonly_accesses: u64,
    writable_accesses: u64,
    dependency_edges: u64,
    raw_conflicts: u64,
    war_conflicts: u64,
    waw_conflicts: u64,
    total_weight: u128,
    critical_path_weight: u128,
    max_plan_bytes: usize,
    max_slot_transactions: usize,
    max_dependency_chain: usize,
    max_initial_ready: usize,
    max_level_width: usize,
    projection_time: Duration,
    graph_time: Duration,
    simulation_time: Duration,
    slot_transactions: Vec<u64>,
    initial_ready: Vec<u64>,
    level_width: Vec<u64>,
    dependency_chain: Vec<u64>,
    parallelism_milli: Vec<u64>,
    metric_fingerprint: u64,
    program_counts: HashMap<[u8; 32], u64>,
}

impl Aggregate {
    fn new(worker_counts: &[usize], capacity: usize) -> Self {
        Self {
            workers: worker_counts
                .iter()
                .copied()
                .map(|workers| WorkerAggregate::new(workers, capacity))
                .collect(),
            slot_transactions: Vec::with_capacity(capacity),
            initial_ready: Vec::with_capacity(capacity),
            level_width: Vec::with_capacity(capacity),
            dependency_chain: Vec::with_capacity(capacity),
            parallelism_milli: Vec::with_capacity(capacity),
            metric_fingerprint: FINGERPRINT_OFFSET,
            program_counts: HashMap::new(),
            slots: 0,
            transactions: 0,
            failed_transactions: 0,
            succeeded_transactions: 0,
            unknown_transactions: 0,
            loader_barrier_transactions: 0,
            instructions: 0,
            readonly_accesses: 0,
            writable_accesses: 0,
            dependency_edges: 0,
            raw_conflicts: 0,
            war_conflicts: 0,
            waw_conflicts: 0,
            total_weight: 0,
            critical_path_weight: 0,
            max_plan_bytes: 0,
            max_slot_transactions: 0,
            max_dependency_chain: 0,
            max_initial_ready: 0,
            max_level_width: 0,
            projection_time: Duration::ZERO,
            graph_time: Duration::ZERO,
            simulation_time: Duration::ZERO,
        }
    }

    fn record_transaction(&mut self, transaction: &CompactTransactionProbe) -> Result<()> {
        self.transactions = checked_increment(self.transactions, "transaction")?;
        self.instructions = self
            .instructions
            .checked_add(transaction.instructions.len() as u64)
            .ok_or_else(|| anyhow!("instruction counter overflow"))?;
        match transaction.archived_outcome {
            CompactArchivedTransactionOutcome::Unknown => {
                self.unknown_transactions =
                    checked_increment(self.unknown_transactions, "unknown transaction")?;
            }
            CompactArchivedTransactionOutcome::Succeeded => {
                self.succeeded_transactions =
                    checked_increment(self.succeeded_transactions, "succeeded transaction")?;
            }
            CompactArchivedTransactionOutcome::Failed => {
                self.failed_transactions =
                    checked_increment(self.failed_transactions, "failed transaction")?;
            }
        }
        for instruction in &transaction.instructions {
            let count = self
                .program_counts
                .entry(instruction.program_id)
                .or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| anyhow!("program instruction counter overflow"))?;
        }
        Ok(())
    }

    fn record_plan(&mut self, slot: u64, plan: &ConflictPlan) -> Result<()> {
        let metrics = plan.metrics();
        self.slots = checked_increment(self.slots, "slot")?;
        self.readonly_accesses = checked_add_usize(
            self.readonly_accesses,
            metrics.readonly_accesses,
            "readonly access",
        )?;
        self.writable_accesses = checked_add_usize(
            self.writable_accesses,
            metrics.writable_accesses,
            "writable access",
        )?;
        self.dependency_edges =
            checked_add_usize(self.dependency_edges, metrics.edge_count, "dependency edge")?;
        self.raw_conflicts =
            checked_add_usize(self.raw_conflicts, metrics.raw_conflicts, "RAW conflict")?;
        self.war_conflicts =
            checked_add_usize(self.war_conflicts, metrics.war_conflicts, "WAR conflict")?;
        self.waw_conflicts =
            checked_add_usize(self.waw_conflicts, metrics.waw_conflicts, "WAW conflict")?;
        self.total_weight = self
            .total_weight
            .checked_add(u128::from(metrics.total_weight))
            .ok_or_else(|| anyhow!("total structural weight overflow"))?;
        self.critical_path_weight = self
            .critical_path_weight
            .checked_add(u128::from(metrics.critical_path_weight))
            .ok_or_else(|| anyhow!("critical-path weight overflow"))?;
        self.max_plan_bytes = self.max_plan_bytes.max(metrics.estimated_bytes);
        self.max_slot_transactions = self.max_slot_transactions.max(metrics.tx_count);
        self.max_dependency_chain = self
            .max_dependency_chain
            .max(metrics.longest_dependency_chain_transactions);
        self.max_initial_ready = self.max_initial_ready.max(metrics.initial_ready);
        self.max_level_width = self.max_level_width.max(metrics.max_dependency_level_width);
        self.slot_transactions.push(metrics.tx_count as u64);
        self.initial_ready.push(metrics.initial_ready as u64);
        self.level_width
            .push(metrics.max_dependency_level_width as u64);
        self.dependency_chain
            .push(metrics.longest_dependency_chain_transactions as u64);
        let parallelism_milli = if metrics.critical_path_weight == 0 {
            0
        } else {
            let value = (u128::from(metrics.total_weight) * 1_000)
                / u128::from(metrics.critical_path_weight);
            value.min(u128::from(u64::MAX)) as u64
        };
        self.parallelism_milli.push(parallelism_milli);
        // This fingerprints the aggregate graph metrics, not every edge.  The
        // per-worker schedule fingerprints below cover actual dispatch order.
        self.metric_fingerprint = fold_fingerprint(
            self.metric_fingerprint,
            [
                slot,
                metrics.tx_count as u64,
                metrics.edge_count as u64,
                metrics.critical_path_weight,
            ],
        );
        Ok(())
    }
}

struct Analyzer {
    cost_model: CostModel,
    progress_rows: usize,
    max_raw_keys: usize,
    scan_started: Instant,
    indexer: RegistryIndexer,
    aggregate: Aggregate,
    projected: SmallVec<[ProjectedAccess; 32]>,
    flat_accesses: Vec<AccountAccess>,
    transaction_ranges: Vec<TransactionRange>,
    loader_flags: Vec<bool>,
}

impl Analyzer {
    fn new(
        cost_model: CostModel,
        progress_rows: usize,
        max_raw_keys: usize,
        worker_counts: &[usize],
        expected_slots: usize,
        indexer: RegistryIndexer,
    ) -> Self {
        Self {
            cost_model,
            progress_rows,
            max_raw_keys,
            scan_started: Instant::now(),
            indexer,
            aggregate: Aggregate::new(worker_counts, expected_slots),
            projected: SmallVec::new(),
            flat_accesses: Vec::new(),
            transaction_ranges: Vec::new(),
            loader_flags: Vec::new(),
        }
    }

    fn analyze_slot(&mut self, slot: &CompactSlotProbe) -> Result<()> {
        ensure!(
            slot.transactions.len() == slot.transaction_count as usize,
            "slot {} retained {} transactions but declares {}",
            slot.slot,
            slot.transactions.len(),
            slot.transaction_count
        );
        self.indexer.begin_slot();
        self.flat_accesses.clear();
        self.transaction_ranges.clear();
        for transaction in &slot.transactions {
            ensure!(
                transaction.version == CompactMessageVersion::Legacy,
                "slot {} tx {} is V0; exact loaded addresses are unavailable in the current replay projection",
                slot.slot,
                transaction.tx_index
            );
            self.aggregate.record_transaction(transaction)?;
        }

        let projection_started = Instant::now();
        self.loader_flags.clear();
        let mut loader_barrier = false;
        for transaction in &slot.transactions {
            let transaction_is_loader = is_loader_transaction(transaction);
            self.loader_flags.push(transaction_is_loader);
            if transaction_is_loader {
                loader_barrier = true;
                self.aggregate.loader_barrier_transactions = checked_increment(
                    self.aggregate.loader_barrier_transactions,
                    "loader-barrier transaction",
                )?;
            }
        }
        for (index, transaction) in slot.transactions.iter().enumerate() {
            let transaction_is_loader = self.loader_flags[index];
            self.project_transaction(
                slot.slot,
                transaction,
                loader_barrier,
                transaction_is_loader,
            )?;
        }
        self.aggregate.projection_time += projection_started.elapsed();

        let graph_started = Instant::now();
        let inputs = self
            .transaction_ranges
            .iter()
            .map(|range| TransactionPlanInput {
                canonical_index: range.canonical_index,
                accesses: &self.flat_accesses[range.start..range.end],
                weight: range.weight,
            })
            .collect::<Vec<_>>();
        let plan = plan_slot(self.indexer.account_count(), &inputs)
            .with_context(|| format!("build conflict plan for slot {}", slot.slot))?;
        self.aggregate.graph_time += graph_started.elapsed();
        ensure!(
            plan.tx_count() == slot.transactions.len(),
            "slot {} plan transaction count mismatch",
            slot.slot
        );
        self.aggregate.record_plan(slot.slot, &plan)?;

        let simulation_started = Instant::now();
        for worker in &mut self.aggregate.workers {
            let simulation = plan.simulate(worker.workers).with_context(|| {
                format!(
                    "simulate slot {} with {} workers",
                    slot.slot, worker.workers
                )
            })?;
            if worker.workers == 1 {
                ensure!(
                    simulation.makespan == plan.total_weight(),
                    "one-worker schedule diverged from total weight at slot {}",
                    slot.slot
                );
            }
            worker.add(slot.slot, simulation)?;
        }
        self.aggregate.simulation_time += simulation_started.elapsed();
        ensure!(
            self.indexer.raw_ids.len() <= self.max_raw_keys,
            "decoded keys absent from the sealed registry exceeded --max-raw-keys: {} > {}",
            self.indexer.raw_ids.len(),
            self.max_raw_keys
        );

        if self.progress_rows != 0
            && self
                .aggregate
                .slots
                .is_multiple_of(self.progress_rows as u64)
        {
            let elapsed = self.scan_started.elapsed().as_secs_f64();
            eprintln!(
                "progress slots={} transactions={} elapsed_s={:.3} slots_per_s={:.3} tx_per_s={:.3} planner_s={:.3} raw_unique_keys={}",
                self.aggregate.slots,
                self.aggregate.transactions,
                elapsed,
                self.aggregate.slots as f64 / elapsed,
                self.aggregate.transactions as f64 / elapsed,
                (self.aggregate.projection_time
                    + self.aggregate.graph_time
                    + self.aggregate.simulation_time)
                    .as_secs_f64(),
                self.indexer.raw_ids.len(),
            );
        }
        Ok(())
    }

    fn project_transaction(
        &mut self,
        slot: u64,
        transaction: &CompactTransactionProbe,
        slot_has_loader_barrier: bool,
        transaction_is_loader: bool,
    ) -> Result<()> {
        let required = transaction.header.num_required_signatures as usize;
        let readonly_signed = transaction.header.num_readonly_signed_accounts as usize;
        let readonly_unsigned = transaction.header.num_readonly_unsigned_accounts as usize;
        let key_count = transaction.account_keys.len();
        ensure!(
            required <= key_count,
            "slot {slot} tx {} required signer count exceeds account keys",
            transaction.tx_index
        );
        ensure!(
            readonly_signed <= required,
            "slot {slot} tx {} readonly signed count exceeds signers",
            transaction.tx_index
        );
        ensure!(
            readonly_unsigned <= key_count - required,
            "slot {slot} tx {} readonly unsigned count exceeds unsigned keys",
            transaction.tx_index
        );
        let writable_signed_end = required - readonly_signed;
        let writable_unsigned_end = key_count - readonly_unsigned;

        self.projected.clear();
        if slot_has_loader_barrier {
            self.projected.push(ProjectedAccess {
                global_id: None,
                access: AccountAccess {
                    account_id: GLOBAL_BARRIER_ACCOUNT,
                    writable: transaction_is_loader,
                },
            });
        }
        for (index, pubkey) in transaction.account_keys.iter().copied().enumerate() {
            let writable =
                index < writable_signed_end || (index >= required && index < writable_unsigned_end);
            let (global_id, local_id) = self.indexer.resolve(pubkey)?;
            self.projected.push(ProjectedAccess {
                global_id: Some(global_id),
                access: AccountAccess {
                    account_id: local_id,
                    writable,
                },
            });
        }
        self.projected
            .sort_unstable_by_key(|projected| projected.access.account_id);

        let mut deduplicated = SmallVec::<[ProjectedAccess; 32]>::new();
        for projected in self.projected.iter().copied() {
            if let Some(previous) = deduplicated.last_mut()
                && previous.access.account_id == projected.access.account_id
            {
                previous.access.writable |= projected.access.writable;
                continue;
            }
            deduplicated.push(projected);
        }
        let start = self.flat_accesses.len();
        for projected in deduplicated {
            if let Some(global_id) = projected.global_id {
                self.indexer
                    .record_access(global_id, projected.access.writable)?;
            }
            self.flat_accesses.push(projected.access);
        }
        let end = self.flat_accesses.len();
        self.transaction_ranges.push(TransactionRange {
            canonical_index: transaction.tx_index,
            start,
            end,
            weight: self.cost_model.weight(transaction),
        });
        Ok(())
    }
}

fn is_loader_transaction(transaction: &CompactTransactionProbe) -> bool {
    transaction
        .instructions
        .iter()
        .any(|instruction| instruction.program_id == BPF_LOADER_PROGRAM_ID)
}

fn checked_increment(value: u64, label: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| anyhow!("{label} counter overflow"))
}

fn checked_add_usize(value: u64, addend: usize, label: &str) -> Result<u64> {
    value
        .checked_add(u64::try_from(addend).with_context(|| format!("{label} exceeds u64"))?)
        .ok_or_else(|| anyhow!("{label} counter overflow"))
}

fn fold_fingerprint<const N: usize>(mut fingerprint: u64, values: [u64; N]) -> u64 {
    for value in values {
        for byte in value.to_le_bytes() {
            fingerprint ^= u64::from(byte);
            fingerprint = fingerprint.wrapping_mul(FINGERPRINT_PRIME);
        }
    }
    fingerprint
}

fn lowercase_hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut rendered, "{byte:02x}").expect("writing to a String cannot fail");
    }
    rendered
}

fn parse_workers(value: &str) -> Result<Vec<usize>> {
    let mut workers = BTreeSet::new();
    for component in value.split(',') {
        ensure!(
            !component.is_empty(),
            "--workers contains an empty component"
        );
        let worker = component
            .parse::<usize>()
            .with_context(|| format!("invalid worker count {component:?}"))?;
        ensure!(worker > 0, "worker counts must be greater than zero");
        workers.insert(worker);
    }
    ensure!(!workers.is_empty(), "--workers must not be empty");
    Ok(workers.into_iter().collect())
}

fn percentile(values: &[u64], numerator: usize, denominator: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn print_distribution(label: &str, values: &[u64]) {
    println!(
        "distribution metric={} p50={} p90={} p99={} p999={} max={}",
        label,
        percentile(values, 50, 100),
        percentile(values, 90, 100),
        percentile(values, 99, 100),
        percentile(values, 999, 1000),
        values.iter().copied().max().unwrap_or(0),
    );
}

fn print_programs(program_counts: &HashMap<[u8; 32], u64>, limit: usize) {
    let mut programs = program_counts
        .iter()
        .map(|(program, count)| (*program, *count))
        .collect::<Vec<_>>();
    programs.sort_unstable_by_key(|(program, count)| Reverse((*count, *program)));
    for (rank, (program, instructions)) in programs.into_iter().take(limit).enumerate() {
        let builtin = matches!(
            program,
            SYSTEM_PROGRAM_ID
                | VOTE_PROGRAM_ID
                | STAKE_PROGRAM_ID
                | CONFIG_PROGRAM_ID
                | BPF_LOADER_PROGRAM_ID
        );
        println!(
            "hot_program rank={} pubkey={} instructions={} known_launch_builtin={}",
            rank + 1,
            bs58::encode(program).into_string(),
            instructions,
            builtin,
        );
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let workers = parse_workers(&args.workers)?;
    let selection = Selection::open(&args.generation, args.start_row, args.rows)?;
    let context = read_compact_generation_context(&args.generation)
        .with_context(|| format!("read generation context {}", args.generation.display()))?;
    let (indexer, registry_index_time) = RegistryIndexer::load(&args.generation)?;
    let mut analyzer = Analyzer::new(
        args.cost_model,
        args.progress_rows,
        args.max_raw_keys,
        &workers,
        selection.rows as usize,
        indexer,
    );

    println!("schema=blockzilla-compact-conflict-bench-v1");
    println!("input_format=blockzilla-compact-archive-v2");
    println!("car=false");
    println!("generation={}", args.generation.display());
    println!("epoch={}", context.epoch);
    println!("generation_id={}", context.generation_id);
    println!(
        "generation_digest={}",
        lowercase_hex(&context.binding.generation_digest)
    );
    println!(
        "selection_rows={}..{}",
        selection.start_row, selection.end_row
    );
    println!(
        "selection_slots={}..{:?}",
        selection.start_slot, selection.end_slot_exclusive
    );
    println!("cost_model={}", args.cost_model.label());
    println!("cost_scope=structural-not-wall-clock");
    println!("access_scope=all-static-message-keys-with-conservative-header-writability");
    println!(
        "workers={}",
        workers
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("registry_entries={}", analyzer.indexer.global_keys.len());
    println!(
        "registry_index_ms={:.3}",
        registry_index_time.as_secs_f64() * 1_000.0
    );

    let scan_started = Instant::now();
    let summary = visit_compact_generation_without_program_counts(
        &args.generation,
        selection.visit_config(),
        |event| {
            let CompactVisitEvent::Slot { slot, .. } = event else {
                return Ok(CompactVisitControl::Continue);
            };
            analyzer
                .analyze_slot(slot)
                .map_err(|error| CompactProbeError::Visitor(error.to_string()))?;
            Ok(CompactVisitControl::Continue)
        },
    )
    .with_context(|| format!("scan conflict graphs in {}", args.generation.display()))?;
    let scan_elapsed = scan_started.elapsed();

    ensure!(
        summary.slots_visited == selection.rows,
        "visitor/index row mismatch"
    );
    ensure!(
        summary.transactions_visited == selection.transactions,
        "visitor/index transaction mismatch"
    );
    ensure!(
        summary.compressed_bytes_visited == selection.compressed_bytes,
        "visitor/index compressed-byte mismatch"
    );
    ensure!(
        analyzer.aggregate.slots == selection.rows,
        "analyzer/index row mismatch"
    );
    ensure!(
        analyzer.aggregate.transactions == selection.transactions,
        "analyzer/index transaction mismatch"
    );

    let aggregate = &analyzer.aggregate;
    let seconds = scan_elapsed.as_secs_f64();
    let online_planner_time = aggregate.projection_time + aggregate.graph_time;
    let analysis_time = online_planner_time + aggregate.simulation_time;
    let tx_denominator = aggregate.transactions.max(1) as f64;
    println!(
        "scan slots={} transactions={} instructions={} compressed_bytes={} uncompressed_bytes={} wall_s={:.6} blocks_per_s={:.3} transactions_per_s={:.3} compressed_gb_per_s={:.6}",
        aggregate.slots,
        aggregate.transactions,
        aggregate.instructions,
        selection.compressed_bytes,
        selection.uncompressed_bytes,
        seconds,
        aggregate.slots as f64 / seconds,
        aggregate.transactions as f64 / seconds,
        selection.compressed_bytes as f64 / 1_000_000_000.0 / seconds,
    );
    println!(
        "outcomes succeeded={} failed={} unknown={} loader_barrier_transactions={} v0_transactions=0 unresolved_loaded_addresses=0",
        aggregate.succeeded_transactions,
        aggregate.failed_transactions,
        aggregate.unknown_transactions,
        aggregate.loader_barrier_transactions,
    );
    println!(
        "planner readonly_accesses={} writable_accesses={} dependency_edges={} raw_conflicts={} war_conflicts={} waw_conflicts={} total_weight={} critical_path_weight={} conservative_ideal_unlimited_speedup={:.6} max_finalized_plan_bytes={} metric_fingerprint={:016x}",
        aggregate.readonly_accesses,
        aggregate.writable_accesses,
        aggregate.dependency_edges,
        aggregate.raw_conflicts,
        aggregate.war_conflicts,
        aggregate.waw_conflicts,
        aggregate.total_weight,
        aggregate.critical_path_weight,
        aggregate.total_weight as f64 / aggregate.critical_path_weight.max(1) as f64,
        aggregate.max_plan_bytes,
        aggregate.metric_fingerprint,
    );
    println!(
        "planner_timing access_projection_s={:.6} graph_build_s={:.6} online_planner_s={:.6} online_planner_ns_per_tx={:.3} offline_simulation_s={:.6} total_analysis_s={:.6} total_analysis_percent_of_scan={:.3}",
        aggregate.projection_time.as_secs_f64(),
        aggregate.graph_time.as_secs_f64(),
        online_planner_time.as_secs_f64(),
        online_planner_time.as_secs_f64() * 1_000_000_000.0 / tx_denominator,
        aggregate.simulation_time.as_secs_f64(),
        analysis_time.as_secs_f64(),
        100.0 * analysis_time.as_secs_f64() / seconds,
    );
    println!(
        "peaks max_slot_transactions={} max_initial_ready={} max_level_width={} max_dependency_chain_transactions={}",
        aggregate.max_slot_transactions,
        aggregate.max_initial_ready,
        aggregate.max_level_width,
        aggregate.max_dependency_chain,
    );
    print_distribution("transactions_per_slot", &aggregate.slot_transactions);
    print_distribution("initial_ready", &aggregate.initial_ready);
    print_distribution("dependency_level_width", &aggregate.level_width);
    print_distribution("dependency_chain_transactions", &aggregate.dependency_chain);
    print_distribution("slot_parallelism_milli", &aggregate.parallelism_milli);
    for worker in &aggregate.workers {
        let makespan = worker.makespan.max(1) as f64;
        let speedup = aggregate.total_weight as f64 / makespan;
        let lower_bound_efficiency = worker.lower_bound as f64 / makespan;
        println!(
            "simulation workers={} makespan={} lower_bound={} speedup={:.6} utilization={:.6} lower_bound_efficiency={:.6} peak_running={} max_ready={} fingerprint={:016x}",
            worker.workers,
            worker.makespan,
            worker.lower_bound,
            speedup,
            speedup / worker.workers as f64,
            lower_bound_efficiency,
            worker.peak_running,
            worker.max_ready,
            worker.schedule_fingerprint,
        );
        print_distribution(
            &format!("max_ready_workers_{}", worker.workers),
            &worker.ready_width,
        );
    }
    println!(
        "registry raw_lookup_occurrences={} raw_unique_keys={} final_global_keys={}",
        analyzer.indexer.raw_lookup_count,
        analyzer.indexer.raw_ids.len(),
        analyzer.indexer.global_keys.len(),
    );
    for (rank, (id, pubkey, accesses, writes)) in analyzer
        .indexer
        .hottest_writable(args.top_accounts)
        .into_iter()
        .enumerate()
    {
        println!(
            "hot_account rank={} registry_id={} pubkey={} accesses={} writes={}",
            rank + 1,
            id,
            bs58::encode(pubkey).into_string(),
            accesses,
            writes,
        );
    }
    print_programs(&aggregate.program_counts, args.top_accounts);
    println!("result=PASS");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_parser_sorts_and_deduplicates() {
        assert_eq!(parse_workers("8,1,4,8").unwrap(), [1, 4, 8]);
        assert!(parse_workers("1,,2").is_err());
        assert!(parse_workers("0").is_err());
    }

    #[test]
    fn percentile_uses_nearest_rank_ceiling() {
        let values = [5, 1, 4, 2, 3];
        assert_eq!(percentile(&values, 50, 100), 3);
        assert_eq!(percentile(&values, 90, 100), 5);
        assert_eq!(percentile(&[1, 2], 50, 100), 1);
        assert_eq!(percentile(&[10, 8, 2, 6, 4, 9, 1, 3, 5, 7], 90, 100), 9);
        assert_eq!(percentile(&[7], 50, 100), 7);
        assert_eq!(percentile(&[2, 1], 100, 100), 2);
        assert_eq!(percentile(&[], 99, 100), 0);
    }
}

//! Per-slot transaction conflict planning and deterministic schedule simulation.
//!
//! The planner consumes canonical transaction order and dense, slot-local
//! account identifiers. It emits the minimum ordering constraints required for
//! declared account accesses:
//!
//! - read-after-write (RAW): the reader waits for the preceding writer;
//! - write-after-read (WAR): the writer waits for every reader since the
//!   preceding writer;
//! - write-after-write (WAW): the writer waits for the preceding writer;
//! - read-after-read: no dependency.
//!
//! Duplicate account metas are folded and writable access dominates readonly
//! access within a transaction. Planning uses flat vectors and dense account
//! state; it performs no per-account or per-transaction heap allocation. The
//! finalized graph uses compressed predecessor and successor arrays.

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    error::Error,
    fmt,
};

const NONE: u32 = u32::MAX;
const SCHEDULE_FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const SCHEDULE_FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

/// One account access in a transaction's canonical account list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountAccess {
    pub account_id: u32,
    pub writable: bool,
}

impl AccountAccess {
    pub const fn readonly(account_id: u32) -> Self {
        Self {
            account_id,
            writable: false,
        }
    }

    pub const fn writable(account_id: u32) -> Self {
        Self {
            account_id,
            writable: true,
        }
    }
}

/// Borrowed input for one transaction in canonical slot order.
#[derive(Debug, Clone, Copy)]
pub struct TransactionPlanInput<'a> {
    /// Original transaction position. Values must be strictly increasing.
    pub canonical_index: u32,
    pub accesses: &'a [AccountAccess],
    /// Positive execution-cost estimate in arbitrary, consistent units.
    pub weight: u64,
}

/// Expected slot sizes used to reserve the planner's flat buffers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConflictPlannerCapacity {
    pub transactions: usize,
    pub readonly_accesses: usize,
    pub dependency_edges: usize,
}

/// Failures caused by malformed inputs or an unrepresentable simulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictPlanError {
    AccountOutOfRange {
        canonical_index: u32,
        account_id: u32,
        account_count: usize,
    },
    CanonicalIndexNotIncreasing {
        previous: u32,
        current: u32,
    },
    ZeroWeight {
        canonical_index: u32,
    },
    TooManyTransactions,
    TooManyReadonlyAccesses,
    TotalWeightOverflow {
        canonical_index: u32,
    },
    WorkerCountZero,
    FinishTimeOverflow,
}

impl fmt::Display for ConflictPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountOutOfRange {
                canonical_index,
                account_id,
                account_count,
            } => write!(
                formatter,
                "transaction {canonical_index} references account {account_id}, but the dense account table has {account_count} entries"
            ),
            Self::CanonicalIndexNotIncreasing { previous, current } => write!(
                formatter,
                "canonical transaction indices must increase: {current} follows {previous}"
            ),
            Self::ZeroWeight { canonical_index } => {
                write!(formatter, "transaction {canonical_index} has zero weight")
            }
            Self::TooManyTransactions => {
                formatter.write_str("a slot cannot contain u32::MAX transactions")
            }
            Self::TooManyReadonlyAccesses => {
                formatter.write_str("the readonly-access frontier exceeds u32 indexing")
            }
            Self::TotalWeightOverflow { canonical_index } => write!(
                formatter,
                "total transaction weight overflows u64 at transaction {canonical_index}"
            ),
            Self::WorkerCountZero => formatter.write_str("worker count must be non-zero"),
            Self::FinishTimeOverflow => {
                formatter.write_str("simulated completion time overflows u64")
            }
        }
    }
}

impl Error for ConflictPlanError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReaderNode {
    transaction: u32,
    next: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransactionRecord {
    canonical_index: u32,
    weight: u64,
}

/// Streaming builder for one slot's dependency graph.
#[derive(Debug)]
pub struct ConflictPlanner {
    account_count: usize,
    transactions: Vec<TransactionRecord>,
    predecessor_offsets: Vec<usize>,
    predecessors: Vec<u32>,
    last_writer: Vec<u32>,
    reader_head: Vec<u32>,
    reader_nodes: Vec<ReaderNode>,
    seen_transaction: Vec<u32>,
    dependency_seen: Vec<u32>,
    scratch_dependencies: Vec<u32>,
    last_canonical_index: Option<u32>,
    total_weight: u64,
    readonly_accesses: usize,
    writable_accesses: usize,
    raw_conflicts: usize,
    war_conflicts: usize,
    waw_conflicts: usize,
}

impl ConflictPlanner {
    pub fn new(account_count: usize) -> Self {
        Self::with_capacity(account_count, ConflictPlannerCapacity::default())
    }

    pub fn with_capacity(account_count: usize, capacity: ConflictPlannerCapacity) -> Self {
        let mut predecessor_offsets = Vec::with_capacity(capacity.transactions.saturating_add(1));
        predecessor_offsets.push(0);
        Self {
            account_count,
            transactions: Vec::with_capacity(capacity.transactions),
            predecessor_offsets,
            predecessors: Vec::with_capacity(capacity.dependency_edges),
            last_writer: vec![NONE; account_count],
            reader_head: vec![NONE; account_count],
            reader_nodes: Vec::with_capacity(capacity.readonly_accesses),
            seen_transaction: vec![NONE; account_count],
            dependency_seen: Vec::with_capacity(capacity.transactions),
            scratch_dependencies: Vec::new(),
            last_canonical_index: None,
            total_weight: 0,
            readonly_accesses: 0,
            writable_accesses: 0,
            raw_conflicts: 0,
            war_conflicts: 0,
            waw_conflicts: 0,
        }
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Add one transaction. Errors are detected before any planner state is
    /// changed, so callers may recover from a rejected input.
    pub fn push_transaction(
        &mut self,
        canonical_index: u32,
        accesses: &[AccountAccess],
        weight: u64,
    ) -> Result<u32, ConflictPlanError> {
        let transaction = u32::try_from(self.transactions.len())
            .ok()
            .filter(|transaction| *transaction != NONE)
            .ok_or(ConflictPlanError::TooManyTransactions)?;

        if let Some(previous) = self.last_canonical_index {
            if canonical_index <= previous {
                return Err(ConflictPlanError::CanonicalIndexNotIncreasing {
                    previous,
                    current: canonical_index,
                });
            }
        }
        if weight == 0 {
            return Err(ConflictPlanError::ZeroWeight { canonical_index });
        }
        let new_total_weight = self
            .total_weight
            .checked_add(weight)
            .ok_or(ConflictPlanError::TotalWeightOverflow { canonical_index })?;
        if self
            .reader_nodes
            .len()
            .checked_add(accesses.len())
            .is_none_or(|length| length >= NONE as usize)
        {
            return Err(ConflictPlanError::TooManyReadonlyAccesses);
        }
        for access in accesses {
            if access.account_id as usize >= self.account_count {
                return Err(ConflictPlanError::AccountOutOfRange {
                    canonical_index,
                    account_id: access.account_id,
                    account_count: self.account_count,
                });
            }
        }

        self.dependency_seen.push(NONE);
        self.scratch_dependencies.clear();

        // Process writes first so writable dominates readonly even when a
        // malformed/legacy account list contains both privileges.
        for access in accesses.iter().filter(|access| access.writable) {
            let account = access.account_id as usize;
            if self.seen_transaction[account] == transaction {
                continue;
            }
            self.seen_transaction[account] = transaction;
            self.writable_accesses += 1;

            let mut reader = self.reader_head[account];
            self.reader_head[account] = NONE;
            if reader == NONE {
                let preceding_writer = self.last_writer[account];
                if preceding_writer != NONE {
                    self.waw_conflicts += 1;
                    self.record_dependency(transaction, preceding_writer);
                }
            } else {
                // Every reader already depends on the preceding writer. The
                // WAR edges therefore preserve WAW transitively without a
                // redundant preceding-writer edge.
                while reader != NONE {
                    let node = self.reader_nodes[reader as usize];
                    self.war_conflicts += 1;
                    self.record_dependency(transaction, node.transaction);
                    reader = node.next;
                }
            }
            self.last_writer[account] = transaction;
        }

        for access in accesses.iter().filter(|access| !access.writable) {
            let account = access.account_id as usize;
            if self.seen_transaction[account] == transaction {
                continue;
            }
            self.seen_transaction[account] = transaction;
            self.readonly_accesses += 1;

            let preceding_writer = self.last_writer[account];
            if preceding_writer != NONE {
                self.raw_conflicts += 1;
                self.record_dependency(transaction, preceding_writer);
            }

            let reader = self.reader_nodes.len() as u32;
            self.reader_nodes.push(ReaderNode {
                transaction,
                next: self.reader_head[account],
            });
            self.reader_head[account] = reader;
        }

        // Stable predecessor ordering makes the plan independent of account
        // list order and simplifies parity/debug output.
        self.scratch_dependencies.sort_unstable();
        self.predecessors
            .extend_from_slice(&self.scratch_dependencies);
        self.predecessor_offsets.push(self.predecessors.len());
        self.transactions.push(TransactionRecord {
            canonical_index,
            weight,
        });
        self.last_canonical_index = Some(canonical_index);
        self.total_weight = new_total_weight;
        Ok(transaction)
    }

    fn record_dependency(&mut self, transaction: u32, predecessor: u32) {
        debug_assert!(predecessor < transaction);
        let marker = &mut self.dependency_seen[predecessor as usize];
        if *marker != transaction {
            *marker = transaction;
            self.scratch_dependencies.push(predecessor);
        }
    }

    pub fn finish(self) -> Result<ConflictPlan, ConflictPlanError> {
        let transaction_count = self.transactions.len();
        let mut successor_counts = vec![0usize; transaction_count];
        for predecessor in &self.predecessors {
            successor_counts[*predecessor as usize] += 1;
        }

        let mut successor_offsets = Vec::with_capacity(transaction_count.saturating_add(1));
        successor_offsets.push(0usize);
        for count in &successor_counts {
            let next = successor_offsets
                .last()
                .copied()
                .expect("the initial successor offset exists")
                .checked_add(*count)
                .ok_or(ConflictPlanError::FinishTimeOverflow)?;
            successor_offsets.push(next);
        }
        let mut successors = vec![0u32; self.predecessors.len()];
        let mut successor_cursors = successor_offsets[..transaction_count].to_vec();
        for transaction in 0..transaction_count {
            let start = self.predecessor_offsets[transaction];
            let end = self.predecessor_offsets[transaction + 1];
            for predecessor in &self.predecessors[start..end] {
                let predecessor = *predecessor as usize;
                let cursor = successor_cursors[predecessor];
                successors[cursor] = transaction as u32;
                successor_cursors[predecessor] += 1;
            }
        }

        let mut root_transactions = 0usize;
        let mut leaf_transactions = 0usize;
        let mut max_predecessors = 0usize;
        let mut max_successors = 0usize;
        let mut longest_dependency_chain_transactions = 0usize;
        let mut critical_path_weight = 0u64;
        let mut levels = vec![0usize; transaction_count];
        let mut chain_lengths = vec![0usize; transaction_count];
        let mut earliest_finishes = vec![0u64; transaction_count];

        for transaction in 0..transaction_count {
            let predecessor_start = self.predecessor_offsets[transaction];
            let predecessor_end = self.predecessor_offsets[transaction + 1];
            let predecessor_slice = &self.predecessors[predecessor_start..predecessor_end];
            if predecessor_slice.is_empty() {
                root_transactions += 1;
            }
            max_predecessors = max_predecessors.max(predecessor_slice.len());

            let mut dependency_level = 0usize;
            let mut chain_length = 1usize;
            let mut earliest_start = 0u64;
            for predecessor in predecessor_slice {
                let predecessor = *predecessor as usize;
                dependency_level = dependency_level.max(levels[predecessor].saturating_add(1));
                chain_length = chain_length.max(chain_lengths[predecessor].saturating_add(1));
                earliest_start = earliest_start.max(earliest_finishes[predecessor]);
            }
            levels[transaction] = dependency_level;
            chain_lengths[transaction] = chain_length;
            longest_dependency_chain_transactions =
                longest_dependency_chain_transactions.max(chain_length);
            let earliest_finish = earliest_start
                .checked_add(self.transactions[transaction].weight)
                .ok_or(ConflictPlanError::FinishTimeOverflow)?;
            earliest_finishes[transaction] = earliest_finish;
            critical_path_weight = critical_path_weight.max(earliest_finish);

            let successor_count =
                successor_offsets[transaction + 1] - successor_offsets[transaction];
            if successor_count == 0 {
                leaf_transactions += 1;
            }
            max_successors = max_successors.max(successor_count);
        }

        let mut level_counts = vec![0usize; longest_dependency_chain_transactions];
        for level in levels {
            level_counts[level] += 1;
        }
        let max_dependency_level_width = level_counts.into_iter().max().unwrap_or(0);

        let mut bottom_path_weights = vec![0u64; transaction_count];
        for transaction in (0..transaction_count).rev() {
            let successor_start = successor_offsets[transaction];
            let successor_end = successor_offsets[transaction + 1];
            let successor_tail = successors[successor_start..successor_end]
                .iter()
                .map(|successor| bottom_path_weights[*successor as usize])
                .max()
                .unwrap_or(0);
            bottom_path_weights[transaction] = self.transactions[transaction]
                .weight
                .checked_add(successor_tail)
                .ok_or(ConflictPlanError::FinishTimeOverflow)?;
        }

        let estimated_bytes = self
            .transactions
            .len()
            .saturating_mul(std::mem::size_of::<TransactionRecord>())
            .saturating_add(
                self.predecessor_offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(
                self.predecessors
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
            .saturating_add(
                successor_offsets
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
            .saturating_add(successors.len().saturating_mul(std::mem::size_of::<u32>()))
            .saturating_add(
                bottom_path_weights
                    .len()
                    .saturating_mul(std::mem::size_of::<u64>()),
            );
        let metrics = ConflictMetrics {
            account_count: self.account_count,
            tx_count: transaction_count,
            readonly_accesses: self.readonly_accesses,
            writable_accesses: self.writable_accesses,
            edge_count: self.predecessors.len(),
            raw_conflicts: self.raw_conflicts,
            war_conflicts: self.war_conflicts,
            waw_conflicts: self.waw_conflicts,
            initial_ready: root_transactions,
            root_transactions,
            leaf_transactions,
            max_predecessors,
            max_successors,
            max_dependency_level_width,
            longest_dependency_chain_transactions,
            total_weight: self.total_weight,
            critical_path_weight,
            estimated_bytes,
        };

        Ok(ConflictPlan {
            transactions: self.transactions,
            predecessor_offsets: self.predecessor_offsets,
            predecessors: self.predecessors,
            successor_offsets,
            successors,
            bottom_path_weights,
            metrics,
        })
    }
}

/// Construct a complete per-slot plan from canonical transaction inputs.
pub fn plan_slot(
    account_count: usize,
    transactions: &[TransactionPlanInput<'_>],
) -> Result<ConflictPlan, ConflictPlanError> {
    let readonly_accesses = transactions
        .iter()
        .map(|transaction| {
            transaction
                .accesses
                .iter()
                .filter(|access| !access.writable)
                .count()
        })
        .sum();
    let mut planner = ConflictPlanner::with_capacity(
        account_count,
        ConflictPlannerCapacity {
            transactions: transactions.len(),
            readonly_accesses,
            dependency_edges: transactions.len(),
        },
    );
    for transaction in transactions {
        planner.push_transaction(
            transaction.canonical_index,
            transaction.accesses,
            transaction.weight,
        )?;
    }
    planner.finish()
}

/// Aggregate shape and work metrics for a finalized slot graph.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConflictMetrics {
    pub account_count: usize,
    pub tx_count: usize,
    /// Unique readonly account accesses after folding duplicate metas.
    pub readonly_accesses: usize,
    /// Unique writable account accesses after folding duplicate metas.
    pub writable_accesses: usize,
    /// Unique transaction-to-transaction edges. Multiple account conflicts may
    /// collapse into one edge.
    pub edge_count: usize,
    /// Emitted per-account frontier constraints before transaction-pair
    /// deduplication; these may exceed dependency edges. A WAW bridged by one
    /// or more RAW/WAR paths is deliberately not counted again.
    pub raw_conflicts: usize,
    pub war_conflicts: usize,
    pub waw_conflicts: usize,
    /// Transactions ready before any execution begins.
    pub initial_ready: usize,
    pub root_transactions: usize,
    pub leaf_transactions: usize,
    pub max_predecessors: usize,
    pub max_successors: usize,
    /// Largest same-depth wave. This is a useful breadth indicator, not the
    /// exact maximum antichain.
    pub max_dependency_level_width: usize,
    pub longest_dependency_chain_transactions: usize,
    pub total_weight: u64,
    pub critical_path_weight: u64,
    /// Logical bytes occupied by the finalized plan's flat arrays. This
    /// excludes `Vec` headers and allocator spare capacity.
    pub estimated_bytes: usize,
}

impl ConflictMetrics {
    /// Work divided by weighted critical path: the idealized unlimited-worker
    /// speedup ceiling before scheduler and account-store overhead.
    pub fn weighted_parallelism(&self) -> f64 {
        if self.critical_path_weight == 0 {
            0.0
        } else {
            self.total_weight as f64 / self.critical_path_weight as f64
        }
    }
}

/// Compact immutable dependency plan. Internal transaction indices correspond
/// to input order; canonical indices remain available for logs and tie breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictPlan {
    transactions: Vec<TransactionRecord>,
    predecessor_offsets: Vec<usize>,
    predecessors: Vec<u32>,
    successor_offsets: Vec<usize>,
    successors: Vec<u32>,
    bottom_path_weights: Vec<u64>,
    metrics: ConflictMetrics,
}

impl ConflictPlan {
    pub fn metrics(&self) -> &ConflictMetrics {
        &self.metrics
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    pub fn tx_count(&self) -> usize {
        self.metrics.tx_count
    }

    pub fn total_weight(&self) -> u64 {
        self.metrics.total_weight
    }

    pub fn edge_count(&self) -> usize {
        self.metrics.edge_count
    }

    pub fn critical_path_weight(&self) -> u64 {
        self.metrics.critical_path_weight
    }

    pub fn estimated_bytes(&self) -> usize {
        self.metrics.estimated_bytes
    }

    pub fn canonical_index(&self, transaction: u32) -> Option<u32> {
        self.transactions
            .get(transaction as usize)
            .map(|record| record.canonical_index)
    }

    pub fn weight(&self, transaction: u32) -> Option<u64> {
        self.transactions
            .get(transaction as usize)
            .map(|record| record.weight)
    }

    pub fn predecessors(&self, transaction: u32) -> Option<&[u32]> {
        let transaction = transaction as usize;
        let start = *self.predecessor_offsets.get(transaction)?;
        let end = *self.predecessor_offsets.get(transaction + 1)?;
        Some(&self.predecessors[start..end])
    }

    pub fn successors(&self, transaction: u32) -> Option<&[u32]> {
        let transaction = transaction as usize;
        let start = *self.successor_offsets.get(transaction)?;
        let end = *self.successor_offsets.get(transaction + 1)?;
        Some(&self.successors[start..end])
    }

    pub fn simulate(&self, workers: usize) -> Result<ScheduleSimulation, ConflictPlanError> {
        simulate(self, workers)
    }
}

/// Aggregate output of deterministic, critical-path-first list scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleSimulation {
    pub workers: usize,
    pub transactions: usize,
    pub makespan: u64,
    pub total_weight: u64,
    pub critical_path_weight: u64,
    pub peak_running: usize,
    /// Greatest ready-queue size observed immediately before a dispatch.
    pub max_ready: usize,
    pub min_worker_busy_weight: u64,
    pub max_worker_busy_weight: u64,
    /// Stable FNV-1a hash of `(transaction, worker, start, finish)` dispatches.
    pub schedule_fingerprint: u64,
}

impl ScheduleSimulation {
    pub fn speedup(&self) -> f64 {
        if self.makespan == 0 {
            0.0
        } else {
            self.total_weight as f64 / self.makespan as f64
        }
    }

    pub fn utilization(&self) -> f64 {
        if self.makespan == 0 {
            return 0.0;
        }
        let capacity = self.makespan as f64 * self.workers as f64;
        self.total_weight as f64 / capacity
    }

    pub fn worker_utilization(&self) -> f64 {
        self.utilization()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyTransaction {
    transaction: u32,
    canonical_index: u32,
    weight: u64,
    bottom_path_weight: u64,
}

impl Ord for ReadyTransaction {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bottom_path_weight
            .cmp(&other.bottom_path_weight)
            .then_with(|| self.weight.cmp(&other.weight))
            // BinaryHeap returns the greatest item. Reverse these comparisons
            // so lower canonical/internal indices win exact ties.
            .then_with(|| other.canonical_index.cmp(&self.canonical_index))
            .then_with(|| other.transaction.cmp(&self.transaction))
    }
}

impl PartialOrd for ReadyTransaction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Completion {
    finish: u64,
    worker: usize,
    transaction: u32,
}

/// Simulate deterministic list scheduling. Ready transactions with the
/// greatest remaining critical-path weight are dispatched first, followed by
/// greater local weight and then lower canonical index.
pub fn simulate(
    plan: &ConflictPlan,
    workers: usize,
) -> Result<ScheduleSimulation, ConflictPlanError> {
    if workers == 0 {
        return Err(ConflictPlanError::WorkerCountZero);
    }

    let transaction_count = plan.transactions.len();
    if transaction_count == 0 {
        return Ok(ScheduleSimulation {
            workers,
            transactions: 0,
            makespan: 0,
            total_weight: 0,
            critical_path_weight: 0,
            peak_running: 0,
            max_ready: 0,
            min_worker_busy_weight: 0,
            max_worker_busy_weight: 0,
            schedule_fingerprint: SCHEDULE_FINGERPRINT_OFFSET,
        });
    }

    let active_workers = workers.min(transaction_count);
    let mut indegrees = Vec::with_capacity(transaction_count);
    let mut ready = BinaryHeap::new();
    for transaction in 0..transaction_count {
        let indegree =
            plan.predecessor_offsets[transaction + 1] - plan.predecessor_offsets[transaction];
        indegrees.push(indegree);
        if indegree == 0 {
            ready.push(ready_transaction(plan, transaction as u32));
        }
    }

    let mut available_workers = (0..active_workers).map(Reverse).collect::<BinaryHeap<_>>();
    let mut running = BinaryHeap::<Reverse<Completion>>::new();
    let mut worker_busy_weights = vec![0u64; active_workers];
    let mut now = 0u64;
    let mut completed = 0usize;
    let mut peak_running = 0usize;
    let mut max_ready = ready.len();
    let mut schedule_fingerprint = SCHEDULE_FINGERPRINT_OFFSET;

    while completed < transaction_count {
        max_ready = max_ready.max(ready.len());
        while !ready.is_empty() && !available_workers.is_empty() {
            let ready_transaction = ready.pop().expect("ready was checked as non-empty");
            let Reverse(worker) = available_workers
                .pop()
                .expect("available workers were checked as non-empty");
            let record = plan.transactions[ready_transaction.transaction as usize];
            let finish = now
                .checked_add(record.weight)
                .ok_or(ConflictPlanError::FinishTimeOverflow)?;
            worker_busy_weights[worker] = worker_busy_weights[worker]
                .checked_add(record.weight)
                .ok_or(ConflictPlanError::FinishTimeOverflow)?;
            schedule_fingerprint = fingerprint_dispatch(
                schedule_fingerprint,
                ready_transaction.transaction,
                worker,
                now,
                finish,
            );
            running.push(Reverse(Completion {
                finish,
                worker,
                transaction: ready_transaction.transaction,
            }));
        }
        peak_running = peak_running.max(running.len());

        let Reverse(first_completion) = running
            .pop()
            .expect("an acyclic conflict graph always has ready work");
        now = first_completion.finish;
        complete_transaction(
            plan,
            first_completion,
            &mut indegrees,
            &mut ready,
            &mut available_workers,
        );
        completed += 1;

        // Release all work that finished at the same timestamp before making
        // another dispatch decision.
        while running
            .peek()
            .is_some_and(|Reverse(completion)| completion.finish == now)
        {
            let Reverse(completion) = running
                .pop()
                .expect("peek observed a simultaneous completion");
            complete_transaction(
                plan,
                completion,
                &mut indegrees,
                &mut ready,
                &mut available_workers,
            );
            completed += 1;
        }
        max_ready = max_ready.max(ready.len());
    }

    let min_worker_busy_weight = if workers > active_workers {
        0
    } else {
        worker_busy_weights.iter().copied().min().unwrap_or(0)
    };
    let max_worker_busy_weight = worker_busy_weights.iter().copied().max().unwrap_or(0);
    Ok(ScheduleSimulation {
        workers,
        transactions: transaction_count,
        makespan: now,
        total_weight: plan.metrics.total_weight,
        critical_path_weight: plan.metrics.critical_path_weight,
        peak_running,
        max_ready,
        min_worker_busy_weight,
        max_worker_busy_weight,
        schedule_fingerprint,
    })
}

fn ready_transaction(plan: &ConflictPlan, transaction: u32) -> ReadyTransaction {
    let record = plan.transactions[transaction as usize];
    ReadyTransaction {
        transaction,
        canonical_index: record.canonical_index,
        weight: record.weight,
        bottom_path_weight: plan.bottom_path_weights[transaction as usize],
    }
}

fn complete_transaction(
    plan: &ConflictPlan,
    completion: Completion,
    indegrees: &mut [usize],
    ready: &mut BinaryHeap<ReadyTransaction>,
    available_workers: &mut BinaryHeap<Reverse<usize>>,
) {
    available_workers.push(Reverse(completion.worker));
    let transaction = completion.transaction as usize;
    for successor in &plan.successors
        [plan.successor_offsets[transaction]..plan.successor_offsets[transaction + 1]]
    {
        let indegree = &mut indegrees[*successor as usize];
        debug_assert!(*indegree > 0);
        *indegree -= 1;
        if *indegree == 0 {
            ready.push(ready_transaction(plan, *successor));
        }
    }
}

fn fingerprint_dispatch(
    mut fingerprint: u64,
    transaction: u32,
    worker: usize,
    start: u64,
    finish: u64,
) -> u64 {
    for value in [transaction as u64, worker as u64, start, finish] {
        for byte in value.to_le_bytes() {
            fingerprint ^= byte as u64;
            fingerprint = fingerprint.wrapping_mul(SCHEDULE_FINGERPRINT_PRIME);
        }
    }
    fingerprint
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(account_id: u32, writable: bool) -> AccountAccess {
        AccountAccess {
            account_id,
            writable,
        }
    }

    #[test]
    fn read_after_read_has_no_edge() {
        let first = [AccountAccess::readonly(0)];
        let second = [AccountAccess::readonly(0)];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 10,
                    accesses: &first,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 11,
                    accesses: &second,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(0), Some([].as_slice()));
        assert_eq!(plan.predecessors(1), Some([].as_slice()));
        assert_eq!(plan.metrics().edge_count, 0);
        assert_eq!(plan.metrics().raw_conflicts, 0);
        assert_eq!(plan.metrics().war_conflicts, 0);
        assert_eq!(plan.metrics().waw_conflicts, 0);
    }

    #[test]
    fn raw_war_and_waw_edges_are_preserved() {
        let write = [AccountAccess::writable(0)];
        let read = [AccountAccess::readonly(0)];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &write,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &read,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 2,
                    accesses: &read,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 3,
                    accesses: &write,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 4,
                    accesses: &write,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(0), Some([].as_slice()));
        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.predecessors(2), Some([0].as_slice()));
        // The readers already depend on writer 0, so their WAR edges preserve
        // 0->3 transitively without a redundant direct WAW edge.
        assert_eq!(plan.predecessors(3), Some([1, 2].as_slice()));
        assert_eq!(plan.predecessors(4), Some([3].as_slice()));
        assert_eq!(plan.metrics().edge_count, 5);
        assert_eq!(plan.metrics().raw_conflicts, 2);
        assert_eq!(plan.metrics().war_conflicts, 2);
        assert_eq!(plan.metrics().waw_conflicts, 1);
    }

    #[test]
    fn writer_clears_the_prior_reader_frontier() {
        let read = [AccountAccess::readonly(0)];
        let write = [AccountAccess::writable(0)];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &read,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &write,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 2,
                    accesses: &write,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.predecessors(2), Some([1].as_slice()));
    }

    #[test]
    fn duplicate_metas_are_folded_and_writable_wins() {
        let first = [
            access(0, false),
            access(0, true),
            access(0, false),
            access(0, true),
        ];
        let second = [access(0, false), access(0, false)];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 5,
                    accesses: &first,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 6,
                    accesses: &second,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(0), Some([].as_slice()));
        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.metrics().writable_accesses, 1);
        assert_eq!(plan.metrics().readonly_accesses, 1);
        assert_eq!(plan.metrics().raw_conflicts, 1);
    }

    #[test]
    fn conflicts_on_multiple_accounts_collapse_to_one_edge() {
        let write_both = [AccountAccess::writable(0), AccountAccess::writable(1)];
        let read_both = [AccountAccess::readonly(1), AccountAccess::readonly(0)];
        let plan = plan_slot(
            2,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &write_both,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &read_both,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.successors(0), Some([1].as_slice()));
        assert_eq!(plan.metrics().edge_count, 1);
        assert_eq!(plan.metrics().raw_conflicts, 2);
    }

    #[test]
    fn duplicate_metas_fold_with_writable_dominance_across_frontiers() {
        let read = [AccountAccess::readonly(0)];
        let duplicate_mixed = [
            AccountAccess::readonly(0),
            AccountAccess::writable(0),
            AccountAccess::writable(0),
        ];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &read,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &duplicate_mixed,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 2,
                    accesses: &read,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 3,
                    accesses: &duplicate_mixed,
                    weight: 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(0), Some([].as_slice()));
        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.predecessors(2), Some([1].as_slice()));
        assert_eq!(plan.predecessors(3), Some([2].as_slice()));
        assert_eq!(plan.metrics().readonly_accesses, 2);
        assert_eq!(plan.metrics().writable_accesses, 2);
        assert_eq!(plan.metrics().raw_conflicts, 1);
        assert_eq!(plan.metrics().war_conflicts, 2);
        assert_eq!(plan.metrics().waw_conflicts, 0);
        assert_eq!(plan.metrics().edge_count, 3);
    }

    #[test]
    fn graph_metrics_capture_weighted_fork_and_join() {
        let tx0 = [AccountAccess::writable(0)];
        let tx1 = [AccountAccess::readonly(0), AccountAccess::writable(1)];
        let tx2 = [AccountAccess::readonly(0), AccountAccess::writable(2)];
        let tx3 = [AccountAccess::readonly(1), AccountAccess::readonly(2)];
        let plan = plan_slot(
            3,
            &[
                TransactionPlanInput {
                    canonical_index: 10,
                    accesses: &tx0,
                    weight: 2,
                },
                TransactionPlanInput {
                    canonical_index: 20,
                    accesses: &tx1,
                    weight: 5,
                },
                TransactionPlanInput {
                    canonical_index: 30,
                    accesses: &tx2,
                    weight: 3,
                },
                TransactionPlanInput {
                    canonical_index: 40,
                    accesses: &tx3,
                    weight: 7,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.predecessors(0), Some([].as_slice()));
        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.predecessors(2), Some([0].as_slice()));
        assert_eq!(plan.predecessors(3), Some([1, 2].as_slice()));
        assert_eq!(plan.successors(0), Some([1, 2].as_slice()));
        assert_eq!(plan.canonical_index(2), Some(30));
        assert_eq!(plan.weight(2), Some(3));

        let metrics = plan.metrics();
        assert_eq!(metrics.tx_count, 4);
        assert_eq!(metrics.root_transactions, 1);
        assert_eq!(metrics.leaf_transactions, 1);
        assert_eq!(metrics.max_predecessors, 2);
        assert_eq!(metrics.max_successors, 2);
        assert_eq!(metrics.max_dependency_level_width, 2);
        assert_eq!(metrics.longest_dependency_chain_transactions, 3);
        assert_eq!(metrics.total_weight, 17);
        assert_eq!(metrics.critical_path_weight, 14);
        assert!((metrics.weighted_parallelism() - 17.0 / 14.0).abs() < f64::EPSILON);
        assert_eq!(plan.bottom_path_weights, [14, 12, 10, 7]);
    }

    #[test]
    fn bottom_path_priority_accepts_a_valid_u64_max_chain() {
        let first = [AccountAccess::writable(0)];
        let second = [AccountAccess::readonly(0)];
        let plan = plan_slot(
            1,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &first,
                    weight: 1,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &second,
                    weight: u64::MAX - 1,
                },
            ],
        )
        .unwrap();

        assert_eq!(plan.metrics().critical_path_weight, u64::MAX);
        assert_eq!(plan.bottom_path_weights, [u64::MAX, u64::MAX - 1]);
        assert_eq!(plan.simulate(1).unwrap().makespan, u64::MAX);
    }

    #[test]
    fn deterministic_list_simulation_reaches_expected_makespans() {
        let tx0 = [AccountAccess::writable(0)];
        let tx1 = [AccountAccess::readonly(0), AccountAccess::writable(1)];
        let tx2 = [AccountAccess::readonly(0), AccountAccess::writable(2)];
        let tx3 = [AccountAccess::readonly(1), AccountAccess::readonly(2)];
        let plan = plan_slot(
            3,
            &[
                TransactionPlanInput {
                    canonical_index: 10,
                    accesses: &tx0,
                    weight: 2,
                },
                TransactionPlanInput {
                    canonical_index: 20,
                    accesses: &tx1,
                    weight: 5,
                },
                TransactionPlanInput {
                    canonical_index: 30,
                    accesses: &tx2,
                    weight: 3,
                },
                TransactionPlanInput {
                    canonical_index: 40,
                    accesses: &tx3,
                    weight: 7,
                },
            ],
        )
        .unwrap();

        let serial = simulate(&plan, 1).unwrap();
        assert_eq!(serial.makespan, 17);
        assert_eq!(serial.peak_running, 1);
        assert_eq!(serial.speedup(), 1.0);

        let parallel = simulate(&plan, 2).unwrap();
        assert_eq!(parallel.makespan, 14);
        assert_eq!(parallel.peak_running, 2);
        assert_eq!(parallel.total_weight, 17);
        assert_eq!(parallel.critical_path_weight, 14);
        assert_eq!(parallel.min_worker_busy_weight, 3);
        assert_eq!(parallel.max_worker_busy_weight, 14);
        assert_eq!(parallel.max_ready, 2);
        assert_eq!(parallel, plan.simulate(2).unwrap());
        assert!((parallel.speedup() - 17.0 / 14.0).abs() < f64::EPSILON);
        assert!((parallel.worker_utilization() - 17.0 / 28.0).abs() < f64::EPSILON);

        let excess_workers = simulate(&plan, 16).unwrap();
        assert_eq!(excess_workers.makespan, 14);
        assert_eq!(excess_workers.min_worker_busy_weight, 0);
    }

    #[test]
    fn simultaneous_completions_unlock_join_before_dispatch() {
        let tx0 = [AccountAccess::writable(0)];
        let tx1 = [AccountAccess::writable(1)];
        let tx2 = [AccountAccess::readonly(0), AccountAccess::readonly(1)];
        let plan = plan_slot(
            2,
            &[
                TransactionPlanInput {
                    canonical_index: 0,
                    accesses: &tx0,
                    weight: 3,
                },
                TransactionPlanInput {
                    canonical_index: 1,
                    accesses: &tx1,
                    weight: 3,
                },
                TransactionPlanInput {
                    canonical_index: 2,
                    accesses: &tx2,
                    weight: 4,
                },
            ],
        )
        .unwrap();

        let simulation = simulate(&plan, 2).unwrap();
        assert_eq!(simulation.makespan, 7);
        assert_eq!(simulation.peak_running, 2);
    }

    #[test]
    fn empty_slot_is_well_defined() {
        let plan = plan_slot(12, &[]).unwrap();
        assert_eq!(plan.metrics().tx_count, 0);
        assert_eq!(plan.metrics().critical_path_weight, 0);
        assert_eq!(plan.metrics().weighted_parallelism(), 0.0);

        let simulation = simulate(&plan, 4).unwrap();
        assert_eq!(simulation.makespan, 0);
        assert_eq!(simulation.worker_utilization(), 0.0);
        assert_eq!(simulation.schedule_fingerprint, SCHEDULE_FINGERPRINT_OFFSET);
    }

    #[test]
    fn malformed_push_does_not_mutate_streaming_planner() {
        let mut planner = ConflictPlanner::new(1);
        planner
            .push_transaction(7, &[AccountAccess::writable(0)], 2)
            .unwrap();
        let error = planner
            .push_transaction(8, &[AccountAccess::readonly(1)], 1)
            .unwrap_err();
        assert_eq!(
            error,
            ConflictPlanError::AccountOutOfRange {
                canonical_index: 8,
                account_id: 1,
                account_count: 1,
            }
        );
        assert_eq!(planner.transaction_count(), 1);

        // The rejected canonical index remains available.
        planner
            .push_transaction(8, &[AccountAccess::readonly(0)], 1)
            .unwrap();
        let plan = planner.finish().unwrap();
        assert_eq!(plan.predecessors(1), Some([0].as_slice()));
        assert_eq!(plan.metrics().total_weight, 3);
    }

    #[test]
    fn rejects_noncanonical_indices_zero_weights_and_zero_workers() {
        let mut planner = ConflictPlanner::new(0);
        planner.push_transaction(9, &[], 1).unwrap();
        assert_eq!(
            planner.push_transaction(9, &[], 1),
            Err(ConflictPlanError::CanonicalIndexNotIncreasing {
                previous: 9,
                current: 9,
            })
        );
        assert_eq!(
            planner.push_transaction(10, &[], 0),
            Err(ConflictPlanError::ZeroWeight {
                canonical_index: 10,
            })
        );

        let plan = planner.finish().unwrap();
        assert_eq!(simulate(&plan, 0), Err(ConflictPlanError::WorkerCountZero));
    }

    #[test]
    fn account_list_order_does_not_change_the_plan() {
        let first_order = [
            AccountAccess::readonly(2),
            AccountAccess::writable(1),
            AccountAccess::readonly(0),
        ];
        let second_order = [
            AccountAccess::readonly(0),
            AccountAccess::readonly(2),
            AccountAccess::writable(1),
        ];
        let initial = [
            AccountAccess::writable(0),
            AccountAccess::writable(1),
            AccountAccess::writable(2),
        ];

        let build = |accesses: &[AccountAccess]| {
            plan_slot(
                3,
                &[
                    TransactionPlanInput {
                        canonical_index: 0,
                        accesses: &initial,
                        weight: 1,
                    },
                    TransactionPlanInput {
                        canonical_index: 1,
                        accesses,
                        weight: 1,
                    },
                ],
            )
            .unwrap()
        };

        assert_eq!(build(&first_order), build(&second_order));
    }

    #[test]
    fn randomized_frontier_has_full_conflict_graph_transitive_closure() {
        fn next_random(state: &mut u64) -> u64 {
            *state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *state
        }

        fn transitive_closure(mut edges: Vec<bool>, transaction_count: usize) -> Vec<bool> {
            for intermediate in 0..transaction_count {
                for source in 0..transaction_count {
                    if !edges[source * transaction_count + intermediate] {
                        continue;
                    }
                    for target in 0..transaction_count {
                        if edges[intermediate * transaction_count + target] {
                            edges[source * transaction_count + target] = true;
                        }
                    }
                }
            }
            edges
        }

        let mut random = 0x4d59_5df4_d0f3_3173;
        for case in 0..512 {
            let account_count = (next_random(&mut random) % 6 + 1) as usize;
            let transaction_count = (next_random(&mut random) % 12 + 1) as usize;
            // 0 = absent, 1 = readonly, 2 = writable.
            let mut modes = vec![vec![0u8; account_count]; transaction_count];
            let mut accesses = Vec::with_capacity(transaction_count);
            for transaction_modes in &mut modes {
                let mut transaction_accesses = Vec::new();
                for (account, mode) in transaction_modes.iter_mut().enumerate() {
                    *mode = match next_random(&mut random) & 3 {
                        0 | 1 => 0,
                        2 => 1,
                        _ => 2,
                    };
                    match *mode {
                        1 => transaction_accesses.push(AccountAccess::readonly(account as u32)),
                        2 => transaction_accesses.push(AccountAccess::writable(account as u32)),
                        _ => {}
                    }
                }
                accesses.push(transaction_accesses);
            }
            let inputs = accesses
                .iter()
                .enumerate()
                .map(|(transaction, accesses)| TransactionPlanInput {
                    canonical_index: (transaction as u32) * 3 + 1,
                    accesses,
                    weight: next_random(&mut random) % 31 + 1,
                })
                .collect::<Vec<_>>();
            let plan = plan_slot(account_count, &inputs).unwrap();

            let mut full_conflicts = vec![false; transaction_count * transaction_count];
            for source in 0..transaction_count {
                for target in source + 1..transaction_count {
                    let conflicts = (0..account_count).any(|account| {
                        modes[source][account] != 0
                            && modes[target][account] != 0
                            && (modes[source][account] == 2 || modes[target][account] == 2)
                    });
                    full_conflicts[source * transaction_count + target] = conflicts;
                }
            }

            let mut frontier = vec![false; transaction_count * transaction_count];
            for target in 0..transaction_count {
                for source in plan.predecessors(target as u32).unwrap() {
                    assert!((*source as usize) < target);
                    frontier[*source as usize * transaction_count + target] = true;
                }
            }
            assert_eq!(
                transitive_closure(frontier, transaction_count),
                transitive_closure(full_conflicts, transaction_count),
                "transitive conflict closure differs in generated case {case}"
            );

            for workers in [1, 2, 4, 16] {
                let first = plan.simulate(workers).unwrap();
                let second = plan.simulate(workers).unwrap();
                assert_eq!(first, second, "nondeterministic generated case {case}");
                assert!(first.critical_path_weight <= first.makespan);
                assert!(first.makespan <= first.total_weight);
                assert!(first.peak_running <= workers);
                if workers == 1 {
                    assert_eq!(first.makespan, first.total_weight);
                }
            }
        }
    }
}

//! Source-neutral instruction adapter for operator-trusted CAR streams.
//!
//! This adapter publishes in order and overlaps reading with projection. It accepts
//! any `Read` that yields a decoded CAR byte stream. The current CAR wire form
//! has no explicit raw-transaction or raw-metadata fallback marker, so decoder
//! failures are archive errors instead of coverage downgrades.
//!
//! The configured entry and block limits apply to decoded CAR payload bytes,
//! not to total process memory. The raw block, reassembled transaction and
//! metadata frames, zstd output, decoded metadata, and canonical projection can
//! coexist. The legacy CAR, wincode, protobuf, and zstd decoders are therefore
//! accepted only under [`SourceVerification::OperatorTrusted`]. Canonical
//! output allocations made by this adapter use fallible reservation.
//!
//! # Construction and scan
//!
//! Obtain the dense slot plan from the trusted block inventory or index that
//! defines the comparison. Pass a local file, a caller-owned remote
//! stream, or a decompressor as the sequential [`Read`] input. This module does
//! not include a network client.
//! `SourceIdentity::binding` must contain a nonblank CAR object binding. The
//! source keeps that input binding and marks whether it is a strong object
//! binding or an operator-trusted descriptor.
//!
//! ```no_run
//! use std::{fs::File, io::BufReader};
//! use blockzilla_query_sdk::{
//!     ArchiveFormat, ArchiveInstructionSourceExt, ScanRequest, SourceIdentity,
//!     SourceVerification,
//! };
//! use of_car_reader::query_sdk::{
//!     CanonicalBlockPlan, CarInstructionSource, CarQueryLimits,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // In production, load every slot from the trusted canonical block plan.
//! let slots = vec![74_396_256, 74_396_258];
//! let block_count = u32::try_from(slots.len())?;
//! let identity = SourceIdentity {
//!     format: ArchiveFormat::Car,
//!     label: "epoch-185.car".into(),
//!     cluster_id: Some("mainnet-beta".into()),
//!     epoch: 185,
//!     first_slot: 74_396_256,
//!     slots_per_epoch: 432_000,
//!     block_count,
//!     verification: SourceVerification::OperatorTrusted,
//!     binding: Some("trusted-car-object-binding".into()),
//! };
//! let car = BufReader::new(File::open("epoch-185.car")?);
//! let mut source = CarInstructionSource::new(
//!     car,
//!     identity,
//!     CanonicalBlockPlan::new(slots),
//!     CarQueryLimits::default(),
//! )?;
//! let request = ScanRequest::all()
//!     .allow_incomplete_instructions()
//!     .allow_incomplete_cpi()
//!     .allow_unknown_execution();
//! let receipt = source.for_each_block(&request, |_block| Ok(()))?;
//! assert_eq!(receipt.blocks, 2);
//! # Ok(())
//! # }
//! ```

use std::io::{self, Read};

use crate::{
    CarBlockReader, LosslessBlockReadLimits,
    confirmed_block::TransactionStatusMeta,
    error::CarReadError,
    metadata_decoder::{
        MetadataDecodeError, ZstdReusableDecoder, decode_transaction_status_meta_from_frame,
    },
    ordered_lossless::OrderedLosslessCarBlock,
    reconstruct::{RawTransactionNode, ReconstructError},
    stored_transaction::StoredTransactionError,
    versioned_transaction::{
        CompiledInstruction, MessageHeader, VersionedMessage, VersionedTransaction,
    },
};
use blockzilla_query_sdk::{
    ArchiveFormat, ArchiveInstructionSource, BlockHeader, BlockSink, CanonicalBlock,
    CanonicalTransaction, CoverageReason, CpiCoverage, Error as QueryError, ExecutionStatus,
    InstructionCoordinate, InstructionCoverage, InstructionDataCoverage,
    InstructionDataRequirement, MAX_CANONICAL_SHORT_VEC_ITEMS, OrderedBlockPublisher,
    RecordedTokenBalance, ResolvedInstruction, ScanIoReceipt, ScanReceipt, ScanRequest,
    SourceIdentity, SourceVerification, TokenBalanceCoverage, TokenBalanceRequirement,
    TokenBalanceSide, TransactionHeader,
};

const HARD_MAX_HEADER_BYTES: usize = 16 << 20;
const HARD_MAX_IO_BUFFER_BYTES: usize = 64 << 20;
const HARD_MAX_ENTRY_PAYLOAD_BYTES: usize = 256 << 20;
const HARD_MAX_BLOCK_PAYLOAD_BYTES: usize = 2 << 30;
const HARD_MAX_ENTRIES_PER_BLOCK: usize = 1_000_000;
const HARD_MAX_TRANSACTION_BYTES: usize = 64 << 20;
const HARD_MAX_METADATA_BYTES: usize = 512 << 20;
const HARD_MAX_RESOLVED_ACCOUNTS: usize = 256;
const HARD_MAX_TRANSACTIONS_PER_BLOCK: usize = u16::MAX as usize;
const HARD_MAX_INSTRUCTIONS_PER_TRANSACTION: usize = u16::MAX as usize;

/// Explicit memory and geometry limits for an operator-trusted CAR source.
///
/// These caps bound raw entry loading and the owned canonical projection. They
/// do not turn the legacy CBOR, wincode, protobuf, or zstd decoders into an
/// adversarial-input parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CarQueryLimits {
    /// Maximum declared CAR header payload before its allocation.
    pub max_header_bytes: usize,
    /// Buffer capacity used around the caller-supplied `Read` input.
    pub io_buffer_bytes: usize,
    /// Maximum decoded payload for one CAR entry, excluding its CID.
    pub max_entry_payload_bytes: usize,
    /// Maximum sum of decoded entry payloads read before one block node.
    /// This is not a total process-memory limit.
    pub max_block_payload_bytes: usize,
    /// Maximum CAR entries read before one block node or clean EOF.
    pub max_entries_per_block: usize,
    /// Maximum canonical transactions referenced by one block.
    pub max_transactions_per_block: usize,
    /// Maximum reassembled transaction frame before wincode decoding.
    pub max_transaction_bytes: usize,
    /// Maximum reassembled metadata frame before optional zstd decoding.
    pub max_metadata_bytes: usize,
    /// Maximum static plus loaded account keys in one message.
    pub max_resolved_accounts: usize,
    /// Maximum outer-plus-recorded-inner instructions in one transaction.
    pub max_instructions_per_transaction: usize,
}

impl Default for CarQueryLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 1 << 20,
            io_buffer_bytes: 8 << 20,
            max_entry_payload_bytes: 64 << 20,
            max_block_payload_bytes: 512 << 20,
            max_entries_per_block: 100_000,
            max_transactions_per_block: HARD_MAX_TRANSACTIONS_PER_BLOCK,
            max_transaction_bytes: 16 << 20,
            max_metadata_bytes: 256 << 20,
            max_resolved_accounts: HARD_MAX_RESOLVED_ACCOUNTS,
            max_instructions_per_transaction: HARD_MAX_INSTRUCTIONS_PER_TRANSACTION,
        }
    }
}

impl CarQueryLimits {
    fn validate(self) -> Result<Self, CarQueryError> {
        validate_limit(
            "max_header_bytes",
            self.max_header_bytes,
            HARD_MAX_HEADER_BYTES,
        )?;
        validate_limit(
            "io_buffer_bytes",
            self.io_buffer_bytes,
            HARD_MAX_IO_BUFFER_BYTES,
        )?;
        validate_limit(
            "max_entry_payload_bytes",
            self.max_entry_payload_bytes,
            HARD_MAX_ENTRY_PAYLOAD_BYTES,
        )?;
        validate_limit(
            "max_block_payload_bytes",
            self.max_block_payload_bytes,
            HARD_MAX_BLOCK_PAYLOAD_BYTES,
        )?;
        validate_limit(
            "max_entries_per_block",
            self.max_entries_per_block,
            HARD_MAX_ENTRIES_PER_BLOCK,
        )?;
        validate_limit(
            "max_transactions_per_block",
            self.max_transactions_per_block,
            HARD_MAX_TRANSACTIONS_PER_BLOCK,
        )?;
        validate_limit(
            "max_transaction_bytes",
            self.max_transaction_bytes,
            HARD_MAX_TRANSACTION_BYTES,
        )?;
        validate_limit(
            "max_metadata_bytes",
            self.max_metadata_bytes,
            HARD_MAX_METADATA_BYTES,
        )?;
        validate_limit(
            "max_resolved_accounts",
            self.max_resolved_accounts,
            HARD_MAX_RESOLVED_ACCOUNTS,
        )?;
        validate_limit(
            "max_instructions_per_transaction",
            self.max_instructions_per_transaction,
            HARD_MAX_INSTRUCTIONS_PER_TRANSACTION,
        )?;
        if self.max_entry_payload_bytes > self.max_block_payload_bytes {
            return Err(CarQueryError::InvalidLimits(
                "max_entry_payload_bytes exceeds max_block_payload_bytes".into(),
            ));
        }
        if self.max_transaction_bytes > self.max_block_payload_bytes
            || self.max_metadata_bytes > self.max_block_payload_bytes
        {
            return Err(CarQueryError::InvalidLimits(
                "a frame limit exceeds max_block_payload_bytes".into(),
            ));
        }
        Ok(self)
    }

    const fn block_read_limits(self) -> LosslessBlockReadLimits {
        LosslessBlockReadLimits {
            max_entry_payload_bytes: self.max_entry_payload_bytes,
            max_block_payload_bytes: self.max_block_payload_bytes,
            max_entries_per_block: self.max_entries_per_block,
            max_transactions_per_block: self.max_transactions_per_block,
        }
    }
}

fn validate_limit(label: &str, value: usize, hard_max: usize) -> Result<(), CarQueryError> {
    if value == 0 || value > hard_max {
        return Err(CarQueryError::InvalidLimits(format!(
            "{label} {value} is outside 1..={hard_max}"
        )));
    }
    Ok(())
}

/// Typed errors retained as the source of `blockzilla_query_sdk::Error::Source`.
#[derive(Debug, thiserror::Error)]
pub enum CarQueryError {
    #[error("invalid CAR source identity: {0}")]
    InvalidIdentity(String),
    #[error("invalid CAR query limits: {0}")]
    InvalidLimits(String),
    #[error("invalid CAR archive: {0}")]
    InvalidArchive(String),
    #[error("CAR transaction decode failed: {0}")]
    TransactionDecode(String),
    #[error("CAR read failed")]
    Read(#[source] CarReadError),
    #[error("CAR reconstruction failed")]
    Reconstruct(#[source] ReconstructError),
    #[error("CAR metadata decode failed")]
    Metadata(#[source] MetadataDecodeError),
    #[error("CAR canonical projection allocation failed: {0}")]
    Allocation(String),
    #[error("this sequential CAR source was already scanned")]
    AlreadyScanned,
}

type CarQueryResult<T> = Result<T, CarQueryError>;

/// Exact dense block-row slot sequence from a trusted block inventory or index.
///
/// A planned slot that is absent from the CAR stream becomes an empty block
/// row. A skipped slot that is absent from this plan never becomes a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalBlockPlan {
    slots: Vec<u64>,
}

impl CanonicalBlockPlan {
    /// Retain the exact dense block-row slot sequence supplied by the caller.
    ///
    /// The adapter validates its length, order, and epoch range at construction.
    /// It does not derive or authenticate this plan from CAR bytes.
    pub fn new(slots: Vec<u64>) -> Self {
        Self { slots }
    }

    pub fn slots(&self) -> &[u64] {
        &self.slots
    }
}

struct CountingRead<R> {
    inner: R,
    calls_with_bytes: u64,
    bytes: u64,
}

impl<R> CountingRead<R> {
    const fn new(inner: R) -> Self {
        Self {
            inner,
            calls_with_bytes: 0,
            bytes: 0,
        }
    }
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(output)?;
        if read != 0 {
            self.calls_with_bytes = self.calls_with_bytes.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "CAR read-call counter overflow")
            })?;
            self.bytes = self
                .bytes
                .checked_add(u64::try_from(read).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "CAR read size exceeds u64")
                })?)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "CAR read-byte counter overflow")
                })?;
        }
        Ok(read)
    }
}

#[derive(Default)]
struct ProjectionScratch {
    output_pool: blockzilla_query_sdk::projection_pool::ProjectionPool,
    block_counts: Option<blockzilla_query_sdk::BlockCounts>,
    metadata: TransactionStatusMeta,
    metadata_zstd: ZstdReusableDecoder,
    transactions: crate::versioned_transaction::VersionedTransactionReuse,
    transaction_output: Vec<CanonicalTransaction>,
}

/// One-pass source-neutral adapter over an uncompressed sequential CAR stream.
///
/// `R` can itself be a local decompressor. The receipt counts exact reads and
/// bytes returned by `R`; it cannot infer stored compressed bytes or cache I/O.
pub struct CarInstructionSource<R: Read> {
    reader: CarBlockReader<CountingRead<R>>,
    identity: SourceIdentity,
    plan: CanonicalBlockPlan,
    limits: CarQueryLimits,
    scanned: bool,
    block: OrderedLosslessCarBlock,
    scratch: ProjectionScratch,
}

#[derive(Debug, Clone, Copy)]
enum CarBindingKind {
    StrongObject,
    OperatorTrustedDescriptor,
}

impl<R: Read> CarInstructionSource<R> {
    /// Bind one sequential decoded-CAR stream to a trusted source identity and
    /// canonical dense block plan.
    ///
    /// The input identity binding is the CAR object binding.
    pub fn new(
        reader: R,
        identity: SourceIdentity,
        plan: CanonicalBlockPlan,
        limits: CarQueryLimits,
    ) -> CarQueryResult<Self> {
        Self::new_with_binding_kind(reader, identity, plan, limits, CarBindingKind::StrongObject)
    }

    /// Bind an operator-trusted URL and length descriptor to a canonical plan.
    ///
    /// The input binding must identify only the accepted descriptor. It must
    /// not claim a strong object validator, content hash, or object identity.
    /// The stored binding keeps that label without hashing the slot plan.
    pub fn new_operator_trusted_descriptor(
        reader: R,
        identity: SourceIdentity,
        plan: CanonicalBlockPlan,
        limits: CarQueryLimits,
    ) -> CarQueryResult<Self> {
        Self::new_with_binding_kind(
            reader,
            identity,
            plan,
            limits,
            CarBindingKind::OperatorTrustedDescriptor,
        )
    }

    fn new_with_binding_kind(
        reader: R,
        mut identity: SourceIdentity,
        plan: CanonicalBlockPlan,
        limits: CarQueryLimits,
        binding_kind: CarBindingKind,
    ) -> CarQueryResult<Self> {
        validate_identity(&identity, &plan)?;
        let input_binding = identity
            .binding
            .take()
            .expect("identity validation required a CAR input binding");
        identity.binding = Some(effective_binding(input_binding, binding_kind));
        let limits = limits.validate()?;
        let mut reader =
            CarBlockReader::with_capacity(CountingRead::new(reader), limits.io_buffer_bytes);
        reader
            .skip_header_bounded(limits.max_header_bytes)
            .map_err(CarQueryError::Read)?;
        Ok(Self {
            reader,
            identity,
            plan,
            limits,
            scanned: false,
            block: OrderedLosslessCarBlock::default(),
            scratch: ProjectionScratch::default(),
        })
    }

    pub const fn limits(&self) -> CarQueryLimits {
        self.limits
    }

    fn scan_inner(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt>
    where
        R: Send,
    {
        if self.scanned {
            return Err(source_error(CarQueryError::AlreadyScanned));
        }
        let identity = self.identity.clone();
        let mut publisher = OrderedBlockPublisher::new(&identity, request, sink)?;
        self.scanned = true;
        let limits = self.limits;
        let reader = &mut self.reader;
        let plan = &self.plan;
        let block = &mut self.block;
        let scratch = &mut self.scratch;
        // Two raw blocks circulate between reading and projection. No payload
        // clone, per-transaction channel, or unbounded read-ahead queue.
        std::thread::scope(|scope| {
            let (free_tx, free_rx) = std::sync::mpsc::sync_channel(2);
            let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(2);
            for _ in 0..2 {
                free_tx.send(OrderedLosslessCarBlock::default()).unwrap();
            }
            let producer = std::thread::Builder::new()
                .name("car-read-ahead".into())
                .spawn_scoped(scope, move || {
                    while let Ok(mut raw) = free_rx.recv() {
                        match reader.read_until_block_ordered_lossless_bounded(
                            &mut raw,
                            limits.block_read_limits(),
                        ) {
                            Ok(true) => {
                                if ready_tx.send(Ok(Some(raw))).is_err() {
                                    break;
                                }
                            }
                            Ok(false) => {
                                let _ = ready_tx.send(Ok(None));
                                break;
                            }
                            Err(error) => {
                                let _ = ready_tx.send(Err(error));
                                break;
                            }
                        }
                    }
                })
                .map_err(|error| {
                    source_error(CarQueryError::InvalidArchive(format!(
                        "cannot start CAR reader: {error}"
                    )))
                })?;
            let mut have_block = false;
            let result = scan_projected_blocks(
                &identity,
                plan,
                limits,
                block,
                scratch,
                request,
                &mut publisher,
                |block| {
                    if have_block {
                        // If the producer reached EOF it may already have closed this queue.
                        let _ = free_tx.send(std::mem::take(block));
                    }
                    match ready_rx.recv() {
                        Ok(Ok(Some(raw))) => {
                            *block = raw;
                            have_block = true;
                            Ok(true)
                        }
                        Ok(Ok(None)) => {
                            have_block = false;
                            Ok(false)
                        }
                        Ok(Err(error)) => Err(source_error(CarQueryError::Read(error))),
                        Err(_) => Err(source_error(CarQueryError::InvalidArchive(
                            "CAR read-ahead stopped without EOF".into(),
                        ))),
                    }
                },
            );
            // Disconnect both directions before joining, including sink errors
            // and partial scans. A producer cannot remain blocked on a queue.
            drop(free_tx);
            drop(ready_rx);
            let joined = producer.join().map_err(|_| {
                source_error(CarQueryError::InvalidArchive(
                    "CAR read-ahead worker panicked".into(),
                ))
            });
            result?;
            joined
        })?;
        let counters = self.reader.reader.get_ref();
        publisher.set_io_receipt(ScanIoReceipt {
            source_read_calls: Some(counters.calls_with_bytes),
            source_read_bytes: Some(counters.bytes),
            decoded_bytes: None,
            cache_read_calls: None,
            cache_read_bytes: None,
        });
        publisher.finish()
    }
}

fn scan_projected_blocks(
    identity: &SourceIdentity,
    plan: &CanonicalBlockPlan,
    limits: CarQueryLimits,
    block: &mut OrderedLosslessCarBlock,
    scratch: &mut ProjectionScratch,
    request: &ScanRequest,
    publisher: &mut OrderedBlockPublisher<'_>,
    mut next_block: impl FnMut(&mut OrderedLosslessCarBlock) -> blockzilla_query_sdk::Result<bool>,
) -> blockzilla_query_sdk::Result<()> {
    let first = request.range.map_or(0, |range| range.first_block);
    let end = request.range.map_or(identity.block_count, |range| {
        range
            .first_block
            .checked_add(range.block_count.get())
            .expect("OrderedBlockPublisher validated the range")
    });
    let full_scan = request.range.is_none();
    let mut previous_real_slot = None;
    let mut pending_real = false;
    let mut clean_eof = false;

    for ordinal in 0..end {
        let planned_slot = plan.slots[ordinal as usize];
        if !pending_real && !clean_eof {
            pending_real = next_block(block)?;
            clean_eof = !pending_real;
            if pending_real {
                let real_slot = block_slot(block).map_err(source_error)?;
                validate_real_slot(&identity, previous_real_slot, real_slot)
                    .map_err(source_error)?;
                previous_real_slot = Some(real_slot);
            }
        }

        let real_slot = pending_real
            .then(|| block_slot(block))
            .transpose()
            .map_err(source_error)?;
        if let Some(real_slot) = real_slot
            && real_slot < planned_slot
        {
            return Err(source_error(CarQueryError::InvalidArchive(format!(
                "CAR block slot {real_slot} is absent from the canonical plan before planned slot {planned_slot}"
            ))));
        }

        if ordinal < first {
            if real_slot == Some(planned_slot) {
                pending_real = false;
            }
            continue;
        }

        if real_slot == Some(planned_slot) {
            let mut block = project_block(block, &identity, ordinal, request, limits, scratch)
                .map_err(source_error)?;
            publisher.publish(&block)?;
            scratch.output_pool.recycle_block(&mut block);
            // The callback has finished; keep the empty output allocation.
            scratch.transaction_output = block.transactions;
            pending_real = false;
        } else {
            publish_empty_row(publisher, &identity, ordinal, planned_slot)?;
        }
    }

    if full_scan {
        if pending_real {
            return Err(source_error(CarQueryError::InvalidArchive(format!(
                "CAR block slot {} appears after the final canonical plan row",
                block_slot(block).map_err(source_error)?
            ))));
        }
        if !clean_eof {
            let has_extra = next_block(block)?;
            if has_extra {
                return Err(source_error(CarQueryError::InvalidArchive(format!(
                    "CAR block slot {} appears after the final canonical plan row",
                    block_slot(block).map_err(source_error)?
                ))));
            }
        }
    }

    Ok(())
}

impl<R: Read + Send> ArchiveInstructionSource for CarInstructionSource<R> {
    fn identity(&self) -> &SourceIdentity {
        &self.identity
    }

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> blockzilla_query_sdk::Result<ScanReceipt> {
        self.scan_inner(request, sink)
    }
}

fn validate_identity(identity: &SourceIdentity, plan: &CanonicalBlockPlan) -> CarQueryResult<()> {
    if identity.format != ArchiveFormat::Car {
        return Err(CarQueryError::InvalidIdentity("format must be CAR".into()));
    }
    if identity.verification != SourceVerification::OperatorTrusted {
        return Err(CarQueryError::InvalidIdentity(
            "verification must be OperatorTrusted".into(),
        ));
    }
    if identity.label.is_empty() {
        return Err(CarQueryError::InvalidIdentity("label is empty".into()));
    }
    if identity.cluster_id.as_ref().is_some_and(String::is_empty) {
        return Err(CarQueryError::InvalidIdentity("cluster_id is empty".into()));
    }
    if identity
        .binding
        .as_ref()
        .is_none_or(|binding| binding.trim().is_empty())
    {
        return Err(CarQueryError::InvalidIdentity(
            "CAR input binding is absent or empty".into(),
        ));
    }
    if identity.slots_per_epoch == 0 {
        return Err(CarQueryError::InvalidIdentity(
            "slots_per_epoch is zero".into(),
        ));
    }
    let plan_len = u32::try_from(plan.slots.len()).map_err(|_| {
        CarQueryError::InvalidIdentity("canonical block plan exceeds u32 rows".into())
    })?;
    if identity.block_count != plan_len {
        return Err(CarQueryError::InvalidIdentity(format!(
            "block_count {} differs from canonical plan length {plan_len}",
            identity.block_count
        )));
    }
    let last_slot = identity
        .first_slot
        .checked_add(identity.slots_per_epoch - 1)
        .ok_or_else(|| CarQueryError::InvalidIdentity("epoch slot range overflows u64".into()))?;
    let mut previous = None;
    for &slot in &plan.slots {
        if slot < identity.first_slot || slot > last_slot {
            return Err(CarQueryError::InvalidIdentity(format!(
                "planned slot {slot} is outside {}..={last_slot}",
                identity.first_slot
            )));
        }
        if previous.is_some_and(|value| slot <= value) {
            return Err(CarQueryError::InvalidIdentity(
                "canonical block plan slots are not strictly increasing".into(),
            ));
        }
        previous = Some(slot);
    }
    Ok(())
}

fn effective_binding(input_binding: String, binding_kind: CarBindingKind) -> String {
    let label = match binding_kind {
        CarBindingKind::StrongObject => "car-object-binding",
        CarBindingKind::OperatorTrustedDescriptor => "operator-trusted-input-descriptor",
    };
    format!("{label}={input_binding}")
}

fn publish_empty_row(
    publisher: &mut OrderedBlockPublisher<'_>,
    identity: &SourceIdentity,
    ordinal: u32,
    slot: u64,
) -> blockzilla_query_sdk::Result<()> {
    publisher.publish(&CanonicalBlock {
        counts: None,
        header: BlockHeader {
            epoch: identity.epoch,
            block_ordinal: ordinal,
            slot,
        },
        transactions: Vec::new(),
    })
}

fn block_slot(block: &OrderedLosslessCarBlock) -> CarQueryResult<u64> {
    block
        .block
        .as_ref()
        .map(|block| block.slot)
        .ok_or_else(|| CarQueryError::InvalidArchive("lossless group has no block node".into()))
}

fn validate_real_slot(
    identity: &SourceIdentity,
    previous: Option<u64>,
    slot: u64,
) -> CarQueryResult<()> {
    let last_slot = identity
        .first_slot
        .checked_add(identity.slots_per_epoch - 1)
        .expect("identity geometry was validated");
    if slot < identity.first_slot || slot > last_slot {
        return Err(CarQueryError::InvalidArchive(format!(
            "real block slot {slot} is outside {}..={last_slot}",
            identity.first_slot
        )));
    }
    if let Some(previous) = previous
        && slot <= previous
    {
        return Err(CarQueryError::InvalidArchive(format!(
            "real block slot {slot} is not after {previous}"
        )));
    }
    Ok(())
}

fn project_block(
    block: &OrderedLosslessCarBlock,
    identity: &SourceIdentity,
    ordinal: u32,
    request: &ScanRequest,
    limits: CarQueryLimits,
    scratch: &mut ProjectionScratch,
) -> CarQueryResult<CanonicalBlock> {
    let raw_block = block
        .block
        .as_ref()
        .ok_or_else(|| CarQueryError::InvalidArchive("block node is absent".into()))?;
    if block.transactions.len() > limits.max_transactions_per_block {
        return Err(CarQueryError::InvalidArchive(format!(
            "slot {} has {} transactions, limit is {}",
            raw_block.slot,
            block.transactions.len(),
            limits.max_transactions_per_block
        )));
    }
    scratch.block_counts = request
        .counts_only
        .then(blockzilla_query_sdk::BlockCounts::default);
    let mut transactions = std::mem::take(&mut scratch.transaction_output);
    if !request.counts_only {
        transactions
            .try_reserve(block.transactions.len())
            .map_err(|_| {
                CarQueryError::InvalidArchive("block transaction allocation failed".into())
            })?;
    }
    for (position, raw) in block.transactions.iter().enumerate() {
        let tx_index = u32::try_from(position)
            .map_err(|_| CarQueryError::InvalidArchive("transaction index exceeds u32".into()))?;
        if raw.slot != raw_block.slot {
            return Err(CarQueryError::InvalidArchive(format!(
                "transaction {tx_index} slot {} differs from block slot {}",
                raw.slot, raw_block.slot
            )));
        }
        if let Some(declared) = raw.index
            && declared != u64::from(tx_index)
        {
            return Err(CarQueryError::InvalidArchive(format!(
                "transaction frame index {declared} differs from canonical referenced position {tx_index}"
            )));
        }
        if let Some(transaction) =
            project_transaction(block, raw, tx_index, request, limits, scratch)?
        {
            transactions.push(transaction);
        }
    }
    Ok(CanonicalBlock {
        counts: scratch.block_counts,
        header: BlockHeader {
            epoch: identity.epoch,
            block_ordinal: ordinal,
            slot: raw_block.slot,
        },
        transactions,
    })
}

fn project_transaction(
    _block: &OrderedLosslessCarBlock,
    raw: &RawTransactionNode,
    tx_index: u32,
    request: &ScanRequest,
    limits: CarQueryLimits,
    scratch: &mut ProjectionScratch,
) -> CarQueryResult<Option<CanonicalTransaction>> {
    if raw.data.data.len() > limits.max_transaction_bytes {
        return Err(CarQueryError::InvalidArchive(
            "transaction frame exceeds limit".into(),
        ));
    }
    let transaction = scratch
        .transactions
        .deserialize_transaction(&raw.data.data)
        .map_err(|error| CarQueryError::TransactionDecode(error.to_string()))?;
    let result = project_decoded_transaction(&transaction, raw, tx_index, request, limits, scratch);
    scratch.transactions.recycle_transaction(transaction);
    result
}

fn project_decoded_transaction(
    transaction: &VersionedTransaction<'_>,
    raw: &RawTransactionNode,
    tx_index: u32,
    request: &ScanRequest,
    limits: CarQueryLimits,
    scratch: &mut ProjectionScratch,
) -> CarQueryResult<Option<CanonicalTransaction>> {
    let ProjectionScratch {
        output_pool,
        block_counts,
        metadata,
        metadata_zstd,
        ..
    } = scratch;
    if raw.data.data.len() > limits.max_transaction_bytes {
        return Err(CarQueryError::InvalidArchive(format!(
            "transaction frame has {} bytes, limit is {}",
            raw.data.data.len(),
            limits.max_transaction_bytes
        )));
    }
    let message = message_view(&transaction.message)?;
    validate_message_geometry(&transaction, &message, limits)?;

    if raw.metadata.data.len() > limits.max_metadata_bytes {
        return Err(CarQueryError::InvalidArchive(format!(
            "transaction metadata frame has {} bytes, limit is {}",
            raw.metadata.data.len(),
            limits.max_metadata_bytes
        )));
    }
    // The count example does not need logs, balances, rewards, or key values.
    // Keep the historical decoder for bincode metadata; protobuf is visited in place.
    if let Some(counts) = block_counts.as_mut()
        && crate::metadata_decoder::slot_uses_protobuf_metadata(raw.slot)
    {
        count_protobuf_transaction(&message, &raw.metadata.data, metadata_zstd, counts, limits)?;
        return Ok(None);
    }
    let metadata = if raw.metadata.data.is_empty() {
        None
    } else {
        decode_projected_metadata(
            raw.slot,
            &raw.metadata.data,
            metadata,
            metadata_zstd,
            request,
        )
        .map_err(CarQueryError::Metadata)?;
        Some(&*metadata)
    };

    let primary_signature = if request.include_primary_signatures {
        transaction.signatures.first().map(|signature| **signature)
    } else {
        None
    };
    let signer_matches = request.required_signer.is_none_or(|key| {
        message
            .static_keys
            .iter()
            .take(usize::from(message.header.num_required_signatures))
            .any(|candidate| **candidate == key)
    });
    let required_signers = if request.include_required_signers && request.required_signer.is_some()
    {
        request
            .required_signer
            .filter(|_| signer_matches)
            .into_iter()
            .collect()
    } else if request.include_required_signers {
        let required = usize::from(message.header.num_required_signatures);
        let mut required_signers = reserved_vec(required, "required signer keys")?;
        required_signers.extend(message.static_keys[..required].iter().map(|key| **key));
        required_signers
    } else {
        Vec::new()
    };

    let (recorded_status, recorded_failed_outer, recorded_cpi) = match metadata {
        None => (
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
            None,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent),
        ),
        Some(metadata) => {
            validate_metadata_geometry(&message, metadata, limits)?;
            let failed = decode_failed_outer_index(metadata)?;
            let status = if metadata.err.is_some() {
                ExecutionStatus::Failed
            } else {
                ExecutionStatus::Succeeded
            };
            let cpi = if metadata.inner_instructions_none {
                CpiCoverage::NotRecorded
            } else {
                CpiCoverage::Complete
            };
            (status, failed, cpi)
        }
    };
    let (status, failed_outer_instruction_index) = if request.include_execution_status {
        (recorded_status, recorded_failed_outer)
    } else {
        (
            ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested),
            None,
        )
    };
    let cpi_coverage = if request.include_instructions {
        recorded_cpi
    } else {
        CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested)
    };

    // Skip unrequested failure details before key resolution and CPI projection.
    // Full-detail requests still validate instruction order below.
    if request.omit_failed_transaction_details && recorded_status == ExecutionStatus::Failed {
        return Ok(Some(CanonicalTransaction {
            header: TransactionHeader {
                tx_index,
                status,
                failed_outer_instruction_index,
                instruction_coverage: InstructionCoverage::Unknown(
                    CoverageReason::ProjectionNotRequested,
                ),
                cpi_coverage: CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested),
            },
            primary_signature: None,
            required_signers: Vec::new(),
            instructions: Vec::new(),
            token_balance_coverage: TokenBalanceCoverage::NotRequested,
            token_balances: Vec::new(),
        }));
    }

    let loaded_keys = if !request.include_instructions {
        None
    } else {
        match metadata {
            Some(metadata) => Some(AccountKeys::new(&message, Some(metadata))?),
            None if message.expected_loaded == (0, 0) => Some(AccountKeys::new(&message, None)?),
            None => None,
        }
    };
    if let Some(counts) = block_counts {
        counts.transactions += 1;
        counts.incomplete_cpi += u64::from(!matches!(recorded_cpi, CpiCoverage::Complete));
        if let Some(keys) = loaded_keys {
            let check = |program: u32, accounts: &[u8], data: &[u8]| -> CarQueryResult<()> {
                if program as usize >= keys.len()
                    || accounts
                        .iter()
                        .any(|index| usize::from(*index) >= keys.len())
                    || data.len() > MAX_CANONICAL_SHORT_VEC_ITEMS
                {
                    return Err(CarQueryError::InvalidArchive(
                        "invalid instruction geometry in count scan".into(),
                    ));
                }
                Ok(())
            };
            for instruction in message.instructions {
                check(
                    u32::from(instruction.program_id_index),
                    &instruction.accounts,
                    &instruction.data,
                )?;
            }
            let mut inner_count = 0usize;
            if let Some(metadata) = metadata {
                for group in &metadata.inner_instructions {
                    for instruction in &group.instructions {
                        check(
                            instruction.program_id_index,
                            &instruction.accounts,
                            &instruction.data,
                        )?;
                        inner_count += 1;
                    }
                }
            }
            let total = message.instructions.len() + inner_count;
            if total > limits.max_instructions_per_transaction
                || total > MAX_CANONICAL_SHORT_VEC_ITEMS
            {
                return Err(CarQueryError::InvalidArchive(
                    "instruction count exceeds limit".into(),
                ));
            }
            counts.instructions += total as u64;
            counts.recorded_inner_instructions += inner_count as u64;
        } else {
            counts.incomplete_instructions += 1;
        }
        return Ok(None);
    }
    let (instruction_coverage, instructions) = if !request.include_instructions {
        (
            InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested),
            Vec::new(),
        )
    } else {
        match loaded_keys {
            Some(account_keys) => {
                let instructions = project_instructions(
                    &message,
                    metadata,
                    &account_keys,
                    request,
                    signer_matches
                        && (request.required_signer.is_none()
                            || matches!(recorded_status, ExecutionStatus::Succeeded)),
                    limits,
                    output_pool,
                )?;
                (InstructionCoverage::Complete, instructions)
            }
            None => (
                InstructionCoverage::Unknown(CoverageReason::MetadataAbsent),
                Vec::new(),
            ),
        }
    };

    let (token_balance_coverage, token_balances) = match &request.token_balances {
        TokenBalanceRequirement::None => (TokenBalanceCoverage::NotRequested, Vec::new()),
        requirement => match metadata {
            Some(metadata) => (
                TokenBalanceCoverage::Complete,
                project_token_balances(metadata, requirement, output_pool)?,
            ),
            None => (
                TokenBalanceCoverage::Unknown(CoverageReason::MetadataAbsent),
                Vec::new(),
            ),
        },
    };

    Ok(Some(CanonicalTransaction {
        header: TransactionHeader {
            tx_index,
            status,
            failed_outer_instruction_index,
            instruction_coverage,
            cpi_coverage,
        },
        primary_signature,
        required_signers,
        instructions,
        token_balance_coverage,
        token_balances,
    }))
}

/// Keep only metadata used by the query. Logs, lamport balances, rewards, and
/// return data are not part of these projections and need no owned objects.
fn decode_projected_metadata(
    slot: u64,
    frame: &[u8],
    out: &mut TransactionStatusMeta,
    zstd: &mut ZstdReusableDecoder,
    request: &ScanRequest,
) -> Result<(), MetadataDecodeError> {
    use crate::confirmed_block::{
        InnerInstruction, InnerInstructions, TokenBalance, TransactionError, UiTokenAmount,
    };
    use crate::metadata_decoder::{
        InnerInstructionVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
        slot_uses_protobuf_metadata, visit_protobuf_transaction_status_meta,
    };
    if !slot_uses_protobuf_metadata(slot) {
        return decode_transaction_status_meta_from_frame(slot, frame, out, zstd);
    }
    struct Projection<'a> {
        out: &'a mut TransactionStatusMeta,
        instructions: bool,
        balances: bool,
    }
    fn token(value: TokenBalanceVisit<'_>) -> TokenBalance {
        TokenBalance {
            account_index: value.account_index,
            mint: value.mint.into(),
            owner: value.owner.into(),
            program_id: value.program_id.into(),
            ui_token_amount: value.ui_token_amount.map(|amount| UiTokenAmount {
                ui_amount: amount.ui_amount,
                decimals: amount.decimals,
                amount: amount.amount.into(),
                ui_amount_string: amount.ui_amount_string.into(),
            }),
        }
    }
    impl<'a> TransactionStatusMetaVisitor<'a> for Projection<'_> {
        fn wants_status_error(&self) -> bool {
            true
        }
        fn wants_inner_instructions(&self) -> bool {
            true
        }
        fn wants_loaded_addresses(&self) -> bool {
            true
        }
        fn wants_pre_token_balances(&self) -> bool {
            self.balances
        }
        fn wants_post_token_balances(&self) -> bool {
            self.balances
        }
        fn status_error(&mut self, bytes: &'a [u8]) {
            self.out.err = Some(TransactionError {
                err: bytes.to_vec(),
            });
        }
        fn inner_instructions_none(&mut self, none: bool) {
            self.out.inner_instructions_none = none;
        }
        fn inner_instruction_group(&mut self, index: u32) {
            self.out.inner_instructions.push(InnerInstructions {
                index,
                instructions: Vec::new(),
            });
        }
        fn inner_instruction(&mut self, value: InnerInstructionVisit<'a>) {
            if self.instructions {
                self.out
                    .inner_instructions
                    .last_mut()
                    .expect("group precedes instruction")
                    .instructions
                    .push(InnerInstruction {
                        program_id_index: value.program_id_index,
                        accounts: value.accounts.to_vec(),
                        data: value.data.to_vec(),
                        stack_height: value.stack_height,
                    });
            }
        }
        fn loaded_writable_address(&mut self, bytes: &'a [u8]) {
            self.out.loaded_writable_addresses.push(bytes.to_vec());
        }
        fn loaded_readonly_address(&mut self, bytes: &'a [u8]) {
            self.out.loaded_readonly_addresses.push(bytes.to_vec());
        }
        fn pre_token_balance(&mut self, value: TokenBalanceVisit<'a>) {
            self.out.pre_token_balances.push(token(value));
        }
        fn post_token_balance(&mut self, value: TokenBalanceVisit<'a>) {
            self.out.post_token_balances.push(token(value));
        }
    }
    prost::Message::clear(out);
    let compressed = zstd
        .decompress_if_zstd(frame)
        .map_err(MetadataDecodeError::ZstdDecompress)?;
    visit_protobuf_transaction_status_meta(
        if compressed { zstd.output() } else { frame },
        &mut Projection {
            out,
            instructions: request.include_instructions,
            balances: !matches!(request.token_balances, TokenBalanceRequirement::None),
        },
    )
    .map_err(|error| MetadataDecodeError::ProtoConvert(error.to_string()))
}

/// Count and validate instruction geometry without materializing protobuf objects.
fn count_protobuf_transaction(
    message: &MessageView<'_>,
    frame: &[u8],
    zstd: &mut ZstdReusableDecoder,
    counts: &mut blockzilla_query_sdk::BlockCounts,
    limits: CarQueryLimits,
) -> CarQueryResult<()> {
    use crate::metadata_decoder::{
        InnerInstructionVisit, TransactionStatusMetaVisitor, visit_protobuf_transaction_status_meta,
    };
    struct Counter {
        outer: usize,
        accounts: usize,
        inner: usize,
        writable: usize,
        readonly: usize,
        previous_group: Option<u32>,
        not_recorded: bool,
        invalid: bool,
    }
    impl<'a> TransactionStatusMetaVisitor<'a> for Counter {
        fn wants_status_error(&self) -> bool {
            true
        }
        fn wants_inner_instructions(&self) -> bool {
            true
        }
        fn wants_loaded_addresses(&self) -> bool {
            true
        }
        fn status_error(&mut self, bytes: &'a [u8]) {
            self.invalid |= decode_failed_outer_bytes(bytes).is_err();
        }
        fn inner_instruction_group(&mut self, index: u32) {
            self.invalid |= index as usize >= self.outer
                || self
                    .previous_group
                    .is_some_and(|previous| index <= previous);
            self.previous_group = Some(index);
        }
        fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
            self.inner += 1;
            self.invalid |= instruction.program_id_index as usize >= self.accounts
                || instruction
                    .accounts
                    .iter()
                    .any(|&index| index as usize >= self.accounts)
                || instruction.data.len() > MAX_CANONICAL_SHORT_VEC_ITEMS;
        }
        fn inner_instructions_none(&mut self, none: bool) {
            self.not_recorded = none;
        }
        fn loaded_writable_address(&mut self, address: &'a [u8]) {
            self.writable += 1;
            self.invalid |= address.len() != 32;
        }
        fn loaded_readonly_address(&mut self, address: &'a [u8]) {
            self.readonly += 1;
            self.invalid |= address.len() != 32;
        }
    }
    let accounts =
        message.static_keys.len() + message.expected_loaded.0 + message.expected_loaded.1;
    let mut counter = Counter {
        outer: message.instructions.len(),
        accounts,
        inner: 0,
        writable: 0,
        readonly: 0,
        previous_group: None,
        not_recorded: false,
        invalid: false,
    };
    if !frame.is_empty() {
        let compressed = zstd
            .decompress_if_zstd(frame)
            .map_err(|error| CarQueryError::Metadata(MetadataDecodeError::ZstdDecompress(error)))?;
        visit_protobuf_transaction_status_meta(
            if compressed { zstd.output() } else { frame },
            &mut counter,
        )
        .map_err(|error| {
            CarQueryError::InvalidArchive(format!("metadata count decode: {error}"))
        })?;
        counter.invalid |= (counter.writable, counter.readonly) != message.expected_loaded
            || (counter.not_recorded && counter.previous_group.is_some());
    }
    let complete = !frame.is_empty() || message.expected_loaded == (0, 0);
    if complete {
        for instruction in message.instructions {
            counter.invalid |= instruction.program_id_index as usize >= accounts
                || instruction
                    .accounts
                    .iter()
                    .any(|&index| index as usize >= accounts)
                || instruction.data.len() > MAX_CANONICAL_SHORT_VEC_ITEMS;
        }
        let total = message.instructions.len() + counter.inner;
        counter.invalid |= total > limits.max_instructions_per_transaction
            || total > MAX_CANONICAL_SHORT_VEC_ITEMS;
        if !counter.invalid {
            counts.instructions += total as u64;
            counts.recorded_inner_instructions += counter.inner as u64;
        }
    }
    if counter.invalid {
        return Err(CarQueryError::InvalidArchive(
            "invalid metadata or instruction geometry in count scan".into(),
        ));
    }
    counts.transactions += 1;
    counts.incomplete_instructions += u64::from(!complete);
    counts.incomplete_cpi += u64::from(frame.is_empty() || counter.not_recorded);
    Ok(())
}

fn project_token_balances(
    metadata: &TransactionStatusMeta,
    requirement: &TokenBalanceRequirement,
    pool: &mut blockzilla_query_sdk::projection_pool::ProjectionPool,
) -> CarQueryResult<Vec<RecordedTokenBalance>> {
    let capacity = metadata
        .pre_token_balances
        .len()
        .checked_add(metadata.post_token_balances.len())
        .ok_or_else(|| CarQueryError::InvalidArchive("token-balance count overflow".into()))?;
    let mut output = pool.balances();
    if matches!(requirement, TokenBalanceRequirement::All) {
        output
            .try_reserve(capacity)
            .map_err(|_| CarQueryError::InvalidArchive("token-balance allocation failed".into()))?;
    }
    for (side, balances) in [
        (TokenBalanceSide::Pre, &metadata.pre_token_balances),
        (TokenBalanceSide::Post, &metadata.post_token_balances),
    ] {
        for (balance_index, balance) in balances.iter().enumerate() {
            let mint = parse_optional_pubkey(&balance.mint);
            if !requirement.selects(mint.as_ref()) {
                continue;
            }
            let (amount, decimals) = match &balance.ui_token_amount {
                Some(amount) => (
                    amount.amount.parse::<u64>().map_err(|error| {
                        CarQueryError::InvalidArchive(format!(
                            "token amount {:?} is not u64: {error}",
                            amount.amount
                        ))
                    })?,
                    amount.decimals as u8,
                ),
                None => (0, 0),
            };
            output.push(RecordedTokenBalance {
                side,
                balance_index: u32::try_from(balance_index).map_err(|_| {
                    CarQueryError::InvalidArchive("token-balance index exceeds u32".into())
                })?,
                account_index: balance.account_index,
                mint,
                owner: parse_optional_pubkey(&balance.owner),
                token_program: parse_optional_pubkey(&balance.program_id),
                amount,
                decimals,
            });
        }
    }
    Ok(output)
}

fn parse_optional_pubkey(value: &str) -> Option<[u8; 32]> {
    if value.is_empty() {
        return None;
    }
    let mut decoded = [0_u8; 32];
    (bs58::decode(value).onto(&mut decoded).ok()? == 32).then_some(decoded)
}

struct MessageView<'a> {
    header: MessageHeader,
    static_keys: &'a [&'a [u8; 32]],
    instructions: &'a [CompiledInstruction],
    expected_loaded: (usize, usize),
}

fn message_view<'a>(message: &'a VersionedMessage<'_>) -> CarQueryResult<MessageView<'a>> {
    match message {
        VersionedMessage::Legacy(message) => Ok(MessageView {
            header: message.header,
            static_keys: &message.account_keys,
            instructions: &message.instructions,
            expected_loaded: (0, 0),
        }),
        VersionedMessage::V0(message) => {
            let mut writable = 0usize;
            let mut readonly = 0usize;
            for lookup in &message.address_table_lookups {
                writable = writable
                    .checked_add(lookup.writable_indexes.len())
                    .ok_or_else(|| {
                        CarQueryError::InvalidArchive("loaded writable count overflow".into())
                    })?;
                readonly = readonly
                    .checked_add(lookup.readonly_indexes.len())
                    .ok_or_else(|| {
                        CarQueryError::InvalidArchive("loaded readonly count overflow".into())
                    })?;
            }
            Ok(MessageView {
                header: message.header,
                static_keys: &message.account_keys,
                instructions: &message.instructions,
                expected_loaded: (writable, readonly),
            })
        }
        VersionedMessage::V1(message) => Ok(MessageView {
            header: message.header,
            static_keys: &message.account_keys,
            instructions: &message.instructions,
            expected_loaded: (0, 0),
        }),
    }
}

fn validate_message_geometry(
    transaction: &VersionedTransaction<'_>,
    message: &MessageView<'_>,
    limits: CarQueryLimits,
) -> CarQueryResult<()> {
    let required = usize::from(message.header.num_required_signatures);
    if transaction.signatures.len() != required {
        return Err(CarQueryError::InvalidArchive(format!(
            "transaction has {} signatures but message requires {required}",
            transaction.signatures.len()
        )));
    }
    if required > message.static_keys.len() {
        return Err(CarQueryError::InvalidArchive(
            "required signer count exceeds static account keys".into(),
        ));
    }
    if usize::from(message.header.num_readonly_signed_accounts) > required {
        return Err(CarQueryError::InvalidArchive(
            "readonly signed count exceeds required signer count".into(),
        ));
    }
    let unsigned = message.static_keys.len() - required;
    if usize::from(message.header.num_readonly_unsigned_accounts) > unsigned {
        return Err(CarQueryError::InvalidArchive(
            "readonly unsigned count exceeds unsigned static keys".into(),
        ));
    }
    let total_accounts = message
        .static_keys
        .len()
        .checked_add(message.expected_loaded.0)
        .and_then(|value| value.checked_add(message.expected_loaded.1))
        .ok_or_else(|| CarQueryError::InvalidArchive("message account count overflow".into()))?;
    if total_accounts > limits.max_resolved_accounts {
        return Err(CarQueryError::InvalidArchive(format!(
            "message has {total_accounts} resolved accounts, limit is {}",
            limits.max_resolved_accounts
        )));
    }
    if message.instructions.len() > limits.max_instructions_per_transaction {
        return Err(CarQueryError::InvalidArchive(format!(
            "message has {} instructions, limit is {}",
            message.instructions.len(),
            limits.max_instructions_per_transaction
        )));
    }
    Ok(())
}

fn validate_metadata_geometry(
    message: &MessageView<'_>,
    metadata: &TransactionStatusMeta,
    limits: CarQueryLimits,
) -> CarQueryResult<()> {
    if metadata.inner_instructions_none && !metadata.inner_instructions.is_empty() {
        return Err(CarQueryError::InvalidArchive(
            "metadata marks CPI not recorded but contains inner groups".into(),
        ));
    }
    if metadata.loaded_writable_addresses.len() != message.expected_loaded.0
        || metadata.loaded_readonly_addresses.len() != message.expected_loaded.1
    {
        return Err(CarQueryError::InvalidArchive(format!(
            "loaded-address counts ({}, {}) differ from message lookup counts ({}, {})",
            metadata.loaded_writable_addresses.len(),
            metadata.loaded_readonly_addresses.len(),
            message.expected_loaded.0,
            message.expected_loaded.1
        )));
    }
    let total = message
        .static_keys
        .len()
        .checked_add(metadata.loaded_writable_addresses.len())
        .and_then(|value| value.checked_add(metadata.loaded_readonly_addresses.len()))
        .ok_or_else(|| CarQueryError::InvalidArchive("metadata account count overflow".into()))?;
    if total > limits.max_resolved_accounts {
        return Err(CarQueryError::InvalidArchive(format!(
            "metadata resolves {total} accounts, limit is {}",
            limits.max_resolved_accounts
        )));
    }
    let mut previous = None;
    for group in &metadata.inner_instructions {
        let outer = usize::try_from(group.index)
            .map_err(|_| CarQueryError::InvalidArchive("inner group index exceeds usize".into()))?;
        if outer >= message.instructions.len() {
            return Err(CarQueryError::InvalidArchive(format!(
                "inner group index {outer} is outside {} outer instructions",
                message.instructions.len()
            )));
        }
        if previous.is_some_and(|value| group.index <= value) {
            return Err(CarQueryError::InvalidArchive(
                "inner groups are not strictly ordered by outer index".into(),
            ));
        }
        previous = Some(group.index);
    }
    Ok(())
}

/// Borrow the three key lanes; do not concatenate them per transaction.
struct AccountKeys<'a> {
    static_keys: &'a [&'a [u8; 32]],
    writable: &'a [Vec<u8>],
    readonly: &'a [Vec<u8>],
}

impl<'a> AccountKeys<'a> {
    fn new(
        message: &MessageView<'a>,
        metadata: Option<&'a TransactionStatusMeta>,
    ) -> CarQueryResult<Self> {
        let writable = metadata.map_or(&[][..], |meta| meta.loaded_writable_addresses.as_slice());
        let readonly = metadata.map_or(&[][..], |meta| meta.loaded_readonly_addresses.as_slice());
        if writable
            .iter()
            .chain(readonly)
            .any(|bytes| bytes.len() != 32)
        {
            return Err(CarQueryError::InvalidArchive(
                "loaded address is not 32 bytes".into(),
            ));
        }
        Ok(Self {
            static_keys: message.static_keys,
            writable,
            readonly,
        })
    }
    fn len(&self) -> usize {
        self.static_keys.len() + self.writable.len() + self.readonly.len()
    }
    fn get(&self, index: usize) -> Option<&[u8; 32]> {
        if let Some(key) = self.static_keys.get(index) {
            return Some(*key);
        }
        let index = index.checked_sub(self.static_keys.len())?;
        let bytes = if index < self.writable.len() {
            &self.writable[index]
        } else {
            self.readonly.get(index - self.writable.len())?
        };
        bytes.as_slice().try_into().ok()
    }
}

fn decode_failed_outer_index(metadata: &TransactionStatusMeta) -> CarQueryResult<Option<u32>> {
    let Some(error) = &metadata.err else {
        return Ok(None);
    };
    decode_failed_outer_bytes(&error.err)
}

fn decode_failed_outer_bytes(bytes: &[u8]) -> CarQueryResult<Option<u32>> {
    let decoded = match wincode::deserialize_exact::<StoredTransactionError>(bytes) {
        Ok(decoded) => decoded,
        Err(err) => {
            if let Some(index) = decode_legacy_unit_borsh_io_error_index(bytes) {
                return Ok(Some(index));
            }
            return Err(CarQueryError::InvalidArchive(format!(
                "transaction error payload is not exact stored-error bytes: {err}"
            )));
        }
    };
    Ok(match decoded {
        StoredTransactionError::InstructionError(index, _) => Some(u32::from(index)),
        _ => None,
    })
}

/// Decode the exact old nine-byte form where `BorshIoError` was a unit variant.
/// Newer stored errors encode its string payload and use the normal decoder.
fn decode_legacy_unit_borsh_io_error_index(bytes: &[u8]) -> Option<u32> {
    const TRANSACTION_ERROR_INSTRUCTION_ERROR: u32 = 8;
    const INSTRUCTION_ERROR_BORSH_IO_ERROR: u32 = 44;

    if bytes.len() != 9 {
        return None;
    }
    let transaction_error_tag = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let instruction_error_tag = u32::from_le_bytes(bytes[5..9].try_into().ok()?);
    if transaction_error_tag != TRANSACTION_ERROR_INSTRUCTION_ERROR
        || instruction_error_tag != INSTRUCTION_ERROR_BORSH_IO_ERROR
    {
        return None;
    }
    Some(u32::from(bytes[4]))
}

fn project_instructions(
    message: &MessageView<'_>,
    metadata: Option<&TransactionStatusMeta>,
    account_keys: &AccountKeys<'_>,
    request: &ScanRequest,
    include_programs: bool,
    limits: CarQueryLimits,
    pool: &mut blockzilla_query_sdk::projection_pool::ProjectionPool,
) -> CarQueryResult<Vec<ResolvedInstruction>> {
    let groups = metadata
        .filter(|metadata| !metadata.inner_instructions_none)
        .map(|metadata| metadata.inner_instructions.as_slice())
        .unwrap_or_default();
    let inner_count = groups.iter().try_fold(0usize, |total, group| {
        total
            .checked_add(group.instructions.len())
            .ok_or_else(|| CarQueryError::InvalidArchive("inner instruction count overflow".into()))
    })?;
    let total = message
        .instructions
        .len()
        .checked_add(inner_count)
        .ok_or_else(|| CarQueryError::InvalidArchive("instruction count overflow".into()))?;
    if total > limits.max_instructions_per_transaction || total > MAX_CANONICAL_SHORT_VEC_ITEMS {
        return Err(CarQueryError::InvalidArchive(format!(
            "canonical instruction count {total} exceeds configured or query limit"
        )));
    }

    let mut output = pool.instructions();
    output
        .try_reserve(total)
        .map_err(|_| CarQueryError::InvalidArchive("instruction allocation failed".into()))?;
    let mut next_group = groups.iter().peekable();
    for (outer_index, instruction) in message.instructions.iter().enumerate() {
        push_instruction(
            &mut output,
            pool,
            outer_index,
            None,
            None,
            u32::from(instruction.program_id_index),
            &instruction.accounts,
            &instruction.data,
            account_keys,
            request,
            include_programs,
        )?;
        if next_group
            .peek()
            .is_some_and(|group| group.index as usize == outer_index)
        {
            let group = next_group.next().expect("peek proved a group");
            for (inner_index, instruction) in group.instructions.iter().enumerate() {
                push_instruction(
                    &mut output,
                    pool,
                    outer_index,
                    Some(inner_index),
                    instruction.stack_height,
                    instruction.program_id_index,
                    &instruction.accounts,
                    &instruction.data,
                    account_keys,
                    request,
                    include_programs,
                )?;
            }
        }
    }
    if next_group.next().is_some() {
        return Err(CarQueryError::InvalidArchive(
            "an inner group has no matching outer instruction".into(),
        ));
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn push_instruction(
    output: &mut Vec<ResolvedInstruction>,
    pool: &mut blockzilla_query_sdk::projection_pool::ProjectionPool,
    outer_index: usize,
    inner_index: Option<usize>,
    stack_height: Option<u32>,
    program_id_index: u32,
    account_indexes: &[u8],
    data: &[u8],
    account_keys: &AccountKeys<'_>,
    request: &ScanRequest,
    include_programs: bool,
) -> CarQueryResult<()> {
    let program_index = usize::try_from(program_id_index)
        .map_err(|_| CarQueryError::InvalidArchive("program index exceeds usize".into()))?;
    if program_index >= account_keys.len() {
        return Err(CarQueryError::InvalidArchive(
            "program index exceeds account count".into(),
        ));
    }
    let program_id = if !include_programs
        || matches!(
            request.instruction_programs,
            InstructionDataRequirement::None
        ) {
        None
    } else {
        let key = *account_keys
            .get(program_index)
            .expect("validated account geometry");
        match &request.instruction_programs {
            InstructionDataRequirement::Programs(keys) => keys.contains(&key).then_some(key),
            _ => Some(key),
        }
    };
    let accounts = if request.include_instruction_accounts {
        let mut accounts = pool.accounts();
        accounts.try_reserve(account_indexes.len()).map_err(|_| {
            CarQueryError::InvalidArchive("instruction account allocation failed".into())
        })?;
        for index in account_indexes {
            accounts.push(*account_keys.get(usize::from(*index)).ok_or_else(|| {
                CarQueryError::InvalidArchive(format!(
                    "instruction account index {index} is outside {} resolved accounts",
                    account_keys.len()
                ))
            })?);
        }
        accounts
    } else {
        for index in account_indexes {
            if usize::from(*index) >= account_keys.len() {
                return Err(CarQueryError::InvalidArchive(format!(
                    "instruction account index {index} is outside {} resolved accounts",
                    account_keys.len()
                )));
            }
        }
        Vec::new()
    };
    if data.len() > MAX_CANONICAL_SHORT_VEC_ITEMS {
        return Err(CarQueryError::InvalidArchive(format!(
            "instruction data length {} exceeds canonical short-vector limit",
            data.len()
        )));
    }
    let selected = program_id
        .as_ref()
        .is_some_and(|key| instruction_data_required(&request.instruction_data, key));
    let (data_coverage, data) = if selected {
        let selected_data = pool.copy_data(data).map_err(|_| {
            CarQueryError::InvalidArchive("instruction data allocation failed".into())
        })?;
        (InstructionDataCoverage::Exact, selected_data)
    } else {
        (InstructionDataCoverage::NotRequested, Vec::new())
    };
    output.push(ResolvedInstruction {
        coordinate: InstructionCoordinate {
            order: u32::try_from(output.len()).map_err(|_| {
                CarQueryError::InvalidArchive("instruction order exceeds u32".into())
            })?,
            outer_index: u32::try_from(outer_index).map_err(|_| {
                CarQueryError::InvalidArchive("outer instruction index exceeds u32".into())
            })?,
            inner_index: inner_index.map(u32::try_from).transpose().map_err(|_| {
                CarQueryError::InvalidArchive("inner instruction index exceeds u32".into())
            })?,
            stack_height,
        },
        program_id,
        accounts,
        data_coverage,
        data,
    });
    Ok(())
}

fn instruction_data_required(
    requirement: &InstructionDataRequirement,
    program_id: &[u8; 32],
) -> bool {
    match requirement {
        InstructionDataRequirement::All => true,
        InstructionDataRequirement::Programs(programs) => programs.contains(program_id),
        InstructionDataRequirement::None => false,
    }
}

fn reserved_vec<T>(capacity: usize, label: &str) -> CarQueryResult<Vec<T>> {
    let mut output = Vec::new();
    output.try_reserve_exact(capacity).map_err(|error| {
        CarQueryError::Allocation(format!("{label} capacity {capacity}: {error}"))
    })?;
    Ok(output)
}

fn source_error(error: CarQueryError) -> QueryError {
    QueryError::source(ArchiveFormat::Car, error)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor},
        num::NonZeroU32,
    };

    use blockzilla_query_sdk::{
        ArchiveInstructionSourceExt, CpiCoverage, ExecutionStatus, InstructionDataCoverage,
        ScanRange,
    };
    use minicbor::Encoder;
    use prost::Message;

    use super::*;
    use crate::{
        confirmed_block::{
            InnerInstruction, InnerInstructions, TokenBalance, TransactionError,
            TransactionStatusMeta, UiTokenAmount,
        },
        stored_transaction::InstructionError as StoredInstructionError,
    };

    const FIRST_SLOT: u64 = 80_000_000;

    #[test]
    fn real_car_blocks_have_the_same_counts_with_borrowed_metadata() {
        for bytes in [
            include_bytes!("../benches/fixtures/epoch-157-biggest.car").as_slice(),
            include_bytes!("../benches/fixtures/epoch-822-biggest.car").as_slice(),
        ] {
            let limits = CarQueryLimits::default();
            let mut reader =
                CarBlockReader::with_capacity(Cursor::new(bytes), limits.io_buffer_bytes);
            reader.skip_header_bounded(limits.max_header_bytes).unwrap();
            let mut raw = OrderedLosslessCarBlock::default();
            let mut slots = Vec::new();
            while reader
                .read_until_block_ordered_lossless_bounded(&mut raw, limits.block_read_limits())
                .unwrap()
            {
                slots.push(block_slot(&raw).unwrap());
            }
            assert!(!slots.is_empty());
            let mut id = identity(&slots);
            id.first_slot = slots[0];
            id.slots_per_epoch = slots.last().unwrap() - slots[0] + 1;
            let full_request = ScanRequest::all()
                .allow_incomplete_instructions()
                .allow_incomplete_cpi()
                .without_instruction_data()
                .without_instruction_accounts();
            let mut full = CarInstructionSource::new(
                Cursor::new(bytes),
                id.clone(),
                CanonicalBlockPlan::new(slots.clone()),
                limits,
            )
            .unwrap();
            let mut inner = 0u64;
            let receipt = full
                .for_each_block(&full_request, |block| {
                    inner += block
                        .transactions
                        .iter()
                        .flat_map(|tx| &tx.instructions)
                        .filter(|ix| ix.coordinate.inner_index.is_some())
                        .count() as u64;
                    Ok(())
                })
                .unwrap();
            let mut count = CarInstructionSource::new(
                Cursor::new(bytes),
                id,
                CanonicalBlockPlan::new(slots),
                limits,
            )
            .unwrap();
            let mut counted_inner = 0;
            let counted = count
                .for_each_block(&full_request.count_instructions_only(), |block| {
                    counted_inner += block.counts.unwrap().recorded_inner_instructions;
                    Ok(())
                })
                .unwrap();
            assert_eq!(counted.transactions, receipt.transactions);
            assert_eq!(counted.instructions, receipt.instructions);
            assert_eq!(counted_inner, inner);
            assert_eq!(
                counted.transactions_with_incomplete_cpi,
                receipt.transactions_with_incomplete_cpi
            );
            assert_eq!(
                counted.transactions_with_incomplete_instructions,
                receipt.transactions_with_incomplete_instructions
            );
        }
    }

    #[test]
    fn protobuf_count_matches_full_projection_and_rejects_bad_groups() {
        let bytes = simple_transaction(7);
        let tx = wincode::deserialize_exact::<VersionedTransaction<'_>>(&bytes).unwrap();
        let message = message_view(&tx.message).unwrap();
        for compressed in [false, true] {
            let meta = TransactionStatusMeta {
                inner_instructions: vec![InnerInstructions {
                    index: 0,
                    instructions: vec![InnerInstruction {
                        program_id_index: 1,
                        accounts: vec![0],
                        data: vec![8],
                        stack_height: Some(2),
                    }],
                }],
                log_messages: vec!["unused log".repeat(100)],
                ..Default::default()
            };
            let wire = meta.encode_to_vec();
            let frame = if compressed {
                zstd::bulk::compress(&wire, 1).unwrap()
            } else {
                wire
            };
            let mut counts = blockzilla_query_sdk::BlockCounts::default();
            count_protobuf_transaction(
                &message,
                &frame,
                &mut ZstdReusableDecoder::new(),
                &mut counts,
                CarQueryLimits::default(),
            )
            .unwrap();
            assert_eq!(counts.transactions, 1);
            assert_eq!(counts.instructions, message.instructions.len() as u64 + 1);
            assert_eq!(counts.recorded_inner_instructions, 1);
            assert_eq!(counts.incomplete_cpi, 0);
        }
        let bad = TransactionStatusMeta {
            inner_instructions: vec![InnerInstructions {
                index: message.instructions.len() as u32,
                instructions: Vec::new(),
            }],
            ..Default::default()
        }
        .encode_to_vec();
        assert!(
            count_protobuf_transaction(
                &message,
                &bad,
                &mut ZstdReusableDecoder::new(),
                &mut Default::default(),
                CarQueryLimits::default()
            )
            .is_err()
        );
    }

    #[derive(Clone)]
    struct FixtureTransaction {
        cid: [u8; 36],
        transaction: Vec<u8>,
        metadata: Vec<u8>,
        index: Option<u64>,
    }

    fn identity(plan: &[u64]) -> SourceIdentity {
        SourceIdentity {
            format: ArchiveFormat::Car,
            label: "trusted-fixture.car".into(),
            cluster_id: Some("fixture-cluster".into()),
            epoch: 185,
            first_slot: FIRST_SLOT,
            slots_per_epoch: 16,
            block_count: plan.len() as u32,
            verification: SourceVerification::OperatorTrusted,
            binding: Some("fixture-root-cid".into()),
        }
    }

    fn source(car: Vec<u8>, plan: Vec<u64>) -> CarInstructionSource<Cursor<Vec<u8>>> {
        CarInstructionSource::new(
            Cursor::new(car),
            identity(&plan),
            CanonicalBlockPlan::new(plan),
            CarQueryLimits::default(),
        )
        .unwrap()
    }

    fn collect(
        source: &mut CarInstructionSource<Cursor<Vec<u8>>>,
        request: &ScanRequest,
    ) -> blockzilla_query_sdk::Result<(ScanReceipt, Vec<CanonicalBlock>)> {
        let mut blocks = Vec::new();
        let receipt = source.for_each_block(request, |block| {
            blocks.push(CanonicalBlock {
                counts: None,
                header: block.header,
                transactions: block.transactions.to_vec(),
            });
            Ok(())
        })?;
        Ok((receipt, blocks))
    }

    fn car_header() -> Vec<u8> {
        vec![1, 0]
    }

    fn cid(fill: u8) -> [u8; 36] {
        let mut output = [fill; 36];
        output[..4].copy_from_slice(&[1, 0x71, 0x12, 0x20]);
        output
    }

    fn append_block(
        car: &mut Vec<u8>,
        slot: u64,
        transactions: &[FixtureTransaction],
        file_order: &[usize],
        canonical_order: &[usize],
        entry_fill: u8,
    ) {
        for &position in file_order {
            let transaction = &transactions[position];
            let payload = transaction_node(
                slot,
                transaction.index,
                &transaction.transaction,
                &transaction.metadata,
            );
            push_car_entry(car, transaction.cid, &payload);
        }
        let entry_cid = cid(entry_fill);
        let tx_cids = canonical_order
            .iter()
            .map(|position| transactions[*position].cid)
            .collect::<Vec<_>>();
        push_car_entry(car, entry_cid, &entry_node(&tx_cids));
        push_car_entry(
            car,
            cid(entry_fill.wrapping_add(1)),
            &block_node(slot, entry_cid),
        );
    }

    fn push_car_entry(car: &mut Vec<u8>, cid: [u8; 36], payload: &[u8]) {
        push_uvarint(car, (cid.len() + payload.len()) as u64);
        car.extend_from_slice(&cid);
        car.extend_from_slice(payload);
    }

    fn push_uvarint(output: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn encode_cid_ref(encoder: &mut Encoder<Vec<u8>>, cid: [u8; 36]) {
        let mut bytes = [0u8; 37];
        bytes[1..].copy_from_slice(&cid);
        encoder.tag(minicbor::data::Tag::new(42)).unwrap();
        encoder.bytes(&bytes).unwrap();
    }

    fn encode_dataframe(encoder: &mut Encoder<Vec<u8>>, data: &[u8]) {
        encoder.array(6).unwrap().u64(6).unwrap();
        encoder.null().unwrap();
        encoder.null().unwrap();
        encoder.null().unwrap();
        encoder.bytes(data).unwrap();
        encoder.null().unwrap();
    }

    fn transaction_node(
        slot: u64,
        index: Option<u64>,
        transaction: &[u8],
        metadata: &[u8],
    ) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(5).unwrap().u64(0).unwrap();
        encode_dataframe(&mut encoder, transaction);
        encode_dataframe(&mut encoder, metadata);
        encoder.u64(slot).unwrap();
        if let Some(index) = index {
            encoder.u64(index).unwrap();
        } else {
            encoder.null().unwrap();
        }
        encoder.into_writer()
    }

    fn entry_node(transactions: &[[u8; 36]]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(4).unwrap().u64(1).unwrap();
        encoder.u64(0).unwrap();
        encoder.bytes(&[7; 32]).unwrap();
        encoder.array(transactions.len() as u64).unwrap();
        for transaction in transactions {
            encode_cid_ref(&mut encoder, *transaction);
        }
        encoder.into_writer()
    }

    fn block_node(slot: u64, entry: [u8; 36]) -> Vec<u8> {
        block_node_entries(slot, &[entry])
    }

    fn block_node_entries(slot: u64, entries: &[[u8; 36]]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(5).unwrap().u64(2).unwrap();
        encoder.u64(slot).unwrap();
        encoder.array(0).unwrap();
        encoder.array(entries.len() as u64).unwrap();
        for entry in entries {
            encode_cid_ref(&mut encoder, *entry);
        }
        encoder.array(3).unwrap();
        encoder.u64(slot.saturating_sub(1)).unwrap();
        encoder.i64(1_700_000_000).unwrap();
        encoder.u64(9).unwrap();
        encoder.into_writer()
    }

    fn dataframe_node(data: &[u8]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encode_dataframe(&mut encoder, data);
        encoder.into_writer()
    }

    fn rewards_node(slot: u64, data: &[u8]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(3).unwrap().u64(5).unwrap();
        encoder.u64(slot).unwrap();
        encode_dataframe(&mut encoder, data);
        encoder.into_writer()
    }

    fn subset_node(first: u64, last: u64) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(4).unwrap().u64(3).unwrap();
        encoder.u64(first).unwrap();
        encoder.u64(last).unwrap();
        encoder.array(0).unwrap();
        encoder.into_writer()
    }

    fn epoch_node(epoch: u64) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new());
        encoder.array(3).unwrap().u64(4).unwrap();
        encoder.u64(epoch).unwrap();
        encoder.array(0).unwrap();
        encoder.into_writer()
    }

    fn push_short_vec(output: &mut Vec<u8>, len: usize) {
        assert!(len < 128, "tiny fixture uses one-byte short vectors");
        output.push(len as u8);
    }

    fn encode_instructions(output: &mut Vec<u8>, instructions: &[(u8, &[u8], &[u8])]) {
        push_short_vec(output, instructions.len());
        for (program, accounts, data) in instructions {
            output.push(*program);
            push_short_vec(output, accounts.len());
            output.extend_from_slice(accounts);
            push_short_vec(output, data.len());
            output.extend_from_slice(data);
        }
    }

    fn legacy_transaction(
        signature: [u8; 64],
        keys: &[[u8; 32]],
        instructions: &[(u8, &[u8], &[u8])],
    ) -> Vec<u8> {
        let mut output = Vec::new();
        push_short_vec(&mut output, 1);
        output.extend_from_slice(&signature);
        output.extend_from_slice(&[1, 0, 0]);
        push_short_vec(&mut output, keys.len());
        for key in keys {
            output.extend_from_slice(key);
        }
        output.extend_from_slice(&[4; 32]);
        encode_instructions(&mut output, instructions);
        output
    }

    fn v0_transaction(
        signature: [u8; 64],
        keys: &[[u8; 32]],
        instructions: &[(u8, &[u8], &[u8])],
        writable_lookup_indexes: &[u8],
        readonly_lookup_indexes: &[u8],
    ) -> Vec<u8> {
        let mut output = Vec::new();
        push_short_vec(&mut output, 1);
        output.extend_from_slice(&signature);
        output.push(0x80);
        output.extend_from_slice(&[1, 0, 0]);
        push_short_vec(&mut output, keys.len());
        for key in keys {
            output.extend_from_slice(key);
        }
        output.extend_from_slice(&[4; 32]);
        encode_instructions(&mut output, instructions);
        push_short_vec(&mut output, 1);
        output.extend_from_slice(&[5; 32]);
        push_short_vec(&mut output, writable_lookup_indexes.len());
        output.extend_from_slice(writable_lookup_indexes);
        push_short_vec(&mut output, readonly_lookup_indexes.len());
        output.extend_from_slice(readonly_lookup_indexes);
        output
    }

    fn v1_transaction(
        signature: [u8; 64],
        keys: &[[u8; 32]],
        instructions: &[(u8, &[u8], &[u8])],
    ) -> Vec<u8> {
        assert!(keys.len() <= 64);
        assert!(instructions.len() <= 64);
        let mut output = Vec::new();
        push_short_vec(&mut output, 1);
        output.extend_from_slice(&signature);
        output.push(0x81);
        output.extend_from_slice(&[1, 0, 0]);
        output.extend_from_slice(&0u32.to_le_bytes());
        output.extend_from_slice(&[4; 32]);
        output.push(instructions.len() as u8);
        output.push(keys.len() as u8);
        for key in keys {
            output.extend_from_slice(key);
        }
        for (program, accounts, data) in instructions {
            output.push(*program);
            output.push(accounts.len() as u8);
            output.extend_from_slice(&(data.len() as u16).to_le_bytes());
        }
        for (_, accounts, data) in instructions {
            output.extend_from_slice(accounts);
            output.extend_from_slice(data);
        }
        output
    }

    fn metadata(metadata: TransactionStatusMeta) -> Vec<u8> {
        metadata.encode_to_vec()
    }

    fn empty_exact_metadata() -> Vec<u8> {
        metadata(TransactionStatusMeta {
            // A fully default protobuf message encodes to zero bytes, which the
            // CAR format uses for absent metadata. Keep this fixture nonempty.
            fee: 1,
            inner_instructions_none: false,
            ..TransactionStatusMeta::default()
        })
    }

    fn simple_transaction(fill: u8) -> Vec<u8> {
        let keys = [[fill; 32], [fill.wrapping_add(1); 32]];
        legacy_transaction([fill; 64], &keys, &[(1, &[0], &[fill])])
    }

    fn token_balance(
        account_index: u32,
        mint: String,
        owner: String,
        program_id: String,
        amount: u64,
    ) -> TokenBalance {
        TokenBalance {
            account_index,
            mint,
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: 0.0,
                decimals: 6,
                amount: amount.to_string(),
                ui_amount_string: String::new(),
            }),
            owner,
            program_id,
        }
    }

    #[test]
    fn source_identity_and_plan_are_explicit_and_warmup_safe() {
        let plan = vec![FIRST_SLOT, FIRST_SLOT + 3];
        let valid = CarInstructionSource::new(
            Cursor::new(car_header()),
            identity(&plan),
            CanonicalBlockPlan::new(plan.clone()),
            CarQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(valid.identity().first_slot, FIRST_SLOT);
        assert_ne!(
            u64::from(valid.identity().block_count),
            valid.identity().slots_per_epoch
        );
        assert_eq!(
            valid.identity().binding.as_deref(),
            Some("car-object-binding=fixture-root-cid")
        );

        let mut descriptor_identity = identity(&plan);
        descriptor_identity.binding = Some("url-length-descriptor-v1".into());
        let descriptor = CarInstructionSource::new_operator_trusted_descriptor(
            Cursor::new(car_header()),
            descriptor_identity,
            CanonicalBlockPlan::new(plan.clone()),
            CarQueryLimits::default(),
        )
        .unwrap();
        let descriptor_binding = descriptor.identity().binding.as_deref().unwrap();
        assert_eq!(
            descriptor_binding,
            "operator-trusted-input-descriptor=url-length-descriptor-v1"
        );
        assert!(!descriptor_binding.contains("car-object-binding"));

        let mut no_binding = identity(&plan);
        no_binding.binding = None;
        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                no_binding,
                CanonicalBlockPlan::new(plan.clone()),
                CarQueryLimits::default(),
            )
            .is_err()
        );
        let mut empty_binding = identity(&plan);
        empty_binding.binding = Some("   ".into());
        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                empty_binding,
                CanonicalBlockPlan::new(plan.clone()),
                CarQueryLimits::default(),
            )
            .is_err()
        );

        let other_plan = vec![FIRST_SLOT, FIRST_SLOT + 4];
        let other = CarInstructionSource::new(
            Cursor::new(car_header()),
            identity(&other_plan),
            CanonicalBlockPlan::new(other_plan),
            CarQueryLimits::default(),
        )
        .unwrap();
        assert_eq!(valid.identity().binding, other.identity().binding);

        let mut wrong_count = identity(&plan);
        wrong_count.block_count = 1;
        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                wrong_count,
                CanonicalBlockPlan::new(plan.clone()),
                CarQueryLimits::default(),
            )
            .is_err()
        );

        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                identity(&plan),
                CanonicalBlockPlan::new(vec![FIRST_SLOT, FIRST_SLOT + 16]),
                CarQueryLimits::default(),
            )
            .is_err()
        );

        let mut wrong_trust = identity(&plan);
        wrong_trust.verification = SourceVerification::Unverified;
        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                wrong_trust,
                CanonicalBlockPlan::new(plan.clone()),
                CarQueryLimits::default(),
            )
            .is_err()
        );

        assert!(
            CarInstructionSource::new(
                Cursor::new(car_header()),
                identity(&plan),
                CanonicalBlockPlan::new(vec![FIRST_SLOT + 3, FIRST_SLOT]),
                CarQueryLimits::default(),
            )
            .is_err()
        );
    }

    #[test]
    fn plan_synthesizes_empty_rows_and_requires_physical_transaction_order() {
        let plan = vec![FIRST_SLOT, FIRST_SLOT + 2, FIRST_SLOT + 5];
        let first = FixtureTransaction {
            cid: cid(0x21),
            transaction: simple_transaction(1),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let second = FixtureTransaction {
            cid: cid(0x22),
            transaction: simple_transaction(2),
            metadata: empty_exact_metadata(),
            index: Some(1),
        };
        let mut car = car_header();
        append_block(
            &mut car,
            FIRST_SLOT + 2,
            &[first.clone(), second.clone()],
            &[1, 0],
            &[0, 1],
            0x31,
        );
        // The streaming adapter rejects reordered frames; it does not build a CID table.
        let error = collect(&mut source(car, plan.clone()), &ScanRequest::all()).unwrap_err();
        assert!(format!("{error:?}").contains("transaction frame index 1"));
        let mut car = car_header();
        append_block(
            &mut car,
            FIRST_SLOT + 2,
            &[first, second],
            &[0, 1],
            &[0, 1],
            0x31,
        );
        append_block(&mut car, FIRST_SLOT + 5, &[], &[], &[], 0x41);
        let car_len = car.len() as u64;

        let mut source = source(car, plan);
        let (receipt, blocks) = collect(&mut source, &ScanRequest::all()).unwrap();
        assert_eq!(receipt.blocks, 3);
        assert_eq!(receipt.transactions, 2);
        assert_eq!(
            blocks
                .iter()
                .map(|block| block.header.slot)
                .collect::<Vec<_>>(),
            vec![FIRST_SLOT, FIRST_SLOT + 2, FIRST_SLOT + 5,]
        );
        assert!(blocks[0].transactions.is_empty());
        assert_eq!(blocks[1].transactions[0].primary_signature, Some([1; 64]));
        assert_eq!(blocks[1].transactions[1].primary_signature, Some([2; 64]));
        assert!(blocks[2].transactions.is_empty());
        assert!(receipt.io.source_read_calls.is_some_and(|calls| calls > 0));
        assert_eq!(receipt.io.source_read_bytes, Some(car_len));
        assert_eq!(receipt.io.decoded_bytes, None);
    }

    #[test]
    fn omitted_primary_signature_keeps_the_same_car_reads() {
        let plan = vec![FIRST_SLOT];
        let transaction = FixtureTransaction {
            cid: cid(0x42),
            transaction: simple_transaction(9),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[transaction], &[0], &[0], 0x43);
        let car_len = car.len() as u64;

        let mut default_source = source(car.clone(), plan.clone());
        let (default_receipt, default_blocks) =
            collect(&mut default_source, &ScanRequest::all()).unwrap();
        assert_eq!(
            default_blocks[0].transactions[0].primary_signature,
            Some([9; 64])
        );

        let mut omitted_source = source(car, plan);
        let (omitted_receipt, omitted_blocks) = collect(
            &mut omitted_source,
            &ScanRequest::all().without_primary_signatures(),
        )
        .unwrap();
        assert_eq!(omitted_blocks[0].transactions[0].primary_signature, None);
        assert_eq!(default_receipt.io.source_read_bytes, Some(car_len));
        assert_eq!(omitted_receipt.io.source_read_bytes, Some(car_len));
    }

    #[test]
    fn token_balance_only_scan_filters_exact_mints_and_keeps_unknown_mints() {
        let target_mint = [0x71; 32];
        let other_mint = [0x72; 32];
        let owner = [0x73; 32];
        let token_program = [0x74; 32];
        let fixture = FixtureTransaction {
            cid: cid(0x4d),
            transaction: simple_transaction(7),
            metadata: metadata(TransactionStatusMeta {
                fee: 1,
                pre_token_balances: vec![
                    token_balance(
                        0,
                        String::new(),
                        bs58::encode(owner).into_string(),
                        bs58::encode(token_program).into_string(),
                        11,
                    ),
                    token_balance(
                        1,
                        bs58::encode(target_mint).into_string(),
                        bs58::encode(owner).into_string(),
                        bs58::encode(token_program).into_string(),
                        22,
                    ),
                    token_balance(
                        2,
                        bs58::encode(other_mint).into_string(),
                        String::new(),
                        String::new(),
                        44,
                    ),
                ],
                post_token_balances: vec![token_balance(
                    1,
                    bs58::encode(target_mint).into_string(),
                    bs58::encode(owner).into_string(),
                    bs58::encode(token_program).into_string(),
                    33,
                )],
                inner_instructions_none: true,
                ..TransactionStatusMeta::default()
            }),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x4e);
        let request = ScanRequest::all()
            .without_instructions()
            .without_required_signers()
            .without_execution_status()
            .without_primary_signatures()
            .with_token_balances_for([target_mint]);
        let (receipt, blocks) = collect(&mut source(car, vec![FIRST_SLOT]), &request).unwrap();
        let transaction = &blocks[0].transactions[0];

        assert_eq!(transaction.primary_signature, None);
        assert!(transaction.required_signers.is_empty());
        assert_eq!(
            transaction.header.status,
            ExecutionStatus::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert!(transaction.instructions.is_empty());
        assert_eq!(
            transaction.token_balance_coverage,
            TokenBalanceCoverage::Complete
        );
        assert_eq!(transaction.token_balances.len(), 3);
        assert_eq!(transaction.token_balances[0].side, TokenBalanceSide::Pre);
        assert_eq!(transaction.token_balances[0].balance_index, 0);
        assert_eq!(transaction.token_balances[0].mint, None);
        assert_eq!(transaction.token_balances[1].side, TokenBalanceSide::Pre);
        assert_eq!(transaction.token_balances[1].balance_index, 1);
        assert_eq!(transaction.token_balances[1].mint, Some(target_mint));
        assert_eq!(transaction.token_balances[1].owner, Some(owner));
        assert_eq!(
            transaction.token_balances[1].token_program,
            Some(token_program)
        );
        assert_eq!(transaction.token_balances[1].amount, 22);
        assert_eq!(transaction.token_balances[2].side, TokenBalanceSide::Post);
        assert_eq!(transaction.token_balances[2].balance_index, 0);
        assert_eq!(transaction.token_balances[2].amount, 33);
        assert_eq!(receipt.instructions, 0);
        assert_eq!(receipt.transactions_with_incomplete_token_balances, 0);
    }

    #[test]
    fn v0_loaded_keys_cpi_stack_and_selected_data_are_exact() {
        let signer = [1; 32];
        let unselected_program = [2; 32];
        let loaded_program = [9; 32];
        let readonly_program = [8; 32];
        let transaction = v0_transaction(
            [7; 64],
            &[signer, unselected_program],
            &[(2, &[0, 3], &[11]), (3, &[2], &[22])],
            &[3],
            &[4],
        );
        let metadata = metadata(TransactionStatusMeta {
            inner_instructions: vec![
                InnerInstructions {
                    index: 0,
                    instructions: vec![
                        InnerInstruction {
                            program_id_index: 1,
                            accounts: vec![2],
                            data: vec![33],
                            stack_height: Some(3),
                        },
                        InnerInstruction {
                            program_id_index: 3,
                            accounts: vec![0],
                            data: vec![34],
                            stack_height: Some(4),
                        },
                    ],
                },
                InnerInstructions {
                    index: 1,
                    instructions: vec![InnerInstruction {
                        program_id_index: 2,
                        accounts: vec![3],
                        data: vec![35],
                        stack_height: Some(2),
                    }],
                },
            ],
            inner_instructions_none: false,
            loaded_writable_addresses: vec![loaded_program.to_vec()],
            loaded_readonly_addresses: vec![readonly_program.to_vec()],
            ..TransactionStatusMeta::default()
        });
        let fixture = FixtureTransaction {
            cid: cid(0x51),
            transaction,
            metadata,
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x52);
        let mut source = source(car, vec![FIRST_SLOT]);
        let request = ScanRequest::all().with_instruction_data_for([loaded_program]);
        let (_, blocks) = collect(&mut source, &request).unwrap();
        let transaction = &blocks[0].transactions[0];
        assert_eq!(transaction.primary_signature, Some([7; 64]));
        assert_eq!(transaction.required_signers, vec![signer]);
        assert_eq!(transaction.header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(transaction.instructions.len(), 5);
        assert_eq!(transaction.instructions[0].program_id, Some(loaded_program));
        assert_eq!(
            transaction.instructions[0].accounts,
            vec![signer, readonly_program]
        );
        assert_eq!(transaction.instructions[0].data, vec![11]);
        assert_eq!(
            transaction.instructions[0].data_coverage,
            InstructionDataCoverage::Exact
        );
        assert_eq!(transaction.instructions[1].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(transaction.instructions[1].coordinate.stack_height, Some(3));
        assert_eq!(transaction.instructions[1].accounts, vec![loaded_program]);
        assert_eq!(
            transaction.instructions[1].data_coverage,
            InstructionDataCoverage::NotRequested
        );
        assert!(transaction.instructions[1].data.is_empty());
        assert_eq!(transaction.instructions[2].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[2].coordinate.inner_index, Some(1));
        assert_eq!(transaction.instructions[2].coordinate.stack_height, Some(4));
        assert_eq!(
            transaction.instructions[2].program_id,
            Some(readonly_program)
        );
        assert_eq!(transaction.instructions[3].coordinate.outer_index, 1);
        assert_eq!(
            transaction.instructions[3].data_coverage,
            InstructionDataCoverage::NotRequested
        );
        assert_eq!(
            transaction.instructions[3].program_id,
            Some(readonly_program)
        );
        assert_eq!(transaction.instructions[4].coordinate.outer_index, 1);
        assert_eq!(transaction.instructions[4].coordinate.inner_index, Some(0));
        assert_eq!(transaction.instructions[4].coordinate.stack_height, Some(2));
        assert_eq!(transaction.instructions[4].program_id, Some(loaded_program));
        assert_eq!(transaction.instructions[4].accounts, vec![readonly_program]);
        assert_eq!(transaction.instructions[4].data, vec![35]);
    }

    #[test]
    fn v0_without_instruction_accounts_keeps_loaded_programs_and_cpi() {
        let signer = [1; 32];
        let static_program = [2; 32];
        let loaded_program = [9; 32];
        let readonly_program = [8; 32];
        let transaction = v0_transaction(
            [7; 64],
            &[signer, static_program],
            &[(2, &[0, 3], &[11])],
            &[3],
            &[4],
        );
        let metadata = metadata(TransactionStatusMeta {
            inner_instructions: vec![InnerInstructions {
                index: 0,
                instructions: vec![InnerInstruction {
                    program_id_index: 3,
                    accounts: vec![2],
                    data: vec![33],
                    stack_height: Some(2),
                }],
            }],
            inner_instructions_none: false,
            loaded_writable_addresses: vec![loaded_program.to_vec()],
            loaded_readonly_addresses: vec![readonly_program.to_vec()],
            ..TransactionStatusMeta::default()
        });
        let fixture = FixtureTransaction {
            cid: cid(0x55),
            transaction,
            metadata,
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x56);
        let request = ScanRequest::all()
            .without_instruction_accounts()
            .without_instruction_data();
        let (receipt, blocks) = collect(&mut source(car, vec![FIRST_SLOT]), &request).unwrap();
        let transaction = &blocks[0].transactions[0];

        assert_eq!(receipt.instructions, 2);
        assert_eq!(receipt.transactions_with_incomplete_instructions, 0);
        assert_eq!(receipt.transactions_with_incomplete_cpi, 0);
        assert_eq!(transaction.header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(transaction.instructions.len(), 2);
        assert_eq!(transaction.instructions[0].program_id, Some(loaded_program));
        assert!(transaction.instructions[0].accounts.is_empty());
        assert_eq!(transaction.instructions[0].coordinate.order, 0);
        assert_eq!(transaction.instructions[0].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[0].coordinate.inner_index, None);
        assert_eq!(
            transaction.instructions[1].program_id,
            Some(readonly_program)
        );
        assert!(transaction.instructions[1].accounts.is_empty());
        assert_eq!(transaction.instructions[1].coordinate.order, 1);
        assert_eq!(transaction.instructions[1].coordinate.outer_index, 0);
        assert_eq!(transaction.instructions[1].coordinate.inner_index, Some(0));
        assert_eq!(transaction.instructions[1].coordinate.stack_height, Some(2));
    }

    #[test]
    fn v1_message_projects_signers_and_outer_instructions() {
        let signer = [0x31; 32];
        let program = [0x32; 32];
        let fixture = FixtureTransaction {
            cid: cid(0x53),
            transaction: v1_transaction([0x33; 64], &[signer, program], &[(1, &[0], &[44])]),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x54);
        let (_, blocks) = collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).unwrap();
        let transaction = &blocks[0].transactions[0];
        assert_eq!(transaction.primary_signature, Some([0x33; 64]));
        assert_eq!(transaction.required_signers, vec![signer]);
        assert_eq!(transaction.instructions.len(), 1);
        assert_eq!(transaction.instructions[0].program_id, Some(program));
        assert_eq!(transaction.instructions[0].accounts, vec![signer]);
        assert_eq!(transaction.instructions[0].data, vec![44]);
    }

    #[test]
    fn failed_outer_index_accepts_exact_legacy_unit_borsh_io_error() {
        let metadata = TransactionStatusMeta {
            err: Some(TransactionError {
                err: vec![
                    8, 0, 0, 0, // StoredTransactionError::InstructionError
                    7, // failed outer instruction index
                    44, 0, 0, 0, // old unit StoredInstructionError::BorshIoError
                ],
            }),
            ..TransactionStatusMeta::default()
        };

        assert_eq!(decode_failed_outer_index(&metadata).unwrap(), Some(7));
    }

    #[test]
    fn failed_outer_index_rejects_near_legacy_malformed_bytes() {
        let malformed = [
            vec![8, 0, 0, 0, 7, 44, 0, 0],
            vec![8, 0, 0, 0, 7, 44, 0, 0, 0, 0],
            vec![0xff, 0xff, 0xff, 0xff, 7, 44, 0, 0, 0],
            vec![8, 0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff],
        ];

        for bytes in malformed {
            let metadata = TransactionStatusMeta {
                err: Some(TransactionError { err: bytes }),
                ..TransactionStatusMeta::default()
            };
            assert!(
                matches!(
                    decode_failed_outer_index(&metadata),
                    Err(CarQueryError::InvalidArchive(message))
                        if message.contains("not exact stored-error bytes")
                ),
                "accepted malformed legacy transaction error"
            );
        }
    }

    #[test]
    fn failed_outer_index_and_metadata_coverage_states_are_preserved() {
        let keys = [[1; 32], [2; 32]];
        let instructions = [(1, &[][..], &[10][..]), (1, &[][..], &[11][..])];
        let failed_error = wincode::serialize(&StoredTransactionError::InstructionError(
            1,
            StoredInstructionError::GenericError,
        ))
        .unwrap();
        let failed = FixtureTransaction {
            cid: cid(0x61),
            transaction: legacy_transaction([1; 64], &keys, &instructions),
            metadata: metadata(TransactionStatusMeta {
                err: Some(TransactionError { err: failed_error }),
                inner_instructions_none: false,
                ..TransactionStatusMeta::default()
            }),
            index: Some(0),
        };
        let absent = FixtureTransaction {
            cid: cid(0x62),
            transaction: simple_transaction(2),
            metadata: Vec::new(),
            index: Some(1),
        };
        let not_recorded = FixtureTransaction {
            cid: cid(0x63),
            transaction: simple_transaction(3),
            metadata: metadata(TransactionStatusMeta {
                inner_instructions_none: true,
                ..TransactionStatusMeta::default()
            }),
            index: Some(2),
        };
        let mut car = car_header();
        append_block(
            &mut car,
            FIRST_SLOT,
            &[failed, absent, not_recorded],
            &[0, 1, 2],
            &[0, 1, 2],
            0x64,
        );
        let mut adapter = source(car, vec![FIRST_SLOT]);
        let request = ScanRequest::all()
            .allow_incomplete_cpi()
            .allow_unknown_execution();
        let (_, blocks) = collect(&mut adapter, &request).unwrap();
        let transactions = &blocks[0].transactions;
        assert_eq!(transactions[0].header.status, ExecutionStatus::Failed);
        assert_eq!(
            transactions[0].header.failed_outer_instruction_index,
            Some(1)
        );
        assert_eq!(transactions[0].header.cpi_coverage, CpiCoverage::Complete);
        assert_eq!(
            transactions[1].header.status,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[1].header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::MetadataAbsent)
        );
        assert_eq!(
            transactions[1].header.instruction_coverage,
            InstructionCoverage::Complete
        );
        assert_eq!(transactions[2].header.status, ExecutionStatus::Succeeded);
        assert_eq!(
            transactions[2].header.cpi_coverage,
            CpiCoverage::NotRecorded
        );

        let cpi_after_failure = FixtureTransaction {
            cid: cid(0x65),
            transaction: legacy_transaction([4; 64], &keys, &instructions),
            metadata: metadata(TransactionStatusMeta {
                err: Some(TransactionError {
                    err: wincode::serialize(&StoredTransactionError::InstructionError(
                        0,
                        StoredInstructionError::GenericError,
                    ))
                    .unwrap(),
                }),
                inner_instructions: vec![InnerInstructions {
                    index: 1,
                    instructions: vec![InnerInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data: Vec::new(),
                        stack_height: Some(2),
                    }],
                }],
                inner_instructions_none: false,
                ..TransactionStatusMeta::default()
            }),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[cpi_after_failure], &[0], &[0], 0x66);
        assert!(
            collect(
                &mut source(car.clone(), vec![FIRST_SLOT]),
                &ScanRequest::all()
            )
            .is_err()
        );

        let request = ScanRequest::all().without_failed_transaction_details();
        let (_, blocks) = collect(&mut source(car, vec![FIRST_SLOT]), &request).unwrap();
        let transaction = &blocks[0].transactions[0];
        assert_eq!(transaction.header.status, ExecutionStatus::Failed);
        assert_eq!(transaction.header.failed_outer_instruction_index, Some(0));
        assert!(transaction.instructions.is_empty());
        assert_eq!(
            transaction.header.instruction_coverage,
            InstructionCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
        assert_eq!(
            transaction.header.cpi_coverage,
            CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested)
        );
    }

    #[test]
    fn malformed_frame_indexes_and_decoder_bytes_fail_closed() {
        let wrong_index = FixtureTransaction {
            cid: cid(0x71),
            transaction: simple_transaction(1),
            metadata: empty_exact_metadata(),
            index: Some(1),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[wrong_index], &[0], &[0], 0x72);
        assert!(collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());

        let raw_transaction = FixtureTransaction {
            cid: cid(0x73),
            transaction: vec![0xff],
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[raw_transaction], &[0], &[0], 0x74);
        let error = collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).unwrap_err();
        assert!(error.to_string().contains("CAR source error"));

        let raw_metadata = FixtureTransaction {
            cid: cid(0x75),
            transaction: simple_transaction(2),
            metadata: vec![0xff],
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[raw_metadata], &[0], &[0], 0x76);
        assert!(collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());
    }

    #[test]
    fn cpi_absence_contradiction_and_bad_loaded_index_are_rejected() {
        let keys = [[1; 32], [2; 32]];
        let contradictory = FixtureTransaction {
            cid: cid(0x81),
            transaction: legacy_transaction([1; 64], &keys, &[(1, &[], &[])]),
            metadata: metadata(TransactionStatusMeta {
                inner_instructions: vec![InnerInstructions {
                    index: 0,
                    instructions: Vec::new(),
                }],
                inner_instructions_none: true,
                ..TransactionStatusMeta::default()
            }),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[contradictory], &[0], &[0], 0x82);
        assert!(collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());

        let bad_program = FixtureTransaction {
            cid: cid(0x83),
            transaction: legacy_transaction([2; 64], &keys, &[(7, &[], &[])]),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[bad_program], &[0], &[0], 0x84);
        assert!(collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());

        let bad_inner_group = FixtureTransaction {
            cid: cid(0x85),
            transaction: legacy_transaction([3; 64], &keys, &[(1, &[], &[])]),
            metadata: metadata(TransactionStatusMeta {
                inner_instructions: vec![InnerInstructions {
                    index: 1,
                    instructions: Vec::new(),
                }],
                inner_instructions_none: false,
                ..TransactionStatusMeta::default()
            }),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[bad_inner_group], &[0], &[0], 0x86);
        assert!(collect(&mut source(car, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());
    }

    #[test]
    fn bounded_range_stops_before_the_next_malformed_entry() {
        let fixture = FixtureTransaction {
            cid: cid(0x91),
            transaction: simple_transaction(1),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x92);
        push_uvarint(&mut car, (HARD_MAX_ENTRY_PAYLOAD_BYTES as u64) + 37);
        let plan = vec![FIRST_SLOT, FIRST_SLOT + 1];
        let request = ScanRequest::bounded(ScanRange {
            first_block: 0,
            block_count: NonZeroU32::new(1).unwrap(),
        });
        let (receipt, blocks) = collect(&mut source(car.clone(), plan.clone()), &request).unwrap();
        assert_eq!(receipt.blocks, 1);
        assert_eq!(blocks[0].header.slot, FIRST_SLOT);

        assert!(collect(&mut source(car, plan), &ScanRequest::all()).is_err());
    }

    #[test]
    fn bounded_reconstruction_rejects_repeated_cid_amplification() {
        let repeated_transaction = FixtureTransaction {
            cid: cid(0x95),
            transaction: simple_transaction(1),
            metadata: empty_exact_metadata(),
            index: None,
        };
        let mut repeated_transaction_car = car_header();
        append_block(
            &mut repeated_transaction_car,
            FIRST_SLOT,
            &[repeated_transaction],
            &[0],
            &[0, 0],
            0x96,
        );
        let plan = vec![FIRST_SLOT];
        let limits = CarQueryLimits {
            max_transactions_per_block: 1,
            ..CarQueryLimits::default()
        };
        let mut adapter = CarInstructionSource::new(
            Cursor::new(repeated_transaction_car),
            identity(&plan),
            CanonicalBlockPlan::new(plan),
            limits,
        )
        .unwrap();
        let error = collect(&mut adapter, &ScanRequest::all()).unwrap_err();
        assert!(
            format!("{error:?}")
                .contains("ordered entries reference 2 transactions but reader collected 1")
        );

        let transaction_cid = cid(0x97);
        let entry_cid = cid(0x98);
        let mut repeated_entry_car = car_header();
        push_car_entry(
            &mut repeated_entry_car,
            transaction_cid,
            &transaction_node(
                FIRST_SLOT,
                Some(0),
                &simple_transaction(2),
                &empty_exact_metadata(),
            ),
        );
        push_car_entry(
            &mut repeated_entry_car,
            entry_cid,
            &entry_node(&[transaction_cid]),
        );
        push_car_entry(
            &mut repeated_entry_car,
            cid(0x99),
            &block_node_entries(FIRST_SLOT, &[entry_cid, entry_cid]),
        );
        let error = collect(
            &mut source(repeated_entry_car, vec![FIRST_SLOT]),
            &ScanRequest::all(),
        )
        .unwrap_err();
        assert!(
            format!("{error:?}")
                .contains("ordered block references 2 entries but reader collected 1")
        );
    }

    #[test]
    fn application_sink_error_is_not_changed() {
        let fixture = FixtureTransaction {
            cid: cid(0x9a),
            transaction: simple_transaction(1),
            metadata: empty_exact_metadata(),
            index: Some(0),
        };
        let mut car = car_header();
        append_block(&mut car, FIRST_SLOT, &[fixture], &[0], &[0], 0x9b);
        let mut adapter = source(car, vec![FIRST_SLOT]);
        let error = adapter
            .for_each_block(&ScanRequest::all(), |_| {
                Err(QueryError::sink(io::Error::other("fixture sink failure")))
            })
            .unwrap_err();
        match error {
            QueryError::Sink { source } => {
                assert_eq!(source.to_string(), "fixture sink failure");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn full_scan_requires_clean_eof_and_entry_caps_precede_payload_allocation() {
        let mut with_trailer = car_header();
        append_block(&mut with_trailer, FIRST_SLOT, &[], &[], &[], 0x9d);
        push_car_entry(
            &mut with_trailer,
            cid(0x9f),
            &subset_node(FIRST_SLOT, FIRST_SLOT),
        );
        push_car_entry(&mut with_trailer, cid(0xa0), &epoch_node(185));
        let (receipt, blocks) = collect(
            &mut source(with_trailer, vec![FIRST_SLOT]),
            &ScanRequest::all(),
        )
        .unwrap();
        assert_eq!(receipt.blocks, 1);
        assert_eq!(blocks[0].header.slot, FIRST_SLOT);

        let mut partial_varint = car_header();
        append_block(&mut partial_varint, FIRST_SLOT, &[], &[], &[], 0xa0);
        partial_varint.push(0x80);
        let error = collect(
            &mut source(partial_varint, vec![FIRST_SLOT]),
            &ScanRequest::all(),
        )
        .unwrap_err();
        assert!(format!("{error:?}").contains("EOF while reading uvarint"));

        let dangling_transaction_cid = cid(0xa7);
        let mut unterminated = car_header();
        push_car_entry(
            &mut unterminated,
            dangling_transaction_cid,
            &transaction_node(
                FIRST_SLOT,
                Some(0),
                &simple_transaction(7),
                &empty_exact_metadata(),
            ),
        );
        push_car_entry(
            &mut unterminated,
            cid(0xa8),
            &entry_node(&[dangling_transaction_cid]),
        );
        push_car_entry(&mut unterminated, cid(0xa9), &rewards_node(FIRST_SLOT, &[]));
        push_car_entry(&mut unterminated, cid(0xaa), &dataframe_node(&[1, 2, 3]));
        let error = collect(
            &mut source(unterminated, vec![FIRST_SLOT]),
            &ScanRequest::all(),
        )
        .unwrap_err();
        let error = format!("{error:?}");
        assert!(error.contains("unterminated block group"));
        assert!(error.contains("txs=1"));
        assert!(error.contains("entries=1"));
        assert!(error.contains("rewards=1"));
        assert!(error.contains("dataframes=1"));

        let mut extra = car_header();
        append_block(&mut extra, FIRST_SLOT, &[], &[], &[], 0xa1);
        append_block(&mut extra, FIRST_SLOT + 1, &[], &[], &[], 0xa3);
        assert!(collect(&mut source(extra, vec![FIRST_SLOT]), &ScanRequest::all()).is_err());

        let mut oversized = car_header();
        push_uvarint(&mut oversized, 200);
        let plan = vec![FIRST_SLOT];
        let limits = CarQueryLimits {
            max_entry_payload_bytes: 100,
            ..CarQueryLimits::default()
        };
        let mut source = CarInstructionSource::new(
            Cursor::new(oversized),
            identity(&plan),
            CanonicalBlockPlan::new(plan),
            limits,
        )
        .unwrap();
        assert!(collect(&mut source, &ScanRequest::all()).is_err());
    }
}

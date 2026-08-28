use std::{fmt, num::NonZeroU32};

use serde::{Deserialize, Serialize};

use crate::{
    BlockView, CanonicalBlock, CpiCoverage, Error, ExecutionStatus, InstructionCoverage,
    InstructionDataCoverage, Result, TransactionView,
};

pub const MAX_INSTRUCTION_DATA_PROGRAMS: usize = 256;

/// Archive format used by one source adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArchiveFormat {
    Car,
    CompactV2,
    IndexerV3,
}

impl fmt::Display for ArchiveFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Car => formatter.write_str("CAR"),
            Self::CompactV2 => formatter.write_str("Compact V2"),
            Self::IndexerV3 => formatter.write_str("Indexer V3"),
        }
    }
}

/// Strength of the source identity checked by one adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceVerification {
    PublishedManifest,
    /// The operator accepted this input without a published manifest.
    /// The input can be local or remote.
    OperatorTrusted,
    InternalBindingOnly,
    Unverified,
}

/// Source identity available to every application before a scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    pub format: ArchiveFormat,
    pub label: String,
    pub cluster_id: Option<String>,
    pub epoch: u64,
    /// First slot in this source epoch. This is explicit because clusters can
    /// use a warm-up epoch schedule.
    pub first_slot: u64,
    pub slots_per_epoch: u64,
    /// Exact number of block rows in this source.
    pub block_count: u32,
    pub verification: SourceVerification,
    /// Format-specific manifest digest, root CID, strong ETag, or internal
    /// candidate identity. This value is not present when the source has no
    /// stable binding.
    pub binding: Option<String>,
}

/// A contiguous block-row range in one epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRange {
    pub first_block: u32,
    pub block_count: NonZeroU32,
}

/// Exact instruction-data scope required by a scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "scope", content = "programs")]
pub enum InstructionDataRequirement {
    All,
    Programs(Vec<[u8; 32]>),
    None,
}

impl InstructionDataRequirement {
    fn requires(&self, program_id: &[u8; 32]) -> bool {
        match self {
            Self::All => true,
            Self::Programs(programs) => programs.contains(program_id),
            Self::None => false,
        }
    }
}

/// One source-neutral ordered scan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub range: Option<ScanRange>,
    pub require_verified_source: bool,
    pub require_complete_instructions: bool,
    pub require_complete_cpi: bool,
    pub require_known_execution: bool,
    /// Stop when selected instruction data is not exact.
    #[serde(default = "default_true")]
    pub require_complete_instruction_data: bool,
    pub instruction_data: InstructionDataRequirement,
}

const fn default_true() -> bool {
    true
}

impl ScanRequest {
    pub const fn all() -> Self {
        Self {
            range: None,
            require_verified_source: true,
            require_complete_instructions: true,
            require_complete_cpi: true,
            require_known_execution: true,
            require_complete_instruction_data: true,
            instruction_data: InstructionDataRequirement::All,
        }
    }

    pub const fn bounded(range: ScanRange) -> Self {
        Self {
            range: Some(range),
            require_verified_source: true,
            require_complete_instructions: true,
            require_complete_cpi: true,
            require_known_execution: true,
            require_complete_instruction_data: true,
            instruction_data: InstructionDataRequirement::All,
        }
    }

    pub const fn allow_unverified_source(mut self) -> Self {
        self.require_verified_source = false;
        self
    }

    pub const fn allow_incomplete_instructions(mut self) -> Self {
        self.require_complete_instructions = false;
        self
    }

    pub const fn allow_incomplete_cpi(mut self) -> Self {
        self.require_complete_cpi = false;
        self
    }

    pub const fn allow_unknown_execution(mut self) -> Self {
        self.require_known_execution = false;
        self
    }

    /// Deliver selected instructions with explicit non-exact data coverage.
    ///
    /// The adapter must still select data for the requested programs. This
    /// option lets an application save an exact coverage issue instead of
    /// stopping the scan when those bytes are unavailable or ambiguous.
    pub const fn allow_incomplete_instruction_data(mut self) -> Self {
        self.require_complete_instruction_data = false;
        self
    }

    /// Require exact instruction data only for the selected programs.
    pub fn with_instruction_data_for(
        mut self,
        programs: impl IntoIterator<Item = [u8; 32]>,
    ) -> Self {
        self.instruction_data =
            InstructionDataRequirement::Programs(programs.into_iter().collect());
        self
    }

    /// Do not require instruction data for this scan.
    pub fn without_instruction_data(mut self) -> Self {
        self.instruction_data = InstructionDataRequirement::None;
        self
    }
}

/// Exact I/O counters when a format adapter can measure them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanIoReceipt {
    /// Local reads or network requests that returned source bytes.
    pub source_read_calls: Option<u64>,
    /// Source bytes returned to the adapter before decompression.
    pub source_read_bytes: Option<u64>,
    /// Bytes produced after source decompression.
    pub decoded_bytes: Option<u64>,
    /// Reads served from a persistent local cache.
    pub cache_read_calls: Option<u64>,
    pub cache_read_bytes: Option<u64>,
}

/// Exact source work and coverage counts returned after a scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanReceipt {
    pub blocks: u64,
    pub transactions: u64,
    pub instructions: u64,
    /// Instructions whose exact bytes the request did not select.
    pub instructions_not_requested: u64,
    /// Instructions whose selected or available bytes are not exact.
    pub instructions_with_unknown_data: u64,
    pub transactions_with_incomplete_instructions: u64,
    pub transactions_with_incomplete_cpi: u64,
    pub transactions_with_unknown_execution: u64,
    pub io: ScanIoReceipt,
}

/// Application callback for canonical blocks in source order.
pub trait BlockSink {
    fn visit_block(&mut self, block: BlockView<'_>) -> Result<()>;
}

/// Adapter from a closure to BlockSink.
pub struct FnBlockSink<F>(F);

impl<F> FnBlockSink<F> {
    pub fn new(visitor: F) -> Self {
        Self(visitor)
    }
}

impl<F> BlockSink for FnBlockSink<F>
where
    F: for<'a> FnMut(BlockView<'a>) -> Result<()>,
{
    fn visit_block(&mut self, block: BlockView<'_>) -> Result<()> {
        (self.0)(block)
    }
}

/// One format adapter that publishes canonical blocks in ledger order.
pub trait ArchiveInstructionSource {
    fn identity(&self) -> &SourceIdentity;

    fn scan_ordered(
        &mut self,
        request: &ScanRequest,
        sink: &mut dyn BlockSink,
    ) -> Result<ScanReceipt>;
}

/// Convenience methods shared by all format adapters.
pub trait ArchiveInstructionSourceExt: ArchiveInstructionSource {
    fn for_each_block<F>(&mut self, request: &ScanRequest, visitor: F) -> Result<ScanReceipt>
    where
        F: for<'a> FnMut(BlockView<'a>) -> Result<()>,
    {
        let mut sink = FnBlockSink::new(visitor);
        self.scan_ordered(request, &mut sink)
    }

    fn for_each_transaction<F>(
        &mut self,
        request: &ScanRequest,
        mut visitor: F,
    ) -> Result<ScanReceipt>
    where
        F: for<'a> FnMut(TransactionView<'a>) -> Result<()>,
    {
        self.for_each_block(request, move |block| {
            for transaction in block.transaction_views() {
                visitor(transaction)?;
            }
            Ok(())
        })
    }
}

impl<T: ArchiveInstructionSource + ?Sized> ArchiveInstructionSourceExt for T {}

/// Checked publisher used by format adapters.
///
/// It enforces exact block-row order, source epoch binding, transaction order,
/// request coverage, and common coverage gates before an application sees a
/// block.
pub struct OrderedBlockPublisher<'a> {
    identity: &'a SourceIdentity,
    request: &'a ScanRequest,
    sink: &'a mut dyn BlockSink,
    next_block: u32,
    end_block: u32,
    previous_slot: Option<u64>,
    receipt: ScanReceipt,
}

impl<'a> OrderedBlockPublisher<'a> {
    pub fn new(
        identity: &'a SourceIdentity,
        request: &'a ScanRequest,
        sink: &'a mut dyn BlockSink,
    ) -> Result<Self> {
        validate_request(identity, request)?;
        let (next_block, end_block) = requested_bounds(identity, request)?;
        Ok(Self {
            identity,
            request,
            sink,
            next_block,
            end_block,
            previous_slot: None,
            receipt: ScanReceipt::default(),
        })
    }

    pub fn publish(&mut self, block: &CanonicalBlock) -> Result<()> {
        if self.next_block >= self.end_block {
            return Err(Error::InvalidStream(
                "adapter published more blocks than the request".into(),
            ));
        }
        if block.header.epoch != self.identity.epoch {
            return Err(Error::InvalidStream(format!(
                "block epoch is {}, source epoch is {}",
                block.header.epoch, self.identity.epoch
            )));
        }
        if block.header.block_ordinal != self.next_block {
            return Err(Error::InvalidStream(format!(
                "block ordinal is {}, expected {}",
                block.header.block_ordinal, self.next_block
            )));
        }
        let first_slot = self.identity.first_slot;
        let last_slot = self
            .identity
            .first_slot
            .checked_add(self.identity.slots_per_epoch - 1)
            .ok_or_else(|| Error::InvalidStream("source epoch slot range overflows u64".into()))?;
        if block.header.slot < first_slot || block.header.slot > last_slot {
            return Err(Error::InvalidStream(format!(
                "block slot {} is outside source epoch slots {first_slot}..={last_slot}",
                block.header.slot
            )));
        }
        if let Some(previous_slot) = self.previous_slot
            && block.header.slot <= previous_slot
        {
            return Err(Error::InvalidStream(format!(
                "block slot {} is not after previous slot {previous_slot}",
                block.header.slot
            )));
        }
        block.validate()?;

        for transaction in &block.transactions {
            if !matches!(
                transaction.header.instruction_coverage,
                InstructionCoverage::Complete
            ) {
                increment(
                    &mut self.receipt.transactions_with_incomplete_instructions,
                    1,
                    "incomplete-instruction transaction count",
                )?;
                if self.request.require_complete_instructions {
                    return Err(Error::InvalidTransaction(format!(
                        "transaction {} in block {} has incomplete instruction coverage",
                        transaction.header.tx_index, block.header.block_ordinal
                    )));
                }
            }
            for instruction in &transaction.instructions {
                if matches!(
                    instruction.data_coverage,
                    InstructionDataCoverage::NotRequested
                ) {
                    increment(
                        &mut self.receipt.instructions_not_requested,
                        1,
                        "not-requested instruction-data count",
                    )?;
                } else if matches!(
                    instruction.data_coverage,
                    InstructionDataCoverage::Unknown(_)
                ) {
                    increment(
                        &mut self.receipt.instructions_with_unknown_data,
                        1,
                        "unknown instruction-data count",
                    )?;
                }
                if !matches!(instruction.data_coverage, InstructionDataCoverage::Exact)
                    && self
                        .request
                        .instruction_data
                        .requires(&instruction.program_id)
                    && self.request.require_complete_instruction_data
                {
                    return Err(Error::InvalidTransaction(format!(
                        "instruction {} in transaction {} block {} has incomplete data coverage",
                        instruction.coordinate.order,
                        transaction.header.tx_index,
                        block.header.block_ordinal
                    )));
                }
            }
            if !matches!(transaction.header.cpi_coverage, CpiCoverage::Complete) {
                increment(
                    &mut self.receipt.transactions_with_incomplete_cpi,
                    1,
                    "incomplete-CPI transaction count",
                )?;
                if self.request.require_complete_cpi {
                    return Err(Error::InvalidTransaction(format!(
                        "transaction {} in block {} has incomplete CPI coverage",
                        transaction.header.tx_index, block.header.block_ordinal
                    )));
                }
            }
            if matches!(transaction.header.status, ExecutionStatus::Unknown(_)) {
                increment(
                    &mut self.receipt.transactions_with_unknown_execution,
                    1,
                    "unknown-execution transaction count",
                )?;
                if self.request.require_known_execution {
                    return Err(Error::InvalidTransaction(format!(
                        "transaction {} in block {} has unknown execution state",
                        transaction.header.tx_index, block.header.block_ordinal
                    )));
                }
            }
            increment(
                &mut self.receipt.instructions,
                u64::try_from(transaction.instructions.len()).map_err(|_| {
                    Error::InvalidTransaction("instruction count exceeds u64".into())
                })?,
                "instruction count",
            )?;
        }

        self.sink.visit_block(block.as_view())?;
        increment(&mut self.receipt.blocks, 1, "block count")?;
        increment(
            &mut self.receipt.transactions,
            u64::try_from(block.transactions.len())
                .map_err(|_| Error::InvalidTransaction("transaction count exceeds u64".into()))?,
            "transaction count",
        )?;
        self.previous_slot = Some(block.header.slot);
        self.next_block = self
            .next_block
            .checked_add(1)
            .ok_or_else(|| Error::InvalidStream("block ordinal overflow".into()))?;
        Ok(())
    }

    /// Set exact I/O counters supplied by the format adapter.
    pub fn set_io_receipt(&mut self, io: ScanIoReceipt) {
        self.receipt.io = io;
    }

    /// Finish only after the adapter published the full requested range.
    pub fn finish(self) -> Result<ScanReceipt> {
        if self.next_block != self.end_block {
            return Err(Error::InvalidStream(format!(
                "adapter stopped at block {}, expected end {}",
                self.next_block, self.end_block
            )));
        }
        Ok(self.receipt)
    }
}

/// Apply request-level gates which are common to all adapters.
pub fn validate_request(identity: &SourceIdentity, request: &ScanRequest) -> Result<()> {
    if identity.label.is_empty() {
        return Err(Error::InvalidRequest("source label is empty".into()));
    }
    if identity.cluster_id.as_ref().is_some_and(String::is_empty) {
        return Err(Error::InvalidRequest("source cluster_id is empty".into()));
    }
    if identity.slots_per_epoch == 0 {
        return Err(Error::InvalidRequest(
            "source slots_per_epoch is zero".into(),
        ));
    }
    if u64::from(identity.block_count) > identity.slots_per_epoch {
        return Err(Error::InvalidRequest(format!(
            "source has {} block rows, more than its {} slots per epoch",
            identity.block_count, identity.slots_per_epoch
        )));
    }
    identity
        .first_slot
        .checked_add(identity.slots_per_epoch - 1)
        .ok_or_else(|| Error::InvalidRequest("source epoch slot range overflows u64".into()))?;
    if matches!(identity.verification, SourceVerification::PublishedManifest)
        && identity.binding.is_none()
    {
        return Err(Error::InvalidRequest(
            "published source has no manifest binding".into(),
        ));
    }
    if request.require_verified_source
        && matches!(
            identity.verification,
            SourceVerification::InternalBindingOnly | SourceVerification::Unverified
        )
    {
        return Err(Error::InvalidRequest(format!(
            "{} source {} is not publication-verified",
            identity.format, identity.label
        )));
    }
    if let InstructionDataRequirement::Programs(programs) = &request.instruction_data {
        if programs.is_empty() || programs.len() > MAX_INSTRUCTION_DATA_PROGRAMS {
            return Err(Error::InvalidRequest(format!(
                "instruction-data program list length {} is outside 1..={MAX_INSTRUCTION_DATA_PROGRAMS}",
                programs.len()
            )));
        }
        let mut sorted = programs.clone();
        sorted.sort_unstable();
        if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::InvalidRequest(
                "instruction-data program list has duplicates".into(),
            ));
        }
    }
    requested_bounds(identity, request)?;
    Ok(())
}

fn requested_bounds(identity: &SourceIdentity, request: &ScanRequest) -> Result<(u32, u32)> {
    let Some(range) = request.range else {
        return Ok((0, identity.block_count));
    };
    let end = range
        .first_block
        .checked_add(range.block_count.get())
        .ok_or_else(|| Error::InvalidRequest("block range overflows u32".into()))?;
    if end > identity.block_count {
        return Err(Error::InvalidRequest(format!(
            "block range ends at {end}, but source has {} blocks",
            identity.block_count
        )));
    }
    Ok((range.first_block, end))
}

fn increment(value: &mut u64, add: u64, label: &str) -> Result<()> {
    *value = value
        .checked_add(add)
        .ok_or_else(|| Error::InvalidStream(format!("{label} overflow")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlockHeader, CanonicalTransaction, InstructionCoordinate, ResolvedInstruction,
        TransactionHeader,
    };

    struct FixtureSource {
        identity: SourceIdentity,
        blocks: Vec<CanonicalBlock>,
    }

    const EPOCH_7_FIRST_SLOT: u64 = 7 * 432_000;

    impl ArchiveInstructionSource for FixtureSource {
        fn identity(&self) -> &SourceIdentity {
            &self.identity
        }

        fn scan_ordered(
            &mut self,
            request: &ScanRequest,
            sink: &mut dyn BlockSink,
        ) -> Result<ScanReceipt> {
            let mut publisher = OrderedBlockPublisher::new(&self.identity, request, sink)?;
            let (first, end) = requested_bounds(&self.identity, request)?;
            for block in &self.blocks[first as usize..end as usize] {
                publisher.publish(block)?;
            }
            publisher.finish()
        }
    }

    fn fixture(verification: SourceVerification) -> FixtureSource {
        FixtureSource {
            identity: SourceIdentity {
                format: ArchiveFormat::CompactV2,
                label: "fixture".into(),
                cluster_id: Some("mainnet-beta".into()),
                epoch: 7,
                first_slot: EPOCH_7_FIRST_SLOT,
                slots_per_epoch: 432_000,
                block_count: 2,
                verification,
                binding: Some("binding".into()),
            },
            blocks: vec![
                CanonicalBlock {
                    header: crate::BlockHeader {
                        epoch: 7,
                        block_ordinal: 0,
                        slot: EPOCH_7_FIRST_SLOT + 9,
                    },
                    transactions: Vec::new(),
                },
                CanonicalBlock {
                    header: crate::BlockHeader {
                        epoch: 7,
                        block_ordinal: 1,
                        slot: EPOCH_7_FIRST_SLOT + 11,
                    },
                    transactions: vec![CanonicalTransaction {
                        header: TransactionHeader {
                            tx_index: 0,
                            status: ExecutionStatus::Succeeded,
                            failed_outer_instruction_index: None,
                            instruction_coverage: InstructionCoverage::Complete,
                            cpi_coverage: CpiCoverage::Complete,
                        },
                        primary_signature: None,
                        required_signers: vec![[9; 32]],
                        instructions: vec![ResolvedInstruction {
                            coordinate: InstructionCoordinate {
                                order: 0,
                                outer_index: 0,
                                inner_index: None,
                                stack_height: None,
                            },
                            program_id: [1; 32],
                            accounts: vec![[2; 32]],
                            data_coverage: InstructionDataCoverage::Exact,
                            data: vec![3],
                        }],
                    }],
                },
            ],
        }
    }

    #[test]
    fn short_transaction_api_keeps_canonical_order_and_empty_blocks() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        let mut coordinates = Vec::new();
        let receipt = source
            .for_each_transaction(&ScanRequest::all(), |transaction| {
                coordinates.push((transaction.block.slot, transaction.header.tx_index));
                Ok(())
            })
            .unwrap();
        assert_eq!(coordinates, [(EPOCH_7_FIRST_SLOT + 11, 0)]);
        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 1);
        assert_eq!(receipt.instructions, 1);
    }

    #[test]
    fn block_api_publishes_empty_blocks_for_checkpoints() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        let mut blocks = Vec::new();
        source
            .for_each_block(&ScanRequest::all(), |block| {
                blocks.push((block.header.block_ordinal, block.transactions.len()));
                Ok(())
            })
            .unwrap();
        assert_eq!(blocks, [(0, 0), (1, 1)]);
    }

    #[test]
    fn sink_error_stops_delivery_once() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        let mut calls = 0;
        let error = source
            .for_each_block(&ScanRequest::all(), |_| {
                calls += 1;
                Err(Error::sink(std::io::Error::other("fixture sink stopped")))
            })
            .unwrap_err();
        assert!(matches!(error, Error::Sink { .. }));
        assert_eq!(calls, 1);
    }

    #[test]
    fn closure_api_works_on_runtime_selected_source() {
        let mut source: Box<dyn ArchiveInstructionSource> =
            Box::new(fixture(SourceVerification::PublishedManifest));
        let mut transactions = 0;
        source
            .for_each_transaction(&ScanRequest::all(), |_| {
                transactions += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(transactions, 1);
    }

    #[test]
    fn verified_gate_rejects_internal_only_candidate() {
        let mut source = fixture(SourceVerification::InternalBindingOnly);
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );
        source
            .for_each_block(&ScanRequest::all().allow_unverified_source(), |_| Ok(()))
            .unwrap();
    }

    #[test]
    fn source_geometry_is_validated_before_output() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        source.identity.slots_per_epoch = 0;
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );

        source.identity.first_slot = 2;
        source.identity.slots_per_epoch = u64::MAX;
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );

        source.identity.first_slot = EPOCH_7_FIRST_SLOT;
        source.identity.slots_per_epoch = 1;
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );
    }

    #[test]
    fn bounded_scan_checks_range_and_publishes_exact_rows() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        let request = ScanRequest::bounded(ScanRange {
            first_block: 1,
            block_count: NonZeroU32::new(1).unwrap(),
        });
        let mut slots = Vec::new();
        let receipt = source
            .for_each_block(&request, |block| {
                slots.push(block.header.slot);
                Ok(())
            })
            .unwrap();
        assert_eq!(slots, [EPOCH_7_FIRST_SLOT + 11]);
        assert_eq!(receipt.blocks, 1);

        let invalid = ScanRequest::bounded(ScanRange {
            first_block: u32::MAX,
            block_count: NonZeroU32::new(2).unwrap(),
        });
        assert!(source.for_each_block(&invalid, |_| Ok(())).is_err());
    }

    #[test]
    fn coverage_gates_are_checked_before_the_sink() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        source.blocks[1].transactions[0].header.cpi_coverage = CpiCoverage::NotRecorded;
        let mut sink_calls = 0;
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| {
                    sink_calls += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(sink_calls, 1, "the valid empty block was published first");

        let receipt = source
            .for_each_block(&ScanRequest::all().allow_incomplete_cpi(), |_| Ok(()))
            .unwrap();
        assert_eq!(receipt.transactions_with_incomplete_cpi, 1);

        source.blocks[1].transactions[0].header.cpi_coverage = CpiCoverage::Complete;
        source.blocks[1].transactions[0].header.instruction_coverage =
            InstructionCoverage::Unknown(crate::CoverageReason::RawTransaction);
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );
        let receipt = source
            .for_each_block(&ScanRequest::all().allow_incomplete_instructions(), |_| {
                Ok(())
            })
            .unwrap();
        assert_eq!(receipt.transactions_with_incomplete_instructions, 1);

        source.blocks[1].transactions[0].header.instruction_coverage =
            InstructionCoverage::Complete;
        source.blocks[1].transactions[0].header.status =
            ExecutionStatus::Unknown(crate::CoverageReason::MetadataAbsent);
        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );
        let receipt = source
            .for_each_block(&ScanRequest::all().allow_unknown_execution(), |_| Ok(()))
            .unwrap();
        assert_eq!(receipt.transactions_with_unknown_execution, 1);
    }

    #[test]
    fn instruction_data_gate_can_select_one_program() {
        let mut source = fixture(SourceVerification::PublishedManifest);
        let instruction = &mut source.blocks[1].transactions[0].instructions[0];
        instruction.data.clear();
        instruction.data_coverage = InstructionDataCoverage::NotRequested;

        assert!(
            source
                .for_each_block(&ScanRequest::all(), |_| Ok(()))
                .is_err()
        );

        assert!(
            source
                .for_each_block(
                    &ScanRequest::all().with_instruction_data_for([[1; 32]]),
                    |_| Ok(()),
                )
                .is_err()
        );
        let receipt = source
            .for_each_block(
                &ScanRequest::all()
                    .with_instruction_data_for([[1; 32]])
                    .allow_incomplete_instruction_data(),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(receipt.instructions_not_requested, 1);

        let receipt = source
            .for_each_block(
                &ScanRequest::all().with_instruction_data_for([[99; 32]]),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(receipt.instructions_not_requested, 1);
        assert_eq!(receipt.instructions_with_unknown_data, 0);

        let duplicate = ScanRequest::all().with_instruction_data_for([[1; 32], [1; 32]]);
        assert!(source.for_each_block(&duplicate, |_| Ok(())).is_err());

        let empty = ScanRequest::all().with_instruction_data_for([]);
        assert!(source.for_each_block(&empty, |_| Ok(())).is_err());

        let too_many = ScanRequest::all().with_instruction_data_for(
            (0..=MAX_INSTRUCTION_DATA_PROGRAMS).map(|index| {
                let mut program = [0; 32];
                program[..8].copy_from_slice(&(index as u64).to_le_bytes());
                program
            }),
        );
        assert!(source.for_each_block(&too_many, |_| Ok(())).is_err());
    }

    #[test]
    fn older_json_requests_keep_strict_instruction_data_default() {
        let request = ScanRequest::all().with_instruction_data_for([[1; 32]]);
        let mut value = serde_json::to_value(&request).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("require_complete_instruction_data");
        let decoded: ScanRequest = serde_json::from_value(value).unwrap();
        assert!(decoded.require_complete_instruction_data);
    }

    #[test]
    fn publisher_rejects_wrong_epoch_slot_and_early_finish() {
        struct NoopSink;
        impl BlockSink for NoopSink {
            fn visit_block(&mut self, _block: BlockView<'_>) -> Result<()> {
                Ok(())
            }
        }

        let identity = fixture(SourceVerification::PublishedManifest).identity;
        let request = ScanRequest::all();
        let mut sink = NoopSink;
        let mut publisher = OrderedBlockPublisher::new(&identity, &request, &mut sink).unwrap();
        let mut wrong = CanonicalBlock {
            header: BlockHeader {
                epoch: 8,
                block_ordinal: 0,
                slot: EPOCH_7_FIRST_SLOT + 9,
            },
            transactions: Vec::new(),
        };
        assert!(publisher.publish(&wrong).is_err());
        wrong.header.epoch = 7;
        wrong.header.slot = EPOCH_7_FIRST_SLOT - 1;
        assert!(publisher.publish(&wrong).is_err());
        wrong.header.slot = EPOCH_7_FIRST_SLOT + identity.slots_per_epoch;
        assert!(publisher.publish(&wrong).is_err());
        wrong.header.slot = EPOCH_7_FIRST_SLOT + 9;
        publisher.publish(&wrong).unwrap();
        assert!(publisher.finish().is_err());
    }

    #[test]
    fn publisher_rejects_decreasing_slots_before_the_sink() {
        let source = fixture(SourceVerification::PublishedManifest);
        let request = ScanRequest::all();
        let mut calls = 0;
        let mut sink = FnBlockSink::new(|_: BlockView<'_>| {
            calls += 1;
            Ok(())
        });
        {
            let mut publisher =
                OrderedBlockPublisher::new(&source.identity, &request, &mut sink).unwrap();
            publisher.publish(&source.blocks[0]).unwrap();
            let mut second = source.blocks[1].clone();
            second.header.slot = source.blocks[0].header.slot;
            assert!(publisher.publish(&second).is_err());
        }
        assert_eq!(calls, 1);
    }
}

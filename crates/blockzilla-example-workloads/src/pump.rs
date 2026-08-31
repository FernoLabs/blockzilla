use std::io::Write;

use blockzilla_query_sdk::{
    BlockSink, BlockView, CpiCoverage, InstructionCoverage, ScanRequest, TransactionView,
};

use crate::{
    CoverageReport, FinishedOutput, OutputReport, Result,
    output::{CanonicalOutput, CoverageTracker, TransactionOrder, add, increment, target_header},
};

pub const MAINNET_PUMP_FUN_PROGRAM_BASE58: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const MAINNET_PUMP_FUN_PROGRAM: [u8; 32] = [
    0x01, 0x56, 0xe0, 0xf6, 0x93, 0x66, 0x5a, 0xcf, 0x44, 0xdb, 0x15, 0x68, 0xbf, 0x17, 0x5b, 0xaa,
    0x51, 0x89, 0xcb, 0x97, 0xf5, 0xd2, 0xff, 0x3b, 0x65, 0x5d, 0x2b, 0xb6, 0xfd, 0x6d, 0x18, 0xb0,
];

pub const PUMP_HEADER_BYTES: usize = 44;
pub const PUMP_RECORD_BYTES: usize = 92;
const PUMP_SCHEMA: &str = "blockzilla-example-pump-transaction/v1";
const PUMP_MAGIC: [u8; 8] = *b"BZPUMP01";

/// Some outer program evidence can be absent.
pub const PUMP_COVERAGE_INCOMPLETE_INSTRUCTIONS: u8 = 1 << 0;
/// Some CPI program evidence can be absent.
pub const PUMP_COVERAGE_INCOMPLETE_CPI: u8 = 1 << 1;
/// A confirmed match cannot be written without its primary signature.
pub const PUMP_COVERAGE_PRIMARY_SIGNATURE_UNAVAILABLE: u8 = 1 << 2;

/// Select program identities and signatures, but not instruction payloads.
pub fn pump_scan_request(request: ScanRequest) -> ScanRequest {
    request
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .without_required_signers()
        .without_execution_status()
        .without_instruction_accounts()
        .without_instruction_data()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpReport {
    pub blocks_seen: u64,
    pub transactions_seen: u64,
    /// Transactions with at least one confirmed direct or CPI invocation.
    pub matching_transactions: u64,
    /// Matching transactions written with an exact primary signature.
    pub written_transactions: u64,
    pub direct_invocations: u64,
    pub cpi_invocations: u64,
    pub incomplete_instruction_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    pub matches_without_primary_signature: u64,
    pub output_complete: bool,
    pub coverage: CoverageReport,
    pub output: OutputReport,
}

/// Write one fixed record for each confirmed Pump.fun transaction match.
pub struct PumpSink<W> {
    program: [u8; 32],
    output: CanonicalOutput<W>,
    order: TransactionOrder,
    coverage: CoverageTracker,
    blocks_seen: u64,
    transactions_seen: u64,
    matching_transactions: u64,
    written_transactions: u64,
    direct_invocations: u64,
    cpi_invocations: u64,
    incomplete_instruction_transactions: u64,
    incomplete_cpi_transactions: u64,
    matches_without_primary_signature: u64,
}

impl<W: Write> PumpSink<W> {
    pub fn new(writer: W, program: [u8; 32]) -> Result<Self> {
        let header = target_header(PUMP_MAGIC, PUMP_RECORD_BYTES as u32, program);
        Ok(Self {
            program,
            output: CanonicalOutput::new(writer, &header)?,
            order: TransactionOrder::default(),
            coverage: CoverageTracker::default(),
            blocks_seen: 0,
            transactions_seen: 0,
            matching_transactions: 0,
            written_transactions: 0,
            direct_invocations: 0,
            cpi_invocations: 0,
            incomplete_instruction_transactions: 0,
            incomplete_cpi_transactions: 0,
            matches_without_primary_signature: 0,
        })
    }

    pub fn mainnet(writer: W) -> Result<Self> {
        Self::new(writer, MAINNET_PUMP_FUN_PROGRAM)
    }

    pub fn process_block(&mut self, block: BlockView<'_>) -> Result<()> {
        for transaction in block.transaction_views() {
            self.process_transaction(transaction)?;
        }
        increment(&mut self.blocks_seen, "Pump.fun block")
    }

    pub fn process_transaction(&mut self, transaction: TransactionView<'_>) -> Result<()> {
        self.order.observe("Pump.fun", transaction)?;
        increment(&mut self.transactions_seen, "Pump.fun transaction")?;

        let mut reason_bits = 0_u8;
        if !matches!(
            transaction.header.instruction_coverage,
            InstructionCoverage::Complete
        ) {
            reason_bits |= PUMP_COVERAGE_INCOMPLETE_INSTRUCTIONS;
            increment(
                &mut self.incomplete_instruction_transactions,
                "Pump.fun incomplete-instruction transaction",
            )?;
        }
        if !matches!(transaction.header.cpi_coverage, CpiCoverage::Complete) {
            reason_bits |= PUMP_COVERAGE_INCOMPLETE_CPI;
            increment(
                &mut self.incomplete_cpi_transactions,
                "Pump.fun incomplete-CPI transaction",
            )?;
        }

        let mut direct_count = 0_u32;
        let mut cpi_count = 0_u32;
        for instruction in transaction.instructions {
            if instruction.program_id != self.program {
                continue;
            }
            let counter = if instruction.coordinate.inner_index.is_some() {
                &mut cpi_count
            } else {
                &mut direct_count
            };
            *counter = counter
                .checked_add(1)
                .ok_or(crate::Error::CounterOverflow("Pump.fun invocation"))?;
        }

        if direct_count != 0 || cpi_count != 0 {
            increment(
                &mut self.matching_transactions,
                "Pump.fun matching transaction",
            )?;
            add(
                &mut self.direct_invocations,
                u64::from(direct_count),
                "Pump.fun direct invocation",
            )?;
            add(
                &mut self.cpi_invocations,
                u64::from(cpi_count),
                "Pump.fun CPI invocation",
            )?;
            if let Some(signature) = transaction.primary_signature {
                let row = encode_match(transaction, *signature, direct_count, cpi_count);
                self.output.write_row(&row)?;
                increment(
                    &mut self.written_transactions,
                    "Pump.fun written transaction",
                )?;
            } else {
                reason_bits |= PUMP_COVERAGE_PRIMARY_SIGNATURE_UNAVAILABLE;
                increment(
                    &mut self.matches_without_primary_signature,
                    "Pump.fun match without primary signature",
                )?;
            }
        }

        if reason_bits != 0 {
            self.coverage.observe(transaction, reason_bits)?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<FinishedOutput<W, PumpReport>> {
        let Self {
            output,
            coverage,
            blocks_seen,
            transactions_seen,
            matching_transactions,
            written_transactions,
            direct_invocations,
            cpi_invocations,
            incomplete_instruction_transactions,
            incomplete_cpi_transactions,
            matches_without_primary_signature,
            ..
        } = self;
        let finished = output.finish(PUMP_SCHEMA)?;
        let coverage = coverage.finish();
        debug_assert_eq!(finished.report.row_count, written_transactions);
        Ok(FinishedOutput {
            writer: finished.writer,
            report: PumpReport {
                blocks_seen,
                transactions_seen,
                matching_transactions,
                written_transactions,
                direct_invocations,
                cpi_invocations,
                incomplete_instruction_transactions,
                incomplete_cpi_transactions,
                matches_without_primary_signature,
                output_complete: coverage.output_complete(),
                coverage,
                output: finished.report,
            },
        })
    }
}

impl<W: Write> BlockSink for PumpSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
        self.process_block(block)
            .map_err(blockzilla_query_sdk::Error::sink)
    }
}

fn encode_match(
    transaction: TransactionView<'_>,
    signature: [u8; 64],
    direct_count: u32,
    cpi_count: u32,
) -> [u8; PUMP_RECORD_BYTES] {
    let mut row = [0_u8; PUMP_RECORD_BYTES];
    row[0..8].copy_from_slice(&transaction.block.epoch.to_be_bytes());
    row[8..16].copy_from_slice(&transaction.block.slot.to_be_bytes());
    row[16..20].copy_from_slice(&transaction.header.tx_index.to_be_bytes());
    row[20..84].copy_from_slice(&signature);
    row[84..88].copy_from_slice(&direct_count.to_be_bytes());
    row[88..92].copy_from_slice(&cpi_count.to_be_bytes());
    row
}

#[cfg(test)]
mod tests {
    use blockzilla_query_sdk::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, ExecutionStatus, InstructionCoordinate,
        InstructionDataCoverage, ResolvedInstruction, TokenBalanceCoverage, TransactionHeader,
    };

    use super::*;

    fn instruction(
        order: u32,
        outer: u32,
        inner: Option<u32>,
        program: [u8; 32],
    ) -> ResolvedInstruction {
        ResolvedInstruction {
            coordinate: InstructionCoordinate {
                order,
                outer_index: outer,
                inner_index: inner,
                stack_height: inner.map(|_| 2),
            },
            program_id: program,
            accounts: vec![],
            data_coverage: InstructionDataCoverage::NotRequested,
            data: vec![],
        }
    }

    fn block(
        slot: u64,
        signature: Option<[u8; 64]>,
        instruction_coverage: InstructionCoverage,
        cpi_coverage: CpiCoverage,
        instructions: Vec<ResolvedInstruction>,
    ) -> CanonicalBlock {
        CanonicalBlock {
            header: BlockHeader {
                epoch: 7,
                block_ordinal: 0,
                slot,
            },
            transactions: vec![CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: 0,
                    status: ExecutionStatus::Succeeded,
                    failed_outer_instruction_index: None,
                    instruction_coverage,
                    cpi_coverage,
                },
                primary_signature: signature,
                required_signers: vec![],
                instructions,
                token_balance_coverage: TokenBalanceCoverage::NotRequested,
                token_balances: vec![],
            }],
        }
    }

    #[test]
    fn writes_signature_and_direct_cpi_counts() {
        let target = [0x44; 32];
        let source = block(
            70,
            Some([0x55; 64]),
            InstructionCoverage::Complete,
            CpiCoverage::Complete,
            vec![
                instruction(0, 0, None, target),
                instruction(1, 0, Some(0), target),
                instruction(2, 1, None, [0x66; 32]),
            ],
        );
        let mut sink = PumpSink::new(Vec::new(), target).unwrap();
        sink.process_block(source.as_view()).unwrap();
        let finished = sink.finish().unwrap();
        assert!(finished.report.output_complete);
        assert_eq!(finished.report.matching_transactions, 1);
        assert_eq!(finished.report.written_transactions, 1);
        assert_eq!(finished.report.direct_invocations, 1);
        assert_eq!(finished.report.cpi_invocations, 1);
        assert_eq!(
            finished.report.output.output_bytes,
            (PUMP_HEADER_BYTES + PUMP_RECORD_BYTES) as u64
        );
        let row = &finished.writer[PUMP_HEADER_BYTES..];
        assert_eq!(&row[20..84], &[0x55; 64]);
        assert_eq!(&row[84..88], &1_u32.to_be_bytes());
        assert_eq!(&row[88..92], &1_u32.to_be_bytes());
    }

    #[test]
    fn omitting_instruction_accounts_preserves_pump_output() {
        let target = [0x44; 32];
        let mut with_accounts = block(
            70,
            Some([0x55; 64]),
            InstructionCoverage::Complete,
            CpiCoverage::Complete,
            vec![
                instruction(0, 0, None, target),
                instruction(1, 0, Some(0), target),
            ],
        );
        for instruction in &mut with_accounts.transactions[0].instructions {
            instruction.accounts = vec![[0xa1; 32], [0xa2; 32]];
        }
        let mut without_accounts = with_accounts.clone();
        for instruction in &mut without_accounts.transactions[0].instructions {
            instruction.accounts.clear();
        }

        let run = |source: CanonicalBlock| {
            let mut sink = PumpSink::new(Vec::new(), target).unwrap();
            sink.process_block(source.as_view()).unwrap();
            sink.finish().unwrap()
        };
        let full = run(with_accounts);
        let projected = run(without_accounts);
        assert_eq!(projected.report, full.report);
        assert_eq!(projected.writer, full.writer);
    }

    #[test]
    fn incomplete_cpi_and_missing_signature_are_coverage_not_errors() {
        let target = [0x44; 32];
        let source = block(
            70,
            None,
            InstructionCoverage::Complete,
            CpiCoverage::NotRecorded,
            vec![instruction(0, 0, None, target)],
        );
        let mut sink = PumpSink::new(Vec::new(), target).unwrap();
        sink.process_block(source.as_view()).unwrap();
        let finished = sink.finish().unwrap();
        assert!(!finished.report.output_complete);
        assert_eq!(finished.report.matching_transactions, 1);
        assert_eq!(finished.report.written_transactions, 0);
        assert_eq!(finished.report.incomplete_cpi_transactions, 1);
        assert_eq!(finished.report.matches_without_primary_signature, 1);
        assert_eq!(finished.report.coverage.indeterminate_transactions, 1);
    }

    #[test]
    fn coverage_digest_binds_the_transaction_coordinate() {
        let target = [0x44; 32];
        let run = |slot| {
            let source = block(
                slot,
                Some([0x55; 64]),
                InstructionCoverage::Unknown(blockzilla_query_sdk::CoverageReason::MetadataAbsent),
                CpiCoverage::Complete,
                vec![],
            );
            let mut sink = PumpSink::new(Vec::new(), target).unwrap();
            sink.process_block(source.as_view()).unwrap();
            sink.finish().unwrap().report.coverage.sha256
        };
        assert_ne!(run(70), run(71));
    }

    #[test]
    fn request_preserves_partial_recorded_evidence() {
        let request = pump_scan_request(ScanRequest::all());
        assert!(!request.require_complete_instructions);
        assert!(!request.require_complete_cpi);
        assert!(!request.require_known_execution);
        assert!(request.include_instructions);
        assert!(!request.include_instruction_accounts);
        assert!(!request.include_required_signers);
        assert!(!request.include_execution_status);
        assert!(request.include_primary_signatures);
        assert!(matches!(
            request.instruction_data,
            blockzilla_query_sdk::InstructionDataRequirement::None
        ));
    }
}

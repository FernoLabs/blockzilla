use std::{collections::BTreeSet, io::Write};

use blockzilla_query_sdk::{
    BlockSink, BlockView, CpiCoverage, ExecutionStatus, InstructionCoverage, ScanRequest,
    TransactionView,
};

use crate::{
    CoverageReport, FinishedOutput, OutputReport, Result,
    output::{CanonicalOutput, CoverageTracker, TransactionOrder, add, increment, target_header},
};

pub const FIREWATCH_HEADER_BYTES: usize = 44;
pub const FIREWATCH_RECORD_BYTES: usize = 64;
const FIREWATCH_SCHEMA: &str = "blockzilla-example-firewatch-wallet-program/v1";
const FIREWATCH_MAGIC: [u8; 8] = *b"BZFWAL01";

/// The signer transaction can be successful or failed.
pub const FIREWATCH_COVERAGE_UNKNOWN_EXECUTION: u8 = 1 << 0;
/// Some outer reached-program evidence can be absent from a successful transaction.
pub const FIREWATCH_COVERAGE_INCOMPLETE_INSTRUCTIONS: u8 = 1 << 1;
/// Some CPI reached-program evidence can be absent from a successful transaction.
pub const FIREWATCH_COVERAGE_INCOMPLETE_CPI: u8 = 1 << 2;

/// Select signer, status, and program identities without payload bytes.
pub fn firewatch_scan_request(request: ScanRequest) -> ScanRequest {
    request
        .allow_incomplete_instructions()
        .allow_incomplete_cpi()
        .allow_unknown_execution()
        .without_primary_signatures()
        .without_instruction_accounts()
        .without_instruction_data()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirewatchReport {
    pub blocks_seen: u64,
    pub transactions_seen: u64,
    pub signer_transactions: u64,
    pub successful_signer_transactions: u64,
    pub failed_signer_transactions: u64,
    pub unknown_execution_signer_transactions: u64,
    pub incomplete_instruction_transactions: u64,
    pub incomplete_cpi_transactions: u64,
    /// Confirmed direct and recorded CPI instructions in successful signer transactions.
    pub reached_instructions: u64,
    pub distinct_programs: u64,
    pub output_complete: bool,
    pub coverage: CoverageReport,
    pub output: OutputReport,
}

/// Build one target signer's distinct successful reached-program list.
pub struct FirewatchSink<W> {
    wallet: [u8; 32],
    output: CanonicalOutput<W>,
    order: TransactionOrder,
    coverage: CoverageTracker,
    programs: BTreeSet<[u8; 32]>,
    blocks_seen: u64,
    transactions_seen: u64,
    signer_transactions: u64,
    successful_signer_transactions: u64,
    failed_signer_transactions: u64,
    unknown_execution_signer_transactions: u64,
    incomplete_instruction_transactions: u64,
    incomplete_cpi_transactions: u64,
    reached_instructions: u64,
}

impl<W: Write> FirewatchSink<W> {
    pub fn new(writer: W, wallet: [u8; 32]) -> Result<Self> {
        let header = target_header(FIREWATCH_MAGIC, FIREWATCH_RECORD_BYTES as u32, wallet);
        Ok(Self {
            wallet,
            output: CanonicalOutput::new(writer, &header)?,
            order: TransactionOrder::default(),
            coverage: CoverageTracker::default(),
            programs: BTreeSet::new(),
            blocks_seen: 0,
            transactions_seen: 0,
            signer_transactions: 0,
            successful_signer_transactions: 0,
            failed_signer_transactions: 0,
            unknown_execution_signer_transactions: 0,
            incomplete_instruction_transactions: 0,
            incomplete_cpi_transactions: 0,
            reached_instructions: 0,
        })
    }

    pub fn process_block(&mut self, block: BlockView<'_>) -> Result<()> {
        for transaction in block.transaction_views() {
            self.process_transaction(transaction)?;
        }
        increment(&mut self.blocks_seen, "FireWatch block")
    }

    pub fn process_transaction(&mut self, transaction: TransactionView<'_>) -> Result<()> {
        self.order.observe("FireWatch", transaction)?;
        increment(&mut self.transactions_seen, "FireWatch transaction")?;
        if !transaction.required_signers.contains(&self.wallet) {
            return Ok(());
        }
        increment(
            &mut self.signer_transactions,
            "FireWatch signer transaction",
        )?;

        match transaction.header.status {
            ExecutionStatus::Failed => {
                increment(
                    &mut self.failed_signer_transactions,
                    "FireWatch failed signer transaction",
                )?;
            }
            ExecutionStatus::Unknown(_) => {
                increment(
                    &mut self.unknown_execution_signer_transactions,
                    "FireWatch unknown-execution signer transaction",
                )?;
                self.coverage
                    .observe(transaction, FIREWATCH_COVERAGE_UNKNOWN_EXECUTION)?;
            }
            ExecutionStatus::Succeeded => self.process_successful(transaction)?,
        }
        Ok(())
    }

    fn process_successful(&mut self, transaction: TransactionView<'_>) -> Result<()> {
        increment(
            &mut self.successful_signer_transactions,
            "FireWatch successful signer transaction",
        )?;
        let mut reason_bits = 0_u8;
        if !matches!(
            transaction.header.instruction_coverage,
            InstructionCoverage::Complete
        ) {
            reason_bits |= FIREWATCH_COVERAGE_INCOMPLETE_INSTRUCTIONS;
            increment(
                &mut self.incomplete_instruction_transactions,
                "FireWatch incomplete-instruction transaction",
            )?;
        }
        if !matches!(transaction.header.cpi_coverage, CpiCoverage::Complete) {
            reason_bits |= FIREWATCH_COVERAGE_INCOMPLETE_CPI;
            increment(
                &mut self.incomplete_cpi_transactions,
                "FireWatch incomplete-CPI transaction",
            )?;
        }
        for instruction in transaction.instructions {
            let program = instruction.program_id.ok_or_else(|| {
                crate::Error::InvalidInput(
                    "FireWatch needs program keys for successful signer transactions".into(),
                )
            })?;
            self.programs.insert(program);
        }
        add(
            &mut self.reached_instructions,
            u64::try_from(transaction.instructions.len())
                .map_err(|_| crate::Error::CounterOverflow("FireWatch reached instruction"))?,
            "FireWatch reached instruction",
        )?;
        if reason_bits != 0 {
            self.coverage.observe(transaction, reason_bits)?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<FinishedOutput<W, FirewatchReport>> {
        let Self {
            wallet,
            mut output,
            coverage,
            programs,
            blocks_seen,
            transactions_seen,
            signer_transactions,
            successful_signer_transactions,
            failed_signer_transactions,
            unknown_execution_signer_transactions,
            incomplete_instruction_transactions,
            incomplete_cpi_transactions,
            reached_instructions,
            ..
        } = self;
        for program in programs {
            let mut row = [0_u8; FIREWATCH_RECORD_BYTES];
            row[..32].copy_from_slice(&wallet);
            row[32..].copy_from_slice(&program);
            output.write_row(&row)?;
        }
        let finished = output.finish(FIREWATCH_SCHEMA)?;
        let coverage = coverage.finish();
        let distinct_programs = finished.report.row_count;
        Ok(FinishedOutput {
            writer: finished.writer,
            report: FirewatchReport {
                blocks_seen,
                transactions_seen,
                signer_transactions,
                successful_signer_transactions,
                failed_signer_transactions,
                unknown_execution_signer_transactions,
                incomplete_instruction_transactions,
                incomplete_cpi_transactions,
                reached_instructions,
                distinct_programs,
                output_complete: coverage.output_complete(),
                coverage,
                output: finished.report,
            },
        })
    }
}

impl<W: Write> BlockSink for FirewatchSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
        self.process_block(block)
            .map_err(blockzilla_query_sdk::Error::sink)
    }
}

#[cfg(test)]
mod tests {
    use blockzilla_query_sdk::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, CoverageReason, InstructionCoordinate,
        InstructionDataCoverage, ResolvedInstruction, TokenBalanceCoverage, TransactionHeader,
    };

    use super::*;

    fn instructions(programs: &[[u8; 32]]) -> Vec<ResolvedInstruction> {
        programs
            .iter()
            .enumerate()
            .map(|(index, program)| ResolvedInstruction {
                coordinate: InstructionCoordinate {
                    order: index as u32,
                    outer_index: index as u32,
                    inner_index: None,
                    stack_height: None,
                },
                program_id: Some(*program),
                accounts: vec![],
                data_coverage: InstructionDataCoverage::NotRequested,
                data: vec![],
            })
            .collect()
    }

    fn transaction(
        tx_index: u32,
        signer: [u8; 32],
        status: ExecutionStatus,
        instruction_coverage: InstructionCoverage,
        cpi_coverage: CpiCoverage,
        programs: &[[u8; 32]],
    ) -> CanonicalTransaction {
        CanonicalTransaction {
            header: TransactionHeader {
                tx_index,
                status,
                failed_outer_instruction_index: None,
                instruction_coverage,
                cpi_coverage,
            },
            primary_signature: None,
            required_signers: vec![signer],
            instructions: instructions(programs),
            token_balance_coverage: TokenBalanceCoverage::NotRequested,
            token_balances: vec![],
        }
    }

    #[test]
    fn writes_sorted_distinct_programs_from_successful_signer_transactions() {
        let wallet = [0x11; 32];
        let source = CanonicalBlock {
            counts: None,
            header: BlockHeader {
                epoch: 8,
                block_ordinal: 0,
                slot: 80,
            },
            transactions: vec![
                transaction(
                    0,
                    wallet,
                    ExecutionStatus::Succeeded,
                    InstructionCoverage::Complete,
                    CpiCoverage::Complete,
                    &[[0x33; 32], [0x22; 32], [0x33; 32]],
                ),
                transaction(
                    1,
                    wallet,
                    ExecutionStatus::Failed,
                    InstructionCoverage::Complete,
                    CpiCoverage::Complete,
                    &[[0x44; 32]],
                ),
                transaction(
                    2,
                    [0x99; 32],
                    ExecutionStatus::Succeeded,
                    InstructionCoverage::Complete,
                    CpiCoverage::Complete,
                    &[[0x55; 32]],
                ),
            ],
        };
        let mut sink = FirewatchSink::new(Vec::new(), wallet).unwrap();
        sink.process_block(source.as_view()).unwrap();
        let finished = sink.finish().unwrap();

        assert!(finished.report.output_complete);
        assert_eq!(finished.report.signer_transactions, 2);
        assert_eq!(finished.report.successful_signer_transactions, 1);
        assert_eq!(finished.report.failed_signer_transactions, 1);
        assert_eq!(finished.report.reached_instructions, 3);
        assert_eq!(finished.report.distinct_programs, 2);
        assert_eq!(
            finished.report.output.output_bytes,
            (FIREWATCH_HEADER_BYTES + 2 * FIREWATCH_RECORD_BYTES) as u64
        );
        let first = &finished.writer
            [FIREWATCH_HEADER_BYTES..FIREWATCH_HEADER_BYTES + FIREWATCH_RECORD_BYTES];
        let second = &finished.writer[FIREWATCH_HEADER_BYTES + FIREWATCH_RECORD_BYTES..];
        assert_eq!(&first[..32], &wallet);
        assert_eq!(&first[32..], &[0x22; 32]);
        assert_eq!(&second[32..], &[0x33; 32]);
    }

    #[test]
    fn omitting_instruction_accounts_preserves_firewatch_output() {
        let wallet = [0x11; 32];
        let mut with_accounts = CanonicalBlock {
            counts: None,
            header: BlockHeader {
                epoch: 8,
                block_ordinal: 0,
                slot: 80,
            },
            transactions: vec![transaction(
                0,
                wallet,
                ExecutionStatus::Succeeded,
                InstructionCoverage::Complete,
                CpiCoverage::Complete,
                &[[0x33; 32], [0x22; 32], [0x33; 32]],
            )],
        };
        for instruction in &mut with_accounts.transactions[0].instructions {
            instruction.accounts = vec![[0xa1; 32], [0xa2; 32]];
        }
        let mut without_accounts = with_accounts.clone();
        for instruction in &mut without_accounts.transactions[0].instructions {
            instruction.accounts.clear();
        }

        let run = |source: CanonicalBlock| {
            let mut sink = FirewatchSink::new(Vec::new(), wallet).unwrap();
            sink.process_block(source.as_view()).unwrap();
            sink.finish().unwrap()
        };
        let full = run(with_accounts);
        let projected = run(without_accounts);
        assert_eq!(projected.report, full.report);
        assert_eq!(projected.writer, full.writer);
    }

    #[test]
    fn keeps_confirmed_programs_and_records_coverage_gaps() {
        let wallet = [0x11; 32];
        let source = CanonicalBlock {
            counts: None,
            header: BlockHeader {
                epoch: 8,
                block_ordinal: 0,
                slot: 80,
            },
            transactions: vec![
                transaction(
                    0,
                    wallet,
                    ExecutionStatus::Succeeded,
                    InstructionCoverage::Complete,
                    CpiCoverage::NotRecorded,
                    &[[0x22; 32]],
                ),
                transaction(
                    1,
                    wallet,
                    ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
                    InstructionCoverage::Complete,
                    CpiCoverage::Complete,
                    &[[0x33; 32]],
                ),
            ],
        };
        let mut sink = FirewatchSink::new(Vec::new(), wallet).unwrap();
        sink.process_block(source.as_view()).unwrap();
        let finished = sink.finish().unwrap();
        assert!(!finished.report.output_complete);
        assert_eq!(finished.report.distinct_programs, 1);
        assert_eq!(finished.report.incomplete_cpi_transactions, 1);
        assert_eq!(finished.report.unknown_execution_signer_transactions, 1);
        assert_eq!(finished.report.coverage.indeterminate_transactions, 2);
    }

    #[test]
    fn request_keeps_partial_historical_coverage_visible() {
        let request = firewatch_scan_request(ScanRequest::all());
        assert!(!request.require_complete_instructions);
        assert!(!request.require_complete_cpi);
        assert!(!request.require_known_execution);
        assert!(!request.include_primary_signatures);
        assert!(!request.include_instruction_accounts);
        assert!(matches!(
            request.instruction_data,
            blockzilla_query_sdk::InstructionDataRequirement::None
        ));
    }
}

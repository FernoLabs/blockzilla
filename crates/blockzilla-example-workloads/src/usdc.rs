use std::io::Write;

use blockzilla_query_sdk::{
    BlockSink, BlockView, RecordedTokenBalance, ScanRequest, TokenBalanceCoverage,
    TokenBalanceSide, TransactionView,
};

use crate::{
    CoverageReport, Error, FinishedOutput, OutputReport, Result,
    output::{CanonicalOutput, CoverageTracker, TransactionOrder, increment, target_header},
};

pub const MAINNET_USDC_MINT_BASE58: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const MAINNET_USDC_MINT: [u8; 32] = [
    0xc6, 0xfa, 0x7a, 0xf3, 0xbe, 0xdb, 0xad, 0x3a, 0x3d, 0x65, 0xf3, 0x6a, 0xab, 0xc9, 0x74, 0x31,
    0xb1, 0xbb, 0xe4, 0xc2, 0xd2, 0xf6, 0xe0, 0xe4, 0x7c, 0xa6, 0x02, 0x03, 0x45, 0x2f, 0x5d, 0x61,
];

pub const USDC_HEADER_BYTES: usize = 44;
pub const USDC_RECORD_BYTES: usize = 136;
const USDC_SCHEMA: &str = "blockzilla-example-usdc-recorded-balance/v1";
const USDC_MAGIC: [u8; 8] = *b"BZUSDC01";

/// The requested token-balance plane was not complete for this transaction.
pub const USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE: u8 = 1 << 0;
/// At least one recorded token-balance row did not contain an exact mint.
pub const USDC_COVERAGE_TOKEN_MINT_UNAVAILABLE: u8 = 1 << 1;

/// Select only the token-balance plane needed by this workload.
pub fn usdc_scan_request(request: ScanRequest, mint: [u8; 32]) -> ScanRequest {
    request
        .without_instructions()
        .without_required_signers()
        .without_execution_status()
        .without_primary_signatures()
        .with_token_balances_for([mint])
        .allow_incomplete_token_balances()
}

/// Exact counters and canonical output identity for one USDC dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsdcReport {
    pub blocks_seen: u64,
    pub transactions_seen: u64,
    pub matching_transactions: u64,
    pub pre_rows: u64,
    pub post_rows: u64,
    pub token_balances_unavailable_transactions: u64,
    pub token_mint_unavailable_transactions: u64,
    pub output_complete: bool,
    pub coverage: CoverageReport,
    pub output: OutputReport,
}

/// Stream selected recorded balances to a deterministic fixed-record file.
pub struct UsdcBalanceSink<W> {
    mint: [u8; 32],
    output: CanonicalOutput<W>,
    order: TransactionOrder,
    coverage: CoverageTracker,
    blocks_seen: u64,
    transactions_seen: u64,
    matching_transactions: u64,
    pre_rows: u64,
    post_rows: u64,
    token_balances_unavailable_transactions: u64,
    token_mint_unavailable_transactions: u64,
}

impl<W: Write> UsdcBalanceSink<W> {
    pub fn new(writer: W, mint: [u8; 32]) -> Result<Self> {
        let header = target_header(USDC_MAGIC, USDC_RECORD_BYTES as u32, mint);
        Ok(Self {
            mint,
            output: CanonicalOutput::new(writer, &header)?,
            order: TransactionOrder::default(),
            coverage: CoverageTracker::default(),
            blocks_seen: 0,
            transactions_seen: 0,
            matching_transactions: 0,
            pre_rows: 0,
            post_rows: 0,
            token_balances_unavailable_transactions: 0,
            token_mint_unavailable_transactions: 0,
        })
    }

    pub fn mainnet(writer: W) -> Result<Self> {
        Self::new(writer, MAINNET_USDC_MINT)
    }

    pub fn process_block(&mut self, block: BlockView<'_>) -> Result<()> {
        for transaction in block.transaction_views() {
            self.process_transaction(transaction)?;
        }
        increment(&mut self.blocks_seen, "USDC block")
    }

    pub fn process_transaction(&mut self, transaction: TransactionView<'_>) -> Result<()> {
        self.order.observe("USDC", transaction)?;
        increment(&mut self.transactions_seen, "USDC transaction")?;
        let mut reason_bits = 0_u8;
        if !matches!(
            transaction.token_balance_coverage,
            TokenBalanceCoverage::Complete
        ) {
            reason_bits |= USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE;
            increment(
                &mut self.token_balances_unavailable_transactions,
                "USDC unavailable token-balance transaction",
            )?;
        }

        validate_balance_order(transaction)?;
        let mut matched = false;
        let mut missing_mint = false;
        for side in [TokenBalanceSide::Pre, TokenBalanceSide::Post] {
            for balance in transaction
                .token_balances
                .iter()
                .filter(|balance| balance.side == side)
            {
                let Some(mint) = balance.mint else {
                    missing_mint = true;
                    continue;
                };
                if mint != self.mint {
                    continue;
                }
                let row = encode_balance(transaction, *balance);
                self.output.write_row(&row)?;
                matched = true;
                match side {
                    TokenBalanceSide::Pre => increment(&mut self.pre_rows, "USDC pre row")?,
                    TokenBalanceSide::Post => increment(&mut self.post_rows, "USDC post row")?,
                }
            }
        }
        if matched {
            increment(&mut self.matching_transactions, "USDC matching transaction")?;
        }
        if missing_mint {
            reason_bits |= USDC_COVERAGE_TOKEN_MINT_UNAVAILABLE;
            increment(
                &mut self.token_mint_unavailable_transactions,
                "USDC unavailable token-mint transaction",
            )?;
        }
        if reason_bits != 0 {
            self.coverage.observe(transaction, reason_bits)?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<FinishedOutput<W, UsdcReport>> {
        let Self {
            output,
            coverage,
            blocks_seen,
            transactions_seen,
            matching_transactions,
            pre_rows,
            post_rows,
            token_balances_unavailable_transactions,
            token_mint_unavailable_transactions,
            ..
        } = self;
        let finished = output.finish(USDC_SCHEMA)?;
        let coverage = coverage.finish();
        debug_assert_eq!(finished.report.row_count, pre_rows + post_rows);
        Ok(FinishedOutput {
            writer: finished.writer,
            report: UsdcReport {
                blocks_seen,
                transactions_seen,
                matching_transactions,
                pre_rows,
                post_rows,
                token_balances_unavailable_transactions,
                token_mint_unavailable_transactions,
                output_complete: coverage.output_complete(),
                coverage,
                output: finished.report,
            },
        })
    }
}

impl<W: Write> BlockSink for UsdcBalanceSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
        self.process_block(block)
            .map_err(blockzilla_query_sdk::Error::sink)
    }
}

fn validate_balance_order(transaction: TransactionView<'_>) -> Result<()> {
    let mut last_pre = None;
    let mut last_post = None;
    for balance in transaction.token_balances {
        let (last, side) = match balance.side {
            TokenBalanceSide::Pre => (&mut last_pre, "pre"),
            TokenBalanceSide::Post => (&mut last_post, "post"),
        };
        if last.is_some_and(|last| balance.balance_index <= last) {
            return Err(Error::TokenBalanceOrder {
                epoch: transaction.block.epoch,
                slot: transaction.block.slot,
                tx_index: transaction.header.tx_index,
                side,
            });
        }
        *last = Some(balance.balance_index);
    }
    Ok(())
}

fn encode_balance(
    transaction: TransactionView<'_>,
    balance: RecordedTokenBalance,
) -> [u8; USDC_RECORD_BYTES] {
    let mut row = [0_u8; USDC_RECORD_BYTES];
    row[0..8].copy_from_slice(&transaction.block.epoch.to_be_bytes());
    row[8..16].copy_from_slice(&transaction.block.slot.to_be_bytes());
    row[16..20].copy_from_slice(&transaction.header.tx_index.to_be_bytes());
    row[20] = match balance.side {
        TokenBalanceSide::Pre => 0,
        TokenBalanceSide::Post => 1,
    };
    row[21..25].copy_from_slice(&balance.balance_index.to_be_bytes());
    row[25..29].copy_from_slice(&balance.account_index.to_be_bytes());
    row[29..61].copy_from_slice(
        balance
            .mint
            .as_ref()
            .expect("validated token balance has a mint"),
    );
    encode_optional_pubkey(&mut row[61..94], balance.owner);
    encode_optional_pubkey(&mut row[94..127], balance.token_program);
    row[127..135].copy_from_slice(&balance.amount.to_be_bytes());
    row[135] = balance.decimals;
    row
}

fn encode_optional_pubkey(output: &mut [u8], value: Option<[u8; 32]>) {
    debug_assert_eq!(output.len(), 33);
    if let Some(value) = value {
        output[0] = 1;
        output[1..].copy_from_slice(&value);
    }
}

#[cfg(test)]
mod tests {
    use blockzilla_query_sdk::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, CpiCoverage, ExecutionStatus,
        InstructionCoverage, TokenBalanceCoverage, TransactionHeader,
    };

    use super::*;

    fn balance(
        side: TokenBalanceSide,
        balance_index: u32,
        mint: Option<[u8; 32]>,
        amount: u64,
    ) -> RecordedTokenBalance {
        RecordedTokenBalance {
            side,
            balance_index,
            account_index: balance_index + 10,
            mint,
            owner: (balance_index == 2).then_some([0x22; 32]),
            token_program: (balance_index == 2).then_some([0x33; 32]),
            amount,
            decimals: 6,
        }
    }

    fn block(
        balances: Vec<RecordedTokenBalance>,
        coverage: TokenBalanceCoverage,
    ) -> CanonicalBlock {
        CanonicalBlock {
            counts: None,
            header: BlockHeader {
                epoch: 9,
                block_ordinal: 0,
                slot: 99,
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
                required_signers: vec![],
                instructions: vec![],
                token_balance_coverage: coverage,
                token_balances: balances,
            }],
        }
    }

    #[test]
    fn writes_selected_rows_in_pre_then_post_order() {
        let target = [0xaa; 32];
        let source = block(
            vec![
                balance(TokenBalanceSide::Post, 2, Some(target), 20),
                balance(TokenBalanceSide::Pre, 1, Some([0xbb; 32]), 10),
                balance(TokenBalanceSide::Pre, 2, Some(target), 15),
            ],
            TokenBalanceCoverage::Complete,
        );
        let mut sink = UsdcBalanceSink::new(Vec::new(), target).unwrap();
        sink.process_block(source.as_view()).unwrap();
        let finished = sink.finish().unwrap();

        assert_eq!(finished.report.blocks_seen, 1);
        assert_eq!(finished.report.transactions_seen, 1);
        assert_eq!(finished.report.matching_transactions, 1);
        assert_eq!(finished.report.pre_rows, 1);
        assert_eq!(finished.report.post_rows, 1);
        assert_eq!(finished.report.output.row_count, 2);
        assert_eq!(
            finished.report.output.output_bytes,
            (USDC_HEADER_BYTES + 2 * USDC_RECORD_BYTES) as u64
        );
        let first = &finished.writer[USDC_HEADER_BYTES..USDC_HEADER_BYTES + USDC_RECORD_BYTES];
        let second = &finished.writer[USDC_HEADER_BYTES + USDC_RECORD_BYTES..];
        assert_eq!(first[20], 0);
        assert_eq!(second[20], 1);
        assert_eq!(&first[127..135], &15_u64.to_be_bytes());
        assert_eq!(&second[127..135], &20_u64.to_be_bytes());
    }

    #[test]
    fn records_incomplete_or_unidentified_token_balances() {
        let target = [0xaa; 32];
        let incomplete = block(vec![], TokenBalanceCoverage::NotRequested);
        let mut sink = UsdcBalanceSink::new(Vec::new(), target).unwrap();
        sink.process_block(incomplete.as_view()).unwrap();
        let report = sink.finish().unwrap().report;
        assert!(!report.output_complete);
        assert_eq!(report.token_balances_unavailable_transactions, 1);
        assert_eq!(report.coverage.indeterminate_transactions, 1);

        let unidentified = block(
            vec![balance(TokenBalanceSide::Pre, 0, None, 1)],
            TokenBalanceCoverage::Complete,
        );
        let mut sink = UsdcBalanceSink::new(Vec::new(), target).unwrap();
        sink.process_block(unidentified.as_view()).unwrap();
        let report = sink.finish().unwrap().report;
        assert!(!report.output_complete);
        assert_eq!(report.token_mint_unavailable_transactions, 1);
        assert_eq!(report.coverage.indeterminate_transactions, 1);
    }

    #[test]
    fn request_selects_only_the_target_balance_plane() {
        let target = [0xaa; 32];
        let request = usdc_scan_request(ScanRequest::all(), target);
        assert!(!request.require_complete_instructions);
        assert!(!request.require_complete_cpi);
        assert!(!request.require_known_execution);
        assert!(!request.include_instructions);
        assert!(!request.include_required_signers);
        assert!(!request.include_execution_status);
        assert!(!request.include_primary_signatures);
        assert!(!request.require_complete_token_balances);
        assert!(matches!(
            request.token_balances,
            blockzilla_query_sdk::TokenBalanceRequirement::Mints(ref mints)
                if mints == &[target]
        ));
    }
}

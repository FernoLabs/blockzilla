//! Small, strict token-balance export example for the common reader API.

use std::{io::Write, time::Instant};

use anyhow::{Context, Result};
use blockzilla_compact_v2_reader::archive::{
    ArchiveInstructionSource, BlockSink, BlockView, CompactV2ParallelScanConfig, QueryError,
    QueryResult,
};
use blockzilla_model::{ExecutionStatus, TokenBalanceCoverage, TokenBalanceSide};
use clap::Args;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::stream::{self, EpochSourceArgs};

#[derive(Args, Debug)]
pub(crate) struct DumpTokensArgs {
    #[command(flatten)]
    pub source: EpochSourceArgs,
    /// Select one base58 mint. Unknown mint identities remain visible as null.
    #[arg(long, value_parser = parse_mint)]
    pub mint: Option<[u8; 32]>,
}

fn parse_mint(value: &str) -> Result<[u8; 32], String> {
    bs58::decode(value)
        .into_vec()
        .map_err(|error| format!("Invalid base58 mint: {error}"))?
        .try_into()
        .map_err(|_| "A mint must decode to exactly 32 bytes".into())
}

#[derive(Serialize)]
struct TokenRow<'a> {
    source_id: &'a str,
    epoch: u64,
    block_ordinal: u32,
    slot: u64,
    tx_index: u32,
    execution_status: &'static str,
    side: TokenBalanceSide,
    balance_index: u32,
    account_index: u32,
    mint: Option<String>,
    owner: Option<String>,
    token_program: Option<String>,
    // A decimal string also preserves every UInt64 value in JavaScript clients.
    amount: String,
    decimals: u8,
}

struct TokenSink<W> {
    output: W,
    source_id: String,
    rows: u64,
    blocks: u64,
    last_report: Instant,
}

fn key_text(key: Option<[u8; 32]>) -> Option<String> {
    key.map(|key| bs58::encode(key).into_string())
}

impl<W: Write> BlockSink for TokenSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> QueryResult<()> {
        // Do not publish a partly accepted block when required facts are absent.
        for tx in block.transaction_views() {
            if matches!(tx.header.status, ExecutionStatus::Unknown(_))
                || tx.token_balance_coverage != TokenBalanceCoverage::Complete
            {
                return Err(QueryError::sink(std::io::Error::other(format!(
                    "Incomplete token data at slot {}, transaction {}",
                    block.header.slot, tx.header.tx_index,
                ))));
            }
        }
        for tx in block.transaction_views() {
            for balance in tx.token_balances {
                let row = TokenRow {
                    source_id: &self.source_id,
                    epoch: block.header.epoch,
                    block_ordinal: block.header.block_ordinal,
                    slot: block.header.slot,
                    tx_index: tx.header.tx_index,
                    execution_status: match tx.header.status {
                        ExecutionStatus::Succeeded => "succeeded",
                        ExecutionStatus::Failed => "failed",
                        ExecutionStatus::Unknown(_) => unreachable!("checked above"),
                    },
                    side: balance.side,
                    balance_index: balance.balance_index,
                    account_index: balance.account_index,
                    mint: key_text(balance.mint),
                    owner: key_text(balance.owner),
                    token_program: key_text(balance.token_program),
                    amount: balance.amount.to_string(),
                    decimals: balance.decimals,
                };
                serde_json::to_writer(&mut self.output, &row).map_err(QueryError::sink)?;
                self.output.write_all(b"\n").map_err(QueryError::sink)?;
                self.rows += 1;
            }
        }
        self.output.flush().map_err(QueryError::sink)?;
        self.blocks += 1;
        if self.last_report.elapsed().as_secs() >= 3 {
            eprintln!(
                "Read {} blocks, wrote {} token balance rows",
                self.blocks, self.rows
            );
            self.last_report = Instant::now();
        }
        Ok(())
    }
}

pub(crate) fn run(args: DumpTokensArgs) -> Result<()> {
    let started = Instant::now();
    let mut archive = stream::open(&args.source)?;
    let request = stream::request(&archive, &args.source)?
        .without_instructions()
        .without_primary_signatures()
        .without_required_signers();
    let request = match args.mint {
        Some(mint) => request.with_token_balances_for([mint]),
        None => request.with_token_balances(),
    };
    let source_bytes = serde_json::to_vec(archive.identity())?;
    let source_id = Sha256::digest(&source_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    eprintln!("Source ID: {source_id}\nWriting recorded token balances to stdout...");
    let stdout = std::io::stdout();
    let mut sink = TokenSink {
        output: stdout.lock(),
        source_id,
        rows: 0,
        blocks: 0,
        last_report: Instant::now(),
    };
    let receipt = archive
        .scan_ordered_parallel(
            &request,
            &mut sink,
            CompactV2ParallelScanConfig::new(usize::from(args.source.threads)),
        )
        .context("Token dump stopped; output can contain an incomplete prefix")?;
    archive.verify_local_unchanged()?;
    anyhow::ensure!(
        receipt.scan.blocks == sink.blocks,
        "Token dump block counts differ"
    );
    eprintln!(
        "Token dump complete: {} blocks, {} balance rows in {:.1}s",
        sink.blocks,
        sink.rows,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_model::{
        BlockHeader, CanonicalTransaction, CoverageReason, CpiCoverage, InstructionCoverage,
        RecordedTokenBalance, TransactionHeader,
    };

    fn transaction() -> CanonicalTransaction {
        CanonicalTransaction {
            header: TransactionHeader {
                tx_index: 0,
                status: ExecutionStatus::Failed,
                failed_outer_instruction_index: None,
                instruction_coverage: InstructionCoverage::Unknown(
                    CoverageReason::ProjectionNotRequested,
                ),
                cpi_coverage: CpiCoverage::NotRecorded,
            },
            primary_signature: None,
            required_signers: Vec::new(),
            instructions: Vec::new(),
            token_balance_coverage: TokenBalanceCoverage::Complete,
            token_balances: vec![RecordedTokenBalance {
                side: TokenBalanceSide::Post,
                balance_index: 9,
                account_index: 4,
                mint: None,
                owner: Some([1; 32]),
                token_program: None,
                amount: u64::MAX,
                decimals: 9,
            }],
        }
    }

    fn sink() -> TokenSink<Vec<u8>> {
        TokenSink {
            output: Vec::new(),
            source_id: "source-test".into(),
            rows: 0,
            blocks: 0,
            last_report: Instant::now(),
        }
    }

    fn header() -> BlockHeader {
        BlockHeader {
            epoch: 900,
            block_ordinal: 7,
            slot: 388_800_010,
        }
    }

    #[test]
    fn dump_keeps_exact_amount_source_coordinates_failed_status_and_unknown_keys() {
        let mut sink = sink();
        sink.visit_block(BlockView {
            counts: None,
            header: header(),
            transactions: &[transaction()],
        })
        .unwrap();
        let row: serde_json::Value = serde_json::from_slice(&sink.output).unwrap();
        assert_eq!(row["amount"], u64::MAX.to_string());
        assert_eq!(row["mint"], serde_json::Value::Null);
        assert_eq!(row["execution_status"], "failed");
        assert_eq!(row["balance_index"], 9);
        assert_eq!(row["account_index"], 4);
        assert_eq!(row["source_id"], "source-test");
        assert_eq!(sink.rows, 1);
    }

    #[test]
    fn incomplete_block_is_rejected_before_its_first_output_row() {
        let first = transaction();
        let mut second = transaction();
        second.header.tx_index = 1;
        second.token_balance_coverage =
            TokenBalanceCoverage::Unknown(CoverageReason::MetadataAbsent);
        let mut sink = sink();
        assert!(
            sink.visit_block(BlockView {
                counts: None,
                header: header(),
                transactions: &[first, second]
            })
            .is_err()
        );
        assert!(sink.output.is_empty());
        assert_eq!(sink.blocks, 0);
    }

    #[test]
    fn mint_parser_requires_exact_public_key_length() {
        assert!(parse_mint("bad!").is_err());
        assert!(parse_mint("1").is_err());
        assert_eq!(
            parse_mint("11111111111111111111111111111111").unwrap(),
            [0; 32]
        );
    }
}

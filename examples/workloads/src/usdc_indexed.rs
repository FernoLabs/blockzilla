//! Optional source-scoped USDC records and a first-observation dictionary.
//!
//! Dictionary entries are written before rows that reference them. This is
//! logical write order, not a power-loss durability guarantee. A caller must
//! publish completion only after both files finish and their hashes are saved.
//! A record-aligned but unfinished file cannot be identified from this format
//! alone. The source scope must bind the exact registry and verification mode;
//! an epoch number or a descriptive archive label is not sufficient.

use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Write},
    num::NonZeroU32,
};

use blockzilla_model::{
    AccountReference, AccountResolver, BlockView, ExecutionStatus, IndexedTokenBalance,
    IndexedTokenSink, TokenBalanceCoverage, TokenBalanceSide, TransactionView,
};

use crate::{
    Error, FinishedOutput, MAINNET_USDC_MINT, OutputReport, Result, UsdcReport,
    output::{CanonicalOutput, CoverageTracker, TransactionOrder, increment, target_header},
    usdc::{
        USDC_COVERAGE_EXECUTION_UNKNOWN, USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE,
        USDC_COVERAGE_TOKEN_MINT_UNAVAILABLE, USDC_RECORD_BYTES,
    },
};

pub const INDEXED_USDC_HEADER_BYTES: usize = 76;
pub const INDEXED_USDC_RECORD_BYTES: usize = 70;
pub const INDEXED_USDC_DICTIONARY_RECORD_BYTES: usize = 60;
pub const INDEXED_USDC_INLINE_ID_START: u64 = 1 << 32;
const DATA_MAGIC: [u8; 8] = *b"BZUSCI01";
const DICTIONARY_MAGIC: [u8; 8] = *b"BZUSDI01";
const DATA_SCHEMA: &str = "blockzilla-example-usdc-indexed-recorded-balance-exclude-failed/v1";
const DICTIONARY_SCHEMA: &str = "blockzilla-example-usdc-source-account-dictionary/v1";
const EXPANDED_SCHEMA: &str = "blockzilla-example-usdc-recorded-balance-exclude-failed/v2";

/// A compact writer with a source-local numeric dictionary. Registry references
/// retain their original nonzero IDs. Inline keys use a separate u64 namespace.
pub struct IndexedUsdcBalanceSink<W, D> {
    mint: [u8; 32],
    output: CanonicalOutput<W>,
    dictionary: CanonicalOutput<D>,
    registry_ids: HashSet<NonZeroU32>,
    target_mint_ids: HashSet<NonZeroU32>,
    inline_ids: HashMap<[u8; 32], u64>,
    next_inline_id: u64,
    order: TransactionOrder,
    coverage: CoverageTracker,
    blocks_seen: u64,
    transactions_seen: u64,
    skipped_failed_transactions: u64,
    matching_transactions: u64,
    pre_rows: u64,
    post_rows: u64,
    token_balances_unavailable_transactions: u64,
    token_mint_unavailable_transactions: u64,
}

impl<W: Write, D: Write> IndexedUsdcBalanceSink<W, D> {
    pub fn new(
        writer: W,
        dictionary_writer: D,
        mint: [u8; 32],
        source_scope: [u8; 32],
    ) -> Result<Self> {
        Ok(Self {
            mint,
            output: CanonicalOutput::new(
                writer,
                &header(DATA_MAGIC, INDEXED_USDC_RECORD_BYTES, mint, source_scope),
            )?,
            dictionary: CanonicalOutput::new(
                dictionary_writer,
                &header(
                    DICTIONARY_MAGIC,
                    INDEXED_USDC_DICTIONARY_RECORD_BYTES,
                    mint,
                    source_scope,
                ),
            )?,
            registry_ids: HashSet::new(),
            target_mint_ids: HashSet::new(),
            inline_ids: HashMap::new(),
            next_inline_id: INDEXED_USDC_INLINE_ID_START,
            order: TransactionOrder::default(),
            coverage: CoverageTracker::default(),
            blocks_seen: 0,
            transactions_seen: 0,
            skipped_failed_transactions: 0,
            matching_transactions: 0,
            pre_rows: 0,
            post_rows: 0,
            token_balances_unavailable_transactions: 0,
            token_mint_unavailable_transactions: 0,
        })
    }

    pub fn mainnet(writer: W, dictionary_writer: D, source_scope: [u8; 32]) -> Result<Self> {
        Self::new(writer, dictionary_writer, MAINNET_USDC_MINT, source_scope)
    }

    pub fn process_block(
        &mut self,
        block: BlockView<'_>,
        balances: &[IndexedTokenBalance],
        resolver: &mut dyn AccountResolver,
    ) -> Result<()> {
        let mut remaining = balances;
        for transaction in block.transaction_views() {
            if !transaction.token_balances.is_empty() {
                return invalid("indexed USDC requires empty canonical balance lists");
            }
            if remaining
                .first()
                .is_some_and(|b| b.tx_index < transaction.header.tx_index)
            {
                return invalid("indexed USDC row refers to an absent or out-of-order transaction");
            }
            let count = remaining
                .iter()
                .take_while(|b| b.tx_index == transaction.header.tx_index)
                .count();
            let (rows, rest) = remaining.split_at(count);
            self.process_transaction(transaction, rows, resolver)?;
            remaining = rest;
        }
        if !remaining.is_empty() {
            return invalid("indexed USDC row refers to an absent transaction");
        }
        increment(&mut self.blocks_seen, "USDC block")
    }

    fn process_transaction(
        &mut self,
        transaction: TransactionView<'_>,
        balances: &[IndexedTokenBalance],
        resolver: &mut dyn AccountResolver,
    ) -> Result<()> {
        self.order.observe("USDC indexed", transaction)?;
        increment(&mut self.transactions_seen, "USDC transaction")?;
        if transaction.header.status == ExecutionStatus::Failed {
            return increment(
                &mut self.skipped_failed_transactions,
                "USDC skipped failure",
            );
        }
        let mut reason_bits = 0;
        if matches!(transaction.header.status, ExecutionStatus::Unknown(_)) {
            reason_bits |= USDC_COVERAGE_EXECUTION_UNKNOWN;
        }
        if transaction.token_balance_coverage != TokenBalanceCoverage::Complete {
            reason_bits |= USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE;
            increment(
                &mut self.token_balances_unavailable_transactions,
                "USDC unavailable token-balance transaction",
            )?;
        }
        let mut last = None;
        let mut matched = false;
        let mut missing_mint = false;
        for balance in balances {
            let position = (side_tag(balance.side), balance.balance_index);
            if last.is_some_and(|last| position <= last) {
                return Err(Error::TokenBalanceOrder {
                    epoch: transaction.block.epoch,
                    slot: transaction.block.slot,
                    tx_index: transaction.header.tx_index,
                    side: if balance.side == TokenBalanceSide::Pre {
                        "pre"
                    } else {
                        "post"
                    },
                });
            }
            last = Some(position);
            let Some(mint) = balance.mint else {
                missing_mint = true;
                continue;
            };
            let mint_id = self.intern(mint, transaction, resolver)?;
            let matches = match mint {
                AccountReference::Registry(id) => self.target_mint_ids.contains(&id),
                AccountReference::Inline(key) => key == self.mint,
            };
            if !matches {
                continue;
            }
            let token_account = self.intern(balance.token_account, transaction, resolver)?;
            let owner = self.intern_optional(balance.owner, transaction, resolver)?;
            let program = self.intern_optional(balance.token_program, transaction, resolver)?;
            let mut row = [0; INDEXED_USDC_RECORD_BYTES];
            encode_position(&mut row[..20], transaction);
            row[20] = side_tag(balance.side);
            row[21..25].copy_from_slice(&balance.balance_index.to_be_bytes());
            row[25..29].copy_from_slice(&balance.account_index.to_be_bytes());
            row[29..37].copy_from_slice(&token_account.to_be_bytes());
            row[37..45].copy_from_slice(&mint_id.to_be_bytes());
            row[45..53].copy_from_slice(&owner.to_be_bytes());
            row[53..61].copy_from_slice(&program.to_be_bytes());
            row[61..69].copy_from_slice(&balance.amount.to_be_bytes());
            row[69] = balance.decimals;
            self.output.write_row(&row)?;
            matched = true;
            match balance.side {
                TokenBalanceSide::Pre => increment(&mut self.pre_rows, "USDC pre row")?,
                TokenBalanceSide::Post => increment(&mut self.post_rows, "USDC post row")?,
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

    fn intern_optional(
        &mut self,
        reference: Option<AccountReference>,
        transaction: TransactionView<'_>,
        resolver: &mut dyn AccountResolver,
    ) -> Result<u64> {
        reference.map_or(Ok(0), |reference| {
            self.intern(reference, transaction, resolver)
        })
    }

    fn intern(
        &mut self,
        reference: AccountReference,
        transaction: TransactionView<'_>,
        resolver: &mut dyn AccountResolver,
    ) -> Result<u64> {
        let (id, key) = match reference {
            AccountReference::Registry(id) => {
                if self.registry_ids.contains(&id) {
                    return Ok(u64::from(id.get()));
                }
                let key = resolver
                    .resolve(reference)
                    .map_err(|error| Error::InvalidInput(error.to_string()))?;
                (u64::from(id.get()), key)
            }
            AccountReference::Inline(key) => {
                if let Some(&id) = self.inline_ids.get(&key) {
                    return Ok(id);
                }
                (self.next_inline_id, key)
            }
        };
        let mut row = [0; INDEXED_USDC_DICTIONARY_RECORD_BYTES];
        row[..8].copy_from_slice(&id.to_be_bytes());
        row[8..40].copy_from_slice(&key);
        encode_position(&mut row[40..60], transaction);
        self.dictionary.write_row(&row)?;
        match reference {
            AccountReference::Registry(id) => {
                self.registry_ids.insert(id);
                if key == self.mint {
                    self.target_mint_ids.insert(id);
                }
            }
            AccountReference::Inline(key) => {
                self.inline_ids.insert(key, id);
                self.next_inline_id = id
                    .checked_add(1)
                    .ok_or(Error::CounterOverflow("USDC inline ID"))?;
            }
        }
        Ok(id)
    }

    pub fn finish(
        self,
    ) -> Result<(
        FinishedOutput<W, UsdcReport>,
        FinishedOutput<D, OutputReport>,
    )> {
        // Finish the dictionary first. The caller owns file sync and publication.
        let dictionary = self.dictionary.finish(DICTIONARY_SCHEMA)?;
        let output = self.output.finish(DATA_SCHEMA)?;
        let coverage = self.coverage.finish();
        debug_assert_eq!(output.report.row_count, self.pre_rows + self.post_rows);
        Ok((
            FinishedOutput {
                writer: output.writer,
                report: UsdcReport {
                    blocks_seen: self.blocks_seen,
                    transactions_seen: self.transactions_seen,
                    skipped_failed_transactions: self.skipped_failed_transactions,
                    matching_transactions: self.matching_transactions,
                    pre_rows: self.pre_rows,
                    post_rows: self.post_rows,
                    token_balances_unavailable_transactions: self
                        .token_balances_unavailable_transactions,
                    token_mint_unavailable_transactions: self.token_mint_unavailable_transactions,
                    output_complete: coverage.output_complete(),
                    coverage,
                    output: output.report,
                },
            },
            dictionary,
        ))
    }
}

impl<W: Write, D: Write> IndexedTokenSink for IndexedUsdcBalanceSink<W, D> {
    fn visit_indexed_block(
        &mut self,
        block: BlockView<'_>,
        balances: &[IndexedTokenBalance],
        resolver: &mut dyn AccountResolver,
    ) -> blockzilla_model::Result<()> {
        self.process_block(block, balances, resolver)
            .map_err(blockzilla_model::Error::sink)
    }
}

type Position = (u64, u64, u32);

/// Expand compact rows through their matching dictionary to exact `BZUSDC02`
/// bytes. The dictionary occupies memory once; data rows stream through fixed
/// stack buffers. Completion and whole-file hashes must be checked by the
/// caller before using files from an interrupted run.
pub fn expand_indexed_usdc<R: Read, D: Read, W: Write>(
    mut reader: R,
    mut dictionary: D,
    writer: W,
) -> Result<FinishedOutput<W, OutputReport>> {
    let (mint, scope) = read_header(&mut reader, DATA_MAGIC, INDEXED_USDC_RECORD_BYTES)?;
    let dictionary_header = read_header(
        &mut dictionary,
        DICTIONARY_MAGIC,
        INDEXED_USDC_DICTIONARY_RECORD_BYTES,
    )?;
    if dictionary_header != (mint, scope) {
        return invalid("indexed USDC dictionary mint or source scope does not match");
    }
    let mut entries = HashMap::<u64, ([u8; 32], Position)>::new();
    let mut inline_keys = HashSet::new();
    let mut next_inline = INDEXED_USDC_INLINE_ID_START;
    let mut last_discovery = None;
    let mut entry = [0; INDEXED_USDC_DICTIONARY_RECORD_BYTES];
    while read_record(&mut dictionary, &mut entry)? {
        let id = u64_at(&entry[..8]);
        if id == 0 {
            return invalid("indexed USDC dictionary ID zero is reserved for a missing reference");
        }
        let key = entry[8..40].try_into().expect("fixed dictionary key");
        let position = decode_position(&entry[40..60]);
        if last_discovery.is_some_and(|last| position < last) {
            return invalid("indexed USDC dictionary observations are not ordered");
        }
        last_discovery = Some(position);
        if id >= INDEXED_USDC_INLINE_ID_START {
            if id != next_inline || !inline_keys.insert(key) {
                return invalid(
                    "indexed USDC inline dictionary IDs must be unique and assigned in first-seen order",
                );
            }
            next_inline = next_inline
                .checked_add(1)
                .ok_or(Error::CounterOverflow("USDC inline ID"))?;
        }
        if entries.insert(id, (key, position)).is_some() {
            return invalid("indexed USDC dictionary contains a duplicate or conflicting ID");
        }
    }
    let mut output = CanonicalOutput::new(
        writer,
        &target_header(*b"BZUSDC02", USDC_RECORD_BYTES as u32, mint),
    )?;
    let mut compact = [0; INDEXED_USDC_RECORD_BYTES];
    let mut last_row = None;
    while read_record(&mut reader, &mut compact)? {
        let position = decode_position(&compact[..20]);
        let side = compact[20];
        if side > 1 {
            return invalid("indexed USDC row has an invalid balance side tag");
        }
        let order = (position, side, u32_at(&compact[21..25]));
        if last_row.is_some_and(|last| order <= last) {
            return invalid("indexed USDC rows are duplicate or not in source order");
        }
        last_row = Some(order);
        // Validate the real token account even though BZUSDC02 cannot store it.
        lookup(&entries, u64_at(&compact[29..37]), position)?;
        let row_mint = lookup(&entries, u64_at(&compact[37..45]), position)?;
        if row_mint != mint {
            return invalid("indexed USDC row mint does not match its header");
        }
        let mut row = [0; USDC_RECORD_BYTES];
        row[..29].copy_from_slice(&compact[..29]);
        row[29..61].copy_from_slice(&row_mint);
        for (input, output_range) in [(45..53, 61..94), (53..61, 94..127)] {
            let id = u64_at(&compact[input]);
            if id != 0 {
                let key = lookup(&entries, id, position)?;
                let field = &mut row[output_range];
                field[0] = 1;
                field[1..].copy_from_slice(&key);
            }
        }
        row[127..135].copy_from_slice(&compact[61..69]);
        row[135] = compact[69];
        output.write_row(&row)?;
    }
    output.finish(EXPANDED_SCHEMA)
}

fn lookup(
    entries: &HashMap<u64, ([u8; 32], Position)>,
    id: u64,
    position: Position,
) -> Result<[u8; 32]> {
    let Some(&(key, first_observed)) = entries.get(&id) else {
        return invalid("indexed USDC row has a missing dictionary reference");
    };
    if first_observed > position {
        return invalid("indexed USDC row precedes its dictionary observation");
    }
    Ok(key)
}

fn header(
    magic: [u8; 8],
    record_bytes: usize,
    mint: [u8; 32],
    scope: [u8; 32],
) -> [u8; INDEXED_USDC_HEADER_BYTES] {
    let mut header = [0; INDEXED_USDC_HEADER_BYTES];
    header[..44].copy_from_slice(&target_header(magic, record_bytes as u32, mint));
    header[44..].copy_from_slice(&scope);
    header
}

fn read_header(
    reader: &mut impl Read,
    magic: [u8; 8],
    record_bytes: usize,
) -> Result<([u8; 32], [u8; 32])> {
    let mut header = [0; INDEXED_USDC_HEADER_BYTES];
    reader.read_exact(&mut header)?;
    if header[..8] != magic || u32_at(&header[8..12]) != record_bytes as u32 {
        return invalid("indexed USDC header has an unsupported magic, version, or record size");
    }
    Ok((
        header[12..44].try_into().expect("fixed mint"),
        header[44..76].try_into().expect("fixed scope"),
    ))
}

fn read_record(reader: &mut impl Read, record: &mut [u8]) -> Result<bool> {
    loop {
        match reader.read(&mut record[..1]) {
            Ok(0) => return Ok(false),
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    match reader.read_exact(&mut record[1..]) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            invalid("truncated indexed USDC record")
        }
        Err(error) => Err(error.into()),
    }
}

fn encode_position(row: &mut [u8], transaction: TransactionView<'_>) {
    row[..8].copy_from_slice(&transaction.block.epoch.to_be_bytes());
    row[8..16].copy_from_slice(&transaction.block.slot.to_be_bytes());
    row[16..20].copy_from_slice(&transaction.header.tx_index.to_be_bytes());
}

fn decode_position(row: &[u8]) -> Position {
    (u64_at(&row[..8]), u64_at(&row[8..16]), u32_at(&row[16..20]))
}

fn u64_at(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("fixed u64 field"))
}

fn u32_at(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("fixed u32 field"))
}

fn side_tag(side: TokenBalanceSide) -> u8 {
    match side {
        TokenBalanceSide::Pre => 0,
        TokenBalanceSide::Post => 1,
    }
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(Error::InvalidInput(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use blockzilla_model::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, CoverageReason, CpiCoverage,
        InstructionCoverage, RecordedTokenBalance, TransactionHeader,
    };

    use super::*;
    use crate::{USDC_HEADER_BYTES, UsdcBalanceSink};

    fn registry(id: u32) -> AccountReference {
        AccountReference::Registry(NonZeroU32::new(id).unwrap())
    }

    fn key(id: u32) -> [u8; 32] {
        // Preserve Some([0; 32]) as distinct from an absent owner.
        if id == 3 { [0; 32] } else { [id as u8; 32] }
    }

    #[derive(Default)]
    struct Resolver {
        calls: HashMap<u32, usize>,
    }
    impl AccountResolver for Resolver {
        fn resolve(&mut self, reference: AccountReference) -> blockzilla_model::Result<[u8; 32]> {
            let AccountReference::Registry(id) = reference else {
                panic!("inline keys do not need a registry resolution");
            };
            assert!(
                id.get() < 100,
                "failed or missing-mint rows must not discover accounts"
            );
            *self.calls.entry(id.get()).or_default() += 1;
            Ok(key(id.get()))
        }
    }

    fn row(tx_index: u32, side: TokenBalanceSide, balance_index: u32) -> IndexedTokenBalance {
        IndexedTokenBalance {
            tx_index,
            side,
            balance_index,
            account_index: 7,
            token_account: registry(2),
            mint: Some(registry(1)),
            owner: Some(registry(3)),
            token_program: Some(AccountReference::Inline([4; 32])),
            amount: 123 + u64::from(tx_index) + u64::from(balance_index),
            decimals: 6,
        }
    }

    fn block(statuses: &[ExecutionStatus]) -> CanonicalBlock {
        CanonicalBlock {
            counts: None,
            header: BlockHeader {
                epoch: 9,
                block_ordinal: 0,
                slot: 99,
            },
            transactions: statuses
                .iter()
                .enumerate()
                .map(|(index, &status)| CanonicalTransaction {
                    header: TransactionHeader {
                        tx_index: index as u32,
                        status,
                        failed_outer_instruction_index: None,
                        instruction_coverage: InstructionCoverage::Unknown(
                            CoverageReason::ProjectionNotRequested,
                        ),
                        cpi_coverage: CpiCoverage::Unknown(CoverageReason::ProjectionNotRequested),
                    },
                    primary_signature: None,
                    required_signers: vec![],
                    instructions: vec![],
                    token_balance_coverage: TokenBalanceCoverage::Complete,
                    token_balances: vec![],
                })
                .collect(),
        }
    }

    fn fixture() -> (Vec<u8>, Vec<u8>) {
        let block = block(&[ExecutionStatus::Succeeded]);
        let mut sink =
            IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), key(1), [8; 32]).unwrap();
        sink.process_block(
            block.as_view(),
            &[row(0, TokenBalanceSide::Pre, 0)],
            &mut Resolver::default(),
        )
        .unwrap();
        let (data, dictionary) = sink.finish().unwrap();
        (data.writer, dictionary.writer)
    }

    #[test]
    fn expansion_matches_existing_bytes_and_coverage_and_resolves_each_registry_id_once() {
        let mut block = block(&[
            ExecutionStatus::Succeeded,
            ExecutionStatus::Failed,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
            ExecutionStatus::Succeeded,
            ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
        ]);
        block.transactions[4].token_balance_coverage = TokenBalanceCoverage::NotRequested;
        let mut failed = row(1, TokenBalanceSide::Pre, 0);
        failed.mint = Some(registry(999));
        failed.token_account = registry(998);
        let mut missing = row(3, TokenBalanceSide::Pre, 0);
        missing.mint = None;
        missing.token_account = registry(997);
        let mut absent_owner = row(0, TokenBalanceSide::Post, 0);
        absent_owner.owner = None;
        let rows = [
            row(0, TokenBalanceSide::Pre, 0),
            absent_owner,
            failed,
            row(2, TokenBalanceSide::Pre, 0),
            missing,
        ];
        let mut sink =
            IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), key(1), [8; 32]).unwrap();
        let mut resolver = Resolver::default();
        sink.process_block(block.as_view(), &rows, &mut resolver)
            .unwrap();
        let (indexed, dictionary) = sink.finish().unwrap();
        assert_eq!(resolver.calls, HashMap::from([(1, 1), (2, 1), (3, 1)]));
        assert_eq!(dictionary.report.row_count, 4);
        assert_eq!(indexed.report.skipped_failed_transactions, 1);
        assert_eq!(indexed.report.matching_transactions, 2);
        assert_eq!(indexed.report.token_mint_unavailable_transactions, 1);
        assert_eq!(indexed.report.token_balances_unavailable_transactions, 1);
        assert_eq!(indexed.report.coverage.indeterminate_transactions, 3);
        let first = &indexed.writer[INDEXED_USDC_HEADER_BYTES..];
        assert_eq!(u64_at(&first[29..37]), 2); // actual token account
        assert_eq!(u64_at(&first[45..53]), 3); // owner, a separate key
        assert_eq!(u32_at(&first[25..29]), 7); // transaction-local account index
        for entry in dictionary.writer[INDEXED_USDC_HEADER_BYTES..]
            .chunks_exact(INDEXED_USDC_DICTIONARY_RECORD_BYTES)
        {
            assert_eq!(decode_position(&entry[40..60]), (9, 99, 0));
        }
        let raw = |reference| match reference {
            AccountReference::Registry(id) => key(id.get()),
            AccountReference::Inline(key) => key,
        };
        for balance in rows {
            block.transactions[balance.tx_index as usize]
                .token_balances
                .push(RecordedTokenBalance {
                    side: balance.side,
                    balance_index: balance.balance_index,
                    account_index: balance.account_index,
                    mint: balance.mint.map(raw),
                    owner: balance.owner.map(raw),
                    token_program: balance.token_program.map(raw),
                    amount: balance.amount,
                    decimals: balance.decimals,
                });
        }
        let mut canonical = UsdcBalanceSink::new(Vec::new(), key(1)).unwrap();
        canonical.process_block(block.as_view()).unwrap();
        let canonical = canonical.finish().unwrap();
        let expanded = expand_indexed_usdc(
            indexed.writer.as_slice(),
            dictionary.writer.as_slice(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(expanded.writer, canonical.writer);
        assert_eq!(expanded.report, canonical.report.output);
        let mut expected = canonical.report;
        expected.output = indexed.report.output;
        assert_eq!(indexed.report, expected);
        assert_eq!(expanded.writer[USDC_HEADER_BYTES + 61], 1);
        assert_eq!(
            expanded.writer[USDC_HEADER_BYTES + USDC_RECORD_BYTES + 61],
            0
        );
    }

    #[test]
    fn registry_and_inline_namespaces_remain_distinct_and_reuse_across_blocks() {
        let mut block = block(&[ExecutionStatus::Succeeded]);
        let mut balance = row(0, TokenBalanceSide::Pre, 0);
        balance.owner = Some(registry(u32::MAX));
        struct LargeIdResolver(usize);
        impl AccountResolver for LargeIdResolver {
            fn resolve(
                &mut self,
                reference: AccountReference,
            ) -> blockzilla_model::Result<[u8; 32]> {
                self.0 += 1;
                let AccountReference::Registry(id) = reference else {
                    unreachable!()
                };
                Ok(if id.get() == u32::MAX {
                    [4; 32]
                } else {
                    key(id.get())
                })
            }
        }
        let mut resolver = LargeIdResolver(0);
        let mut sink =
            IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), key(1), [8; 32]).unwrap();
        sink.process_block(block.as_view(), &[balance], &mut resolver)
            .unwrap();
        block.header.slot += 1;
        block.header.block_ordinal += 1;
        sink.process_block(block.as_view(), &[balance], &mut resolver)
            .unwrap();
        let (data, dictionary) = sink.finish().unwrap();
        assert_eq!(resolver.0, 3);
        assert_eq!(dictionary.report.row_count, 4);
        let row = &data.writer[INDEXED_USDC_HEADER_BYTES..];
        assert_eq!(u64_at(&row[45..53]), u64::from(u32::MAX));
        assert_eq!(u64_at(&row[53..61]), INDEXED_USDC_INLINE_ID_START);
        expand_indexed_usdc(
            data.writer.as_slice(),
            dictionary.writer.as_slice(),
            Vec::new(),
        )
        .unwrap();
    }

    #[test]
    fn expansion_rejects_bad_headers_references_order_and_truncation() {
        let (data, dictionary) = fixture();
        let rejected = |data: &[u8], dictionary: &[u8]| {
            assert!(expand_indexed_usdc(data, dictionary, Vec::new()).is_err());
        };
        for offset in [0, 8, 12, 44] {
            let mut bad = dictionary.clone();
            bad[offset] ^= 1;
            rejected(&data, &bad);
        }
        for offset in [29, 37, 45, 53] {
            let mut bad = data.clone();
            bad[INDEXED_USDC_HEADER_BYTES + offset..INDEXED_USDC_HEADER_BYTES + offset + 8]
                .copy_from_slice(&999_u64.to_be_bytes());
            rejected(&bad, &dictionary);
        }
        for offset in [29, 37] {
            let mut bad = data.clone();
            bad[INDEXED_USDC_HEADER_BYTES + offset..INDEXED_USDC_HEADER_BYTES + offset + 8].fill(0);
            rejected(&bad, &dictionary);
        }
        let mut bad = data.clone();
        bad[INDEXED_USDC_HEADER_BYTES + 20] = 2;
        rejected(&bad, &dictionary);
        let mut bad = data.clone();
        bad.extend_from_slice(&data[INDEXED_USDC_HEADER_BYTES..]);
        rejected(&bad, &dictionary);
        let mut bad = dictionary.clone();
        bad.extend_from_slice(
            &dictionary[INDEXED_USDC_HEADER_BYTES
                ..INDEXED_USDC_HEADER_BYTES + INDEXED_USDC_DICTIONARY_RECORD_BYTES],
        );
        rejected(&data, &bad);
        let mut conflicting = bad;
        let key_offset = conflicting.len() - INDEXED_USDC_DICTIONARY_RECORD_BYTES + 8;
        conflicting[key_offset] ^= 1;
        rejected(&data, &conflicting);
        let mut bad = dictionary.clone();
        bad[INDEXED_USDC_HEADER_BYTES..INDEXED_USDC_HEADER_BYTES + 8].fill(0);
        rejected(&data, &bad);
        let mut bad = dictionary.clone();
        // Move the final dictionary entry after the first data row.
        let tx_offset = bad.len() - 4;
        bad[tx_offset..].copy_from_slice(&1_u32.to_be_bytes());
        rejected(&data, &bad);
        for length in [0, 7, INDEXED_USDC_HEADER_BYTES - 1, data.len() - 1] {
            rejected(&data[..length], &dictionary);
        }
        for length in [0, 7, INDEXED_USDC_HEADER_BYTES - 1, dictionary.len() - 1] {
            rejected(&data, &dictionary[..length]);
        }
    }

    #[test]
    fn sink_rejects_unbound_transactions_and_balance_order_but_accepts_empty_output() {
        let block = block(&[ExecutionStatus::Succeeded]);
        for rows in [
            vec![row(1, TokenBalanceSide::Pre, 0)],
            vec![
                row(0, TokenBalanceSide::Pre, 0),
                row(0, TokenBalanceSide::Pre, 0),
            ],
            vec![
                row(0, TokenBalanceSide::Post, 0),
                row(0, TokenBalanceSide::Pre, 1),
            ],
        ] {
            let mut sink =
                IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), key(1), [8; 32]).unwrap();
            assert!(
                sink.process_block(block.as_view(), &rows, &mut Resolver::default())
                    .is_err()
            );
        }
        let sink = IndexedUsdcBalanceSink::new(Vec::new(), Vec::new(), key(1), [8; 32]).unwrap();
        let (data, dictionary) = sink.finish().unwrap();
        let expanded = expand_indexed_usdc(
            data.writer.as_slice(),
            dictionary.writer.as_slice(),
            Vec::new(),
        )
        .unwrap();
        let canonical = UsdcBalanceSink::new(Vec::new(), key(1))
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(expanded.writer, canonical.writer);
        assert_eq!(expanded.report.row_count, 0);
    }
}

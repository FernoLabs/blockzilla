use std::path::Path;

use blockzilla_index_archive_format::{
    ArchiveId,
    catalog::blocks::{self as catalog_blocks, BlockRow, PageSpan},
    dictionary::pubkeys,
    ledger::transactions::{
        self, CpiState, EffectFileIndex, EffectKind, EffectState, RowRestart, Transaction,
        TransactionBlock,
    },
    runtime::inner_instructions::{self, TransactionInner},
};

use crate::{container::HeaderedWriter, transaction_view::ResolvedAccounts};

pub(crate) struct FixtureBlock {
    pub(crate) slot: u64,
    pub(crate) parent_slot: u64,
    pub(crate) transactions: Vec<Transaction>,
    pub(crate) inner: Vec<Option<TransactionInner>>,
}

pub(crate) fn write_merged_fixture(
    root: &Path,
    archive_id: ArchiveId,
    transaction_archive_id: ArchiveId,
    inner_archive_id: ArchiveId,
    pubkey_count: u32,
    blocks: Vec<FixtureBlock>,
) {
    let mut transaction_writer = HeaderedWriter::create(root, transactions::PATH, 1024).unwrap();
    let mut inner_writer = HeaderedWriter::create(root, inner_instructions::PATH, 1024).unwrap();
    let mut rows = Vec::with_capacity(blocks.len());
    let mut first_transaction = 0_u64;
    let mut first_signature = 0_u64;
    let mut dense_inner_records = 0_u64;

    for block in blocks {
        assert_eq!(block.transactions.len(), block.inner.len());
        let transaction_count = block.transactions.len() as u32;
        let chunk_count =
            transaction_count.div_ceil(transactions::EFFECT_CHUNK_TRANSACTIONS) as usize;
        let states: Vec<_> = block
            .inner
            .iter()
            .map(|inner| {
                EffectState::new(if inner.is_some() {
                    CpiState::SourcePresent
                } else {
                    CpiState::SourceEmpty
                })
            })
            .collect();
        let mut effect_files: [EffectFileIndex; transactions::EFFECT_KIND_COUNT] =
            std::array::from_fn(|_| EffectFileIndex {
                first_chunk_offset: 0,
                chunks: vec![transactions::ChunkFrame::EMPTY; chunk_count],
            });
        let mut first_inner_offset = 0_u64;
        for (chunk_index, values) in block
            .inner
            .chunks(transactions::EFFECT_CHUNK_TRANSACTIONS as usize)
            .enumerate()
        {
            let start = chunk_index * transactions::EFFECT_CHUNK_TRANSACTIONS as usize;
            let mut bytes = Vec::new();
            for (relative, value) in values.iter().enumerate() {
                let Some(value) = value else {
                    continue;
                };
                let transaction = &block.transactions[start + relative];
                inner_instructions::append_record(
                    &mut bytes,
                    value,
                    transaction.message.instructions().len(),
                    ResolvedAccounts::new(transaction).resolved_len(),
                )
                .unwrap();
                dense_inner_records += 1;
            }
            if bytes.is_empty() {
                continue;
            }
            let offset = inner_writer.append(&bytes, bytes.len() as u64).unwrap();
            if first_inner_offset == 0 {
                first_inner_offset = offset;
            }
            effect_files[EffectKind::InnerInstructions.index()].chunks[chunk_index] =
                transactions::ChunkFrame::raw(bytes.len() as u32).unwrap();
        }
        effect_files[EffectKind::InnerInstructions.index()].first_chunk_offset = first_inner_offset;

        let mut transaction_rows = Vec::new();
        let mut row_restarts = Vec::new();
        let mut signature_delta = 0_u32;
        for (index, transaction) in block.transactions.iter().enumerate() {
            if index.is_multiple_of(transactions::ROW_RESTART_INTERVAL as usize) {
                row_restarts.push(RowRestart {
                    row_byte_offset: transaction_rows.len() as u32,
                    signature_delta,
                });
            }
            transactions::append_transaction(&mut transaction_rows, transaction).unwrap();
            signature_delta += u32::from(transaction.header.num_required_signatures);
        }
        let transaction_block = TransactionBlock {
            effect_states: states,
            row_restarts,
            effect_files,
            transaction_rows,
        };
        let bytes = transactions::encode_block(&transaction_block).unwrap();
        let offset = transaction_writer
            .append(&bytes, bytes.len() as u64)
            .unwrap();
        let span = PageSpan {
            offset,
            stored_len: bytes.len() as u32,
            decoded_len: bytes.len() as u32,
        };
        rows.push(BlockRow {
            slot: block.slot,
            parent_slot: block.parent_slot,
            first_transaction,
            transaction_count,
            first_signature,
            transactions: span,
            ..BlockRow::default()
        });
        first_transaction += u64::from(transaction_count);
        first_signature += u64::from(signature_delta);
    }

    transaction_writer
        .finish(transaction_archive_id, first_transaction)
        .unwrap();
    inner_writer
        .finish(inner_archive_id, dense_inner_records)
        .unwrap();
    let catalog = catalog_blocks::encode_table(&rows).unwrap();
    crate::container::write_payload(
        root,
        catalog_blocks::PATH,
        archive_id,
        rows.len() as u64,
        &catalog,
    )
    .unwrap();
    crate::container::write_payload(
        root,
        pubkeys::PATH,
        archive_id,
        u64::from(pubkey_count),
        &vec![0_u8; pubkey_count as usize * pubkeys::RECORD_LEN],
    )
    .unwrap();
}

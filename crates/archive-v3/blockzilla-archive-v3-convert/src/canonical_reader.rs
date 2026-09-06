//! Compatibility exports for the canonical reader, now owned by the reader crate.

pub use blockzilla_archive_v3_reader::canonical::*;

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, os::unix::fs::FileExt};

    use blockzilla_archive_v3::{
        catalog::blocks::{self as catalog_blocks, BlockRow, PageSpan},
        ledger::transactions::{
            ChunkFrame, CpiState, EffectFileIndex, EffectState, HashOwner, HashRef, Instruction,
            Message, MessageHeader, PubkeyId, RowRestart,
        },
        runtime::{
            inner_instructions::{InnerGroup, InnerInstruction},
            outcomes::TransactionOutcome,
        },
    };
    use tempfile::tempdir;

    use crate::{
        container::write_payload,
        test_fixture::{FixtureBlock, write_merged_fixture},
        transaction_view::TransactionArenaEncoder,
    };

    use super::*;
    use blockzilla_archive_v3::{
        ArchiveId, FILE_HEADER_LEN,
        ledger::transactions::{self as transactions, EffectKind, Transaction, TransactionBlock},
        runtime::{
            balances::{self, Balances},
            inner_instructions::{self, TransactionInner},
            logs, outcomes, rewards, token_balances,
        },
    };
    use std::path::Path;

    fn transaction() -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 1,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::Legacy {
                static_accounts: vec![PubkeyId(1), PubkeyId(2)],
                instructions: vec![Instruction {
                    program_position: 1,
                    account_positions: vec![0],
                    data: vec![7],
                }],
            },
        }
    }

    fn write_transaction_catalog(
        root: &Path,
        archive_id: ArchiveId,
        transactions_to_write: &[Transaction],
        effect_states: Vec<EffectState>,
        effect_files: [EffectFileIndex; transactions::EFFECT_KIND_COUNT],
    ) {
        let mut transaction_rows = Vec::new();
        for transaction in transactions_to_write {
            transactions::append_transaction(&mut transaction_rows, transaction).unwrap();
        }
        let transaction_block = TransactionBlock {
            effect_states,
            row_restarts: vec![RowRestart {
                row_byte_offset: 0,
                signature_delta: 0,
            }],
            effect_files,
            transaction_rows,
        };
        let payload = transactions::encode_block(&transaction_block).unwrap();
        write_payload(
            root,
            transactions::PATH,
            archive_id,
            transactions_to_write.len() as u64,
            &payload,
        )
        .unwrap();
        let catalog = catalog_blocks::encode_table(&[BlockRow {
            slot: 100,
            parent_slot: 99,
            transaction_count: transactions_to_write.len() as u32,
            transactions: PageSpan {
                offset: FILE_HEADER_LEN as u64,
                stored_len: payload.len() as u32,
                decoded_len: payload.len() as u32,
            },
            ..BlockRow::default()
        }])
        .unwrap();
        write_payload(root, catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();
    }

    fn write_effect(
        root: &Path,
        archive_id: ArchiveId,
        path: &'static str,
        record_count: u64,
        payload: &[u8],
    ) -> ChunkFrame {
        write_payload(root, path, archive_id, record_count, payload).unwrap();
        ChunkFrame::raw(payload.len() as u32).unwrap()
    }

    fn overwrite_catalog_row(root: &Path, ordinal: u64, row: BlockRow) {
        let offset = FILE_HEADER_LEN as u64 + ordinal * catalog_blocks::ROW_LEN as u64;
        let file = OpenOptions::new()
            .write(true)
            .open(root.join(catalog_blocks::PATH))
            .unwrap();
        file.write_all_at(&row.encode().unwrap(), offset).unwrap();
    }

    #[test]
    fn full_reader_and_audit_preserve_absent_and_recorded_empty() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([5; 16]);
        let transactions_to_write = vec![transaction(), transaction(), transaction()];

        let inner = TransactionInner {
            groups: vec![InnerGroup {
                parent_index: 0,
                instructions: vec![InnerInstruction {
                    stack_height: Some(2),
                    instruction: Instruction {
                        program_position: 1,
                        account_positions: vec![0],
                        data: vec![8],
                    },
                }],
            }],
        };
        let inner_payload = inner_instructions::encode_record(&inner, 1, 2).unwrap();
        let outcome = TransactionOutcome {
            error: None,
            fee: 0,
            compute_units_consumed: None,
            cost_units: None,
            return_data: None,
        };
        let mut outcome_payload = Vec::new();
        outcomes::append_record(&mut outcome_payload, &outcome).unwrap();
        outcomes::append_record(&mut outcome_payload, &outcome).unwrap();
        let unchanged_balances = Balances {
            pre: vec![10, 20],
            post: vec![10, 20],
        };
        let mut balance_payload = Vec::new();
        balances::append_record(&mut balance_payload, &unchanged_balances).unwrap();
        balances::append_record(&mut balance_payload, &unchanged_balances).unwrap();
        let log_payload = logs::encode_record(&[]).unwrap();
        let frames = [
            write_effect(
                root.path(),
                archive_id,
                inner_instructions::PATH,
                1,
                &inner_payload,
            ),
            write_effect(root.path(), archive_id, outcomes::PATH, 2, &outcome_payload),
            write_effect(root.path(), archive_id, balances::PATH, 2, &balance_payload),
            ChunkFrame::EMPTY,
            write_effect(root.path(), archive_id, logs::PATH, 1, &log_payload),
            ChunkFrame::EMPTY,
        ];
        write_payload(root.path(), token_balances::PATH, archive_id, 0, &[]).unwrap();
        write_payload(root.path(), rewards::PATH, archive_id, 0, &[]).unwrap();
        let effect_files = std::array::from_fn(|index| EffectFileIndex {
            first_chunk_offset: if frames[index].is_empty() {
                0
            } else {
                FILE_HEADER_LEN as u64
            },
            chunks: vec![frames[index]],
        });

        let mut not_recorded = EffectState::new(CpiState::NotRecorded);
        not_recorded.set_present(EffectKind::Outcome, true);
        not_recorded.set_present(EffectKind::Balances, true);
        let mut recorded_empty = EffectState::new(CpiState::SourceEmpty);
        for kind in [EffectKind::Outcome, EffectKind::Balances, EffectKind::Logs] {
            recorded_empty.set_present(kind, true);
        }
        let cpi_present = EffectState::new(CpiState::BackfillPresent);
        write_transaction_catalog(
            root.path(),
            archive_id,
            &transactions_to_write,
            vec![not_recorded, recorded_empty, cpi_present],
            effect_files,
        );

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let replay = reader.read_slot(100).unwrap().unwrap();
        let full = reader.read_full_block(root.path(), replay).unwrap();
        assert_eq!(
            full.replay.index.effect_states[0].cpi().unwrap(),
            CpiState::NotRecorded
        );
        assert_eq!(
            full.replay.index.effect_states[1].cpi().unwrap(),
            CpiState::SourceEmpty
        );
        assert!(full.inner[0].is_none());
        assert!(full.inner[1].is_none());
        assert_eq!(full.inner[2], Some(inner));
        assert!(full.outcomes[0].is_some());
        assert!(full.outcomes[1].is_some());
        assert_eq!(full.balances[0], Some(unchanged_balances.clone()));
        assert_eq!(full.balances[1], Some(unchanged_balances));
        assert!(full.token_balances[0].as_ref().unwrap().is_empty());
        assert!(full.token_balances[1].as_ref().unwrap().is_empty());
        assert!(full.token_balances[2].is_none());
        assert!(full.logs[0].is_none());
        assert!(full.logs[1].as_ref().unwrap().is_empty());
        assert!(full.rewards[0].as_ref().unwrap().is_empty());
        assert!(full.rewards[1].as_ref().unwrap().is_empty());
        assert!(full.rewards[2].is_none());

        let audit = validate_all_effects(root.path(), 1 << 20).unwrap();
        assert_eq!(audit.blocks, 1);
        assert_eq!(audit.transactions, 3);
        assert_eq!(audit.records, [1, 2, 2, 0, 1, 0]);
    }

    #[test]
    fn replay_read_does_not_open_unselected_effect_files() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([6; 16]);
        let transaction = transaction();
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY],
        });
        write_transaction_catalog(
            root.path(),
            archive_id,
            &[transaction],
            vec![EffectState::new(CpiState::Unavailable)],
            effect_files,
        );
        assert!(!root.path().join(inner_instructions::PATH).exists());
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let replay = reader.read_slot(100).unwrap().unwrap();
        assert_eq!(replay.transactions.len(), 1);
    }

    #[test]
    fn canonical_reader_decodes_split_transaction_frames_with_exact_indexes() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([9; 16]);
        let transactions_to_write = (0..512).map(|_| transaction()).collect::<Vec<_>>();
        let effect_states = vec![EffectState::new(CpiState::Unavailable); 512];
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY; 2],
        });
        let arena = TransactionArenaEncoder::new()
            .prepare(&transactions_to_write)
            .unwrap();
        let page = arena
            .into_page(effect_states.clone(), effect_files.clone())
            .unwrap();
        assert!(page.compressed);
        write_payload(
            root.path(),
            transactions::PATH,
            archive_id,
            transactions_to_write.len() as u64,
            &page.stored,
        )
        .unwrap();
        let catalog = catalog_blocks::encode_table(&[BlockRow {
            slot: 100,
            parent_slot: 99,
            transaction_count: transactions_to_write.len() as u32,
            transactions: PageSpan {
                offset: FILE_HEADER_LEN as u64,
                stored_len: page.stored.len() as u32,
                decoded_len: page.decoded_len,
            },
            ..BlockRow::default()
        }])
        .unwrap();
        write_payload(root.path(), catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let replay = reader.read_slot(100).unwrap().unwrap();
        assert_eq!(replay.transactions, transactions_to_write);
        assert_eq!(replay.index.effect_states, effect_states);
        assert_eq!(replay.index.effect_files, effect_files);
    }

    #[test]
    fn point_reader_rejects_a_wrong_poh_catalog_ordinal() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let effect_files = std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY],
        });
        write_transaction_catalog(
            root.path(),
            archive_id,
            &[transaction()],
            vec![EffectState::new(CpiState::Unavailable)],
            effect_files,
        );
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let mut row = reader.block_at(0).unwrap();
        row.blockhash = HashRef {
            owner: HashOwner::PohBlockFinal,
            ordinal: 9,
        };
        drop(reader);
        overwrite_catalog_row(root.path(), 0, row);

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let error = reader.block_at(0).unwrap_err();
        assert!(format!("{error:#}").contains("blockhash PoH block ordinal is 9, expected 0"));
    }

    #[test]
    fn point_and_sequential_readers_reject_a_broken_catalog_link() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([8; 16]);
        write_merged_fixture(
            root.path(),
            archive_id,
            archive_id,
            archive_id,
            2,
            vec![
                FixtureBlock {
                    slot: 100,
                    parent_slot: 99,
                    transactions: vec![transaction()],
                    inner: vec![None],
                },
                FixtureBlock {
                    slot: 102,
                    parent_slot: 100,
                    transactions: vec![transaction()],
                    inner: vec![None],
                },
            ],
        );
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let mut second = reader.block_at(1).unwrap();
        second.parent_slot = 99;
        drop(reader);
        overwrite_catalog_row(root.path(), 1, second);

        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let point_error = reader.block_at(1).unwrap_err();
        assert!(format!("{point_error:#}").contains("expected 100"));

        let scan_error =
            scan_transactions_with_inner(root.path(), 1 << 20, |_| Ok(())).unwrap_err();
        assert!(format!("{scan_error:#}").contains("expected 100"));

        second.parent_slot = 100;
        second.previous_blockhash = HashRef {
            owner: HashOwner::NonPoh,
            ordinal: 77,
        };
        overwrite_catalog_row(root.path(), 1, second);
        let reader = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let hash_error = reader.block_at(1).unwrap_err();
        assert!(
            format!("{hash_error:#}")
                .contains("previous blockhash does not match the prior catalog row")
        );
    }
}

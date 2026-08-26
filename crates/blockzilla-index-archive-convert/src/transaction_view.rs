//! Borrowed helpers and worker-side encoding for the canonical transaction stream.

use std::io::Write;

use anyhow::{Context, Result, ensure};

use blockzilla_index_archive_format::ledger::transactions::{
    EFFECT_KIND_COUNT, EffectFileIndex, EffectState, LoadedAddresses, Message, MessageHeader,
    PubkeyId, ROW_RESTART_INTERVAL, RowRestart, Transaction, TransactionBlockHeader,
    encode_block_prefix,
};

use crate::container::decode_zstd_exact;

const TRANSACTION_ARENA_ZSTD_LEVEL: i32 = 3;

/// The resolved runtime account order for one transaction.
///
/// Known IDs are always static, loaded writable, then loaded readonly. When a
/// V0 source did not retain loaded pubkeys, `resolved_len` still includes the
/// exact width declared by its lookup descriptors, but `get` returns `None`
/// for those unknown positions.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResolvedAccounts<'a> {
    static_accounts: &'a [PubkeyId],
    loaded_writable: &'a [PubkeyId],
    loaded_readonly: &'a [PubkeyId],
    resolved_len: usize,
    complete: bool,
}

impl<'a> ResolvedAccounts<'a> {
    pub(crate) fn new(transaction: &'a Transaction) -> Self {
        match &transaction.message {
            Message::Legacy {
                static_accounts, ..
            } => Self {
                static_accounts,
                loaded_writable: &[],
                loaded_readonly: &[],
                resolved_len: static_accounts.len(),
                complete: true,
            },
            Message::V0 {
                static_accounts,
                loaded_addresses,
                lookups,
                ..
            } => match loaded_addresses {
                LoadedAddresses::Source { writable, readonly }
                | LoadedAddresses::Backfilled { writable, readonly } => Self {
                    static_accounts,
                    loaded_writable: writable,
                    loaded_readonly: readonly,
                    resolved_len: static_accounts.len() + writable.len() + readonly.len(),
                    complete: true,
                },
                LoadedAddresses::Unavailable => {
                    let (writable, readonly) =
                        lookups
                            .iter()
                            .fold((0_usize, 0_usize), |(writable, readonly), lookup| {
                                (
                                    writable + lookup.writable_indexes.len(),
                                    readonly + lookup.readonly_indexes.len(),
                                )
                            });
                    Self {
                        static_accounts,
                        loaded_writable: &[],
                        loaded_readonly: &[],
                        resolved_len: static_accounts.len() + writable + readonly,
                        complete: false,
                    }
                }
            },
        }
    }

    pub(crate) fn static_len(self) -> usize {
        self.static_accounts.len()
    }

    pub(crate) fn loaded_writable_len(self) -> usize {
        self.loaded_writable.len()
    }

    pub(crate) fn resolved_len(self) -> usize {
        self.resolved_len
    }

    pub(crate) fn is_complete(self) -> bool {
        self.complete
    }

    pub(crate) fn get(self, position: usize) -> Option<u32> {
        self.iter().nth(position)
    }

    pub(crate) fn iter(self) -> impl Iterator<Item = u32> + 'a {
        self.static_accounts
            .iter()
            .chain(self.loaded_writable)
            .chain(self.loaded_readonly)
            .map(|id| id.0)
    }

    pub(crate) fn positional_roles(self, header: MessageHeader, position: usize) -> u8 {
        use blockzilla_index_archive_format::indexes::accounts::{ROLE_SIGNER, ROLE_WRITABLE};

        let signer_count = usize::from(header.num_required_signatures);
        let mut roles = 0;
        if position < signer_count {
            roles |= ROLE_SIGNER;
            if position < signer_count - usize::from(header.num_readonly_signed) {
                roles |= ROLE_WRITABLE;
            }
        } else if position < self.static_len() {
            if position < self.static_len() - usize::from(header.num_readonly_unsigned) {
                roles |= ROLE_WRITABLE;
            }
        } else if position < self.static_len() + self.loaded_writable_len() {
            roles |= ROLE_WRITABLE;
        }
        roles
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionArenaEncoding {
    Raw,
    Zstd,
}

#[derive(Debug, PartialEq, Eq)]
enum TransactionArenaStorage {
    Raw(Vec<u8>),
    Zstd(Vec<u8>),
}

/// One block's transaction rows after worker-side encode and compression.
///
/// The ordered mapping stage can construct target `Transaction` values, then
/// move them to an encoding worker. No absolute target-file offset occurs in
/// this value. `row_restarts` and the encoded rows depend only on transaction
/// order, so one and many workers produce identical bytes.
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedTransactionArena {
    transaction_count: u32,
    signature_count: u32,
    decoded_len: u32,
    row_restarts: Vec<RowRestart>,
    storage: TransactionArenaStorage,
}

impl PreparedTransactionArena {
    pub const fn transaction_count(&self) -> u32 {
        self.transaction_count
    }

    pub const fn signature_count(&self) -> u32 {
        self.signature_count
    }

    pub const fn decoded_len(&self) -> u32 {
        self.decoded_len
    }

    pub fn stored_len(&self) -> usize {
        self.stored().len()
    }

    pub const fn encoding(&self) -> TransactionArenaEncoding {
        match self.storage {
            TransactionArenaStorage::Raw(_) => TransactionArenaEncoding::Raw,
            TransactionArenaStorage::Zstd(_) => TransactionArenaEncoding::Zstd,
        }
    }

    pub fn row_restarts(&self) -> &[RowRestart] {
        &self.row_restarts
    }

    fn stored(&self) -> &[u8] {
        match &self.storage {
            TransactionArenaStorage::Raw(bytes) | TransactionArenaStorage::Zstd(bytes) => bytes,
        }
    }

    /// Decode the row arena with the same exact-length and trailing-data rules
    /// used by canonical page reads.
    pub fn decode_rows(&self) -> Result<Vec<u8>> {
        let decoded_len = self.decoded_len as usize;
        match &self.storage {
            TransactionArenaStorage::Raw(bytes) => {
                ensure!(
                    bytes.len() == decoded_len,
                    "raw transaction arena has {} bytes, expected {decoded_len}",
                    bytes.len()
                );
                Ok(bytes.clone())
            }
            TransactionArenaStorage::Zstd(bytes) => {
                decode_zstd_exact(bytes, decoded_len, "prepared transaction arena")
            }
        }
    }

    /// Add the ordered effect-file offsets and create one physical page.
    ///
    /// A compressed result is two concatenated zstd frames: the small prefix
    /// frame made here and the large row frame made by a worker. The existing
    /// decoder consumes concatenated frames and reconstructs the unchanged
    /// `TransactionBlock` Wincode stream. If split compression does not reduce
    /// size, this method emits the exact raw stream instead.
    pub fn into_page(
        self,
        effect_states: Vec<EffectState>,
        effect_files: [EffectFileIndex; EFFECT_KIND_COUNT],
    ) -> Result<PreparedTransactionPage> {
        let header = TransactionBlockHeader {
            effect_states,
            row_restarts: self.row_restarts,
            effect_files,
        };
        header
            .validate(self.transaction_count)
            .context("validate transaction block header")?;
        let prefix = encode_block_prefix(&header, self.decoded_len as usize)
            .context("encode transaction block prefix")?;
        let decoded_len = prefix
            .len()
            .checked_add(self.decoded_len as usize)
            .context("transaction block page length overflow")?;
        let decoded_len_u32 =
            u32::try_from(decoded_len).context("transaction block page exceeds u32")?;

        match self.storage {
            TransactionArenaStorage::Raw(mut rows) => {
                ensure!(
                    rows.len() == self.decoded_len as usize,
                    "raw transaction arena length drifted"
                );
                let arena_stored_offset = prefix.len();
                let mut stored = prefix;
                stored.append(&mut rows);
                Ok(PreparedTransactionPage {
                    stored,
                    decoded_len: decoded_len_u32,
                    compressed: false,
                    arena_stored_offset,
                })
            }
            TransactionArenaStorage::Zstd(rows_frame) => {
                let prefix_frame = compress_zstd(&prefix, "transaction block prefix")?;
                let arena_stored_offset = prefix_frame.len();
                let split_stored_len = prefix_frame
                    .len()
                    .checked_add(rows_frame.len())
                    .context("transaction block stored length overflow")?;
                if split_stored_len >= decoded_len {
                    let mut rows = decode_zstd_exact(
                        &rows_frame,
                        self.decoded_len as usize,
                        "prepared transaction arena raw fallback",
                    )?;
                    let arena_stored_offset = prefix.len();
                    let mut stored = prefix;
                    stored.append(&mut rows);
                    return Ok(PreparedTransactionPage {
                        stored,
                        decoded_len: decoded_len_u32,
                        compressed: false,
                        arena_stored_offset,
                    });
                }

                let mut stored = Vec::with_capacity(split_stored_len);
                stored.extend_from_slice(&prefix_frame);
                stored.extend_from_slice(&rows_frame);
                Ok(PreparedTransactionPage {
                    stored,
                    decoded_len: decoded_len_u32,
                    compressed: true,
                    arena_stored_offset,
                })
            }
        }
    }
}

/// The final byte page. Ordered commit only appends `stored` and assigns its
/// absolute catalog offset.
#[derive(Debug, PartialEq, Eq)]
pub struct PreparedTransactionPage {
    pub stored: Vec<u8>,
    pub decoded_len: u32,
    pub compressed: bool,
    arena_stored_offset: usize,
}

impl PreparedTransactionPage {
    pub const fn arena_stored_offset(&self) -> usize {
        self.arena_stored_offset
    }

    pub fn decode(&self) -> Result<Vec<u8>> {
        if self.compressed {
            ensure!(
                self.stored.len() != self.decoded_len as usize,
                "compressed transaction page has an ambiguous raw length"
            );
            decode_zstd_exact(
                &self.stored,
                self.decoded_len as usize,
                "prepared transaction page",
            )
        } else {
            ensure!(
                self.stored.len() == self.decoded_len as usize,
                "raw transaction page has {} bytes, expected {}",
                self.stored.len(),
                self.decoded_len
            );
            Ok(self.stored.clone())
        }
    }
}

/// Reusable worker-local row encoder.
///
/// On a preparation error, the scratch buffer is cleared and retained. A
/// caller can use the same encoder for the next task. When a result is no
/// longer needed, [`Self::recycle`] returns its allocation to this encoder.
#[derive(Debug, Default)]
pub struct TransactionArenaEncoder {
    row_scratch: Vec<u8>,
}

impl TransactionArenaEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prepare(&mut self, transactions: &[Transaction]) -> Result<PreparedTransactionArena> {
        let transaction_count =
            u32::try_from(transactions.len()).context("transaction arena count exceeds u32")?;
        let mut rows = std::mem::take(&mut self.row_scratch);
        rows.clear();
        let mut row_restarts =
            Vec::with_capacity(transaction_count.div_ceil(ROW_RESTART_INTERVAL) as usize);
        let mut signature_count = 0_u32;

        for (index, transaction) in transactions.iter().enumerate() {
            let index = u32::try_from(index).expect("transaction count fits u32");
            if index % ROW_RESTART_INTERVAL == 0 {
                let row_byte_offset = match u32::try_from(rows.len()) {
                    Ok(offset) => offset,
                    Err(_) => {
                        rows.clear();
                        self.row_scratch = rows;
                        return Err(anyhow::anyhow!("transaction row arena exceeds u32"));
                    }
                };
                row_restarts.push(RowRestart {
                    row_byte_offset,
                    signature_delta: signature_count,
                });
            }
            if let Err(error) =
                blockzilla_index_archive_format::ledger::transactions::append_transaction(
                    &mut rows,
                    transaction,
                )
            {
                rows.clear();
                self.row_scratch = rows;
                return Err(error).context("encode transaction row");
            }
            let Some(next_signature_count) =
                signature_count.checked_add(u32::from(transaction.header.num_required_signatures))
            else {
                rows.clear();
                self.row_scratch = rows;
                return Err(anyhow::anyhow!("transaction signature count exceeds u32"));
            };
            signature_count = next_signature_count;
        }

        let decoded_len = match u32::try_from(rows.len()) {
            Ok(length) => length,
            Err(_) => {
                rows.clear();
                self.row_scratch = rows;
                return Err(anyhow::anyhow!("transaction row arena exceeds u32"));
            }
        };
        let compressed = match compress_zstd(&rows, "transaction row arena") {
            Ok(compressed) => compressed,
            Err(error) => {
                rows.clear();
                self.row_scratch = rows;
                return Err(error);
            }
        };
        let storage = if compressed.len() < rows.len() {
            self.row_scratch = rows;
            TransactionArenaStorage::Zstd(compressed)
        } else {
            TransactionArenaStorage::Raw(rows)
        };
        Ok(PreparedTransactionArena {
            transaction_count,
            signature_count,
            decoded_len,
            row_restarts,
            storage,
        })
    }

    pub fn recycle(&mut self, arena: PreparedTransactionArena) {
        let mut buffer = match arena.storage {
            TransactionArenaStorage::Raw(bytes) | TransactionArenaStorage::Zstd(bytes) => bytes,
        };
        buffer.clear();
        if buffer.capacity() > self.row_scratch.capacity() {
            self.row_scratch = buffer;
        }
    }

    #[cfg(test)]
    fn scratch_capacity(&self) -> usize {
        self.row_scratch.capacity()
    }
}

fn compress_zstd(bytes: &[u8], label: &str) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), TRANSACTION_ARENA_ZSTD_LEVEL)
        .with_context(|| format!("create zstd encoder for {label}"))?;
    encoder
        .include_checksum(true)
        .with_context(|| format!("enable zstd checksum for {label}"))?;
    encoder
        .set_pledged_src_size(Some(bytes.len() as u64))
        .with_context(|| format!("set zstd content size for {label}"))?;
    encoder
        .write_all(bytes)
        .with_context(|| format!("compress {label}"))?;
    encoder
        .finish()
        .with_context(|| format!("finish compressed {label}"))
}

#[cfg(test)]
mod tests {
    use blockzilla_index_archive_format::{
        indexes::accounts::{ROLE_SIGNER, ROLE_WRITABLE},
        ledger::transactions::{
            AddressTableLookup, ChunkFrame, EffectFileIndex, HashOwner, HashRef, Instruction,
            TransactionBlock, decode_block, encode_block,
        },
    };

    use super::*;
    use crate::pipeline::{
        OrderedTask, PipelineConfig, run_inline_ordered_encoding_stage, run_ordered_encoding_stage,
    };

    fn empty_effects(transaction_count: usize) -> [EffectFileIndex; EFFECT_KIND_COUNT] {
        let chunks = transaction_count.div_ceil(
            blockzilla_index_archive_format::ledger::transactions::EFFECT_CHUNK_TRANSACTIONS
                as usize,
        );
        std::array::from_fn(|_| EffectFileIndex {
            first_chunk_offset: 0,
            chunks: vec![ChunkFrame::EMPTY; chunks],
        })
    }

    fn unavailable_states(transaction_count: usize) -> Vec<EffectState> {
        vec![
            EffectState::new(
                blockzilla_index_archive_format::ledger::transactions::CpiState::Unavailable,
            );
            transaction_count
        ]
    }

    fn transaction(loaded_addresses: LoadedAddresses) -> Transaction {
        Transaction {
            header: MessageHeader {
                num_required_signatures: 2,
                num_readonly_signed: 1,
                num_readonly_unsigned: 1,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::V0 {
                static_accounts: [1, 2, 3, 4]
                    .into_iter()
                    .map(|id| PubkeyId::new(id).unwrap())
                    .collect(),
                loaded_addresses,
                lookups: vec![AddressTableLookup {
                    table_id: PubkeyId::new(9).unwrap(),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
                instructions: vec![Instruction {
                    program_position: 0,
                    account_positions: vec![],
                    data: vec![],
                }],
            },
        }
    }

    #[test]
    fn preserves_runtime_order_and_positional_roles() {
        let transaction = transaction(LoadedAddresses::Source {
            writable: vec![PubkeyId::new(5).unwrap()],
            readonly: vec![PubkeyId::new(6).unwrap()],
        });
        let view = ResolvedAccounts::new(&transaction);
        assert_eq!(view.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            view.positional_roles(transaction.header, 0),
            ROLE_SIGNER | ROLE_WRITABLE
        );
        assert_eq!(view.positional_roles(transaction.header, 1), ROLE_SIGNER);
        assert_eq!(view.positional_roles(transaction.header, 2), ROLE_WRITABLE);
        assert_eq!(view.positional_roles(transaction.header, 3), 0);
        assert_eq!(view.positional_roles(transaction.header, 4), ROLE_WRITABLE);
        assert_eq!(view.positional_roles(transaction.header, 5), 0);
    }

    #[test]
    fn unavailable_loaded_addresses_keep_width_without_inventing_ids() {
        let transaction = transaction(LoadedAddresses::Unavailable);
        let view = ResolvedAccounts::new(&transaction);
        assert_eq!(view.iter().count(), 4);
        assert_eq!(view.resolved_len(), 6);
        assert!(!view.is_complete());
        assert_eq!(view.get(3), Some(4));
        assert_eq!(view.get(4), None);
    }

    #[test]
    fn split_zstd_frames_decode_to_the_existing_logical_block() {
        let transactions = vec![
            transaction(LoadedAddresses::Source {
                writable: vec![PubkeyId::new(5).unwrap()],
                readonly: vec![PubkeyId::new(6).unwrap()],
            });
            512
        ];
        let mut encoder = TransactionArenaEncoder::new();
        let arena = encoder.prepare(&transactions).unwrap();
        assert_eq!(arena.encoding(), TransactionArenaEncoding::Zstd);
        let rows = arena.decode_rows().unwrap();
        let header = TransactionBlockHeader {
            effect_states: unavailable_states(transactions.len()),
            row_restarts: arena.row_restarts().to_vec(),
            effect_files: empty_effects(transactions.len()),
        };
        let logical = encode_block(&TransactionBlock::from_parts(header, rows)).unwrap();

        let page = arena
            .into_page(
                unavailable_states(transactions.len()),
                empty_effects(transactions.len()),
            )
            .unwrap();
        assert!(page.compressed);
        assert!(page.arena_stored_offset() > 0);
        assert_eq!(page.decode().unwrap(), logical);
        assert_eq!(
            decode_block(&page.decode().unwrap(), transactions.len() as u32)
                .unwrap()
                .transaction_rows,
            TransactionArenaEncoder::new()
                .prepare(&transactions)
                .unwrap()
                .decode_rows()
                .unwrap()
        );
    }

    #[test]
    fn exact_decoder_rejects_length_corruption_trailing_data_and_bad_arena_frame() {
        fn page() -> PreparedTransactionPage {
            let transactions = vec![transaction(LoadedAddresses::Unavailable); 512];
            TransactionArenaEncoder::new()
                .prepare(&transactions)
                .unwrap()
                .into_page(
                    unavailable_states(transactions.len()),
                    empty_effects(transactions.len()),
                )
                .unwrap()
        }

        let mut short = page();
        assert!(short.compressed);
        short.decoded_len -= 1;
        assert!(short.decode().is_err());

        let mut long = page();
        long.decoded_len += 1;
        assert!(long.decode().is_err());

        let mut trailing = page();
        trailing.stored.push(0);
        assert!(trailing.decode().is_err());

        let mut corrupt = page();
        let arena_offset = corrupt.arena_stored_offset();
        let byte = corrupt.stored.len() - 5;
        assert!(byte >= arena_offset);
        corrupt.stored[byte] ^= 0x40;
        assert!(corrupt.decode().is_err());
    }

    #[test]
    fn raw_arena_and_encoder_scratch_recover_after_error_and_recycle() {
        let small = vec![transaction(LoadedAddresses::Unavailable)];
        let mut encoder = TransactionArenaEncoder::new();
        let arena = encoder.prepare(&small).unwrap();
        assert_eq!(arena.encoding(), TransactionArenaEncoding::Raw);
        assert_eq!(
            blockzilla_index_archive_format::ledger::transactions::decode_transactions(
                &arena.decode_rows().unwrap(),
                1,
            )
            .unwrap(),
            small
        );
        encoder.recycle(arena);
        let recycled_capacity = encoder.scratch_capacity();
        assert!(recycled_capacity > 0);

        let mut invalid = transaction(LoadedAddresses::Unavailable);
        invalid.header.num_required_signatures = 0;
        assert!(encoder.prepare(&[invalid]).is_err());
        assert_eq!(encoder.scratch_capacity(), recycled_capacity);

        let recovered = encoder.prepare(&small).unwrap();
        assert_eq!(
            blockzilla_index_archive_format::ledger::transactions::decode_transactions(
                &recovered.decode_rows().unwrap(),
                1,
            )
            .unwrap(),
            small
        );
    }

    fn encoded_pages(worker_count: usize) -> Vec<u8> {
        let batches = (0..24)
            .map(|index| {
                vec![
                    transaction(LoadedAddresses::Source {
                        writable: vec![PubkeyId::new(5).unwrap()],
                        readonly: vec![PubkeyId::new(6).unwrap()],
                    });
                    64 + index % 5
                ]
            })
            .collect::<Vec<_>>();
        let mut output = Vec::new();
        let config = PipelineConfig {
            worker_count,
            max_in_flight_tasks: 6,
            max_in_flight_bytes: 8 << 20,
            first_sequence: 0,
        };
        let commit = |sequence: u64, arena: PreparedTransactionArena| {
            let count = arena.transaction_count() as usize;
            let page = arena.into_page(unavailable_states(count), empty_effects(count))?;
            output.extend_from_slice(&sequence.to_le_bytes());
            output.extend_from_slice(&page.decoded_len.to_le_bytes());
            output.extend_from_slice(&(page.stored.len() as u32).to_le_bytes());
            output.extend_from_slice(&page.stored);
            Ok::<_, anyhow::Error>(())
        };

        if worker_count == 0 {
            run_inline_ordered_encoding_stage(
                config,
                |_| {
                    let mut encoder = TransactionArenaEncoder::new();
                    Ok::<_, anyhow::Error>(move |transactions: Vec<Transaction>| {
                        encoder.prepare(&transactions)
                    })
                },
                commit,
                |sink| {
                    for (sequence, transactions) in batches.into_iter().enumerate() {
                        sink.submit(OrderedTask::new(sequence as u64, 256 << 10, transactions))
                            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        } else {
            run_ordered_encoding_stage(
                config,
                |_| {
                    let mut encoder = TransactionArenaEncoder::new();
                    Ok::<_, anyhow::Error>(move |transactions: Vec<Transaction>| {
                        encoder.prepare(&transactions)
                    })
                },
                commit,
                |sink| {
                    for (sequence, transactions) in batches.into_iter().enumerate() {
                        sink.submit(OrderedTask::new(sequence as u64, 256 << 10, transactions))
                            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        }
        output
    }

    #[test]
    fn transaction_pages_are_identical_inline_and_with_many_workers() {
        let inline = encoded_pages(0);
        assert_eq!(encoded_pages(1), inline);
        assert_eq!(encoded_pages(4), inline);
    }
}

//! Full Ed25519 verification for published Archive V2 transaction messages.

use std::{
    collections::{HashMap, VecDeque},
    ops::Range,
    time::{Duration, Instant},
};

use blockzilla_format::{
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2HotInstruction, ArchiveV2HotMessagePayload, CompactMessageHeader, CompactPubkey,
    OwnedCompactRecentBlockhash,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    ArchiveReader, BlockhashResolver, BlockhashResolverError, BorrowedDecodedBlock,
    Error as ReaderError, OrderedParallelBlockConfig, PreviousBlockhashTail,
    PreviousBlockhashTailSchema, RangeSource, SourceError,
    archive_integrity::IntegrityReaderReport,
    manifest::SIGNATURES_FILE,
    parse_previous_blockhash_tail,
    signed_message::{
        MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, ResolvedAddressTableLookup,
        SignedInstructionCandidates, SignedMessageCandidates, SignedMessageError,
        SignedMessageVersion, SignedTransactionConfig, VoteHashRegistry, VoteHashResolver,
        reconstruct_instruction_data_candidates, select_signed_message_candidate_ed25519,
    },
};

const HASH_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const PREDECESSOR_TAIL_RECORDS: usize = 300;
const PREDECESSOR_TAIL_RECORD_BYTES: usize = 40;
const VOTE_HASH_RECORD_BYTES: usize = 65;
const DEFAULT_REGISTRY_CACHE_ENTRIES: usize = 16_384;
const MAX_SIGNATURE_BYTES_PER_BLOCK: usize = 256 << 20;
const MAX_TOTAL_WORKER_SIGNATURE_BYTES: usize = 256 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveSignatureConfig {
    pub workers: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveSignatureReport {
    pub blocks_verified: u64,
    pub transactions_verified: u64,
    pub signatures_verified: u64,
    pub reader: IntegrityReaderReport,
    pub elapsed_millis: u64,
    pub ed25519_signature_verification: &'static str,
    pub block_decode_worker_threads: usize,
    pub max_signature_bytes_per_block: usize,
    pub max_total_worker_signature_bytes: usize,
}

#[derive(Debug, Error)]
pub enum ArchiveSignatureError {
    #[error("Archive V2 reader error: {0}")]
    Reader(#[from] ReaderError),
    #[error("Archive V2 source error: {0}")]
    Source(#[from] SourceError),
    #[error("signed-message verification error: {0}")]
    SignedMessage(#[from] SignedMessageError),
    #[error("blockhash resolver error: {0}")]
    Blockhash(#[from] BlockhashResolverError),
    #[error("Archive V2 signature verification error: {0}")]
    Invalid(String),
}

pub type SignatureResult<T> = std::result::Result<T, ArchiveSignatureError>;

fn load_blockhash_resolver<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> SignatureResult<BlockhashResolver> {
    let current_size = reader
        .source()
        .size(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?
        .ok_or_else(|| {
            ArchiveSignatureError::Invalid("blockhash registry is missing".to_owned())
        })?;
    if current_size % HASH_BYTES as u64 != 0 {
        return Err(ArchiveSignatureError::Invalid(format!(
            "blockhash registry size {current_size} is not a multiple of {HASH_BYTES}"
        )));
    }
    let maximum_records = reader.index().rows.len().checked_add(1).ok_or_else(|| {
        ArchiveSignatureError::Invalid("blockhash registry bound overflow".to_owned())
    })?;
    let maximum_bytes = maximum_records.checked_mul(HASH_BYTES).ok_or_else(|| {
        ArchiveSignatureError::Invalid("blockhash registry bound overflow".to_owned())
    })?;
    let current = reader
        .source()
        .read_all_bounded(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, maximum_bytes)?;

    let previous = if reader.manifest().epoch == 0 {
        PreviousBlockhashTail {
            schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
            entries: Vec::new(),
        }
    } else {
        let bytes = reader.source().read_all_bounded(
            ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
            PREDECESSOR_TAIL_RECORDS * PREDECESSOR_TAIL_RECORD_BYTES,
        )?;
        if bytes.len() != PREDECESSOR_TAIL_RECORDS * PREDECESSOR_TAIL_RECORD_BYTES {
            return Err(ArchiveSignatureError::Invalid(format!(
                "predecessor blockhash tail has {} bytes, expected {}",
                bytes.len(),
                PREDECESSOR_TAIL_RECORDS * PREDECESSOR_TAIL_RECORD_BYTES
            )));
        }
        parse_previous_blockhash_tail(&bytes, PreviousBlockhashTailSchema::CurrentHashAndSlot)?
    };
    BlockhashResolver::from_bytes(&current, previous).map_err(Into::into)
}

#[derive(Debug)]
struct SignatureWorker {
    registry: RegistryCache,
    signature_bytes: Vec<u8>,
    max_signature_bytes: usize,
}

impl SignatureWorker {
    fn new(max_signature_bytes: usize) -> Self {
        Self {
            registry: RegistryCache::new(DEFAULT_REGISTRY_CACHE_ENTRIES),
            signature_bytes: Vec::new(),
            max_signature_bytes,
        }
    }
}

#[derive(Debug)]
struct RegistryCache {
    capacity: usize,
    values: HashMap<u32, [u8; 32]>,
    order: VecDeque<u32>,
}

impl RegistryCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    fn resolve<S: RangeSource>(
        &mut self,
        reader: &ArchiveReader<S>,
        reference: &CompactPubkey,
    ) -> SignatureResult<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            return reader.resolve_pubkey(reference).map_err(Into::into);
        };
        if let Some(value) = self.values.get(id) {
            return Ok(*value);
        }
        let value = reader.resolve_pubkey(reference)?;
        if self.capacity != 0 {
            if self.values.len() == self.capacity
                && let Some(evicted) = self.order.pop_front()
            {
                self.values.remove(&evicted);
            }
            self.values.insert(*id, value);
            self.order.push_back(*id);
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SignatureBlockReport {
    transactions_verified: u64,
    signatures_verified: u64,
}

/// Verify every required Ed25519 signature in one published Archive V2 epoch.
///
/// The first signer selects the unique exact message candidate. The verifier
/// then checks every required signer against those same bytes. Raw transaction
/// fallbacks and incomplete or invalid proofs are hard errors.
pub fn verify_archive_v2_signatures<S: RangeSource>(
    reader: &ArchiveReader<S>,
    config: ArchiveSignatureConfig,
) -> SignatureResult<ArchiveSignatureReport> {
    let started = Instant::now();
    if config.workers == 0 || config.workers > crate::MAX_ORDERED_PARALLEL_DECODE_WORKERS {
        return Err(ArchiveSignatureError::Invalid(format!(
            "signature workers must be in 1..={}",
            crate::MAX_ORDERED_PARALLEL_DECODE_WORKERS
        )));
    }
    if !reader.signatures_available() {
        return Err(ArchiveSignatureError::Invalid(
            "signatures.bin is required for Ed25519 verification".to_owned(),
        ));
    }
    let blockhashes = load_blockhash_resolver(reader)?;
    let vote_hashes = read_vote_hash_registry(reader)?;
    let retained_per_worker = (32 * 1024 * 1024)
        .min(crate::MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES / config.workers);
    let signature_bytes_per_worker =
        (MAX_TOTAL_WORKER_SIGNATURE_BYTES / config.workers).min(MAX_SIGNATURE_BYTES_PER_BLOCK);
    let mut totals = SignatureBlockReport::default();
    let stats = reader.process_borrowed_blocks_parallel_ordered(
        Range {
            start: 0,
            end: reader.index().rows.len(),
        },
        OrderedParallelBlockConfig {
            decode_workers: config.workers,
            compressed_buffer_count: config.workers.clamp(1, 3),
            max_blocks_per_batch: 1_024,
            uncompressed_batch_budget_bytes: 256 * 1024 * 1024,
            retained_decompressed_bytes_per_worker: retained_per_worker,
            discard_rewards: true,
            ..OrderedParallelBlockConfig::default()
        },
        |_| Ok(SignatureWorker::new(signature_bytes_per_worker)),
        |worker, sequence, block| {
            verify_signature_block(
                reader,
                &blockhashes,
                vote_hashes.as_ref(),
                worker,
                sequence,
                block,
            )
        },
        |_sequence, block| {
            totals.transactions_verified = totals
                .transactions_verified
                .checked_add(block.transactions_verified)
                .ok_or_else(|| {
                    ArchiveSignatureError::Invalid("verified transaction count overflow".to_owned())
                })?;
            totals.signatures_verified = totals
                .signatures_verified
                .checked_add(block.signatures_verified)
                .ok_or_else(|| {
                    ArchiveSignatureError::Invalid("verified signature count overflow".to_owned())
                })?;
            Ok(())
        },
    )?;
    Ok(ArchiveSignatureReport {
        blocks_verified: stats.block_count,
        transactions_verified: totals.transactions_verified,
        signatures_verified: totals.signatures_verified,
        reader: stats.into(),
        elapsed_millis: elapsed_millis(started.elapsed()),
        ed25519_signature_verification: "complete",
        block_decode_worker_threads: config.workers,
        max_signature_bytes_per_block: signature_bytes_per_worker,
        max_total_worker_signature_bytes: MAX_TOTAL_WORKER_SIGNATURE_BYTES,
    })
}

fn verify_signature_block<S: RangeSource>(
    reader: &ArchiveReader<S>,
    blockhashes: &BlockhashResolver,
    vote_hashes: Option<&VoteHashRegistry>,
    worker: &mut SignatureWorker,
    sequence: usize,
    block: BorrowedDecodedBlock<'_>,
) -> SignatureResult<SignatureBlockReport> {
    if block.index_row.block_id as usize != sequence
        || block.header().slot != block.index_row.slot
        || block.tx_count() != block.index_row.tx_count
        || block.tx_rows_len() != block.index_row.tx_count as usize
    {
        return Err(ArchiveSignatureError::Invalid(format!(
            "block {sequence} identity or transaction count differs from its index row"
        )));
    }
    let signature_len = usize::try_from(block.index_row.signature_count)
        .ok()
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .ok_or_else(|| {
            ArchiveSignatureError::Invalid(format!(
                "block {sequence} signature byte length overflow"
            ))
        })?;
    let admitted_from_rows = usize::try_from(block.index_row.tx_count)
        .ok()
        .and_then(|count| count.checked_mul(u8::MAX as usize))
        .and_then(|count| count.checked_mul(SIGNATURE_BYTES))
        .unwrap_or(usize::MAX);
    let admitted = admitted_from_rows
        .min(MAX_SIGNATURE_BYTES_PER_BLOCK)
        .min(worker.max_signature_bytes);
    if signature_len > admitted {
        return Err(ArchiveSignatureError::Invalid(format!(
            "block {sequence} declares {signature_len} signature bytes, above admitted bound {admitted}"
        )));
    }
    let signature_offset = block
        .index_row
        .first_signature_ordinal
        .checked_mul(SIGNATURE_BYTES as u64)
        .ok_or_else(|| {
            ArchiveSignatureError::Invalid(format!(
                "block {sequence} signature byte offset overflow"
            ))
        })?;
    if signature_len == 0 {
        worker.signature_bytes.clear();
    } else {
        reader.source().read_range_into(
            SIGNATURES_FILE,
            signature_offset,
            signature_len,
            &mut worker.signature_bytes,
        )?;
    }

    let mut signature_cursor = 0usize;
    let mut report = SignatureBlockReport::default();
    for row in block.tx_rows() {
        let context = || {
            format!(
                "epoch {} slot {} transaction {}",
                reader.manifest().epoch,
                block.index_row.slot,
                row.tx_index
            )
        };
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            return Err(ArchiveSignatureError::Invalid(format!(
                "{} is a raw transaction fallback",
                context()
            )));
        }
        let message_bytes = lane_region(
            block.message_bytes(),
            row.message_offset,
            row.message_len,
            "message",
            &context(),
        )?;
        let message = reader.decode_message(message_bytes).map_err(|error| {
            ArchiveSignatureError::Invalid(format!("decode {} message: {error}", context()))
        })?;
        let (header, _, _, _) = message_parts(&message);
        if row.signature_count != header.num_required_signatures {
            return Err(ArchiveSignatureError::Invalid(format!(
                "{} has {} sidecar signatures, but its message requires {}",
                context(),
                row.signature_count,
                header.num_required_signatures
            )));
        }
        let transaction_signature_len = usize::from(row.signature_count)
            .checked_mul(SIGNATURE_BYTES)
            .ok_or_else(|| {
                ArchiveSignatureError::Invalid(format!(
                    "{} signature byte length overflow",
                    context()
                ))
            })?;
        let signature_end = signature_cursor
            .checked_add(transaction_signature_len)
            .filter(|end| *end <= worker.signature_bytes.len())
            .ok_or_else(|| {
                ArchiveSignatureError::Invalid(format!(
                    "{} signature range exceeds its block sidecar range",
                    context()
                ))
            })?;
        let signatures = worker.signature_bytes[signature_cursor..signature_end]
            .chunks_exact(SIGNATURE_BYTES)
            .map(|bytes| bytes.try_into().expect("signature chunk is 64 bytes"))
            .collect::<Vec<[u8; 64]>>();
        verify_message(
            reader,
            &mut worker.registry,
            blockhashes,
            vote_hashes,
            &message,
            &signatures,
        )
        .map_err(|error| {
            ArchiveSignatureError::Invalid(format!("verify {}: {error}", context()))
        })?;
        signature_cursor = signature_end;
        report.transactions_verified =
            report.transactions_verified.checked_add(1).ok_or_else(|| {
                ArchiveSignatureError::Invalid(format!(
                    "block {sequence} verified transaction count overflow"
                ))
            })?;
        report.signatures_verified = report
            .signatures_verified
            .checked_add(u64::from(row.signature_count))
            .ok_or_else(|| {
                ArchiveSignatureError::Invalid(format!(
                    "block {sequence} verified signature count overflow"
                ))
            })?;
    }
    if signature_cursor != worker.signature_bytes.len() {
        return Err(ArchiveSignatureError::Invalid(format!(
            "block {sequence} transaction rows consume {signature_cursor} of {} signature bytes",
            worker.signature_bytes.len()
        )));
    }
    Ok(report)
}

fn verify_message<S: RangeSource>(
    reader: &ArchiveReader<S>,
    registry: &mut RegistryCache,
    blockhashes: &BlockhashResolver,
    vote_hashes: Option<&VoteHashRegistry>,
    payload: &ArchiveV2HotMessagePayload,
    signatures: &[[u8; 64]],
) -> SignatureResult<()> {
    let (header, account_keys, recent_blockhash, instructions) = message_parts(payload);
    let static_account_keys = account_keys
        .iter()
        .map(|reference| registry.resolve(reader, reference))
        .collect::<SignatureResult<Vec<_>>>()?;
    let recent_blockhash = match recent_blockhash {
        OwnedCompactRecentBlockhash::Id(id) => blockhashes.resolve(*id)?,
        OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
    };
    let instruction_candidates = instructions
        .iter()
        .map(|instruction| {
            reconstruct_instruction_data_candidates(
                &instruction.data,
                vote_hashes.map(|resolver| resolver as &dyn VoteHashResolver),
            )
            .map(|candidates| (instruction, candidates))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let candidate_views = instruction_candidates
        .iter()
        .map(|(instruction, candidates)| SignedInstructionCandidates {
            program_id_index: instruction.program_id_index,
            accounts: &instruction.accounts,
            data_candidates: candidates,
        })
        .collect::<Vec<_>>();
    let resolved_lookups = match payload {
        ArchiveV2HotMessagePayload::Legacy(_) | ArchiveV2HotMessagePayload::V1(_) => Vec::new(),
        ArchiveV2HotMessagePayload::V0(message) => message
            .address_table_lookups
            .iter()
            .map(|lookup| {
                registry
                    .resolve(reader, &lookup.account_key)
                    .map(|account_key| ResolvedAddressTableLookup {
                        account_key,
                        writable_indexes: &lookup.writable_indexes,
                        readonly_indexes: &lookup.readonly_indexes,
                    })
            })
            .collect::<SignatureResult<Vec<_>>>()?,
    };
    let version = match payload {
        ArchiveV2HotMessagePayload::Legacy(_) => SignedMessageVersion::Legacy,
        ArchiveV2HotMessagePayload::V0(_) => SignedMessageVersion::V0 {
            address_table_lookups: &resolved_lookups,
        },
        ArchiveV2HotMessagePayload::V1(message) => SignedMessageVersion::V1 {
            config: SignedTransactionConfig {
                priority_fee: message.config.priority_fee,
                compute_unit_limit: message.config.compute_unit_limit,
                loaded_accounts_data_size_limit: message.config.loaded_accounts_data_size_limit,
                heap_size: message.config.heap_size,
            },
        },
    };
    select_signed_message_candidate_ed25519(
        &SignedMessageCandidates {
            version,
            header: *header,
            static_account_keys: &static_account_keys,
            recent_blockhash,
            instructions: &candidate_views,
        },
        MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
        signatures,
    )?;
    Ok(())
}

fn message_parts(
    payload: &ArchiveV2HotMessagePayload,
) -> (
    &CompactMessageHeader,
    &[CompactPubkey],
    &OwnedCompactRecentBlockhash,
    &[ArchiveV2HotInstruction],
) {
    match payload {
        ArchiveV2HotMessagePayload::Legacy(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
        ),
        ArchiveV2HotMessagePayload::V0(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
        ),
        ArchiveV2HotMessagePayload::V1(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
        ),
    }
}

fn read_vote_hash_registry<S: RangeSource>(
    reader: &ArchiveReader<S>,
) -> SignatureResult<Option<VoteHashRegistry>> {
    let Some(binding) = reader.manifest().file(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE) else {
        return Ok(None);
    };
    let maximum = reader
        .index()
        .rows
        .len()
        .checked_mul(VOTE_HASH_RECORD_BYTES)
        .ok_or_else(|| {
            ArchiveSignatureError::Invalid("vote-hash registry bound overflow".to_owned())
        })?;
    let size = usize::try_from(binding.size).map_err(|_| {
        ArchiveSignatureError::Invalid("vote-hash registry exceeds address space".to_owned())
    })?;
    if size > maximum {
        return Err(ArchiveSignatureError::Invalid(format!(
            "vote-hash registry has {size} bytes, above the {maximum}-byte block bound"
        )));
    }
    let bytes = reader
        .source()
        .read_all_bounded(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, maximum)?;
    VoteHashRegistry::from_bytes(&bytes)
        .map(Some)
        .map_err(Into::into)
}

fn lane_region<'a>(
    lane: &'a [u8],
    offset: u32,
    length: u32,
    name: &'static str,
    transaction: &str,
) -> SignatureResult<&'a [u8]> {
    let start = usize::try_from(offset).map_err(|_| {
        ArchiveSignatureError::Invalid(format!("{transaction} {name} offset exceeds address space"))
    })?;
    let end = start
        .checked_add(usize::try_from(length).map_err(|_| {
            ArchiveSignatureError::Invalid(format!(
                "{transaction} {name} length exceeds address space"
            ))
        })?)
        .filter(|end| *end <= lane.len())
        .ok_or_else(|| {
            ArchiveSignatureError::Invalid(format!(
                "{transaction} {name} range {offset}+{length} exceeds {} bytes",
                lane.len()
            ))
        })?;
    if start == end {
        return Err(ArchiveSignatureError::Invalid(format!(
            "{transaction} has an empty {name} record"
        )));
    }
    Ok(&lane[start..end])
}

fn elapsed_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    use blockzilla_format::{
        ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_SIGNATURES_FILE, ArchiveV2HotBlockBlob,
        ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow, ArchiveV2HotInstructionData,
        ArchiveV2HotLegacyMessage, ArchiveV2HotMetaRecord, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        wincode_leb128_config, write_archive_v2_hot_block_index, write_u32_varint,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use tempfile::TempDir;

    use crate::{
        HashVerification, OpenOptions, PinnedLocalRangeSource,
        manifest::TrustedGenerationIdentity,
        signed_message::{SignedInstruction, SignedMessage, serialize_signed_message},
    };

    #[test]
    fn archive_signature_pass_checks_every_required_signer() {
        let valid = TempDir::new().unwrap();
        write_signature_fixture(valid.path(), false);
        let reader = open_signature_fixture(valid.path());
        let report =
            verify_archive_v2_signatures(&reader, ArchiveSignatureConfig { workers: 2 }).unwrap();
        assert_eq!(report.blocks_verified, 1);
        assert_eq!(report.transactions_verified, 1);
        assert_eq!(report.signatures_verified, 2);
        assert!(report.max_signature_bytes_per_block > 0);
        assert!(
            report.max_signature_bytes_per_block * 2 <= report.max_total_worker_signature_bytes
        );

        let invalid = TempDir::new().unwrap();
        write_signature_fixture(invalid.path(), true);
        let reader = open_signature_fixture(invalid.path());
        let error = verify_archive_v2_signatures(&reader, ArchiveSignatureConfig { workers: 1 })
            .unwrap_err();
        assert!(error.to_string().contains("signature 1 did not verify"));
    }

    fn write_signature_fixture(root: &Path, corrupt_second: bool) {
        let first = SigningKey::from_bytes(&[7; 32]);
        let second = SigningKey::from_bytes(&[9; 32]);
        let keys = [
            first.verifying_key().to_bytes(),
            second.verifying_key().to_bytes(),
            [4; 32],
        ];
        let header = CompactMessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };
        let instruction_data = [5, 6, 7];
        let instruction_accounts = [0, 1];
        let signed_instruction = [SignedInstruction {
            program_id_index: 2,
            accounts: &instruction_accounts,
            data: &instruction_data,
        }];
        let signed_message = serialize_signed_message(&SignedMessage {
            version: SignedMessageVersion::Legacy,
            header,
            static_account_keys: &keys,
            recent_blockhash: [3; 32],
            instructions: &signed_instruction,
        })
        .unwrap();
        let mut second_signature = second.sign(&signed_message).to_bytes();
        if corrupt_second {
            second_signature[0] ^= 1;
        }
        let signatures = [first.sign(&signed_message).to_bytes(), second_signature].concat();

        let payload = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header,
            account_keys: keys.into_iter().map(CompactPubkey::Raw).collect(),
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([3; 32]),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: instruction_accounts.to_vec(),
                data: ArchiveV2HotInstructionData::Raw(instruction_data.to_vec()),
            }],
        });
        let message_bytes = wincode::config::serialize(&payload, wincode_leb128_config()).unwrap();
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 0,
                parent_slot: 0,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![blockzilla_format::ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: message_bytes.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 2,
                reserved: [0; 3],
            }],
            message_bytes,
            metadata_bytes: Vec::new(),
        };
        let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 1).unwrap();
        fs::write(root.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        write_archive_v2_hot_block_index(
            &root.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            1,
            0,
            &[ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 0,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: uncompressed.len() as u32,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 2,
            }],
        )
        .unwrap();
        fs::write(root.join(ARCHIVE_V2_SIGNATURES_FILE), signatures).unwrap();
        fs::write(root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), keys.concat()).unwrap();
        fs::write(
            root.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [[2; 32], [3; 32]].concat(),
        )
        .unwrap();

        let records = [
            ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
                version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
            }),
            ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
                blocks: 1,
                transactions: 1,
                ..WincodeArchiveV2Footer::default()
            }),
        ];
        let mut metadata = Vec::new();
        for record in records {
            let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();
            write_u32_varint(&mut metadata, bytes.len() as u32).unwrap();
            metadata.extend_from_slice(&bytes);
        }
        fs::write(root.join(ARCHIVE_V2_META_FILE), metadata).unwrap();
    }

    fn open_signature_fixture(root: &Path) -> ArchiveReader<PinnedLocalRangeSource> {
        ArchiveReader::open_trusted(
            PinnedLocalRangeSource::new(root),
            TrustedGenerationIdentity {
                cluster_id: "signature-test".to_owned(),
                epoch: 0,
                generation_id: "signature-test-0".to_owned(),
                slots_per_epoch: 100,
            },
            OpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..OpenOptions::default()
            },
        )
        .unwrap()
    }
}

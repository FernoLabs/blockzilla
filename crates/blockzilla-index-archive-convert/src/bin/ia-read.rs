//! Read one replay-ready block from the merged transaction stream.

use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_convert::{
    canonical_reader::{CanonicalReader, DEFAULT_MAX_BLOCK_DECODED_BYTES, ReplayBlock},
    container::validate_open_file,
};
use blockzilla_index_archive_format::{
    ArchiveId, FILE_HEADER_LEN,
    dictionary::blockhashes,
    ledger::transactions::{
        CpiState, EffectKind, HashOwner, HashRef, LoadedAddresses, Message, Transaction,
    },
    sidecars::{framing, poh, signatures},
};

const SIGNATURE_LEN: usize = 64;

struct FixedHashLane {
    file: File,
    records: u64,
}

struct PohLane {
    file: File,
    file_len: u64,
    profile: poh::PohWireProfile,
}

/// Lazy point readers for the two disjoint recent-blockhash owners.
///
/// A PoH ordinal is a catalog block ordinal. One lookup reads that fixed
/// catalog row and its exact retained PoH frame. It does not scan frames or
/// build a generation-local PoH index.
struct HashReaders<'a> {
    root: &'a Path,
    archive_id: ArchiveId,
    catalog: &'a CanonicalReader,
    non_poh: Option<FixedHashLane>,
    poh: Option<PohLane>,
    non_poh_cache: BTreeMap<u64, [u8; 32]>,
    poh_cache: BTreeMap<u64, [u8; 32]>,
}

impl<'a> HashReaders<'a> {
    fn new(root: &'a Path, catalog: &'a CanonicalReader) -> Self {
        Self {
            root,
            archive_id: catalog.archive_id(),
            catalog,
            non_poh: None,
            poh: None,
            non_poh_cache: BTreeMap::new(),
            poh_cache: BTreeMap::new(),
        }
    }

    fn read(&mut self, reference: HashRef) -> Result<[u8; 32]> {
        match reference.owner {
            HashOwner::NonPoh => self.read_non_poh(reference.ordinal),
            HashOwner::PohBlockFinal => self.read_poh_block_final(reference.ordinal),
        }
    }

    fn read_non_poh(&mut self, ordinal: u64) -> Result<[u8; 32]> {
        if let Some(hash) = self.non_poh_cache.get(&ordinal) {
            return Ok(*hash);
        }
        if self.non_poh.is_none() {
            let path = self.root.join(blockhashes::PATH);
            let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
            let header = validate_open_file(&file, blockhashes::PATH, self.archive_id)?;
            ensure!(
                header.payload_bytes == header.decoded_bytes,
                "{} must keep its fixed-width payload raw",
                blockhashes::PATH
            );
            ensure!(
                header
                    .payload_bytes
                    .is_multiple_of(blockhashes::RECORD_LEN as u64),
                "{} payload is not hash-record aligned",
                blockhashes::PATH
            );
            let records = header.payload_bytes / blockhashes::RECORD_LEN as u64;
            ensure!(
                header.record_count == records,
                "{} header declares {} records, payload contains {records}",
                blockhashes::PATH,
                header.record_count
            );
            self.non_poh = Some(FixedHashLane { file, records });
        }
        let lane = self.non_poh.as_ref().expect("initialized above");
        ensure!(
            ordinal < lane.records,
            "non-PoH hash ordinal {ordinal} is outside {} records",
            lane.records
        );
        let offset = ordinal
            .checked_mul(blockhashes::RECORD_LEN as u64)
            .and_then(|offset| offset.checked_add(FILE_HEADER_LEN as u64))
            .context("non-PoH hash offset overflow")?;
        let mut hash = [0_u8; blockhashes::RECORD_LEN];
        lane.file
            .read_exact_at(&mut hash, offset)
            .with_context(|| format!("read non-PoH hash ordinal {ordinal}"))?;
        self.non_poh_cache.insert(ordinal, hash);
        Ok(hash)
    }

    fn read_poh_block_final(&mut self, ordinal: u64) -> Result<[u8; 32]> {
        if let Some(hash) = self.poh_cache.get(&ordinal) {
            return Ok(*hash);
        }
        ensure!(
            ordinal < self.catalog.block_count(),
            "PoH block ordinal {ordinal} is outside {} catalog rows",
            self.catalog.block_count()
        );
        let row = self.catalog.block_at(ordinal)?;
        let span = row
            .poh
            .span()
            .with_context(|| format!("catalog block ordinal {ordinal} has no PoH frame"))?;

        if self.poh.is_none() {
            let path = self.root.join(poh::PATH);
            let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
            let header = validate_open_file(&file, poh::PATH, self.archive_id)?;
            ensure!(
                header.record_count == self.catalog.block_count(),
                "{} header declares {} blocks, catalog declares {}",
                poh::PATH,
                header.record_count,
                self.catalog.block_count()
            );
            ensure!(
                header.payload_bytes == header.decoded_bytes,
                "{} must keep its retained frames raw",
                poh::PATH
            );
            ensure!(
                header.payload_bytes >= poh::PREAMBLE_LEN as u64,
                "{} is too short for its preamble",
                poh::PATH
            );
            let mut preamble = [0_u8; poh::PREAMBLE_LEN];
            file.read_exact_at(&mut preamble, FILE_HEADER_LEN as u64)
                .context("read PoH preamble")?;
            let profile = poh::PohPreamble::decode(&preamble)
                .context("decode PoH preamble")?
                .profile;
            let file_len = file.metadata()?.len();
            self.poh = Some(PohLane {
                file,
                file_len,
                profile,
            });
        }
        let lane = self.poh.as_ref().expect("initialized above");
        ensure!(
            span.offset >= (FILE_HEADER_LEN + poh::PREAMBLE_LEN) as u64,
            "PoH span for block ordinal {ordinal} overlaps its header or preamble"
        );
        ensure!(
            span.stored_len == span.decoded_len,
            "PoH span for block ordinal {ordinal} must be a raw retained frame"
        );
        ensure!(
            span.stored_len as usize <= framing::MAX_FRAME_BYTES + framing::MAX_PREFIX_BYTES,
            "PoH frame for block ordinal {ordinal} is above the retained-frame decode guard"
        );
        let end = span
            .offset
            .checked_add(u64::from(span.stored_len))
            .context("PoH frame extent overflow")?;
        ensure!(
            end <= lane.file_len,
            "PoH span for block ordinal {ordinal} ends outside {}",
            poh::PATH
        );
        let mut frame = vec![0_u8; span.stored_len as usize];
        lane.file
            .read_exact_at(&mut frame, span.offset)
            .with_context(|| format!("read PoH frame for block ordinal {ordinal}"))?;
        let decoded = poh::decode_frame(lane.profile, &frame)
            .with_context(|| format!("decode PoH frame for block ordinal {ordinal}"))?;
        let (block_id, slot) = decoded.identity();
        ensure!(
            u64::from(block_id) == ordinal && slot == row.slot,
            "PoH frame identity ({block_id}, {slot}) disagrees with catalog block ordinal {ordinal}, slot {}",
            row.slot
        );
        let hash = *decoded
            .final_hash()
            .with_context(|| format!("PoH frame for block ordinal {ordinal} has no final entry"))?;
        self.poh_cache.insert(ordinal, hash);
        Ok(hash)
    }
}

fn usage() -> &'static str {
    "usage: ia-read <generation-dir> <slot> [--full]"
}

fn base58_hash(hash: [u8; 32]) -> String {
    solana_pubkey::Pubkey::new_from_array(hash).to_string()
}

fn resolved_shape(transaction: &Transaction) -> (Vec<u32>, usize, bool, usize) {
    match &transaction.message {
        Message::Legacy {
            static_accounts, ..
        } => (
            static_accounts.iter().map(|id| id.0).collect(),
            static_accounts.len(),
            true,
            0,
        ),
        Message::V0 {
            static_accounts,
            loaded_addresses,
            lookups,
            ..
        } => match loaded_addresses {
            LoadedAddresses::Source { writable, readonly }
            | LoadedAddresses::Backfilled { writable, readonly } => {
                let mut ids =
                    Vec::with_capacity(static_accounts.len() + writable.len() + readonly.len());
                ids.extend(static_accounts.iter().map(|id| id.0));
                ids.extend(writable.iter().map(|id| id.0));
                ids.extend(readonly.iter().map(|id| id.0));
                let resolved_count = ids.len();
                (ids, resolved_count, true, lookups.len())
            }
            LoadedAddresses::Unavailable => {
                let loaded = lookups.iter().fold(0_usize, |count, lookup| {
                    count + lookup.writable_indexes.len() + lookup.readonly_indexes.len()
                });
                (
                    static_accounts.iter().map(|id| id.0).collect(),
                    static_accounts.len() + loaded,
                    false,
                    lookups.len(),
                )
            }
        },
    }
}

fn read_signature_bytes(
    root: &Path,
    archive_id: blockzilla_index_archive_format::ArchiveId,
    replay: &ReplayBlock,
) -> Result<Vec<u8>> {
    let signature_count = replay
        .transactions
        .iter()
        .try_fold(0_u64, |count, transaction| {
            count
                .checked_add(u64::from(transaction.header.num_required_signatures))
                .context("block signature count overflow")
        })?;
    let byte_len = signature_count
        .checked_mul(SIGNATURE_LEN as u64)
        .context("block signature byte count overflow")?;
    ensure!(
        byte_len <= DEFAULT_MAX_BLOCK_DECODED_BYTES as u64,
        "block signature range has {byte_len} bytes, above the {}-byte guard",
        DEFAULT_MAX_BLOCK_DECODED_BYTES
    );
    let offset = replay
        .catalog
        .first_signature
        .checked_mul(SIGNATURE_LEN as u64)
        .and_then(|offset| offset.checked_add(FILE_HEADER_LEN as u64))
        .context("block signature offset overflow")?;
    let path = root.join(signatures::PATH);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let header = validate_open_file(&file, signatures::PATH, archive_id)?;
    ensure!(
        replay
            .catalog
            .first_signature
            .checked_add(signature_count)
            .is_some_and(|end| end <= header.record_count),
        "block signature range ends outside the signature sidecar"
    );
    let mut bytes = vec![0_u8; usize::try_from(byte_len).context("signature bytes exceed usize")?];
    file.read_exact_at(&mut bytes, offset)
        .context("read block signature range")?;
    Ok(bytes)
}

fn main() -> Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let root = PathBuf::from(arguments.next().context(usage())?);
    let slot: u64 = arguments
        .next()
        .context(usage())?
        .to_str()
        .context("slot is not valid UTF-8")?
        .parse()
        .context("slot must be an integer")?;
    let full = match arguments.next() {
        None => false,
        Some(value) if value == "--full" => true,
        Some(_) => bail!(usage()),
    };
    if arguments.next().is_some() {
        bail!(usage());
    }

    let started = Instant::now();
    let reader = CanonicalReader::open(&root, DEFAULT_MAX_BLOCK_DECODED_BYTES)?;
    let Some(replay) = reader.read_slot(slot)? else {
        bail!("slot {slot} is not in the block catalog");
    };
    let signature_bytes = read_signature_bytes(&root, reader.archive_id(), &replay)?;
    let transaction_bytes = u64::from(replay.catalog.transactions.stored_len);
    let signature_count = signature_bytes.len() / SIGNATURE_LEN;
    let mut hash_readers = HashReaders::new(&root, &reader);
    let blockhash = hash_readers
        .read(replay.catalog.blockhash)
        .context("read current blockhash")?;
    let previous_blockhash = hash_readers
        .read(replay.catalog.previous_blockhash)
        .context("read previous blockhash")?;
    let recent_blockhashes = replay
        .transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            hash_readers
                .read(transaction.recent_blockhash)
                .with_context(|| format!("read recent blockhash for transaction {index}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let replay_elapsed = started.elapsed();

    println!("block {}", replay.catalog.slot);
    println!("  catalog ordinal      {}", replay.ordinal);
    println!("  parent slot          {}", replay.catalog.parent_slot);
    println!("  blockhash            {}", base58_hash(blockhash));
    println!("  previous blockhash   {}", base58_hash(previous_blockhash));
    println!("  transactions         {}", replay.transactions.len());
    println!("  signatures           {signature_count}");
    println!("  transaction bytes    {transaction_bytes}");
    println!("  replay read elapsed  {replay_elapsed:?}");

    for (index, transaction) in replay.transactions.iter().enumerate() {
        let (account_ids, resolved_count, complete, lookup_count) = resolved_shape(transaction);
        let instruction_count = transaction.message.instructions().len();
        let data_bytes: usize = transaction
            .message
            .instructions()
            .iter()
            .map(|instruction| instruction.data.len())
            .sum();
        let state = replay.index.effect_states[index];
        println!(
            "  tx {index:>4}  accounts {}/{resolved_count}  instructions {instruction_count}  data {data_bytes} B  signatures {}  hash {}  effects 0x{:02x}",
            account_ids.len(),
            transaction.header.num_required_signatures,
            base58_hash(recent_blockhashes[index]),
            state.as_byte()
        );
        if !complete {
            println!(
                "           loaded account IDs unavailable; lookup descriptors {lookup_count}"
            );
        }
        if full {
            println!("           account IDs {account_ids:?}");
        }
    }

    if full {
        let effect_started = Instant::now();
        let full = reader.read_full_block(&root, replay)?;
        let cpi_records = full.inner.iter().flatten().count();
        let cpi_empty = full
            .replay
            .index
            .effect_states
            .iter()
            .filter(|state| {
                matches!(
                    state.cpi(),
                    Ok(CpiState::SourceEmpty | CpiState::BackfillEmpty)
                )
            })
            .count();
        let cpi_instructions = full
            .inner
            .iter()
            .flatten()
            .flat_map(|record| &record.groups)
            .map(|group| group.instructions.len())
            .sum::<usize>();
        let effect_bytes = |kind: EffectKind| {
            full.replay.index.effect_files[kind.index()]
                .chunks
                .iter()
                .map(|frame| u64::from(frame.stored_len()))
                .sum::<u64>()
        };
        let outcome_records = full.outcomes.iter().flatten().count();
        let failed = full
            .outcomes
            .iter()
            .flatten()
            .filter(|outcome| outcome.error.is_some())
            .count();
        let return_records = full
            .outcomes
            .iter()
            .flatten()
            .filter(|outcome| outcome.return_data.is_some())
            .count();
        let return_bytes = full
            .outcomes
            .iter()
            .flatten()
            .filter_map(|outcome| outcome.return_data.as_ref())
            .map(|data| data.data.len())
            .sum::<usize>();
        let balance_records = full.balances.iter().flatten().count();
        let balance_empty = full
            .balances
            .iter()
            .flatten()
            .filter(|balances| balances.pre.is_empty())
            .count();
        let balance_changes = full
            .balances
            .iter()
            .flatten()
            .map(|balances| balances.changes().count())
            .sum::<usize>();
        let token_records = full.token_balances.iter().flatten().count();
        let token_empty = full
            .token_balances
            .iter()
            .flatten()
            .filter(|balances| balances.is_empty())
            .count();
        let token_entries = full
            .token_balances
            .iter()
            .flatten()
            .map(Vec::len)
            .sum::<usize>();
        let log_records = full.logs.iter().flatten().count();
        let log_empty = full
            .logs
            .iter()
            .flatten()
            .filter(|lines| lines.is_empty())
            .count();
        let log_lines = full.logs.iter().flatten().map(Vec::len).sum::<usize>();
        let reward_records = full.rewards.iter().flatten().count();
        let reward_empty = full
            .rewards
            .iter()
            .flatten()
            .filter(|rewards| rewards.is_empty())
            .count();
        let reward_entries = full.rewards.iter().flatten().map(Vec::len).sum::<usize>();
        println!();
        println!("runtime effect streams");
        println!(
            "  CPI          records {cpi_records}, recorded-empty {cpi_empty}, instructions {cpi_instructions}, stored {} B",
            effect_bytes(EffectKind::InnerInstructions)
        );
        println!(
            "  outcomes     records {outcome_records}, failed {failed}, return data {return_records}/{return_bytes} B, stored {} B",
            effect_bytes(EffectKind::Outcome)
        );
        println!(
            "  balances     records {balance_records}, recorded-empty {balance_empty}, changes {balance_changes}, stored {} B",
            effect_bytes(EffectKind::Balances)
        );
        println!(
            "  tokens       known {token_records}, known-empty {token_empty}, entries {token_entries}, stored {} B",
            effect_bytes(EffectKind::TokenBalances)
        );
        println!(
            "  logs         records {log_records}, recorded-empty {log_empty}, lines {log_lines}, stored {} B",
            effect_bytes(EffectKind::Logs)
        );
        println!(
            "  rewards      known {reward_records}, known-empty {reward_empty}, entries {reward_entries}, stored {} B",
            effect_bytes(EffectKind::Rewards)
        );
        println!("  effect read elapsed  {:?}", effect_started.elapsed());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use blockzilla_index_archive_convert::container::write_payload;
    use blockzilla_index_archive_format::{
        catalog::blocks::{self as catalog_blocks, BlockRow, FactLocator, PageSpan},
        ledger::transactions::{
            self, AddressTableLookup, HashRef, Instruction, MessageHeader, PubkeyId,
            TransactionBlock,
        },
        sidecars::{
            framing,
            poh::{CurrentPohEntry, CurrentPohRecord, PohPreamble, PohWireProfile},
        },
        wincode as archive_wincode,
    };
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn complete_v0_shape_reports_all_resolved_accounts() {
        let transaction = Transaction {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed: 0,
                num_readonly_unsigned: 0,
            },
            recent_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            message: Message::V0 {
                static_accounts: vec![PubkeyId(1), PubkeyId(2)],
                loaded_addresses: LoadedAddresses::Source {
                    writable: vec![PubkeyId(3)],
                    readonly: vec![PubkeyId(4)],
                },
                lookups: vec![AddressTableLookup {
                    table_id: PubkeyId(5),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![1],
                }],
                instructions: vec![Instruction {
                    program_position: 1,
                    account_positions: vec![0, 2, 3],
                    data: Vec::new(),
                }],
            },
        };
        let (ids, resolved_count, complete, lookup_count) = resolved_shape(&transaction);
        assert_eq!(ids, [1, 2, 3, 4]);
        assert_eq!(resolved_count, ids.len());
        assert!(complete);
        assert_eq!(lookup_count, 1);
    }

    #[test]
    fn hash_reader_uses_the_catalog_addressed_poh_frame() {
        let root = tempdir().unwrap();
        let archive_id = ArchiveId::new([9; 16]);
        let transaction_block = TransactionBlock {
            effect_states: Vec::new(),
            row_restarts: Vec::new(),
            effect_files: std::array::from_fn(|_| Default::default()),
            transaction_rows: Vec::new(),
        };
        let transaction_payload = transactions::encode_block(&transaction_block).unwrap();
        write_payload(
            root.path(),
            transactions::PATH,
            archive_id,
            0,
            &transaction_payload,
        )
        .unwrap();

        let poh_record = CurrentPohRecord {
            block_id: 0,
            slot: 100,
            entries: vec![
                CurrentPohEntry {
                    num_hashes: 1,
                    hash: [7; 32],
                    transaction_count: 0,
                    signature_count: 0,
                },
                CurrentPohEntry {
                    num_hashes: 2,
                    hash: [8; 32],
                    transaction_count: 0,
                    signature_count: 0,
                },
            ],
        };
        let frame = framing::encode_frame(&archive_wincode::encode(&poh_record).unwrap()).unwrap();
        let mut poh_payload = PohPreamble {
            profile: PohWireProfile::ArchiveV2CurrentWincode055,
        }
        .encode()
        .to_vec();
        poh_payload.extend_from_slice(&frame);
        write_payload(root.path(), poh::PATH, archive_id, 1, &poh_payload).unwrap();
        write_payload(root.path(), blockhashes::PATH, archive_id, 1, &[3; 32]).unwrap();

        let row = BlockRow {
            slot: 100,
            parent_slot: 99,
            transaction_count: 0,
            blockhash: HashRef {
                owner: HashOwner::PohBlockFinal,
                ordinal: 0,
            },
            previous_blockhash: HashRef {
                owner: HashOwner::NonPoh,
                ordinal: 0,
            },
            transactions: PageSpan {
                offset: FILE_HEADER_LEN as u64,
                stored_len: transaction_payload.len() as u32,
                decoded_len: transaction_payload.len() as u32,
            },
            poh: FactLocator::Source(PageSpan {
                offset: (FILE_HEADER_LEN + poh::PREAMBLE_LEN) as u64,
                stored_len: frame.len() as u32,
                decoded_len: frame.len() as u32,
            }),
            ..BlockRow::default()
        };
        let catalog = catalog_blocks::encode_table(&[row]).unwrap();
        write_payload(root.path(), catalog_blocks::PATH, archive_id, 1, &catalog).unwrap();

        let catalog = CanonicalReader::open(root.path(), 1 << 20).unwrap();
        let mut readers = HashReaders::new(root.path(), &catalog);
        assert_eq!(readers.read(row.blockhash).unwrap(), [8; 32]);
        assert_eq!(readers.read(row.previous_blockhash).unwrap(), [3; 32]);
    }
}

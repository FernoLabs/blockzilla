use crate::{
    encode_signature_base58,
    store::{RECORD_BYTES, Record, ScanOutput, ScanStats, hash_signature, signature_fingerprint},
    types::{MetadataView, TransactionView},
};
use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use of_car_reader::{
    CarBlockReader,
    metadata_decoder::decode_transaction_status_meta,
    node::{Node, decode_node, peek_node_type},
    reader::entry_payload_slice,
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use ph::fmph;
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

const CAR_BUF: usize = 128 << 20;

pub(crate) fn scan_car(path: &Path) -> Result<ScanOutput> {
    let mut keys = Vec::new();
    let stats = visit_car_transactions(path, |_, signature, _, _| {
        keys.push(hash_signature(signature));
        Ok(())
    })?;
    Ok(ScanOutput { keys, stats })
}

pub(crate) fn write_values_from_car(
    path: &Path,
    mphf: &fmph::Function,
    values: &mut [u8],
    expected_records: usize,
) -> Result<ScanStats> {
    let expected_len = expected_records
        .checked_mul(RECORD_BYTES)
        .context("values.bin size overflow")?;
    if values.len() != expected_len {
        bail!(
            "values.bin length mismatch: expected {} bytes for {} records, got {}",
            expected_len,
            expected_records,
            values.len()
        );
    }

    let mut seen_slots = SlotBitmap::new(expected_records);
    let stats = visit_car_transactions(path, |slot, signature, entry_start, entry_size| {
        let key = hash_signature(signature);
        let Some(pos) = mphf.get(&key) else {
            bail!("mphf missing key at slot {} offset {}", slot, entry_start);
        };
        let pos = pos as usize;
        if pos >= expected_records {
            bail!("mphf returned out-of-range index {}", pos);
        }
        if !seen_slots.mark(pos) {
            bail!("mphf collision at slot {}", pos);
        }

        let record = Record {
            fingerprint: signature_fingerprint(signature),
            slot,
            offset: entry_start,
            size: entry_size,
        };
        let start = pos * RECORD_BYTES;
        values[start..start + RECORD_BYTES].copy_from_slice(&record.encode());
        Ok(())
    })?;

    if stats.indexed_txs != expected_records {
        bail!(
            "second pass indexed {} records, expected {}",
            stats.indexed_txs,
            expected_records
        );
    }
    if seen_slots.count() != expected_records {
        bail!(
            "second pass populated {} record slots, expected {}",
            seen_slots.count(),
            expected_records
        );
    }

    Ok(stats)
}

pub(crate) fn load_transaction_view_from_file(
    car_path: &Path,
    target_offset: u64,
    entry_size: u32,
    expected_signature: &[u8; 64],
) -> Result<TransactionView> {
    let entry = read_entry_by_seek(car_path, target_offset, entry_size)?;
    decode_transaction_view_from_entry(&entry, expected_signature)
}

pub(crate) fn decode_transaction_view_from_entry(
    entry: &[u8],
    expected_signature: &[u8; 64],
) -> Result<TransactionView> {
    let payload = entry_payload(entry)?;

    let Node::Transaction(tx) = decode_node(payload).context("decode transaction node")? else {
        bail!("entry does not point to a transaction node");
    };

    if tx.data.next.is_some() || tx.metadata.next.is_some() {
        bail!("transaction uses DataFrame continuations and is not yet supported by this query");
    }

    let versioned_tx = wincode::deserialize::<VersionedTransaction<'_>>(tx.data.data)
        .context("decode transaction bytes")?;
    let Some(first_signature) = versioned_tx.signatures.first().copied() else {
        bail!("transaction has no signatures");
    };
    if first_signature != expected_signature {
        bail!("index lookup did not match the requested signature after CAR verification");
    }

    let message_version = match &versioned_tx.message {
        VersionedMessage::Legacy(_) => "legacy",
        VersionedMessage::V0(_) => "v0",
    };
    let signatures = versioned_tx
        .signatures
        .iter()
        .map(|signature| encode_signature_base58(signature))
        .collect();

    let metadata = if tx.metadata.data.is_empty() {
        None
    } else {
        match decode_transaction_status_meta(tx.slot, tx.metadata.data) {
            Ok(meta) => Some(MetadataView {
                fee: meta.fee,
                has_error: meta.err.is_some(),
                log_count: meta.log_messages.len(),
                inner_instruction_count: meta
                    .inner_instructions
                    .iter()
                    .map(|ix| ix.instructions.len())
                    .sum(),
            }),
            Err(_) => None,
        }
    };

    Ok(TransactionView {
        slot: tx.slot,
        index: tx.index,
        message_version,
        signatures,
        transaction_data_base64: BASE64.encode(tx.data.data),
        metadata,
    })
}

fn visit_car_transactions<F>(path: &Path, visit: F) -> Result<ScanStats>
where
    F: FnMut(u64, &[u8; 64], u64, u32) -> Result<()>,
{
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    visit_reader(file, visit)
}

fn visit_reader<R: Read, F>(reader: R, mut visit: F) -> Result<ScanStats>
where
    F: FnMut(u64, &[u8; 64], u64, u32) -> Result<()>,
{
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;

    let mut stats = ScanStats::default();
    let mut scratch = Vec::with_capacity(1 << 20);

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .context("read CAR entry")?
    {
        if peek_node_type(entry.payload).context("peek node type")? != 0 {
            continue;
        }

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };

        if tx.data.next.is_some() {
            stats.skipped_txs += 1;
            continue;
        }

        let versioned_tx = match wincode::deserialize::<VersionedTransaction<'_>>(tx.data.data) {
            Ok(tx) => tx,
            Err(_) => {
                stats.skipped_txs += 1;
                continue;
            }
        };

        let Some(signature) = versioned_tx.signatures.first().copied() else {
            stats.skipped_txs += 1;
            continue;
        };

        let entry_size = u32::try_from(entry.total_len)
            .with_context(|| format!("entry too large at offset {}", entry.location.car_offset))?;

        visit(tx.slot, signature, entry.location.car_offset, entry_size)?;
        stats.indexed_txs += 1;
    }

    Ok(stats)
}

#[derive(Debug, Default)]
struct SlotBitmap {
    words: Vec<u64>,
    marked: usize,
}

impl SlotBitmap {
    fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            marked: 0,
        }
    }

    fn mark(&mut self, index: usize) -> bool {
        let word = index / 64;
        let bit = index % 64;
        let mask = 1u64 << bit;
        if self.words[word] & mask != 0 {
            return false;
        }
        self.words[word] |= mask;
        self.marked += 1;
        true
    }

    fn count(&self) -> usize {
        self.marked
    }
}

fn read_entry_by_seek(path: &Path, target_offset: u64, entry_size: u32) -> Result<Vec<u8>> {
    if entry_size == 0 {
        bail!("entry size cannot be zero");
    }

    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(CAR_BUF, file);
    reader
        .seek(SeekFrom::Start(target_offset))
        .with_context(|| format!("seek {} to {}", path.display(), target_offset))?;

    let mut entry = vec![0u8; entry_size as usize];
    reader
        .read_exact(&mut entry)
        .with_context(|| format!("read entry at offset {}", target_offset))?;
    Ok(entry)
}

fn entry_payload(entry: &[u8]) -> Result<&[u8]> {
    entry_payload_slice(entry).context("extract CAR entry payload")
}

#[cfg(test)]
mod tests {
    use super::{SlotBitmap, entry_payload};

    #[test]
    fn extracts_payload_from_entry_bytes() {
        let mut entry = Vec::with_capacity(1 + 36 + 2);
        entry.push(38);
        entry.extend_from_slice(&[0u8; 36]);
        entry.extend_from_slice(&[1u8, 2u8]);

        let payload = entry_payload(&entry).expect("extract payload");
        assert_eq!(payload, [1u8, 2u8]);
    }

    #[test]
    fn slot_bitmap_rejects_duplicate_marks() {
        let mut bitmap = SlotBitmap::new(130);

        assert!(bitmap.mark(0));
        assert!(bitmap.mark(64));
        assert!(bitmap.mark(129));
        assert!(!bitmap.mark(64));
        assert_eq!(bitmap.count(), 3);
    }
}

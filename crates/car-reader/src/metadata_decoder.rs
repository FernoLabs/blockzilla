use core::fmt;
use std::io::Read;

use prost::Message;

use crate::confirmed_block::TransactionStatusMeta;
use crate::stored_transaction_status_meta::StoredTransactionStatusMeta;

// Epochs earlier than this were bincode-encoded in Old Faithful.
pub const BINCODE_EPOCH_CUTOFF: u64 = 157;

#[derive(Debug)]
pub enum MetadataDecodeError {
    ZstdDecompress(std::io::Error),
    Bincode(String),
    ProstDecode(prost::DecodeError),
    ProtoConvert(String),
}

impl fmt::Display for MetadataDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataDecodeError::ZstdDecompress(e) => write!(f, "zstd decompress: {e}"),
            MetadataDecodeError::Bincode(e) => write!(f, "bincode decode: {e}"),
            MetadataDecodeError::ProstDecode(e) => write!(f, "protobuf decode: {e}"),
            MetadataDecodeError::ProtoConvert(e) => write!(f, "protobuf convert: {e}"),
        }
    }
}

impl std::error::Error for MetadataDecodeError {}

/// Decode TransactionStatusMeta from a "frame" (possibly zstd-compressed; possibly empty).
///
/// Behavior matches the snippet you pasted:
/// - empty => default meta
/// - try zstd first; if not zstd, fall back to raw decode
pub fn decode_transaction_status_meta_from_frame(
    slot: u64,
    reassembled_metadata: &[u8],
    out: &mut TransactionStatusMeta,
) -> Result<(), MetadataDecodeError> {
    out.clear();

    if reassembled_metadata.is_empty() {
        return Ok(());
    }

    // Try zstd. If it fails, treat the bytes as raw.
    match decompress_zstd(reassembled_metadata) {
        Ok(decompressed) => decode_transaction_status_meta(slot, &decompressed, out),
        Err(decomp_err) => decode_transaction_status_meta(slot, reassembled_metadata, out),
    }
}

/// Decode TransactionStatusMeta from raw bytes (either bincode StoredTransactionStatusMeta
/// for early epochs, or protobuf for later epochs).
pub fn decode_transaction_status_meta(
    slot: u64,
    metadata_bytes: &[u8],
    out: &mut TransactionStatusMeta,
) -> Result<(), MetadataDecodeError> {
    let epoch = slot_to_epoch(slot);

    if epoch < BINCODE_EPOCH_CUTOFF {
        *out = wincode::deserialize::<StoredTransactionStatusMeta>(metadata_bytes)
            .map_err(|err| MetadataDecodeError::Bincode(err.to_string()))?
            .into();
    } else {
        out.merge(metadata_bytes)
            .map_err(MetadataDecodeError::ProstDecode)?;
    }

    Ok(())
}

// todo fixme
pub type SharedError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, SharedError> {
    let mut decoder = zstd::Decoder::new(data)?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

#[inline(always)]
/// Converts a slot back into the epoch that contains it.
pub const fn slot_to_epoch(slot: u64) -> u64 {
    slot / 432000
}

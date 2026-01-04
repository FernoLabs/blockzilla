use core::fmt;
use prost::Message;

use crate::confirmed_block::TransactionStatusMeta;
use crate::stored_transaction_status_meta::StoredTransactionStatusMeta;

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

#[inline]
fn looks_like_zstd_frame(data: &[u8]) -> bool {
    // zstd frame magic number: 28 B5 2F FD
    data.len() >= 4 && data[0..4] == [0x28, 0xB5, 0x2F, 0xFD]
}

/// Reusable zstd context + reusable output buffer.
/// Keep one per worker thread. Do not share across threads.
pub struct ZstdReusableDecoder {
    dctx: zstd::zstd_safe::DCtx<'static>,
    out: Vec<u8>,
}

impl ZstdReusableDecoder {
    /// `out_capacity` should be your typical decompressed metadata size.
    #[inline]
    pub fn new(out_capacity: usize) -> Self {
        Self {
            dctx: zstd::zstd_safe::DCtx::create(),
            out: Vec::with_capacity(out_capacity),
        }
    }

    #[inline]
    pub fn output(&self) -> &[u8] {
        &self.out
    }

    /// If `input` is zstd, decompress into the internal buffer and return Ok(true).
    /// If it is not zstd, return Ok(false) and leave output empty.
    pub fn decompress_if_zstd(&mut self, input: &[u8]) -> Result<bool, std::io::Error> {
        self.out.clear();
        let size_hint = 10 * input.len();
        if size_hint > self.out.len() {
            self.out.resize(size_hint, 0);
        }

        if !looks_like_zstd_frame(input) {
            return Ok(false);
        }

        let read = self
            .dctx
            .decompress(&mut self.out, input)
            .inspect_err(|err| println!("{}", err))
            .expect("error zstd decoding");
        self.out.truncate(read);
        Ok(true)
    }
}

/// Decode TransactionStatusMeta from a "frame" (possibly zstd-compressed; possibly empty).
///
/// Behavior:
/// - empty => default meta
/// - if zstd magic, decompress using reusable decoder
/// - else treat bytes as raw
pub fn decode_transaction_status_meta_from_frame(
    slot: u64,
    reassembled_metadata: &[u8],
    out: &mut TransactionStatusMeta,
    zstd: &mut ZstdReusableDecoder,
) -> Result<(), MetadataDecodeError> {
    out.clear();

    if reassembled_metadata.is_empty() {
        return Ok(());
    }

    if zstd
        .decompress_if_zstd(reassembled_metadata)
        .map_err(MetadataDecodeError::ZstdDecompress)?
    {
        decode_transaction_status_meta(slot, zstd.output(), out)
    } else {
        decode_transaction_status_meta(slot, reassembled_metadata, out)
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

#[inline(always)]
pub const fn slot_to_epoch(slot: u64) -> u64 {
    slot / 432000
}

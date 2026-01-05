use core::fmt;
use prost::Message;
use zstd::zstd_safe;

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
    len: usize,
    out: [u8; 32 * 1024], // 10KB max log + inner instruction usually ~= log len
}

impl ZstdReusableDecoder {
    /// `out_capacity` should be your typical decompressed metadata size.
    #[inline]
    pub fn new() -> Self {
        Self {
            dctx: zstd::zstd_safe::DCtx::create(),
            out: [0; _],
            len: 0,
        }
    }

    #[inline]
    pub fn output(&self) -> &[u8] {
        &self.out[..self.len]
    }

    /// If `input` is zstd, decompress into the internal buffer and return Ok(true).
    /// If it is not zstd, return Ok(false) and leave output empty.
    pub fn decompress_if_zstd(&mut self, input: &[u8]) -> Result<bool, std::io::Error> {
        if !looks_like_zstd_frame(input) {
            return Ok(false);
        }

        let read = self
            .dctx
            .decompress(&mut self.out, input)
            .inspect_err(|code| {
                let name = zstd_safe::get_error_name(*code);
                eprintln!("zstd decode failed: {name} (raw={code})");
            })
            .expect("error zstd decoding");
        self.len = read;
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
            .inspect_err(|_err| println!("invalid metadata : {:?}", metadata_bytes))
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

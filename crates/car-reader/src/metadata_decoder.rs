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
    // 10KB max log + inner instruction usually ~= log len (32k was weirdly not enouth)
    out: [u8; 1024 * 1024],
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
                eprintln!(
                    "zstd decode failed: {name} (raw={code}) input {} buffer {}",
                    input.len(),
                    self.out.len()
                );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_invalid_early_epoch_metadata_returns_bincode_error() {
        // Paste the metadata byte vector here
        let metadata: &[u8] = &[0, 0, 0, 0, 16, 39, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 72, 25, 41, 83, 215, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 0, 192, 56, 7, 0, 0, 0, 0, 128, 205, 171, 66, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 56, 242, 40, 83, 215, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 0, 192, 56, 7, 0, 0, 0, 0, 128, 205, 171, 66, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 7, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102, 72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69, 113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 105, 110, 118, 111, 107, 101, 32, 91, 49, 93, 88, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102, 72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69, 113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 99, 111, 110, 115, 117, 109, 101, 100, 32, 51, 49, 50, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111, 109, 112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102, 72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69, 113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 115, 117, 99, 99, 101, 115, 115, 62, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102, 101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66, 118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 105, 110, 118, 111, 107, 101, 32, 91, 49, 93, 34, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 108, 111, 103, 58, 32, 73, 110, 115, 116, 114, 117, 99, 116, 105, 111, 110, 58, 32, 84, 114, 97, 110, 115, 102, 101, 114, 89, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102, 101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66, 118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 99, 111, 110, 115, 117, 109, 101, 100, 32, 53, 51, 50, 56, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111, 109, 112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 0, 0, 0, 0, 0, 0, 0, 80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102, 101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66, 118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 115, 117, 99, 99, 101, 115, 115, 1, 2, 0, 0, 0, 0, 0, 0, 0, 2, 43, 0, 0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65, 72, 74, 113, 54, 236, 23, 236, 20, 137, 62, 95, 65, 5, 12, 0, 0, 0, 0, 0, 0, 0, 56, 49, 57, 48, 53, 48, 48, 51, 50, 54, 57, 49, 3, 43, 0, 0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65, 72, 74, 113, 54, 0, 0, 0, 0, 0, 112, 167, 64, 5, 9, 0, 0, 0, 0, 0, 0, 0, 51, 48, 48, 48, 48, 48, 48, 48, 48, 1, 2, 0, 0, 0, 0, 0, 0, 0, 3, 43, 0, 0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65, 72, 74, 113, 54, 0, 0, 0, 0, 0, 64, 159, 64, 5, 9, 0, 0, 0, 0, 0, 0, 0, 50, 48, 48, 48, 48, 48, 48, 48, 48, 2, 43, 0, 0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65, 72, 74, 113, 54, 236, 23, 236, 20, 131, 63, 95, 65, 5, 12, 0, 0, 0, 0, 0, 0, 0, 56, 49, 57, 49, 53, 48, 48, 51, 50, 54, 57, 49];

        // Any slot < BINCODE_EPOCH_CUTOFF * 432000 forces bincode path
        let slot = (BINCODE_EPOCH_CUTOFF - 1) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta(slot, metadata, &mut out).inspect_err(|err|println!("{err}"));
        assert!(res.is_ok() )
    }
}
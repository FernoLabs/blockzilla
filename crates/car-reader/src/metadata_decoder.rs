use core::fmt;
use prost::Message;
use zstd::zstd_safe;

use crate::confirmed_block::TransactionStatusMeta;
use crate::stored_transaction_status_meta::StoredTransactionStatusMeta;

pub const BINCODE_EPOCH_CUTOFF: u64 = 156;

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

impl Default for ZstdReusableDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZstdReusableDecoder {
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

    // epoch 148 is a special case it is protobuf encoded
    if epoch < BINCODE_EPOCH_CUTOFF && epoch != 148 {
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
    fn decode_invalid() {
        let metadata = &[
            16, 136, 39, 26, 14, 133, 136, 213, 196, 188, 9, 146, 130, 204, 203, 2, 1, 1, 1, 34,
            14, 253, 224, 212, 196, 188, 9, 146, 130, 204, 203, 2, 1, 1, 1, 50, 62, 80, 114, 111,
            103, 114, 97, 109, 32, 86, 111, 116, 101, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 32, 105, 110, 118, 111, 107, 101, 32, 91, 49, 93, 50, 59, 80,
            114, 111, 103, 114, 97, 109, 32, 86, 111, 116, 101, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 32, 115, 117, 99, 99, 101, 115, 115,
        ];
        let slot = (BINCODE_EPOCH_CUTOFF) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }

    #[test]
    fn decode_invalid_early_epoch_metadata_returns_bincode_error() {
        // Paste the metadata byte vector here
        let metadata: &[u8] = &[
            0, 0, 0, 0, // status: Ok(())
            16, 39, 0, 0, 0, 0, 0, 0, // fee: u64 = 10_000
            // pre_balances: Vec<u64>
            6, 0, 0, 0, 0, 0, 0, 0, // pre_balances len = 6
            72, 25, 41, 83, 215, 9, 0, 0, // pre_balances[0] (u64 LE)
            0, 0, 0, 0, 0, 0, 0, 0, // pre_balances[1]
            240, 29, 31, 0, 0, 0, 0, 0, // pre_balances[2]
            240, 29, 31, 0, 0, 0, 0, 0, // pre_balances[3]
            0, 192, 56, 7, 0, 0, 0, 0, // pre_balances[4]
            128, 205, 171, 66, 0, 0, 0, 0, // pre_balances[5]
            // post_balances: Vec<u64>
            6, 0, 0, 0, 0, 0, 0, 0, // post_balances len = 6
            56, 242, 40, 83, 215, 9, 0, 0, // post_balances[0]
            0, 0, 0, 0, 0, 0, 0, 0, // post_balances[1]
            240, 29, 31, 0, 0, 0, 0, 0, // post_balances[2]
            240, 29, 31, 0, 0, 0, 0, 0, // post_balances[3]
            0, 192, 56, 7, 0, 0, 0, 0, // post_balances[4]
            128, 205, 171, 66, 0, 0, 0, 0, // post_balances[5]
            // inner_instructions: Option<Vec<InnerInstructions>>
            1, // Option::Some
            0, 0, 0, 0, 0, 0, 0, 0, // Vec len = 0 (Some(vec![]))
            // log_messages: Option<Vec<String>>
            1, // Option::Some
            7, 0, 0, 0, 0, 0, 0, 0, // log_messages len = 7
            62, 0, 0, 0, 0, 0, 0, 0, // log[0] string len = 62
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 105, 110, 118, 111, 107, 101,
            32, 91, 49, 93, 88, 0, 0, 0, 0, 0, 0, 0, // log[1] string len = 88
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 99, 111, 110, 115, 117, 109,
            101, 100, 32, 51, 49, 50, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111, 109,
            112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 59, 0, 0, 0, 0, 0, 0,
            0, // log[2] string len = 59
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 115, 117, 99, 99, 101, 115,
            115, 62, 0, 0, 0, 0, 0, 0, 0, // log[3] string len = 62
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 105, 110, 118, 111, 107,
            101, 32, 91, 49, 93, 34, 0, 0, 0, 0, 0, 0, 0, // log[4] string len = 34
            80, 114, 111, 103, 114, 97, 109, 32, 108, 111, 103, 58, 32, 73, 110, 115, 116, 114,
            117, 99, 116, 105, 111, 110, 58, 32, 84, 114, 97, 110, 115, 102, 101, 114, 89, 0, 0, 0,
            0, 0, 0, 0, // log[5] string len = 89
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 99, 111, 110, 115, 117, 109,
            101, 100, 32, 53, 51, 50, 56, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111,
            109, 112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 59, 0, 0, 0, 0, 0, 0,
            0, // log[6] string len = 59
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 115, 117, 99, 99, 101, 115,
            115, // pre_token_balances: Option<Vec<TokenBalance>>
            1,   // Option::Some
            2, 0, 0, 0, 0, 0, 0, 0, // len = 2
            2, // token_balance[0].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[0].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[0].ui_token_amount: StoredTokenAmount
            236, 23, 236, 20, 137, 62, 95, 65, // ui_amount: f64 = 8_190_500.32691
            5,  // decimals: u8
            12, 0, 0, 0, 0, 0, 0, 0, // amount String len = 12
            56, 49, 57, 48, 53, 48, 48, 51, 50, 54, 57, 49, // "819050032691"
            3,  // token_balance[1].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[1].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[1].ui_token_amount: StoredTokenAmount
            0, 0, 0, 0, 0, 112, 167, 64, // ui_amount: f64 = 3000.0
            5,  // decimals: u8
            9, 0, 0, 0, 0, 0, 0, 0, // amount String len = 9
            51, 48, 48, 48, 48, 48, 48, 48, 48, // "300000000"
            // post_token_balances: Option<Vec<TokenBalance>>
            1, // Option::Some
            2, 0, 0, 0, 0, 0, 0, 0, // len = 2
            3, // token_balance[0].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[0].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[0].ui_token_amount: StoredTokenAmount
            0, 0, 0, 0, 0, 64, 159, 64, // ui_amount: f64 = 2000.0
            5,  // decimals: u8
            9, 0, 0, 0, 0, 0, 0, 0, // amount String len = 9
            50, 48, 48, 48, 48, 48, 48, 48, 48, // "200000000"
            2,  // token_balance[1].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[1].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[1].ui_token_amount: StoredTokenAmount
            236, 23, 236, 20, 131, 63, 95, 65, // ui_amount: f64 = 8_191_500.32691
            5,  // decimals: u8
            12, 0, 0, 0, 0, 0, 0, 0, // amount String len = 12
            56, 49, 57, 49, 53, 48, 48, 51, 50, 54, 57, 49, // "819150032691"
        ];

        // Any slot < BINCODE_EPOCH_CUTOFF * 432000 forces bincode path
        let slot = (BINCODE_EPOCH_CUTOFF - 1) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }
}

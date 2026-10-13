use std::mem::size_of;

use prost::Message;
use prost::encoding::{self, DecodeContext};

use crate::confirmed_block::{TokenBalance, TransactionStatusMeta};

const MAX_RETAINED_BYTES: usize = 1024 * 1024;
const MAX_LOG_STRINGS: usize = 512;
const MAX_TOKEN_BALANCES: usize = 128;

/// Bounded scratch storage for fully owned protobuf metadata.
///
/// Keep one workspace per decode worker. The 1 MiB limit includes the actual
/// capacity of both pool vectors and all String buffers stored in them. It does
/// not limit the decoded output. Buffers that do not fit are dropped.
/// Optional token amounts are cleared normally; their nested strings are not pooled.
#[derive(Debug)]
pub struct TransactionStatusMetaReuse {
    logs: Vec<String>,
    tokens: Vec<TokenBalance>,
    retained_string_bytes: usize,
}

impl Default for TransactionStatusMetaReuse {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionStatusMetaReuse {
    pub fn new() -> Self {
        let mut reuse = Self {
            logs: Vec::with_capacity(MAX_LOG_STRINGS),
            tokens: Vec::with_capacity(MAX_TOKEN_BALANCES),
            retained_string_bytes: 0,
        };
        // Count actual capacities even if an allocator reserves extra space.
        if reuse.retained_capacity_bytes() > MAX_RETAINED_BYTES {
            reuse.logs = Vec::new();
            reuse.tokens = Vec::new();
        }
        reuse
    }

    /// Allocation capacity retained by the scratch pools, including their vectors.
    pub fn retained_capacity_bytes(&self) -> usize {
        self.retained_string_bytes
            .saturating_add(self.logs.capacity().saturating_mul(size_of::<String>()))
            .saturating_add(
                self.tokens
                    .capacity()
                    .saturating_mul(size_of::<TokenBalance>()),
            )
    }

    pub(super) fn reset(&mut self, out: &mut TransactionStatusMeta) {
        for value in out.log_messages.drain(..).rev() {
            self.retain_log(value);
        }
        for value in out.pre_token_balances.drain(..).rev() {
            self.retain_token(value);
        }
        for value in out.post_token_balances.drain(..).rev() {
            self.retain_token(value);
        }
        out.clear();
    }

    fn retain_log(&mut self, mut value: String) {
        value.clear();
        let bytes = value.capacity();
        if bytes == 0
            || self.logs.len() >= MAX_LOG_STRINGS
            || self.logs.len() == self.logs.capacity()
            || bytes > MAX_RETAINED_BYTES.saturating_sub(self.retained_capacity_bytes())
        {
            return;
        }
        self.retained_string_bytes += bytes;
        self.logs.push(value);
    }

    fn retain_token(&mut self, mut value: TokenBalance) {
        // Prost clears the three direct strings in place and drops the optional
        // UiTokenAmount. An absent nested message must stay absent on the next merge.
        value.clear();
        let bytes = token_string_capacity(&value);
        if bytes == 0
            || self.tokens.len() >= MAX_TOKEN_BALANCES
            || self.tokens.len() == self.tokens.capacity()
            || bytes > MAX_RETAINED_BYTES.saturating_sub(self.retained_capacity_bytes())
        {
            return;
        }
        self.retained_string_bytes += bytes;
        self.tokens.push(value);
    }

    fn take_log(&mut self) -> String {
        match self.logs.pop() {
            Some(value) => {
                self.retained_string_bytes -= value.capacity();
                value
            }
            None => String::new(),
        }
    }

    fn take_token(&mut self) -> TokenBalance {
        match self.tokens.pop() {
            Some(value) => {
                self.retained_string_bytes -= token_string_capacity(&value);
                value
            }
            None => TokenBalance::default(),
        }
    }

    pub(super) fn merge_protobuf(
        &mut self,
        out: &mut TransactionStatusMeta,
        mut bytes: &[u8],
    ) -> Result<(), prost::DecodeError> {
        // Match Message::merge's key loop and recursion context. Only repeated
        // fields with pooled allocations differ; all wire parsing stays in prost.
        let ctx = DecodeContext::default();
        while !bytes.is_empty() {
            let (tag, wire_type) = encoding::decode_key(&mut bytes)?;
            match tag {
                6 => {
                    let mut value = self.take_log();
                    encoding::string::merge(wire_type, &mut value, &mut bytes, ctx.clone())
                        .map_err(|mut error| {
                            error.push("TransactionStatusMeta", "log_messages");
                            error
                        })?;
                    out.log_messages.push(value);
                }
                7 | 8 => {
                    let mut value = self.take_token();
                    encoding::message::merge(wire_type, &mut value, &mut bytes, ctx.clone())
                        .map_err(|mut error| {
                            error.push(
                                "TransactionStatusMeta",
                                if tag == 7 {
                                    "pre_token_balances"
                                } else {
                                    "post_token_balances"
                                },
                            );
                            error
                        })?;
                    if tag == 7 {
                        out.pre_token_balances.push(value);
                    } else {
                        out.post_token_balances.push(value);
                    }
                }
                _ => out.merge_field(tag, wire_type, &mut bytes, ctx.clone())?,
            }
        }
        Ok(())
    }
}

fn token_string_capacity(value: &TokenBalance) -> usize {
    value
        .mint
        .capacity()
        .saturating_add(value.owner.capacity())
        .saturating_add(value.program_id.capacity())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::confirmed_block::{
        InnerInstruction, InnerInstructions, ReturnData, TransactionError, UiTokenAmount,
    };
    use crate::metadata_decoder::{
        ZstdReusableDecoder, decode_transaction_status_meta_from_frame,
        decode_transaction_status_meta_from_frame_reusing, decode_transaction_status_meta_into,
        decode_transaction_status_meta_into_reusing,
    };

    const SLOT: u64 = 432_000_001;

    fn token() -> TokenBalance {
        TokenBalance {
            account_index: 7,
            mint: "mint with retained capacity".into(),
            owner: "owner with retained capacity".into(),
            program_id: "program with retained capacity".into(),
            ui_token_amount: Some(UiTokenAmount {
                ui_amount: 1.25,
                decimals: 2,
                amount: "125".into(),
                ui_amount_string: "1.25".into(),
            }),
        }
    }

    fn fixture() -> TransactionStatusMeta {
        TransactionStatusMeta {
            err: Some(TransactionError { err: vec![1, 2] }),
            fee: 5000,
            pre_balances: vec![10, 20],
            post_balances: vec![8, 19],
            log_messages: vec!["Program log: first".into(), "Program log: second".into()],
            pre_token_balances: vec![token()],
            post_token_balances: vec![token()],
            inner_instructions: vec![InnerInstructions {
                index: 2,
                instructions: vec![InnerInstruction {
                    program_id_index: 3,
                    accounts: vec![4, 5],
                    data: vec![6],
                    stack_height: Some(0),
                }],
            }],
            loaded_writable_addresses: vec![vec![1; 32]],
            loaded_readonly_addresses: vec![vec![2; 32]],
            return_data: Some(ReturnData {
                program_id: vec![3; 32],
                data: vec![4],
            }),
            compute_units_consumed: Some(0),
            cost_units: Some(17),
            ..Default::default()
        }
    }

    fn check_budget(reuse: &TransactionStatusMetaReuse) {
        let strings = reuse.logs.iter().map(String::capacity).sum::<usize>()
            + reuse
                .tokens
                .iter()
                .map(token_string_capacity)
                .sum::<usize>();
        assert_eq!(reuse.retained_string_bytes, strings);
        assert!(reuse.retained_capacity_bytes() <= MAX_RETAINED_BYTES);
        assert!(reuse.logs.len() <= MAX_LOG_STRINGS);
        assert!(reuse.tokens.len() <= MAX_TOKEN_BALANCES);
        assert!(reuse.logs.iter().all(String::is_empty));
        for token in &reuse.tokens {
            assert_eq!(token, &TokenBalance::default());
        }
    }

    fn compare_frame(
        slot: u64,
        bytes: &[u8],
        expected: &mut TransactionStatusMeta,
        actual: &mut TransactionStatusMeta,
        zstd: &mut (ZstdReusableDecoder, ZstdReusableDecoder),
        reuse: &mut TransactionStatusMetaReuse,
    ) {
        let before = decode_transaction_status_meta_from_frame(slot, bytes, expected, &mut zstd.0)
            .map_err(|error| error.to_string());
        let after = decode_transaction_status_meta_from_frame_reusing(
            slot,
            bytes,
            actual,
            &mut zstd.1,
            reuse,
        )
        .map_err(|error| error.to_string());
        assert_eq!(after, before);
        assert_eq!(actual, expected);
        check_budget(reuse);
    }

    #[test]
    fn owned_reuse_frame_sequences_match_original() {
        let rich = fixture().encode_to_vec();
        let sparse = TransactionStatusMeta {
            log_messages: vec![String::new()],
            pre_token_balances: vec![TokenBalance::default()],
            log_messages_none: true,
            inner_instructions_none: true,
            return_data_none: true,
            ..Default::default()
        }
        .encode_to_vec();
        let mut expected = fixture();
        let mut actual = expected.clone();
        let mut zstd = (ZstdReusableDecoder::new(), ZstdReusableDecoder::new());
        let mut reuse = TransactionStatusMetaReuse::new();
        for bytes in [&rich[..], &sparse, &[], &rich, &[], &sparse] {
            compare_frame(
                SLOT,
                bytes,
                &mut expected,
                &mut actual,
                &mut zstd,
                &mut reuse,
            );
        }
    }

    #[test]
    fn owned_reuse_into_keeps_merge_and_nested_message_semantics() {
        let mut bytes = Vec::new();
        encoding::string::encode(6, &"first".into(), &mut bytes);
        encoding::string::encode(6, &String::new(), &mut bytes);
        let mut balance = Vec::new();
        encoding::string::encode(2, &"old mint".into(), &mut balance);
        encoding::string::encode(2, &"last mint".into(), &mut balance);
        encoding::message::encode(
            3,
            &UiTokenAmount {
                amount: "123".into(),
                decimals: 2,
                ..Default::default()
            },
            &mut balance,
        );
        encoding::message::encode(
            3,
            &UiTokenAmount {
                ui_amount_string: "1.23".into(),
                ..Default::default()
            },
            &mut balance,
        );
        encoding::bytes::encode(7, &balance, &mut bytes);
        encoding::bytes::encode(8, &Vec::<u8>::new(), &mut bytes);
        // Unknown fields and groups still pass through generated prost code.
        encoding::uint64::encode(99, &42, &mut bytes);
        encoding::encode_key(100, encoding::WireType::StartGroup, &mut bytes);
        encoding::uint64::encode(101, &7, &mut bytes);
        encoding::encode_key(100, encoding::WireType::EndGroup, &mut bytes);

        let mut reuse = TransactionStatusMetaReuse::new();
        let mut warm = fixture();
        reuse.reset(&mut warm);
        let mut expected = fixture();
        let mut actual = expected.clone();
        for _ in 0..2 {
            decode_transaction_status_meta_into(SLOT, &bytes, &mut expected).unwrap();
            decode_transaction_status_meta_into_reusing(SLOT, &bytes, &mut actual, &mut reuse)
                .unwrap();
            assert_eq!(actual, expected);
            check_budget(&reuse);
        }
        let merged = actual.pre_token_balances.last().unwrap();
        assert_eq!(merged.mint, "last mint");
        assert_eq!(merged.ui_token_amount.as_ref().unwrap().amount, "123");
        assert_eq!(
            merged.ui_token_amount.as_ref().unwrap().ui_amount_string,
            "1.23"
        );
        assert_eq!(
            actual.post_token_balances.last().unwrap(),
            &TokenBalance::default()
        );
    }

    #[test]
    fn owned_reuse_errors_and_partial_outputs_match_original() {
        let valid = TransactionStatusMeta {
            log_messages: vec!["valid".into()],
            pre_token_balances: vec![token()],
            ..Default::default()
        }
        .encode_to_vec();
        let mut cases = vec![
            vec![0x32, 1, 0xff],          // invalid log UTF-8
            vec![0x3a, 3, 0x12, 1, 0xff], // invalid pre-token mint UTF-8
            vec![0x42, 3, 0x22, 1, 0xff], // invalid post-token owner UTF-8
            vec![0x30, 1],
            vec![0x38, 1],
            vec![0x40, 1], // wrong wire types
            vec![0],
            vec![0x80],
            vec![0x0c], // invalid tag, key, end group
        ];
        cases.extend((0..valid.len()).map(|end| valid[..end].to_vec()));
        // Exercise the same recursive unknown-field limit inside a pooled token.
        let mut nested = Vec::new();
        for _ in 0..110 {
            encoding::encode_key(99, encoding::WireType::StartGroup, &mut nested);
        }
        for _ in 0..110 {
            encoding::encode_key(99, encoding::WireType::EndGroup, &mut nested);
        }
        let mut recursive = Vec::new();
        encoding::bytes::encode(7, &nested, &mut recursive);
        cases.push(recursive);

        let mut reuse = TransactionStatusMetaReuse::new();
        for bytes in cases {
            let mut warm = fixture();
            reuse.reset(&mut warm);
            let mut expected = fixture();
            let mut actual = expected.clone();
            let before = decode_transaction_status_meta_into(SLOT, &bytes, &mut expected)
                .map_err(|error| error.to_string());
            let after =
                decode_transaction_status_meta_into_reusing(SLOT, &bytes, &mut actual, &mut reuse)
                    .map_err(|error| error.to_string());
            assert_eq!(after, before, "wire bytes: {bytes:?}");
            assert_eq!(actual, expected, "wire bytes: {bytes:?}");
            check_budget(&reuse);
            let mut zstd = (ZstdReusableDecoder::new(), ZstdReusableDecoder::new());
            compare_frame(
                SLOT,
                &bytes,
                &mut expected,
                &mut actual,
                &mut zstd,
                &mut reuse,
            );
        }
    }

    #[test]
    fn owned_reuse_retains_direct_string_allocations() {
        let mut out = TransactionStatusMeta {
            log_messages: vec!["Program log: reusable".into()],
            pre_token_balances: vec![token()],
            ..Default::default()
        };
        let bytes = out.encode_to_vec();
        let pointers = (
            out.log_messages[0].as_ptr(),
            out.pre_token_balances[0].mint.as_ptr(),
            out.pre_token_balances[0].owner.as_ptr(),
            out.pre_token_balances[0].program_id.as_ptr(),
        );
        let mut reuse = TransactionStatusMetaReuse::new();
        let mut zstd = ZstdReusableDecoder::new();
        decode_transaction_status_meta_from_frame_reusing(
            SLOT, &bytes, &mut out, &mut zstd, &mut reuse,
        )
        .unwrap();
        assert_eq!(
            pointers,
            (
                out.log_messages[0].as_ptr(),
                out.pre_token_balances[0].mint.as_ptr(),
                out.pre_token_balances[0].owner.as_ptr(),
                out.pre_token_balances[0].program_id.as_ptr(),
            )
        );
        assert_eq!(
            out,
            TransactionStatusMeta::decode(bytes.as_slice()).unwrap()
        );
        check_budget(&reuse);
    }

    #[test]
    fn owned_reuse_bounds_scratch_without_limiting_output() {
        let mut reuse = TransactionStatusMetaReuse::new();
        let header_bytes = reuse.retained_capacity_bytes();
        assert_eq!(
            header_bytes,
            reuse.logs.capacity() * size_of::<String>()
                + reuse.tokens.capacity() * size_of::<TokenBalance>()
        );
        let mut count_limited = TransactionStatusMeta {
            log_messages: (0..MAX_LOG_STRINGS + 3).map(|_| "x".into()).collect(),
            pre_token_balances: (0..MAX_TOKEN_BALANCES + 3).map(|_| token()).collect(),
            ..Default::default()
        };
        reuse.reset(&mut count_limited);
        assert_eq!(reuse.logs.len(), MAX_LOG_STRINGS);
        assert_eq!(reuse.tokens.len(), MAX_TOKEN_BALANCES);
        check_budget(&reuse);
        reuse = TransactionStatusMetaReuse::new();
        let mut out = TransactionStatusMeta {
            log_messages: (0..MAX_LOG_STRINGS + 3)
                .map(|_| "log".repeat(1024))
                .collect(),
            pre_token_balances: (0..MAX_TOKEN_BALANCES + 3).map(|_| token()).collect(),
            ..Default::default()
        };
        reuse.reset(&mut out);
        assert_eq!(out, TransactionStatusMeta::default());
        assert!(reuse.logs.len() < MAX_LOG_STRINGS);
        check_budget(&reuse);
        let large = TransactionStatusMeta {
            log_messages: vec!["x".repeat(MAX_RETAINED_BYTES + 1)],
            pre_token_balances: vec![TokenBalance {
                mint: "y".repeat(MAX_RETAINED_BYTES + 1),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes = large.encode_to_vec();
        let mut zstd = ZstdReusableDecoder::new();
        decode_transaction_status_meta_from_frame_reusing(
            SLOT, &bytes, &mut out, &mut zstd, &mut reuse,
        )
        .unwrap();
        assert_eq!(out, large);
        decode_transaction_status_meta_from_frame_reusing(
            SLOT,
            &[],
            &mut out,
            &mut zstd,
            &mut reuse,
        )
        .unwrap();
        check_budget(&reuse);
        assert!(
            reuse
                .logs
                .iter()
                .all(|value| value.capacity() <= MAX_RETAINED_BYTES)
        );
        assert!(
            reuse
                .tokens
                .iter()
                .all(|value| token_string_capacity(value) <= MAX_RETAINED_BYTES)
        );
    }

    #[test]
    fn owned_reuse_legacy_frames_keep_replacement_behavior() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes()); // status Ok
        bytes.extend_from_slice(&10_000u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes()); // empty pre balances
        bytes.extend_from_slice(&0u64.to_le_bytes()); // empty post balances
        // Later optional fields can be absent at EOF in both legacy layouts.
        let mut reuse = TransactionStatusMetaReuse::new();
        let mut zstd = (ZstdReusableDecoder::new(), ZstdReusableDecoder::new());
        for slot in [1, 67_681_336] {
            let mut expected = fixture();
            let mut actual = expected.clone();
            decode_transaction_status_meta_into(slot, &bytes, &mut expected).unwrap();
            decode_transaction_status_meta_into_reusing(slot, &bytes, &mut actual, &mut reuse)
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.fee, 10_000);
            compare_frame(
                slot,
                &bytes,
                &mut expected,
                &mut actual,
                &mut zstd,
                &mut reuse,
            );
            compare_frame(slot, &[], &mut expected, &mut actual, &mut zstd, &mut reuse);
        }
    }

    #[cfg(feature = "zstd-native")]
    #[test]
    fn owned_reuse_compressed_frames_and_decompression_errors_match_original() {
        let compressed = zstd::bulk::compress(&fixture().encode_to_vec(), 1).unwrap();
        let empty = zstd::bulk::compress(&[], 1).unwrap();
        let mut expected = fixture();
        let mut actual = expected.clone();
        let mut zstd = (ZstdReusableDecoder::new(), ZstdReusableDecoder::new());
        let mut reuse = TransactionStatusMetaReuse::new();
        for bytes in [
            &compressed[..],
            &empty,
            &[0x28, 0xb5, 0x2f, 0xfd],
            &compressed,
        ] {
            compare_frame(
                SLOT,
                bytes,
                &mut expected,
                &mut actual,
                &mut zstd,
                &mut reuse,
            );
        }
    }
}

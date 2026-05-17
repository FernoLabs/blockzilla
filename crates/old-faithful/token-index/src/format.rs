use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::sync::OnceLock;

const TOKEN_PROGRAM_ID_STR: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID_STR: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub const TX_RECORD_BYTES: usize = 108;
pub const NO_TX_INDEX: u64 = u64::MAX;
pub const OUTER_INSTRUCTION_SENTINEL: u32 = u32::MAX;
pub const NO_STACK_HEIGHT: u32 = u32::MAX;

pub const TX_FLAG_HAS_ERROR: u8 = 1 << 0;
pub const TX_FLAG_VERSIONED: u8 = 1 << 1;

pub const INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED: u8 = 1 << 0;

pub const PROGRAM_MASK_TOKEN: u8 = 1 << 0;
pub const PROGRAM_MASK_TOKEN_2022: u8 = 1 << 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub format_version: u32,
    pub transaction_record_bytes: usize,
    pub car_path: String,
    pub compressed: bool,
    pub offset_kind: String,
    pub scanned_txs: usize,
    pub skipped_txs: usize,
    pub skipped_tx_data_continuations: usize,
    pub skipped_metadata_continuations: usize,
    pub tx_decode_failures: usize,
    pub missing_metadata_txs: usize,
    pub metadata_decode_failures: usize,
    pub transactions_with_unresolved_program_ids: usize,
    pub instructions_with_unresolved_program_ids: usize,
    pub instructions_with_unresolved_accounts: usize,
    pub matched_txs: usize,
    pub failed_matched_txs: usize,
    pub matched_instructions: usize,
    pub token_instructions: usize,
    pub token_2022_instructions: usize,
    pub transactions_file_bytes: u64,
    pub instructions_file_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProgramKind {
    Token = 1,
    Token2022 = 2,
}

impl ProgramKind {
    pub fn from_program_pubkey(pubkey: [u8; 32]) -> Option<Self> {
        if pubkey == token_program_id() {
            Some(Self::Token)
        } else if pubkey == token_2022_program_id() {
            Some(Self::Token2022)
        } else {
            None
        }
    }

    pub const fn mask(self) -> u8 {
        match self {
            Self::Token => PROGRAM_MASK_TOKEN,
            Self::Token2022 => PROGRAM_MASK_TOKEN_2022,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Token),
            2 => Ok(Self::Token2022),
            _ => bail!("unknown program kind {}", value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRecord {
    pub signature: [u8; 64],
    pub slot: u64,
    pub tx_index: u64,
    pub car_offset: u64,
    pub car_size: u32,
    pub first_instruction_offset: u64,
    pub instruction_count: u32,
    pub flags: u8,
    pub program_mask: u8,
}

impl TransactionRecord {
    pub fn has_error(&self) -> bool {
        self.flags & TX_FLAG_HAS_ERROR != 0
    }

    pub fn is_versioned(&self) -> bool {
        self.flags & TX_FLAG_VERSIONED != 0
    }

    pub fn tx_index_option(&self) -> Option<u64> {
        (self.tx_index != NO_TX_INDEX).then_some(self.tx_index)
    }

    pub fn encode(&self) -> [u8; TX_RECORD_BYTES] {
        let mut out = [0u8; TX_RECORD_BYTES];
        out[0..64].copy_from_slice(&self.signature);
        out[64..72].copy_from_slice(&self.slot.to_le_bytes());
        out[72..80].copy_from_slice(&self.tx_index.to_le_bytes());
        out[80..88].copy_from_slice(&self.car_offset.to_le_bytes());
        out[88..92].copy_from_slice(&self.car_size.to_le_bytes());
        out[92..100].copy_from_slice(&self.first_instruction_offset.to_le_bytes());
        out[100..104].copy_from_slice(&self.instruction_count.to_le_bytes());
        out[104] = self.flags;
        out[105] = self.program_mask;
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != TX_RECORD_BYTES {
            bail!(
                "invalid transaction record size: expected {} bytes, got {}",
                TX_RECORD_BYTES,
                bytes.len()
            );
        }

        let mut signature = [0u8; 64];
        signature.copy_from_slice(&bytes[0..64]);

        Ok(Self {
            signature,
            slot: u64::from_le_bytes(bytes[64..72].try_into().unwrap()),
            tx_index: u64::from_le_bytes(bytes[72..80].try_into().unwrap()),
            car_offset: u64::from_le_bytes(bytes[80..88].try_into().unwrap()),
            car_size: u32::from_le_bytes(bytes[88..92].try_into().unwrap()),
            first_instruction_offset: u64::from_le_bytes(bytes[92..100].try_into().unwrap()),
            instruction_count: u32::from_le_bytes(bytes[100..104].try_into().unwrap()),
            flags: bytes[104],
            program_mask: bytes[105],
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionRecord {
    pub tx_ordinal: u32,
    pub outer_instruction_index: u32,
    pub inner_instruction_index: u32,
    pub stack_height: u32,
    pub program_kind: ProgramKind,
    pub flags: u8,
    pub raw_account_indices: Vec<u8>,
    pub account_pubkeys: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

impl InstructionRecord {
    pub fn is_inner(&self) -> bool {
        self.inner_instruction_index != OUTER_INSTRUCTION_SENTINEL
    }

    pub fn all_accounts_resolved(&self) -> bool {
        self.flags & INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED != 0
    }

    pub fn encoded_len(&self) -> usize {
        4 + 24
            + self.raw_account_indices.len()
            + (self.account_pubkeys.len() * 32)
            + self.data.len()
    }

    pub fn encode_into<W: Write>(&self, writer: &mut W) -> Result<usize> {
        if self.raw_account_indices.len() != self.account_pubkeys.len() {
            bail!(
                "instruction record has {} raw account indexes but {} pubkeys",
                self.raw_account_indices.len(),
                self.account_pubkeys.len()
            );
        }

        let account_count = u16::try_from(self.raw_account_indices.len())
            .context("instruction account count exceeds u16")?;
        let data_len =
            u32::try_from(self.data.len()).context("instruction data length exceeds u32")?;
        let payload_len = 24usize
            .checked_add(self.raw_account_indices.len())
            .and_then(|len| len.checked_add(self.account_pubkeys.len() * 32))
            .and_then(|len| len.checked_add(self.data.len()))
            .context("instruction record size overflow")?;
        let payload_len_u32 =
            u32::try_from(payload_len).context("instruction payload length exceeds u32")?;

        writer.write_all(&payload_len_u32.to_le_bytes())?;
        writer.write_all(&self.tx_ordinal.to_le_bytes())?;
        writer.write_all(&self.outer_instruction_index.to_le_bytes())?;
        writer.write_all(&self.inner_instruction_index.to_le_bytes())?;
        writer.write_all(&self.stack_height.to_le_bytes())?;
        writer.write_all(&[self.program_kind as u8])?;
        writer.write_all(&[self.flags])?;
        writer.write_all(&account_count.to_le_bytes())?;
        writer.write_all(&data_len.to_le_bytes())?;
        writer.write_all(&self.raw_account_indices)?;
        for pubkey in &self.account_pubkeys {
            writer.write_all(pubkey)?;
        }
        writer.write_all(&self.data)?;

        Ok(4 + payload_len)
    }

    pub fn decode_from_reader<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err.into()),
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;
        Ok(Some(Self::decode_from_payload(&payload)?))
    }

    pub fn decode_from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < 24 {
            bail!(
                "instruction payload too short: expected at least 24 bytes, got {}",
                payload.len()
            );
        }

        let tx_ordinal = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        let outer_instruction_index = u32::from_le_bytes(payload[4..8].try_into().unwrap());
        let inner_instruction_index = u32::from_le_bytes(payload[8..12].try_into().unwrap());
        let stack_height = u32::from_le_bytes(payload[12..16].try_into().unwrap());
        let program_kind = ProgramKind::from_u8(payload[16])?;
        let flags = payload[17];
        let account_count = u16::from_le_bytes(payload[18..20].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(payload[20..24].try_into().unwrap()) as usize;

        let indexes_end = 24usize
            .checked_add(account_count)
            .context("instruction indexes length overflow")?;
        let pubkeys_end = indexes_end
            .checked_add(account_count * 32)
            .context("instruction pubkeys length overflow")?;
        let data_end = pubkeys_end
            .checked_add(data_len)
            .context("instruction data length overflow")?;
        if data_end != payload.len() {
            bail!(
                "instruction payload length mismatch: header implies {} bytes, got {}",
                data_end,
                payload.len()
            );
        }

        let raw_account_indices = payload[24..indexes_end].to_vec();
        let mut account_pubkeys = Vec::with_capacity(account_count);
        for chunk in payload[indexes_end..pubkeys_end].chunks_exact(32) {
            let mut pubkey = [0u8; 32];
            pubkey.copy_from_slice(chunk);
            account_pubkeys.push(pubkey);
        }
        let data = payload[pubkeys_end..data_end].to_vec();

        Ok(Self {
            tx_ordinal,
            outer_instruction_index,
            inner_instruction_index,
            stack_height,
            program_kind,
            flags,
            raw_account_indices,
            account_pubkeys,
            data,
        })
    }
}

pub fn token_program_id() -> [u8; 32] {
    *TOKEN_PROGRAM_ID.get_or_init(|| decode_program_id(TOKEN_PROGRAM_ID_STR))
}

pub fn token_2022_program_id() -> [u8; 32] {
    *TOKEN_2022_PROGRAM_ID.get_or_init(|| decode_program_id(TOKEN_2022_PROGRAM_ID_STR))
}

static TOKEN_PROGRAM_ID: OnceLock<[u8; 32]> = OnceLock::new();
static TOKEN_2022_PROGRAM_ID: OnceLock<[u8; 32]> = OnceLock::new();

fn decode_program_id(value: &str) -> [u8; 32] {
    let bytes = bs58::decode(value)
        .into_vec()
        .unwrap_or_else(|err| panic!("decode program id {value}: {err}"));
    assert_eq!(bytes.len(), 32, "program id {value} must be 32 bytes");
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

#[cfg(test)]
mod tests {
    use super::{
        INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED, InstructionRecord, NO_STACK_HEIGHT, NO_TX_INDEX,
        OUTER_INSTRUCTION_SENTINEL, PROGRAM_MASK_TOKEN, ProgramKind, TX_FLAG_HAS_ERROR,
        TX_FLAG_VERSIONED, TX_RECORD_BYTES, TransactionRecord, token_2022_program_id,
        token_program_id,
    };

    #[test]
    fn transaction_record_round_trip() {
        let mut signature = [0u8; 64];
        for (index, byte) in signature.iter_mut().enumerate() {
            *byte = index as u8;
        }

        let record = TransactionRecord {
            signature,
            slot: 42,
            tx_index: NO_TX_INDEX,
            car_offset: 99,
            car_size: 123,
            first_instruction_offset: 456,
            instruction_count: 2,
            flags: TX_FLAG_HAS_ERROR | TX_FLAG_VERSIONED,
            program_mask: PROGRAM_MASK_TOKEN,
        };

        let encoded = record.clone().encode();
        assert_eq!(encoded.len(), TX_RECORD_BYTES);

        let decoded = TransactionRecord::decode(&encoded).expect("decode transaction record");
        assert_eq!(decoded, record);
        assert!(decoded.has_error());
        assert!(decoded.is_versioned());
        assert_eq!(decoded.tx_index_option(), None);
    }

    #[test]
    fn instruction_record_round_trip() {
        let record = InstructionRecord {
            tx_ordinal: 7,
            outer_instruction_index: 3,
            inner_instruction_index: OUTER_INSTRUCTION_SENTINEL,
            stack_height: NO_STACK_HEIGHT,
            program_kind: ProgramKind::Token2022,
            flags: INSTRUCTION_FLAG_ALL_ACCOUNTS_RESOLVED,
            raw_account_indices: vec![1, 4],
            account_pubkeys: vec![[11u8; 32], [22u8; 32]],
            data: vec![9, 8, 7, 6],
        };

        let mut encoded = Vec::new();
        let bytes_written = record
            .encode_into(&mut encoded)
            .expect("encode instruction record");
        assert_eq!(bytes_written, encoded.len());

        let decoded = InstructionRecord::decode_from_reader(&mut encoded.as_slice())
            .expect("decode instruction record")
            .expect("record should exist");
        assert_eq!(decoded, record);
        assert!(!decoded.is_inner());
        assert!(decoded.all_accounts_resolved());
    }

    #[test]
    fn decodes_token_program_ids() {
        assert_ne!(token_program_id(), [0u8; 32]);
        assert_ne!(token_2022_program_id(), [0u8; 32]);
    }
}

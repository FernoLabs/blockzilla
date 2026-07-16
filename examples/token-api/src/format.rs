use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

pub const FORMAT_VERSION: u32 = 1;
pub const META_FILE: &str = "meta.json";
pub const PUBKEYS_FILE: &str = "pubkeys.bin";
pub const TOKENS_FILE: &str = "tokens.bin";
pub const TOKEN_ACCOUNTS_FILE: &str = "token_accounts.bin";
pub const BALANCE_CHANGES_FILE: &str = "balance_changes.bin";
pub const SWAPS_FILE: &str = "swaps.bin";

pub const NO_ID: u32 = 0;
pub const TOKEN_FLAG_QUOTE: u8 = 1 << 0;
pub const BALANCE_FLAG_CHANGED: u8 = 1 << 0;
pub const BALANCE_FLAG_TX_ERROR: u8 = 1 << 1;
pub const SWAP_FLAG_TX_ERROR: u16 = 1 << 0;
pub const SWAP_FLAG_KNOWN_DEX: u16 = 1 << 1;
pub const SWAP_FLAG_QUOTE_IN: u16 = 1 << 2;
pub const SWAP_FLAG_QUOTE_OUT: u16 = 1 << 3;

pub const PUBKEY_RECORD_BYTES: usize = 36;
pub const TOKEN_INFO_RECORD_BYTES: usize = 72;
pub const TOKEN_ACCOUNT_RECORD_BYTES: usize = 48;
pub const BALANCE_CHANGE_RECORD_BYTES: usize = 64;
pub const SWAP_RECORD_BYTES: usize = 72;

pub const PUBKEYS_MAGIC: [u8; 8] = *b"BZTPK001";
pub const TOKENS_MAGIC: [u8; 8] = *b"BZTTK001";
pub const TOKEN_ACCOUNTS_MAGIC: [u8; 8] = *b"BZTAC001";
pub const BALANCE_CHANGES_MAGIC: [u8; 8] = *b"BZTBL001";
pub const SWAPS_MAGIC: [u8; 8] = *b"BZTSW001";

const HEADER_BYTES: usize = 24;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub format_version: u32,
    pub archive_input: String,
    pub archive_index: String,
    pub archive_registry: String,
    pub compressed_input: bool,
    pub raw_blocks: bool,
    pub quote_mints: Vec<String>,
    pub dex_programs: Vec<String>,
    pub blocks: u64,
    pub transactions: u64,
    pub metadata_decoded: u64,
    pub token_balance_rows: u64,
    pub token_accounts: u64,
    pub tokens: u64,
    pub swaps: u64,
    pub pubkeys: u64,
    pub skipped_raw_txs: u64,
    pub skipped_raw_metadata: u64,
    #[serde(default)]
    pub metadata_decode_errors: u64,
    pub skipped_missing_keys: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubkeyRecord {
    pub id: u32,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenInfoRecord {
    pub mint_id: u32,
    pub program_id: u32,
    pub decimals: u8,
    pub flags: u8,
    pub first_slot: u64,
    pub last_slot: u64,
    pub first_block_time: i64,
    pub last_block_time: i64,
    pub balance_change_count: u64,
    pub swap_count: u64,
    pub token_account_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenAccountRecord {
    pub account_id: u32,
    pub mint_id: u32,
    pub owner_id: u32,
    pub program_id: u32,
    pub first_slot: u64,
    pub last_slot: u64,
    pub first_block_time: i64,
    pub last_block_time: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBalanceChangeRecord {
    pub slot: u64,
    pub block_time: i64,
    pub tx_index: u32,
    pub block_id: u32,
    pub account_id: u32,
    pub mint_id: u32,
    pub owner_id: u32,
    pub program_id: u32,
    pub pre_amount: u64,
    pub post_amount: u64,
    pub decimals: u8,
    pub flags: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapRecord {
    pub slot: u64,
    pub block_time: i64,
    pub tx_index: u32,
    pub block_id: u32,
    pub signature_id: u32,
    pub owner_id: u32,
    pub dex_program_id: u32,
    pub in_mint_id: u32,
    pub out_mint_id: u32,
    pub in_decimals: u8,
    pub out_decimals: u8,
    pub flags: u16,
    pub amount_in: u64,
    pub amount_out: u64,
    pub price_micros: u64,
}

pub trait BinaryRecord: Sized {
    const MAGIC: [u8; 8];
    const RECORD_BYTES: usize;

    fn encode_into(&self, out: &mut [u8]);
    fn decode(bytes: &[u8]) -> Result<Self>;
}

impl BinaryRecord for PubkeyRecord {
    const MAGIC: [u8; 8] = PUBKEYS_MAGIC;
    const RECORD_BYTES: usize = PUBKEY_RECORD_BYTES;

    fn encode_into(&self, out: &mut [u8]) {
        put_u32(out, 0, self.id);
        out[4..36].copy_from_slice(&self.pubkey);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure_len(bytes, Self::RECORD_BYTES)?;
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&bytes[4..36]);
        Ok(Self {
            id: get_u32(bytes, 0),
            pubkey,
        })
    }
}

impl BinaryRecord for TokenInfoRecord {
    const MAGIC: [u8; 8] = TOKENS_MAGIC;
    const RECORD_BYTES: usize = TOKEN_INFO_RECORD_BYTES;

    fn encode_into(&self, out: &mut [u8]) {
        put_u32(out, 0, self.mint_id);
        put_u32(out, 4, self.program_id);
        out[8] = self.decimals;
        out[9] = self.flags;
        put_u64(out, 16, self.first_slot);
        put_u64(out, 24, self.last_slot);
        put_i64(out, 32, self.first_block_time);
        put_i64(out, 40, self.last_block_time);
        put_u64(out, 48, self.balance_change_count);
        put_u64(out, 56, self.swap_count);
        put_u64(out, 64, self.token_account_count);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure_len(bytes, Self::RECORD_BYTES)?;
        Ok(Self {
            mint_id: get_u32(bytes, 0),
            program_id: get_u32(bytes, 4),
            decimals: bytes[8],
            flags: bytes[9],
            first_slot: get_u64(bytes, 16),
            last_slot: get_u64(bytes, 24),
            first_block_time: get_i64(bytes, 32),
            last_block_time: get_i64(bytes, 40),
            balance_change_count: get_u64(bytes, 48),
            swap_count: get_u64(bytes, 56),
            token_account_count: get_u64(bytes, 64),
        })
    }
}

impl BinaryRecord for TokenAccountRecord {
    const MAGIC: [u8; 8] = TOKEN_ACCOUNTS_MAGIC;
    const RECORD_BYTES: usize = TOKEN_ACCOUNT_RECORD_BYTES;

    fn encode_into(&self, out: &mut [u8]) {
        put_u32(out, 0, self.account_id);
        put_u32(out, 4, self.mint_id);
        put_u32(out, 8, self.owner_id);
        put_u32(out, 12, self.program_id);
        put_u64(out, 16, self.first_slot);
        put_u64(out, 24, self.last_slot);
        put_i64(out, 32, self.first_block_time);
        put_i64(out, 40, self.last_block_time);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure_len(bytes, Self::RECORD_BYTES)?;
        Ok(Self {
            account_id: get_u32(bytes, 0),
            mint_id: get_u32(bytes, 4),
            owner_id: get_u32(bytes, 8),
            program_id: get_u32(bytes, 12),
            first_slot: get_u64(bytes, 16),
            last_slot: get_u64(bytes, 24),
            first_block_time: get_i64(bytes, 32),
            last_block_time: get_i64(bytes, 40),
        })
    }
}

impl BinaryRecord for TokenBalanceChangeRecord {
    const MAGIC: [u8; 8] = BALANCE_CHANGES_MAGIC;
    const RECORD_BYTES: usize = BALANCE_CHANGE_RECORD_BYTES;

    fn encode_into(&self, out: &mut [u8]) {
        put_u64(out, 0, self.slot);
        put_i64(out, 8, self.block_time);
        put_u32(out, 16, self.tx_index);
        put_u32(out, 20, self.block_id);
        put_u32(out, 24, self.account_id);
        put_u32(out, 28, self.mint_id);
        put_u32(out, 32, self.owner_id);
        put_u32(out, 36, self.program_id);
        put_u64(out, 40, self.pre_amount);
        put_u64(out, 48, self.post_amount);
        out[56] = self.decimals;
        out[57] = self.flags;
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure_len(bytes, Self::RECORD_BYTES)?;
        Ok(Self {
            slot: get_u64(bytes, 0),
            block_time: get_i64(bytes, 8),
            tx_index: get_u32(bytes, 16),
            block_id: get_u32(bytes, 20),
            account_id: get_u32(bytes, 24),
            mint_id: get_u32(bytes, 28),
            owner_id: get_u32(bytes, 32),
            program_id: get_u32(bytes, 36),
            pre_amount: get_u64(bytes, 40),
            post_amount: get_u64(bytes, 48),
            decimals: bytes[56],
            flags: bytes[57],
        })
    }
}

impl BinaryRecord for SwapRecord {
    const MAGIC: [u8; 8] = SWAPS_MAGIC;
    const RECORD_BYTES: usize = SWAP_RECORD_BYTES;

    fn encode_into(&self, out: &mut [u8]) {
        put_u64(out, 0, self.slot);
        put_i64(out, 8, self.block_time);
        put_u32(out, 16, self.tx_index);
        put_u32(out, 20, self.block_id);
        put_u32(out, 24, self.signature_id);
        put_u32(out, 28, self.owner_id);
        put_u32(out, 32, self.dex_program_id);
        put_u32(out, 36, self.in_mint_id);
        put_u32(out, 40, self.out_mint_id);
        out[44] = self.in_decimals;
        out[45] = self.out_decimals;
        put_u16(out, 46, self.flags);
        put_u64(out, 48, self.amount_in);
        put_u64(out, 56, self.amount_out);
        put_u64(out, 64, self.price_micros);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure_len(bytes, Self::RECORD_BYTES)?;
        Ok(Self {
            slot: get_u64(bytes, 0),
            block_time: get_i64(bytes, 8),
            tx_index: get_u32(bytes, 16),
            block_id: get_u32(bytes, 20),
            signature_id: get_u32(bytes, 24),
            owner_id: get_u32(bytes, 28),
            dex_program_id: get_u32(bytes, 32),
            in_mint_id: get_u32(bytes, 36),
            out_mint_id: get_u32(bytes, 40),
            in_decimals: bytes[44],
            out_decimals: bytes[45],
            flags: get_u16(bytes, 46),
            amount_in: get_u64(bytes, 48),
            amount_out: get_u64(bytes, 56),
            price_micros: get_u64(bytes, 64),
        })
    }
}

pub fn write_record_file<T: BinaryRecord>(path: &Path, rows: &[T]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 << 20, file);
    writer.write_all(&T::MAGIC)?;
    writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&(T::RECORD_BYTES as u32).to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    let mut row = vec![0u8; T::RECORD_BYTES];
    for item in rows {
        row.fill(0);
        item.encode_into(&mut row);
        writer.write_all(&row)?;
    }
    writer.flush()?;
    Ok(())
}

pub struct RecordFileWriter<T: BinaryRecord> {
    path: PathBuf,
    writer: BufWriter<File>,
    row: Vec<u8>,
    count: u64,
    _record: PhantomData<T>,
}

impl<T: BinaryRecord> RecordFileWriter<T> {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(8 << 20, file);
        writer.write_all(&T::MAGIC)?;
        writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
        writer.write_all(&(T::RECORD_BYTES as u32).to_le_bytes())?;
        writer.write_all(&0u64.to_le_bytes())?;
        Ok(Self {
            path: path.to_path_buf(),
            writer,
            row: vec![0u8; T::RECORD_BYTES],
            count: 0,
            _record: PhantomData,
        })
    }

    pub fn write(&mut self, item: &T) -> Result<()> {
        self.row.fill(0);
        item.encode_into(&mut self.row);
        self.writer
            .write_all(&self.row)
            .with_context(|| format!("write row {}", self.path.display()))?;
        self.count += 1;
        Ok(())
    }

    pub fn write_all(&mut self, rows: &[T]) -> Result<()> {
        for row in rows {
            self.write(row)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64> {
        self.writer
            .flush()
            .with_context(|| format!("flush {}", self.path.display()))?;
        let mut file = self
            .writer
            .into_inner()
            .map_err(|err| err.into_error())
            .with_context(|| format!("finish {}", self.path.display()))?;
        file.seek(SeekFrom::Start(16))
            .with_context(|| format!("seek row count {}", self.path.display()))?;
        file.write_all(&self.count.to_le_bytes())
            .with_context(|| format!("write row count {}", self.path.display()))?;
        file.flush()
            .with_context(|| format!("flush row count {}", self.path.display()))?;
        Ok(self.count)
    }
}

pub fn read_record_file<T: BinaryRecord>(path: &Path) -> Result<Vec<T>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut header = [0u8; HEADER_BYTES];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("read header {}", path.display()))?;
    if header[0..8] != T::MAGIC {
        bail!("{} has invalid magic", path.display());
    }
    let version = get_u32(&header, 8);
    if version != FORMAT_VERSION {
        bail!(
            "{} has unsupported format version {}",
            path.display(),
            version
        );
    }
    let row_size = get_u32(&header, 12) as usize;
    if row_size != T::RECORD_BYTES {
        bail!(
            "{} has row size {}, expected {}",
            path.display(),
            row_size,
            T::RECORD_BYTES
        );
    }
    let count = usize::try_from(get_u64(&header, 16)).context("row count exceeds usize")?;
    let mut rows = Vec::with_capacity(count);
    let mut row = vec![0u8; T::RECORD_BYTES];
    for _ in 0..count {
        reader
            .read_exact(&mut row)
            .with_context(|| format!("read row {}", path.display()))?;
        rows.push(T::decode(&row)?);
    }
    Ok(rows)
}

#[inline]
pub fn encode_pubkey(pubkey: &[u8; 32]) -> String {
    bs58::encode(pubkey).into_string()
}

#[inline]
pub fn decode_pubkey(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode pubkey {value}"))?;
    if bytes.len() != 32 {
        bail!(
            "pubkey {value} decoded to {} bytes, expected 32",
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[inline]
pub fn amount_to_ui(amount: u64, decimals: u8) -> f64 {
    if decimals == 0 {
        amount as f64
    } else {
        amount as f64 / 10f64.powi(decimals as i32)
    }
}

#[inline]
pub fn price_micros(
    amount_base: u64,
    base_decimals: u8,
    amount_quote: u64,
    quote_decimals: u8,
) -> u64 {
    let base = amount_to_ui(amount_base, base_decimals);
    let quote = amount_to_ui(amount_quote, quote_decimals);
    if base <= 0.0 || quote <= 0.0 {
        return 0;
    }
    (quote / base * 1_000_000.0)
        .round()
        .clamp(0.0, u64::MAX as f64) as u64
}

fn ensure_len(bytes: &[u8], expected: usize) -> Result<()> {
    if bytes.len() != expected {
        bail!("invalid record size {}, expected {}", bytes.len(), expected);
    }
    Ok(())
}

#[inline]
fn put_u16(out: &mut [u8], offset: usize, value: u16) {
    out[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_u32(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_u64(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_i64(out: &mut [u8], offset: usize, value: i64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("checked offset"),
    )
}

#[inline]
fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked offset"),
    )
}

#[inline]
fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked offset"),
    )
}

#[inline]
fn get_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked offset"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swap_record_roundtrips() {
        let record = SwapRecord {
            slot: 7,
            block_time: 8,
            tx_index: 9,
            block_id: 10,
            signature_id: 11,
            owner_id: 12,
            dex_program_id: 13,
            in_mint_id: 14,
            out_mint_id: 15,
            in_decimals: 6,
            out_decimals: 9,
            flags: 3,
            amount_in: 16,
            amount_out: 17,
            price_micros: 18,
        };
        let mut bytes = vec![0u8; SwapRecord::RECORD_BYTES];
        record.encode_into(&mut bytes);
        assert_eq!(SwapRecord::decode(&bytes).unwrap(), record);
    }
}

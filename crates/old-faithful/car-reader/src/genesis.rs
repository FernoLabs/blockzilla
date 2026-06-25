use crate::error::{CarReadError, CarReadResult};
use bzip2::read::BzDecoder;
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    fs::File,
    io::{self, BufReader, Cursor, Read},
    path::Path,
};

const MAX_GENESIS_BIN_BYTES: usize = 10_000_001;
pub const MAINNET_GENESIS_URL: &str = "https://api.mainnet-beta.solana.com/genesis.tar.bz2";

#[derive(Debug, Clone)]
pub struct GenesisArchive {
    pub archive_entries: Vec<GenesisArchiveEntry>,
    pub genesis_bin_len: usize,
    pub genesis_hash: [u8; 32],
    pub genesis: GenesisConfig,
}

#[derive(Debug, Clone)]
pub struct GenesisArchiveEntry {
    pub path: String,
    pub size: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Clone)]
pub struct GenesisConfig {
    pub creation_time_unix: i64,
    pub accounts: Vec<GenesisAccountEntry>,
    pub builtins: Vec<BuiltinProgram>,
    pub reward_pools: Vec<GenesisAccountEntry>,
    pub ticks_per_slot: u64,
    pub poh_params: PohParams,
    pub fees: FeeParams,
    pub rent: RentParams,
    pub inflation: InflationParams,
    pub epoch_schedule: EpochSchedule,
    pub cluster_id: u32,
}

#[derive(Debug, Clone)]
pub struct GenesisAccountEntry {
    pub pubkey: [u8; 32],
    pub account: GenesisAccount,
}

#[derive(Debug, Clone)]
pub struct GenesisAccount {
    pub lamports: u64,
    pub data: Vec<u8>,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
}

#[derive(Debug, Clone)]
pub struct BuiltinProgram {
    pub key: String,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct PohParams {
    pub tick_duration_secs: u64,
    pub tick_duration_nanos: u32,
    pub tick_count: Option<u64>,
    pub hashes_per_tick: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct FeeParams {
    pub target_lamports_per_sig: u64,
    pub target_sigs_per_slot: u64,
    pub min_lamports_per_sig: u64,
    pub max_lamports_per_sig: u64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone)]
pub struct RentParams {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: f64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone)]
pub struct InflationParams {
    pub initial: f64,
    pub terminal: f64,
    pub taper: f64,
    pub foundation: f64,
    pub foundation_term: f64,
    pub padding: [u8; 8],
}

#[derive(Debug, Clone)]
pub struct EpochSchedule {
    pub slots_per_epoch: u64,
    pub leader_schedule_slot_offset: u64,
    pub warmup: bool,
    pub first_normal_epoch: u64,
    pub first_normal_slot: u64,
}

#[derive(Debug, Clone, Default)]
pub struct GenesisAccountStats {
    pub count: usize,
    pub total_lamports: u128,
    pub total_data_bytes: u64,
    pub executable_accounts: usize,
}

impl GenesisConfig {
    pub fn account_stats(&self) -> GenesisAccountStats {
        account_stats(&self.accounts)
    }

    pub fn reward_pool_stats(&self) -> GenesisAccountStats {
        account_stats(&self.reward_pools)
    }
}

pub fn read_genesis_archive_from_file(path: impl AsRef<Path>) -> CarReadResult<GenesisArchive> {
    let file = File::open(path.as_ref()).map_err(|err| {
        CarReadError::Io(format!(
            "open genesis archive {}: {err}",
            path.as_ref().display()
        ))
    })?;
    read_genesis_archive(file)
}

pub fn read_genesis_archive(reader: impl Read) -> CarReadResult<GenesisArchive> {
    let mut archive_bytes = Vec::new();
    let mut reader = BufReader::new(reader);
    reader
        .read_to_end(&mut archive_bytes)
        .map_err(|err| CarReadError::Io(format!("read genesis archive: {err}")))?;

    let tar_reader: Box<dyn Read> = if archive_bytes.starts_with(b"BZ") {
        Box::new(BzDecoder::new(Cursor::new(archive_bytes)))
    } else {
        Box::new(Cursor::new(archive_bytes))
    };

    let mut archive = tar::Archive::new(tar_reader);
    let mut entries = Vec::new();
    let mut genesis_bytes = None;

    let archive_entries = archive
        .entries()
        .map_err(|err| CarReadError::InvalidData(format!("open genesis tar: {err}")))?;

    for entry in archive_entries {
        let mut entry =
            entry.map_err(|err| CarReadError::InvalidData(format!("read tar entry: {err}")))?;
        let path = entry
            .path()
            .map_err(|err| CarReadError::InvalidData(format!("read tar entry path: {err}")))?
            .to_string_lossy()
            .into_owned();
        let size = entry.size();

        let bytes_read = if path == "genesis.bin" {
            let mut bytes = Vec::new();
            let bytes_read = entry
                .read_to_end(&mut bytes)
                .map_err(|err| CarReadError::Io(format!("read genesis.bin: {err}")))?
                as u64;
            genesis_bytes = Some(bytes);
            bytes_read
        } else {
            io::copy(&mut entry, &mut io::sink())
                .map_err(|err| CarReadError::Io(format!("read tar entry {path}: {err}")))?
        };

        entries.push(GenesisArchiveEntry {
            path,
            size,
            bytes_read,
        });
    }

    if entries.first().map(|entry| entry.path.as_str()) != Some("genesis.bin") {
        return Err(CarReadError::InvalidData(
            "first file in genesis archive is not genesis.bin".to_string(),
        ));
    }

    let genesis_bytes = genesis_bytes
        .ok_or_else(|| CarReadError::InvalidData("genesis archive missing genesis.bin".into()))?;
    if genesis_bytes.len() >= MAX_GENESIS_BIN_BYTES {
        return Err(CarReadError::InvalidData(format!(
            "genesis.bin too large: {} bytes",
            genesis_bytes.len()
        )));
    }

    let hash = Sha256::digest(&genesis_bytes);
    let mut genesis_hash = [0u8; 32];
    genesis_hash.copy_from_slice(&hash);
    let genesis = parse_genesis_bin(&genesis_bytes)?;

    Ok(GenesisArchive {
        archive_entries: entries,
        genesis_bin_len: genesis_bytes.len(),
        genesis_hash,
        genesis,
    })
}

pub fn parse_genesis_bin(bytes: &[u8]) -> CarReadResult<GenesisConfig> {
    let mut reader = BinReader::new(bytes);
    let genesis = GenesisConfig {
        creation_time_unix: reader.i64()?,
        accounts: read_account_entries(&mut reader, "accounts")?,
        builtins: read_builtins(&mut reader)?,
        reward_pools: read_account_entries(&mut reader, "reward pools")?,
        ticks_per_slot: reader.u64()?,
        poh_params: {
            let _padding = reader.u64()?;
            read_poh_params(&mut reader)?
        },
        fees: {
            let _padding = reader.u64()?;
            read_fee_params(&mut reader)?
        },
        rent: read_rent_params(&mut reader)?,
        inflation: read_inflation_params(&mut reader)?,
        epoch_schedule: read_epoch_schedule(&mut reader)?,
        cluster_id: reader.u32()?,
    };
    if reader.remaining() != 0 {
        return Err(CarReadError::InvalidData(format!(
            "not all of genesis.bin was read ({} bytes remaining)",
            reader.remaining()
        )));
    }
    Ok(genesis)
}

pub fn pubkey_to_base58(pubkey: &[u8; 32]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    let zeroes = pubkey.iter().take_while(|byte| **byte == 0).count();
    let mut digits = Vec::<u8>::with_capacity(45);

    for byte in pubkey {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            carry += (*digit as u32) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.insert(0, (carry % 58) as u8);
            carry /= 58;
        }
    }

    let mut out = String::with_capacity(zeroes + digits.len());
    out.extend(std::iter::repeat_n('1', zeroes));
    out.extend(
        digits
            .into_iter()
            .map(|digit| ALPHABET[digit as usize] as char),
    );
    out
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn account_stats(accounts: &[GenesisAccountEntry]) -> GenesisAccountStats {
    let mut stats = GenesisAccountStats {
        count: accounts.len(),
        ..GenesisAccountStats::default()
    };

    for entry in accounts {
        stats.total_lamports += entry.account.lamports as u128;
        stats.total_data_bytes += entry.account.data.len() as u64;
        if entry.account.executable {
            stats.executable_accounts += 1;
        }
    }

    stats
}

fn read_account_entries(
    reader: &mut BinReader<'_>,
    label: &'static str,
) -> CarReadResult<Vec<GenesisAccountEntry>> {
    let len = reader.len_u64(label)?;
    let mut out = Vec::with_capacity(len);
    for index in 0..len {
        out.push(GenesisAccountEntry {
            pubkey: reader.array_32()?,
            account: GenesisAccount {
                lamports: reader.u64()?,
                data: reader.byte_vec(&format!("{label}[{index}].data"))?,
                owner: reader.array_32()?,
                executable: reader.bool()?,
                rent_epoch: reader.u64()?,
            },
        });
    }
    Ok(out)
}

fn read_builtins(reader: &mut BinReader<'_>) -> CarReadResult<Vec<BuiltinProgram>> {
    let len = reader.len_u64("builtins")?;
    let mut out = Vec::with_capacity(len);
    for index in 0..len {
        let key = reader.string(&format!("builtins[{index}].key"))?;
        out.push(BuiltinProgram {
            key,
            pubkey: reader.array_32()?,
        });
    }
    Ok(out)
}

fn read_poh_params(reader: &mut BinReader<'_>) -> CarReadResult<PohParams> {
    let tick_duration_secs = reader.u64()?;
    let tick_duration_nanos = reader.u32()?;
    if tick_duration_nanos >= 1_000_000_000 {
        return Err(CarReadError::InvalidData(format!(
            "malformed PoH tick duration nanos: {tick_duration_nanos}"
        )));
    }

    let tick_count = if reader.bool()? {
        Some(reader.u64()?)
    } else {
        None
    };
    let hashes_per_tick = if reader.bool()? {
        Some(reader.u64()?)
    } else {
        None
    };

    Ok(PohParams {
        tick_duration_secs,
        tick_duration_nanos,
        tick_count,
        hashes_per_tick,
    })
}

fn read_fee_params(reader: &mut BinReader<'_>) -> CarReadResult<FeeParams> {
    Ok(FeeParams {
        target_lamports_per_sig: reader.u64()?,
        target_sigs_per_slot: reader.u64()?,
        min_lamports_per_sig: reader.u64()?,
        max_lamports_per_sig: reader.u64()?,
        burn_percent: reader.u8()?,
    })
}

fn read_rent_params(reader: &mut BinReader<'_>) -> CarReadResult<RentParams> {
    Ok(RentParams {
        lamports_per_byte_year: reader.u64()?,
        exemption_threshold: reader.f64()?,
        burn_percent: reader.u8()?,
    })
}

fn read_inflation_params(reader: &mut BinReader<'_>) -> CarReadResult<InflationParams> {
    Ok(InflationParams {
        initial: reader.f64()?,
        terminal: reader.f64()?,
        taper: reader.f64()?,
        foundation: reader.f64()?,
        foundation_term: reader.f64()?,
        padding: reader.array_8()?,
    })
}

fn read_epoch_schedule(reader: &mut BinReader<'_>) -> CarReadResult<EpochSchedule> {
    Ok(EpochSchedule {
        slots_per_epoch: reader.u64()?,
        leader_schedule_slot_offset: reader.u64()?,
        warmup: reader.bool()?,
        first_normal_epoch: reader.u64()?,
        first_normal_slot: reader.u64()?,
    })
}

struct BinReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BinReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn take(&mut self, len: usize) -> CarReadResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| CarReadError::InvalidData("genesis offset overflow".to_string()))?;
        if end > self.data.len() {
            return Err(CarReadError::UnexpectedEof(format!(
                "wanted {len} bytes at offset {}, remaining {}",
                self.pos,
                self.remaining()
            )));
        }
        let bytes = &self.data[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> CarReadResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> CarReadResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(CarReadError::InvalidData(format!(
                "invalid bool value in genesis.bin: {value}"
            ))),
        }
    }

    fn u32(&mut self) -> CarReadResult<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> CarReadResult<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> CarReadResult<i64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(i64::from_le_bytes(bytes))
    }

    fn f64(&mut self) -> CarReadResult<f64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(f64::from_le_bytes(bytes))
    }

    fn array_8(&mut self) -> CarReadResult<[u8; 8]> {
        Ok(self.take(8)?.try_into().unwrap())
    }

    fn array_32(&mut self) -> CarReadResult<[u8; 32]> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn len_u64(&mut self, label: &str) -> CarReadResult<usize> {
        let len = self.u64()?;
        usize::try_from(len)
            .map_err(|_| CarReadError::InvalidData(format!("{label} length exceeds usize: {len}")))
    }

    fn byte_vec(&mut self, label: &str) -> CarReadResult<Vec<u8>> {
        let len = self.len_u64(label)?;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self, label: &str) -> CarReadResult<String> {
        let bytes = self.byte_vec(label)?;
        String::from_utf8(bytes)
            .map_err(|err| CarReadError::InvalidData(format!("{label} is not UTF-8: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tar::{Builder, Header};

    #[test]
    fn parses_minimal_uncompressed_genesis_tar() {
        let genesis_bin = minimal_genesis_bin();
        let mut tar = Vec::new();
        {
            let mut builder = Builder::new(&mut tar);
            let mut header = Header::new_gnu();
            header.set_size(genesis_bin.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, "genesis.bin", genesis_bin.as_slice())
                .unwrap();
            let extra = b"hello";
            let mut header = Header::new_gnu();
            header.set_size(extra.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, "rocksdb/CURRENT", &extra[..])
                .unwrap();
            builder.finish().unwrap();
        }

        let archive = read_genesis_archive(Cursor::new(tar)).expect("read genesis archive");
        assert_eq!(archive.archive_entries.len(), 2);
        assert_eq!(archive.genesis_bin_len, genesis_bin.len());
        assert_eq!(archive.genesis.creation_time_unix, 1_584_368_940);
        assert_eq!(archive.genesis.accounts.len(), 1);
        assert_eq!(archive.genesis.accounts[0].account.lamports, 42);
        assert_eq!(archive.genesis.accounts[0].account.data, vec![1, 2, 3]);
        assert_eq!(archive.genesis.builtins[0].key, "solana_system_program");
        assert_eq!(archive.genesis.ticks_per_slot, 64);
        assert_eq!(archive.genesis.poh_params.hashes_per_tick, Some(12_500));
        assert_eq!(archive.genesis.cluster_id, 1);
    }

    #[test]
    fn formats_pubkeys_as_base58() {
        assert_eq!(
            pubkey_to_base58(&[0; 32]),
            "11111111111111111111111111111111"
        );
        assert_eq!(
            pubkey_to_base58(&[1; 32]),
            "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"
        );
    }

    fn minimal_genesis_bin() -> Vec<u8> {
        let mut out = Vec::new();
        put_i64(&mut out, 1_584_368_940);
        put_u64(&mut out, 1);
        out.extend_from_slice(&[1; 32]);
        put_u64(&mut out, 42);
        put_u64(&mut out, 3);
        out.extend_from_slice(&[1, 2, 3]);
        out.extend_from_slice(&[2; 32]);
        out.push(1);
        put_u64(&mut out, 99);

        put_u64(&mut out, 1);
        let name = b"solana_system_program";
        put_u64(&mut out, name.len() as u64);
        out.extend_from_slice(name);
        out.extend_from_slice(&[3; 32]);

        put_u64(&mut out, 0);
        put_u64(&mut out, 64);
        put_u64(&mut out, 0);
        put_u64(&mut out, 0);
        put_u32(&mut out, 6_250_000);
        out.push(0);
        out.push(1);
        put_u64(&mut out, 12_500);
        put_u64(&mut out, 0);

        put_u64(&mut out, 10_000);
        put_u64(&mut out, 20_000);
        put_u64(&mut out, 5_000);
        put_u64(&mut out, 100_000);
        out.push(100);

        put_u64(&mut out, 3_480);
        put_f64(&mut out, 2.0);
        out.push(100);

        for value in [0.0, 0.0, 0.0, 0.0, 0.0] {
            put_f64(&mut out, value);
        }
        out.extend_from_slice(&[0; 8]);

        put_u64(&mut out, 432_000);
        put_u64(&mut out, 432_000);
        out.push(0);
        put_u64(&mut out, 0);
        put_u64(&mut out, 0);
        put_u32(&mut out, 1);
        out
    }

    fn put_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn put_u64(out: &mut Vec<u8>, value: u64) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn put_i64(out: &mut Vec<u8>, value: i64) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn put_f64(out: &mut Vec<u8>, value: f64) {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

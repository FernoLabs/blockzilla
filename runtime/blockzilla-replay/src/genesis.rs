use sha2::Digest;
use std::{
    fmt::Write as _,
    fs::File,
    io::{self, BufReader, Cursor, Read},
    path::Path,
};
use thiserror::Error;

use bzip2::read::BzDecoder;
use tar::Archive;

const MAX_GENESIS_BIN_BYTES: usize = 10_000_001;

pub const MAINNET_BETA_GENESIS_HASH_BASE58: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const MINIMUM_SLOTS_PER_EPOCH: u64 = 32;

#[derive(Debug, Clone)]
pub struct GenesisArchive {
    pub archive_entries: Vec<GenesisArchiveEntry>,
    pub genesis_bin_len: usize,
    pub genesis_hash: [u8; 32],
    pub genesis_bin: Vec<u8>,
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
    pub builtins: Vec<GenesisBuiltin>,
    pub reward_pools: Vec<GenesisAccountEntry>,
    pub ticks_per_slot: u64,
    pub slots_per_segment: u64,
    pub poh_params: GenesisPohParams,
    pub backwards_compat_with_v0_23: u64,
    pub fees: GenesisFeeParams,
    pub rent: GenesisRentParams,
    pub inflation: GenesisInflationParams,
    pub epoch_schedule: GenesisEpochSchedule,
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
pub struct GenesisBuiltin {
    pub key: String,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct GenesisPohParams {
    pub tick_duration_secs: u64,
    pub tick_duration_nanos: u32,
    pub tick_count: Option<u64>,
    pub hashes_per_tick: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct GenesisFeeParams {
    pub target_lamports_per_sig: u64,
    pub target_sigs_per_slot: u64,
    pub min_lamports_per_sig: u64,
    pub max_lamports_per_sig: u64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone)]
pub struct GenesisRentParams {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: f64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone)]
pub struct GenesisInflationParams {
    pub initial: f64,
    pub terminal: f64,
    pub taper: f64,
    pub foundation: f64,
    pub foundation_term: f64,
    pub storage: f64,
}

#[derive(Debug, Clone)]
pub struct GenesisEpochSchedule {
    pub slots_per_epoch: u64,
    pub leader_schedule_slot_offset: u64,
    pub warmup: bool,
    pub first_normal_epoch: u64,
    pub first_normal_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenesisBuiltinSummary {
    pub name: String,
    pub pubkey_base58: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochWindow {
    pub epoch: u64,
    pub first_slot: u64,
    pub end_slot_exclusive: u64,
    pub slots: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenesisFeeSummary {
    pub target_lamports_per_signature: u64,
    pub target_signatures_per_slot: u64,
    pub minimum_lamports_per_signature: u64,
    pub maximum_lamports_per_signature: u64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GenesisRentSummary {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: f64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GenesisInflationSummary {
    pub initial: f64,
    pub terminal: f64,
    pub taper: f64,
    pub foundation: f64,
    pub foundation_term: f64,
    pub storage: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenesisSummary {
    /// SHA-256 of uncompressed `genesis.bin`, which is Solana's cluster ID.
    pub genesis_hash_hex: String,
    pub genesis_hash_base58: String,
    pub genesis_bin_len: usize,
    pub creation_time_unix: i64,
    /// Historical `OperatingMode` enum discriminant. The shared CAR parser
    /// currently calls this legacy field `cluster_id`; mainnet value `1` is
    /// the Stable profile.
    pub operating_mode_discriminant: u32,
    pub account_count: usize,
    pub account_data_bytes: u64,
    pub executable_account_count: usize,
    pub capitalization_lamports: u128,
    pub reward_pool_count: usize,
    pub builtins: Vec<GenesisBuiltinSummary>,
    pub ticks_per_slot: u64,
    pub slots_per_segment: u64,
    pub tick_duration_seconds: u64,
    pub tick_duration_nanoseconds: u32,
    pub tick_count: Option<u64>,
    pub hashes_per_tick: Option<u64>,
    pub fees: GenesisFeeSummary,
    pub rent: GenesisRentSummary,
    pub inflation: GenesisInflationSummary,
    pub slots_per_epoch: u64,
    pub leader_schedule_slot_offset: u64,
    pub warmup: bool,
    pub first_normal_epoch: u64,
    pub first_normal_slot: u64,
    pub epoch_zero: EpochWindow,
    pub epoch_one: EpochWindow,
    pub is_mainnet_beta: bool,
}

#[derive(Debug, Error)]
pub enum GenesisSummaryError {
    #[error("read genesis archive: {0}")]
    Read(String),
    #[error("invalid epoch schedule in genesis")]
    InvalidEpochSchedule,
}

#[derive(Debug, Error)]
pub enum GenesisArchiveError {
    #[error("read genesis archive: {0}")]
    Read(String),
    #[error("invalid genesis archive: {0}")]
    Invalid(String),
}

pub fn read_genesis_summary(path: impl AsRef<Path>) -> Result<GenesisSummary, GenesisSummaryError> {
    let archive = read_genesis_archive_from_file(path)
        .map_err(|error| GenesisSummaryError::Read(error.to_string()))?;
    summarize_genesis(&archive)
}

pub fn read_genesis_archive_from_file(
    path: impl AsRef<Path>,
) -> Result<GenesisArchive, GenesisArchiveError> {
    let file = File::open(path.as_ref()).map_err(|err| {
        GenesisArchiveError::Read(format!(
            "open genesis archive {}: {err}",
            path.as_ref().display()
        ))
    })?;
    read_genesis_archive(file)
}

pub fn read_genesis_archive(reader: impl Read) -> Result<GenesisArchive, GenesisArchiveError> {
    let mut archive_bytes = Vec::new();
    let mut reader = BufReader::new(reader);
    reader
        .read_to_end(&mut archive_bytes)
        .map_err(|err| GenesisArchiveError::Read(format!("read genesis archive: {err}")))?;

    let tar_reader: Box<dyn Read> = if archive_bytes.starts_with(b"BZ") {
        Box::new(BzDecoder::new(Cursor::new(archive_bytes)))
    } else {
        Box::new(Cursor::new(archive_bytes))
    };

    let mut archive = Archive::new(tar_reader);
    let mut entries = Vec::new();
    let mut genesis_bytes = None;

    let archive_entries = archive
        .entries()
        .map_err(|err| GenesisArchiveError::Invalid(format!("open genesis tar: {err}")))?;

    for entry in archive_entries {
        let mut entry =
            entry.map_err(|err| GenesisArchiveError::Invalid(format!("read tar entry: {err}")))?;
        let path = entry
            .path()
            .map_err(|err| GenesisArchiveError::Invalid(format!("read tar entry path: {err}")))?
            .to_string_lossy()
            .into_owned();
        let size = entry.size();

        let bytes_read = if path == "genesis.bin" {
            let mut bytes = Vec::new();
            let bytes_read = entry
                .read_to_end(&mut bytes)
                .map_err(|err| GenesisArchiveError::Read(format!("read genesis.bin: {err}")))?
                as u64;
            genesis_bytes = Some(bytes);
            bytes_read
        } else {
            io::copy(&mut entry, &mut io::sink())
                .map_err(|err| GenesisArchiveError::Read(format!("read tar entry {path}: {err}")))?
        };

        entries.push(GenesisArchiveEntry {
            path,
            size,
            bytes_read,
        });
    }

    if entries.first().map(|entry| entry.path.as_str()) != Some("genesis.bin") {
        return Err(GenesisArchiveError::Invalid(
            "first file in genesis archive is not genesis.bin".to_string(),
        ));
    }

    let genesis_bytes = genesis_bytes.ok_or_else(|| {
        GenesisArchiveError::Invalid("genesis archive missing genesis.bin".into())
    })?;
    if genesis_bytes.len() >= MAX_GENESIS_BIN_BYTES {
        return Err(GenesisArchiveError::Invalid(format!(
            "genesis.bin too large: {} bytes",
            genesis_bytes.len()
        )));
    }

    let hash = sha2::Sha256::digest(&genesis_bytes);
    let mut genesis_hash = [0u8; 32];
    genesis_hash.copy_from_slice(&hash);
    let genesis = parse_genesis_bin(&genesis_bytes)?;

    Ok(GenesisArchive {
        archive_entries: entries,
        genesis_bin_len: genesis_bytes.len(),
        genesis_hash,
        genesis_bin: genesis_bytes,
        genesis,
    })
}

pub fn parse_genesis_bin(bytes: &[u8]) -> Result<GenesisConfig, GenesisArchiveError> {
    let mut reader = GenesisBinReader::new(bytes);
    let genesis = GenesisConfig {
        creation_time_unix: reader.i64()?,
        accounts: read_account_entries(&mut reader, "accounts")?,
        builtins: read_builtins(&mut reader)?,
        reward_pools: read_account_entries(&mut reader, "reward pools")?,
        ticks_per_slot: reader.u64()?,
        slots_per_segment: reader.u64()?,
        poh_params: read_poh_params(&mut reader)?,
        backwards_compat_with_v0_23: reader.u64()?,
        fees: read_fee_params(&mut reader)?,
        rent: read_rent_params(&mut reader)?,
        inflation: read_inflation_params(&mut reader)?,
        epoch_schedule: read_epoch_schedule(&mut reader)?,
        cluster_id: reader.u32()?,
    };
    if reader.remaining() != 0 {
        return Err(GenesisArchiveError::Invalid(format!(
            "not all of genesis.bin was read ({} bytes remaining)",
            reader.remaining()
        )));
    }
    Ok(genesis)
}

fn read_account_entries(
    reader: &mut GenesisBinReader<'_>,
    label: &'static str,
) -> Result<Vec<GenesisAccountEntry>, GenesisArchiveError> {
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

fn read_builtins(
    reader: &mut GenesisBinReader<'_>,
) -> Result<Vec<GenesisBuiltin>, GenesisArchiveError> {
    let len = reader.len_u64("builtins")?;
    let mut out = Vec::with_capacity(len);
    for index in 0..len {
        let key = reader.string(&format!("builtins[{index}].key"))?;
        out.push(GenesisBuiltin {
            key,
            pubkey: reader.array_32()?,
        });
    }
    Ok(out)
}

fn read_poh_params(
    reader: &mut GenesisBinReader<'_>,
) -> Result<GenesisPohParams, GenesisArchiveError> {
    let tick_duration_secs = reader.u64()?;
    let tick_duration_nanos = reader.u32()?;
    if tick_duration_nanos >= 1_000_000_000 {
        return Err(GenesisArchiveError::Invalid(format!(
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
    Ok(GenesisPohParams {
        tick_duration_secs,
        tick_duration_nanos,
        tick_count,
        hashes_per_tick,
    })
}

fn read_fee_params(
    reader: &mut GenesisBinReader<'_>,
) -> Result<GenesisFeeParams, GenesisArchiveError> {
    Ok(GenesisFeeParams {
        target_lamports_per_sig: reader.u64()?,
        target_sigs_per_slot: reader.u64()?,
        min_lamports_per_sig: reader.u64()?,
        max_lamports_per_sig: reader.u64()?,
        burn_percent: reader.u8()?,
    })
}

fn read_rent_params(
    reader: &mut GenesisBinReader<'_>,
) -> Result<GenesisRentParams, GenesisArchiveError> {
    Ok(GenesisRentParams {
        lamports_per_byte_year: reader.u64()?,
        exemption_threshold: reader.f64()?,
        burn_percent: reader.u8()?,
    })
}

fn read_inflation_params(
    reader: &mut GenesisBinReader<'_>,
) -> Result<GenesisInflationParams, GenesisArchiveError> {
    Ok(GenesisInflationParams {
        initial: reader.f64()?,
        terminal: reader.f64()?,
        taper: reader.f64()?,
        foundation: reader.f64()?,
        foundation_term: reader.f64()?,
        storage: reader.f64()?,
    })
}

fn read_epoch_schedule(
    reader: &mut GenesisBinReader<'_>,
) -> Result<GenesisEpochSchedule, GenesisArchiveError> {
    Ok(GenesisEpochSchedule {
        slots_per_epoch: reader.u64()?,
        leader_schedule_slot_offset: reader.u64()?,
        warmup: reader.bool()?,
        first_normal_epoch: reader.u64()?,
        first_normal_slot: reader.u64()?,
    })
}

struct GenesisBinReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> GenesisBinReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], GenesisArchiveError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| GenesisArchiveError::Invalid("genesis offset overflow".to_string()))?;
        if end > self.data.len() {
            return Err(GenesisArchiveError::Invalid(format!(
                "wanted {len} bytes at offset {}, remaining {}",
                self.pos,
                self.remaining()
            )));
        }
        let bytes = &self.data[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, GenesisArchiveError> {
        Ok(self.take(1)?[0])
    }

    fn bool(&mut self) -> Result<bool, GenesisArchiveError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(GenesisArchiveError::Invalid(format!(
                "invalid bool value in genesis.bin: {value}"
            ))),
        }
    }

    fn u32(&mut self) -> Result<u32, GenesisArchiveError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, GenesisArchiveError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64, GenesisArchiveError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(i64::from_le_bytes(bytes))
    }

    fn f64(&mut self) -> Result<f64, GenesisArchiveError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
        Ok(f64::from_le_bytes(bytes))
    }

    fn array_32(&mut self) -> Result<[u8; 32], GenesisArchiveError> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn len_u64(&mut self, label: &str) -> Result<usize, GenesisArchiveError> {
        let len = self.u64()?;
        usize::try_from(len).map_err(|_| {
            GenesisArchiveError::Invalid(format!("{label} length exceeds usize: {len}"))
        })
    }

    fn byte_vec(&mut self, label: &str) -> Result<Vec<u8>, GenesisArchiveError> {
        let len = self.len_u64(label)?;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self, label: &str) -> Result<String, GenesisArchiveError> {
        let bytes = self.byte_vec(label)?;
        String::from_utf8(bytes)
            .map_err(|err| GenesisArchiveError::Invalid(format!("{label} is not UTF-8: {err}")))
    }
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

pub fn summarize_genesis(archive: &GenesisArchive) -> Result<GenesisSummary, GenesisSummaryError> {
    let genesis = &archive.genesis;
    let accounts = genesis.account_stats();
    let reward_pools = genesis.reward_pool_stats();
    let epoch_zero = epoch_window(
        0,
        genesis.epoch_schedule.slots_per_epoch,
        genesis.epoch_schedule.warmup,
        genesis.epoch_schedule.first_normal_epoch,
        genesis.epoch_schedule.first_normal_slot,
    )?;
    let epoch_one = epoch_window(
        1,
        genesis.epoch_schedule.slots_per_epoch,
        genesis.epoch_schedule.warmup,
        genesis.epoch_schedule.first_normal_epoch,
        genesis.epoch_schedule.first_normal_slot,
    )?;
    let genesis_hash_base58 = pubkey_to_base58(&archive.genesis_hash);
    let is_mainnet_beta = genesis_hash_base58 == MAINNET_BETA_GENESIS_HASH_BASE58
        && archive.genesis_bin_len == 132_347
        && genesis.creation_time_unix == 1_584_368_940
        && genesis.cluster_id == 1
        && genesis.accounts.len() == 431
        && genesis.builtins.len() == 4
        && genesis.epoch_schedule.slots_per_epoch == 432_000
        && !genesis.epoch_schedule.warmup;
    Ok(GenesisSummary {
        genesis_hash_hex: bytes_to_hex(&archive.genesis_hash),
        genesis_hash_base58,
        genesis_bin_len: archive.genesis_bin_len,
        creation_time_unix: genesis.creation_time_unix,
        operating_mode_discriminant: genesis.cluster_id,
        account_count: accounts.count,
        account_data_bytes: accounts.total_data_bytes,
        executable_account_count: accounts.executable_accounts,
        capitalization_lamports: accounts
            .total_lamports
            .saturating_add(reward_pools.total_lamports),
        reward_pool_count: reward_pools.count,
        builtins: genesis
            .builtins
            .iter()
            .map(|builtin| GenesisBuiltinSummary {
                name: builtin.key.clone(),
                pubkey_base58: pubkey_to_base58(&builtin.pubkey),
            })
            .collect(),
        ticks_per_slot: genesis.ticks_per_slot,
        slots_per_segment: genesis.slots_per_segment,
        tick_duration_seconds: genesis.poh_params.tick_duration_secs,
        tick_duration_nanoseconds: genesis.poh_params.tick_duration_nanos,
        tick_count: genesis.poh_params.tick_count,
        hashes_per_tick: genesis.poh_params.hashes_per_tick,
        fees: GenesisFeeSummary {
            target_lamports_per_signature: genesis.fees.target_lamports_per_sig,
            target_signatures_per_slot: genesis.fees.target_sigs_per_slot,
            minimum_lamports_per_signature: genesis.fees.min_lamports_per_sig,
            maximum_lamports_per_signature: genesis.fees.max_lamports_per_sig,
            burn_percent: genesis.fees.burn_percent,
        },
        rent: GenesisRentSummary {
            lamports_per_byte_year: genesis.rent.lamports_per_byte_year,
            exemption_threshold: genesis.rent.exemption_threshold,
            burn_percent: genesis.rent.burn_percent,
        },
        inflation: GenesisInflationSummary {
            initial: genesis.inflation.initial,
            terminal: genesis.inflation.terminal,
            taper: genesis.inflation.taper,
            foundation: genesis.inflation.foundation,
            foundation_term: genesis.inflation.foundation_term,
            storage: genesis.inflation.storage,
        },
        slots_per_epoch: genesis.epoch_schedule.slots_per_epoch,
        leader_schedule_slot_offset: genesis.epoch_schedule.leader_schedule_slot_offset,
        warmup: genesis.epoch_schedule.warmup,
        first_normal_epoch: genesis.epoch_schedule.first_normal_epoch,
        first_normal_slot: genesis.epoch_schedule.first_normal_slot,
        epoch_zero,
        epoch_one,
        is_mainnet_beta,
    })
}

fn epoch_window(
    epoch: u64,
    slots_per_epoch: u64,
    warmup: bool,
    first_normal_epoch: u64,
    first_normal_slot: u64,
) -> Result<EpochWindow, GenesisSummaryError> {
    let (first_slot, slots) = if !warmup {
        (
            epoch
                .checked_mul(slots_per_epoch)
                .ok_or(GenesisSummaryError::InvalidEpochSchedule)?,
            slots_per_epoch,
        )
    } else if epoch >= first_normal_epoch {
        let normal_epoch_offset = epoch
            .checked_sub(first_normal_epoch)
            .ok_or(GenesisSummaryError::InvalidEpochSchedule)?;
        let slot_offset = normal_epoch_offset
            .checked_mul(slots_per_epoch)
            .ok_or(GenesisSummaryError::InvalidEpochSchedule)?;
        (
            first_normal_slot
                .checked_add(slot_offset)
                .ok_or(GenesisSummaryError::InvalidEpochSchedule)?,
            slots_per_epoch,
        )
    } else {
        let slots = MINIMUM_SLOTS_PER_EPOCH
            .checked_shl(epoch as u32)
            .ok_or(GenesisSummaryError::InvalidEpochSchedule)?;
        (slots.saturating_sub(MINIMUM_SLOTS_PER_EPOCH), slots)
    };
    let end_slot_exclusive = first_slot
        .checked_add(slots)
        .ok_or(GenesisSummaryError::InvalidEpochSchedule)?;
    Ok(EpochWindow {
        epoch,
        first_slot,
        end_slot_exclusive,
        slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_non_warmup_epoch_windows_are_full_length() {
        let epoch_zero = epoch_window(0, 432_000, false, 0, 0).unwrap();
        let epoch_one = epoch_window(1, 432_000, false, 0, 0).unwrap();
        assert_eq!(epoch_zero.first_slot, 0);
        assert_eq!(epoch_zero.end_slot_exclusive, 432_000);
        assert_eq!(epoch_one.first_slot, 432_000);
        assert_eq!(epoch_one.end_slot_exclusive, 864_000);
    }

    #[test]
    fn warmup_schedule_doubles_from_32_slots() {
        let epoch_zero = epoch_window(0, 432_000, true, 14, 524_256).unwrap();
        let epoch_one = epoch_window(1, 432_000, true, 14, 524_256).unwrap();
        assert_eq!(
            epoch_zero,
            EpochWindow {
                epoch: 0,
                first_slot: 0,
                end_slot_exclusive: 32,
                slots: 32,
            }
        );
        assert_eq!(
            epoch_one,
            EpochWindow {
                epoch: 1,
                first_slot: 32,
                end_slot_exclusive: 96,
                slots: 64,
            }
        );
    }
}

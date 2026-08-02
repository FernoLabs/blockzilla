use of_car_reader::genesis::{
    GenesisArchive, bytes_to_hex, pubkey_to_base58, read_genesis_archive_from_file,
};
use std::path::Path;
use thiserror::Error;

pub const MAINNET_BETA_GENESIS_HASH_BASE58: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const MINIMUM_SLOTS_PER_EPOCH: u64 = 32;

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

pub fn read_genesis_summary(path: impl AsRef<Path>) -> Result<GenesisSummary, GenesisSummaryError> {
    let archive = read_genesis_archive_from_file(path)
        .map_err(|error| GenesisSummaryError::Read(error.to_string()))?;
    summarize_genesis(&archive)
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

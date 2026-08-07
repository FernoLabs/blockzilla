//! Launch-era (`v1.0.7`) native Config-program mutation.
//!
//! The historical Config program did not have an instruction enum.  Its
//! instruction bytes begin with a short-vector of `(pubkey, is_signer)` pairs
//! and the remainder is opaque program-specific configuration data.  A
//! successful invocation copies the complete instruction byte string into the
//! front of account zero and intentionally leaves any remaining account tail
//! untouched.

use std::{collections::BTreeMap, fmt, marker::PhantomData, mem::size_of};

use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};
use thiserror::Error;

use crate::{AccountSnapshot, LaunchAccountMeta, default_system_account};

const MAX_INSTRUCTION_LEN: usize = 1_232;

/// `Config1111111111111111111111111111111111111`.
pub const CONFIG_PROGRAM_ID: [u8; 32] = [
    3, 6, 74, 163, 0, 47, 116, 220, 200, 110, 67, 49, 15, 12, 5, 42, 248, 197, 218, 39, 246, 16,
    64, 25, 163, 35, 239, 160, 0, 0, 0, 0,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaunchConfigKey {
    pub pubkey: [u8; 32],
    pub is_signer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchConfigMutation {
    pub config_account: [u8; 32],
    pub keys: Vec<LaunchConfigKey>,
    pub data_len: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LaunchConfigError {
    #[error("config instruction data does not contain a valid ConfigKeys prefix")]
    InvalidInstructionData,
    #[error("config instruction is missing account position {position}")]
    MissingAccount { position: usize },
    #[error("config account {pubkey:?} does not contain valid ConfigKeys data")]
    InvalidAccountData { pubkey: [u8; 32] },
    #[error("account {pubkey:?} must sign the Config instruction")]
    MissingRequiredSignature { pubkey: [u8; 32] },
    #[error("Config program cannot change the owner of account {pubkey:?}")]
    ModifiedProgramId { pubkey: [u8; 32] },
    #[error("Config program cannot spend lamports from externally owned account {pubkey:?}")]
    ExternalAccountLamportSpend { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: [u8; 32] },
    #[error("Config program cannot resize account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: [u8; 32] },
    #[error("Config program changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: [u8; 32] },
    #[error("Config program made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: [u8; 32] },
    #[error("Config program changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: [u8; 32] },
    #[error("Config instruction is unbalanced: pre={pre_lamports}, post={post_lamports}")]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
}

/// Apply one launch Config instruction atomically to a transaction overlay.
///
/// This preserves the v1.0.7 processor's error ordering and positional signer
/// behavior. Signature *cryptography* is deliberately outside replay; the
/// `is_signer` flags are trusted message privileges.
///
/// Instruction-atomic for external callers. Replay uses
/// [`apply_launch_config_instruction_in_place`] on a disposable overlay.
pub fn apply_launch_config_instruction(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut BTreeMap<[u8; 32], AccountSnapshot>,
) -> Result<LaunchConfigMutation, LaunchConfigError> {
    let mut working = accounts.clone();
    let mutation =
        apply_launch_config_instruction_in_place(instruction_data, account_metas, &mut working)?;
    *accounts = working;
    Ok(mutation)
}

/// Replay-only fast path. On error `accounts` may be partially mutated and
/// must be discarded with the transaction overlay.
pub fn apply_launch_config_instruction_in_place(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut BTreeMap<[u8; 32], AccountSnapshot>,
) -> Result<LaunchConfigMutation, LaunchConfigError> {
    // v1.0.7 calls limited_deserialize before asking for account zero.
    let incoming = decode_instruction_keys(instruction_data)?;
    let config_meta = account_metas
        .first()
        .ok_or(LaunchConfigError::MissingAccount { position: 0 })?;

    for meta in account_metas {
        accounts
            .entry(meta.pubkey)
            .or_insert_with(default_system_account);
    }
    let pre_accounts = launch_pre_accounts(account_metas, accounts);

    let current = decode_account_keys(
        &accounts
            .get(&config_meta.pubkey)
            .expect("instruction accounts were materialized")
            .data,
    )
    .map_err(|()| LaunchConfigError::InvalidAccountData {
        pubkey: config_meta.pubkey,
    })?;
    let current_signer_keys = current
        .keys
        .iter()
        .filter(|(_, is_signer)| *is_signer)
        .map(|(pubkey, _)| *pubkey)
        .collect::<Vec<_>>();

    if current_signer_keys.is_empty() && !config_meta.is_signer {
        return Err(LaunchConfigError::MissingRequiredSignature {
            pubkey: config_meta.pubkey,
        });
    }

    let mut additional_accounts = account_metas.iter().skip(1);
    let mut incoming_signer_count = 0_usize;
    for (signer, _) in incoming.keys.iter().filter(|(_, is_signer)| *is_signer) {
        incoming_signer_count += 1;
        if signer == &config_meta.pubkey {
            if !config_meta.is_signer {
                return Err(LaunchConfigError::MissingRequiredSignature { pubkey: *signer });
            }
            continue;
        }

        let Some(signer_meta) = additional_accounts.next() else {
            return Err(LaunchConfigError::MissingRequiredSignature { pubkey: *signer });
        };
        if !signer_meta.is_signer || signer_meta.pubkey != *signer {
            return Err(LaunchConfigError::MissingRequiredSignature { pubkey: *signer });
        }
        if !current.keys.is_empty() && !current_signer_keys.contains(signer) {
            return Err(LaunchConfigError::MissingRequiredSignature { pubkey: *signer });
        }
    }

    if current_signer_keys.len() > incoming_signer_count {
        let missing = current_signer_keys
            .get(incoming_signer_count)
            .copied()
            .unwrap_or(config_meta.pubkey);
        return Err(LaunchConfigError::MissingRequiredSignature { pubkey: missing });
    }

    let config_account = accounts
        .get_mut(&config_meta.pubkey)
        .expect("instruction account was materialized");
    if config_account.data.len() < instruction_data.len() {
        return Err(LaunchConfigError::InvalidInstructionData);
    }
    config_account.data[..instruction_data.len()].copy_from_slice(instruction_data);

    verify_launch_config_instruction(&pre_accounts, accounts)?;
    Ok(LaunchConfigMutation {
        config_account: config_meta.pubkey,
        keys: incoming
            .keys
            .into_iter()
            .map(|(pubkey, is_signer)| LaunchConfigKey { pubkey, is_signer })
            .collect(),
        data_len: instruction_data.len(),
    })
}

#[derive(Debug, Deserialize)]
struct ConfigKeysWire {
    #[serde(with = "short_vec")]
    keys: Vec<([u8; 32], bool)>,
}

fn decode_instruction_keys(data: &[u8]) -> Result<ConfigKeysWire, LaunchConfigError> {
    // Matches v1.0.7 `program_utils::limited_deserialize`: fixed integers,
    // trailing bytes accepted, and a packet-sized deserialization budget.
    if data.len() > MAX_INSTRUCTION_LEN {
        return Err(LaunchConfigError::InvalidInstructionData);
    }
    let (keys, _) = decode_config_keys(data)?;
    Ok(keys)
}

fn decode_account_keys(data: &[u8]) -> Result<ConfigKeysWire, ()> {
    decode_config_keys(data)
        .map(|(keys, _)| keys)
        .map_err(|_| ())
}

fn decode_config_keys(data: &[u8]) -> Result<(ConfigKeysWire, usize), LaunchConfigError> {
    let (key_count, header_len) = decode_short_u16_length(data)?;
    let key_len_usize = usize::from(key_count);
    let mut position = header_len;
    let mut keys = Vec::with_capacity(key_len_usize);

    for _ in 0..key_count {
        let start = position;
        let end = start
            .checked_add(32)
            .ok_or(LaunchConfigError::InvalidInstructionData)?;
        let pubkey = data
            .get(start..end)
            .ok_or(LaunchConfigError::InvalidInstructionData)?
            .try_into()
            .map_err(|_| LaunchConfigError::InvalidInstructionData)?;
        position = end;

        let is_signer = *data
            .get(position)
            .ok_or(LaunchConfigError::InvalidInstructionData)?;
        position = position
            .checked_add(1)
            .ok_or(LaunchConfigError::InvalidInstructionData)?;
        let is_signer = match is_signer {
            0 => false,
            1 => true,
            _ => return Err(LaunchConfigError::InvalidInstructionData),
        };
        keys.push((pubkey, is_signer));
    }

    Ok((ConfigKeysWire { keys }, position))
}

fn decode_short_u16_length(data: &[u8]) -> Result<(u16, usize), LaunchConfigError> {
    let mut value = 0_usize;
    let mut shift = 0_usize;
    let mut consumed = 0_usize;

    loop {
        let byte = data
            .get(consumed)
            .copied()
            .ok_or(LaunchConfigError::InvalidInstructionData)?;
        consumed += 1;
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 21 {
            return Err(LaunchConfigError::InvalidInstructionData);
        }
    }

    let key_count = u16::try_from(value).map_err(|_| LaunchConfigError::InvalidInstructionData)?;
    Ok((key_count, consumed))
}

/// The subset of v1.0.7 `message_processor::PreAccount` required to enforce
/// native-program account invariants after the processor returns.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchPreAccount {
    pubkey: [u8; 32],
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<Vec<u8>>,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
}

impl LaunchPreAccount {
    fn new(pubkey: [u8; 32], is_writable: bool, account: &AccountSnapshot) -> Self {
        Self {
            pubkey,
            is_writable,
            lamports: account.lamports,
            data_len: account.data.len(),
            data: should_verify_data(&account.owner, is_writable).then(|| account.data.to_vec()),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }

    fn verify(&self, post: &AccountSnapshot) -> Result<(), LaunchConfigError> {
        if self.owner != post.owner
            && (!self.is_writable || self.owner != CONFIG_PROGRAM_ID || !is_zeroed(&post.data))
        {
            return Err(LaunchConfigError::ModifiedProgramId {
                pubkey: self.pubkey,
            });
        }
        if self.owner != CONFIG_PROGRAM_ID && self.lamports > post.lamports {
            return Err(LaunchConfigError::ExternalAccountLamportSpend {
                pubkey: self.pubkey,
            });
        }
        if !self.is_writable && self.lamports != post.lamports {
            return Err(LaunchConfigError::ReadonlyLamportChange {
                pubkey: self.pubkey,
            });
        }
        // Only System may resize System-owned data.  Config is never System.
        if self.data_len != post.data.len() {
            return Err(LaunchConfigError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }
        if should_verify_data(&self.owner, self.is_writable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.is_writable {
                LaunchConfigError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchConfigError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }
        if self.executable != post.executable
            && (!self.is_writable || self.executable || self.owner != CONFIG_PROGRAM_ID)
        {
            return Err(LaunchConfigError::ExecutableModified {
                pubkey: self.pubkey,
            });
        }
        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchConfigError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }
        Ok(())
    }
}

fn should_verify_data(owner: &[u8; 32], is_writable: bool) -> bool {
    *owner != CONFIG_PROGRAM_ID || !is_writable
}

fn is_zeroed(data: &[u8]) -> bool {
    data.iter().all(|byte| *byte == 0)
}

fn launch_pre_accounts(
    account_metas: &[LaunchAccountMeta],
    accounts: &BTreeMap<[u8; 32], AccountSnapshot>,
) -> Vec<LaunchPreAccount> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| {
            !account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
        .map(|(_, meta)| {
            LaunchPreAccount::new(
                meta.pubkey,
                meta.is_writable,
                accounts
                    .get(&meta.pubkey)
                    .expect("instruction accounts were materialized before snapshot"),
            )
        })
        .collect()
}

fn verify_launch_config_instruction(
    pre_accounts: &[LaunchPreAccount],
    accounts: &BTreeMap<[u8; 32], AccountSnapshot>,
) -> Result<(), LaunchConfigError> {
    let mut pre_lamports = 0_u128;
    let mut post_lamports = 0_u128;
    for pre in pre_accounts {
        let post = accounts
            .get(&pre.pubkey)
            .expect("instruction accounts remain materialized through verification");
        pre.verify(post)?;
        pre_lamports += u128::from(pre.lamports);
        post_lamports += u128::from(post.lamports);
    }
    if pre_lamports != post_lamports {
        return Err(LaunchConfigError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
    }
    Ok(())
}

// Exact deserializer used by the launch SDK's `#[serde(with = "short_vec")]`.
mod short_vec {
    use super::*;

    struct ShortU16(u16);

    struct ShortLenVisitor;

    impl<'de> Visitor<'de> for ShortLenVisitor {
        type Value = ShortU16;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a multi-byte length")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut len = 0_usize;
            let mut size = 0_usize;
            loop {
                let elem: u8 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(size, &self))?;
                len |= (usize::from(elem) & 0x7f) << (size * 7);
                size += 1;
                if usize::from(elem) & 0x80 == 0 {
                    break;
                }
                if size > size_of::<u16>() + 1 {
                    return Err(de::Error::invalid_length(size, &self));
                }
            }
            Ok(ShortU16(len as u16))
        }
    }

    impl<'de> Deserialize<'de> for ShortU16 {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_tuple(3, ShortLenVisitor)
        }
    }

    struct ShortVecVisitor<T> {
        marker: PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for ShortVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a Vec with a multi-byte length")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let short_len: ShortU16 = seq
                .next_element()?
                .ok_or_else(|| de::Error::invalid_length(0, &self))?;
            let len = usize::from(short_len.0);
            let mut result = Vec::with_capacity(len);
            for index in 0..len {
                let element = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(index, &self))?;
                result.push(element);
            }
            Ok(result)
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        deserializer.deserialize_tuple(
            usize::MAX,
            ShortVecVisitor {
                marker: PhantomData,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: [u8; 32] = [11; 32];
    const SIGNER_A: [u8; 32] = [12; 32];
    const SIGNER_B: [u8; 32] = [13; 32];

    fn meta(pubkey: [u8; 32], is_signer: bool, is_writable: bool) -> LaunchAccountMeta {
        LaunchAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        }
    }

    fn config_account(data: Vec<u8>) -> AccountSnapshot {
        AccountSnapshot {
            lamports: 10,
            owner: CONFIG_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: data.into(),
        }
    }

    fn wire(keys: &[([u8; 32], bool)], opaque: &[u8]) -> Vec<u8> {
        assert!(keys.len() < 128);
        let mut data = vec![keys.len() as u8];
        for (pubkey, is_signer) in keys {
            data.extend_from_slice(pubkey);
            data.push(u8::from(*is_signer));
        }
        data.extend_from_slice(opaque);
        data
    }

    #[test]
    fn initializes_zeroed_account_and_preserves_unused_tail() {
        let instruction = wire(&[], &[1, 2, 3]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(vec![0; 12]))]);
        let mutation = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, true, true)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(mutation.config_account, CONFIG);
        assert!(mutation.keys.is_empty());
        assert_eq!(mutation.data_len, 4);
        assert_eq!(&accounts[&CONFIG].data[..4], instruction);
        assert_eq!(&accounts[&CONFIG].data[4..], &[0; 8]);
    }

    #[test]
    fn configured_external_signers_can_update_without_config_signature() {
        let current = wire(&[(SIGNER_A, true), (SIGNER_B, true)], &[0; 4]);
        let incoming = wire(&[(SIGNER_A, true), (SIGNER_B, true)], &[9; 4]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);

        apply_launch_config_instruction(
            &incoming,
            &[
                meta(CONFIG, false, true),
                meta(SIGNER_A, true, false),
                meta(SIGNER_B, true, false),
            ],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&CONFIG].data, incoming);
    }

    #[test]
    fn missing_current_signer_rejects_update_atomically() {
        let current = wire(&[(SIGNER_A, true), (SIGNER_B, true)], &[0; 4]);
        let incoming = wire(&[(SIGNER_A, true)], &[9; 4]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);
        let before = accounts.clone();

        let error = apply_launch_config_instruction(
            &incoming,
            &[meta(CONFIG, false, true), meta(SIGNER_A, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::MissingRequiredSignature { pubkey: SIGNER_B }
        );
        assert_eq!(accounts, before);
    }

    #[test]
    fn positional_signer_mismatch_is_rejected() {
        let current = wire(&[(SIGNER_A, true)], &[0]);
        let incoming = wire(&[(SIGNER_A, true)], &[1]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);
        let error = apply_launch_config_instruction(
            &incoming,
            &[meta(CONFIG, false, true), meta(SIGNER_B, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::MissingRequiredSignature { pubkey: SIGNER_A }
        );
    }

    #[test]
    fn data_store_requires_ownership_and_writability() {
        let instruction = wire(&[], &[1]);
        let mut external = config_account(vec![0; 2]);
        external.owner = [99; 32];
        let mut accounts = BTreeMap::from([(CONFIG, external)]);
        let error = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, true, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::ExternalAccountDataModified { pubkey: CONFIG }
        );

        let mut accounts = BTreeMap::from([(CONFIG, config_account(vec![0; 2]))]);
        let error = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::ReadonlyDataModified { pubkey: CONFIG }
        );
    }

    #[test]
    fn identical_write_can_succeed_on_readonly_wrong_owner_account() {
        let instruction = wire(&[], &[1]);
        let mut account = config_account(instruction.clone());
        account.owner = [99; 32];
        let mut accounts = BTreeMap::from([(CONFIG, account.clone())]);
        let mutation = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, true, false)],
            &mut accounts,
        )
        .unwrap();

        assert_eq!(mutation.config_account, CONFIG);
        assert_eq!(accounts[&CONFIG], account);
    }

    #[test]
    fn initialized_config_cannot_introduce_unknown_external_signer() {
        let current = wire(&[(SIGNER_A, true)], &[0]);
        let incoming = wire(&[(SIGNER_B, true)], &[1]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);
        let error = apply_launch_config_instruction(
            &incoming,
            &[meta(CONFIG, false, true), meta(SIGNER_B, true, false)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::MissingRequiredSignature { pubkey: SIGNER_B }
        );
    }

    #[test]
    fn config_signer_substitution_preserves_launch_cardinality_quirk() {
        let current = wire(&[(SIGNER_A, true), (SIGNER_B, true)], &[0]);
        let incoming = wire(&[(CONFIG, true), (CONFIG, true)], &[1]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);

        apply_launch_config_instruction(&incoming, &[meta(CONFIG, true, true)], &mut accounts)
            .unwrap();
        assert_eq!(accounts[&CONFIG].data, incoming);
    }

    #[test]
    fn repeated_authorized_signer_preserves_launch_cardinality_quirk() {
        let current = wire(&[(SIGNER_A, true), (SIGNER_B, true)], &[0]);
        let incoming = wire(&[(SIGNER_A, true), (SIGNER_A, true)], &[1]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(current))]);

        apply_launch_config_instruction(
            &incoming,
            &[
                meta(CONFIG, false, true),
                meta(SIGNER_A, true, false),
                meta(SIGNER_A, true, false),
            ],
            &mut accounts,
        )
        .unwrap();
        assert_eq!(accounts[&CONFIG].data, incoming);
    }

    #[test]
    fn malformed_current_state_precedes_signature_validation() {
        let instruction = wire(&[], &[1]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(Vec::new()))]);
        let error = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, false, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            LaunchConfigError::InvalidAccountData { pubkey: CONFIG }
        );
    }

    #[test]
    fn invalid_data_is_reported_before_missing_account() {
        let error = apply_launch_config_instruction(&[], &[], &mut BTreeMap::new()).unwrap_err();
        assert_eq!(error, LaunchConfigError::InvalidInstructionData);
    }

    #[test]
    fn account_must_hold_complete_instruction_and_rollback_on_failure() {
        let instruction = wire(&[], &[1, 2, 3]);
        let mut accounts = BTreeMap::from([(CONFIG, config_account(vec![0; 3]))]);
        let before = accounts.clone();
        let error = apply_launch_config_instruction(
            &instruction,
            &[meta(CONFIG, true, true)],
            &mut accounts,
        )
        .unwrap_err();
        assert_eq!(error, LaunchConfigError::InvalidInstructionData);
        assert_eq!(accounts, before);
    }
}

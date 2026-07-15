use anyhow::{Result, bail};
use blockzilla_format::{
    CompactLogStream, CompactMetaV1, CompactPubkey, KeyIndex, LogEvent, OwnedCompactMessage,
    WincodeArchiveV2Block, WincodeArchiveV2Payload,
    program_logs::{
        ProgramLog,
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
};

/// Result of rewriting the raw pubkey references in a pre-hot block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PreHotRekeyStats {
    pub(crate) raw_seen: u64,
    pub(crate) rekeyed: u64,
    pub(crate) raw_remaining: u64,
}

/// Visits pubkeys using the historical CAR registry-counting semantics.
///
/// In particular, this deliberately excludes pubkeys found only while parsing
/// logs. Keeping this definition aligned with the existing registry pass makes
/// the frequency ordering (and therefore the assigned ids) byte-compatible
/// with an archive built directly from CAR.
pub(crate) fn count_registry_pubkeys(
    block: &WincodeArchiveV2Block,
    mut add: impl FnMut(&[u8; 32]),
) -> Result<u64> {
    let mut refs = 0u64;

    // The historical CAR registry pass does not count block-level rewards.
    // Keep them raw unless the same key is referenced by a transaction/meta.

    for transaction in &block.txs {
        let value = match &transaction.tx {
            WincodeArchiveV2Payload::Decoded { value, .. } => value,
            WincodeArchiveV2Payload::Raw { error, .. } => {
                bail!(
                    "pre-hot tx_index {} contains a raw transaction fallback: {error}",
                    transaction.tx_index
                );
            }
        };
        match &value.message {
            OwnedCompactMessage::Legacy(message) => {
                for key in &message.account_keys {
                    visit_required_raw(*key, &mut add, &mut refs)?;
                }
            }
            OwnedCompactMessage::V0(message) => {
                for key in &message.account_keys {
                    visit_required_raw(*key, &mut add, &mut refs)?;
                }
                for lookup in &message.address_table_lookups {
                    visit_required_raw(lookup.account_key, &mut add, &mut refs)?;
                }
            }
        }

        // Raw metadata fallback bytes are opaque and are copied unchanged by
        // the hot writer; there are no traversable CompactPubkey fields in it.
        if let Some(WincodeArchiveV2Payload::Decoded {
            value: metadata, ..
        }) = &transaction.metadata
        {
            count_registry_meta_pubkeys(metadata, &mut add, &mut refs)?;
        }
    }

    Ok(refs)
}

/// Counts every raw `CompactPubkey` encoded in a pre-hot block, including
/// pubkeys nested in compact log events.
pub(crate) fn count_all_raw_pubkey_refs(block: &WincodeArchiveV2Block) -> Result<u64> {
    let mut raw = 0u64;

    if let Some(rewards) = &block.header.rewards {
        ensure_decoded_rewards(rewards)?;
        for reward in rewards.decoded.as_deref().unwrap_or_default() {
            count_raw_key(reward.pubkey, &mut raw);
        }
    }

    for transaction in &block.txs {
        let value = match &transaction.tx {
            WincodeArchiveV2Payload::Decoded { value, .. } => value,
            WincodeArchiveV2Payload::Raw { error, .. } => {
                bail!(
                    "pre-hot tx_index {} contains a raw transaction fallback: {error}",
                    transaction.tx_index
                );
            }
        };
        count_message_raw_pubkeys(&value.message, &mut raw);

        if let Some(WincodeArchiveV2Payload::Decoded {
            value: metadata, ..
        }) = &transaction.metadata
        {
            count_meta_raw_pubkeys(metadata, &mut raw);
        }
    }

    Ok(raw)
}

/// Rewrites all known raw pubkey references in a pre-hot block to registry ids.
///
/// A raw reference not present in `key_index` remains raw and is reported in
/// `raw_remaining`. This matches the historical behavior for log-only pubkeys,
/// which were deliberately not included in the registry counting pass.
pub(crate) fn rekey_block_pubkeys(
    block: &mut WincodeArchiveV2Block,
    key_index: &KeyIndex,
) -> Result<PreHotRekeyStats> {
    rewrite_block_pubkeys(block, &mut |raw| key_index.lookup(raw))
}

/// Rewrites every raw pubkey reference to an ID assigned by `intern`.
///
/// Unlike [`rekey_block_pubkeys`], this is a strict all-reference operation:
/// the callback is invoked once for every encoded raw `CompactPubkey`,
/// including block rewards and structured logs, and no raw reference may
/// remain afterward.
pub(crate) fn intern_block_pubkeys(
    block: &mut WincodeArchiveV2Block,
    mut intern: impl FnMut(&[u8; 32]) -> Result<u32>,
) -> Result<PreHotRekeyStats> {
    let mut intern_error = None;
    let stats = rewrite_block_pubkeys(block, &mut |raw| {
        if intern_error.is_some() {
            return None;
        }
        match intern(raw) {
            Ok(id) => Some(id),
            Err(error) => {
                intern_error = Some(error);
                None
            }
        }
    })?;
    if let Some(error) = intern_error {
        return Err(error);
    }
    debug_assert_eq!(stats.raw_remaining, 0);
    Ok(stats)
}

fn rewrite_block_pubkeys(
    block: &mut WincodeArchiveV2Block,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
) -> Result<PreHotRekeyStats> {
    let mut stats = PreHotRekeyStats::default();

    if let Some(rewards) = &mut block.header.rewards {
        ensure_decoded_rewards(rewards)?;
        for reward in rewards.decoded.as_deref_mut().unwrap_or_default() {
            rekey_pubkey(&mut reward.pubkey, resolve, &mut stats);
        }
    }

    for transaction in &mut block.txs {
        let value = match &mut transaction.tx {
            WincodeArchiveV2Payload::Decoded { value, .. } => value,
            WincodeArchiveV2Payload::Raw { error, .. } => {
                bail!(
                    "pre-hot tx_index {} contains a raw transaction fallback: {error}",
                    transaction.tx_index
                );
            }
        };
        rekey_message_pubkeys(&mut value.message, resolve, &mut stats);

        if let Some(WincodeArchiveV2Payload::Decoded {
            value: metadata, ..
        }) = &mut transaction.metadata
        {
            rekey_meta_pubkeys(metadata, resolve, &mut stats);
        }
    }

    debug_assert_eq!(
        stats.raw_seen,
        stats.rekeyed.saturating_add(stats.raw_remaining)
    );
    Ok(stats)
}

fn ensure_decoded_rewards(rewards: &blockzilla_format::WincodeArchiveV2Rewards) -> Result<()> {
    if rewards.raw_fallback.is_some() || rewards.decode_error.is_some() {
        bail!("pre-hot block contains a raw rewards fallback");
    }
    if rewards.decoded.is_none() {
        bail!("pre-hot rewards record is missing its decoded rewards");
    }
    Ok(())
}

fn visit_required_raw(
    key: CompactPubkey,
    add: &mut impl FnMut(&[u8; 32]),
    refs: &mut u64,
) -> Result<()> {
    match key {
        CompactPubkey::Raw(raw) => {
            add(&raw);
            *refs = refs.saturating_add(1);
            Ok(())
        }
        CompactPubkey::Id(id) => {
            bail!("pre-hot registry counting encountered finalized pubkey id {id}")
        }
    }
}

fn count_registry_meta_pubkeys(
    meta: &CompactMetaV1,
    add: &mut impl FnMut(&[u8; 32]),
    refs: &mut u64,
) -> Result<()> {
    for key in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        visit_required_raw(*key, add, refs)?;
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        for key in [balance.mint, balance.owner, balance.program_id]
            .into_iter()
            .flatten()
        {
            visit_required_raw(key, add, refs)?;
        }
    }
    for reward in &meta.rewards {
        visit_required_raw(reward.pubkey, add, refs)?;
    }
    if let Some(return_data) = &meta.return_data {
        visit_required_raw(return_data.program_id, add, refs)?;
    }
    Ok(())
}

fn count_message_raw_pubkeys(message: &OwnedCompactMessage, raw: &mut u64) {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            for key in &message.account_keys {
                count_raw_key(*key, raw);
            }
        }
        OwnedCompactMessage::V0(message) => {
            for key in &message.account_keys {
                count_raw_key(*key, raw);
            }
            for lookup in &message.address_table_lookups {
                count_raw_key(lookup.account_key, raw);
            }
        }
    }
}

fn count_meta_raw_pubkeys(meta: &CompactMetaV1, raw: &mut u64) {
    for key in meta
        .loaded_writable_addresses
        .iter()
        .chain(meta.loaded_readonly_addresses.iter())
    {
        count_raw_key(*key, raw);
    }
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        for key in [balance.mint, balance.owner, balance.program_id]
            .into_iter()
            .flatten()
        {
            count_raw_key(key, raw);
        }
    }
    for reward in &meta.rewards {
        count_raw_key(reward.pubkey, raw);
    }
    if let Some(return_data) = &meta.return_data {
        count_raw_key(return_data.program_id, raw);
    }
    if let Some(logs) = &meta.logs {
        count_log_raw_pubkeys(logs, raw);
    }
}

fn count_log_raw_pubkeys(logs: &CompactLogStream, raw: &mut u64) {
    for event in &logs.events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::Invoke { program, .. }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. } => count_raw_key(*program, raw),
            LogEvent::ProgramIdLog { program, log } => {
                count_raw_key(*program, raw);
                count_program_log_raw_pubkeys(log, raw);
            }
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                count_raw_key(*account, raw);
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    count_raw_key(*program, raw);
                }
            }
            LogEvent::System(log) => count_system_log_raw_pubkeys(log, raw),
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                count_program_log_raw_pubkeys(log, raw);
            }
            LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
}

fn count_program_log_raw_pubkeys(log: &ProgramLog, raw: &mut u64) {
    if let ProgramLog::Token2022(log) = log {
        match log {
            Token2022Log::ErrorHarvestingFrom { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
            | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => {
                count_raw_key(*account_key, raw);
            }
            _ => {}
        }
    }
}

fn count_system_log_raw_pubkeys(log: &SystemProgramLog, raw: &mut u64) {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            count_raw_key(*provided_addr, raw);
            count_pubkey_or_string_raw(*derived_addr, raw);
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            count_system_address_raw(*addr, raw);
        }
        SystemProgramLog::TransferFromMustSign { from } => count_raw_key(*from, raw),
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            count_pubkey_or_string_raw(*account, raw);
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
}

fn count_system_address_raw(address: SystemAddress, raw: &mut u64) {
    match address {
        SystemAddress::Pubkey(pubkey) => count_pubkey_or_string_raw(pubkey, raw),
        SystemAddress::Debug { address, base } => {
            count_pubkey_or_string_raw(address, raw);
            if let Some(base) = base {
                count_pubkey_or_string_raw(base, raw);
            }
        }
    }
}

fn count_pubkey_or_string_raw(value: PubkeyOrString, raw: &mut u64) {
    if let PubkeyOrString::Pubkey(pubkey) = value {
        count_raw_key(pubkey, raw);
    }
}

fn count_raw_key(key: CompactPubkey, raw: &mut u64) {
    if matches!(key, CompactPubkey::Raw(_)) {
        *raw = raw.saturating_add(1);
    }
}

fn rekey_message_pubkeys(
    message: &mut OwnedCompactMessage,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    match message {
        OwnedCompactMessage::Legacy(message) => {
            for key in &mut message.account_keys {
                rekey_pubkey(key, resolve, stats);
            }
        }
        OwnedCompactMessage::V0(message) => {
            for key in &mut message.account_keys {
                rekey_pubkey(key, resolve, stats);
            }
            for lookup in &mut message.address_table_lookups {
                rekey_pubkey(&mut lookup.account_key, resolve, stats);
            }
        }
    }
}

fn rekey_meta_pubkeys(
    meta: &mut CompactMetaV1,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    for key in meta
        .loaded_writable_addresses
        .iter_mut()
        .chain(meta.loaded_readonly_addresses.iter_mut())
    {
        rekey_pubkey(key, resolve, stats);
    }
    for balance in meta
        .pre_token_balances
        .iter_mut()
        .chain(meta.post_token_balances.iter_mut())
    {
        for key in [
            &mut balance.mint,
            &mut balance.owner,
            &mut balance.program_id,
        ] {
            if let Some(key) = key.as_mut() {
                rekey_pubkey(key, resolve, stats);
            }
        }
    }
    for reward in &mut meta.rewards {
        rekey_pubkey(&mut reward.pubkey, resolve, stats);
    }
    if let Some(return_data) = &mut meta.return_data {
        rekey_pubkey(&mut return_data.program_id, resolve, stats);
    }
    if let Some(logs) = &mut meta.logs {
        rekey_log_pubkeys(logs, resolve, stats);
    }
}

fn rekey_log_pubkeys(
    logs: &mut CompactLogStream,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    for event in &mut logs.events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::Invoke { program, .. }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. } => rekey_pubkey(program, resolve, stats),
            LogEvent::ProgramIdLog { program, log } => {
                rekey_pubkey(program, resolve, stats);
                rekey_program_log_pubkeys(log, resolve, stats);
            }
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                rekey_pubkey(account, resolve, stats);
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    rekey_pubkey(program, resolve, stats);
                }
            }
            LogEvent::System(log) => rekey_system_log_pubkeys(log, resolve, stats),
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                rekey_program_log_pubkeys(log, resolve, stats);
            }
            LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
}

fn rekey_program_log_pubkeys(
    log: &mut ProgramLog,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    match log {
        ProgramLog::Token2022(log) => rekey_token_2022_log_pubkeys(log, resolve, stats),
        ProgramLog::Empty
        | ProgramLog::Token(_)
        | ProgramLog::Ata(_)
        | ProgramLog::AddressLookupTable(_)
        | ProgramLog::LoaderV3(_)
        | ProgramLog::LoaderV4(_)
        | ProgramLog::Memo(_)
        | ProgramLog::Record(_)
        | ProgramLog::TransferHook(_)
        | ProgramLog::AccountCompression(_)
        | ProgramLog::Stake(_)
        | ProgramLog::ZkElgamalProof(_)
        | ProgramLog::AnchorInstruction { .. }
        | ProgramLog::AnchorErrorOccurred { .. }
        | ProgramLog::AnchorErrorThrown { .. }
        | ProgramLog::Unknown(_) => {}
        #[cfg(feature = "known-program-logs")]
        ProgramLog::Known(_) => {}
    }
}

fn rekey_token_2022_log_pubkeys(
    log: &mut Token2022Log,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    match log {
        Token2022Log::ErrorHarvestingFrom { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => {
            rekey_pubkey(account_key, resolve, stats);
        }
        Token2022Log::Error(_)
        | Token2022Log::Static(_)
        | Token2022Log::CalculatedFee { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug2 { .. } => {}
    }
}

fn rekey_system_log_pubkeys(
    log: &mut SystemProgramLog,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            rekey_pubkey(provided_addr, resolve, stats);
            rekey_pubkey_or_string(derived_addr, resolve, stats);
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            rekey_system_address(addr, resolve, stats);
        }
        SystemProgramLog::TransferFromMustSign { from } => {
            rekey_pubkey(from, resolve, stats);
        }
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            rekey_pubkey_or_string(account, resolve, stats);
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
}

fn rekey_system_address(
    address: &mut SystemAddress,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    match address {
        SystemAddress::Pubkey(pubkey) => rekey_pubkey_or_string(pubkey, resolve, stats),
        SystemAddress::Debug { address, base } => {
            rekey_pubkey_or_string(address, resolve, stats);
            if let Some(base) = base {
                rekey_pubkey_or_string(base, resolve, stats);
            }
        }
    }
}

fn rekey_pubkey_or_string(
    value: &mut PubkeyOrString,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    if let PubkeyOrString::Pubkey(pubkey) = value {
        rekey_pubkey(pubkey, resolve, stats);
    }
}

fn rekey_pubkey(
    pubkey: &mut CompactPubkey,
    resolve: &mut impl FnMut(&[u8; 32]) -> Option<u32>,
    stats: &mut PreHotRekeyStats,
) {
    let CompactPubkey::Raw(raw) = pubkey else {
        return;
    };
    stats.raw_seen = stats.raw_seen.saturating_add(1);
    if let Some(id) = resolve(raw) {
        *pubkey = CompactPubkey::id(id);
        stats.rekeyed = stats.rekeyed.saturating_add(1);
    } else {
        stats.raw_remaining = stats.raw_remaining.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        CompactBlockHeader, CompactMessageHeader, CompactReturnData, CompactReward,
        CompactTokenBalance, DataTable, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash, OwnedCompactTransaction, OwnedCompactV0Message, StringTable,
        WincodeArchiveV2BlockHeader, WincodeArchiveV2Rewards, WincodeArchiveV2Transaction,
    };

    fn raw(value: u8) -> CompactPubkey {
        CompactPubkey::raw([value; 32])
    }

    fn test_block() -> WincodeArchiveV2Block {
        let logs = CompactLogStream {
            events: vec![
                LogEvent::Invoke {
                    program: raw(10),
                    depth: 1,
                },
                LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                    provided_addr: raw(11),
                    derived_addr: PubkeyOrString::Pubkey(raw(12)),
                }),
                LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                    account_key: raw(13),
                    error: 0,
                })),
            ],
            strings: StringTable::default(),
            data: DataTable::default(),
        };
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: Some(logs),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(raw(7)),
                owner: Some(raw(8)),
                program_id: Some(raw(9)),
                amount: 1,
                decimals: 0,
            }],
            post_token_balances: Vec::new(),
            rewards: vec![CompactReward {
                pubkey: raw(14),
                lamports: 1,
                post_balance: 1,
                reward_type: 0,
                commission: None,
            }],
            loaded_writable_addresses: vec![raw(5)],
            loaded_readonly_addresses: vec![raw(6)],
            return_data: Some(CompactReturnData {
                program_id: raw(15),
                data: Vec::new(),
            }),
            compute_units_consumed: None,
            cost_units: None,
        };
        let transaction = OwnedCompactTransaction {
            signatures: Vec::new(),
            message: OwnedCompactMessage::V0(OwnedCompactV0Message {
                header: CompactMessageHeader {
                    num_required_signatures: 0,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![raw(2), raw(3)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: Vec::new(),
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: raw(4),
                    writable_indexes: Vec::new(),
                    readonly_indexes: Vec::new(),
                }],
            }),
        };

        WincodeArchiveV2Block {
            header: WincodeArchiveV2BlockHeader {
                compact: CompactBlockHeader {
                    slot: 42,
                    parent_slot: 41,
                    blockhash: 0,
                    previous_blockhash: 0,
                    block_time: None,
                    block_height: None,
                    shredding: Vec::new(),
                    poh_entries: Vec::new(),
                    rewards: None,
                },
                rewards: Some(WincodeArchiveV2Rewards {
                    source_len: 0,
                    num_partitions: None,
                    decoded: Some(vec![CompactReward {
                        pubkey: raw(1),
                        lamports: 1,
                        post_balance: 1,
                        reward_type: 0,
                        commission: None,
                    }]),
                    raw_fallback: None,
                    decode_error: None,
                }),
            },
            txs: vec![WincodeArchiveV2Transaction {
                tx_index: 0,
                tx: WincodeArchiveV2Payload::Decoded {
                    source_len: 0,
                    value: transaction,
                },
                metadata: Some(WincodeArchiveV2Payload::Decoded {
                    source_len: 0,
                    value: metadata,
                }),
            }],
        }
    }

    #[test]
    fn registry_count_matches_historical_fields_and_excludes_logs() {
        let block = test_block();
        let mut visited = Vec::new();
        let refs = count_registry_pubkeys(&block, |key| visited.push(*key)).unwrap();

        assert_eq!(refs, 10);
        assert_eq!(
            visited,
            [2u8, 3, 4, 5, 6, 7, 8, 9, 14, 15]
                .map(|value| [value; 32])
                .to_vec()
        );
        assert_eq!(count_all_raw_pubkey_refs(&block).unwrap(), 15);
    }

    #[test]
    fn rekey_covers_structured_and_nested_log_pubkeys() {
        let mut block = test_block();
        let registry = (1u8..=15)
            .filter(|value| *value != 13)
            .map(|value| [value; 32])
            .collect::<Vec<_>>();
        let key_index = KeyIndex::build(registry);

        let stats = rekey_block_pubkeys(&mut block, &key_index).unwrap();
        assert_eq!(
            stats,
            PreHotRekeyStats {
                raw_seen: 15,
                rekeyed: 14,
                raw_remaining: 1,
            }
        );
        assert_eq!(count_all_raw_pubkey_refs(&block).unwrap(), 1);

        let WincodeArchiveV2Payload::Decoded { value, .. } = &block.txs[0].tx else {
            panic!("expected decoded transaction")
        };
        let OwnedCompactMessage::V0(message) = &value.message else {
            panic!("expected v0 message")
        };
        assert_eq!(
            message.account_keys[0],
            CompactPubkey::id(key_index.lookup(&[2; 32]).unwrap())
        );

        let Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) = &block.txs[0].metadata
        else {
            panic!("expected decoded metadata")
        };
        let logs = meta.logs.as_ref().unwrap();
        let LogEvent::System(SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }) = &logs.events[1]
        else {
            panic!("expected system log")
        };
        assert!(matches!(provided_addr, CompactPubkey::Id(_)));
        assert!(matches!(
            derived_addr,
            PubkeyOrString::Pubkey(CompactPubkey::Id(_))
        ));

        let LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
            account_key,
            ..
        })) = &logs.events[2]
        else {
            panic!("expected token-2022 log")
        };
        assert_eq!(*account_key, raw(13));
    }

    #[test]
    fn first_seen_interns_every_raw_reference_and_counts_duplicates() {
        let mut block = test_block();
        let expected_refs = count_all_raw_pubkey_refs(&block).unwrap();
        let mut ids = std::collections::HashMap::<[u8; 32], u32>::new();
        let mut counts = Vec::<u32>::new();

        let stats = intern_block_pubkeys(&mut block, |key| {
            if let Some(id) = ids.get(key).copied() {
                counts[id as usize - 1] += 1;
                return Ok(id);
            }
            let id = u32::try_from(ids.len() + 1).unwrap();
            ids.insert(*key, id);
            counts.push(1);
            Ok(id)
        })
        .unwrap();

        assert_eq!(stats.raw_seen, expected_refs);
        assert_eq!(stats.rekeyed, expected_refs);
        assert_eq!(stats.raw_remaining, 0);
        assert_eq!(count_all_raw_pubkey_refs(&block).unwrap(), 0);
        assert_eq!(
            counts.iter().map(|&count| u64::from(count)).sum::<u64>(),
            expected_refs
        );
        assert_eq!(ids.len(), 15);
    }

    #[test]
    fn raw_metadata_fallback_is_preserved_and_excluded_from_pubkey_traversal() {
        let mut block = test_block();
        block.txs[0].metadata = Some(WincodeArchiveV2Payload::Raw {
            bytes: vec![0xde, 0xad, 0xbe, 0xef],
            error: "scripted undecodable metadata".to_owned(),
        });
        let expected_refs = count_all_raw_pubkey_refs(&block).unwrap();
        count_registry_pubkeys(&block, |_| {}).unwrap();

        let mut next_id = 1u32;
        let stats = intern_block_pubkeys(&mut block, |_| {
            let id = next_id;
            next_id += 1;
            Ok(id)
        })
        .unwrap();
        assert_eq!(stats.raw_seen, expected_refs);
        assert_eq!(stats.raw_remaining, 0);
        let Some(WincodeArchiveV2Payload::Raw { bytes, error }) = &block.txs[0].metadata else {
            panic!("raw metadata fallback was not preserved")
        };
        assert_eq!(bytes, &[0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(error, "scripted undecodable metadata");
    }
}

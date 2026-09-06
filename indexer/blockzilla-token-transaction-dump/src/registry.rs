use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use blockzilla_program_logs::{program_logs::ProgramLog, program_logs::system_program::PubkeyOrString, program_logs::system_program::SystemAddress, program_logs::system_program::SystemProgramLog, program_logs::token_2022::Token2022Log};
use blockzilla_archive_v2::{ArchiveV2HotBlockHeader, ArchiveV2HotMessagePayload};
use blockzilla_compact::{CompactLogStream, CompactMetaV1, LogEvent, OwnedCompactRecentBlockhash};
use blockzilla_primitives::CompactPubkey;
use memmap2::{Mmap, MmapOptions};

const KEY_BYTES: usize = 32;
const REGISTRY_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// A deterministic pubkey registry.
///
/// Keys are sorted by their raw 32-byte value and have one-based IDs. ID zero
/// stays reserved for the raw [`CompactPubkey`] wire form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DensePubkeyRegistry {
    keys: Vec<[u8; KEY_BYTES]>,
}

impl DensePubkeyRegistry {
    pub fn from_keys(keys: impl IntoIterator<Item = [u8; KEY_BYTES]>) -> Result<Self> {
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        ensure!(
            keys.len() < u32::MAX as usize,
            "pubkey registry has too many keys for one-based u32 IDs"
        );
        Ok(Self { keys })
    }

    /// Load a final dense registry. The file must already use canonical sorted order.
    pub fn load(path: &Path) -> Result<Self> {
        let keys = read_raw_32_registry(path)?;
        ensure!(
            keys.windows(2).all(|pair| pair[0] < pair[1]),
            "dense pubkey registry {} is not strictly sorted and unique",
            path.display()
        );
        ensure!(
            keys.len() < u32::MAX as usize,
            "pubkey registry has too many keys for one-based u32 IDs"
        );
        Ok(Self { keys })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_raw_32_registry(path, &self.keys)
    }

    #[inline]
    pub fn keys(&self) -> &[[u8; KEY_BYTES]] {
        &self.keys
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Return the one-based dense ID for a raw key.
    #[inline]
    pub fn id(&self, key: &[u8; KEY_BYTES]) -> Option<u32> {
        self.keys
            .binary_search(key)
            .ok()
            .and_then(|index| u32::try_from(index).ok())
            .and_then(|index| index.checked_add(1))
    }

    /// Resolve a one-based dense ID.
    #[inline]
    pub fn resolve_id(&self, id: u32) -> Result<[u8; KEY_BYTES]> {
        let index = id
            .checked_sub(1)
            .context("pubkey registry ID zero is reserved")?;
        self.keys
            .get(index as usize)
            .copied()
            .with_context(|| format!("pubkey registry ID {id} is outside 1..={}", self.len()))
    }

    #[inline]
    pub fn compact(&self, key: &[u8; KEY_BYTES]) -> Result<CompactPubkey> {
        self.id(key)
            .map(CompactPubkey::Id)
            .with_context(|| "pubkey is absent from the dense registry")
    }

    pub fn id_map_to(&self, target: &Self) -> Result<OneBasedPubkeyIdMap> {
        OneBasedPubkeyIdMap::from_keys_to_target(&self.keys, target)
    }
}

/// An epoch-local source registry. File order is significant and is preserved.
#[derive(Debug, Clone)]
pub struct SourcePubkeyRegistry {
    storage: SourcePubkeyStorage,
}

#[derive(Debug, Clone)]
enum SourcePubkeyStorage {
    Owned(Vec<[u8; KEY_BYTES]>),
    Mapped(Arc<Mmap>),
}

impl Default for SourcePubkeyRegistry {
    fn default() -> Self {
        Self {
            storage: SourcePubkeyStorage::Owned(Vec::new()),
        }
    }
}

impl PartialEq for SourcePubkeyRegistry {
    fn eq(&self, other: &Self) -> bool {
        self.keys() == other.keys()
    }
}

impl Eq for SourcePubkeyRegistry {}

impl SourcePubkeyRegistry {
    pub fn from_keys(keys: Vec<[u8; KEY_BYTES]>) -> Result<Self> {
        ensure!(
            keys.len() < u32::MAX as usize,
            "source pubkey registry has too many keys for one-based u32 IDs"
        );
        Ok(Self {
            storage: SourcePubkeyStorage::Owned(keys),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        Self::from_keys(read_raw_32_registry(path)?)
    }

    /// Map a pinned source registry without copying all keys into the heap.
    pub fn map_file(file: File, path: &Path) -> Result<Self> {
        let length = file
            .metadata()
            .with_context(|| format!("inspect {}", path.display()))?
            .len();
        ensure!(
            length.is_multiple_of(KEY_BYTES as u64),
            "source pubkey registry {} byte length is not a multiple of {KEY_BYTES}",
            path.display()
        );
        ensure!(
            (length / KEY_BYTES as u64) < u64::from(u32::MAX),
            "source pubkey registry has too many keys for one-based u32 IDs"
        );
        if length == 0 {
            return Ok(Self::default());
        }
        // SAFETY: the file is an immutable, pinned generation file. Its length
        // was checked above. The caller verifies that generation again after
        // each scan phase.
        let mapped = unsafe { MmapOptions::new().map(&file) }
            .with_context(|| format!("map {}", path.display()))?;
        Ok(Self {
            storage: SourcePubkeyStorage::Mapped(Arc::new(mapped)),
        })
    }

    #[inline]
    pub fn keys(&self) -> &[[u8; KEY_BYTES]] {
        match &self.storage {
            SourcePubkeyStorage::Owned(keys) => keys,
            SourcePubkeyStorage::Mapped(bytes) => {
                // SAFETY: `[u8; 32]` has byte alignment and every bit pattern
                // is valid. `map_file` requires an exact multiple of 32 bytes.
                unsafe {
                    std::slice::from_raw_parts(
                        bytes.as_ptr().cast::<[u8; KEY_BYTES]>(),
                        bytes.len() / KEY_BYTES,
                    )
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys().is_empty()
    }

    #[inline]
    pub fn resolve_id(&self, id: u32) -> Result<[u8; KEY_BYTES]> {
        let index = id
            .checked_sub(1)
            .context("source pubkey registry ID zero is reserved")?;
        self.keys().get(index as usize).copied().with_context(|| {
            format!(
                "source pubkey registry ID {id} is outside 1..={}",
                self.len()
            )
        })
    }

    /// Resolve both source IDs and inline raw references to one raw key.
    #[inline]
    pub fn resolve(&self, reference: CompactPubkey) -> Result<[u8; KEY_BYTES]> {
        match reference {
            CompactPubkey::Id(id) => self.resolve_id(id),
            CompactPubkey::Raw(key) => Ok(key),
        }
    }

    pub fn id_map_to(&self, target: &DensePubkeyRegistry) -> Result<OneBasedPubkeyIdMap> {
        OneBasedPubkeyIdMap::from_keys_to_target(self.keys(), target)
    }
}

/// An old one-based ID to new one-based ID map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OneBasedPubkeyIdMap {
    old_to_new: Vec<u32>,
}

impl OneBasedPubkeyIdMap {
    fn from_keys_to_target(
        source_keys: &[[u8; KEY_BYTES]],
        target: &DensePubkeyRegistry,
    ) -> Result<Self> {
        let old_to_new = source_keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                target.id(key).with_context(|| {
                    format!(
                        "source pubkey registry ID {} is absent from the target registry",
                        index + 1
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { old_to_new })
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.old_to_new
    }

    #[inline]
    pub fn remap(&self, source_id: u32) -> Result<u32> {
        let index = source_id
            .checked_sub(1)
            .context("source pubkey registry ID zero is reserved")?;
        self.old_to_new
            .get(index as usize)
            .copied()
            .with_context(|| {
                format!(
                    "source pubkey registry ID {source_id} is outside the remap of {} IDs",
                    self.old_to_new.len()
                )
            })
    }

    /// Remap an ID. Inline raw keys are reconstructed through the target registry.
    pub fn remap_reference(
        &self,
        reference: &mut CompactPubkey,
        target: &DensePubkeyRegistry,
    ) -> Result<()> {
        *reference = match *reference {
            CompactPubkey::Id(id) => CompactPubkey::Id(self.remap(id)?),
            CompactPubkey::Raw(key) => target.compact(&key)?,
        };
        Ok(())
    }
}

/// A deterministic blockhash registry with zero-based IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DenseBlockhashRegistry {
    hashes: Vec<[u8; KEY_BYTES]>,
}

impl DenseBlockhashRegistry {
    pub fn from_hashes(hashes: impl IntoIterator<Item = [u8; KEY_BYTES]>) -> Result<Self> {
        let mut hashes = hashes.into_iter().collect::<Vec<_>>();
        hashes.sort_unstable();
        hashes.dedup();
        ensure!(
            hashes.len() <= u32::MAX as usize,
            "blockhash registry has too many hashes for zero-based u32 IDs"
        );
        Ok(Self { hashes })
    }

    /// Load a final dense registry. The file must already use canonical sorted order.
    pub fn load(path: &Path) -> Result<Self> {
        let hashes = read_raw_32_registry(path)?;
        ensure!(
            hashes.windows(2).all(|pair| pair[0] < pair[1]),
            "dense blockhash registry {} is not strictly sorted and unique",
            path.display()
        );
        ensure!(
            hashes.len() <= u32::MAX as usize,
            "blockhash registry has too many hashes for zero-based u32 IDs"
        );
        Ok(Self { hashes })
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        write_raw_32_registry(path, &self.hashes)
    }

    #[inline]
    pub fn hashes(&self) -> &[[u8; KEY_BYTES]] {
        &self.hashes
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// Return the zero-based dense ID for a raw hash.
    #[inline]
    pub fn id(&self, hash: &[u8; KEY_BYTES]) -> Option<u32> {
        self.hashes
            .binary_search(hash)
            .ok()
            .and_then(|index| u32::try_from(index).ok())
    }

    #[inline]
    pub fn resolve_id(&self, id: u32) -> Result<[u8; KEY_BYTES]> {
        self.hashes
            .get(id as usize)
            .copied()
            .with_context(|| format!("blockhash registry ID {id} is outside 0..{}", self.len()))
    }

    pub fn id_map_to(&self, target: &Self) -> Result<ZeroBasedBlockhashIdMap> {
        ZeroBasedBlockhashIdMap::from_hashes_to_target(&self.hashes, target)
    }
}

/// An old zero-based ID to new zero-based ID map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZeroBasedBlockhashIdMap {
    old_to_new: Vec<u32>,
}

impl ZeroBasedBlockhashIdMap {
    fn from_hashes_to_target(
        source_hashes: &[[u8; KEY_BYTES]],
        target: &DenseBlockhashRegistry,
    ) -> Result<Self> {
        let old_to_new = source_hashes
            .iter()
            .enumerate()
            .map(|(index, hash)| {
                target.id(hash).with_context(|| {
                    format!(
                        "source blockhash registry ID {index} is absent from the target registry"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { old_to_new })
    }

    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.old_to_new
    }

    #[inline]
    pub fn remap(&self, source_id: u32) -> Result<u32> {
        self.old_to_new
            .get(source_id as usize)
            .copied()
            .with_context(|| {
                format!(
                    "source blockhash registry ID {source_id} is outside the remap of {} IDs",
                    self.old_to_new.len()
                )
            })
    }
}

/// Current-epoch hashes plus the retained tail from the previous epoch.
///
/// Current IDs are nonnegative. Negative IDs address `previous_tail`, with
/// `-1` as the newest retained hash.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceBlockhashRegistry {
    current: Vec<[u8; KEY_BYTES]>,
    previous_tail: Vec<[u8; KEY_BYTES]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBlockHeaderHashes {
    pub blockhash: [u8; KEY_BYTES],
    pub previous_blockhash: [u8; KEY_BYTES],
}

impl SourceBlockhashRegistry {
    pub fn new(current: Vec<[u8; KEY_BYTES]>, previous_tail: Vec<[u8; KEY_BYTES]>) -> Result<Self> {
        ensure!(
            current.len() <= i32::MAX as usize + 1,
            "source blockhash registry has too many current hashes for signed IDs"
        );
        ensure!(
            previous_tail.len() <= i32::MAX as usize,
            "source previous blockhash tail has too many hashes for signed IDs"
        );
        Ok(Self {
            current,
            previous_tail,
        })
    }

    pub fn load(current_path: &Path, previous_tail_path: Option<&Path>) -> Result<Self> {
        let current = read_raw_32_registry(current_path)?;
        let previous_tail = previous_tail_path
            .map(read_previous_blockhash_tail)
            .transpose()?
            .unwrap_or_default();
        Self::new(current, previous_tail)
    }

    #[inline]
    pub fn current(&self) -> &[[u8; KEY_BYTES]] {
        &self.current
    }

    #[inline]
    pub fn previous_tail(&self) -> &[[u8; KEY_BYTES]] {
        &self.previous_tail
    }

    pub fn resolve_signed_id(&self, id: i32) -> Result<[u8; KEY_BYTES]> {
        if id >= 0 {
            return self.current.get(id as usize).copied().with_context(|| {
                format!(
                    "source blockhash ID {id} is outside 0..{}",
                    self.current.len()
                )
            });
        }

        let index = i64::try_from(self.previous_tail.len())?
            .checked_add(i64::from(id))
            .context("previous-tail blockhash index overflow")?;
        ensure!(
            index >= 0,
            "source previous-tail blockhash ID {id} is outside -{}..=-1",
            self.previous_tail.len()
        );
        self.previous_tail
            .get(index as usize)
            .copied()
            .with_context(|| {
                format!(
                    "source previous-tail blockhash ID {id} is outside -{}..=-1",
                    self.previous_tail.len()
                )
            })
    }

    /// Decode the signed bit pattern stored in an unsigned hot-header field.
    #[inline]
    pub fn resolve_header_id(&self, id: u32) -> Result<[u8; KEY_BYTES]> {
        self.resolve_signed_id(i32::from_ne_bytes(id.to_ne_bytes()))
    }

    pub fn resolve_recent_blockhash(
        &self,
        recent_blockhash: &OwnedCompactRecentBlockhash,
    ) -> Result<[u8; KEY_BYTES]> {
        match recent_blockhash {
            OwnedCompactRecentBlockhash::Id(id) => self.resolve_signed_id(*id),
            OwnedCompactRecentBlockhash::Nonce(hash) => Ok(*hash),
        }
    }

    /// Resolve both block-header hashes.
    ///
    /// At an epoch boundary the historical 0/0 header shape means that the
    /// previous hash is the newest item in the prior-epoch tail.
    pub fn resolve_header_hashes(
        &self,
        header: &ArchiveV2HotBlockHeader,
    ) -> Result<ResolvedBlockHeaderHashes> {
        let blockhash = self.resolve_header_id(header.blockhash_id)?;
        let previous_blockhash = if header.blockhash_id == 0
            && header.previous_blockhash_id == 0
            && let Some(previous) = self.previous_tail.last()
        {
            *previous
        } else {
            self.resolve_header_id(header.previous_blockhash_id)?
        };
        Ok(ResolvedBlockHeaderHashes {
            blockhash,
            previous_blockhash,
        })
    }
}

/// Replace a source ID or inline raw key with its one-based dense ID.
pub fn rewrite_pubkey_to_dense(
    reference: &mut CompactPubkey,
    source: &SourcePubkeyRegistry,
    target: &DensePubkeyRegistry,
) -> Result<()> {
    let key = source.resolve(*reference)?;
    *reference = target.compact(&key)?;
    Ok(())
}

/// Replace a signed source recent-blockhash ID with a zero-based dense ID.
/// Durable nonce hashes stay inline.
pub fn rewrite_recent_blockhash_to_dense(
    recent_blockhash: &mut OwnedCompactRecentBlockhash,
    source: &SourceBlockhashRegistry,
    target: &DenseBlockhashRegistry,
) -> Result<()> {
    let OwnedCompactRecentBlockhash::Id(source_id) = recent_blockhash else {
        return Ok(());
    };
    let hash = source.resolve_signed_id(*source_id)?;
    let target_id = target
        .id(&hash)
        .context("recent blockhash is absent from the dense registry")?;
    *source_id = i32::try_from(target_id).context("dense blockhash ID exceeds i32::MAX")?;
    Ok(())
}

/// Mutate every typed pubkey reference in a hot message.
///
/// The match is exhaustive. A new message variant cannot compile until this
/// visitor is updated.
pub fn visit_message_pubkeys_mut(
    message: &mut ArchiveV2HotMessagePayload,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => {
            for key in &mut message.account_keys {
                visitor(key)?;
            }
        }
        ArchiveV2HotMessagePayload::V0(message) => {
            for key in &mut message.account_keys {
                visitor(key)?;
            }
            for lookup in &mut message.address_table_lookups {
                visitor(&mut lookup.account_key)?;
            }
        }
    }
    Ok(())
}

/// Mutate the recent-blockhash reference in either hot-message variant.
pub fn visit_message_recent_blockhash_mut(
    message: &mut ArchiveV2HotMessagePayload,
    visitor: &mut impl FnMut(&mut OwnedCompactRecentBlockhash) -> Result<()>,
) -> Result<()> {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => visitor(&mut message.recent_blockhash),
        ArchiveV2HotMessagePayload::V0(message) => visitor(&mut message.recent_blockhash),
    }
}

/// Reconstruct all source references in a message and assign dedicated dense IDs.
pub fn rewrite_message_to_dense(
    message: &mut ArchiveV2HotMessagePayload,
    source_pubkeys: &SourcePubkeyRegistry,
    target_pubkeys: &DensePubkeyRegistry,
    source_blockhashes: &SourceBlockhashRegistry,
    target_blockhashes: &DenseBlockhashRegistry,
) -> Result<()> {
    visit_message_pubkeys_mut(message, &mut |reference| {
        rewrite_pubkey_to_dense(reference, source_pubkeys, target_pubkeys)
    })?;
    visit_message_recent_blockhash_mut(message, &mut |recent| {
        rewrite_recent_blockhash_to_dense(recent, source_blockhashes, target_blockhashes)
    })
}

/// Mutate every typed pubkey reference in transaction metadata.
///
/// This includes references embedded in compact runtime logs. The match trees
/// below are exhaustive for `LogEvent`, `ProgramLog`, `SystemProgramLog`, and
/// `Token2022Log`.
pub fn visit_metadata_pubkeys_mut(
    metadata: &mut CompactMetaV1,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    if let Some(logs) = &mut metadata.logs {
        visit_log_stream_pubkeys_mut(logs, visitor)?;
    }

    for balance in metadata
        .pre_token_balances
        .iter_mut()
        .chain(&mut metadata.post_token_balances)
    {
        if let Some(mint) = &mut balance.mint {
            visitor(mint)?;
        }
        if let Some(owner) = &mut balance.owner {
            visitor(owner)?;
        }
        if let Some(program_id) = &mut balance.program_id {
            visitor(program_id)?;
        }
    }

    for reward in &mut metadata.rewards {
        visitor(&mut reward.pubkey)?;
    }
    for key in &mut metadata.loaded_writable_addresses {
        visitor(key)?;
    }
    for key in &mut metadata.loaded_readonly_addresses {
        visitor(key)?;
    }
    if let Some(return_data) = &mut metadata.return_data {
        visitor(&mut return_data.program_id)?;
    }
    Ok(())
}

/// Reconstruct all source references in metadata and assign dedicated dense IDs.
pub fn rewrite_metadata_to_dense(
    metadata: &mut CompactMetaV1,
    source: &SourcePubkeyRegistry,
    target: &DensePubkeyRegistry,
) -> Result<()> {
    visit_metadata_pubkeys_mut(metadata, &mut |reference| {
        rewrite_pubkey_to_dense(reference, source, target)
    })
}

fn visit_log_stream_pubkeys_mut(
    logs: &mut CompactLogStream,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    for event in &mut logs.events {
        visit_log_event_pubkeys_mut(event, visitor)?;
    }
    Ok(())
}

fn visit_log_event_pubkeys_mut(
    event: &mut LogEvent,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match event {
        LogEvent::System(log) => visit_system_program_log_pubkeys_mut(log, visitor)?,
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
        | LogEvent::Return { program, .. } => visitor(program)?,
        LogEvent::LoaderFinalizedAccount { account }
        | LogEvent::RuntimeWritablePrivilegeEscalated { account }
        | LogEvent::RuntimeSignerPrivilegeEscalated { account }
        | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => visitor(account)?,
        LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
            visit_program_log_pubkeys_mut(log, visitor)?;
        }
        LogEvent::ProgramIdLog { program, log } => {
            visitor(program)?;
            visit_program_log_pubkeys_mut(log, visitor)?;
        }
        LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
            if let Some(program) = program {
                visitor(program)?;
            }
        }
        LogEvent::LogTruncated
        | LogEvent::StakeMergingAccounts
        | LogEvent::ProgramLogError { .. }
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
        | LogEvent::CloseContextState
        | LogEvent::Plain { .. }
        | LogEvent::Unparsed { .. } => {}
    }
    Ok(())
}

fn visit_program_log_pubkeys_mut(
    log: &mut ProgramLog,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match log {
        ProgramLog::Token2022(log) => visit_token_2022_log_pubkeys_mut(log, visitor)?,
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
        | ProgramLog::Unknown(_)
        | ProgramLog::Known(_) => {}
    }
    Ok(())
}

fn visit_token_2022_log_pubkeys_mut(
    log: &mut Token2022Log,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match log {
        Token2022Log::ErrorHarvestingFrom { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, .. } => visitor(account_key)?,
        Token2022Log::Error(_)
        | Token2022Log::Static(_)
        | Token2022Log::CalculatedFee { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug { .. }
        | Token2022Log::AccountNeedsResizePlusBytesDebug2 { .. } => {}
    }
    Ok(())
}

fn visit_system_program_log_pubkeys_mut(
    log: &mut SystemProgramLog,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            visitor(provided_addr)?;
            visit_pubkey_or_string_mut(derived_addr, visitor)?;
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            visit_system_address_mut(addr, visitor)?;
        }
        SystemProgramLog::TransferFromMustSign { from } => visitor(from)?,
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            visit_pubkey_or_string_mut(account, visitor)?;
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
    Ok(())
}

fn visit_system_address_mut(
    address: &mut SystemAddress,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match address {
        SystemAddress::Pubkey(value) => visit_pubkey_or_string_mut(value, visitor),
        SystemAddress::Debug { address, base } => {
            visit_pubkey_or_string_mut(address, visitor)?;
            if let Some(base) = base {
                visit_pubkey_or_string_mut(base, visitor)?;
            }
            Ok(())
        }
    }
}

fn visit_pubkey_or_string_mut(
    value: &mut PubkeyOrString,
    visitor: &mut impl FnMut(&mut CompactPubkey) -> Result<()>,
) -> Result<()> {
    match value {
        PubkeyOrString::Pubkey(pubkey) => visitor(pubkey),
        PubkeyOrString::Text(_) => Ok(()),
    }
}

fn read_raw_32_registry(path: &Path) -> Result<Vec<[u8; KEY_BYTES]>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let byte_len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    ensure!(
        byte_len.is_multiple_of(KEY_BYTES as u64),
        "registry {} has {byte_len} bytes, not a multiple of {KEY_BYTES}",
        path.display()
    );
    let key_count =
        usize::try_from(byte_len / KEY_BYTES as u64).context("registry key count exceeds usize")?;
    let mut reader = BufReader::with_capacity(REGISTRY_BUFFER_BYTES, file);
    let mut keys = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let mut key = [0u8; KEY_BYTES];
        reader
            .read_exact(&mut key)
            .with_context(|| format!("read {} key {index}", path.display()))?;
        keys.push(key);
    }
    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing)? != 0 {
        bail!("registry {} changed while it was read", path.display());
    }
    Ok(keys)
}

fn write_raw_32_registry(path: &Path, keys: &[[u8; KEY_BYTES]]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(REGISTRY_BUFFER_BYTES, file);
    for key in keys {
        writer
            .write_all(key)
            .with_context(|| format!("write {}", path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    Ok(())
}

/// Read the published previous-epoch blockhash tail.
///
/// Current Archive V2 rows contain a 32-byte hash followed by a little-endian
/// slot. The early hash-only sidecar used 32-byte rows. Published current
/// generations select the 40-byte form when the length permits it, as the
/// Archive V2 reader does.
fn read_previous_blockhash_tail(path: &Path) -> Result<Vec<[u8; KEY_BYTES]>> {
    let mut bytes = Vec::new();
    BufReader::with_capacity(REGISTRY_BUFFER_BYTES, File::open(path)?)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let stride = if bytes.len().is_multiple_of(40) {
        40
    } else if bytes.len().is_multiple_of(KEY_BYTES) {
        KEY_BYTES
    } else {
        bail!(
            "previous blockhash tail {} has invalid byte length {}",
            path.display(),
            bytes.len()
        );
    };
    Ok(bytes
        .chunks_exact(stride)
        .map(|row| row[..KEY_BYTES].try_into().expect("checked 32-byte prefix"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_program_logs::program_logs::system_program::NonceAction;
    use blockzilla_archive_v2::{ArchiveV2HotLegacyMessage, ArchiveV2HotV0Message};
    use blockzilla_compact::{CompactMessageHeader, CompactReturnData, CompactReward, CompactTokenBalance, DataTable, OwnedCompactAddressTableLookup};
    use blockzilla_primitives::StringTable;
    use tempfile::tempdir;

    fn key(marker: u8) -> [u8; KEY_BYTES] {
        [marker; KEY_BYTES]
    }

    fn header(blockhash_id: u32, previous_blockhash_id: u32) -> ArchiveV2HotBlockHeader {
        ArchiveV2HotBlockHeader {
            slot: 1,
            parent_slot: 0,
            blockhash_id,
            previous_blockhash_id,
            block_time: None,
            block_height: None,
            rewards: None,
        }
    }

    #[test]
    fn dense_pubkeys_are_sorted_unique_and_one_based() {
        let registry = DensePubkeyRegistry::from_keys([key(3), key(1), key(3), key(2)]).unwrap();
        assert_eq!(registry.keys(), &[key(1), key(2), key(3)]);
        assert_eq!(registry.id(&key(1)), Some(1));
        assert_eq!(registry.id(&key(3)), Some(3));
        assert_eq!(
            registry.resolve_id(0).unwrap_err().to_string(),
            "pubkey registry ID zero is reserved"
        );

        let directory = tempdir().unwrap();
        let path = directory.path().join("registry.bin");
        registry.write(&path).unwrap();
        assert_eq!(DensePubkeyRegistry::load(&path).unwrap(), registry);
    }

    #[test]
    fn source_ids_and_raw_keys_reconstruct_to_dense_ids() {
        let source = SourcePubkeyRegistry::from_keys(vec![key(9), key(2)]).unwrap();
        let target = DensePubkeyRegistry::from_keys([key(2), key(9), key(7)]).unwrap();
        let map = source.id_map_to(&target).unwrap();
        assert_eq!(map.as_slice(), &[3, 1]);

        let mut indexed = CompactPubkey::Id(1);
        map.remap_reference(&mut indexed, &target).unwrap();
        assert_eq!(indexed, CompactPubkey::Id(3));

        let mut raw = CompactPubkey::Raw(key(7));
        map.remap_reference(&mut raw, &target).unwrap();
        assert_eq!(raw, CompactPubkey::Id(2));
        assert_eq!(source.resolve(CompactPubkey::Id(2)).unwrap(), key(2));
        assert_eq!(source.resolve(CompactPubkey::Raw(key(8))).unwrap(), key(8));
    }

    #[test]
    fn source_registry_can_resolve_from_a_read_only_mapping() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("source-registry.bin");
        write_raw_32_registry(&path, &[key(5), key(8)]).unwrap();
        let source = SourcePubkeyRegistry::map_file(File::open(&path).unwrap(), &path).unwrap();

        assert_eq!(source.len(), 2);
        assert_eq!(source.keys(), &[key(5), key(8)]);
        assert_eq!(source.resolve_id(1).unwrap(), key(5));
        assert_eq!(source.resolve_id(2).unwrap(), key(8));
    }

    #[test]
    fn dense_blockhashes_are_zero_based_and_remappable() {
        let source = DenseBlockhashRegistry::from_hashes([key(4), key(1)]).unwrap();
        let target = DenseBlockhashRegistry::from_hashes([key(4), key(1), key(2)]).unwrap();
        assert_eq!(source.hashes(), &[key(1), key(4)]);
        assert_eq!(source.id(&key(1)), Some(0));
        assert_eq!(source.id(&key(4)), Some(1));
        assert_eq!(source.id_map_to(&target).unwrap().as_slice(), &[0, 2]);

        let directory = tempdir().unwrap();
        let path = directory.path().join("blockhashes.bin");
        target.write(&path).unwrap();
        assert_eq!(DenseBlockhashRegistry::load(&path).unwrap(), target);
    }

    #[test]
    fn signed_tail_and_epoch_boundary_header_are_resolved() {
        let source =
            SourceBlockhashRegistry::new(vec![key(10), key(11)], vec![key(7), key(8), key(9)])
                .unwrap();
        assert_eq!(source.resolve_signed_id(0).unwrap(), key(10));
        assert_eq!(source.resolve_signed_id(-1).unwrap(), key(9));
        assert_eq!(source.resolve_signed_id(-3).unwrap(), key(7));
        assert!(source.resolve_signed_id(-4).is_err());
        assert_eq!(source.resolve_header_id(u32::MAX).unwrap(), key(9));

        assert_eq!(
            source.resolve_header_hashes(&header(0, 0)).unwrap(),
            ResolvedBlockHeaderHashes {
                blockhash: key(10),
                previous_blockhash: key(9),
            }
        );
        assert_eq!(
            source.resolve_header_hashes(&header(1, 0)).unwrap(),
            ResolvedBlockHeaderHashes {
                blockhash: key(11),
                previous_blockhash: key(10),
            }
        );
    }

    #[test]
    fn current_previous_tail_file_keeps_hashes_and_skips_slots() {
        let directory = tempdir().unwrap();
        let current_path = directory.path().join("blockhash_registry.bin");
        let tail_path = directory.path().join("prev_blockhash_tail.bin");
        write_raw_32_registry(&current_path, &[key(10)]).unwrap();

        let mut tail = Vec::new();
        tail.extend_from_slice(&key(8));
        tail.extend_from_slice(&346_031_998u64.to_le_bytes());
        tail.extend_from_slice(&key(9));
        tail.extend_from_slice(&346_031_999u64.to_le_bytes());
        std::fs::write(&tail_path, tail).unwrap();

        let source = SourceBlockhashRegistry::load(&current_path, Some(&tail_path)).unwrap();
        assert_eq!(source.previous_tail(), &[key(8), key(9)]);
        assert_eq!(source.resolve_signed_id(-1).unwrap(), key(9));
    }

    #[test]
    fn message_account_lookup_and_recent_hash_references_are_densified() {
        let source_pubkeys = SourcePubkeyRegistry::from_keys(vec![key(9), key(2)]).unwrap();
        let target_pubkeys = DensePubkeyRegistry::from_keys([key(9), key(2), key(5)]).unwrap();
        let source_hashes =
            SourceBlockhashRegistry::new(vec![key(31)], vec![key(29), key(30)]).unwrap();
        let target_hashes = DenseBlockhashRegistry::from_hashes([key(30), key(31)]).unwrap();
        let mut message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw(key(5))],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-1),
            instructions: Vec::new(),
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(2),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });

        rewrite_message_to_dense(
            &mut message,
            &source_pubkeys,
            &target_pubkeys,
            &source_hashes,
            &target_hashes,
        )
        .unwrap();

        let ArchiveV2HotMessagePayload::V0(message) = message else {
            unreachable!();
        };
        assert_eq!(
            message.account_keys,
            [CompactPubkey::Id(3), CompactPubkey::Id(2)]
        );
        assert_eq!(
            message.address_table_lookups[0].account_key,
            CompactPubkey::Id(1)
        );
        assert!(matches!(
            message.recent_blockhash,
            OwnedCompactRecentBlockhash::Id(0)
        ));

        let mut legacy = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: message.header,
            account_keys: vec![CompactPubkey::Raw(key(5))],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce(key(27)),
            instructions: Vec::new(),
        });
        rewrite_message_to_dense(
            &mut legacy,
            &source_pubkeys,
            &target_pubkeys,
            &source_hashes,
            &target_hashes,
        )
        .unwrap();
        let ArchiveV2HotMessagePayload::Legacy(legacy) = legacy else {
            unreachable!();
        };
        assert_eq!(legacy.account_keys, [CompactPubkey::Id(2)]);
        assert!(
            matches!(legacy.recent_blockhash, OwnedCompactRecentBlockhash::Nonce(hash) if hash == key(27))
        );
    }

    #[test]
    fn metadata_principal_and_nested_log_pubkeys_are_densified() {
        let source = SourcePubkeyRegistry::from_keys((1..=20).rev().map(key).collect()).unwrap();
        let target = DensePubkeyRegistry::from_keys((1..=20).map(key)).unwrap();
        let source_ref = |marker: u8| CompactPubkey::Id(u32::from(21 - marker));

        let logs = CompactLogStream {
            events: vec![
                LogEvent::Invoke {
                    program: source_ref(10),
                    depth: 1,
                },
                LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                    provided_addr: source_ref(11),
                    derived_addr: PubkeyOrString::Pubkey(source_ref(12)),
                }),
                LogEvent::System(SystemProgramLog::AllocateAlreadyInUse {
                    addr: SystemAddress::Debug {
                        address: PubkeyOrString::Pubkey(source_ref(13)),
                        base: Some(PubkeyOrString::Pubkey(source_ref(14))),
                    },
                }),
                LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                    account_key: source_ref(15),
                    error: 0,
                })),
                LogEvent::ProgramIdLog {
                    program: source_ref(16),
                    log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom2 {
                        account_key: source_ref(17),
                        error: 0,
                    }),
                },
                LogEvent::ProgramNotDeployed {
                    program: Some(source_ref(18)),
                },
                LogEvent::System(SystemProgramLog::NonceAccountMustSign {
                    action: NonceAction::Advance,
                    account: PubkeyOrString::Pubkey(source_ref(19)),
                }),
                LogEvent::RuntimeAccountOwnerBalanceVerificationFailed {
                    account: source_ref(20),
                },
            ],
            strings: StringTable::default(),
            data: DataTable::default(),
        };
        let mut metadata = CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: Some(logs),
            pre_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(source_ref(1)),
                owner: Some(source_ref(2)),
                program_id: Some(source_ref(3)),
                amount: 1,
                decimals: 6,
            }],
            post_token_balances: vec![CompactTokenBalance {
                account_index: 0,
                mint: Some(CompactPubkey::Raw(key(4))),
                owner: None,
                program_id: None,
                amount: 2,
                decimals: 6,
            }],
            rewards: vec![CompactReward {
                pubkey: source_ref(5),
                lamports: 1,
                post_balance: 2,
                reward_type: 0,
                commission: None,
            }],
            loaded_writable_addresses: vec![source_ref(6)],
            loaded_readonly_addresses: vec![CompactPubkey::Raw(key(7))],
            return_data: Some(CompactReturnData {
                program_id: source_ref(8),
                data: vec![1],
            }),
            compute_units_consumed: Some(1),
            cost_units: Some(2),
        };

        rewrite_metadata_to_dense(&mut metadata, &source, &target).unwrap();
        let mut ids = Vec::new();
        visit_metadata_pubkeys_mut(&mut metadata, &mut |reference| {
            let CompactPubkey::Id(id) = reference else {
                bail!("metadata retained a raw pubkey");
            };
            ids.push(*id);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            ids,
            vec![
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 2, 3, 4, 5, 6, 7, 8,
            ]
        );
    }
}

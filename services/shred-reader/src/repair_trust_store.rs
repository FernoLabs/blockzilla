//! Thread-safe trust anchors for admitting recent-slot repair responses.
//!
//! This store learns fork-selecting FEC identities from the original Turbine receive path.
//! Callers pass the serialized UDP shred to [`RepairTrustStore::observe_turbine_packet`]; the store
//! reparses, sanitizes, version-checks, and leader-verifies it before retaining any identity. A
//! separately named post-WAL promotion may extend a root strictly backwards through Agave's
//! leader-signed chained roots; repair bytes must never be fed through the Turbine method.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fmt,
    net::SocketAddr,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use solana_ledger::shred::{
    DATA_SHREDS_PER_FEC_BLOCK, MAX_CODE_SHREDS_PER_SLOT, MAX_DATA_SHREDS_PER_SLOT, Shred,
};
use solana_pubkey::Pubkey;

use crate::{
    repair_runtime::{ChainedMerkleRootExpectation, RepairPeer, RepairTrust, TrustedFecIdentity},
    repair_tracker::RepairRequest,
    repair_wire::ShredRepairRequest,
};

const CODING_NUM_DATA_OFFSET: usize = 83;
const CODING_NUM_CODING_OFFSET: usize = 85;
const CODING_POSITION_OFFSET: usize = 87;

/// Synchronous slot-leader lookup used by the receive and repair trust paths.
///
/// A small adapter around `leader_schedule::LeaderScheduleCache` can implement this trait without
/// making the trust store depend on a particular cache implementation or async runtime.
pub trait SlotLeaderLookup: Send + Sync + 'static {
    fn slot_leader(&self, slot: u64) -> Option<Pubkey>;
}

impl<F> SlotLeaderLookup for F
where
    F: Fn(u64) -> Option<Pubkey> + Send + Sync + 'static,
{
    fn slot_leader(&self, slot: u64) -> Option<Pubkey> {
        self(slot)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepairTrustStoreConfig {
    pub shred_version: u16,
    pub max_slots: usize,
    pub max_fec_sets_per_slot: usize,
    pub max_authorized_peers: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairTrustStoreBuildError {
    ZeroBound(&'static str),
    NoAuthorizedPeers,
    TooManyAuthorizedPeers { actual: usize, maximum: usize },
    DuplicatePeerPubkey { pubkey: Pubkey },
    DuplicatePeerAddress { address: SocketAddr },
}

impl fmt::Display for RepairTrustStoreBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBound(name) => write!(formatter, "{name} must be nonzero"),
            Self::NoAuthorizedPeers => write!(formatter, "at least one repair peer is required"),
            Self::TooManyAuthorizedPeers { actual, maximum } => write!(
                formatter,
                "{actual} repair peers exceed the configured maximum of {maximum}"
            ),
            Self::DuplicatePeerPubkey { pubkey } => {
                write!(
                    formatter,
                    "repair peer pubkey {pubkey} is configured more than once"
                )
            }
            Self::DuplicatePeerAddress { address } => {
                write!(
                    formatter,
                    "repair peer address {address} is configured more than once"
                )
            }
        }
    }
}

impl Error for RepairTrustStoreBuildError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TurbineTrustError {
    MalformedShred,
    ShredSanitizeFailed,
    ShredVersionMismatch {
        expected: u16,
        actual: u16,
    },
    InvalidFecLayout {
        fec_set_index: u32,
        shred_index: u32,
    },
    MissingMerkleRoot,
    MissingChainedMerkleRoot,
    MissingSlotLeader {
        slot: u64,
    },
    InvalidSlotLeaderSignature {
        slot: u64,
    },
    TrustStateUnavailable,
}

impl fmt::Display for TurbineTrustError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedShred => write!(formatter, "Turbine packet is not a serialized shred"),
            Self::ShredSanitizeFailed => {
                write!(formatter, "Turbine shred failed structural sanitization")
            }
            Self::ShredVersionMismatch { expected, actual } => write!(
                formatter,
                "Turbine shred version {actual} does not match configured version {expected}"
            ),
            Self::InvalidFecLayout {
                fec_set_index,
                shred_index,
            } => write!(
                formatter,
                "Turbine shred index {shred_index} is invalid for FEC set {fec_set_index}"
            ),
            Self::MissingMerkleRoot => write!(formatter, "Turbine shred has no Merkle root"),
            Self::MissingChainedMerkleRoot => {
                write!(formatter, "Turbine shred has no chained Merkle root")
            }
            Self::MissingSlotLeader { slot } => {
                write!(
                    formatter,
                    "slot leader is unavailable for Turbine slot {slot}"
                )
            }
            Self::InvalidSlotLeaderSignature { slot } => {
                write!(
                    formatter,
                    "Turbine shred signature is invalid for slot {slot}"
                )
            }
            Self::TrustStateUnavailable => {
                write!(formatter, "repair trust state lock is poisoned")
            }
        }
    }
}

impl Error for TurbineTrustError {}

/// A conflict is sticky for the lifetime of the retained slot.  The store will not provide any
/// trust material or authorize any request for a blocked slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairTrustConflict {
    FecIdentity {
        fec_set_index: u32,
        first: TrustedFecIdentity,
        conflicting: TrustedFecIdentity,
    },
    FecCapacityExceeded {
        rejected_fec_set_index: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TurbineTrustObservation {
    Inserted {
        slot: u64,
        fec_set_index: u32,
        evicted_slot: Option<u64>,
    },
    Duplicate {
        slot: u64,
        fec_set_index: u32,
    },
    Blocked {
        slot: u64,
        conflict: RepairTrustConflict,
    },
    /// A late packet older than the bounded recent-slot window cannot displace newer trust state.
    IgnoredTooOld {
        slot: u64,
        oldest_retained_slot: u64,
    },
}

#[derive(Debug, Default)]
struct SlotTrustState {
    fec_sets: BTreeMap<u32, TrustedFecIdentity>,
    blocked_by: Option<RepairTrustConflict>,
}

#[derive(Debug, Default)]
struct TrustState {
    slots: BTreeMap<u64, SlotTrustState>,
}

/// Bounded, thread-safe implementation of [`RepairTrust`].
#[derive(Clone)]
pub struct RepairTrustStore {
    config: RepairTrustStoreConfig,
    authorized_peers: Arc<HashSet<(Pubkey, SocketAddr)>>,
    leader_lookup: Arc<dyn SlotLeaderLookup>,
    state: Arc<RwLock<TrustState>>,
}

impl RepairTrustStore {
    pub fn new<I, L>(
        config: RepairTrustStoreConfig,
        authorized_peers: I,
        leader_lookup: L,
    ) -> Result<Self, RepairTrustStoreBuildError>
    where
        I: IntoIterator<Item = RepairPeer>,
        L: SlotLeaderLookup,
    {
        for (name, value) in [
            ("max_slots", config.max_slots),
            ("max_fec_sets_per_slot", config.max_fec_sets_per_slot),
            ("max_authorized_peers", config.max_authorized_peers),
        ] {
            if value == 0 {
                return Err(RepairTrustStoreBuildError::ZeroBound(name));
            }
        }

        let peers: Vec<_> = authorized_peers.into_iter().collect();
        if peers.is_empty() {
            return Err(RepairTrustStoreBuildError::NoAuthorizedPeers);
        }
        if peers.len() > config.max_authorized_peers {
            return Err(RepairTrustStoreBuildError::TooManyAuthorizedPeers {
                actual: peers.len(),
                maximum: config.max_authorized_peers,
            });
        }

        let mut peer_pubkeys = HashSet::with_capacity(peers.len());
        let mut peer_addresses = HashSet::with_capacity(peers.len());
        let mut exact_peers = HashSet::with_capacity(peers.len());
        for peer in peers {
            if !peer_pubkeys.insert(peer.pubkey) {
                return Err(RepairTrustStoreBuildError::DuplicatePeerPubkey {
                    pubkey: peer.pubkey,
                });
            }
            if !peer_addresses.insert(peer.repair_addr) {
                return Err(RepairTrustStoreBuildError::DuplicatePeerAddress {
                    address: peer.repair_addr,
                });
            }
            exact_peers.insert((peer.pubkey, peer.repair_addr));
        }

        Ok(Self {
            config,
            authorized_peers: Arc::new(exact_peers),
            leader_lookup: Arc::new(leader_lookup),
            state: Arc::new(RwLock::new(TrustState::default())),
        })
    }

    /// Learn one FEC identity from the original Turbine UDP receive path.
    ///
    /// Parsing and verification happen inside the trust boundary.  In particular, this method does
    /// not accept pre-extracted `(slot, FEC, root)` fields that could accidentally originate from a
    /// repair response.
    pub fn observe_turbine_packet(
        &self,
        payload: &[u8],
    ) -> Result<TurbineTrustObservation, TurbineTrustError> {
        let shred = Shred::new_from_serialized_shred(payload.to_vec())
            .map_err(|_| TurbineTrustError::MalformedShred)?;
        shred
            .sanitize()
            .map_err(|_| TurbineTrustError::ShredSanitizeFailed)?;

        if shred.version() != self.config.shred_version {
            return Err(TurbineTrustError::ShredVersionMismatch {
                expected: self.config.shred_version,
                actual: shred.version(),
            });
        }
        if !valid_fixed_fec_layout(&shred) {
            return Err(TurbineTrustError::InvalidFecLayout {
                fec_set_index: shred.fec_set_index(),
                shred_index: shred.index(),
            });
        }

        let identity = TrustedFecIdentity {
            merkle_root: shred
                .merkle_root()
                .map_err(|_| TurbineTrustError::MissingMerkleRoot)?,
            chained_merkle_root: ChainedMerkleRootExpectation::Exact(Some(
                shred
                    .chained_merkle_root()
                    .map_err(|_| TurbineTrustError::MissingChainedMerkleRoot)?,
            )),
            trust_anchor_fec_set_index: shred.fec_set_index(),
        };
        let slot = shred.slot();
        let Some(leader) = self.leader_lookup.slot_leader(slot) else {
            return Err(TurbineTrustError::MissingSlotLeader { slot });
        };
        if !shred.verify(&leader) {
            return Err(TurbineTrustError::InvalidSlotLeaderSignature { slot });
        }

        let fec_set_index = shred.fec_set_index();
        let mut state = self.write_state()?;
        let mut evicted_slot = None;
        if !state.slots.contains_key(&slot) && state.slots.len() >= self.config.max_slots {
            let oldest_retained_slot = *state
                .slots
                .first_key_value()
                .expect("a full nonzero-capacity slot map is nonempty")
                .0;
            if slot <= oldest_retained_slot {
                return Ok(TurbineTrustObservation::IgnoredTooOld {
                    slot,
                    oldest_retained_slot,
                });
            }
            state.slots.remove(&oldest_retained_slot);
            evicted_slot = Some(oldest_retained_slot);
        }

        let slot_state = state.slots.entry(slot).or_default();
        if let Some(conflict) = slot_state.blocked_by.clone() {
            return Ok(TurbineTrustObservation::Blocked { slot, conflict });
        }
        if let Some(first) = slot_state.fec_sets.get(&fec_set_index).copied() {
            if exact_commitment_matches(first, identity) {
                // Direct leader-verified Turbine evidence is stronger provenance than a root that
                // was previously promoted backwards from a successor repair.
                if first.trust_anchor_fec_set_index != fec_set_index {
                    slot_state.fec_sets.insert(fec_set_index, identity);
                }
                return Ok(TurbineTrustObservation::Duplicate {
                    slot,
                    fec_set_index,
                });
            }
            let conflict = RepairTrustConflict::FecIdentity {
                fec_set_index,
                first,
                conflicting: identity,
            };
            slot_state.blocked_by = Some(conflict.clone());
            return Ok(TurbineTrustObservation::Blocked { slot, conflict });
        }
        if let Some(anchored) = trusted_fec_identity_in_slot(slot_state, fec_set_index)
            && anchored.merkle_root != identity.merkle_root
        {
            let conflict = RepairTrustConflict::FecIdentity {
                fec_set_index,
                first: anchored,
                conflicting: identity,
            };
            slot_state.blocked_by = Some(conflict.clone());
            return Ok(TurbineTrustObservation::Blocked { slot, conflict });
        }
        if !chain_is_consistent_with_neighbors(slot_state, fec_set_index, identity) {
            let first = trusted_fec_identity_in_slot(slot_state, fec_set_index).unwrap_or(identity);
            let conflict = RepairTrustConflict::FecIdentity {
                fec_set_index,
                first,
                conflicting: identity,
            };
            slot_state.blocked_by = Some(conflict.clone());
            return Ok(TurbineTrustObservation::Blocked { slot, conflict });
        }
        if slot_state.fec_sets.len() >= self.config.max_fec_sets_per_slot {
            let conflict = RepairTrustConflict::FecCapacityExceeded {
                rejected_fec_set_index: fec_set_index,
            };
            slot_state.blocked_by = Some(conflict.clone());
            return Ok(TurbineTrustObservation::Blocked { slot, conflict });
        }

        slot_state.fec_sets.insert(fec_set_index, identity);
        Ok(TurbineTrustObservation::Inserted {
            slot,
            fec_set_index,
            evicted_slot,
        })
    }

    /// Preflight a tracker request before allocating a nonce or sending network traffic.
    pub fn can_request(&self, request: &RepairRequest) -> bool {
        match *request {
            RepairRequest::WindowIndex { slot, index } => {
                self.can_request_wire(ShredRepairRequest::Shred {
                    slot,
                    shred_index: u64::from(index),
                })
            }
            RepairRequest::HighestWindowIndex {
                slot,
                next_expected_data_index,
            } => self.can_request_wire(ShredRepairRequest::HighestShred {
                slot,
                shred_index: u64::from(next_expected_data_index),
            }),
        }
    }

    /// Preflight a wire request. Exact requests require a trusted FEC covering the index. Highest
    /// requests require at least one already-trusted FEC that could contain a matching response.
    /// Orphan repair is intentionally unsupported because it has no requested data index to bind.
    pub fn can_request_wire(&self, request: ShredRepairRequest) -> bool {
        let Some(state) = self.read_state() else {
            return false;
        };
        match request {
            ShredRepairRequest::Shred { slot, shred_index } => {
                let Ok(index) = u32::try_from(shred_index) else {
                    return false;
                };
                trusted_slot(&state, slot).is_some_and(|slot_state| {
                    let fec_set_index =
                        index / DATA_SHREDS_PER_FEC_BLOCK as u32 * DATA_SHREDS_PER_FEC_BLOCK as u32;
                    data_index_is_in_fec(index, fec_set_index)
                        && trusted_fec_identity_in_slot(slot_state, fec_set_index).is_some()
                })
            }
            ShredRepairRequest::HighestShred { slot, shred_index } => {
                let Ok(first_index) = u32::try_from(shred_index) else {
                    return false;
                };
                trusted_slot(&state, slot).is_some_and(|slot_state| {
                    slot_state.fec_sets.keys().copied().any(|fec_set_index| {
                        [
                            Some(fec_set_index),
                            fec_set_index.checked_sub(DATA_SHREDS_PER_FEC_BLOCK as u32),
                        ]
                        .into_iter()
                        .flatten()
                        .any(|candidate| {
                            candidate
                                .checked_add(DATA_SHREDS_PER_FEC_BLOCK as u32)
                                .is_some_and(|end| first_index < end)
                                && trusted_fec_identity_in_slot(slot_state, candidate).is_some()
                        })
                    })
                })
            }
            ShredRepairRequest::Orphan { .. } => false,
        }
    }

    pub fn trusted_fec_identity(
        &self,
        slot: u64,
        fec_set_index: u32,
    ) -> Option<TrustedFecIdentity> {
        let state = self.read_state()?;
        trusted_fec_identity_in_slot(trusted_slot(&state, slot)?, fec_set_index)
    }

    fn reserve_repair_commitment(&self, shred: &Shred) -> bool {
        if !valid_fixed_fec_layout(shred) || !shred.is_data() {
            return false;
        }
        let Ok(actual_root) = shred.merkle_root() else {
            return false;
        };
        let Ok(actual_chain) = shred.chained_merkle_root() else {
            return false;
        };
        let Ok(mut state) = self.write_state() else {
            return false;
        };
        let Some(slot_state) = state.slots.get_mut(&shred.slot()) else {
            return false;
        };
        if slot_state.blocked_by.is_some() {
            return false;
        }
        let fec_set_index = shred.fec_set_index();
        let Some(expected) = trusted_fec_identity_in_slot(slot_state, fec_set_index) else {
            return false;
        };
        if expected.merkle_root != actual_root {
            return false;
        }
        match expected.chained_merkle_root {
            ChainedMerkleRootExpectation::Exact(Some(root)) if root == actual_chain => return true,
            ChainedMerkleRootExpectation::Exact(_) => return false,
            ChainedMerkleRootExpectation::LearnFromAnchoredRoot => {}
        }
        if slot_state.fec_sets.len() >= self.config.max_fec_sets_per_slot {
            slot_state.blocked_by = Some(RepairTrustConflict::FecCapacityExceeded {
                rejected_fec_set_index: fec_set_index,
            });
            return false;
        }
        let promoted = TrustedFecIdentity {
            merkle_root: actual_root,
            chained_merkle_root: ChainedMerkleRootExpectation::Exact(Some(actual_chain)),
            trust_anchor_fec_set_index: expected.trust_anchor_fec_set_index,
        };
        if !chain_is_consistent_with_neighbors(slot_state, fec_set_index, promoted) {
            slot_state.blocked_by = Some(RepairTrustConflict::FecIdentity {
                fec_set_index,
                first: expected,
                conflicting: promoted,
            });
            return false;
        }
        slot_state.fec_sets.insert(fec_set_index, promoted);
        true
    }

    #[cfg(test)]
    pub fn retained_slot_count(&self) -> usize {
        self.read_state().map_or(0, |state| state.slots.len())
    }

    fn peer_is_exactly_authorized(&self, peer: &RepairPeer) -> bool {
        self.authorized_peers
            .contains(&(peer.pubkey, peer.repair_addr))
    }

    fn read_state(&self) -> Option<RwLockReadGuard<'_, TrustState>> {
        self.state.read().ok()
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, TrustState>, TurbineTrustError> {
        self.state
            .write()
            .map_err(|_| TurbineTrustError::TrustStateUnavailable)
    }
}

impl RepairTrust for RepairTrustStore {
    fn peer_is_authorized(&self, peer: &RepairPeer, source: SocketAddr) -> bool {
        source == peer.repair_addr && self.peer_is_exactly_authorized(peer)
    }

    fn request_response_is_authorized(
        &self,
        peer: &RepairPeer,
        request: ShredRepairRequest,
        shred: &Shred,
    ) -> bool {
        if !self.peer_is_exactly_authorized(peer) || !shred.is_data() {
            return false;
        }
        let request_matches = match request {
            ShredRepairRequest::Shred { slot, shred_index } => {
                slot == shred.slot() && shred_index == u64::from(shred.index())
            }
            ShredRepairRequest::HighestShred { slot, shred_index } => {
                slot == shred.slot() && u64::from(shred.index()) >= shred_index
            }
            ShredRepairRequest::Orphan { .. } => false,
        };
        if !request_matches || !data_index_is_in_fec(shred.index(), shred.fec_set_index()) {
            return false;
        }

        let Some(expected) = self.trusted_fec_identity(shred.slot(), shred.fec_set_index()) else {
            return false;
        };
        let Ok(actual_root) = shred.merkle_root() else {
            return false;
        };
        if expected.merkle_root != actual_root {
            return false;
        }
        match expected.chained_merkle_root {
            ChainedMerkleRootExpectation::Exact(expected) => {
                shred.chained_merkle_root().ok() == expected
            }
            ChainedMerkleRootExpectation::LearnFromAnchoredRoot => {
                shred.chained_merkle_root().is_ok()
            }
        }
    }

    fn expected_shred_version(&self, slot: u64) -> Option<u16> {
        let state = self.read_state()?;
        trusted_slot(&state, slot).map(|_| self.config.shred_version)
    }

    fn expected_slot_leader(&self, slot: u64) -> Option<Pubkey> {
        {
            let state = self.read_state()?;
            trusted_slot(&state, slot)?;
        }
        self.leader_lookup.slot_leader(slot)
    }

    fn expected_fec_identity(&self, slot: u64, fec_set_index: u32) -> Option<TrustedFecIdentity> {
        self.trusted_fec_identity(slot, fec_set_index)
    }

    fn reserve_repair_commitment(&self, shred: &Shred) -> bool {
        self.reserve_repair_commitment(shred)
    }
}

fn trusted_slot(state: &TrustState, slot: u64) -> Option<&SlotTrustState> {
    state
        .slots
        .get(&slot)
        .filter(|slot_state| slot_state.blocked_by.is_none() && !slot_state.fec_sets.is_empty())
}

fn trusted_fec_identity_in_slot(
    slot_state: &SlotTrustState,
    fec_set_index: u32,
) -> Option<TrustedFecIdentity> {
    if let Some(identity) = slot_state.fec_sets.get(&fec_set_index) {
        return Some(*identity);
    }
    let successor_fec_set_index = fec_set_index.checked_add(DATA_SHREDS_PER_FEC_BLOCK as u32)?;
    let successor = slot_state.fec_sets.get(&successor_fec_set_index)?;
    let ChainedMerkleRootExpectation::Exact(Some(root)) = successor.chained_merkle_root else {
        return None;
    };
    Some(TrustedFecIdentity {
        merkle_root: root,
        chained_merkle_root: ChainedMerkleRootExpectation::LearnFromAnchoredRoot,
        trust_anchor_fec_set_index: successor_fec_set_index,
    })
}

fn exact_commitment_matches(first: TrustedFecIdentity, second: TrustedFecIdentity) -> bool {
    first.merkle_root == second.merkle_root
        && matches!(
            (first.chained_merkle_root, second.chained_merkle_root),
            (
                ChainedMerkleRootExpectation::Exact(first),
                ChainedMerkleRootExpectation::Exact(second),
            ) if first == second
        )
}

fn chain_is_consistent_with_neighbors(
    slot_state: &SlotTrustState,
    fec_set_index: u32,
    identity: TrustedFecIdentity,
) -> bool {
    let ChainedMerkleRootExpectation::Exact(Some(chained_root)) = identity.chained_merkle_root
    else {
        return false;
    };
    if let Some(previous_fec_set_index) =
        fec_set_index.checked_sub(DATA_SHREDS_PER_FEC_BLOCK as u32)
        && let Some(previous) = slot_state.fec_sets.get(&previous_fec_set_index)
        && previous.merkle_root != chained_root
    {
        return false;
    }
    if let Some(successor_fec_set_index) =
        fec_set_index.checked_add(DATA_SHREDS_PER_FEC_BLOCK as u32)
        && let Some(successor) = slot_state.fec_sets.get(&successor_fec_set_index)
        && !matches!(
            successor.chained_merkle_root,
            ChainedMerkleRootExpectation::Exact(Some(root)) if root == identity.merkle_root
        )
    {
        return false;
    }
    true
}

fn valid_fixed_fec_layout(shred: &Shred) -> bool {
    let fec_set_index = shred.fec_set_index();
    let fec_width = DATA_SHREDS_PER_FEC_BLOCK as u32;
    if !fec_set_index.is_multiple_of(fec_width)
        || !fec_set_index
            .checked_add(fec_width)
            .is_some_and(|end| end <= MAX_DATA_SHREDS_PER_SLOT as u32)
    {
        return false;
    }
    if shred.is_data() {
        return data_index_is_in_fec(shred.index(), fec_set_index);
    }
    if !shred.is_code() {
        return false;
    }
    let Some((num_data, num_coding, position)) = coding_header(shred) else {
        return false;
    };
    let Some(first_coding_index) = shred.index().checked_sub(u32::from(position)) else {
        return false;
    };
    num_data == DATA_SHREDS_PER_FEC_BLOCK as u16
        && num_coding == DATA_SHREDS_PER_FEC_BLOCK as u16
        && u32::from(position) < fec_width
        && first_coding_index == fec_set_index
        && first_coding_index
            .checked_add(fec_width)
            .is_some_and(|end| end <= MAX_CODE_SHREDS_PER_SLOT as u32)
}

fn data_index_is_in_fec(index: u32, fec_set_index: u32) -> bool {
    fec_set_index.is_multiple_of(DATA_SHREDS_PER_FEC_BLOCK as u32)
        && fec_set_index
            .checked_add(DATA_SHREDS_PER_FEC_BLOCK as u32)
            .is_some_and(|end| index >= fec_set_index && index < end)
}

fn coding_header(shred: &Shred) -> Option<(u16, u16, u16)> {
    let payload = shred.payload();
    Some((
        read_u16(payload, CODING_NUM_DATA_OFFSET)?,
        read_u16(payload, CODING_NUM_CODING_OFFSET)?,
        read_u16(payload, CODING_POSITION_OFFSET)?,
    ))
}

fn read_u16(payload: &[u8], offset: usize) -> Option<u16> {
    payload
        .get(offset..offset + 2)
        .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
        .map(u16::from_le_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signer};

    const VERSION: u16 = 50_093;

    fn peer(port: u16) -> RepairPeer {
        RepairPeer {
            pubkey: Keypair::new().pubkey(),
            repair_addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn config(max_slots: usize, max_fec_sets_per_slot: usize) -> RepairTrustStoreConfig {
        RepairTrustStoreConfig {
            shred_version: VERSION,
            max_slots,
            max_fec_sets_per_slot,
            max_authorized_peers: 4,
        }
    }

    fn make_shreds(slot: u64, leader: &Keypair, data: &[u8]) -> Vec<Shred> {
        make_shreds_with_version(slot, leader, data, VERSION)
    }

    fn make_shreds_with_version(
        slot: u64,
        leader: &Keypair,
        data: &[u8],
        version: u16,
    ) -> Vec<Shred> {
        vec![signed_test_data_shred(slot, 0, version, leader, data)]
    }

    fn signed_test_data_shred(
        slot: u64,
        index: u32,
        version: u16,
        leader: &Keypair,
        data: &[u8],
    ) -> Shred {
        signed_test_data_shred_with_chain(slot, index, version, leader, data, Hash::default())
    }

    fn signed_test_data_shred_with_chain(
        slot: u64,
        index: u32,
        version: u16,
        leader: &Keypair,
        data: &[u8],
        chained_merkle_root: Hash,
    ) -> Shred {
        let mut payload = vec![0u8; 1_203];
        payload[64] = 0x90; // MerkleData { proof_size: 0, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&index.to_le_bytes());
        payload[77..79].copy_from_slice(&version.to_le_bytes());
        payload[79..83].copy_from_slice(&(index / 32 * 32).to_le_bytes());
        payload[83..85].copy_from_slice(&1u16.to_le_bytes());
        payload[85] = 0b1100_0000;
        payload[86..88].copy_from_slice(&88u16.to_le_bytes());
        let chained_root_offset = payload.len() - 32;
        let copy_len = data.len().min(chained_root_offset - 88);
        payload[88..88 + copy_len].copy_from_slice(&data[..copy_len]);
        payload[chained_root_offset..].copy_from_slice(chained_merkle_root.as_ref());
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        let shred = Shred::new_from_serialized_shred(payload).unwrap();
        assert!(shred.verify(&leader.pubkey()));
        shred
    }

    fn signed_test_code_shred(
        slot: u64,
        fec_set_index: u32,
        first_coding_index: u32,
        num_data: u16,
        num_coding: u16,
        position: u16,
        leader: &Keypair,
    ) -> Shred {
        let mut payload = vec![0u8; 1_228];
        // A fixed 32+32 FEC has 64 Merkle leaves, so even this synthetic fixture needs a
        // six-entry proof. The zeroed siblings are sufficient because we sign the resulting root.
        payload[64] = 0x66; // MerkleCode { proof_size: 6, resigned: false }
        payload[65..73].copy_from_slice(&slot.to_le_bytes());
        payload[73..77].copy_from_slice(&(first_coding_index + u32::from(position)).to_le_bytes());
        payload[77..79].copy_from_slice(&VERSION.to_le_bytes());
        payload[79..83].copy_from_slice(&fec_set_index.to_le_bytes());
        payload[83..85].copy_from_slice(&num_data.to_le_bytes());
        payload[85..87].copy_from_slice(&num_coding.to_le_bytes());
        payload[87..89].copy_from_slice(&position.to_le_bytes());
        let unsigned = Shred::new_from_serialized_shred(payload.clone()).unwrap();
        let signature = leader.sign_message(unsigned.merkle_root().unwrap().as_ref());
        payload[..64].copy_from_slice(signature.as_ref());
        let shred = Shred::new_from_serialized_shred(payload).unwrap();
        assert!(shred.verify(&leader.pubkey()));
        shred
    }

    #[test]
    fn fixed_coding_shreds_are_direct_anchors_but_nonfixed_geometry_is_rejected() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(4, 4), [peer(10_008)], move |_| Some(leader_pubkey))
                .unwrap();
        let fixed = signed_test_code_shred(501, 32, 32, 32, 32, 7, &leader);
        assert!(matches!(
            store.observe_turbine_packet(fixed.payload()),
            Ok(TurbineTrustObservation::Inserted {
                slot: 501,
                fec_set_index: 32,
                ..
            })
        ));

        let nonfixed = signed_test_code_shred(502, 0, 0, 31, 33, 0, &leader);
        assert!(matches!(
            store.observe_turbine_packet(nonfixed.payload()),
            Err(TurbineTrustError::InvalidFecLayout { .. })
        ));
    }

    #[test]
    fn successor_chains_recover_consecutive_wholly_missing_fecs_backwards() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(4, 4), [peer(10_007)], move |_| Some(leader_pubkey))
                .unwrap();
        let fec_0 = signed_test_data_shred_with_chain(
            500,
            0,
            VERSION,
            &leader,
            b"fec zero",
            Hash::new_from_array([7; 32]),
        );
        let fec_32 = signed_test_data_shred_with_chain(
            500,
            32,
            VERSION,
            &leader,
            b"fec thirty two",
            fec_0.merkle_root().unwrap(),
        );
        let fec_64 = signed_test_data_shred_with_chain(
            500,
            64,
            VERSION,
            &leader,
            b"fec sixty four",
            fec_32.merkle_root().unwrap(),
        );

        store.observe_turbine_packet(fec_64.payload()).unwrap();
        let anchored_32 = store.trusted_fec_identity(500, 32).unwrap();
        assert_eq!(anchored_32.merkle_root, fec_32.merkle_root().unwrap());
        assert_eq!(
            anchored_32.chained_merkle_root,
            ChainedMerkleRootExpectation::LearnFromAnchoredRoot
        );
        assert_eq!(anchored_32.trust_anchor_fec_set_index, 64);
        assert!(store.can_request(&RepairRequest::WindowIndex {
            slot: 500,
            index: 32,
        }));

        assert!(store.reserve_repair_commitment(&fec_32));
        let anchored_0 = store.trusted_fec_identity(500, 0).unwrap();
        assert_eq!(anchored_0.merkle_root, fec_0.merkle_root().unwrap());
        assert_eq!(anchored_0.trust_anchor_fec_set_index, 32);
        assert!(store.can_request(&RepairRequest::WindowIndex {
            slot: 500,
            index: 0,
        }));
        assert!(store.reserve_repair_commitment(&fec_0));
        assert_eq!(
            store
                .trusted_fec_identity(500, 0)
                .unwrap()
                .chained_merkle_root,
            ChainedMerkleRootExpectation::Exact(fec_0.chained_merkle_root().ok())
        );
    }

    #[test]
    fn conflicting_predecessor_chain_blocks_only_that_slot_before_persistence() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(4, 4), [peer(10_009)], move |_| Some(leader_pubkey))
                .unwrap();
        let fec_0 = signed_test_data_shred_with_chain(
            503,
            0,
            VERSION,
            &leader,
            b"direct predecessor",
            Hash::new_from_array([1; 32]),
        );
        let conflicting_fec_32 = signed_test_data_shred_with_chain(
            503,
            32,
            VERSION,
            &leader,
            b"derived middle",
            Hash::new_from_array([2; 32]),
        );
        let fec_64 = signed_test_data_shred_with_chain(
            503,
            64,
            VERSION,
            &leader,
            b"direct successor",
            conflicting_fec_32.merkle_root().unwrap(),
        );
        store.observe_turbine_packet(fec_0.payload()).unwrap();
        store.observe_turbine_packet(fec_64.payload()).unwrap();

        assert!(store.can_request(&RepairRequest::WindowIndex {
            slot: 503,
            index: 32,
        }));
        assert!(!store.reserve_repair_commitment(&conflicting_fec_32));
        assert!(store.trusted_fec_identity(503, 0).is_none());
        assert!(!store.can_request(&RepairRequest::WindowIndex {
            slot: 503,
            index: 32,
        }));

        // An unrelated retained slot remains repairable; the conflict is slot-local.
        let unrelated = signed_test_data_shred(504, 0, VERSION, &leader, b"unrelated");
        store.observe_turbine_packet(unrelated.payload()).unwrap();
        assert!(store.trusted_fec_identity(504, 0).is_some());
    }

    #[test]
    fn accepts_only_parsed_versioned_leader_signed_turbine_shreds() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let authorized = peer(10_001);
        let store = RepairTrustStore::new(config(4, 4), [authorized], move |_| Some(leader_pubkey))
            .unwrap();
        let shred = make_shreds(123, &leader, b"trusted Turbine packet")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();

        assert!(matches!(
            store.observe_turbine_packet(shred.payload()),
            Ok(TurbineTrustObservation::Inserted {
                slot: 123,
                fec_set_index: 0,
                evicted_slot: None,
            })
        ));
        let trusted = store.trusted_fec_identity(123, 0).unwrap();
        assert_eq!(trusted.merkle_root, shred.merkle_root().unwrap());
        assert_eq!(
            trusted.chained_merkle_root,
            ChainedMerkleRootExpectation::Exact(shred.chained_merkle_root().ok())
        );

        assert_eq!(
            store.observe_turbine_packet(&[1, 2, 3]),
            Err(TurbineTrustError::MalformedShred)
        );
        let wrong_leader = Keypair::new();
        let bad_signature = make_shreds(124, &wrong_leader, b"not the scheduled leader")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        assert_eq!(
            store.observe_turbine_packet(bad_signature.payload()),
            Err(TurbineTrustError::InvalidSlotLeaderSignature { slot: 124 })
        );
        assert!(store.trusted_fec_identity(124, 0).is_none());

        let wrong_version = make_shreds_with_version(
            125,
            &leader,
            b"valid leader but wrong shred version",
            VERSION + 1,
        )
        .into_iter()
        .find(Shred::is_data)
        .unwrap();
        assert_eq!(
            store.observe_turbine_packet(wrong_version.payload()),
            Err(TurbineTrustError::ShredVersionMismatch {
                expected: VERSION,
                actual: VERSION + 1,
            })
        );
        assert!(store.trusted_fec_identity(125, 0).is_none());
    }

    #[test]
    fn conflicting_fec_identity_blocks_the_whole_slot_stickily() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(4, 4), [peer(10_002)], move |_| Some(leader_pubkey))
                .unwrap();
        let first = make_shreds(200, &leader, b"first fork")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        let conflicting = make_shreds(200, &leader, b"second fork")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        assert_ne!(
            first.merkle_root().unwrap(),
            conflicting.merkle_root().unwrap()
        );

        store.observe_turbine_packet(first.payload()).unwrap();
        let blocked = store.observe_turbine_packet(conflicting.payload()).unwrap();
        assert!(matches!(
            blocked,
            TurbineTrustObservation::Blocked {
                slot: 200,
                conflict: RepairTrustConflict::FecIdentity {
                    fec_set_index: 0,
                    ..
                }
            }
        ));
        assert!(store.trusted_fec_identity(200, 0).is_none());
        assert!(!store.can_request_wire(ShredRepairRequest::Shred {
            slot: 200,
            shred_index: 0,
        }));

        let still_blocked = store.observe_turbine_packet(first.payload()).unwrap();
        assert!(matches!(
            still_blocked,
            TurbineTrustObservation::Blocked { slot: 200, .. }
        ));
    }

    #[test]
    fn bounds_slots_without_allowing_late_packets_to_evict_recent_state() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(1, 4), [peer(10_003)], move |_| Some(leader_pubkey))
                .unwrap();
        let slot_10 = make_shreds(10, &leader, b"ten")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        let slot_9 = make_shreds(9, &leader, b"nine")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        let slot_11 = make_shreds(11, &leader, b"eleven")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();

        store.observe_turbine_packet(slot_10.payload()).unwrap();
        assert_eq!(
            store.observe_turbine_packet(slot_9.payload()).unwrap(),
            TurbineTrustObservation::IgnoredTooOld {
                slot: 9,
                oldest_retained_slot: 10,
            }
        );
        assert!(store.trusted_fec_identity(10, 0).is_some());
        assert!(matches!(
            store.observe_turbine_packet(slot_11.payload()),
            Ok(TurbineTrustObservation::Inserted {
                evicted_slot: Some(10),
                ..
            })
        ));
        assert!(store.trusted_fec_identity(10, 0).is_none());
        assert!(store.trusted_fec_identity(11, 0).is_some());
        assert_eq!(store.retained_slot_count(), 1);
    }

    #[test]
    fn request_preflight_and_response_authorization_require_a_known_fec() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let authorized = peer(10_004);
        let store = RepairTrustStore::new(config(4, 4), [authorized.clone()], move |_| {
            Some(leader_pubkey)
        })
        .unwrap();
        let shred = make_shreds(300, &leader, b"request authorization")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();
        store.observe_turbine_packet(shred.payload()).unwrap();
        let index = shred.index();

        assert!(store.can_request(&RepairRequest::WindowIndex { slot: 300, index }));
        assert!(!store.can_request(&RepairRequest::WindowIndex {
            slot: 300,
            index: 32,
        }));
        assert!(store.can_request_wire(ShredRepairRequest::HighestShred {
            slot: 300,
            shred_index: u64::from(index),
        }));
        assert!(!store.can_request_wire(ShredRepairRequest::HighestShred {
            slot: 300,
            shred_index: 32,
        }));
        assert!(!store.can_request_wire(ShredRepairRequest::Orphan { slot: 300 }));

        let exact = ShredRepairRequest::Shred {
            slot: 300,
            shred_index: u64::from(index),
        };
        assert!(store.request_response_is_authorized(&authorized, exact, &shred));
        assert!(!store.request_response_is_authorized(
            &authorized,
            ShredRepairRequest::Shred {
                slot: 300,
                shred_index: u64::from(index) + 1,
            },
            &shred,
        ));
        let unauthorized = RepairPeer {
            pubkey: authorized.pubkey,
            repair_addr: SocketAddr::from(([127, 0, 0, 1], 65_000)),
        };
        assert!(!store.request_response_is_authorized(&unauthorized, exact, &shred));
    }

    #[test]
    fn cloned_handles_share_the_same_bounded_trust_state() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let receiver_handle =
            RepairTrustStore::new(config(4, 4), [peer(10_006)], move |_| Some(leader_pubkey))
                .unwrap();
        let runtime_handle = receiver_handle.clone();
        let shred = make_shreds(350, &leader, b"shared state")
            .into_iter()
            .find(Shred::is_data)
            .unwrap();

        assert!(!runtime_handle.can_request_wire(ShredRepairRequest::Shred {
            slot: 350,
            shred_index: u64::from(shred.index()),
        }));
        receiver_handle
            .observe_turbine_packet(shred.payload())
            .unwrap();
        assert!(runtime_handle.can_request_wire(ShredRepairRequest::Shred {
            slot: 350,
            shred_index: u64::from(shred.index()),
        }));
        assert_eq!(runtime_handle.retained_slot_count(), 1);
    }

    #[test]
    fn fec_capacity_conflict_blocks_instead_of_silently_evicting_identity() {
        let leader = Keypair::new();
        let leader_pubkey = leader.pubkey();
        let store =
            RepairTrustStore::new(config(4, 1), [peer(10_005)], move |_| Some(leader_pubkey))
                .unwrap();
        let first = signed_test_data_shred(400, 0, VERSION, &leader, b"first FEC");
        let second = signed_test_data_shred_with_chain(
            400,
            32,
            VERSION,
            &leader,
            b"second FEC",
            first.merkle_root().unwrap(),
        );
        store.observe_turbine_packet(first.payload()).unwrap();

        assert!(matches!(
            store.observe_turbine_packet(second.payload()),
            Ok(TurbineTrustObservation::Blocked {
                slot: 400,
                conflict: RepairTrustConflict::FecCapacityExceeded {
                    rejected_fec_set_index: 32,
                },
            })
        ));
        assert!(store.trusted_fec_identity(400, 0).is_none());
        assert!(store.trusted_fec_identity(400, 32).is_none());
    }
}

//! Source-independent identities and deterministic deduplication decisions.
//!
//! The dedup index is deliberately abstract: its implementation must be durable and bounded in
//! memory. The raw ingress record must already be committed to the local spool before it is passed
//! to [`DedupManager`]. This lets a crashed index be rebuilt from the spool without asking an
//! upstream source to replay data it may no longer retain.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable identity of one transport observation.
///
/// A journal id is regenerated whenever an origin creates a new journal. Sequences may restart in
/// a new journal, but they must never be reused with different content inside the same journal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObservationId {
    pub origin_node_id: String,
    pub journal_id: [u8; 16],
    pub sequence: u64,
}

/// Digest of the canonical ingress payload, including a format/domain prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentDigest(pub [u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShredKind {
    Data,
    Coding,
}

/// Semantic identity used to group exact duplicates and conflicting candidates.
///
/// Blocks are never keyed by slot alone because two providers may expose different fork
/// candidates for the same slot. Likewise, equal shred coordinates with different payloads are a
/// conflict, not an exact duplicate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalKey {
    Block {
        slot: u64,
        blockhash: [u8; 32],
    },
    Entry {
        slot: u64,
        entry_index: u64,
        entry_hash: [u8; 32],
    },
    Shred {
        slot: u64,
        kind: ShredKind,
        shred_index: u32,
        fec_set_index: Option<u32>,
    },
}

impl LogicalKey {
    pub fn slot(&self) -> u64 {
        match self {
            Self::Block { slot, .. } | Self::Entry { slot, .. } | Self::Shred { slot, .. } => *slot,
        }
    }

    fn blockhash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Block { blockhash, .. } => Some(*blockhash),
            Self::Entry { .. } | Self::Shred { .. } => None,
        }
    }
}

/// Metadata kept in the dedup index. Payload bytes remain in the segmented ingress spool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressRecordMeta {
    pub cluster_id: String,
    pub observation: ObservationId,
    pub source_id: String,
    pub logical_key: LogicalKey,
    pub payload_format_version: u16,
    pub content_digest: ContentDigest,
    pub payload_len: u64,
}

impl IngressRecordMeta {
    pub fn from_payload(
        cluster_id: String,
        observation: ObservationId,
        source_id: String,
        logical_key: LogicalKey,
        payload_format_version: u16,
        payload: &[u8],
    ) -> Self {
        let content_digest =
            compute_content_digest(&cluster_id, &logical_key, payload_format_version, payload);
        Self {
            cluster_id,
            observation,
            source_id,
            logical_key,
            payload_format_version,
            content_digest,
            payload_len: payload.len() as u64,
        }
    }
}

/// Domain-separated exact-content digest recomputed by every durable receiver.
pub fn compute_content_digest(
    cluster_id: &str,
    logical_key: &LogicalKey,
    payload_format_version: u16,
    payload: &[u8],
) -> ContentDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"BLOCKZILLA-INGRESS-CONTENT-v1");
    hasher.update((cluster_id.len() as u64).to_le_bytes());
    hasher.update(cluster_id.as_bytes());
    hasher.update(payload_format_version.to_le_bytes());
    match logical_key {
        LogicalKey::Block { slot, blockhash } => {
            hasher.update([1]);
            hasher.update(slot.to_le_bytes());
            hasher.update(blockhash);
        }
        LogicalKey::Entry {
            slot,
            entry_index,
            entry_hash,
        } => {
            hasher.update([2]);
            hasher.update(slot.to_le_bytes());
            hasher.update(entry_index.to_le_bytes());
            hasher.update(entry_hash);
        }
        LogicalKey::Shred {
            slot,
            kind,
            shred_index,
            fec_set_index,
        } => {
            hasher.update([3]);
            hasher.update(slot.to_le_bytes());
            hasher.update([match kind {
                ShredKind::Data => 1,
                ShredKind::Coding => 2,
            }]);
            hasher.update(shred_index.to_le_bytes());
            match fec_set_index {
                Some(index) => {
                    hasher.update([1]);
                    hasher.update(index.to_le_bytes());
                }
                None => hasher.update([0]),
            }
        }
    }
    hasher.update((payload.len() as u64).to_le_bytes());
    hasher.update(payload);
    ContentDigest(hasher.finalize().into())
}

/// Compact identity returned by the persistent index; payload bytes remain in the spool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedRecordIdentity {
    pub cluster_id: String,
    pub source_id: String,
    pub logical_key: LogicalKey,
    pub payload_format_version: u16,
    pub content_digest: ContentDigest,
    pub payload_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DedupDecision {
    /// First content seen for this logical key and slot.
    AcceptNew,
    /// The same observation was replayed with exactly the same bytes.
    ObservationReplay,
    /// A different source/observation delivered already-known exact content.
    ExactDuplicate,
    /// The observation id was reused with different content. The input must be quarantined.
    ObservationIdentityViolation,
    /// Equal logical coordinates carried different bytes. Preserve both candidates.
    ConflictingPayload,
    /// A different blockhash was observed for the same slot. Preserve both fork candidates.
    ForkCandidate,
    /// One digest resolved to a different logical identity. Treat as corrupt/invalid input.
    DigestIdentityViolation,
}

impl DedupDecision {
    /// Whether a candidate may enter normal merge/canonical-selection processing.
    pub fn is_candidate(self) -> bool {
        matches!(
            self,
            Self::AcceptNew | Self::ConflictingPayload | Self::ForkCandidate
        )
    }

    /// Whether payload bytes may be shared with an existing content-addressed blob.
    pub fn reuses_existing_payload(self) -> bool {
        matches!(self, Self::ObservationReplay | Self::ExactDuplicate)
    }

    pub fn must_quarantine(self) -> bool {
        matches!(
            self,
            Self::ObservationIdentityViolation | Self::DigestIdentityViolation
        )
    }
}

/// Exact, disk-backed index needed by [`DedupManager`].
///
/// Implementations should return only the small set of fork/conflict candidates for one key or
/// slot; they must not load epoch-wide observation history into memory. `record` must atomically
/// persist the observation and its decision (or be rebuildable from the already-durable spool).
pub trait DurableDedupIndex {
    fn observation_identity(
        &mut self,
        observation: &ObservationId,
    ) -> Result<Option<IndexedRecordIdentity>>;

    fn content_identity(&mut self, digest: ContentDigest) -> Result<Option<IndexedRecordIdentity>>;

    /// Existence query stays bounded even under adversarial numbers of conflicting candidates.
    fn logical_has_content(&mut self, key: &LogicalKey) -> Result<bool>;

    /// Existence query stays bounded; canonical selection loads candidates separately in pages.
    fn block_slot_has_other_hash(&mut self, slot: u64, blockhash: [u8; 32]) -> Result<bool>;

    fn record(&mut self, incoming: &IngressRecordMeta, decision: DedupDecision) -> Result<()>;
}

/// Source-independent deduplication coordinator.
///
/// It intentionally does not choose a canonical fork. Canonical selection depends on commitment,
/// completeness, and configured source policy and must remain a later deterministic stage.
#[derive(Debug)]
pub struct DedupManager<I> {
    index: I,
}

impl<I> DedupManager<I>
where
    I: DurableDedupIndex,
{
    pub fn new(index: I) -> Self {
        Self { index }
    }

    pub fn index(&self) -> &I {
        &self.index
    }

    pub fn into_index(self) -> I {
        self.index
    }

    /// Classify and persist an event that is already durable in the raw ingress spool.
    pub fn classify(&mut self, incoming: &IngressRecordMeta) -> Result<DedupDecision> {
        let decision =
            if let Some(existing) = self.index.observation_identity(&incoming.observation)? {
                if existing.content_digest == incoming.content_digest
                    && existing.cluster_id == incoming.cluster_id
                    && existing.source_id == incoming.source_id
                    && existing.logical_key == incoming.logical_key
                    && existing.payload_format_version == incoming.payload_format_version
                    && existing.payload_len == incoming.payload_len
                {
                    DedupDecision::ObservationReplay
                } else {
                    DedupDecision::ObservationIdentityViolation
                }
            } else if let Some(existing) = self.index.content_identity(incoming.content_digest)? {
                if existing.cluster_id == incoming.cluster_id
                    && existing.logical_key == incoming.logical_key
                    && existing.payload_format_version == incoming.payload_format_version
                    && existing.payload_len == incoming.payload_len
                {
                    DedupDecision::ExactDuplicate
                } else {
                    DedupDecision::DigestIdentityViolation
                }
            } else if self.index.logical_has_content(&incoming.logical_key)? {
                DedupDecision::ConflictingPayload
            } else if let Some(incoming_hash) = incoming.logical_key.blockhash() {
                let has_other_fork = self
                    .index
                    .block_slot_has_other_hash(incoming.logical_key.slot(), incoming_hash)?;
                if has_other_fork {
                    DedupDecision::ForkCandidate
                } else {
                    DedupDecision::AcceptNew
                }
            } else {
                DedupDecision::AcceptNew
            };

        self.index.record(incoming, decision)?;
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Debug, Default)]
    struct MemoryIndex {
        observations: HashMap<ObservationId, IndexedRecordIdentity>,
        content: HashMap<ContentDigest, IndexedRecordIdentity>,
        logical: HashMap<LogicalKey, Vec<ContentDigest>>,
        blocks: HashMap<u64, Vec<LogicalKey>>,
    }

    impl DurableDedupIndex for MemoryIndex {
        fn observation_identity(
            &mut self,
            observation: &ObservationId,
        ) -> Result<Option<IndexedRecordIdentity>> {
            Ok(self.observations.get(observation).cloned())
        }

        fn content_identity(
            &mut self,
            digest: ContentDigest,
        ) -> Result<Option<IndexedRecordIdentity>> {
            Ok(self.content.get(&digest).cloned())
        }

        fn logical_has_content(&mut self, key: &LogicalKey) -> Result<bool> {
            Ok(self
                .logical
                .get(key)
                .is_some_and(|digests| !digests.is_empty()))
        }

        fn block_slot_has_other_hash(&mut self, slot: u64, blockhash: [u8; 32]) -> Result<bool> {
            Ok(self
                .blocks
                .get(&slot)
                .is_some_and(|keys| keys.iter().any(|key| key.blockhash() != Some(blockhash))))
        }

        fn record(&mut self, incoming: &IngressRecordMeta, decision: DedupDecision) -> Result<()> {
            if decision.must_quarantine() {
                return Ok(());
            }
            let identity = IndexedRecordIdentity {
                cluster_id: incoming.cluster_id.clone(),
                source_id: incoming.source_id.clone(),
                logical_key: incoming.logical_key.clone(),
                payload_format_version: incoming.payload_format_version,
                content_digest: incoming.content_digest,
                payload_len: incoming.payload_len,
            };
            self.observations
                .insert(incoming.observation.clone(), identity.clone());
            self.content
                .entry(incoming.content_digest)
                .or_insert(identity);
            let digests = self
                .logical
                .entry(incoming.logical_key.clone())
                .or_default();
            if !digests.contains(&incoming.content_digest) {
                digests.push(incoming.content_digest);
            }
            if matches!(incoming.logical_key, LogicalKey::Block { .. }) {
                let keys = self.blocks.entry(incoming.logical_key.slot()).or_default();
                if !keys.contains(&incoming.logical_key) {
                    keys.push(incoming.logical_key.clone());
                }
            }
            Ok(())
        }
    }

    fn record(
        source: &str,
        journal_byte: u8,
        sequence: u64,
        slot: u64,
        blockhash_byte: u8,
        digest_byte: u8,
    ) -> IngressRecordMeta {
        IngressRecordMeta {
            cluster_id: "solana-mainnet".to_string(),
            observation: ObservationId {
                origin_node_id: "node-a".to_string(),
                journal_id: [journal_byte; 16],
                sequence,
            },
            source_id: source.to_string(),
            logical_key: LogicalKey::Block {
                slot,
                blockhash: [blockhash_byte; 32],
            },
            payload_format_version: 1,
            content_digest: ContentDigest([digest_byte; 32]),
            payload_len: 1024,
        }
    }

    #[test]
    fn distinguishes_replay_duplicate_conflict_and_fork() {
        let first = record("grpc-a", 1, 1, 42, 7, 10);
        let replay = first.clone();
        let exact_other_source = record("grpc-b", 2, 1, 42, 7, 10);
        let conflict = record("grpc-b", 2, 2, 42, 7, 11);
        let fork = record("grpc-c", 3, 1, 42, 8, 12);
        let mut manager = DedupManager::new(MemoryIndex::default());

        assert_eq!(manager.classify(&first).unwrap(), DedupDecision::AcceptNew);
        assert_eq!(
            manager.classify(&replay).unwrap(),
            DedupDecision::ObservationReplay
        );
        assert_eq!(
            manager.classify(&exact_other_source).unwrap(),
            DedupDecision::ExactDuplicate
        );
        assert_eq!(
            manager.classify(&conflict).unwrap(),
            DedupDecision::ConflictingPayload
        );
        assert_eq!(
            manager.classify(&fork).unwrap(),
            DedupDecision::ForkCandidate
        );
    }

    #[test]
    fn reused_observation_with_new_bytes_is_quarantined() {
        let first = record("grpc-a", 1, 1, 42, 7, 10);
        let mut reused = first.clone();
        reused.content_digest = ContentDigest([99; 32]);
        let mut manager = DedupManager::new(MemoryIndex::default());

        manager.classify(&first).unwrap();
        assert_eq!(
            manager.classify(&reused).unwrap(),
            DedupDecision::ObservationIdentityViolation
        );
    }

    #[test]
    fn replay_must_preserve_logical_identity_and_length() {
        let first = record("grpc-a", 1, 1, 42, 7, 10);
        let mut changed_source = first.clone();
        changed_source.source_id = "grpc-b".to_string();
        let mut changed_key = first.clone();
        changed_key.logical_key = LogicalKey::Block {
            slot: 43,
            blockhash: [8; 32],
        };
        let mut changed_len = first.clone();
        changed_len.payload_len += 1;

        let mut key_manager = DedupManager::new(MemoryIndex::default());
        key_manager.classify(&first).unwrap();
        assert_eq!(
            key_manager.classify(&changed_key).unwrap(),
            DedupDecision::ObservationIdentityViolation
        );

        let mut source_manager = DedupManager::new(MemoryIndex::default());
        source_manager.classify(&first).unwrap();
        assert_eq!(
            source_manager.classify(&changed_source).unwrap(),
            DedupDecision::ObservationIdentityViolation
        );

        let mut length_manager = DedupManager::new(MemoryIndex::default());
        length_manager.classify(&first).unwrap();
        assert_eq!(
            length_manager.classify(&changed_len).unwrap(),
            DedupDecision::ObservationIdentityViolation
        );
    }

    #[test]
    fn equal_digest_cannot_change_logical_identity() {
        let first = record("grpc-a", 1, 1, 42, 7, 10);
        let same_digest_different_slot = record("grpc-b", 2, 1, 43, 9, 10);
        let mut manager = DedupManager::new(MemoryIndex::default());

        manager.classify(&first).unwrap();
        assert_eq!(
            manager.classify(&same_digest_different_slot).unwrap(),
            DedupDecision::DigestIdentityViolation
        );
    }

    #[test]
    fn canonical_digest_binds_cluster_key_format_and_payload() {
        let key = LogicalKey::Block {
            slot: 42,
            blockhash: [7; 32],
        };
        let baseline = compute_content_digest("solana-mainnet", &key, 1, b"payload");
        assert_eq!(
            baseline,
            compute_content_digest("solana-mainnet", &key, 1, b"payload")
        );
        assert_ne!(
            baseline,
            compute_content_digest("solana-testnet", &key, 1, b"payload")
        );
        assert_ne!(
            baseline,
            compute_content_digest("solana-mainnet", &key, 2, b"payload")
        );
        assert_ne!(
            baseline,
            compute_content_digest("solana-mainnet", &key, 1, b"other")
        );
        assert_ne!(
            compute_content_digest("solana-mainnet", &key, 1, b"payload-a"),
            compute_content_digest("solana-mainnet", &key, 1, b"payload-b")
        );
    }
}

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::{
    ClusterGenesisHash, CursorV1, DeletionAuthorizingStoreId, DurabilityPolicyId,
    DurabilityTargetDescriptorSha256, DurabilityTargetId, FailureDomainId, OverflowNamespaceSha256,
    ProducerConfigSha256, ProtocolError, Result, StreamHeaderV1, StreamId, StreamManifestSha256,
    TerminalCatalogDescriptorSha256,
};

pub const MANIFEST_VERSION_V1: u16 = 1;
pub const MAX_PRODUCER_DESCRIPTOR_BYTES: u64 = 1_048_576;
pub const DURABILITY_POLICY_MIN_TARGETS: usize = 2;
pub const DURABILITY_POLICY_MAX_TARGETS: usize = 256;
pub const GAP_EVENT_PRODUCER_DESCRIPTOR_VERSION_V1: u16 = 1;
pub const GAP_EVENT_MIN_PERMITTED_REASONS: usize = 1;
pub const GAP_EVENT_MAX_PERMITTED_REASONS: usize = 5;
pub const MAX_SOURCE_POSITION_DESCRIPTOR_BYTES: u64 = 65_536;
pub const CONTINUITY_CONTIGUOUS: u8 = 1;
pub const CONTINUITY_GAP_POSSIBLE: u8 = 2;
pub const CONTINUITY_GAP_CONFIRMED: u8 = 3;

const PRODUCER_CONFIG_DOMAIN: &[u8] = b"hive/v1/producer-config";
const MANIFEST_DOMAIN: &[u8] = b"hive/v1/manifest";
const DURABILITY_TARGET_DOMAIN: &[u8] = b"hive/v1/durability-target";
const TERMINAL_CATALOG_DOMAIN: &[u8] = b"hive/v1/terminal-catalog";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum LineageReasonV1 {
    ConfigChange = 1,
    WalReset = 2,
    UnsafeHandoff = 3,
    SourceHostLoss = 4,
    TerminalStoreRollover = 5,
    OperatorSplit = 6,
}

impl TryFrom<u16> for LineageReasonV1 {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::ConfigChange),
            2 => Ok(Self::WalReset),
            3 => Ok(Self::UnsafeHandoff),
            4 => Ok(Self::SourceHostLoss),
            5 => Ok(Self::TerminalStoreRollover),
            6 => Ok(Self::OperatorSplit),
            value => Err(ProtocolError::UnknownLineageReason { value }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum LineageContinuityV1 {
    Contiguous = CONTINUITY_CONTIGUOUS,
    GapPossible = CONTINUITY_GAP_POSSIBLE,
    GapConfirmed = CONTINUITY_GAP_CONFIRMED,
}

impl TryFrom<u8> for LineageContinuityV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            CONTINUITY_CONTIGUOUS => Ok(Self::Contiguous),
            CONTINUITY_GAP_POSSIBLE => Ok(Self::GapPossible),
            CONTINUITY_GAP_CONFIRMED => Ok(Self::GapConfirmed),
            value => Err(ProtocolError::UnknownLineageContinuity { value }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u16)]
pub enum GapEventReasonV1 {
    UpstreamSequenceJump = 1,
    UdpDropCounter = 2,
    LocalSourceLoss = 3,
    ColdRecoveryIncomplete = 4,
    OperatorDeclared = 5,
}

impl TryFrom<u16> for GapEventReasonV1 {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::UpstreamSequenceJump),
            2 => Ok(Self::UdpDropCounter),
            3 => Ok(Self::LocalSourceLoss),
            4 => Ok(Self::ColdRecoveryIncomplete),
            5 => Ok(Self::OperatorDeclared),
            value => Err(ProtocolError::UnknownGapEventReason { value }),
        }
    }
}

/// Canonical producer descriptor required by a format-7 gap-event stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GapEventProducerDescriptorV1 {
    target_stream_id: StreamId,
    target_producer_config_sha256: ProducerConfigSha256,
    permitted_reasons: Vec<GapEventReasonV1>,
    source_position_descriptor: Vec<u8>,
}

impl GapEventProducerDescriptorV1 {
    pub fn new(
        target_stream_id: StreamId,
        target_producer_config_sha256: ProducerConfigSha256,
        mut permitted_reasons: Vec<GapEventReasonV1>,
        source_position_descriptor: Vec<u8>,
    ) -> Result<Self> {
        permitted_reasons.sort_unstable();
        validate_gap_event_reasons(&permitted_reasons)?;
        validate_source_position_descriptor_len(source_position_descriptor.len() as u64)?;
        Ok(Self {
            target_stream_id,
            target_producer_config_sha256,
            permitted_reasons,
            source_position_descriptor,
        })
    }

    #[must_use]
    pub const fn target_stream_id(&self) -> StreamId {
        self.target_stream_id
    }

    #[must_use]
    pub const fn target_producer_config_sha256(&self) -> ProducerConfigSha256 {
        self.target_producer_config_sha256
    }

    #[must_use]
    pub fn permitted_reasons(&self) -> &[GapEventReasonV1] {
        &self.permitted_reasons
    }

    #[must_use]
    pub fn source_position_descriptor(&self) -> &[u8] {
        &self.source_position_descriptor
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(
            2 + StreamId::LENGTH
                + ProducerConfigSha256::LENGTH
                + 4
                + 2 * self.permitted_reasons.len()
                + 8
                + self.source_position_descriptor.len(),
        );
        encoded.extend_from_slice(&GAP_EVENT_PRODUCER_DESCRIPTOR_VERSION_V1.to_be_bytes());
        encoded.extend_from_slice(self.target_stream_id.as_bytes());
        encoded.extend_from_slice(self.target_producer_config_sha256.as_bytes());
        encoded.extend_from_slice(&(self.permitted_reasons.len() as u32).to_be_bytes());
        for reason in &self.permitted_reasons {
            encoded.extend_from_slice(&(*reason as u16).to_be_bytes());
        }
        encoded.extend_from_slice(&(self.source_position_descriptor.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.source_position_descriptor);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let descriptor_version = reader.u16("gap_event_descriptor_version")?;
        if descriptor_version != GAP_EVENT_PRODUCER_DESCRIPTOR_VERSION_V1 {
            return Err(ProtocolError::UnknownGapEventProducerDescriptorVersion {
                value: descriptor_version,
            });
        }
        let target_stream_id =
            StreamId::try_from(reader.take(StreamId::LENGTH, "gap_event_target_stream_id")?)?;
        let target_producer_config_sha256 = ProducerConfigSha256::try_from(reader.take(
            ProducerConfigSha256::LENGTH,
            "gap_event_target_producer_config_sha256",
        )?)?;
        let reason_count_u32 = reader.u32("gap_event_permitted_reason_count")?;
        let reason_count =
            usize::try_from(reason_count_u32).map_err(|_| ProtocolError::IntegerOverflow {
                field: "gap_event_permitted_reason_count",
            })?;
        validate_gap_event_reason_count(reason_count)?;
        let mut permitted_reasons = Vec::with_capacity(reason_count);
        for _ in 0..reason_count {
            permitted_reasons.push(GapEventReasonV1::try_from(
                reader.u16("gap_event_permitted_reason")?,
            )?);
        }
        if permitted_reasons.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "GapEventProducerDescriptorV1.permitted_reasons",
            });
        }

        let source_position_descriptor_len = reader.u64("source_position_descriptor_len")?;
        validate_source_position_descriptor_len(source_position_descriptor_len)?;
        let source_position_descriptor_len = usize::try_from(source_position_descriptor_len)
            .map_err(|_| ProtocolError::IntegerOverflow {
                field: "source_position_descriptor_len",
            })?;
        let source_position_descriptor = reader
            .take(source_position_descriptor_len, "source_position_descriptor")?
            .to_vec();
        reader.finish("GapEventProducerDescriptorV1")?;

        let decoded = Self::new(
            target_stream_id,
            target_producer_config_sha256,
            permitted_reasons,
            source_position_descriptor,
        )?;
        if decoded.encode() != encoded {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "GapEventProducerDescriptorV1",
            });
        }
        Ok(decoded)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineageV1 {
    pub predecessor_stream_id: StreamId,
    pub predecessor_last_known_cursor: CursorV1,
    pub reason: LineageReasonV1,
    pub continuity: LineageContinuityV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurabilityTargetV1 {
    pub target_id: DurabilityTargetId,
    pub failure_domain_id: FailureDomainId,
    pub target_descriptor_sha256: DurabilityTargetDescriptorSha256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurabilityPolicyV1 {
    policy_id: DurabilityPolicyId,
    minimum_independent_copies: u8,
    catalog_descriptor_sha256: TerminalCatalogDescriptorSha256,
    targets: Vec<DurabilityTargetV1>,
}

impl DurabilityPolicyV1 {
    pub fn new(
        policy_id: DurabilityPolicyId,
        minimum_independent_copies: u8,
        catalog_descriptor_sha256: TerminalCatalogDescriptorSha256,
        mut targets: Vec<DurabilityTargetV1>,
    ) -> Result<Self> {
        targets.sort_by_key(|target| target.target_id);
        validate_policy(minimum_independent_copies, &targets)?;
        Ok(Self {
            policy_id,
            minimum_independent_copies,
            catalog_descriptor_sha256,
            targets,
        })
    }

    #[must_use]
    pub const fn policy_id(&self) -> DurabilityPolicyId {
        self.policy_id
    }

    #[must_use]
    pub const fn minimum_independent_copies(&self) -> u8 {
        self.minimum_independent_copies
    }

    #[must_use]
    pub const fn catalog_descriptor_sha256(&self) -> TerminalCatalogDescriptorSha256 {
        self.catalog_descriptor_sha256
    }

    #[must_use]
    pub fn targets(&self) -> &[DurabilityTargetV1] {
        &self.targets
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamManifestV1 {
    stream: StreamHeaderV1,
    producer_descriptor: Vec<u8>,
    lineage: Option<LineageV1>,
    gap_event_stream_id: Option<StreamId>,
    overflow_namespace_sha256: Option<OverflowNamespaceSha256>,
    deletion_authorizing_store_id: Option<DeletionAuthorizingStoreId>,
    durability_policy: Option<DurabilityPolicyV1>,
}

impl StreamManifestV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: StreamId,
        cluster_genesis_hash: ClusterGenesisHash,
        payload_format: u32,
        payload_format_version: u16,
        producer_descriptor: Vec<u8>,
        lineage: Option<LineageV1>,
        gap_event_stream_id: Option<StreamId>,
        overflow_namespace_sha256: Option<OverflowNamespaceSha256>,
        deletion_authorizing_store_id: Option<DeletionAuthorizingStoreId>,
        durability_policy: Option<DurabilityPolicyV1>,
    ) -> Result<Self> {
        validate_producer_descriptor(&producer_descriptor)?;
        validate_manifest_shape(
            stream_id,
            payload_format,
            &producer_descriptor,
            gap_event_stream_id,
            overflow_namespace_sha256,
            deletion_authorizing_store_id,
            durability_policy.as_ref(),
        )?;

        let producer_config_sha256 = producer_config_sha256(&producer_descriptor);
        let placeholder_stream = StreamHeaderV1::new(
            stream_id,
            cluster_genesis_hash,
            payload_format,
            payload_format_version,
            producer_config_sha256,
            StreamManifestSha256::new([0; 32]),
        )?;
        let mut manifest = Self {
            stream: placeholder_stream,
            producer_descriptor,
            lineage,
            gap_event_stream_id,
            overflow_namespace_sha256,
            deletion_authorizing_store_id,
            durability_policy,
        };
        let manifest_sha256 = manifest.compute_manifest_sha256();
        manifest.stream = StreamHeaderV1::new(
            stream_id,
            cluster_genesis_hash,
            payload_format,
            payload_format_version,
            producer_config_sha256,
            manifest_sha256,
        )?;
        Ok(manifest)
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub fn producer_descriptor(&self) -> &[u8] {
        &self.producer_descriptor
    }

    #[must_use]
    pub const fn lineage(&self) -> Option<LineageV1> {
        self.lineage
    }

    #[must_use]
    pub const fn gap_event_stream_id(&self) -> Option<StreamId> {
        self.gap_event_stream_id
    }

    #[must_use]
    pub const fn overflow_namespace_sha256(&self) -> Option<OverflowNamespaceSha256> {
        self.overflow_namespace_sha256
    }

    #[must_use]
    pub const fn deletion_authorizing_store_id(&self) -> Option<DeletionAuthorizingStoreId> {
        self.deletion_authorizing_store_id
    }

    #[must_use]
    pub fn durability_policy(&self) -> Option<&DurabilityPolicyV1> {
        self.durability_policy.as_ref()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.encode_with_manifest_hash(true)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let (decoded, consumed) = Self::decode_prefix(encoded)?;
        if consumed != encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context: "StreamManifestV1",
                count: encoded.len() - consumed,
            });
        }
        Ok(decoded)
    }

    /// Decodes one complete canonical stored manifest from the beginning of
    /// `encoded` and returns its exact byte length. This is required by
    /// self-describing containers such as `TerminalRawHeaderV1`; the stored
    /// manifest is self-delimiting and must not be wrapped in another length.
    pub fn decode_prefix(encoded: &[u8]) -> Result<(Self, usize)> {
        let mut reader = Reader::new(encoded);
        let manifest_version = reader.u16("manifest_version")?;
        if manifest_version != MANIFEST_VERSION_V1 {
            return Err(ProtocolError::UnknownManifestVersion {
                value: manifest_version,
            });
        }
        let stream = StreamHeaderV1::decode(
            reader.take(crate::STREAM_HEADER_V1_ENCODED_LEN, "StreamHeaderV1")?,
        )?;
        let descriptor_len = reader.u64("producer_descriptor_len")?;
        validate_declared_producer_descriptor_len(descriptor_len)?;
        let descriptor_len =
            usize::try_from(descriptor_len).map_err(|_| ProtocolError::IntegerOverflow {
                field: "producer_descriptor_len",
            })?;
        let producer_descriptor = reader.take(descriptor_len, "producer_descriptor")?.to_vec();
        let lineage = decode_option(&mut reader, "lineage", decode_lineage)?;
        let gap_event_stream_id = decode_option(&mut reader, "gap_event_stream_id", |reader| {
            StreamId::try_from(reader.take(StreamId::LENGTH, "gap_event_stream_id")?)
        })?;
        let overflow_namespace_sha256 =
            decode_option(&mut reader, "overflow_namespace_sha256", |reader| {
                OverflowNamespaceSha256::try_from(
                    reader.take(OverflowNamespaceSha256::LENGTH, "overflow_namespace_sha256")?,
                )
            })?;
        let deletion_authorizing_store_id =
            decode_option(&mut reader, "deletion_authorizing_store_id", |reader| {
                DeletionAuthorizingStoreId::try_from(reader.take(
                    DeletionAuthorizingStoreId::LENGTH,
                    "deletion_authorizing_store_id",
                )?)
            })?;
        let durability_policy = decode_option(&mut reader, "durability_policy", decode_policy)?;
        let consumed = reader.offset();

        let decoded = Self::new(
            stream.stream_id(),
            stream.cluster_genesis_hash(),
            stream.payload_format(),
            stream.payload_format_version(),
            producer_descriptor,
            lineage,
            gap_event_stream_id,
            overflow_namespace_sha256,
            deletion_authorizing_store_id,
            durability_policy,
        )?;
        if stream.producer_config_sha256() != decoded.stream.producer_config_sha256() {
            return Err(ProtocolError::ProducerConfigMismatch);
        }
        if stream.stream_manifest_sha256() != decoded.stream.stream_manifest_sha256() {
            return Err(ProtocolError::ManifestHashMismatch);
        }
        if decoded.encode() != encoded[..consumed] {
            return Err(ProtocolError::NonCanonicalOrder {
                context: "StreamManifestV1",
            });
        }
        Ok((decoded, consumed))
    }

    fn compute_manifest_sha256(&self) -> StreamManifestSha256 {
        let mut hasher = Sha256::new();
        hasher.update(MANIFEST_DOMAIN);
        hasher.update(self.encode_with_manifest_hash(false));
        StreamManifestSha256::new(hasher.finalize().into())
    }

    fn encode_with_manifest_hash(&self, include_manifest_hash: bool) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(
            2 + crate::STREAM_HEADER_V1_ENCODED_LEN + 8 + self.producer_descriptor.len() + 256,
        );
        encoded.extend_from_slice(&MANIFEST_VERSION_V1.to_be_bytes());
        encoded.extend_from_slice(self.stream.stream_id().as_bytes());
        encoded.extend_from_slice(self.stream.cluster_genesis_hash().as_bytes());
        encoded.extend_from_slice(&self.stream.payload_format().to_be_bytes());
        encoded.extend_from_slice(&self.stream.payload_format_version().to_be_bytes());
        encoded.extend_from_slice(self.stream.producer_config_sha256().as_bytes());
        if include_manifest_hash {
            encoded.extend_from_slice(self.stream.stream_manifest_sha256().as_bytes());
        }
        encoded.extend_from_slice(&(self.producer_descriptor.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.producer_descriptor);
        encode_option(&mut encoded, self.lineage.as_ref(), encode_lineage);
        encode_option(
            &mut encoded,
            self.gap_event_stream_id.as_ref(),
            |encoded, stream_id| encoded.extend_from_slice(stream_id.as_bytes()),
        );
        encode_option(
            &mut encoded,
            self.overflow_namespace_sha256.as_ref(),
            |encoded, digest| encoded.extend_from_slice(digest.as_bytes()),
        );
        encode_option(
            &mut encoded,
            self.deletion_authorizing_store_id.as_ref(),
            |encoded, store_id| encoded.extend_from_slice(store_id.as_bytes()),
        );
        encode_option(&mut encoded, self.durability_policy.as_ref(), encode_policy);
        encoded
    }
}

#[must_use]
pub fn producer_config_sha256(descriptor: &[u8]) -> ProducerConfigSha256 {
    ProducerConfigSha256::new(domain_hash(PRODUCER_CONFIG_DOMAIN, descriptor))
}

#[must_use]
pub fn durability_target_descriptor_sha256(descriptor: &[u8]) -> DurabilityTargetDescriptorSha256 {
    DurabilityTargetDescriptorSha256::new(domain_hash(DURABILITY_TARGET_DOMAIN, descriptor))
}

#[must_use]
pub fn terminal_catalog_descriptor_sha256(descriptor: &[u8]) -> TerminalCatalogDescriptorSha256 {
    TerminalCatalogDescriptorSha256::new(domain_hash(TERMINAL_CATALOG_DOMAIN, descriptor))
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().into()
}

fn validate_producer_descriptor(descriptor: &[u8]) -> Result<()> {
    validate_declared_producer_descriptor_len(descriptor.len() as u64)
}

fn validate_declared_producer_descriptor_len(length: u64) -> Result<()> {
    if length == 0 {
        return Err(ProtocolError::EmptyProducerDescriptor);
    }
    if length > MAX_PRODUCER_DESCRIPTOR_BYTES {
        return Err(ProtocolError::ProducerDescriptorTooLarge {
            actual: length,
            max: MAX_PRODUCER_DESCRIPTOR_BYTES,
        });
    }
    Ok(())
}

fn validate_gap_event_reason_count(count: usize) -> Result<()> {
    if !(GAP_EVENT_MIN_PERMITTED_REASONS..=GAP_EVENT_MAX_PERMITTED_REASONS).contains(&count) {
        return Err(ProtocolError::InvalidGapEventReasonSet {
            reason: "permitted reasons must contain 1..=5 entries",
        });
    }
    Ok(())
}

fn validate_gap_event_reasons(reasons: &[GapEventReasonV1]) -> Result<()> {
    validate_gap_event_reason_count(reasons.len())?;
    if reasons.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProtocolError::InvalidGapEventReasonSet {
            reason: "permitted reasons must be unique",
        });
    }
    Ok(())
}

fn validate_source_position_descriptor_len(length: u64) -> Result<()> {
    if length == 0 {
        return Err(ProtocolError::EmptySourcePositionDescriptor);
    }
    if length > MAX_SOURCE_POSITION_DESCRIPTOR_BYTES {
        return Err(ProtocolError::SourcePositionDescriptorTooLarge {
            actual: length,
            max: MAX_SOURCE_POSITION_DESCRIPTOR_BYTES,
        });
    }
    Ok(())
}

fn validate_policy(minimum_independent_copies: u8, targets: &[DurabilityTargetV1]) -> Result<()> {
    if !(DURABILITY_POLICY_MIN_TARGETS..=DURABILITY_POLICY_MAX_TARGETS).contains(&targets.len()) {
        return Err(ProtocolError::InvalidDurabilityPolicy {
            reason: "target count must be in 2..=256",
        });
    }
    if minimum_independent_copies < 2 {
        return Err(ProtocolError::InvalidDurabilityPolicy {
            reason: "minimum_independent_copies must be at least two",
        });
    }
    if targets
        .windows(2)
        .any(|pair| pair[0].target_id >= pair[1].target_id)
    {
        return Err(ProtocolError::InvalidDurabilityPolicy {
            reason: "target IDs must be unique",
        });
    }
    let failure_domains = targets
        .iter()
        .map(|target| target.failure_domain_id)
        .collect::<BTreeSet<_>>();
    if usize::from(minimum_independent_copies) > failure_domains.len() {
        return Err(ProtocolError::InvalidDurabilityPolicy {
            reason: "minimum copies exceeds distinct failure domains",
        });
    }
    Ok(())
}

fn validate_manifest_shape(
    stream_id: StreamId,
    payload_format: u32,
    producer_descriptor: &[u8],
    gap_event_stream_id: Option<StreamId>,
    overflow_namespace_sha256: Option<OverflowNamespaceSha256>,
    deletion_authorizing_store_id: Option<DeletionAuthorizingStoreId>,
    durability_policy: Option<&DurabilityPolicyV1>,
) -> Result<()> {
    let custody_fields = (
        overflow_namespace_sha256.is_some(),
        deletion_authorizing_store_id.is_some(),
        durability_policy.is_some(),
    );
    match payload_format {
        1..=5 => {
            if gap_event_stream_id.is_none() || custody_fields != (true, true, true) {
                return Err(ProtocolError::InvalidManifestShape {
                    payload_format,
                    reason: "capture streams require gap, overflow, terminal-store, and policy fields",
                });
            }
            if gap_event_stream_id == Some(stream_id) {
                return Err(ProtocolError::InvalidManifestShape {
                    payload_format,
                    reason: "capture stream must reference a separate gap-event stream",
                });
            }
        }
        6 => {
            if gap_event_stream_id.is_some() || custody_fields != (false, false, false) {
                return Err(ProtocolError::InvalidManifestShape {
                    payload_format,
                    reason: "derived streams must omit custody and gap-stream fields",
                });
            }
        }
        7 => {
            if gap_event_stream_id.is_some() || custody_fields != (true, true, true) {
                return Err(ProtocolError::InvalidManifestShape {
                    payload_format,
                    reason: "gap streams require custody fields and must omit a recursive gap stream",
                });
            }
            let descriptor = GapEventProducerDescriptorV1::decode(producer_descriptor)?;
            if descriptor.target_stream_id() == stream_id {
                return Err(ProtocolError::InvalidManifestShape {
                    payload_format,
                    reason: "gap-event stream must target a separate capture stream",
                });
            }
        }
        _ => {}
    }
    Ok(())
}

fn encode_option<T>(
    encoded: &mut Vec<u8>,
    value: Option<&T>,
    encode_value: impl FnOnce(&mut Vec<u8>, &T),
) {
    if let Some(value) = value {
        encoded.push(1);
        encode_value(encoded, value);
    } else {
        encoded.push(0);
    }
}

fn encode_lineage(encoded: &mut Vec<u8>, lineage: &LineageV1) {
    encoded.extend_from_slice(lineage.predecessor_stream_id.as_bytes());
    encoded.extend_from_slice(&lineage.predecessor_last_known_cursor.fixed_encode());
    encoded.extend_from_slice(&(lineage.reason as u16).to_be_bytes());
    encoded.push(lineage.continuity as u8);
}

fn encode_policy(encoded: &mut Vec<u8>, policy: &DurabilityPolicyV1) {
    encoded.extend_from_slice(policy.policy_id.as_bytes());
    encoded.push(policy.minimum_independent_copies);
    encoded.extend_from_slice(policy.catalog_descriptor_sha256.as_bytes());
    encoded.extend_from_slice(&(policy.targets.len() as u32).to_be_bytes());
    for target in &policy.targets {
        encoded.extend_from_slice(target.target_id.as_bytes());
        encoded.extend_from_slice(target.failure_domain_id.as_bytes());
        encoded.extend_from_slice(target.target_descriptor_sha256.as_bytes());
    }
}

fn decode_option<T>(
    reader: &mut Reader<'_>,
    field: &'static str,
    decode_value: impl FnOnce(&mut Reader<'_>) -> Result<T>,
) -> Result<Option<T>> {
    match reader.u8(field)? {
        0 => Ok(None),
        1 => decode_value(reader).map(Some),
        value => Err(ProtocolError::InvalidOptionTag { field, value }),
    }
}

fn decode_lineage(reader: &mut Reader<'_>) -> Result<LineageV1> {
    Ok(LineageV1 {
        predecessor_stream_id: StreamId::try_from(
            reader.take(StreamId::LENGTH, "predecessor_stream_id")?,
        )?,
        predecessor_last_known_cursor: CursorV1::decode(reader.take(
            crate::CURSOR_V1_ENCODED_LEN,
            "predecessor_last_known_cursor",
        )?)?,
        reason: LineageReasonV1::try_from(reader.u16("lineage_reason")?)?,
        continuity: LineageContinuityV1::try_from(reader.u8("lineage_continuity")?)?,
    })
}

fn decode_policy(reader: &mut Reader<'_>) -> Result<DurabilityPolicyV1> {
    let policy_id = DurabilityPolicyId::try_from(
        reader.take(DurabilityPolicyId::LENGTH, "durability_policy_id")?,
    )?;
    let minimum_independent_copies = reader.u8("minimum_independent_copies")?;
    let catalog_descriptor_sha256 = TerminalCatalogDescriptorSha256::try_from(reader.take(
        TerminalCatalogDescriptorSha256::LENGTH,
        "catalog_descriptor_sha256",
    )?)?;
    let target_count_u32 = reader.u32("durability_target_count")?;
    let target_count =
        usize::try_from(target_count_u32).map_err(|_| ProtocolError::IntegerOverflow {
            field: "durability_target_count",
        })?;
    if !(DURABILITY_POLICY_MIN_TARGETS..=DURABILITY_POLICY_MAX_TARGETS).contains(&target_count) {
        return Err(ProtocolError::InvalidDurabilityPolicy {
            reason: "target count must be in 2..=256",
        });
    }
    let mut targets = Vec::with_capacity(target_count);
    for _ in 0..target_count {
        targets.push(DurabilityTargetV1 {
            target_id: DurabilityTargetId::try_from(
                reader.take(DurabilityTargetId::LENGTH, "durability_target_id")?,
            )?,
            failure_domain_id: FailureDomainId::try_from(
                reader.take(FailureDomainId::LENGTH, "failure_domain_id")?,
            )?,
            target_descriptor_sha256: DurabilityTargetDescriptorSha256::try_from(reader.take(
                DurabilityTargetDescriptorSha256::LENGTH,
                "target_descriptor_sha256",
            )?)?,
        });
    }
    if targets
        .windows(2)
        .any(|pair| pair[0].target_id >= pair[1].target_id)
    {
        return Err(ProtocolError::NonCanonicalOrder {
            context: "DurabilityPolicyV1.targets",
        });
    }
    DurabilityPolicyV1::new(
        policy_id,
        minimum_independent_copies,
        catalog_descriptor_sha256,
        targets,
    )
}

struct Reader<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn take(&mut self, length: usize, context: &'static str) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ProtocolError::IntegerOverflow { field: context })?;
        if end > self.encoded.len() {
            return Err(ProtocolError::Truncated {
                context,
                expected: end,
                actual: self.encoded.len(),
            });
        }
        let bytes = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8> {
        Ok(self.take(1, field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(2, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4, field)?.try_into().expect("fixed slice"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8, field)?.try_into().expect("fixed slice"),
        ))
    }

    const fn offset(&self) -> usize {
        self.offset
    }

    fn finish(self, context: &'static str) -> Result<()> {
        if self.offset != self.encoded.len() {
            return Err(ProtocolError::TrailingBytes {
                context,
                count: self.encoded.len() - self.offset,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(id: u8, domain: u8) -> DurabilityTargetV1 {
        DurabilityTargetV1 {
            target_id: DurabilityTargetId::new([id; 16]),
            failure_domain_id: FailureDomainId::new([domain; 16]),
            target_descriptor_sha256: durability_target_descriptor_sha256(&[id, domain]),
        }
    }

    fn policy() -> DurabilityPolicyV1 {
        DurabilityPolicyV1::new(
            DurabilityPolicyId::new([0x70; 16]),
            2,
            terminal_catalog_descriptor_sha256(b"terminal-catalog"),
            vec![target(2, 2), target(1, 1)],
        )
        .unwrap()
    }

    fn capture_manifest() -> StreamManifestV1 {
        StreamManifestV1::new(
            StreamId::new([0x10; 16]),
            ClusterGenesisHash::new([0x20; 32]),
            2,
            1,
            b"exact-shred-boundary-v1".to_vec(),
            None,
            Some(StreamId::new([0x30; 16])),
            Some(OverflowNamespaceSha256::new([0x40; 32])),
            Some(DeletionAuthorizingStoreId::new([0x50; 16])),
            Some(policy()),
        )
        .unwrap()
    }

    fn gap_descriptor(target_stream_id: StreamId) -> GapEventProducerDescriptorV1 {
        GapEventProducerDescriptorV1::new(
            target_stream_id,
            ProducerConfigSha256::new([0x22; 32]),
            vec![
                GapEventReasonV1::OperatorDeclared,
                GapEventReasonV1::UdpDropCounter,
            ],
            b"u64-be-counter".to_vec(),
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn capture_manifest_is_canonical_and_round_trips() {
        // This fixture was assembled from the normative field recipe and its
        // digests were computed outside this crate. Do not regenerate it by
        // calling `StreamManifestV1::encode`.
        let manifest = capture_manifest();
        assert_eq!(
            hex(manifest.stream().stream_manifest_sha256().as_bytes()),
            "962a6faeb95cd6fac5c8d4e487d61476bed05a9259017893c0869d4254157ef2"
        );
        let encoded = manifest.encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "000110101010101010101010101010101010202020202020202020202020202020202020202020202020202020202020",
                "20200000000200010a9db534a2ae6e963d4e7c12ceb201a01173f1d20ffb1c588f3f009e3160a6a5962a6faeb95cd6fa",
                "c5c8d4e487d61476bed05a9259017893c0869d4254157ef2000000000000001765786163742d73687265642d626f756e",
                "646172792d76310001303030303030303030303030303030300140404040404040404040404040404040404040404040",
                "404040404040404040400150505050505050505050505050505050017070707070707070707070707070707002aff1b9",
                "9d54b7292f4a6d8e2847f6c04bee3f09205ed2840c2afaa9c335ffd66600000002010101010101010101010101010101",
                "0101010101010101010101010101010101623e76aefe3d4f3ed414f60681e95a4e58a6607ff396b3ad977f2901f90815",
                "3102020202020202020202020202020202020202020202020202020202020202025c6c79c26b4ddc8f1ec1a35bc08ca0",
                "721641236ee14ac95db4c0542fd823f4e5"
            )
        );
        assert_eq!(StreamManifestV1::decode(&encoded), Ok(manifest));
    }

    #[test]
    fn descriptor_hash_domains_have_independent_golden_values() {
        assert_eq!(
            hex(producer_config_sha256(b"exact-shred-boundary-v1").as_bytes()),
            "0a9db534a2ae6e963d4e7c12ceb201a01173f1d20ffb1c588f3f009e3160a6a5"
        );
        assert_eq!(
            hex(durability_target_descriptor_sha256(&[1, 1]).as_bytes()),
            "623e76aefe3d4f3ed414f60681e95a4e58a6607ff396b3ad977f2901f9081531"
        );
        assert_eq!(
            hex(terminal_catalog_descriptor_sha256(b"terminal-catalog").as_bytes()),
            "aff1b99d54b7292f4a6d8e2847f6c04bee3f09205ed2840c2afaa9c335ffd666"
        );
    }

    #[test]
    fn gap_event_descriptor_is_golden_canonical_and_bounded() {
        let descriptor = gap_descriptor(StreamId::new([0x11; 16]));
        assert_eq!(
            descriptor.permitted_reasons(),
            &[
                GapEventReasonV1::UdpDropCounter,
                GapEventReasonV1::OperatorDeclared,
            ]
        );
        let encoded = descriptor.encode();
        assert_eq!(
            hex(&encoded),
            concat!(
                "000111111111111111111111111111111111",
                "2222222222222222222222222222222222222222222222222222222222222222",
                "0000000200020005",
                "000000000000000e7536342d62652d636f756e746572"
            )
        );
        assert_eq!(
            GapEventProducerDescriptorV1::decode(&encoded),
            Ok(descriptor)
        );

        let mut noncanonical = encoded.clone();
        let reasons_offset = 2 + StreamId::LENGTH + ProducerConfigSha256::LENGTH + 4;
        noncanonical[reasons_offset..reasons_offset + 2]
            .copy_from_slice(&(GapEventReasonV1::OperatorDeclared as u16).to_be_bytes());
        noncanonical[reasons_offset + 2..reasons_offset + 4]
            .copy_from_slice(&(GapEventReasonV1::UdpDropCounter as u16).to_be_bytes());
        assert!(matches!(
            GapEventProducerDescriptorV1::decode(&noncanonical),
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        let mut oversized = encoded;
        let source_len_offset = reasons_offset + 4;
        oversized[source_len_offset..source_len_offset + 8]
            .copy_from_slice(&(MAX_SOURCE_POSITION_DESCRIPTOR_BYTES + 1).to_be_bytes());
        assert_eq!(
            GapEventProducerDescriptorV1::decode(&oversized),
            Err(ProtocolError::SourcePositionDescriptorTooLarge {
                actual: MAX_SOURCE_POSITION_DESCRIPTOR_BYTES + 1,
                max: MAX_SOURCE_POSITION_DESCRIPTOR_BYTES,
            })
        );
    }

    #[test]
    fn durability_targets_are_sorted_and_require_distinct_domains() {
        let policy = policy();
        assert_eq!(
            policy.targets()[0].target_id,
            DurabilityTargetId::new([1; 16])
        );
        assert_eq!(
            policy.targets()[1].target_id,
            DurabilityTargetId::new([2; 16])
        );

        assert!(matches!(
            DurabilityPolicyV1::new(
                DurabilityPolicyId::new([0; 16]),
                2,
                TerminalCatalogDescriptorSha256::new([0; 32]),
                vec![target(1, 1), target(2, 1)],
            ),
            Err(ProtocolError::InvalidDurabilityPolicy { .. })
        ));
        assert!(matches!(
            DurabilityPolicyV1::new(
                DurabilityPolicyId::new([0; 16]),
                2,
                TerminalCatalogDescriptorSha256::new([0; 32]),
                vec![target(1, 1), target(1, 2)],
            ),
            Err(ProtocolError::InvalidDurabilityPolicy { .. })
        ));

        let policy_with_reused_nonessential_domain = DurabilityPolicyV1::new(
            DurabilityPolicyId::new([0; 16]),
            2,
            TerminalCatalogDescriptorSha256::new([0; 32]),
            vec![target(1, 1), target(2, 2), target(3, 2)],
        )
        .unwrap();
        assert_eq!(policy_with_reused_nonessential_domain.targets().len(), 3);
    }

    #[test]
    fn format_specific_custody_shape_fails_closed() {
        let invalid_capture = StreamManifestV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            1,
            1,
            b"grpc".to_vec(),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(matches!(
            invalid_capture,
            Err(ProtocolError::InvalidManifestShape { .. })
        ));

        let derived = StreamManifestV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            6,
            1,
            b"derived-observation".to_vec(),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(StreamManifestV1::decode(&derived.encode()), Ok(derived));

        let derived_with_custody = StreamManifestV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            6,
            1,
            b"derived-observation".to_vec(),
            None,
            None,
            Some(OverflowNamespaceSha256::new([3; 32])),
            None,
            None,
        );
        assert!(matches!(
            derived_with_custody,
            Err(ProtocolError::InvalidManifestShape { .. })
        ));

        let capture_stream_id = StreamId::new([0x10; 16]);
        let gap_stream_id = StreamId::new([0x30; 16]);
        for payload_format in 1..=5 {
            StreamManifestV1::new(
                capture_stream_id,
                ClusterGenesisHash::new([2; 32]),
                payload_format,
                1,
                b"capture".to_vec(),
                None,
                Some(gap_stream_id),
                Some(OverflowNamespaceSha256::new([3; 32])),
                Some(DeletionAuthorizingStoreId::new([4; 16])),
                Some(policy()),
            )
            .unwrap();
        }

        let gap_manifest = StreamManifestV1::new(
            gap_stream_id,
            ClusterGenesisHash::new([2; 32]),
            7,
            1,
            gap_descriptor(capture_stream_id).encode(),
            None,
            None,
            Some(OverflowNamespaceSha256::new([3; 32])),
            Some(DeletionAuthorizingStoreId::new([4; 16])),
            Some(policy()),
        )
        .unwrap();
        assert_eq!(
            StreamManifestV1::decode(&gap_manifest.encode()),
            Ok(gap_manifest)
        );

        let self_referencing_capture = StreamManifestV1::new(
            capture_stream_id,
            ClusterGenesisHash::new([2; 32]),
            2,
            1,
            b"capture".to_vec(),
            None,
            Some(capture_stream_id),
            Some(OverflowNamespaceSha256::new([3; 32])),
            Some(DeletionAuthorizingStoreId::new([4; 16])),
            Some(policy()),
        );
        assert!(matches!(
            self_referencing_capture,
            Err(ProtocolError::InvalidManifestShape { .. })
        ));

        let self_targeting_gap = StreamManifestV1::new(
            gap_stream_id,
            ClusterGenesisHash::new([2; 32]),
            7,
            1,
            gap_descriptor(gap_stream_id).encode(),
            None,
            None,
            Some(OverflowNamespaceSha256::new([3; 32])),
            Some(DeletionAuthorizingStoreId::new([4; 16])),
            Some(policy()),
        );
        assert!(matches!(
            self_targeting_gap,
            Err(ProtocolError::InvalidManifestShape { .. })
        ));
    }

    #[test]
    fn descriptor_and_manifest_hash_tampering_is_rejected() {
        let manifest = capture_manifest();
        let mut descriptor_tampered = manifest.encode();
        let descriptor_offset = 2 + crate::STREAM_HEADER_V1_ENCODED_LEN + 8;
        descriptor_tampered[descriptor_offset] ^= 1;
        assert_eq!(
            StreamManifestV1::decode(&descriptor_tampered),
            Err(ProtocolError::ProducerConfigMismatch)
        );

        let mut hash_tampered = manifest.encode();
        let manifest_hash_offset = 2 + 16 + 32 + 4 + 2 + 32;
        hash_tampered[manifest_hash_offset] ^= 1;
        assert_eq!(
            StreamManifestV1::decode(&hash_tampered),
            Err(ProtocolError::ManifestHashMismatch)
        );
    }

    #[test]
    fn decoder_rejects_noncanonical_target_order_and_trailing_bytes() {
        let manifest = capture_manifest();
        let mut encoded = manifest.encode();
        let policy_len = 16 + 1 + 32 + 4;
        let target_len = 16 + 16 + 32;
        let policy_start = encoded.len() - (policy_len + 2 * target_len);
        let targets_start = policy_start + policy_len;
        let first = encoded[targets_start..targets_start + target_len].to_vec();
        let second = encoded[targets_start + target_len..targets_start + 2 * target_len].to_vec();
        encoded[targets_start..targets_start + target_len].copy_from_slice(&second);
        encoded[targets_start + target_len..targets_start + 2 * target_len].copy_from_slice(&first);
        assert!(matches!(
            StreamManifestV1::decode(&encoded),
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        let mut trailing = manifest.encode();
        trailing.push(0);
        assert!(matches!(
            StreamManifestV1::decode(&trailing),
            Err(ProtocolError::TrailingBytes { .. })
        ));
    }

    #[test]
    fn decoder_rejects_declared_lengths_before_allocating() {
        let manifest = capture_manifest();
        let mut oversized_descriptor = manifest.encode();
        let descriptor_len_offset = 2 + crate::STREAM_HEADER_V1_ENCODED_LEN;
        oversized_descriptor[descriptor_len_offset..descriptor_len_offset + 8]
            .copy_from_slice(&(MAX_PRODUCER_DESCRIPTOR_BYTES + 1).to_be_bytes());
        assert_eq!(
            StreamManifestV1::decode(&oversized_descriptor),
            Err(ProtocolError::ProducerDescriptorTooLarge {
                actual: MAX_PRODUCER_DESCRIPTOR_BYTES + 1,
                max: MAX_PRODUCER_DESCRIPTOR_BYTES,
            })
        );

        let mut oversized_target_count = manifest.encode();
        let policy_fixed_len = 16 + 1 + 32 + 4;
        let target_len = 16 + 16 + 32;
        let policy_start = oversized_target_count.len() - (policy_fixed_len + 2 * target_len);
        let target_count_offset = policy_start + 16 + 1 + 32;
        oversized_target_count[target_count_offset..target_count_offset + 4]
            .copy_from_slice(&((DURABILITY_POLICY_MAX_TARGETS as u32) + 1).to_be_bytes());
        assert!(matches!(
            StreamManifestV1::decode(&oversized_target_count),
            Err(ProtocolError::InvalidDurabilityPolicy { .. })
        ));
    }
}

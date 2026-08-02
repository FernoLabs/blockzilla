use crate::codec::{Reader, put_option, put_u8, put_u16, put_u32, put_u64};
use crate::job::{OLD_FAITHFUL_CAR_LOGICAL_NAME_V1, OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1};
use crate::types::{MAX_REQUIRED_INPUTS, validate_unique_object_keys};
use crate::{
    CompactionJobSpecV1, HashedDescriptorV1, InputObjectV1, InputStreamRangeV1, ProtocolError,
    Result, SlotRangeV1,
};

pub const MAX_FINALITY_ENTRIES: usize = 1_048_576;
pub const FINALITY_MANIFEST_VERSION_V2: u16 = 2;

/// Era-exact identity of one finalized produced block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizedBlockIdentityV1 {
    final_poh_hash: [u8; 32],
    consensus_block_id: Option<[u8; 32]>,
}

impl FinalizedBlockIdentityV1 {
    #[must_use]
    pub const fn new(final_poh_hash: [u8; 32], consensus_block_id: Option<[u8; 32]>) -> Self {
        Self {
            final_poh_hash,
            consensus_block_id,
        }
    }

    #[must_use]
    pub const fn final_poh_hash(self) -> [u8; 32] {
        self.final_poh_hash
    }

    #[must_use]
    pub const fn consensus_block_id(self) -> Option<[u8; 32]> {
        self.consensus_block_id
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(if self.consensus_block_id.is_some() {
            65
        } else {
            33
        });
        self.encode_into(&mut output);
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("FinalizedBlockIdentityV1")?;
        Ok(value)
    }

    fn encode_into(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.final_poh_hash);
        put_option(
            output,
            self.consensus_block_id.as_ref(),
            |output, consensus_block_id| {
                output.extend_from_slice(consensus_block_id);
                Ok(())
            },
        )
        .expect("fixed consensus block ID always encodes");
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let final_poh_hash = reader.array("FinalizedBlockIdentityV1.final_poh_hash")?;
        let consensus_block_id = reader
            .option("FinalizedBlockIdentityV1.consensus_block_id", |reader| {
                reader.array("FinalizedBlockIdentityV1.consensus_block_id")
            })?;
        Ok(Self::new(final_poh_hash, consensus_block_id))
    }
}

/// Exact finalized disposition for one slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizedDispositionV1 {
    Produced { identity: FinalizedBlockIdentityV1 },
    Skipped,
    Unresolved,
}

impl FinalizedDispositionV1 {
    const PRODUCED: u8 = 1;
    const SKIPPED: u8 = 2;
    const UNRESOLVED: u8 = 3;

    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Produced { .. } => Self::PRODUCED,
            Self::Skipped => Self::SKIPPED,
            Self::Unresolved => Self::UNRESOLVED,
        }
    }
}

/// One slot and its authoritative finalized disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizedSlotV1 {
    slot: u64,
    disposition: FinalizedDispositionV1,
}

impl FinalizedSlotV1 {
    #[must_use]
    pub const fn new(slot: u64, disposition: FinalizedDispositionV1) -> Self {
        Self { slot, disposition }
    }

    #[must_use]
    pub const fn produced(slot: u64, identity: FinalizedBlockIdentityV1) -> Self {
        Self::new(slot, FinalizedDispositionV1::Produced { identity })
    }

    #[must_use]
    pub const fn skipped(slot: u64) -> Self {
        Self::new(slot, FinalizedDispositionV1::Skipped)
    }

    #[must_use]
    pub const fn unresolved(slot: u64) -> Self {
        Self::new(slot, FinalizedDispositionV1::Unresolved)
    }

    #[must_use]
    pub const fn slot(self) -> u64 {
        self.slot
    }

    #[must_use]
    pub const fn disposition(self) -> FinalizedDispositionV1 {
        self.disposition
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(74);
        self.encode_into(&mut output);
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("FinalizedSlotV1")?;
        Ok(value)
    }

    fn encode_into(&self, output: &mut Vec<u8>) {
        put_u64(output, self.slot);
        put_u8(output, self.disposition.tag());
        if let FinalizedDispositionV1::Produced { identity } = self.disposition {
            identity.encode_into(output);
        }
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let slot = reader.u64("FinalizedSlotV1.slot")?;
        let disposition = match reader.u8("FinalizedSlotV1.disposition")? {
            FinalizedDispositionV1::PRODUCED => FinalizedDispositionV1::Produced {
                identity: FinalizedBlockIdentityV1::decode_from(reader)?,
            },
            FinalizedDispositionV1::SKIPPED => FinalizedDispositionV1::Skipped,
            FinalizedDispositionV1::UNRESOLVED => FinalizedDispositionV1::Unresolved,
            value => {
                return Err(ProtocolError::UnknownEnum {
                    field: "FinalizedSlotV1.disposition",
                    value,
                });
            }
        };
        Ok(Self::new(slot, disposition))
    }
}

/// Finalized parent immediately outside the manifest range when required.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizedParentAnchorV1 {
    slot: u64,
    identity: FinalizedBlockIdentityV1,
}

impl FinalizedParentAnchorV1 {
    #[must_use]
    pub const fn new(slot: u64, identity: FinalizedBlockIdentityV1) -> Self {
        Self { slot, identity }
    }

    #[must_use]
    pub const fn slot(self) -> u64 {
        self.slot
    }

    #[must_use]
    pub const fn identity(self) -> FinalizedBlockIdentityV1 {
        self.identity
    }

    #[must_use]
    pub const fn final_poh_hash(self) -> [u8; 32] {
        self.identity.final_poh_hash()
    }

    #[must_use]
    pub const fn consensus_block_id(self) -> Option<[u8; 32]> {
        self.identity.consensus_block_id()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(if self.identity.consensus_block_id().is_some() {
            73
        } else {
            41
        });
        self.encode_into(&mut encoded);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("FinalizedParentAnchorV1")?;
        Ok(value)
    }

    fn encode_into(&self, output: &mut Vec<u8>) {
        put_u64(output, self.slot);
        self.identity.encode_into(output);
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        Ok(Self::new(
            reader.u64("FinalizedParentAnchorV1.slot")?,
            FinalizedBlockIdentityV1::decode_from(reader)?,
        ))
    }
}

/// Immutable full-slot finality decision and its exact custodial evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalityManifestV1 {
    manifest_version: u16,
    cluster_genesis_hash: [u8; 32],
    epoch: u64,
    slots: SlotRangeV1,
    finality_validation_slots: SlotRangeV1,
    validation_slots: SlotRangeV1,
    authority: HashedDescriptorV1,
    evidence_stream_inputs: Vec<InputStreamRangeV1>,
    evidence_object_inputs: Vec<InputObjectV1>,
    predecessor_parent: Option<FinalizedParentAnchorV1>,
    entries: Vec<FinalizedSlotV1>,
}

impl FinalityManifestV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_genesis_hash: [u8; 32],
        epoch: u64,
        slots: SlotRangeV1,
        finality_validation_slots: SlotRangeV1,
        validation_slots: SlotRangeV1,
        authority: HashedDescriptorV1,
        evidence_stream_inputs: Vec<InputStreamRangeV1>,
        evidence_object_inputs: Vec<InputObjectV1>,
        predecessor_parent: Option<FinalizedParentAnchorV1>,
        entries: Vec<FinalizedSlotV1>,
    ) -> Result<Self> {
        validate_ranges(slots, finality_validation_slots, validation_slots)?;
        validate_evidence_streams(&evidence_stream_inputs, cluster_genesis_hash)?;
        validate_evidence_objects(&evidence_object_inputs)?;
        validate_entries(slots, &entries)?;
        validate_predecessor_parent(slots, predecessor_parent, &entries)?;
        Ok(Self {
            manifest_version: FINALITY_MANIFEST_VERSION_V2,
            cluster_genesis_hash,
            epoch,
            slots,
            finality_validation_slots,
            validation_slots,
            authority,
            evidence_stream_inputs,
            evidence_object_inputs,
            predecessor_parent,
            entries,
        })
    }

    #[must_use]
    pub const fn manifest_version(&self) -> u16 {
        self.manifest_version
    }

    #[must_use]
    pub const fn cluster_genesis_hash(&self) -> [u8; 32] {
        self.cluster_genesis_hash
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub const fn slots(&self) -> SlotRangeV1 {
        self.slots
    }

    #[must_use]
    pub const fn finality_validation_slots(&self) -> SlotRangeV1 {
        self.finality_validation_slots
    }

    #[must_use]
    pub const fn validation_slots(&self) -> SlotRangeV1 {
        self.validation_slots
    }

    #[must_use]
    pub const fn authority(&self) -> &HashedDescriptorV1 {
        &self.authority
    }

    #[must_use]
    pub fn evidence_stream_inputs(&self) -> &[InputStreamRangeV1] {
        &self.evidence_stream_inputs
    }

    #[must_use]
    pub fn evidence_object_inputs(&self) -> &[InputObjectV1] {
        &self.evidence_object_inputs
    }

    #[must_use]
    pub const fn predecessor_parent(&self) -> Option<FinalizedParentAnchorV1> {
        self.predecessor_parent
    }

    #[must_use]
    pub fn entries(&self) -> &[FinalizedSlotV1] {
        &self.entries
    }

    #[must_use]
    pub fn has_unresolved(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.disposition == FinalizedDispositionV1::Unresolved)
    }

    /// Return exact produced/skipped counts only for a publishable manifest.
    pub fn complete_coverage_counts(&self) -> Result<(u32, u32)> {
        let mut produced = 0u32;
        let mut skipped = 0u32;
        for entry in &self.entries {
            match entry.disposition {
                FinalizedDispositionV1::Produced { .. } => {
                    produced = produced
                        .checked_add(1)
                        .ok_or(ProtocolError::IntegerOverflow {
                            field: "FinalityManifestV1.produced_count",
                        })?;
                }
                FinalizedDispositionV1::Skipped => {
                    skipped = skipped
                        .checked_add(1)
                        .ok_or(ProtocolError::IntegerOverflow {
                            field: "FinalityManifestV1.skipped_count",
                        })?;
                }
                FinalizedDispositionV1::Unresolved => {
                    return Err(ProtocolError::InvalidField {
                        field: "FinalityManifestV1.entries",
                        reason: "UNRESOLVED entries cannot publish a COMPLETE result",
                    });
                }
            }
        }
        Ok((produced, skipped))
    }

    /// Exact canonical finality-manifest object bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        put_u16(&mut output, self.manifest_version);
        output.extend_from_slice(&self.cluster_genesis_hash);
        put_u64(&mut output, self.epoch);
        self.slots.encode_into(&mut output);
        self.finality_validation_slots.encode_into(&mut output);
        self.validation_slots.encode_into(&mut output);
        self.authority
            .encode_into(&mut output)
            .expect("validated descriptor always encodes");
        put_u32(
            &mut output,
            self.evidence_stream_inputs.len(),
            "FinalityManifestV1.evidence_stream_inputs",
        )
        .expect("bounded stream evidence count always fits u32");
        for input in &self.evidence_stream_inputs {
            input.encode_into(&mut output);
        }
        put_u32(
            &mut output,
            self.evidence_object_inputs.len(),
            "FinalityManifestV1.evidence_object_inputs",
        )
        .expect("bounded object evidence count always fits u32");
        for input in &self.evidence_object_inputs {
            input
                .encode_into(&mut output)
                .expect("validated input object always encodes");
        }
        put_option(
            &mut output,
            self.predecessor_parent.as_ref(),
            |output, anchor| {
                anchor.encode_into(output);
                Ok(())
            },
        )
        .expect("fixed parent anchor always encodes");
        put_u32(
            &mut output,
            self.entries.len(),
            "FinalityManifestV1.entries",
        )
        .expect("bounded finality entry count always fits u32");
        for entry in &self.entries {
            entry.encode_into(&mut output);
        }
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let manifest_version = reader.u16("FinalityManifestV1.manifest_version")?;
        if manifest_version != FINALITY_MANIFEST_VERSION_V2 {
            return Err(ProtocolError::UnknownFinalityManifestVersion {
                value: manifest_version,
            });
        }
        let cluster_genesis_hash = reader.array("FinalityManifestV1.cluster_genesis_hash")?;
        let epoch = reader.u64("FinalityManifestV1.epoch")?;
        let slots = SlotRangeV1::decode_from(&mut reader)?;
        let finality_validation_slots = SlotRangeV1::decode_from(&mut reader)?;
        let validation_slots = SlotRangeV1::decode_from(&mut reader)?;
        validate_ranges(slots, finality_validation_slots, validation_slots)?;
        let authority = HashedDescriptorV1::decode_from(&mut reader)?;
        let stream_count = reader.count(
            MAX_REQUIRED_INPUTS,
            "FinalityManifestV1.evidence_stream_inputs",
        )?;
        let mut evidence_stream_inputs = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            evidence_stream_inputs.push(InputStreamRangeV1::decode_from(&mut reader)?);
        }
        let object_count = reader.count(
            MAX_REQUIRED_INPUTS,
            "FinalityManifestV1.evidence_object_inputs",
        )?;
        let mut evidence_object_inputs = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            evidence_object_inputs.push(InputObjectV1::decode_from(&mut reader)?);
        }
        let predecessor_parent = reader.option(
            "FinalityManifestV1.predecessor_parent",
            FinalizedParentAnchorV1::decode_from,
        )?;
        let entry_count = reader.count(MAX_FINALITY_ENTRIES, "FinalityManifestV1.entries")?;
        let expected_entry_count =
            usize::try_from(slots.len()).map_err(|_| ProtocolError::IntegerOverflow {
                field: "FinalityManifestV1.entries",
            })?;
        if entry_count != expected_entry_count {
            return Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.entries",
                reason: "entry count must equal the published slot-range length",
            });
        }
        let minimum_entry_bytes =
            entry_count
                .checked_mul(9)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "FinalityManifestV1.entries",
                })?;
        if reader.remaining() < minimum_entry_bytes {
            return Err(ProtocolError::Truncated {
                context: "FinalityManifestV1.entries",
                needed: minimum_entry_bytes,
                remaining: reader.remaining(),
            });
        }
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(FinalizedSlotV1::decode_from(&mut reader)?);
        }
        reader.finish("FinalityManifestV1")?;
        Self::new(
            cluster_genesis_hash,
            epoch,
            slots,
            finality_validation_slots,
            validation_slots,
            authority,
            evidence_stream_inputs,
            evidence_object_inputs,
            predecessor_parent,
            entries,
        )
    }

    /// Validates the manifest's exact immutable job binding.
    ///
    /// Authority-descriptor interpretation and proof semantics remain the
    /// responsibility of the configured finality-authority validator.
    pub fn validate_against_job(&self, job: &CompactionJobSpecV1) -> Result<()> {
        if self.cluster_genesis_hash != job.cluster_genesis_hash()
            || self.epoch != job.epoch()
            || self.slots != job.slots()
        {
            return Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.job_binding",
                reason: "cluster, epoch, and slots must exactly equal the job",
            });
        }
        if self
            .evidence_stream_inputs
            .iter()
            .any(|input| !job.required_stream_inputs().contains(input))
            || self
                .evidence_object_inputs
                .iter()
                .any(|input| !job.required_object_inputs().contains(input))
        {
            return Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.evidence_inputs",
                reason: "every evidence input must exactly equal a corresponding required job input",
            });
        }
        job.finality_manifest().verify_bytes(&self.encode())
    }
}

fn validate_ranges(
    slots: SlotRangeV1,
    finality_validation_slots: SlotRangeV1,
    validation_slots: SlotRangeV1,
) -> Result<()> {
    if finality_validation_slots.first_slot() != slots.first_slot() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.finality_validation_slots.first_slot",
            reason: "must equal slots.first_slot",
        });
    }
    if validation_slots.first_slot() != slots.first_slot() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.validation_slots.first_slot",
            reason: "must equal slots.first_slot",
        });
    }
    if slots.next_slot() > finality_validation_slots.next_slot() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.finality_validation_slots.next_slot",
            reason: "must be at least slots.next_slot",
        });
    }
    if finality_validation_slots.next_slot() > validation_slots.next_slot() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.validation_slots.next_slot",
            reason: "must be at least finality_validation_slots.next_slot",
        });
    }
    if validation_slots.len() > MAX_FINALITY_ENTRIES as u64 {
        return Err(ProtocolError::CountOutOfBounds {
            field: "FinalityManifestV1.validation_slots",
            max: MAX_FINALITY_ENTRIES,
            actual: validation_slots.len(),
        });
    }
    Ok(())
}

fn validate_predecessor_parent(
    slots: SlotRangeV1,
    predecessor_parent: Option<FinalizedParentAnchorV1>,
    entries: &[FinalizedSlotV1],
) -> Result<()> {
    if predecessor_parent.is_some_and(|anchor| anchor.slot >= slots.first_slot()) {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.predecessor_parent.slot",
            reason: "must be before the manifest slot range",
        });
    }

    let first_produced_slot = entries.iter().find_map(|entry| {
        matches!(entry.disposition(), FinalizedDispositionV1::Produced { .. })
            .then_some(entry.slot())
    });
    let anchor_required = first_produced_slot.is_some_and(|slot| slot != 0);
    if anchor_required != predecessor_parent.is_some() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.predecessor_parent",
            reason: "must be present exactly when the first produced slot is non-genesis",
        });
    }
    Ok(())
}

fn validate_evidence_streams(
    inputs: &[InputStreamRangeV1],
    cluster_genesis_hash: [u8; 32],
) -> Result<()> {
    if inputs.len() > MAX_REQUIRED_INPUTS {
        return Err(ProtocolError::CountOutOfBounds {
            field: "FinalityManifestV1.evidence_stream_inputs",
            max: MAX_REQUIRED_INPUTS,
            actual: inputs.len() as u64,
        });
    }
    if inputs
        .windows(2)
        .any(|pair| pair[0].stream().stream_id() >= pair[1].stream().stream_id())
    {
        return Err(ProtocolError::NonCanonicalOrder {
            field: "FinalityManifestV1.evidence_stream_inputs",
        });
    }
    if inputs
        .iter()
        .any(|input| input.stream().cluster_genesis_hash().as_bytes() != &cluster_genesis_hash)
    {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.evidence_stream_inputs",
            reason: "every evidence stream must use the manifest cluster genesis hash",
        });
    }
    Ok(())
}

fn validate_evidence_objects(inputs: &[InputObjectV1]) -> Result<()> {
    if inputs.len() > MAX_REQUIRED_INPUTS {
        return Err(ProtocolError::CountOutOfBounds {
            field: "FinalityManifestV1.evidence_object_inputs",
            max: MAX_REQUIRED_INPUTS,
            actual: inputs.len() as u64,
        });
    }
    if inputs
        .windows(2)
        .any(|pair| pair[0].logical_name() >= pair[1].logical_name())
    {
        return Err(ProtocolError::NonCanonicalOrder {
            field: "FinalityManifestV1.evidence_object_inputs",
        });
    }
    if inputs.iter().any(|input| {
        !matches!(
            input.logical_name(),
            OLD_FAITHFUL_CAR_LOGICAL_NAME_V1 | OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1
        )
    }) {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.evidence_object_inputs.logical_name",
            reason: "logical name is not registered as a V1 finite input",
        });
    }
    validate_unique_object_keys(
        inputs.iter().map(InputObjectV1::object),
        "FinalityManifestV1.evidence_object_inputs",
    )
}

fn validate_entries(slots: SlotRangeV1, entries: &[FinalizedSlotV1]) -> Result<()> {
    if entries.len() > MAX_FINALITY_ENTRIES {
        return Err(ProtocolError::CountOutOfBounds {
            field: "FinalityManifestV1.entries",
            max: MAX_FINALITY_ENTRIES,
            actual: entries.len() as u64,
        });
    }
    if entries.len() as u64 != slots.len() {
        return Err(ProtocolError::InvalidField {
            field: "FinalityManifestV1.entries",
            reason: "must contain every slot in the manifest range exactly once",
        });
    }
    for (offset, entry) in entries.iter().enumerate() {
        let offset = u64::try_from(offset).map_err(|_| ProtocolError::IntegerOverflow {
            field: "FinalityManifestV1.entries",
        })?;
        let expected =
            slots
                .first_slot()
                .checked_add(offset)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "FinalityManifestV1.entries.slot",
                })?;
        if entry.slot != expected {
            return Err(ProtocolError::NonCanonicalOrder {
                field: "FinalityManifestV1.entries",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use hivezilla_protocol::{
        ClusterGenesisHash, CursorV1, PrefixHash, ProducerConfigSha256, StreamHeaderV1, StreamId,
        StreamManifestSha256,
    };

    use crate::types::ordinary_sha256;
    use crate::{
        OLD_FAITHFUL_CAR_LOGICAL_NAME_V1, OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1, ObjectRefV1,
    };

    use super::*;

    fn descriptor(bytes: &[u8]) -> HashedDescriptorV1 {
        HashedDescriptorV1::new(bytes.to_vec()).unwrap()
    }

    fn object(key: &[u8], len: u64, digest: u8) -> ObjectRefV1 {
        ObjectRefV1::new(key.to_vec(), None, len, [digest; 32]).unwrap()
    }

    const fn identity(poh: u8, consensus: Option<u8>) -> FinalizedBlockIdentityV1 {
        FinalizedBlockIdentityV1::new(
            [poh; 32],
            match consensus {
                Some(value) => Some([value; 32]),
                None => None,
            },
        )
    }

    fn stream_input(id: u8) -> InputStreamRangeV1 {
        let stream = StreamHeaderV1::new(
            StreamId::new([id; 16]),
            ClusterGenesisHash::new([1; 32]),
            1,
            1,
            ProducerConfigSha256::new([id.wrapping_add(1); 32]),
            StreamManifestSha256::new([id.wrapping_add(2); 32]),
        )
        .unwrap();
        InputStreamRangeV1::new(
            stream,
            CursorV1::new(0, PrefixHash::new([id.wrapping_add(3); 32])),
            CursorV1::new(1, PrefixHash::new([id.wrapping_add(4); 32])),
        )
        .unwrap()
    }

    fn golden_manifest() -> FinalityManifestV1 {
        FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 13).unwrap(),
            SlotRangeV1::new(10, 15).unwrap(),
            SlotRangeV1::new(10, 16).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            Some(FinalizedParentAnchorV1::new(9, identity(2, Some(4)))),
            vec![
                FinalizedSlotV1::produced(10, identity(3, Some(5))),
                FinalizedSlotV1::skipped(11),
                FinalizedSlotV1::unresolved(12),
            ],
        )
        .unwrap()
    }

    fn one_slot_manifest(
        finality_validation_slots: SlotRangeV1,
        validation_slots: SlotRangeV1,
    ) -> Result<FinalityManifestV1> {
        FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            finality_validation_slots,
            validation_slots,
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            None,
            vec![FinalizedSlotV1::skipped(10)],
        )
    }

    /// Frozen test representation of the superseded, unversioned V1 wire.
    ///
    /// Unlike V2, a produced entry and predecessor parent each carried one
    /// opaque block hash. Keeping this encoder independent of the current
    /// production types makes the version-collision fixtures genuine V1
    /// payloads rather than mutated V2 payloads.
    #[derive(Clone, Copy)]
    enum FrozenLegacyDispositionV1 {
        Produced([u8; 32]),
        Skipped,
    }

    fn encode_frozen_legacy_manifest_v1(
        predecessor_parent: Option<(u64, [u8; 32])>,
        entries: &[(u64, FrozenLegacyDispositionV1)],
    ) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut cluster_genesis_hash = [0x11; 32];
        cluster_genesis_hash[..2].copy_from_slice(&FINALITY_MANIFEST_VERSION_V2.to_be_bytes());

        // The legacy wire began directly with the genesis hash: it had no
        // manifest-version field.
        encoded.extend_from_slice(&cluster_genesis_hash);
        put_u64(&mut encoded, 7);
        SlotRangeV1::new(10, 12).unwrap().encode_into(&mut encoded);
        descriptor(b"authority-v1")
            .encode_into(&mut encoded)
            .unwrap();
        encoded.extend_from_slice(&0_u32.to_be_bytes()); // evidence streams
        encoded.extend_from_slice(&0_u32.to_be_bytes()); // evidence objects
        put_option(
            &mut encoded,
            predecessor_parent.as_ref(),
            |output, (slot, block_hash)| {
                put_u64(output, *slot);
                output.extend_from_slice(block_hash);
                Ok(())
            },
        )
        .unwrap();
        encoded.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (slot, disposition) in entries {
            put_u64(&mut encoded, *slot);
            match disposition {
                FrozenLegacyDispositionV1::Produced(block_hash) => {
                    put_u8(&mut encoded, FinalizedDispositionV1::PRODUCED);
                    encoded.extend_from_slice(block_hash);
                }
                FrozenLegacyDispositionV1::Skipped => {
                    put_u8(&mut encoded, FinalizedDispositionV1::SKIPPED);
                }
            }
        }
        encoded
    }

    #[test]
    fn finality_manifest_encoding_is_golden() {
        let manifest = golden_manifest();
        let encoded = manifest.encode();
        assert_eq!(manifest.manifest_version(), FINALITY_MANIFEST_VERSION_V2);
        assert_eq!(manifest.finality_validation_slots().next_slot(), 15);
        assert_eq!(manifest.validation_slots().next_slot(), 16);
        assert_eq!(&encoded[..2], &FINALITY_MANIFEST_VERSION_V2.to_be_bytes());
        assert_eq!(encoded.len(), 316);
        const GOLDEN_MANIFEST_V2_HEX: &str = concat!(
            "0002",                                                             // manifest version
            "0101010101010101010101010101010101010101010101010101010101010101", // genesis
            "0000000000000007",                                                 // epoch
            "000000000000000a000000000000000d",                                 // published slots
            "000000000000000a000000000000000f",                                 // finality slots
            "000000000000000a0000000000000010",                                 // validation slots
            "67908dfe94e28c4e1fa39b3d880184ad4fc63a1b9362d5349c44d480d0093888", // authority hash
            "0000000c617574686f726974792d7631",                                 // authority bytes
            "00000000",                                                         // evidence streams
            "00000000",                                                         // evidence objects
            "01",               // predecessor parent: Some
            "0000000000000009", // predecessor slot
            "0202020202020202020202020202020202020202020202020202020202020202", // parent PoH
            "01",               // parent consensus block ID: Some
            "0404040404040404040404040404040404040404040404040404040404040404", // parent consensus block ID
            "00000003",                                                         // entry count
            "000000000000000a01",                                               // slot 10, produced
            "0303030303030303030303030303030303030303030303030303030303030303", // slot 10 PoH
            "01", // slot 10 consensus block ID: Some
            "0505050505050505050505050505050505050505050505050505050505050505", // slot 10 consensus block ID
            "000000000000000b02",                                               // slot 11, skipped
            "000000000000000c03", // slot 12, unresolved
        );
        assert_eq!(to_hex(&encoded), GOLDEN_MANIFEST_V2_HEX);
        assert_eq!(
            to_hex(&ordinary_sha256(&encoded)),
            "3ebb765404737486c8f43ae4add4f80f5e0cd7ef3c15c53e774f42fb81275ec1"
        );
        assert!(manifest.has_unresolved());
        assert_eq!(FinalityManifestV1::decode(&encoded), Ok(manifest));
    }

    #[test]
    fn manifest_rejects_unknown_and_frozen_legacy_v1_shapes() {
        let encoded = golden_manifest().encode();

        let mut unknown = encoded.clone();
        unknown[..2].copy_from_slice(&3_u16.to_be_bytes());
        assert_eq!(
            FinalityManifestV1::decode(&unknown),
            Err(ProtocolError::UnknownFinalityManifestVersion { value: 3 })
        );

        let all_skipped = encode_frozen_legacy_manifest_v1(
            None,
            &[
                (10, FrozenLegacyDispositionV1::Skipped),
                (11, FrozenLegacyDispositionV1::Skipped),
            ],
        );
        let produced = encode_frozen_legacy_manifest_v1(
            Some((9, [0x22; 32])),
            &[
                (10, FrozenLegacyDispositionV1::Produced([0x33; 32])),
                (11, FrozenLegacyDispositionV1::Skipped),
            ],
        );
        for frozen_legacy_v1 in [&all_skipped, &produced] {
            assert_eq!(
                &frozen_legacy_v1[..2],
                &FINALITY_MANIFEST_VERSION_V2.to_be_bytes()
            );
            assert!(FinalityManifestV1::decode(frozen_legacy_v1).is_err());
        }

        assert!(matches!(
            FinalityManifestV1::decode(&[0]),
            Err(ProtocolError::Truncated {
                context: "FinalityManifestV1.manifest_version",
                ..
            })
        ));
    }

    #[test]
    fn validation_ranges_require_exact_nested_endpoints() {
        assert!(matches!(
            one_slot_manifest(
                SlotRangeV1::new(9, 11).unwrap(),
                SlotRangeV1::new(10, 11).unwrap(),
            ),
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.finality_validation_slots.first_slot",
                ..
            })
        ));
        assert!(matches!(
            one_slot_manifest(
                SlotRangeV1::new(10, 11).unwrap(),
                SlotRangeV1::new(9, 11).unwrap(),
            ),
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.validation_slots.first_slot",
                ..
            })
        ));
        assert!(
            one_slot_manifest(
                SlotRangeV1::new(10, 11).unwrap(),
                SlotRangeV1::new(10, 12).unwrap(),
            )
            .is_ok()
        );

        let published_wider_than_finality = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 12).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 12).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            None,
            vec![FinalizedSlotV1::skipped(10), FinalizedSlotV1::skipped(11)],
        );
        assert!(matches!(
            published_wider_than_finality,
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.finality_validation_slots.next_slot",
                ..
            })
        ));

        assert!(matches!(
            one_slot_manifest(
                SlotRangeV1::new(10, 13).unwrap(),
                SlotRangeV1::new(10, 12).unwrap(),
            ),
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.validation_slots.next_slot",
                ..
            })
        ));

        assert_eq!(
            one_slot_manifest(
                SlotRangeV1::new(10, 11).unwrap(),
                SlotRangeV1::new(10, 10 + MAX_FINALITY_ENTRIES as u64 + 1).unwrap(),
            ),
            Err(ProtocolError::CountOutOfBounds {
                field: "FinalityManifestV1.validation_slots",
                max: MAX_FINALITY_ENTRIES,
                actual: (MAX_FINALITY_ENTRIES + 1) as u64,
            })
        );

        let mut invalid_endpoint = golden_manifest().encode();
        invalid_endpoint[66..74].copy_from_slice(&10_u64.to_be_bytes());
        assert!(matches!(
            FinalityManifestV1::decode(&invalid_endpoint),
            Err(ProtocolError::InvalidField {
                field: "SlotRangeV1",
                ..
            })
        ));
    }

    #[test]
    fn disposition_tags_and_payload_shapes_are_exact() {
        let legacy_identity = identity(0xab, None);
        assert_eq!(legacy_identity.encode().len(), 33);
        assert_eq!(
            FinalizedBlockIdentityV1::decode(&legacy_identity.encode()),
            Ok(legacy_identity)
        );

        let modern_identity = identity(0xab, Some(0xcd));
        assert_eq!(modern_identity.encode().len(), 65);
        assert_eq!(modern_identity.final_poh_hash(), [0xab; 32]);
        assert_eq!(modern_identity.consensus_block_id(), Some([0xcd; 32]));
        assert_eq!(
            FinalizedBlockIdentityV1::decode(&modern_identity.encode()),
            Ok(modern_identity)
        );

        let produced = FinalizedSlotV1::produced(5, modern_identity);
        assert_eq!(produced.encode().len(), 74);
        assert_eq!(FinalizedSlotV1::decode(&produced.encode()), Ok(produced));
        assert_eq!(FinalizedSlotV1::skipped(5).encode().len(), 9);
        assert_eq!(FinalizedSlotV1::unresolved(5).encode().len(), 9);

        let parent = FinalizedParentAnchorV1::new(4, modern_identity);
        assert_eq!(parent.encode().len(), 73);
        assert_eq!(parent.identity(), modern_identity);
        assert_eq!(parent.final_poh_hash(), [0xab; 32]);
        assert_eq!(parent.consensus_block_id(), Some([0xcd; 32]));
        assert_eq!(
            FinalizedParentAnchorV1::decode(&parent.encode()),
            Ok(parent)
        );

        let mut invalid_identity_option = legacy_identity.encode();
        invalid_identity_option[32] = 2;
        assert_eq!(
            FinalizedBlockIdentityV1::decode(&invalid_identity_option),
            Err(ProtocolError::InvalidOptionTag {
                field: "FinalizedBlockIdentityV1.consensus_block_id",
                value: 2,
            })
        );

        let mut unknown = FinalizedSlotV1::skipped(5).encode();
        unknown[8] = 4;
        assert!(matches!(
            FinalizedSlotV1::decode(&unknown),
            Err(ProtocolError::UnknownEnum { .. })
        ));
    }

    #[test]
    fn finality_entries_require_dense_full_ordered_coverage() {
        let result = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 13).unwrap(),
            SlotRangeV1::new(10, 13).unwrap(),
            SlotRangeV1::new(10, 13).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            None,
            vec![
                FinalizedSlotV1::skipped(10),
                FinalizedSlotV1::skipped(12),
                FinalizedSlotV1::skipped(11),
            ],
        );
        assert!(matches!(
            result,
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        let mut encoded = golden_manifest().encode();
        let entries_len = 74 + 9 + 9;
        let count_offset = encoded.len() - entries_len - 4;
        encoded[count_offset..count_offset + 4]
            .copy_from_slice(&((MAX_FINALITY_ENTRIES as u32) + 1).to_be_bytes());
        assert_eq!(
            FinalityManifestV1::decode(&encoded),
            Err(ProtocolError::CountOutOfBounds {
                field: "FinalityManifestV1.entries",
                max: MAX_FINALITY_ENTRIES,
                actual: (MAX_FINALITY_ENTRIES + 1) as u64,
            })
        );
    }

    #[test]
    fn evidence_vectors_and_parent_anchor_are_canonical() {
        let unsorted_streams = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            descriptor(b"authority-v1"),
            vec![stream_input(2), stream_input(1)],
            Vec::new(),
            None,
            vec![FinalizedSlotV1::skipped(10)],
        );
        assert!(matches!(
            unsorted_streams,
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        let unsorted_objects = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            vec![
                InputObjectV1::new(
                    OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1.to_vec(),
                    descriptor(b"car-zst-v1"),
                    object(b"zst", 1, 1),
                )
                .unwrap(),
                InputObjectV1::new(
                    OLD_FAITHFUL_CAR_LOGICAL_NAME_V1.to_vec(),
                    descriptor(b"car-v1"),
                    object(b"car", 1, 2),
                )
                .unwrap(),
            ],
            None,
            vec![FinalizedSlotV1::skipped(10)],
        );
        assert!(matches!(
            unsorted_objects,
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));

        let in_range_anchor = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            Some(FinalizedParentAnchorV1::new(10, identity(9, None))),
            vec![FinalizedSlotV1::skipped(10)],
        );
        assert!(matches!(
            in_range_anchor,
            Err(ProtocolError::InvalidField { .. })
        ));

        let missing_required_anchor = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            None,
            vec![FinalizedSlotV1::produced(10, identity(9, None))],
        );
        assert!(matches!(
            missing_required_anchor,
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.predecessor_parent",
                ..
            })
        ));

        let unneeded_anchor = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            SlotRangeV1::new(10, 11).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            Vec::new(),
            Some(FinalizedParentAnchorV1::new(9, identity(8, None))),
            vec![FinalizedSlotV1::skipped(10)],
        );
        assert!(matches!(
            unneeded_anchor,
            Err(ProtocolError::InvalidField {
                field: "FinalityManifestV1.predecessor_parent",
                ..
            })
        ));
    }

    #[test]
    fn manifest_binds_exactly_to_job_and_object_reference() {
        let input = InputObjectV1::new(
            OLD_FAITHFUL_CAR_LOGICAL_NAME_V1.to_vec(),
            descriptor(b"car-v1"),
            object(b"input.car", 20, 5),
        )
        .unwrap();
        let manifest = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 12).unwrap(),
            SlotRangeV1::new(10, 12).unwrap(),
            SlotRangeV1::new(10, 12).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            vec![input.clone()],
            Some(FinalizedParentAnchorV1::new(9, identity(6, None))),
            vec![
                FinalizedSlotV1::produced(10, identity(7, None)),
                FinalizedSlotV1::skipped(11),
            ],
        )
        .unwrap();
        let finality_ref = ObjectRefV1::new(
            b"finality/7".to_vec(),
            None,
            manifest.encode().len() as u64,
            ordinary_sha256(&manifest.encode()),
        )
        .unwrap();
        let job = CompactionJobSpecV1::new(
            [2; 16],
            [1; 32],
            7,
            SlotRangeV1::new(10, 12).unwrap(),
            Vec::new(),
            vec![input],
            None,
            finality_ref,
            descriptor(b"selection-v1"),
            descriptor(b"normalization-v1"),
            descriptor(b"archive-v2"),
            descriptor(b"epoch-schedule-v1"),
            None,
            0,
            b"archive/".to_vec(),
        )
        .unwrap();
        manifest.validate_against_job(&job).unwrap();

        let undeclared = FinalityManifestV1::new(
            [1; 32],
            7,
            SlotRangeV1::new(10, 12).unwrap(),
            SlotRangeV1::new(10, 12).unwrap(),
            SlotRangeV1::new(10, 12).unwrap(),
            descriptor(b"authority-v1"),
            Vec::new(),
            vec![
                InputObjectV1::new(
                    OLD_FAITHFUL_CAR_LOGICAL_NAME_V1.to_vec(),
                    descriptor(b"car-v1"),
                    object(b"other.car", 20, 6),
                )
                .unwrap(),
            ],
            Some(FinalizedParentAnchorV1::new(9, identity(6, None))),
            vec![
                FinalizedSlotV1::produced(10, identity(7, None)),
                FinalizedSlotV1::skipped(11),
            ],
        )
        .unwrap();
        assert!(matches!(
            undeclared.validate_against_job(&job),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

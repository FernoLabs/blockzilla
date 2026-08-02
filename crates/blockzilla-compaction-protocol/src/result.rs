use crate::codec::{Reader, put_option, put_u8, put_u32, put_u64};
use crate::types::{MAX_PUBLICATION_OBJECTS, validate_named_objects};
use crate::{LeaseFenceV1, NamedObjectRefV1, ObjectRefV1, ProtocolError, Result, SlotRangeV1};

/// Complete immutable candidate declaration produced below one fenced prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateManifestV1 {
    job_id: [u8; 16],
    job_spec_hash: [u8; 32],
    fence: LeaseFenceV1,
    epoch: u64,
    slots: SlotRangeV1,
    finality_manifest: ObjectRefV1,
    produced_count: u32,
    skipped_count: u32,
    objects: Vec<NamedObjectRefV1>,
}

impl CandidateManifestV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: [u8; 16],
        job_spec_hash: [u8; 32],
        fence: LeaseFenceV1,
        epoch: u64,
        slots: SlotRangeV1,
        finality_manifest: ObjectRefV1,
        produced_count: u32,
        skipped_count: u32,
        objects: Vec<NamedObjectRefV1>,
    ) -> Result<Self> {
        validate_coverage_counts(slots, produced_count, skipped_count)?;
        validate_named_objects(&objects, 1, "CandidateManifestV1.objects")?;
        Ok(Self {
            job_id,
            job_spec_hash,
            fence,
            epoch,
            slots,
            finality_manifest,
            produced_count,
            skipped_count,
            objects,
        })
    }

    #[must_use]
    pub const fn job_id(&self) -> [u8; 16] {
        self.job_id
    }

    #[must_use]
    pub const fn job_spec_hash(&self) -> [u8; 32] {
        self.job_spec_hash
    }

    #[must_use]
    pub const fn fence(&self) -> LeaseFenceV1 {
        self.fence
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
    pub const fn finality_manifest(&self) -> &ObjectRefV1 {
        &self.finality_manifest
    }

    #[must_use]
    pub const fn produced_count(&self) -> u32 {
        self.produced_count
    }

    #[must_use]
    pub const fn skipped_count(&self) -> u32 {
        self.skipped_count
    }

    #[must_use]
    pub fn objects(&self) -> &[NamedObjectRefV1] {
        &self.objects
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated candidate manifest always fits u32 counts");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("CandidateManifestV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(&self.job_id);
        output.extend_from_slice(&self.job_spec_hash);
        put_u64(output, self.fence.get());
        put_u64(output, self.epoch);
        self.slots.encode_into(output);
        self.finality_manifest.encode_into(output)?;
        output.extend_from_slice(&self.produced_count.to_be_bytes());
        output.extend_from_slice(&self.skipped_count.to_be_bytes());
        put_u32(output, self.objects.len(), "CandidateManifestV1.objects")?;
        for object in &self.objects {
            object.encode_into(output)?;
        }
        Ok(())
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let job_id = reader.array("CandidateManifestV1.job_id")?;
        let job_spec_hash = reader.array("CandidateManifestV1.job_spec_hash")?;
        let fence = LeaseFenceV1::new(reader.u64("CandidateManifestV1.fence")?);
        let epoch = reader.u64("CandidateManifestV1.epoch")?;
        let slots = SlotRangeV1::decode_from(reader)?;
        let finality_manifest = ObjectRefV1::decode_from(reader)?;
        let produced_count = reader.u32("CandidateManifestV1.produced_count")?;
        let skipped_count = reader.u32("CandidateManifestV1.skipped_count")?;
        let object_count = reader.count(MAX_PUBLICATION_OBJECTS, "CandidateManifestV1.objects")?;
        let mut objects = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            objects.push(NamedObjectRefV1::decode_from(reader)?);
        }
        Self::new(
            job_id,
            job_spec_hash,
            fence,
            epoch,
            slots,
            finality_manifest,
            produced_count,
            skipped_count,
            objects,
        )
    }
}

/// Only the two integrity-relevant V1 outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompactionOutcomeV1 {
    Complete = 1,
    NotComplete = 2,
}

impl TryFrom<u8> for CompactionOutcomeV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Complete),
            2 => Ok(Self::NotComplete),
            value => Err(ProtocolError::UnknownEnum {
                field: "CompactionResultV1.outcome",
                value,
            }),
        }
    }
}

/// Minimal worker result envelope. Diagnostics are intentionally out of band.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionResultV1 {
    job_id: [u8; 16],
    job_spec_hash: [u8; 32],
    fence: LeaseFenceV1,
    outcome: CompactionOutcomeV1,
    candidate_manifest: Option<ObjectRefV1>,
}

impl CompactionResultV1 {
    pub fn new(
        job_id: [u8; 16],
        job_spec_hash: [u8; 32],
        fence: LeaseFenceV1,
        outcome: CompactionOutcomeV1,
        candidate_manifest: Option<ObjectRefV1>,
    ) -> Result<Self> {
        if candidate_manifest.is_some() != (outcome == CompactionOutcomeV1::Complete) {
            return Err(ProtocolError::InvalidField {
                field: "CompactionResultV1.candidate_manifest",
                reason: "must be present exactly for COMPLETE",
            });
        }
        Ok(Self {
            job_id,
            job_spec_hash,
            fence,
            outcome,
            candidate_manifest,
        })
    }

    pub fn complete(
        job_id: [u8; 16],
        job_spec_hash: [u8; 32],
        fence: LeaseFenceV1,
        candidate_manifest: ObjectRefV1,
    ) -> Self {
        Self::new(
            job_id,
            job_spec_hash,
            fence,
            CompactionOutcomeV1::Complete,
            Some(candidate_manifest),
        )
        .expect("COMPLETE carries a candidate manifest")
    }

    #[must_use]
    pub fn not_complete(job_id: [u8; 16], job_spec_hash: [u8; 32], fence: LeaseFenceV1) -> Self {
        Self::new(
            job_id,
            job_spec_hash,
            fence,
            CompactionOutcomeV1::NotComplete,
            None,
        )
        .expect("NOT_COMPLETE omits a candidate manifest")
    }

    #[must_use]
    pub const fn job_id(&self) -> [u8; 16] {
        self.job_id
    }

    #[must_use]
    pub const fn job_spec_hash(&self) -> [u8; 32] {
        self.job_spec_hash
    }

    #[must_use]
    pub const fn fence(&self) -> LeaseFenceV1 {
        self.fence
    }

    #[must_use]
    pub const fn outcome(&self) -> CompactionOutcomeV1 {
        self.outcome
    }

    #[must_use]
    pub const fn candidate_manifest(&self) -> Option<&ObjectRefV1> {
        self.candidate_manifest.as_ref()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&self.job_id);
        output.extend_from_slice(&self.job_spec_hash);
        put_u64(&mut output, self.fence.get());
        put_u8(&mut output, self.outcome as u8);
        put_option(
            &mut output,
            self.candidate_manifest.as_ref(),
            |output, object| object.encode_into(output),
        )
        .expect("validated object references always fit u32 lengths");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let job_id = reader.array("CompactionResultV1.job_id")?;
        let job_spec_hash = reader.array("CompactionResultV1.job_spec_hash")?;
        let fence = LeaseFenceV1::new(reader.u64("CompactionResultV1.fence")?);
        let outcome = CompactionOutcomeV1::try_from(reader.u8("CompactionResultV1.outcome")?)?;
        let candidate_manifest = reader.option(
            "CompactionResultV1.candidate_manifest",
            ObjectRefV1::decode_from,
        )?;
        reader.finish("CompactionResultV1")?;
        Self::new(job_id, job_spec_hash, fence, outcome, candidate_manifest)
    }
}

fn validate_coverage_counts(
    slots: SlotRangeV1,
    produced_count: u32,
    skipped_count: u32,
) -> Result<()> {
    if u64::from(produced_count) + u64::from(skipped_count) != slots.len() {
        return Err(ProtocolError::InvalidField {
            field: "CandidateManifestV1.coverage_counts",
            reason: "produced_count plus skipped_count must cover the slot range exactly",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ordinary_sha256;

    fn object(key: &[u8], digest: u8) -> ObjectRefV1 {
        ObjectRefV1::new(key.to_vec(), None, 55, [digest; 32]).unwrap()
    }

    #[test]
    fn minimal_result_envelopes_are_golden() {
        let incomplete = CompactionResultV1::not_complete([1; 16], [2; 32], LeaseFenceV1::new(7));
        assert_eq!(
            to_hex(&incomplete.encode()),
            concat!(
                "01010101010101010101010101010101",
                "0202020202020202020202020202020202020202020202020202020202020202",
                "0000000000000007",
                "02",
                "00"
            )
        );
        assert_eq!(
            CompactionResultV1::decode(&incomplete.encode()),
            Ok(incomplete)
        );
    }

    #[test]
    fn candidate_manifest_and_complete_result_have_pinned_digests() {
        let candidate = CandidateManifestV1::new(
            [1; 16],
            [2; 32],
            LeaseFenceV1::new(7),
            9,
            SlotRangeV1::new(100, 102).unwrap(),
            object(b"finality", 3),
            1,
            1,
            vec![NamedObjectRefV1::new(b"blocks".to_vec(), object(b"objects/blocks", 4)).unwrap()],
        )
        .unwrap();
        let candidate_bytes = candidate.encode();
        assert_eq!(candidate_bytes.len(), 214);
        assert_eq!(
            to_hex(&ordinary_sha256(&candidate_bytes)),
            "1888d37a1381429a0306f340e5fd14f3044d81daad98dc8d3158d7ee8b925845"
        );
        assert_eq!(CandidateManifestV1::decode(&candidate_bytes), Ok(candidate));

        let complete = CompactionResultV1::complete(
            [1; 16],
            [2; 32],
            LeaseFenceV1::new(7),
            object(b"candidate", 5),
        );
        let complete_bytes = complete.encode();
        assert_eq!(complete_bytes.len(), 112);
        assert_eq!(
            to_hex(&ordinary_sha256(&complete_bytes)),
            "a54c5603c12cc1bdd72bb7a041ba1a653427eb465eaa565ccef6cfeae4774049"
        );
        assert_eq!(CompactionResultV1::decode(&complete_bytes), Ok(complete));
    }

    #[test]
    fn complete_shape_and_counts_fail_closed() {
        assert!(matches!(
            CompactionResultV1::new(
                [0; 16],
                [0; 32],
                LeaseFenceV1::new(0),
                CompactionOutcomeV1::Complete,
                None,
            ),
            Err(ProtocolError::InvalidField { .. })
        ));

        assert!(matches!(
            CandidateManifestV1::new(
                [0; 16],
                [0; 32],
                LeaseFenceV1::new(0),
                0,
                SlotRangeV1::new(1, 4).unwrap(),
                object(b"finality", 1),
                1,
                1,
                vec![NamedObjectRefV1::new(b"blocks".to_vec(), object(b"blocks", 2)).unwrap()],
            ),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    #[test]
    fn unsorted_or_empty_object_set_is_rejected() {
        let objects = vec![
            NamedObjectRefV1::new(b"z".to_vec(), object(b"z", 1)).unwrap(),
            NamedObjectRefV1::new(b"a".to_vec(), object(b"a", 2)).unwrap(),
        ];
        assert!(matches!(
            CandidateManifestV1::new(
                [0; 16],
                [0; 32],
                LeaseFenceV1::new(0),
                0,
                SlotRangeV1::new(1, 3).unwrap(),
                object(b"finality", 3),
                2,
                0,
                objects,
            ),
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

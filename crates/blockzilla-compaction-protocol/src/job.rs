use hivezilla_protocol::{
    CURSOR_V1_ENCODED_LEN, CursorV1, STREAM_HEADER_V1_ENCODED_LEN, StreamHeaderV1,
};

use crate::codec::{Reader, put_bytes, put_option, put_u32, put_u64, validate_len};
use crate::types::{
    MAX_LOGICAL_NAME_BYTES, MAX_OUTPUT_NAMESPACE_BYTES, MAX_REQUIRED_INPUTS, ordinary_sha256,
    validate_logical_name, validate_unique_object_keys,
};
use crate::{HashedDescriptorV1, ObjectRefV1, ProtocolError, Result, SlotRangeV1};

pub const JOB_SPEC_OBJECT_DOMAIN: &[u8] = b"blockzilla/v1/compaction-job-spec";

pub const OLD_FAITHFUL_CAR_LOGICAL_NAME_V1: &[u8] = b"old-faithful.car";
pub const OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1: &[u8] = b"old-faithful.car-zst";
pub const SHRED_TRUST_CONTEXT_LOGICAL_NAME_V1: &[u8] = b"solana.shred-trust-context";

/// Inclusive/exclusive exact prefix anchors for one required stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputStreamRangeV1 {
    stream: StreamHeaderV1,
    start: CursorV1,
    end: CursorV1,
}

impl InputStreamRangeV1 {
    pub fn new(stream: StreamHeaderV1, start: CursorV1, end: CursorV1) -> Result<Self> {
        if start.next_sequence() >= end.next_sequence() {
            return Err(ProtocolError::InvalidField {
                field: "InputStreamRangeV1",
                reason: "start must precede end",
            });
        }
        if !matches!(stream.payload_format(), 1..=5 | 7) {
            return Err(ProtocolError::InvalidField {
                field: "InputStreamRangeV1.stream.payload_format",
                reason: "V1 compaction accepts only formats 1 through 5 or 7",
            });
        }
        Ok(Self { stream, start, end })
    }

    #[must_use]
    pub const fn stream(&self) -> StreamHeaderV1 {
        self.stream
    }

    #[must_use]
    pub const fn start(&self) -> CursorV1 {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> CursorV1 {
        self.end
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output =
            Vec::with_capacity(STREAM_HEADER_V1_ENCODED_LEN + 2 * CURSOR_V1_ENCODED_LEN);
        self.encode_into(&mut output);
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("InputStreamRangeV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.stream.fixed_encode());
        output.extend_from_slice(&self.start.fixed_encode());
        output.extend_from_slice(&self.end.fixed_encode());
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let stream = StreamHeaderV1::decode(
            reader.take(STREAM_HEADER_V1_ENCODED_LEN, "InputStreamRangeV1.stream")?,
        )?;
        let start =
            CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "InputStreamRangeV1.start")?)?;
        let end = CursorV1::decode(reader.take(CURSOR_V1_ENCODED_LEN, "InputStreamRangeV1.end")?)?;
        Self::new(stream, start, end)
    }
}

/// Registered finite input object, exact decoder contract, and byte identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputObjectV1 {
    logical_name: Vec<u8>,
    format: HashedDescriptorV1,
    object: ObjectRefV1,
}

impl InputObjectV1 {
    pub fn new(
        logical_name: Vec<u8>,
        format: HashedDescriptorV1,
        object: ObjectRefV1,
    ) -> Result<Self> {
        validate_logical_name(&logical_name, "InputObjectV1.logical_name")?;
        Ok(Self {
            logical_name,
            format,
            object,
        })
    }

    #[must_use]
    pub fn logical_name(&self) -> &[u8] {
        &self.logical_name
    }

    #[must_use]
    pub const fn format(&self) -> &HashedDescriptorV1 {
        &self.format
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectRefV1 {
        &self.object
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated InputObjectV1 always fits u32 lengths");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("InputObjectV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        put_bytes(output, &self.logical_name, "InputObjectV1.logical_name")?;
        self.format.encode_into(output)?;
        self.object.encode_into(output)
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let logical_name = reader.bytes(1, MAX_LOGICAL_NAME_BYTES, "InputObjectV1.logical_name")?;
        let format = HashedDescriptorV1::decode_from(reader)?;
        let object = ObjectRefV1::decode_from(reader)?;
        Self::new(logical_name, format, object)
    }
}

/// Durable monotonic attempt generation used as the sole V1 fence value.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct LeaseFenceV1(u64);

impl LeaseFenceV1 {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ProtocolError::IntegerOverflow {
                field: "LeaseFenceV1",
            })
    }

    #[must_use]
    pub const fn encode(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let value: [u8; 8] = encoded
            .try_into()
            .map_err(|_| ProtocolError::LengthOutOfBounds {
                field: "LeaseFenceV1",
                min: 8,
                max: 8,
                actual: encoded.len(),
            })?;
        Ok(Self::new(u64::from_be_bytes(value)))
    }
}

/// Immutable fields whose domain-prefixed canonical bytes define a job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionJobSpecV1 {
    job_id: [u8; 16],
    cluster_genesis_hash: [u8; 32],
    epoch: u64,
    slots: SlotRangeV1,
    required_stream_inputs: Vec<InputStreamRangeV1>,
    required_object_inputs: Vec<InputObjectV1>,
    shred_trust_context: Option<InputObjectV1>,
    finality_manifest: ObjectRefV1,
    selection_policy: HashedDescriptorV1,
    normalization_algorithm: HashedDescriptorV1,
    archive_format: HashedDescriptorV1,
    epoch_schedule: HashedDescriptorV1,
    expected_catalog_predecessor: Option<[u8; 32]>,
    expected_catalog_generation: u64,
    output_namespace: Vec<u8>,
}

impl CompactionJobSpecV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: [u8; 16],
        cluster_genesis_hash: [u8; 32],
        epoch: u64,
        slots: SlotRangeV1,
        required_stream_inputs: Vec<InputStreamRangeV1>,
        required_object_inputs: Vec<InputObjectV1>,
        shred_trust_context: Option<InputObjectV1>,
        finality_manifest: ObjectRefV1,
        selection_policy: HashedDescriptorV1,
        normalization_algorithm: HashedDescriptorV1,
        archive_format: HashedDescriptorV1,
        epoch_schedule: HashedDescriptorV1,
        expected_catalog_predecessor: Option<[u8; 32]>,
        expected_catalog_generation: u64,
        output_namespace: Vec<u8>,
    ) -> Result<Self> {
        validate_required_streams(&required_stream_inputs, cluster_genesis_hash)?;
        validate_required_objects(&required_object_inputs)?;
        if required_stream_inputs.len() + required_object_inputs.len() == 0 {
            return Err(ProtocolError::InvalidField {
                field: "CompactionJobSpecV1.required_inputs",
                reason: "at least one stream or finite object is required",
            });
        }
        let has_known_shred_stream = required_stream_inputs
            .iter()
            .any(|input| matches!(input.stream.payload_format(), 2 | 3));
        validate_shred_trust_context(shred_trust_context.as_ref(), has_known_shred_stream)?;
        validate_unique_object_keys(
            required_object_inputs
                .iter()
                .map(InputObjectV1::object)
                .chain(shred_trust_context.iter().map(InputObjectV1::object))
                .chain(std::iter::once(&finality_manifest)),
            "CompactionJobSpecV1.object_references",
        )?;
        validate_catalog_predecessor(expected_catalog_generation, expected_catalog_predecessor)?;
        validate_output_namespace(&output_namespace)?;
        Ok(Self {
            job_id,
            cluster_genesis_hash,
            epoch,
            slots,
            required_stream_inputs,
            required_object_inputs,
            shred_trust_context,
            finality_manifest,
            selection_policy,
            normalization_algorithm,
            archive_format,
            epoch_schedule,
            expected_catalog_predecessor,
            expected_catalog_generation,
            output_namespace,
        })
    }

    #[must_use]
    pub const fn job_id(&self) -> [u8; 16] {
        self.job_id
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
    pub fn required_stream_inputs(&self) -> &[InputStreamRangeV1] {
        &self.required_stream_inputs
    }

    #[must_use]
    pub fn required_object_inputs(&self) -> &[InputObjectV1] {
        &self.required_object_inputs
    }

    #[must_use]
    pub const fn shred_trust_context(&self) -> Option<&InputObjectV1> {
        self.shred_trust_context.as_ref()
    }

    #[must_use]
    pub const fn finality_manifest(&self) -> &ObjectRefV1 {
        &self.finality_manifest
    }

    #[must_use]
    pub const fn selection_policy(&self) -> &HashedDescriptorV1 {
        &self.selection_policy
    }

    #[must_use]
    pub const fn normalization_algorithm(&self) -> &HashedDescriptorV1 {
        &self.normalization_algorithm
    }

    #[must_use]
    pub const fn archive_format(&self) -> &HashedDescriptorV1 {
        &self.archive_format
    }

    #[must_use]
    pub const fn epoch_schedule(&self) -> &HashedDescriptorV1 {
        &self.epoch_schedule
    }

    #[must_use]
    pub const fn expected_catalog_predecessor(&self) -> Option<[u8; 32]> {
        self.expected_catalog_predecessor
    }

    #[must_use]
    pub const fn expected_catalog_generation(&self) -> u64 {
        self.expected_catalog_generation
    }

    #[must_use]
    pub fn output_namespace(&self) -> &[u8] {
        &self.output_namespace
    }

    /// Exact stored `JobSpecObjectV1` bytes, including the domain prefix.
    #[must_use]
    pub fn job_spec_object_bytes(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(JOB_SPEC_OBJECT_DOMAIN);
        self.encode_fields(&mut output)
            .expect("validated job fields always fit u32 lengths");
        output
    }

    /// Ordinary SHA-256 of the exact stored job-spec object bytes.
    #[must_use]
    pub fn job_spec_hash(&self) -> [u8; 32] {
        ordinary_sha256(&self.job_spec_object_bytes())
    }

    /// Verifies that a locator names these exact stored `JobSpecObjectV1` bytes.
    pub fn verify_job_spec_object_ref(&self, object: &ObjectRefV1) -> Result<()> {
        object.verify_bytes(&self.job_spec_object_bytes())
    }

    /// Enforces the exact trust-context `if and only if` after descriptor interpretation.
    ///
    /// This dependency-light layer recognizes known shred stream formats 2 and
    /// 3, but cannot infer whether an opaque finite-input descriptor contributes
    /// shred-derived bytes. The descriptor registry must determine
    /// `inputs_can_contribute_shreds` and call this before accepting the job.
    pub fn validate_shred_trust_requirement(
        &self,
        inputs_can_contribute_shreds: bool,
    ) -> Result<()> {
        if self.shred_trust_context.is_some() != inputs_can_contribute_shreds {
            return Err(ProtocolError::InvalidField {
                field: "CompactionJobSpecV1.shred_trust_context",
                reason: "must be present if and only if interpreted inputs can contribute shred-derived bytes",
            });
        }
        Ok(())
    }

    pub fn decode_job_spec_object(encoded: &[u8]) -> Result<Self> {
        let fields =
            encoded
                .strip_prefix(JOB_SPEC_OBJECT_DOMAIN)
                .ok_or(ProtocolError::InvalidDomain {
                    context: "JobSpecObjectV1",
                })?;
        let mut reader = Reader::new(fields);
        let value = Self::decode_fields(&mut reader)?;
        reader.finish("JobSpecObjectV1")?;
        Ok(value)
    }

    pub(crate) fn encode_fields(&self, output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(&self.job_id);
        output.extend_from_slice(&self.cluster_genesis_hash);
        put_u64(output, self.epoch);
        self.slots.encode_into(output);
        put_u32(
            output,
            self.required_stream_inputs.len(),
            "CompactionJobSpecV1.required_stream_inputs",
        )?;
        for input in &self.required_stream_inputs {
            input.encode_into(output);
        }
        put_u32(
            output,
            self.required_object_inputs.len(),
            "CompactionJobSpecV1.required_object_inputs",
        )?;
        for input in &self.required_object_inputs {
            input.encode_into(output)?;
        }
        put_option(
            output,
            self.shred_trust_context.as_ref(),
            |output, input| input.encode_into(output),
        )?;
        self.finality_manifest.encode_into(output)?;
        self.selection_policy.encode_into(output)?;
        self.normalization_algorithm.encode_into(output)?;
        self.archive_format.encode_into(output)?;
        self.epoch_schedule.encode_into(output)?;
        put_option(
            output,
            self.expected_catalog_predecessor.as_ref(),
            |output, digest| {
                output.extend_from_slice(digest);
                Ok(())
            },
        )?;
        put_u64(output, self.expected_catalog_generation);
        put_bytes(
            output,
            &self.output_namespace,
            "CompactionJobSpecV1.output_namespace",
        )
    }

    pub(crate) fn decode_fields(reader: &mut Reader<'_>) -> Result<Self> {
        let job_id = reader.array("CompactionJobSpecV1.job_id")?;
        let cluster_genesis_hash = reader.array("CompactionJobSpecV1.cluster_genesis_hash")?;
        let epoch = reader.u64("CompactionJobSpecV1.epoch")?;
        let slots = SlotRangeV1::decode_from(reader)?;
        let stream_count = reader.count(
            MAX_REQUIRED_INPUTS,
            "CompactionJobSpecV1.required_stream_inputs",
        )?;
        let mut required_stream_inputs = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            required_stream_inputs.push(InputStreamRangeV1::decode_from(reader)?);
        }
        let object_count = reader.count(
            MAX_REQUIRED_INPUTS,
            "CompactionJobSpecV1.required_object_inputs",
        )?;
        let mut required_object_inputs = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            required_object_inputs.push(InputObjectV1::decode_from(reader)?);
        }
        let shred_trust_context = reader.option(
            "CompactionJobSpecV1.shred_trust_context",
            InputObjectV1::decode_from,
        )?;
        let finality_manifest = ObjectRefV1::decode_from(reader)?;
        let selection_policy = HashedDescriptorV1::decode_from(reader)?;
        let normalization_algorithm = HashedDescriptorV1::decode_from(reader)?;
        let archive_format = HashedDescriptorV1::decode_from(reader)?;
        let epoch_schedule = HashedDescriptorV1::decode_from(reader)?;
        let expected_catalog_predecessor = reader.option(
            "CompactionJobSpecV1.expected_catalog_predecessor",
            |reader| reader.array("CompactionJobSpecV1.expected_catalog_predecessor"),
        )?;
        let expected_catalog_generation =
            reader.u64("CompactionJobSpecV1.expected_catalog_generation")?;
        let output_namespace = reader.bytes(
            1,
            MAX_OUTPUT_NAMESPACE_BYTES,
            "CompactionJobSpecV1.output_namespace",
        )?;
        Self::new(
            job_id,
            cluster_genesis_hash,
            epoch,
            slots,
            required_stream_inputs,
            required_object_inputs,
            shred_trust_context,
            finality_manifest,
            selection_policy,
            normalization_algorithm,
            archive_format,
            epoch_schedule,
            expected_catalog_predecessor,
            expected_catalog_generation,
            output_namespace,
        )
    }
}

/// The immutable job plus the currently granted, monotonically fenced attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionJobV1 {
    spec: CompactionJobSpecV1,
    fence: LeaseFenceV1,
}

impl CompactionJobV1 {
    #[must_use]
    pub const fn new(spec: CompactionJobSpecV1, fence: LeaseFenceV1) -> Self {
        Self { spec, fence }
    }

    #[must_use]
    pub const fn spec(&self) -> &CompactionJobSpecV1 {
        &self.spec
    }

    #[must_use]
    pub const fn fence(&self) -> LeaseFenceV1 {
        self.fence
    }

    /// Canonical encoding of all `CompactionJobV1` fields, with `fence` last.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.spec
            .encode_fields(&mut output)
            .expect("validated job fields always fit u32 lengths");
        put_u64(&mut output, self.fence.get());
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let spec = CompactionJobSpecV1::decode_fields(&mut reader)?;
        let fence = LeaseFenceV1::new(reader.u64("CompactionJobV1.fence")?);
        reader.finish("CompactionJobV1")?;
        Ok(Self::new(spec, fence))
    }

    /// Exact fenced worker output prefix from the V1 specification.
    #[must_use]
    pub fn attempt_prefix(&self) -> Vec<u8> {
        let mut prefix = self.spec.output_namespace.clone();
        prefix.extend_from_slice(b"jobs/");
        append_lower_hex(&mut prefix, &self.spec.job_id);
        prefix.push(b'/');
        prefix.extend_from_slice(format!("{:016x}", self.fence.get()).as_bytes());
        prefix.push(b'/');
        prefix
    }
}

fn validate_required_streams(
    inputs: &[InputStreamRangeV1],
    cluster_genesis_hash: [u8; 32],
) -> Result<()> {
    if inputs.len() > MAX_REQUIRED_INPUTS {
        return Err(ProtocolError::CountOutOfBounds {
            field: "CompactionJobSpecV1.required_stream_inputs",
            max: MAX_REQUIRED_INPUTS,
            actual: inputs.len() as u64,
        });
    }
    if inputs
        .windows(2)
        .any(|pair| pair[0].stream.stream_id() >= pair[1].stream.stream_id())
    {
        return Err(ProtocolError::NonCanonicalOrder {
            field: "CompactionJobSpecV1.required_stream_inputs",
        });
    }
    if inputs
        .iter()
        .any(|input| input.stream.cluster_genesis_hash().as_bytes() != &cluster_genesis_hash)
    {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.required_stream_inputs",
            reason: "every stream must use the job cluster genesis hash",
        });
    }
    Ok(())
}

fn validate_required_objects(inputs: &[InputObjectV1]) -> Result<()> {
    if inputs.len() > MAX_REQUIRED_INPUTS {
        return Err(ProtocolError::CountOutOfBounds {
            field: "CompactionJobSpecV1.required_object_inputs",
            max: MAX_REQUIRED_INPUTS,
            actual: inputs.len() as u64,
        });
    }
    if inputs
        .windows(2)
        .any(|pair| pair[0].logical_name >= pair[1].logical_name)
    {
        return Err(ProtocolError::NonCanonicalOrder {
            field: "CompactionJobSpecV1.required_object_inputs",
        });
    }
    if inputs.iter().any(|input| {
        !matches!(
            input.logical_name.as_slice(),
            OLD_FAITHFUL_CAR_LOGICAL_NAME_V1 | OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1
        )
    }) {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.required_object_inputs.logical_name",
            reason: "logical name is not registered as a V1 finite input",
        });
    }
    Ok(())
}

fn validate_shred_trust_context(
    context: Option<&InputObjectV1>,
    has_known_shred_stream: bool,
) -> Result<()> {
    if has_known_shred_stream && context.is_none() {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.shred_trust_context",
            reason: "is mandatory for a known format-2 or format-3 shred stream",
        });
    }
    if context.is_some_and(|input| input.logical_name != SHRED_TRUST_CONTEXT_LOGICAL_NAME_V1) {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.shred_trust_context.logical_name",
            reason: "must be solana.shred-trust-context",
        });
    }
    Ok(())
}

fn validate_catalog_predecessor(generation: u64, predecessor: Option<[u8; 32]>) -> Result<()> {
    if predecessor.is_some() != (generation > 0) {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.expected_catalog_predecessor",
            reason: "must be absent only for generation zero",
        });
    }
    Ok(())
}

fn validate_output_namespace(namespace: &[u8]) -> Result<()> {
    validate_len(
        namespace,
        1,
        MAX_OUTPUT_NAMESPACE_BYTES,
        "CompactionJobSpecV1.output_namespace",
    )?;
    if !namespace.ends_with(b"/") {
        return Err(ProtocolError::InvalidField {
            field: "CompactionJobSpecV1.output_namespace",
            reason: "must end in /",
        });
    }
    Ok(())
}

fn append_lower_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)]);
        output.push(HEX[usize::from(byte & 0x0f)]);
    }
}

#[cfg(test)]
mod tests {
    use hivezilla_protocol::{
        ClusterGenesisHash, PrefixHash, ProducerConfigSha256, StreamId, StreamManifestSha256,
    };

    use super::*;

    fn descriptor(name: &[u8]) -> HashedDescriptorV1 {
        HashedDescriptorV1::new(name.to_vec()).unwrap()
    }

    fn object(key: &[u8], digest: u8) -> ObjectRefV1 {
        ObjectRefV1::new(key.to_vec(), None, 100, [digest; 32]).unwrap()
    }

    fn object_only_spec() -> CompactionJobSpecV1 {
        spec_with_inputs(
            Vec::new(),
            vec![finite_input(
                OLD_FAITHFUL_CAR_LOGICAL_NAME_V1,
                b"inputs/e9.car",
                3,
            )],
            None,
        )
        .unwrap()
    }

    fn spec_with_inputs(
        streams: Vec<InputStreamRangeV1>,
        objects: Vec<InputObjectV1>,
        trust: Option<InputObjectV1>,
    ) -> Result<CompactionJobSpecV1> {
        CompactionJobSpecV1::new(
            [1; 16],
            [2; 32],
            9,
            SlotRangeV1::new(100, 103).unwrap(),
            streams,
            objects,
            trust,
            object(b"finality/e9", 4),
            descriptor(b"selection-v1"),
            descriptor(b"normalization-v1"),
            descriptor(b"archive-v2"),
            descriptor(b"epoch-schedule-v1"),
            None,
            0,
            b"archive/".to_vec(),
        )
    }

    fn finite_input(name: &[u8], key: &[u8], digest: u8) -> InputObjectV1 {
        InputObjectV1::new(name.to_vec(), descriptor(b"car-v1"), object(key, digest)).unwrap()
    }

    fn stream_input(id: u8, payload_format: u32) -> InputStreamRangeV1 {
        let stream = StreamHeaderV1::new(
            StreamId::new([id; 16]),
            ClusterGenesisHash::new([2; 32]),
            payload_format,
            1,
            ProducerConfigSha256::new([id.wrapping_add(1); 32]),
            StreamManifestSha256::new([id.wrapping_add(2); 32]),
        )
        .unwrap();
        InputStreamRangeV1::new(
            stream,
            CursorV1::new(10, PrefixHash::new([id.wrapping_add(3); 32])),
            CursorV1::new(20, PrefixHash::new([id.wrapping_add(4); 32])),
        )
        .unwrap()
    }

    #[test]
    fn object_only_job_spec_and_attempt_prefix_are_golden() {
        let spec = object_only_spec();
        let bytes = spec.job_spec_object_bytes();
        assert_eq!(bytes.len(), 510);
        assert_eq!(
            to_hex(&spec.job_spec_hash()),
            "0e391b8ee064f4af7be78627b59ebcff579f66862dc383cfc741ca2c5f0b6c5b"
        );
        assert_eq!(
            CompactionJobSpecV1::decode_job_spec_object(&bytes),
            Ok(spec.clone())
        );

        let job = CompactionJobV1::new(spec, LeaseFenceV1::new(42));
        assert_eq!(
            job.attempt_prefix(),
            b"archive/jobs/01010101010101010101010101010101/000000000000002a/"
        );
        assert_eq!(job.encode().len(), 485);
        assert_eq!(
            to_hex(&ordinary_sha256(&job.encode())),
            "00158883600490e1af12147bc458fe6ad929e819772ffe6987fee266c38d63f6"
        );
        assert_eq!(CompactionJobV1::decode(&job.encode()), Ok(job));
    }

    #[test]
    fn job_spec_hash_excludes_fence() {
        let spec = object_only_spec();
        let first = CompactionJobV1::new(spec.clone(), LeaseFenceV1::new(1));
        let retry = CompactionJobV1::new(spec, LeaseFenceV1::new(2));
        assert_eq!(first.spec().job_spec_hash(), retry.spec().job_spec_hash());
        assert_ne!(first.encode(), retry.encode());
    }

    #[test]
    fn all_registered_v1_job_shapes_round_trip() {
        let trust = || {
            InputObjectV1::new(
                SHRED_TRUST_CONTEXT_LOGICAL_NAME_V1.to_vec(),
                descriptor(b"shred-trust-v1"),
                object(b"trust/e9", 8),
            )
            .unwrap()
        };
        let jobs = [
            spec_with_inputs(vec![stream_input(1, 1)], Vec::new(), None).unwrap(),
            spec_with_inputs(
                vec![stream_input(1, 1), stream_input(2, 4)],
                Vec::new(),
                None,
            )
            .unwrap(),
            spec_with_inputs(
                Vec::new(),
                vec![finite_input(
                    OLD_FAITHFUL_CAR_ZST_LOGICAL_NAME_V1,
                    b"inputs/e9.car.zst",
                    5,
                )],
                None,
            )
            .unwrap(),
            spec_with_inputs(
                vec![stream_input(1, 4)],
                vec![finite_input(
                    OLD_FAITHFUL_CAR_LOGICAL_NAME_V1,
                    b"inputs/e9.car",
                    5,
                )],
                None,
            )
            .unwrap(),
            spec_with_inputs(vec![stream_input(1, 2)], Vec::new(), Some(trust())).unwrap(),
        ];
        for job in jobs {
            assert_eq!(
                CompactionJobSpecV1::decode_job_spec_object(&job.job_spec_object_bytes()),
                Ok(job)
            );
        }
    }

    #[test]
    fn shred_trust_context_presence_is_exact() {
        assert!(matches!(
            spec_with_inputs(vec![stream_input(1, 2)], Vec::new(), None),
            Err(ProtocolError::InvalidField { .. })
        ));
        let unnecessary = InputObjectV1::new(
            SHRED_TRUST_CONTEXT_LOGICAL_NAME_V1.to_vec(),
            descriptor(b"shred-trust-v1"),
            object(b"trust/e9", 8),
        )
        .unwrap();
        let structurally_valid =
            spec_with_inputs(vec![stream_input(1, 1)], Vec::new(), Some(unnecessary)).unwrap();
        assert!(matches!(
            structurally_valid.validate_shred_trust_requirement(false),
            Err(ProtocolError::InvalidField { .. })
        ));
        structurally_valid
            .validate_shred_trust_requirement(true)
            .unwrap();
    }

    #[test]
    fn derived_stream_is_rejected_before_job_construction() {
        let stream = StreamHeaderV1::new(
            StreamId::new([1; 16]),
            ClusterGenesisHash::new([2; 32]),
            6,
            1,
            ProducerConfigSha256::new([3; 32]),
            StreamManifestSha256::new([4; 32]),
        )
        .unwrap();
        assert!(matches!(
            InputStreamRangeV1::new(
                stream,
                CursorV1::new(0, PrefixHash::new([5; 32])),
                CursorV1::new(1, PrefixHash::new([6; 32]))
            ),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    #[test]
    fn generation_and_predecessor_must_agree() {
        let mut encoded = object_only_spec().job_spec_object_bytes();
        // Last 12 bytes are generation zero followed by the 4-byte namespace length;
        // mutate generation without adding the required predecessor option payload.
        let generation_offset = encoded.len() - 8 - 4 - b"archive/".len();
        encoded[generation_offset + 7] = 1;
        assert!(matches!(
            CompactionJobSpecV1::decode_job_spec_object(&encoded),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    #[test]
    fn oversized_vector_count_is_rejected_before_allocation() {
        let mut encoded = object_only_spec().job_spec_object_bytes();
        let count_offset = JOB_SPEC_OBJECT_DOMAIN.len() + 16 + 32 + 8 + 16;
        encoded[count_offset..count_offset + 4]
            .copy_from_slice(&((MAX_REQUIRED_INPUTS as u32) + 1).to_be_bytes());
        assert_eq!(
            CompactionJobSpecV1::decode_job_spec_object(&encoded),
            Err(ProtocolError::CountOutOfBounds {
                field: "CompactionJobSpecV1.required_stream_inputs",
                max: MAX_REQUIRED_INPUTS,
                actual: (MAX_REQUIRED_INPUTS + 1) as u64,
            })
        );
    }

    #[test]
    fn lease_fence_overflow_fails_closed() {
        assert!(matches!(
            LeaseFenceV1::new(u64::MAX).next(),
            Err(ProtocolError::IntegerOverflow { .. })
        ));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

use sha2::{Digest, Sha256};

use crate::codec::{Reader, put_option, put_u32, put_u64};
use crate::types::{MAX_PUBLICATION_OBJECTS, validate_named_objects, validate_unique_object_keys};
use crate::{NamedObjectRefV1, ObjectRefV1, ProtocolError, Result, SlotRangeV1};

pub const CATALOG_ENTRY_DOMAIN: &[u8] = b"blockzilla/v1/catalog-entry";

/// Reader-visible immutable publication manifest created by Blockzilla.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionManifestV1 {
    catalog_generation: u64,
    catalog_predecessor: Option<[u8; 32]>,
    job_id: [u8; 16],
    job_spec_hash: [u8; 32],
    epoch: u64,
    slots: SlotRangeV1,
    job_spec: ObjectRefV1,
    candidate_manifest: ObjectRefV1,
    published_finality_manifest: ObjectRefV1,
    produced_count: u32,
    skipped_count: u32,
    objects: Vec<NamedObjectRefV1>,
}

impl CompletionManifestV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_generation: u64,
        catalog_predecessor: Option<[u8; 32]>,
        job_id: [u8; 16],
        job_spec_hash: [u8; 32],
        epoch: u64,
        slots: SlotRangeV1,
        job_spec: ObjectRefV1,
        candidate_manifest: ObjectRefV1,
        published_finality_manifest: ObjectRefV1,
        produced_count: u32,
        skipped_count: u32,
        objects: Vec<NamedObjectRefV1>,
    ) -> Result<Self> {
        validate_generation_predecessor(catalog_generation, catalog_predecessor.is_some())?;
        if job_spec.sha256() != job_spec_hash {
            return Err(ProtocolError::DigestMismatch {
                field: "CompletionManifestV1.job_spec",
            });
        }
        if u64::from(produced_count) + u64::from(skipped_count) != slots.len() {
            return Err(ProtocolError::InvalidField {
                field: "CompletionManifestV1.coverage_counts",
                reason: "produced_count plus skipped_count must cover the slot range exactly",
            });
        }
        validate_named_objects(&objects, 1, "CompletionManifestV1.objects")?;
        validate_unique_object_keys(
            [&job_spec, &candidate_manifest, &published_finality_manifest]
                .into_iter()
                .chain(objects.iter().map(NamedObjectRefV1::object)),
            "CompletionManifestV1.object_references",
        )?;
        Ok(Self {
            catalog_generation,
            catalog_predecessor,
            job_id,
            job_spec_hash,
            epoch,
            slots,
            job_spec,
            candidate_manifest,
            published_finality_manifest,
            produced_count,
            skipped_count,
            objects,
        })
    }

    #[must_use]
    pub const fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }

    #[must_use]
    pub const fn catalog_predecessor(&self) -> Option<[u8; 32]> {
        self.catalog_predecessor
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
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub const fn slots(&self) -> SlotRangeV1 {
        self.slots
    }

    #[must_use]
    pub const fn job_spec(&self) -> &ObjectRefV1 {
        &self.job_spec
    }

    #[must_use]
    pub const fn candidate_manifest(&self) -> &ObjectRefV1 {
        &self.candidate_manifest
    }

    #[must_use]
    pub const fn published_finality_manifest(&self) -> &ObjectRefV1 {
        &self.published_finality_manifest
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
        put_u64(&mut output, self.catalog_generation);
        put_option(
            &mut output,
            self.catalog_predecessor.as_ref(),
            |output, predecessor| {
                output.extend_from_slice(predecessor);
                Ok(())
            },
        )
        .expect("fixed digest always encodes");
        output.extend_from_slice(&self.job_id);
        output.extend_from_slice(&self.job_spec_hash);
        put_u64(&mut output, self.epoch);
        self.slots.encode_into(&mut output);
        self.job_spec
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        self.candidate_manifest
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        self.published_finality_manifest
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        output.extend_from_slice(&self.produced_count.to_be_bytes());
        output.extend_from_slice(&self.skipped_count.to_be_bytes());
        put_u32(
            &mut output,
            self.objects.len(),
            "CompletionManifestV1.objects",
        )
        .expect("validated object count always fits u32");
        for object in &self.objects {
            object
                .encode_into(&mut output)
                .expect("validated named object always encodes");
        }
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let catalog_generation = reader.u64("CompletionManifestV1.catalog_generation")?;
        let catalog_predecessor = reader
            .option("CompletionManifestV1.catalog_predecessor", |reader| {
                reader.array("CompletionManifestV1.catalog_predecessor")
            })?;
        let job_id = reader.array("CompletionManifestV1.job_id")?;
        let job_spec_hash = reader.array("CompletionManifestV1.job_spec_hash")?;
        let epoch = reader.u64("CompletionManifestV1.epoch")?;
        let slots = SlotRangeV1::decode_from(&mut reader)?;
        let job_spec = ObjectRefV1::decode_from(&mut reader)?;
        let candidate_manifest = ObjectRefV1::decode_from(&mut reader)?;
        let published_finality_manifest = ObjectRefV1::decode_from(&mut reader)?;
        let produced_count = reader.u32("CompletionManifestV1.produced_count")?;
        let skipped_count = reader.u32("CompletionManifestV1.skipped_count")?;
        let object_count = reader.count(MAX_PUBLICATION_OBJECTS, "CompletionManifestV1.objects")?;
        let mut objects = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            objects.push(NamedObjectRefV1::decode_from(&mut reader)?);
        }
        reader.finish("CompletionManifestV1")?;
        Self::new(
            catalog_generation,
            catalog_predecessor,
            job_id,
            job_spec_hash,
            epoch,
            slots,
            job_spec,
            candidate_manifest,
            published_finality_manifest,
            produced_count,
            skipped_count,
            objects,
        )
    }
}

/// Immutable catalog-chain node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogEntryV1 {
    generation: u64,
    predecessor: Option<ObjectRefV1>,
    completion_manifest: ObjectRefV1,
}

impl CatalogEntryV1 {
    pub fn new(
        generation: u64,
        predecessor: Option<ObjectRefV1>,
        completion_manifest: ObjectRefV1,
    ) -> Result<Self> {
        validate_generation_predecessor(generation, predecessor.is_some())?;
        validate_unique_object_keys(
            predecessor
                .iter()
                .chain(std::iter::once(&completion_manifest)),
            "CatalogEntryV1.object_references",
        )?;
        Ok(Self {
            generation,
            predecessor,
            completion_manifest,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn predecessor(&self) -> Option<&ObjectRefV1> {
        self.predecessor.as_ref()
    }

    #[must_use]
    pub const fn completion_manifest(&self) -> &ObjectRefV1 {
        &self.completion_manifest
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        put_u64(&mut output, self.generation);
        put_option(&mut output, self.predecessor.as_ref(), |output, object| {
            object.encode_into(output)
        })
        .expect("validated object reference always encodes");
        self.completion_manifest
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let generation = reader.u64("CatalogEntryV1.generation")?;
        let predecessor = reader.option("CatalogEntryV1.predecessor", ObjectRefV1::decode_from)?;
        let completion_manifest = ObjectRefV1::decode_from(&mut reader)?;
        reader.finish("CatalogEntryV1")?;
        Self::new(generation, predecessor, completion_manifest)
    }

    /// Domain-separated semantic identity used by successor job specifications.
    #[must_use]
    pub fn semantic_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(CATALOG_ENTRY_DOMAIN);
        hasher.update(self.encode());
        hasher.finalize().into()
    }
}

/// Exact non-empty value stored in the linearizable catalog-head backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogHeadV1 {
    generation: u64,
    entry: ObjectRefV1,
}

impl CatalogHeadV1 {
    #[must_use]
    pub const fn new(generation: u64, entry: ObjectRefV1) -> Self {
        Self { generation, entry }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn entry(&self) -> &ObjectRefV1 {
        &self.entry
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        put_u64(&mut output, self.generation);
        self.entry
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let generation = reader.u64("CatalogHeadV1.generation")?;
        let entry = ObjectRefV1::decode_from(&mut reader)?;
        reader.finish("CatalogHeadV1")?;
        Ok(Self::new(generation, entry))
    }
}

/// Exact CAS value: zero bytes for the empty head, otherwise `CatalogHeadV1`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogHeadCasValueV1 {
    Empty,
    Head(CatalogHeadV1),
}

impl CatalogHeadCasValueV1 {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Empty => Vec::new(),
            Self::Head(head) => head.encode(),
        }
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.is_empty() {
            Ok(Self::Empty)
        } else {
            CatalogHeadV1::decode(encoded).map(Self::Head)
        }
    }
}

/// Checks only the generation rule for an exact head CAS transition.
///
/// Entry, completion, epoch, slot, and descriptor-chain checks require fetched
/// referenced bytes and belong to the higher-level catalog validator.
pub fn validate_catalog_head_advance(
    current: &CatalogHeadCasValueV1,
    next: &CatalogHeadV1,
) -> Result<()> {
    let expected = match current {
        CatalogHeadCasValueV1::Empty => 0,
        CatalogHeadCasValueV1::Head(head) => {
            head.generation
                .checked_add(1)
                .ok_or(ProtocolError::IntegerOverflow {
                    field: "CatalogHeadV1.generation",
                })?
        }
    };
    if next.generation != expected {
        return Err(ProtocolError::InvalidField {
            field: "CatalogHeadV1.generation",
            reason: "must create generation zero from empty or increment the current head by one",
        });
    }
    Ok(())
}

fn validate_generation_predecessor(generation: u64, has_predecessor: bool) -> Result<()> {
    if has_predecessor != (generation > 0) {
        return Err(ProtocolError::InvalidField {
            field: "catalog predecessor",
            reason: "must be absent only for generation zero",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ordinary_sha256;

    fn object(key: &[u8], len: u64, digest: u8) -> ObjectRefV1 {
        ObjectRefV1::new(key.to_vec(), None, len, [digest; 32]).unwrap()
    }

    #[test]
    fn catalog_entry_encoding_and_semantic_digest_are_golden() {
        let entry = CatalogEntryV1::new(0, None, object(b"completion/0", 99, 7)).unwrap();
        assert_eq!(
            to_hex(&entry.encode()),
            concat!(
                "0000000000000000",
                "00",
                "0000000c636f6d706c6574696f6e2f30",
                "00",
                "0000000000000063",
                "0707070707070707070707070707070707070707070707070707070707070707"
            )
        );
        assert_eq!(
            to_hex(&entry.semantic_digest()),
            "88a374b8c9ada0a15f889cfa9abf7eb3f93fafadccc37f5632cbc5809f83fd2a"
        );
        assert_eq!(CatalogEntryV1::decode(&entry.encode()), Ok(entry));
    }

    #[test]
    fn exact_head_empty_and_append_values_round_trip() {
        assert_eq!(CatalogHeadCasValueV1::Empty.encode(), Vec::<u8>::new());
        assert_eq!(
            CatalogHeadCasValueV1::decode(&[]),
            Ok(CatalogHeadCasValueV1::Empty)
        );
        let head = CatalogHeadV1::new(0, object(b"entry/0", 80, 8));
        validate_catalog_head_advance(&CatalogHeadCasValueV1::Empty, &head).unwrap();
        assert_eq!(
            CatalogHeadCasValueV1::decode(&head.encode()),
            Ok(CatalogHeadCasValueV1::Head(head))
        );
    }

    #[test]
    fn invalid_generation_shapes_are_rejected() {
        assert!(matches!(
            CatalogEntryV1::new(1, None, object(b"completion/1", 1, 1)),
            Err(ProtocolError::InvalidField { .. })
        ));
        let bad_first = CatalogHeadV1::new(1, object(b"entry/1", 1, 1));
        assert!(matches!(
            validate_catalog_head_advance(&CatalogHeadCasValueV1::Empty, &bad_first),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    #[test]
    fn completion_job_spec_digest_must_match() {
        assert!(matches!(
            CompletionManifestV1::new(
                0,
                None,
                [0; 16],
                [9; 32],
                0,
                SlotRangeV1::new(1, 2).unwrap(),
                object(b"job", 1, 8),
                object(b"candidate", 1, 2),
                object(b"finality", 1, 3),
                1,
                0,
                vec![NamedObjectRefV1::new(b"blocks".to_vec(), object(b"blocks", 1, 4)).unwrap()],
            ),
            Err(ProtocolError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn completion_manifest_has_a_pinned_digest_and_round_trips() {
        let completion = CompletionManifestV1::new(
            0,
            None,
            [1; 16],
            [6; 32],
            9,
            SlotRangeV1::new(100, 102).unwrap(),
            object(b"job", 10, 6),
            object(b"candidate", 11, 7),
            object(b"finality", 12, 8),
            1,
            1,
            vec![
                NamedObjectRefV1::new(b"blocks".to_vec(), object(b"objects/blocks", 13, 9))
                    .unwrap(),
            ],
        )
        .unwrap();
        let encoded = completion.encode();
        assert_eq!(encoded.len(), 317);
        assert_eq!(
            to_hex(&ordinary_sha256(&encoded)),
            "8ba98687f80dd4e6cf542f32303c4864ecb75d3fa269671bd8973046e792b43d"
        );
        assert_eq!(CompletionManifestV1::decode(&encoded), Ok(completion));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

use std::cmp::Ordering;

use crate::codec::{Reader, put_option, put_u8, put_u32, put_u64};
use crate::types::{MAX_PUBLICATION_OBJECTS, validate_unique_object_keys};
use crate::{
    CatalogEntryV1, CatalogHeadV1, HashedDescriptorV1, ObjectRefV1, ProtocolError, Result,
};

const RECOVERY_KEY_PREFIX: &[u8] = b"blockzilla-recovery/v1/";

/// Independent verification used for one archive recovery copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecoveryVerificationV1 {
    ProviderSha256 = 1,
    FullReadbackSha256 = 2,
}

impl TryFrom<u8> for RecoveryVerificationV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::ProviderSha256),
            2 => Ok(Self::FullReadbackSha256),
            value => Err(ProtocolError::UnknownEnum {
                field: "ArchiveRecoveryObjectV1.verification",
                value,
            }),
        }
    }
}

/// Mapping from one canonical object identity to its recovery-provider locator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveRecoveryObjectV1 {
    canonical: ObjectRefV1,
    recovery: ObjectRefV1,
    verification: RecoveryVerificationV1,
}

impl ArchiveRecoveryObjectV1 {
    pub fn new(
        canonical: ObjectRefV1,
        recovery: ObjectRefV1,
        verification: RecoveryVerificationV1,
    ) -> Result<Self> {
        if canonical.encoded_len() != recovery.encoded_len() {
            return Err(ProtocolError::InvalidField {
                field: "ArchiveRecoveryObjectV1.recovery.encoded_len",
                reason: "must equal the canonical encoded length",
            });
        }
        if canonical.sha256() != recovery.sha256() {
            return Err(ProtocolError::DigestMismatch {
                field: "ArchiveRecoveryObjectV1.recovery.sha256",
            });
        }
        Ok(Self {
            canonical,
            recovery,
            verification,
        })
    }

    #[must_use]
    pub const fn canonical(&self) -> &ObjectRefV1 {
        &self.canonical
    }

    #[must_use]
    pub const fn recovery(&self) -> &ObjectRefV1 {
        &self.recovery
    }

    #[must_use]
    pub const fn verification(&self) -> RecoveryVerificationV1 {
        self.verification
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated recovery object always encodes");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("ArchiveRecoveryObjectV1")?;
        Ok(value)
    }

    fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        self.canonical.encode_into(output)?;
        self.recovery.encode_into(output)?;
        put_u8(output, self.verification as u8);
        Ok(())
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let canonical = ObjectRefV1::decode_from(reader)?;
        let recovery = ObjectRefV1::decode_from(reader)?;
        let verification =
            RecoveryVerificationV1::try_from(reader.u8("ArchiveRecoveryObjectV1.verification")?)?;
        Self::new(canonical, recovery, verification)
    }
}

/// Complete, chained recovery mapping for one committed catalog generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveRecoveryReceiptV1 {
    catalog_generation: u64,
    catalog_entry: ObjectRefV1,
    recovery_target_id: [u8; 16],
    recovery_failure_domain_id: [u8; 16],
    recovery_target: HashedDescriptorV1,
    previous_receipt: Option<ObjectRefV1>,
    objects: Vec<ArchiveRecoveryObjectV1>,
}

impl ArchiveRecoveryReceiptV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog_generation: u64,
        catalog_entry: ObjectRefV1,
        recovery_target_id: [u8; 16],
        recovery_failure_domain_id: [u8; 16],
        recovery_target: HashedDescriptorV1,
        previous_receipt: Option<ObjectRefV1>,
        objects: Vec<ArchiveRecoveryObjectV1>,
    ) -> Result<Self> {
        if previous_receipt.is_some() != (catalog_generation > 0) {
            return Err(ProtocolError::InvalidField {
                field: "ArchiveRecoveryReceiptV1.previous_receipt",
                reason: "must be absent only for generation zero",
            });
        }
        validate_recovery_objects(&objects)?;
        if !objects
            .iter()
            .any(|object| object.canonical == catalog_entry)
        {
            return Err(ProtocolError::InvalidField {
                field: "ArchiveRecoveryReceiptV1.objects",
                reason: "must include the exact canonical catalog entry reference",
            });
        }
        Ok(Self {
            catalog_generation,
            catalog_entry,
            recovery_target_id,
            recovery_failure_domain_id,
            recovery_target,
            previous_receipt,
            objects,
        })
    }

    #[must_use]
    pub const fn catalog_generation(&self) -> u64 {
        self.catalog_generation
    }

    #[must_use]
    pub const fn catalog_entry(&self) -> &ObjectRefV1 {
        &self.catalog_entry
    }

    #[must_use]
    pub const fn recovery_target_id(&self) -> [u8; 16] {
        self.recovery_target_id
    }

    #[must_use]
    pub const fn recovery_failure_domain_id(&self) -> [u8; 16] {
        self.recovery_failure_domain_id
    }

    #[must_use]
    pub const fn recovery_target(&self) -> &HashedDescriptorV1 {
        &self.recovery_target
    }

    #[must_use]
    pub const fn previous_receipt(&self) -> Option<&ObjectRefV1> {
        self.previous_receipt.as_ref()
    }

    #[must_use]
    pub fn objects(&self) -> &[ArchiveRecoveryObjectV1] {
        &self.objects
    }

    pub fn validate_target_configuration(
        &self,
        target_id: [u8; 16],
        failure_domain_id: [u8; 16],
        target: &HashedDescriptorV1,
    ) -> Result<()> {
        if self.recovery_target_id != target_id
            || self.recovery_failure_domain_id != failure_domain_id
            || &self.recovery_target != target
        {
            return Err(ProtocolError::InvalidField {
                field: "ArchiveRecoveryReceiptV1.target_configuration",
                reason: "target ID, failure domain, and descriptor must exactly match configuration",
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        put_u64(&mut output, self.catalog_generation);
        self.catalog_entry
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        output.extend_from_slice(&self.recovery_target_id);
        output.extend_from_slice(&self.recovery_failure_domain_id);
        self.recovery_target
            .encode_into(&mut output)
            .expect("validated descriptor always encodes");
        put_option(
            &mut output,
            self.previous_receipt.as_ref(),
            |output, object| object.encode_into(output),
        )
        .expect("validated object reference always encodes");
        put_u32(
            &mut output,
            self.objects.len(),
            "ArchiveRecoveryReceiptV1.objects",
        )
        .expect("validated mapping count always fits u32");
        for object in &self.objects {
            object
                .encode_into(&mut output)
                .expect("validated mapping always encodes");
        }
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let catalog_generation = reader.u64("ArchiveRecoveryReceiptV1.catalog_generation")?;
        let catalog_entry = ObjectRefV1::decode_from(&mut reader)?;
        let recovery_target_id = reader.array("ArchiveRecoveryReceiptV1.recovery_target_id")?;
        let recovery_failure_domain_id =
            reader.array("ArchiveRecoveryReceiptV1.recovery_failure_domain_id")?;
        let recovery_target = HashedDescriptorV1::decode_from(&mut reader)?;
        let previous_receipt = reader.option(
            "ArchiveRecoveryReceiptV1.previous_receipt",
            ObjectRefV1::decode_from,
        )?;
        let object_count =
            reader.count(MAX_PUBLICATION_OBJECTS, "ArchiveRecoveryReceiptV1.objects")?;
        let mut objects = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            objects.push(ArchiveRecoveryObjectV1::decode_from(&mut reader)?);
        }
        reader.finish("ArchiveRecoveryReceiptV1")?;
        Self::new(
            catalog_generation,
            catalog_entry,
            recovery_target_id,
            recovery_failure_domain_id,
            recovery_target,
            previous_receipt,
            objects,
        )
    }
}

/// Exact recoverable discovery point in one recovery failure domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveRecoveryCheckpointV1 {
    recovery_target_id: [u8; 16],
    catalog_head: CatalogHeadV1,
    latest_receipt: ObjectRefV1,
}

impl ArchiveRecoveryCheckpointV1 {
    #[must_use]
    pub const fn new(
        recovery_target_id: [u8; 16],
        catalog_head: CatalogHeadV1,
        latest_receipt: ObjectRefV1,
    ) -> Self {
        Self {
            recovery_target_id,
            catalog_head,
            latest_receipt,
        }
    }

    #[must_use]
    pub const fn recovery_target_id(&self) -> [u8; 16] {
        self.recovery_target_id
    }

    #[must_use]
    pub const fn catalog_head(&self) -> &CatalogHeadV1 {
        &self.catalog_head
    }

    #[must_use]
    pub const fn latest_receipt(&self) -> &ObjectRefV1 {
        &self.latest_receipt
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&self.recovery_target_id);
        output.extend_from_slice(&self.catalog_head.encode());
        self.latest_receipt
            .encode_into(&mut output)
            .expect("validated object reference always encodes");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let recovery_target_id = reader.array("ArchiveRecoveryCheckpointV1.recovery_target_id")?;
        let generation = reader.u64("ArchiveRecoveryCheckpointV1.catalog_head.generation")?;
        let entry = ObjectRefV1::decode_from(&mut reader)?;
        let latest_receipt = ObjectRefV1::decode_from(&mut reader)?;
        reader.finish("ArchiveRecoveryCheckpointV1")?;
        Ok(Self::new(
            recovery_target_id,
            CatalogHeadV1::new(generation, entry),
            latest_receipt,
        ))
    }
}

/// Exact recovery checkpoint CAS value: empty bytes or one checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveRecoveryCheckpointValueV1 {
    Empty,
    Checkpoint(ArchiveRecoveryCheckpointV1),
}

impl ArchiveRecoveryCheckpointValueV1 {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Empty => Vec::new(),
            Self::Checkpoint(checkpoint) => checkpoint.encode(),
        }
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.is_empty() {
            Ok(Self::Empty)
        } else {
            ArchiveRecoveryCheckpointV1::decode(encoded).map(Self::Checkpoint)
        }
    }
}

/// Exact immutable receipt key defined by V1.
#[must_use]
pub fn archive_recovery_receipt_key(
    recovery_target_id: [u8; 16],
    catalog_generation: u64,
    catalog_entry: &ObjectRefV1,
) -> Vec<u8> {
    let mut key = RECOVERY_KEY_PREFIX.to_vec();
    append_lower_hex(&mut key, &recovery_target_id);
    key.push(b'/');
    key.extend_from_slice(format!("{catalog_generation:016x}").as_bytes());
    key.push(b'-');
    append_lower_hex(&mut key, &catalog_entry.sha256());
    key.extend_from_slice(b".receipt");
    key
}

/// Exact single mutable checkpoint key defined by V1.
#[must_use]
pub fn archive_recovery_checkpoint_key(recovery_target_id: [u8; 16]) -> Vec<u8> {
    let mut key = RECOVERY_KEY_PREFIX.to_vec();
    append_lower_hex(&mut key, &recovery_target_id);
    key.extend_from_slice(b"/head.checkpoint");
    key
}

/// Validates the protocol-visible relationships for one exact checkpoint CAS.
///
/// Completeness of the transitive mapping set and equality to configured
/// credentials/scopes require the publication graph and deployment
/// configuration and are intentionally outside this fixed-value crate.
pub fn validate_recovery_checkpoint_advance(
    current: &ArchiveRecoveryCheckpointValueV1,
    next: &ArchiveRecoveryCheckpointV1,
    latest_receipt_ref: &ObjectRefV1,
    latest_receipt: &ArchiveRecoveryReceiptV1,
    catalog_entry: &CatalogEntryV1,
) -> Result<()> {
    latest_receipt_ref.verify_bytes(&latest_receipt.encode())?;
    latest_receipt
        .catalog_entry
        .verify_bytes(&catalog_entry.encode())?;
    if latest_receipt_ref.key()
        != archive_recovery_receipt_key(
            latest_receipt.recovery_target_id,
            latest_receipt.catalog_generation,
            &latest_receipt.catalog_entry,
        )
    {
        return Err(ProtocolError::InvalidField {
            field: "ArchiveRecoveryReceiptV1.object_key",
            reason: "must equal the exact target/generation/catalog-entry receipt key",
        });
    }
    if next.latest_receipt != *latest_receipt_ref
        || next.catalog_head.entry() != latest_receipt.catalog_entry()
        || next.catalog_head.generation() != latest_receipt.catalog_generation
        || catalog_entry.generation() != latest_receipt.catalog_generation
        || next.recovery_target_id != latest_receipt.recovery_target_id
    {
        return Err(ProtocolError::InvalidField {
            field: "ArchiveRecoveryCheckpointV1",
            reason: "checkpoint, receipt, receipt reference, and catalog entry must identify one target and generation",
        });
    }

    match current {
        ArchiveRecoveryCheckpointValueV1::Empty => {
            if latest_receipt.catalog_generation != 0
                || latest_receipt.previous_receipt.is_some()
                || catalog_entry.predecessor().is_some()
            {
                return Err(ProtocolError::InvalidField {
                    field: "ArchiveRecoveryCheckpointV1.generation",
                    reason: "the empty checkpoint accepts only generation zero without predecessors",
                });
            }
        }
        ArchiveRecoveryCheckpointValueV1::Checkpoint(previous) => {
            let expected_generation = previous.catalog_head.generation().checked_add(1).ok_or(
                ProtocolError::IntegerOverflow {
                    field: "ArchiveRecoveryCheckpointV1.catalog_head.generation",
                },
            )?;
            if latest_receipt.catalog_generation != expected_generation
                || next.recovery_target_id != previous.recovery_target_id
                || latest_receipt.previous_receipt.as_ref() != Some(&previous.latest_receipt)
                || catalog_entry.predecessor() != Some(previous.catalog_head.entry())
            {
                return Err(ProtocolError::InvalidField {
                    field: "ArchiveRecoveryCheckpointV1.predecessor",
                    reason: "must increment the same target and exactly chain receipt and catalog predecessors",
                });
            }
        }
    }
    Ok(())
}

fn validate_recovery_objects(objects: &[ArchiveRecoveryObjectV1]) -> Result<()> {
    if objects.is_empty() {
        return Err(ProtocolError::InvalidField {
            field: "ArchiveRecoveryReceiptV1.objects",
            reason: "mapping vector must not be empty",
        });
    }
    if objects.len() > MAX_PUBLICATION_OBJECTS {
        return Err(ProtocolError::CountOutOfBounds {
            field: "ArchiveRecoveryReceiptV1.objects",
            max: MAX_PUBLICATION_OBJECTS,
            actual: objects.len() as u64,
        });
    }
    if objects
        .windows(2)
        .any(|pair| pair[0].canonical.locator_cmp(&pair[1].canonical) != Ordering::Less)
    {
        return Err(ProtocolError::NonCanonicalOrder {
            field: "ArchiveRecoveryReceiptV1.objects",
        });
    }
    validate_unique_object_keys(
        objects.iter().map(ArchiveRecoveryObjectV1::canonical),
        "ArchiveRecoveryReceiptV1.objects.canonical",
    )?;
    validate_unique_object_keys(
        objects.iter().map(ArchiveRecoveryObjectV1::recovery),
        "ArchiveRecoveryReceiptV1.objects.recovery",
    )?;
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
    use super::*;
    use crate::types::ordinary_sha256;

    fn object(key: &[u8], len: u64, digest: u8) -> ObjectRefV1 {
        ObjectRefV1::new(key.to_vec(), None, len, [digest; 32]).unwrap()
    }

    fn mapping(canonical_key: &[u8], recovery_key: &[u8], digest: u8) -> ArchiveRecoveryObjectV1 {
        ArchiveRecoveryObjectV1::new(
            object(canonical_key, 10, digest),
            object(recovery_key, 10, digest),
            RecoveryVerificationV1::FullReadbackSha256,
        )
        .unwrap()
    }

    #[test]
    fn recovery_keys_are_exact_golden_values() {
        let target = [0xab; 16];
        let entry = object(b"catalog/entry/42", 10, 0xcd);
        assert_eq!(
            archive_recovery_receipt_key(target, 42, &entry),
            concat!(
                "blockzilla-recovery/v1/",
                "abababababababababababababababab/",
                "000000000000002a-",
                "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
                ".receipt"
            )
            .as_bytes()
        );
        assert_eq!(
            archive_recovery_checkpoint_key(target),
            b"blockzilla-recovery/v1/abababababababababababababababab/head.checkpoint"
        );
    }

    #[test]
    fn recovery_mapping_requires_equal_bytes_and_strict_order() {
        assert!(matches!(
            ArchiveRecoveryObjectV1::new(
                object(b"a", 10, 1),
                object(b"copy/a", 11, 1),
                RecoveryVerificationV1::ProviderSha256,
            ),
            Err(ProtocolError::InvalidField { .. })
        ));
        let result = ArchiveRecoveryReceiptV1::new(
            0,
            object(b"entry/0", 10, 5),
            [1; 16],
            [2; 16],
            HashedDescriptorV1::new(b"r2-target".to_vec()).unwrap(),
            None,
            vec![mapping(b"z", b"copy/z", 1), mapping(b"a", b"copy/a", 2)],
        );
        assert!(matches!(
            result,
            Err(ProtocolError::NonCanonicalOrder { .. })
        ));
    }

    #[test]
    fn generation_zero_checkpoint_round_trips_and_validates() {
        let target = [1; 16];
        let entry = CatalogEntryV1::new(0, None, object(b"completion/0", 10, 4)).unwrap();
        let entry_ref = object_for_bytes(b"entry/0", &entry.encode());
        let recovery_entry_ref = ObjectRefV1::new(
            b"copy/entry/0".to_vec(),
            None,
            entry_ref.encoded_len(),
            entry_ref.sha256(),
        )
        .unwrap();
        let receipt = ArchiveRecoveryReceiptV1::new(
            0,
            entry_ref.clone(),
            target,
            [2; 16],
            HashedDescriptorV1::new(b"r2-target".to_vec()).unwrap(),
            None,
            vec![
                ArchiveRecoveryObjectV1::new(
                    entry_ref.clone(),
                    recovery_entry_ref,
                    RecoveryVerificationV1::FullReadbackSha256,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let receipt_ref = object_for_bytes(
            &archive_recovery_receipt_key(target, 0, &entry_ref),
            &receipt.encode(),
        );
        let checkpoint = ArchiveRecoveryCheckpointV1::new(
            target,
            CatalogHeadV1::new(0, entry_ref),
            receipt_ref.clone(),
        );
        validate_recovery_checkpoint_advance(
            &ArchiveRecoveryCheckpointValueV1::Empty,
            &checkpoint,
            &receipt_ref,
            &receipt,
            &entry,
        )
        .unwrap();
        assert_eq!(receipt.encode().len(), 252);
        assert_eq!(
            hex(&ordinary_sha256(&receipt.encode())),
            "465362ac6d5313b9059c4c200148e6bcdf143ba2fc74403b314fc3b11798fe33"
        );
        assert_eq!(checkpoint.encode().len(), 266);
        assert_eq!(
            hex(&ordinary_sha256(&checkpoint.encode())),
            "5d930f942fc50debd36d8027dcd64da88ee695c096e10ef0246f1f87add1bb34"
        );
        assert_eq!(
            ArchiveRecoveryReceiptV1::decode(&receipt.encode()),
            Ok(receipt)
        );
        assert_eq!(
            ArchiveRecoveryCheckpointV1::decode(&checkpoint.encode()),
            Ok(checkpoint)
        );
    }

    #[test]
    fn unknown_verification_and_oversized_count_fail_before_allocation() {
        let recovery_mapping = mapping(b"a", b"copy/a", 1);
        let mut encoded = recovery_mapping.encode();
        *encoded.last_mut().unwrap() = 3;
        assert!(matches!(
            ArchiveRecoveryObjectV1::decode(&encoded),
            Err(ProtocolError::UnknownEnum { .. })
        ));

        let receipt = ArchiveRecoveryReceiptV1::new(
            0,
            object(b"a", 10, 1),
            [1; 16],
            [2; 16],
            HashedDescriptorV1::new(b"target".to_vec()).unwrap(),
            None,
            vec![mapping(b"a", b"copy/a", 1)],
        )
        .unwrap();
        let mut encoded = receipt.encode();
        // The count is immediately before the one final mapping.
        let mapping_len = receipt.objects()[0].encode().len();
        let count_offset = encoded.len() - mapping_len - 4;
        encoded[count_offset..count_offset + 4]
            .copy_from_slice(&((MAX_PUBLICATION_OBJECTS as u32) + 1).to_be_bytes());
        assert_eq!(
            ArchiveRecoveryReceiptV1::decode(&encoded),
            Err(ProtocolError::CountOutOfBounds {
                field: "ArchiveRecoveryReceiptV1.objects",
                max: MAX_PUBLICATION_OBJECTS,
                actual: (MAX_PUBLICATION_OBJECTS + 1) as u64,
            })
        );
    }

    fn object_for_bytes(key: &[u8], bytes: &[u8]) -> ObjectRefV1 {
        ObjectRefV1::new(
            key.to_vec(),
            None,
            bytes.len() as u64,
            crate::types::ordinary_sha256(bytes),
        )
        .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

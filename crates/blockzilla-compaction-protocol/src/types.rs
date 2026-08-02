use std::cmp::Ordering;
use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::codec::{Reader, put_bytes, put_option, put_u64, validate_len};
use crate::{ProtocolError, Result};

pub const MAX_OBJECT_KEY_BYTES: usize = 4_096;
pub const MAX_OBJECT_VERSION_BYTES: usize = 4_096;
pub const MAX_DESCRIPTOR_BYTES: usize = 1_048_576;
pub const MAX_LOGICAL_NAME_BYTES: usize = 128;
pub const MAX_OUTPUT_NAMESPACE_BYTES: usize = 4_096;
pub const MAX_REQUIRED_INPUTS: usize = 4_096;
pub const MAX_PUBLICATION_OBJECTS: usize = 65_536;

pub const DESCRIPTOR_HASH_DOMAIN: &[u8] = b"blockzilla/v1/descriptor";

/// Exact immutable object locator and byte identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRefV1 {
    key: Vec<u8>,
    object_version: Option<Vec<u8>>,
    encoded_len: u64,
    sha256: [u8; 32],
}

impl ObjectRefV1 {
    pub fn new(
        key: Vec<u8>,
        object_version: Option<Vec<u8>>,
        encoded_len: u64,
        sha256: [u8; 32],
    ) -> Result<Self> {
        validate_len(&key, 1, MAX_OBJECT_KEY_BYTES, "ObjectRefV1.key")?;
        if let Some(version) = object_version.as_deref() {
            validate_len(
                version,
                1,
                MAX_OBJECT_VERSION_BYTES,
                "ObjectRefV1.object_version",
            )?;
        }
        Ok(Self {
            key,
            object_version,
            encoded_len,
            sha256,
        })
    }

    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[must_use]
    pub fn object_version(&self) -> Option<&[u8]> {
        self.object_version.as_deref()
    }

    #[must_use]
    pub const fn encoded_len(&self) -> u64 {
        self.encoded_len
    }

    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    /// Verifies exact stored bytes against both declared length and SHA-256.
    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<()> {
        let actual_len =
            u64::try_from(bytes.len()).map_err(|_| ProtocolError::IntegerOverflow {
                field: "ObjectRefV1.encoded_len",
            })?;
        if self.encoded_len != actual_len {
            return Err(ProtocolError::InvalidField {
                field: "ObjectRefV1.encoded_len",
                reason: "does not match the exact stored bytes",
            });
        }
        if self.sha256 != ordinary_sha256(bytes) {
            return Err(ProtocolError::DigestMismatch {
                field: "ObjectRefV1.sha256",
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated ObjectRefV1 always fits u32 lengths");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("ObjectRefV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        put_bytes(output, &self.key, "ObjectRefV1.key")?;
        put_option(output, self.object_version.as_ref(), |output, version| {
            put_bytes(output, version, "ObjectRefV1.object_version")
        })?;
        put_u64(output, self.encoded_len);
        output.extend_from_slice(&self.sha256);
        Ok(())
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let key = reader.bytes(1, MAX_OBJECT_KEY_BYTES, "ObjectRefV1.key")?;
        let object_version = reader.option("ObjectRefV1.object_version", |reader| {
            reader.bytes(1, MAX_OBJECT_VERSION_BYTES, "ObjectRefV1.object_version")
        })?;
        let encoded_len = reader.u64("ObjectRefV1.encoded_len")?;
        let sha256 = reader.array("ObjectRefV1.sha256")?;
        Self::new(key, object_version, encoded_len, sha256)
    }

    pub(crate) fn locator_cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.object_version.cmp(&other.object_version))
    }
}

/// A registered logical object role and its immutable reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedObjectRefV1 {
    logical_name: Vec<u8>,
    object: ObjectRefV1,
}

impl NamedObjectRefV1 {
    pub fn new(logical_name: Vec<u8>, object: ObjectRefV1) -> Result<Self> {
        validate_logical_name(&logical_name, "NamedObjectRefV1.logical_name")?;
        Ok(Self {
            logical_name,
            object,
        })
    }

    #[must_use]
    pub fn logical_name(&self) -> &[u8] {
        &self.logical_name
    }

    #[must_use]
    pub const fn object(&self) -> &ObjectRefV1 {
        &self.object
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated NamedObjectRefV1 always fits u32 lengths");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("NamedObjectRefV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        put_bytes(output, &self.logical_name, "NamedObjectRefV1.logical_name")?;
        self.object.encode_into(output)
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let logical_name =
            reader.bytes(1, MAX_LOGICAL_NAME_BYTES, "NamedObjectRefV1.logical_name")?;
        let object = ObjectRefV1::decode_from(reader)?;
        Self::new(logical_name, object)
    }
}

/// Exact descriptor bytes and their V1 domain-separated identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashedDescriptorV1 {
    sha256: [u8; 32],
    bytes: Vec<u8>,
}

impl HashedDescriptorV1 {
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        validate_len(&bytes, 1, MAX_DESCRIPTOR_BYTES, "HashedDescriptorV1.bytes")?;
        let sha256 = descriptor_sha256(&bytes);
        Ok(Self { sha256, bytes })
    }

    pub fn from_parts(sha256: [u8; 32], bytes: Vec<u8>) -> Result<Self> {
        let descriptor = Self::new(bytes)?;
        if descriptor.sha256 != sha256 {
            return Err(ProtocolError::DigestMismatch {
                field: "HashedDescriptorV1.sha256",
            });
        }
        Ok(descriptor)
    }

    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output)
            .expect("validated HashedDescriptorV1 always fits u32 lengths");
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("HashedDescriptorV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) -> Result<()> {
        output.extend_from_slice(&self.sha256);
        put_bytes(output, &self.bytes, "HashedDescriptorV1.bytes")
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        let sha256 = reader.array("HashedDescriptorV1.sha256")?;
        let bytes = reader.bytes(1, MAX_DESCRIPTOR_BYTES, "HashedDescriptorV1.bytes")?;
        Self::from_parts(sha256, bytes)
    }
}

/// Inclusive/exclusive slot range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotRangeV1 {
    first_slot: u64,
    next_slot: u64,
}

impl SlotRangeV1 {
    pub fn new(first_slot: u64, next_slot: u64) -> Result<Self> {
        if first_slot >= next_slot {
            return Err(ProtocolError::InvalidField {
                field: "SlotRangeV1",
                reason: "first_slot must be less than next_slot",
            });
        }
        Ok(Self {
            first_slot,
            next_slot,
        })
    }

    #[must_use]
    pub const fn first_slot(&self) -> u64 {
        self.first_slot
    }

    #[must_use]
    pub const fn next_slot(&self) -> u64 {
        self.next_slot
    }

    #[must_use]
    pub const fn len(&self) -> u64 {
        self.next_slot - self.first_slot
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(16);
        self.encode_into(&mut output);
        output
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(encoded);
        let value = Self::decode_from(&mut reader)?;
        reader.finish("SlotRangeV1")?;
        Ok(value)
    }

    pub(crate) fn encode_into(&self, output: &mut Vec<u8>) {
        put_u64(output, self.first_slot);
        put_u64(output, self.next_slot);
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self> {
        Self::new(
            reader.u64("SlotRangeV1.first_slot")?,
            reader.u64("SlotRangeV1.next_slot")?,
        )
    }
}

pub(crate) fn validate_logical_name(value: &[u8], field: &'static str) -> Result<()> {
    validate_len(value, 1, MAX_LOGICAL_NAME_BYTES, field)?;
    if !value
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte))
    {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "must use only [a-z0-9._-]",
        });
    }
    Ok(())
}

pub(crate) fn validate_named_objects(
    objects: &[NamedObjectRefV1],
    min: usize,
    field: &'static str,
) -> Result<()> {
    if objects.len() < min {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "object vector must not be empty",
        });
    }
    if objects.len() > MAX_PUBLICATION_OBJECTS {
        return Err(ProtocolError::CountOutOfBounds {
            field,
            max: MAX_PUBLICATION_OBJECTS,
            actual: objects.len() as u64,
        });
    }
    if objects
        .windows(2)
        .any(|pair| pair[0].logical_name >= pair[1].logical_name)
    {
        return Err(ProtocolError::NonCanonicalOrder { field });
    }
    validate_unique_object_keys(objects.iter().map(NamedObjectRefV1::object), field)?;
    Ok(())
}

pub(crate) fn validate_unique_object_keys<'a>(
    objects: impl IntoIterator<Item = &'a ObjectRefV1>,
    field: &'static str,
) -> Result<()> {
    let mut keys = BTreeSet::new();
    if objects.into_iter().any(|object| !keys.insert(object.key())) {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "duplicate object-store key",
        });
    }
    Ok(())
}

#[must_use]
pub(crate) fn descriptor_sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(DESCRIPTOR_HASH_DOMAIN);
    hasher.update(bytes);
    hasher.finalize().into()
}

#[must_use]
pub(crate) fn ordinary_sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_golden_and_digest_rejection() {
        let descriptor = HashedDescriptorV1::new(b"archive-v2/fixed".to_vec()).unwrap();
        assert_eq!(
            to_hex(&descriptor.sha256()),
            "cc9fb8f16c9e4d487b601321d6616e8dc4ea6f0cb7f60df16c5325a58e394069"
        );
        assert_eq!(
            HashedDescriptorV1::from_parts([9; 32], b"archive-v2/fixed".to_vec()),
            Err(ProtocolError::DigestMismatch {
                field: "HashedDescriptorV1.sha256"
            })
        );
    }

    #[test]
    fn object_ref_rejects_empty_version_and_trailing_bytes() {
        assert!(matches!(
            ObjectRefV1::new(b"key".to_vec(), Some(Vec::new()), 0, [0; 32]),
            Err(ProtocolError::LengthOutOfBounds { .. })
        ));
        let object = ObjectRefV1::new(b"key".to_vec(), None, 0, [0; 32]).unwrap();
        let mut encoded = object.encode();
        encoded.push(0);
        assert_eq!(
            ObjectRefV1::decode(&encoded),
            Err(ProtocolError::TrailingBytes {
                context: "ObjectRefV1",
                count: 1
            })
        );
    }

    #[test]
    fn logical_name_is_ascii_and_bounded() {
        let object = ObjectRefV1::new(b"key".to_vec(), None, 0, [0; 32]).unwrap();
        assert!(NamedObjectRefV1::new(b"blocks.bin".to_vec(), object.clone()).is_ok());
        assert!(matches!(
            NamedObjectRefV1::new(b"Blocks/bin".to_vec(), object),
            Err(ProtocolError::InvalidField { .. })
        ));
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

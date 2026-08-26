use thiserror::Error;

use crate::layout::{
    ArchiveId, FORMAT_MAJOR, FileEncoding, ObjectRole, ObjectSpec, UnknownObjectRole,
};

pub const FILE_MAGIC: [u8; 8] = *b"BZIAFILE";
pub const FILE_HEADER_LEN: usize = 64;
pub const KNOWN_FILE_FLAGS: u16 = 0;

/// Common envelope for each headered binary object.
///
/// Exact source objects, such as `sidecars/genesis.bin`, remain unwrapped and
/// are identified and bound by the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub format_major: u16,
    pub schema: u16,
    pub role: ObjectRole,
    pub flags: u16,
    pub record_count: u64,
    pub decoded_bytes: u64,
    pub archive_id: ArchiveId,
    pub payload_bytes: u64,
}

impl FileHeader {
    pub fn new(
        spec: &ObjectSpec,
        archive_id: ArchiveId,
        record_count: u64,
        decoded_bytes: u64,
        payload_bytes: u64,
    ) -> Result<Self, HeaderError> {
        if spec.encoding != FileEncoding::HeaderedBinary {
            return Err(HeaderError::ObjectHasNoHeader(spec.path));
        }
        Ok(Self {
            format_major: FORMAT_MAJOR,
            schema: spec.schema,
            role: spec.role,
            flags: 0,
            record_count,
            decoded_bytes,
            archive_id,
            payload_bytes,
        })
    }

    pub fn encode(self) -> [u8; FILE_HEADER_LEN] {
        let mut output = [0u8; FILE_HEADER_LEN];
        output[0..8].copy_from_slice(&FILE_MAGIC);
        output[8..10].copy_from_slice(&self.format_major.to_le_bytes());
        output[10..12].copy_from_slice(&self.schema.to_le_bytes());
        output[12..14].copy_from_slice(&self.role.code().to_le_bytes());
        output[14..16].copy_from_slice(&self.flags.to_le_bytes());
        output[16..20].copy_from_slice(&(FILE_HEADER_LEN as u32).to_le_bytes());
        // 20..24 is reserved and stays zero.
        output[24..32].copy_from_slice(&self.record_count.to_le_bytes());
        output[32..40].copy_from_slice(&self.decoded_bytes.to_le_bytes());
        output[40..56].copy_from_slice(self.archive_id.as_bytes());
        output[56..64].copy_from_slice(&self.payload_bytes.to_le_bytes());
        output
    }

    pub fn decode(input: &[u8]) -> Result<Self, HeaderError> {
        if input.len() < FILE_HEADER_LEN {
            return Err(HeaderError::Truncated(input.len()));
        }
        if input[0..8] != FILE_MAGIC {
            return Err(HeaderError::WrongMagic);
        }
        let format_major = u16::from_le_bytes(input[8..10].try_into().unwrap());
        if format_major != FORMAT_MAJOR {
            return Err(HeaderError::UnsupportedFormatMajor(format_major));
        }
        let schema = u16::from_le_bytes(input[10..12].try_into().unwrap());
        if schema == 0 {
            return Err(HeaderError::ZeroSchema);
        }
        let role = ObjectRole::try_from(u16::from_le_bytes(input[12..14].try_into().unwrap()))?;
        let flags = u16::from_le_bytes(input[14..16].try_into().unwrap());
        if flags & !KNOWN_FILE_FLAGS != 0 {
            return Err(HeaderError::UnknownFlags(flags & !KNOWN_FILE_FLAGS));
        }
        let header_len = u32::from_le_bytes(input[16..20].try_into().unwrap());
        if header_len != FILE_HEADER_LEN as u32 {
            return Err(HeaderError::WrongHeaderLength(header_len));
        }
        if input[20..24] != [0; 4] {
            return Err(HeaderError::ReservedBytes);
        }

        let record_count = u64::from_le_bytes(input[24..32].try_into().unwrap());
        let decoded_bytes = u64::from_le_bytes(input[32..40].try_into().unwrap());
        let mut archive_id = [0u8; 16];
        archive_id.copy_from_slice(&input[40..56]);
        let payload_bytes = u64::from_le_bytes(input[56..64].try_into().unwrap());

        Ok(Self {
            format_major,
            schema,
            role,
            flags,
            record_count,
            decoded_bytes,
            archive_id: ArchiveId::new(archive_id),
            payload_bytes,
        })
    }

    pub fn validate_for(
        &self,
        spec: &ObjectSpec,
        expected_archive_id: ArchiveId,
        file_bytes: u64,
    ) -> Result<(), HeaderError> {
        if spec.encoding != FileEncoding::HeaderedBinary {
            return Err(HeaderError::ObjectHasNoHeader(spec.path));
        }
        if self.role != spec.role {
            return Err(HeaderError::WrongRole {
                actual: self.role,
                expected: spec.role,
            });
        }
        if self.schema != spec.schema {
            return Err(HeaderError::WrongSchema {
                actual: self.schema,
                expected: spec.schema,
            });
        }
        if self.archive_id != expected_archive_id {
            return Err(HeaderError::WrongArchiveId);
        }
        let expected_file_bytes = (FILE_HEADER_LEN as u64)
            .checked_add(self.payload_bytes)
            .ok_or(HeaderError::FileLengthOverflow)?;
        if file_bytes != expected_file_bytes {
            return Err(HeaderError::WrongFileLength {
                actual: file_bytes,
                expected: expected_file_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HeaderError {
    #[error("archive object {0} does not use the common file header")]
    ObjectHasNoHeader(&'static str),
    #[error("file header has {0} bytes, expected at least {FILE_HEADER_LEN}")]
    Truncated(usize),
    #[error("wrong Blockzilla Index Archive file magic")]
    WrongMagic,
    #[error("unsupported archive format major {0}")]
    UnsupportedFormatMajor(u16),
    #[error("file schema cannot be zero")]
    ZeroSchema,
    #[error(transparent)]
    UnknownRole(#[from] UnknownObjectRole),
    #[error("file header has unknown flags {0:#x}")]
    UnknownFlags(u16),
    #[error("file header length is {0}, expected {FILE_HEADER_LEN}")]
    WrongHeaderLength(u32),
    #[error("file header has non-zero reserved bytes")]
    ReservedBytes,
    #[error("file role is {actual:?}, expected {expected:?}")]
    WrongRole {
        actual: ObjectRole,
        expected: ObjectRole,
    },
    #[error("file schema is {actual}, expected {expected}")]
    WrongSchema { actual: u16, expected: u16 },
    #[error("file belongs to a different archive ID")]
    WrongArchiveId,
    #[error("file byte length overflows u64")]
    FileLengthOverflow,
    #[error("file has {actual} bytes, expected {expected}")]
    WrongFileLength { actual: u64, expected: u64 },
}

#[cfg(test)]
mod tests {
    use crate::{catalog, layout::object_by_path};

    use super::*;

    #[test]
    fn common_header_round_trips_and_binds_role_schema_and_archive() {
        let spec = object_by_path(catalog::blocks::PATH).unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let header = FileHeader::new(spec, archive_id, 12, 400, 200).unwrap();
        let bytes = header.encode();
        let decoded = FileHeader::decode(&bytes).unwrap();
        assert_eq!(decoded, header);
        decoded
            .validate_for(spec, archive_id, FILE_HEADER_LEN as u64 + 200)
            .unwrap();
    }

    #[test]
    fn header_rejects_untagged_and_cross_archive_files() {
        let spec = object_by_path(catalog::blocks::PATH).unwrap();
        let archive_id = ArchiveId::new([7; 16]);
        let mut bytes = FileHeader::new(spec, archive_id, 0, 0, 0).unwrap().encode();
        bytes[0] ^= 0xff;
        assert_eq!(FileHeader::decode(&bytes), Err(HeaderError::WrongMagic));

        let decoded = FileHeader::new(spec, archive_id, 0, 0, 0).unwrap();
        assert_eq!(
            decoded.validate_for(spec, ArchiveId::new([8; 16]), FILE_HEADER_LEN as u64),
            Err(HeaderError::WrongArchiveId)
        );
    }

    #[test]
    fn exact_genesis_bytes_do_not_get_a_header() {
        let spec = crate::layout::object_by_path(crate::sidecars::genesis::PATH).unwrap();
        assert_eq!(
            FileHeader::new(spec, ArchiveId::new([0; 16]), 0, 0, 0),
            Err(HeaderError::ObjectHasNoHeader(spec.path))
        );
    }
}

use sha2::{Digest, Sha256};
use thiserror::Error;

const ELF64_HEADER_LEN: usize = 64;
const ELF_MAGIC: &[u8; 4] = b"\x7fELF";
const ELFCLASS64: u8 = 2;
const ELFDATA2LSB: u8 = 1;
const PN_XNUM: u16 = 0xffff;
const SHT_NOBITS: u32 = 8;

/// Account layout containing an SBPF ELF image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderAccountKind {
    /// `BPFLoader111...` and `BPFLoader211...` keep the ELF at byte zero of the
    /// executable program account.
    Legacy,
    /// An upgradeable-loader Buffer account.  This is useful for speculative
    /// pre-compilation but is not executable until deployment succeeds.
    UpgradeableBuffer,
    /// An upgradeable-loader ProgramData account containing active code.
    UpgradeableProgramData,
    /// A bare ELF file, used by tooling and fixtures.
    BareElf,
}

impl LoaderAccountKind {
    pub const fn elf_offset(self) -> usize {
        match self {
            Self::Legacy | Self::BareElf => 0,
            // Frozen bincode layout sizes from UpgradeableLoaderState.
            Self::UpgradeableBuffer => 37,
            Self::UpgradeableProgramData => 45,
        }
    }
}

/// A canonical ELF extracted from an account's allocated data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedProgram {
    pub loader: LoaderAccountKind,
    pub account_data_len: usize,
    pub elf_offset: usize,
    pub elf: Vec<u8>,
    pub elf_sha256: [u8; 32],
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProgramExtractError {
    #[error("{loader:?} account is only {actual} bytes; ELF starts at byte {required}")]
    AccountTooShort {
        loader: LoaderAccountKind,
        required: usize,
        actual: usize,
    },
    #[error("upgradeable account has state tag {actual}; expected {expected}")]
    WrongUpgradeableState { expected: u32, actual: u32 },
    #[error("input does not start with an ELF file")]
    MissingElfMagic,
    #[error("only ELF64 little-endian program images are supported")]
    UnsupportedElfEncoding,
    #[error("extended ELF header counts are not supported by this POC")]
    ExtendedElfCounts,
    #[error("malformed ELF: {0}")]
    MalformedElf(&'static str),
}

/// Extracts the meaningful ELF bytes and removes account-allocation padding.
///
/// The digest is over the canonical ELF extent, not the whole ProgramData
/// allocation.  Consequently the same deployed code deduplicates even when
/// two programs reserve different upgradeable-loader `max_data_len` values.
pub fn extract_program(
    loader: LoaderAccountKind,
    account_data: &[u8],
) -> Result<ExtractedProgram, ProgramExtractError> {
    let offset = loader.elf_offset();
    if account_data.len() < offset.saturating_add(ELF64_HEADER_LEN) {
        return Err(ProgramExtractError::AccountTooShort {
            loader,
            required: offset.saturating_add(ELF64_HEADER_LEN),
            actual: account_data.len(),
        });
    }

    match loader {
        LoaderAccountKind::UpgradeableBuffer => validate_state_tag(account_data, 1)?,
        LoaderAccountKind::UpgradeableProgramData => validate_state_tag(account_data, 3)?,
        LoaderAccountKind::Legacy | LoaderAccountKind::BareElf => {}
    }

    let allocated_elf = &account_data[offset..];
    let elf_len = canonical_elf_len(allocated_elf)?;
    let elf = allocated_elf[..elf_len].to_vec();
    let elf_sha256 = Sha256::digest(&elf).into();
    Ok(ExtractedProgram {
        loader,
        account_data_len: account_data.len(),
        elf_offset: offset,
        elf,
        elf_sha256,
    })
}

fn validate_state_tag(data: &[u8], expected: u32) -> Result<(), ProgramExtractError> {
    let actual = u32::from_le_bytes(data[..4].try_into().expect("length checked by caller"));
    if actual == expected {
        Ok(())
    } else {
        Err(ProgramExtractError::WrongUpgradeableState { expected, actual })
    }
}

fn canonical_elf_len(elf: &[u8]) -> Result<usize, ProgramExtractError> {
    if elf.get(..4) != Some(ELF_MAGIC) {
        return Err(ProgramExtractError::MissingElfMagic);
    }
    if elf.get(4) != Some(&ELFCLASS64) || elf.get(5) != Some(&ELFDATA2LSB) {
        return Err(ProgramExtractError::UnsupportedElfEncoding);
    }
    if elf.len() < ELF64_HEADER_LEN {
        return Err(ProgramExtractError::MalformedElf("truncated ELF header"));
    }

    let header_len = read_u16(elf, 52)? as usize;
    if header_len < ELF64_HEADER_LEN || header_len > elf.len() {
        return Err(ProgramExtractError::MalformedElf("invalid ELF header size"));
    }

    let program_offset = usize_from_u64(read_u64(elf, 32)?)?;
    let section_offset = usize_from_u64(read_u64(elf, 40)?)?;
    let program_entry_len = read_u16(elf, 54)? as usize;
    let program_count = read_u16(elf, 56)?;
    let section_entry_len = read_u16(elf, 58)? as usize;
    let section_count = read_u16(elf, 60)?;
    if program_count == PN_XNUM || (section_count == 0 && section_offset != 0) {
        return Err(ProgramExtractError::ExtendedElfCounts);
    }

    let mut extent = header_len;
    if program_count != 0 {
        if program_entry_len < 56 {
            return Err(ProgramExtractError::MalformedElf(
                "program header entry is too small",
            ));
        }
        extent = extent.max(table_end(
            program_offset,
            program_entry_len,
            program_count as usize,
            elf.len(),
        )?);
        for index in 0..program_count as usize {
            let base = checked_entry_offset(program_offset, program_entry_len, index, elf.len())?;
            let file_offset = usize_from_u64(read_u64(elf, base + 8)?)?;
            let file_size = usize_from_u64(read_u64(elf, base + 32)?)?;
            extent = extent.max(checked_end(file_offset, file_size, elf.len())?);
        }
    }

    if section_count != 0 {
        if section_entry_len < 64 {
            return Err(ProgramExtractError::MalformedElf(
                "section header entry is too small",
            ));
        }
        extent = extent.max(table_end(
            section_offset,
            section_entry_len,
            section_count as usize,
            elf.len(),
        )?);
        for index in 0..section_count as usize {
            let base = checked_entry_offset(section_offset, section_entry_len, index, elf.len())?;
            let section_type = read_u32(elf, base + 4)?;
            if section_type == SHT_NOBITS {
                continue;
            }
            let file_offset = usize_from_u64(read_u64(elf, base + 24)?)?;
            let file_size = usize_from_u64(read_u64(elf, base + 32)?)?;
            extent = extent.max(checked_end(file_offset, file_size, elf.len())?);
        }
    }

    Ok(extent)
}

fn checked_entry_offset(
    table_offset: usize,
    entry_len: usize,
    index: usize,
    input_len: usize,
) -> Result<usize, ProgramExtractError> {
    let relative = entry_len
        .checked_mul(index)
        .ok_or(ProgramExtractError::MalformedElf("ELF table overflow"))?;
    let offset = table_offset
        .checked_add(relative)
        .ok_or(ProgramExtractError::MalformedElf("ELF table overflow"))?;
    if offset > input_len {
        return Err(ProgramExtractError::MalformedElf("truncated ELF table"));
    }
    Ok(offset)
}

fn table_end(
    offset: usize,
    entry_len: usize,
    count: usize,
    input_len: usize,
) -> Result<usize, ProgramExtractError> {
    let len = entry_len
        .checked_mul(count)
        .ok_or(ProgramExtractError::MalformedElf("ELF table overflow"))?;
    checked_end(offset, len, input_len)
}

fn checked_end(offset: usize, len: usize, input_len: usize) -> Result<usize, ProgramExtractError> {
    let end = offset
        .checked_add(len)
        .ok_or(ProgramExtractError::MalformedElf("ELF range overflow"))?;
    if end > input_len {
        return Err(ProgramExtractError::MalformedElf("ELF range is truncated"));
    }
    Ok(end)
}

fn usize_from_u64(value: u64) -> Result<usize, ProgramExtractError> {
    value
        .try_into()
        .map_err(|_| ProgramExtractError::MalformedElf("ELF offset does not fit usize"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, ProgramExtractError> {
    let value = bytes
        .get(offset..offset.saturating_add(2))
        .ok_or(ProgramExtractError::MalformedElf("truncated u16"))?;
    Ok(u16::from_le_bytes(value.try_into().expect("slice length")))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ProgramExtractError> {
    let value = bytes
        .get(offset..offset.saturating_add(4))
        .ok_or(ProgramExtractError::MalformedElf("truncated u32"))?;
    Ok(u32::from_le_bytes(value.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, ProgramExtractError> {
    let value = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or(ProgramExtractError::MalformedElf("truncated u64"))?;
    Ok(u64::from_le_bytes(value.try_into().expect("slice length")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    fn fixture() -> Vec<u8> {
        STANDARD
            .decode(include_str!("../fixtures/relative_call_sbpfv0.so.b64").trim())
            .unwrap()
    }

    #[test]
    fn extracts_legacy_elf_and_trims_account_padding() {
        let elf = fixture();
        let mut account = elf.clone();
        account.extend_from_slice(&[0; 8192]);
        let extracted = extract_program(LoaderAccountKind::Legacy, &account).unwrap();
        assert_eq!(extracted.elf, elf);
        assert_eq!(extracted.account_data_len, account.len());
        assert_eq!(extracted.elf_offset, 0);
    }

    #[test]
    fn extracts_upgradeable_programdata() {
        let elf = fixture();
        let mut account = vec![0; LoaderAccountKind::UpgradeableProgramData.elf_offset()];
        account[..4].copy_from_slice(&3_u32.to_le_bytes());
        account.extend_from_slice(&elf);
        account.extend_from_slice(&[0; 1024]);
        let extracted =
            extract_program(LoaderAccountKind::UpgradeableProgramData, &account).unwrap();
        assert_eq!(extracted.elf, elf);
        assert_eq!(extracted.elf_offset, 45);
    }

    #[test]
    fn rejects_wrong_upgradeable_state() {
        let mut account = vec![0; 128];
        account[..4].copy_from_slice(&1_u32.to_le_bytes());
        let error = extract_program(LoaderAccountKind::UpgradeableProgramData, &account)
            .expect_err("buffer must not be treated as active ProgramData");
        assert_eq!(
            error,
            ProgramExtractError::WrongUpgradeableState {
                expected: 3,
                actual: 1
            }
        );
    }
}

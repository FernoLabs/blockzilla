use anyhow::{Context, Result, bail, ensure};
use blockzilla_token_transaction_dump::{
    ACCOUNTS_FILE, DUMP_MANIFEST_FILE, PUBKEY_REGISTRY_FILE, TRANSACTIONS_FILE,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::index_format::{IndexFileBinding, parse_hex_digest};

pub const POSTINGS_SCHEMA_VERSION: u16 = 2;
pub const POSTINGS_MANIFEST_FILE: &str = "postings-manifest.json";
pub const TARGET_ADDRESS_DIRECTORY_FILE: &str = "target-address-directory.bin";
pub const TARGET_ADDRESS_POSTINGS_FILE: &str = "target-address-postings.bin";
pub const PROGRAM_DIRECTORY_FILE: &str = "program-directory.bin";
pub const PROGRAM_POSTINGS_FILE: &str = "program-postings.bin";
pub const PROGRAM_DIRECT_DIRECTORY_FILE: &str = "program-direct-directory.bin";
pub const PROGRAM_DIRECT_POSTINGS_FILE: &str = "program-direct-postings.bin";
pub const PROGRAM_INNER_DIRECTORY_FILE: &str = "program-inner-directory.bin";
pub const PROGRAM_INNER_POSTINGS_FILE: &str = "program-inner-postings.bin";

pub const POSTINGS_HEADER_BYTES: usize = 128;
pub const POSTINGS_DIRECTORY_RECORD_BYTES: usize = 24;
pub const POSTINGS_BODY_RECORD_BYTES: usize = 8;
pub const POSTINGS_FLAG_COMPLETE: u16 = 1;

pub const TARGET_ADDRESS_FLAG_MINT: u32 = 1;
pub const TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT: u32 = 2;

pub const TARGET_ADDRESS_SEMANTIC_DOMAIN: &[u8] =
    b"blockzilla-spyx-target-address-postings-semantic-v1\0";
pub const PROGRAM_SEMANTIC_DOMAIN: &[u8] = b"blockzilla-spyx-program-postings-semantic-v2\0";
pub const PROGRAM_DIRECT_SEMANTIC_DOMAIN: &[u8] =
    b"blockzilla-spyx-program-direct-postings-semantic-v2\0";
pub const PROGRAM_INNER_SEMANTIC_DOMAIN: &[u8] =
    b"blockzilla-spyx-program-inner-postings-semantic-v2\0";
pub const OWNER_SEMANTIC_DOMAIN: &[u8] =
    b"blockzilla-spyx-owner-linked-target-postings-semantic-v1\0";

pub const TARGET_ADDRESS_DIRECTORY_MAGIC: [u8; 8] = *b"BZSTAD02";
pub const TARGET_ADDRESS_POSTINGS_MAGIC: [u8; 8] = *b"BZSTAP02";
pub const PROGRAM_DIRECTORY_MAGIC: [u8; 8] = *b"BZSPRD02";
pub const PROGRAM_POSTINGS_MAGIC: [u8; 8] = *b"BZSPRP02";
pub const PROGRAM_DIRECT_DIRECTORY_MAGIC: [u8; 8] = *b"BZSPDD02";
pub const PROGRAM_DIRECT_POSTINGS_MAGIC: [u8; 8] = *b"BZSPDP02";
pub const PROGRAM_INNER_DIRECTORY_MAGIC: [u8; 8] = *b"BZSPID02";
pub const PROGRAM_INNER_POSTINGS_MAGIC: [u8; 8] = *b"BZSPIP02";

pub const PROGRAM_INSTRUCTION_SCOPE_DIRECT: u8 = 1;
pub const PROGRAM_INSTRUCTION_SCOPE_INNER: u8 = 2;
pub const PROGRAM_INSTRUCTION_SCOPE_MASK: u8 =
    PROGRAM_INSTRUCTION_SCOPE_DIRECT | PROGRAM_INSTRUCTION_SCOPE_INNER;

/// The instruction origin selected for a program-postings query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgramInstructionScope {
    #[default]
    All,
    Direct,
    Inner,
}

impl ProgramInstructionScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Direct => "direct",
            Self::Inner => "inner",
        }
    }

    pub const fn required_flag(self) -> u8 {
        match self {
            Self::All => PROGRAM_INSTRUCTION_SCOPE_MASK,
            Self::Direct => PROGRAM_INSTRUCTION_SCOPE_DIRECT,
            Self::Inner => PROGRAM_INSTRUCTION_SCOPE_INNER,
        }
    }

    pub const fn includes(self, instruction_scope_mask: u8) -> bool {
        match self {
            Self::All => instruction_scope_mask & PROGRAM_INSTRUCTION_SCOPE_MASK != 0,
            Self::Direct => instruction_scope_mask & PROGRAM_INSTRUCTION_SCOPE_DIRECT != 0,
            Self::Inner => instruction_scope_mask & PROGRAM_INSTRUCTION_SCOPE_INNER != 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostingsFileKind {
    TargetAddressDirectory,
    TargetAddressPostings,
    ProgramDirectory,
    ProgramPostings,
    ProgramDirectDirectory,
    ProgramDirectPostings,
    ProgramInnerDirectory,
    ProgramInnerPostings,
}

impl PostingsFileKind {
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::TargetAddressDirectory => TARGET_ADDRESS_DIRECTORY_FILE,
            Self::TargetAddressPostings => TARGET_ADDRESS_POSTINGS_FILE,
            Self::ProgramDirectory => PROGRAM_DIRECTORY_FILE,
            Self::ProgramPostings => PROGRAM_POSTINGS_FILE,
            Self::ProgramDirectDirectory => PROGRAM_DIRECT_DIRECTORY_FILE,
            Self::ProgramDirectPostings => PROGRAM_DIRECT_POSTINGS_FILE,
            Self::ProgramInnerDirectory => PROGRAM_INNER_DIRECTORY_FILE,
            Self::ProgramInnerPostings => PROGRAM_INNER_POSTINGS_FILE,
        }
    }

    pub const fn magic(self) -> [u8; 8] {
        match self {
            Self::TargetAddressDirectory => TARGET_ADDRESS_DIRECTORY_MAGIC,
            Self::TargetAddressPostings => TARGET_ADDRESS_POSTINGS_MAGIC,
            Self::ProgramDirectory => PROGRAM_DIRECTORY_MAGIC,
            Self::ProgramPostings => PROGRAM_POSTINGS_MAGIC,
            Self::ProgramDirectDirectory => PROGRAM_DIRECT_DIRECTORY_MAGIC,
            Self::ProgramDirectPostings => PROGRAM_DIRECT_POSTINGS_MAGIC,
            Self::ProgramInnerDirectory => PROGRAM_INNER_DIRECTORY_MAGIC,
            Self::ProgramInnerPostings => PROGRAM_INNER_POSTINGS_MAGIC,
        }
    }

    pub const fn record_bytes(self) -> u16 {
        match self {
            Self::TargetAddressDirectory
            | Self::ProgramDirectory
            | Self::ProgramDirectDirectory
            | Self::ProgramInnerDirectory => POSTINGS_DIRECTORY_RECORD_BYTES as u16,
            Self::TargetAddressPostings
            | Self::ProgramPostings
            | Self::ProgramDirectPostings
            | Self::ProgramInnerPostings => POSTINGS_BODY_RECORD_BYTES as u16,
        }
    }

    pub fn encoded_file_bytes(self, record_count: u64) -> Result<u64> {
        record_count
            .checked_mul(u64::from(self.record_bytes()))
            .and_then(|body| body.checked_add(POSTINGS_HEADER_BYTES as u64))
            .context("postings file byte length overflow")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingsFileHeader {
    pub kind: PostingsFileKind,
    pub complete: bool,
    pub record_count: u64,
    pub source_manifest_sha256: [u8; 32],
    pub source_transaction_sha256: [u8; 32],
}

impl PostingsFileHeader {
    pub fn encode(self) -> [u8; POSTINGS_HEADER_BYTES] {
        let mut bytes = [0u8; POSTINGS_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&self.kind.magic());
        bytes[8..10].copy_from_slice(&POSTINGS_SCHEMA_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(POSTINGS_HEADER_BYTES as u16).to_le_bytes());
        bytes[12..14].copy_from_slice(&self.kind.record_bytes().to_le_bytes());
        let flags = if self.complete {
            POSTINGS_FLAG_COMPLETE
        } else {
            0
        };
        bytes[14..16].copy_from_slice(&flags.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.source_manifest_sha256);
        bytes[56..88].copy_from_slice(&self.source_transaction_sha256);
        bytes
    }

    pub fn decode(bytes: &[u8], expected_kind: PostingsFileKind) -> Result<Self> {
        ensure!(
            bytes.len() >= POSTINGS_HEADER_BYTES,
            "postings file is shorter than its header"
        );
        let header = &bytes[..POSTINGS_HEADER_BYTES];
        ensure!(
            header[0..8] == expected_kind.magic(),
            "postings file magic differs"
        );
        ensure!(
            read_u16(header, 8) == POSTINGS_SCHEMA_VERSION,
            "postings schema version differs"
        );
        ensure!(
            usize::from(read_u16(header, 10)) == POSTINGS_HEADER_BYTES,
            "postings header byte length differs"
        );
        ensure!(
            read_u16(header, 12) == expected_kind.record_bytes(),
            "postings record byte length differs"
        );
        let flags = read_u16(header, 14);
        ensure!(
            flags & !POSTINGS_FLAG_COMPLETE == 0,
            "postings header has unknown flags"
        );
        ensure!(
            header[88..POSTINGS_HEADER_BYTES]
                .iter()
                .all(|byte| *byte == 0),
            "postings header has non-zero reserved bytes"
        );
        Ok(Self {
            kind: expected_kind,
            complete: flags & POSTINGS_FLAG_COMPLETE != 0,
            record_count: read_u64(header, 16),
            source_manifest_sha256: header[24..56]
                .try_into()
                .expect("fixed source manifest digest range"),
            source_transaction_sha256: header[56..88]
                .try_into()
                .expect("fixed source transaction digest range"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostingsDirectoryKind {
    TargetAddress,
    Program,
    Owner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingsDirectoryRecord {
    pub registry_id: u32,
    pub flags: u32,
    pub first_posting_row: u64,
    pub posting_count: u64,
}

impl PostingsDirectoryRecord {
    pub fn encode(
        self,
        kind: PostingsDirectoryKind,
    ) -> Result<[u8; POSTINGS_DIRECTORY_RECORD_BYTES]> {
        self.validate(kind)?;
        let mut bytes = [0u8; POSTINGS_DIRECTORY_RECORD_BYTES];
        bytes[0..4].copy_from_slice(&self.registry_id.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.first_posting_row.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.posting_count.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8], kind: PostingsDirectoryKind) -> Result<Self> {
        ensure!(
            bytes.len() == POSTINGS_DIRECTORY_RECORD_BYTES,
            "postings directory row byte length differs"
        );
        let record = Self {
            registry_id: read_u32(bytes, 0),
            flags: read_u32(bytes, 4),
            first_posting_row: read_u64(bytes, 8),
            posting_count: read_u64(bytes, 16),
        };
        record.validate(kind)?;
        Ok(record)
    }

    pub fn end_posting_row(self) -> Result<u64> {
        self.first_posting_row
            .checked_add(self.posting_count)
            .context("postings directory range overflow")
    }

    fn validate(self, kind: PostingsDirectoryKind) -> Result<()> {
        ensure!(self.registry_id != 0, "postings registry ID is zero");
        match kind {
            PostingsDirectoryKind::TargetAddress => ensure!(
                self.flags == TARGET_ADDRESS_FLAG_MINT
                    || self.flags == TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                "target-address directory flags must select exactly one role"
            ),
            PostingsDirectoryKind::Program | PostingsDirectoryKind::Owner => {
                ensure!(
                    self.flags == 0,
                    "program and owner directory flags must be zero"
                )
            }
        }
        self.end_posting_row()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PostingRecord {
    pub transaction_ordinal: u64,
}

impl PostingRecord {
    pub fn encode(self) -> [u8; POSTINGS_BODY_RECORD_BYTES] {
        self.transaction_ordinal.to_le_bytes()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == POSTINGS_BODY_RECORD_BYTES,
            "posting row byte length differs"
        );
        Ok(Self {
            transaction_ordinal: read_u64(bytes, 0),
        })
    }
}

/// One program posting with its top-level/inner instruction-origin mask.
///
/// The two low bits store the mask. The remaining high bits store the source
/// transaction ordinal. This keeps the fixed eight-byte posting row size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProgramPostingRecord {
    pub transaction_ordinal: u64,
    pub instruction_scope_mask: u8,
}

impl ProgramPostingRecord {
    const TRANSACTION_SHIFT: u32 = 2;

    pub fn encode(self) -> Result<[u8; POSTINGS_BODY_RECORD_BYTES]> {
        self.validate()?;
        let packed = (self.transaction_ordinal << Self::TRANSACTION_SHIFT)
            | u64::from(self.instruction_scope_mask);
        Ok(packed.to_le_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == POSTINGS_BODY_RECORD_BYTES,
            "program posting row byte length differs"
        );
        let packed = u64::from_le_bytes(bytes.try_into().expect("fixed program posting row"));
        let record = Self {
            transaction_ordinal: packed >> Self::TRANSACTION_SHIFT,
            instruction_scope_mask: (packed & u64::from(PROGRAM_INSTRUCTION_SCOPE_MASK)) as u8,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.instruction_scope_mask != 0
                && self.instruction_scope_mask & !PROGRAM_INSTRUCTION_SCOPE_MASK == 0,
            "program posting has an invalid instruction scope mask"
        );
        ensure!(
            self.transaction_ordinal <= (u64::MAX >> Self::TRANSACTION_SHIFT),
            "program posting transaction ordinal exceeds 62 bits"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostingsManifest {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub transactions: u64,
    pub created_unix_seconds: u64,
    pub source: PostingsSourceBinding,
    pub target_address_semantic_sha256: String,
    pub program_semantic_sha256: String,
    pub program_direct_semantic_sha256: String,
    pub program_inner_semantic_sha256: String,
    pub target_address_directory: IndexFileBinding,
    pub target_address_postings: IndexFileBinding,
    pub program_directory: IndexFileBinding,
    pub program_postings: IndexFileBinding,
    pub program_direct_directory: IndexFileBinding,
    pub program_direct_postings: IndexFileBinding,
    pub program_inner_directory: IndexFileBinding,
    pub program_inner_postings: IndexFileBinding,
}

impl PostingsManifest {
    pub const ARTIFACT_KIND: &'static str = "blockzilla_spyx_postings";

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == POSTINGS_SCHEMA_VERSION
                && self.artifact_kind == Self::ARTIFACT_KIND
                && self.transactions != 0
                && self.created_unix_seconds != 0,
            "invalid postings manifest header"
        );
        self.source.validate()?;
        parse_hex_digest(
            &self.target_address_semantic_sha256,
            "target-address semantic digest",
        )?;
        parse_hex_digest(&self.program_semantic_sha256, "program semantic digest")?;
        parse_hex_digest(
            &self.program_direct_semantic_sha256,
            "direct program semantic digest",
        )?;
        parse_hex_digest(
            &self.program_inner_semantic_sha256,
            "inner program semantic digest",
        )?;
        ensure!(
            self.transactions <= self.source.transactions,
            "postings transaction count exceeds its source"
        );
        match (self.complete, self.canary_max_transactions) {
            (true, None) => ensure!(
                self.transactions == self.source.transactions,
                "complete postings do not cover the exact source transaction count"
            ),
            (false, Some(maximum)) => ensure!(
                maximum != 0 && self.transactions == maximum.min(self.source.transactions),
                "incomplete postings have an invalid canary transaction limit"
            ),
            _ => bail!("postings completion markers are inconsistent"),
        }
        for (binding, kind) in [
            (
                &self.target_address_directory,
                PostingsFileKind::TargetAddressDirectory,
            ),
            (
                &self.target_address_postings,
                PostingsFileKind::TargetAddressPostings,
            ),
            (&self.program_directory, PostingsFileKind::ProgramDirectory),
            (&self.program_postings, PostingsFileKind::ProgramPostings),
            (
                &self.program_direct_directory,
                PostingsFileKind::ProgramDirectDirectory,
            ),
            (
                &self.program_direct_postings,
                PostingsFileKind::ProgramDirectPostings,
            ),
            (
                &self.program_inner_directory,
                PostingsFileKind::ProgramInnerDirectory,
            ),
            (
                &self.program_inner_postings,
                PostingsFileKind::ProgramInnerPostings,
            ),
        ] {
            validate_file_binding(binding, kind)?;
        }
        ensure!(
            self.program_direct_directory.records == self.program_directory.records
                && self.program_inner_directory.records == self.program_directory.records
                && self.program_direct_postings.records <= self.program_postings.records
                && self.program_inner_postings.records <= self.program_postings.records
                && self
                    .program_direct_postings
                    .records
                    .checked_add(self.program_inner_postings.records)
                    .is_some_and(|scoped| scoped >= self.program_postings.records),
            "scoped program posting counts are inconsistent with all-scope postings"
        );
        Ok(())
    }

    pub fn binding(&self, kind: PostingsFileKind) -> &IndexFileBinding {
        match kind {
            PostingsFileKind::TargetAddressDirectory => &self.target_address_directory,
            PostingsFileKind::TargetAddressPostings => &self.target_address_postings,
            PostingsFileKind::ProgramDirectory => &self.program_directory,
            PostingsFileKind::ProgramPostings => &self.program_postings,
            PostingsFileKind::ProgramDirectDirectory => &self.program_direct_directory,
            PostingsFileKind::ProgramDirectPostings => &self.program_direct_postings,
            PostingsFileKind::ProgramInnerDirectory => &self.program_inner_directory,
            PostingsFileKind::ProgramInnerPostings => &self.program_inner_postings,
        }
    }

    pub fn validate_header(&self, header: PostingsFileHeader) -> Result<()> {
        self.validate()?;
        let binding = self.binding(header.kind);
        ensure!(
            header.complete == self.complete
                && header.record_count == binding.records
                && header.source_manifest_sha256
                    == parse_hex_digest(&self.source.manifest_sha256, "source manifest digest")?
                && header.source_transaction_sha256
                    == parse_hex_digest(
                        &self.source.transaction_sha256,
                        "source transaction digest",
                    )?,
            "postings header differs from its manifest binding"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostingsSourceBinding {
    pub manifest_file: String,
    pub manifest_bytes: u64,
    pub manifest_sha256: String,
    pub transaction_file: String,
    pub transaction_bytes: u64,
    pub transaction_sha256: String,
    pub registry_file: String,
    pub registry_bytes: u64,
    pub registry_sha256: String,
    pub accounts_file: String,
    pub accounts_bytes: u64,
    pub accounts_sha256: String,
    pub transactions: u64,
    pub pubkeys: u64,
    pub accounts: u64,
}

impl PostingsSourceBinding {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.manifest_file == DUMP_MANIFEST_FILE
                && self.transaction_file == TRANSACTIONS_FILE
                && self.registry_file == PUBKEY_REGISTRY_FILE
                && self.accounts_file == ACCOUNTS_FILE,
            "postings source file names differ from the schema-3 dump"
        );
        ensure!(
            self.manifest_bytes != 0
                && self.transaction_bytes != 0
                && self.registry_bytes != 0
                && self.accounts_bytes != 0
                && self.transactions != 0
                && self.pubkeys != 0
                && self.accounts <= self.pubkeys,
            "postings source sizes or counts are invalid"
        );
        for (digest, label) in [
            (&self.manifest_sha256, "source manifest digest"),
            (&self.transaction_sha256, "source transaction digest"),
            (&self.registry_sha256, "source registry digest"),
            (&self.accounts_sha256, "source accounts digest"),
        ] {
            parse_hex_digest(digest, label)?;
        }
        Ok(())
    }
}

pub fn validate_postings_data(
    kind: PostingsDirectoryKind,
    directory: &[PostingsDirectoryRecord],
    postings: &[PostingRecord],
    source_transaction_count: u64,
) -> Result<()> {
    let posting_rows = u64::try_from(postings.len()).context("posting row count exceeds u64")?;
    let mut expected_first_row = 0u64;
    let mut previous_registry_id = None;
    for record in directory {
        record.validate(kind)?;
        ensure!(
            previous_registry_id.is_none_or(|previous| previous < record.registry_id),
            "postings directory registry IDs are not strictly sorted"
        );
        ensure!(
            record.first_posting_row == expected_first_row,
            "postings directory ranges are not contiguous"
        );
        let end = record.end_posting_row()?;
        ensure!(
            end <= posting_rows,
            "postings directory range exceeds its body"
        );
        let first = usize::try_from(record.first_posting_row)
            .context("posting range start exceeds usize")?;
        let end = usize::try_from(end).context("posting range end exceeds usize")?;
        let range = &postings[first..end];
        ensure!(
            range
                .iter()
                .all(|posting| posting.transaction_ordinal < source_transaction_count),
            "posting transaction ordinal is outside the source transaction range"
        );
        ensure!(
            range.windows(2).all(|pair| pair[0] < pair[1]),
            "one postings range is not strictly sorted and unique"
        );
        expected_first_row = u64::try_from(end).expect("validated posting end fits u64");
        previous_registry_id = Some(record.registry_id);
    }
    ensure!(
        expected_first_row == posting_rows,
        "postings directory does not cover its body exactly"
    );
    Ok(())
}

pub fn validate_program_postings_data(
    scope: ProgramInstructionScope,
    directory: &[PostingsDirectoryRecord],
    postings: &[ProgramPostingRecord],
    source_transaction_count: u64,
) -> Result<()> {
    let posting_rows =
        u64::try_from(postings.len()).context("program posting row count exceeds u64")?;
    let mut expected_first_row = 0u64;
    let mut previous_registry_id = None;
    for record in directory {
        record.validate(PostingsDirectoryKind::Program)?;
        ensure!(
            previous_registry_id.is_none_or(|previous| previous < record.registry_id),
            "program postings directory registry IDs are not strictly sorted"
        );
        ensure!(
            record.first_posting_row == expected_first_row,
            "program postings directory ranges are not contiguous"
        );
        let end = record.end_posting_row()?;
        ensure!(
            end <= posting_rows,
            "program postings directory range exceeds its body"
        );
        let first = usize::try_from(record.first_posting_row)
            .context("program posting range start exceeds usize")?;
        let end = usize::try_from(end).context("program posting range end exceeds usize")?;
        let range = &postings[first..end];
        ensure!(
            range.iter().all(|posting| {
                posting.transaction_ordinal < source_transaction_count
                    && scope.includes(posting.instruction_scope_mask)
            }),
            "program posting is outside the source range or selected instruction scope"
        );
        ensure!(
            range
                .windows(2)
                .all(|pair| pair[0].transaction_ordinal < pair[1].transaction_ordinal),
            "one program postings range is not strictly sorted and unique"
        );
        expected_first_row = u64::try_from(end).expect("validated program posting end fits u64");
        previous_registry_id = Some(record.registry_id);
    }
    ensure!(
        expected_first_row == posting_rows,
        "program postings directory does not cover its body exactly"
    );
    Ok(())
}

/// A constant-memory semantic digest builder for canonical posting tuples.
pub struct PostingsSemanticHasher {
    kind: PostingsDirectoryKind,
    hasher: Sha256,
    declared_item_count: u64,
    observed_item_count: u64,
    previous: Option<(u32, u32, u64)>,
}

impl PostingsSemanticHasher {
    pub fn new(kind: PostingsDirectoryKind, declared_item_count: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(match kind {
            PostingsDirectoryKind::TargetAddress => TARGET_ADDRESS_SEMANTIC_DOMAIN,
            PostingsDirectoryKind::Program => PROGRAM_SEMANTIC_DOMAIN,
            PostingsDirectoryKind::Owner => OWNER_SEMANTIC_DOMAIN,
        });
        hasher.update(declared_item_count.to_le_bytes());
        Self {
            kind,
            hasher,
            declared_item_count,
            observed_item_count: 0,
            previous: None,
        }
    }

    pub fn update(&mut self, registry_id: u32, flags: u32, transaction_ordinal: u64) -> Result<()> {
        ensure!(registry_id != 0, "semantic posting registry ID is zero");
        match self.kind {
            PostingsDirectoryKind::TargetAddress => ensure!(
                flags == TARGET_ADDRESS_FLAG_MINT || flags == TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                "semantic target-address flags must select exactly one role"
            ),
            PostingsDirectoryKind::Program | PostingsDirectoryKind::Owner => {
                ensure!(flags == 0, "semantic program and owner flags must be zero")
            }
        }
        ensure!(
            self.observed_item_count < self.declared_item_count,
            "semantic posting count exceeds its declaration"
        );
        if let Some((previous_registry_id, previous_flags, previous_transaction_ordinal)) =
            self.previous
        {
            ensure!(
                registry_id > previous_registry_id
                    || (registry_id == previous_registry_id
                        && flags == previous_flags
                        && transaction_ordinal > previous_transaction_ordinal),
                "semantic posting tuples are not in strict numeric canonical order"
            );
        }
        self.hasher.update(registry_id.to_le_bytes());
        self.hasher.update(flags.to_le_bytes());
        self.hasher.update(transaction_ordinal.to_le_bytes());
        self.observed_item_count += 1;
        self.previous = Some((registry_id, flags, transaction_ordinal));
        Ok(())
    }

    pub fn finish(self) -> Result<[u8; 32]> {
        ensure!(
            self.observed_item_count == self.declared_item_count,
            "semantic posting count differs from its declaration"
        );
        Ok(self.hasher.finalize().into())
    }
}

/// A constant-memory semantic digest builder for scoped program postings.
pub struct ProgramPostingsSemanticHasher {
    scope: ProgramInstructionScope,
    hasher: Sha256,
    declared_item_count: u64,
    observed_item_count: u64,
    previous: Option<(u32, u64)>,
}

impl ProgramPostingsSemanticHasher {
    pub fn new(scope: ProgramInstructionScope, declared_item_count: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(match scope {
            ProgramInstructionScope::All => PROGRAM_SEMANTIC_DOMAIN,
            ProgramInstructionScope::Direct => PROGRAM_DIRECT_SEMANTIC_DOMAIN,
            ProgramInstructionScope::Inner => PROGRAM_INNER_SEMANTIC_DOMAIN,
        });
        hasher.update(declared_item_count.to_le_bytes());
        Self {
            scope,
            hasher,
            declared_item_count,
            observed_item_count: 0,
            previous: None,
        }
    }

    pub fn update(
        &mut self,
        registry_id: u32,
        instruction_scope_mask: u8,
        transaction_ordinal: u64,
    ) -> Result<()> {
        ensure!(registry_id != 0, "semantic program registry ID is zero");
        ensure!(
            instruction_scope_mask != 0
                && instruction_scope_mask & !PROGRAM_INSTRUCTION_SCOPE_MASK == 0
                && self.scope.includes(instruction_scope_mask),
            "semantic program posting does not match its instruction scope"
        );
        ensure!(
            self.observed_item_count < self.declared_item_count,
            "semantic program posting count exceeds its declaration"
        );
        if let Some((previous_registry_id, previous_transaction_ordinal)) = self.previous {
            ensure!(
                registry_id > previous_registry_id
                    || (registry_id == previous_registry_id
                        && transaction_ordinal > previous_transaction_ordinal),
                "semantic program postings are not in strict canonical order"
            );
        }
        self.hasher.update(registry_id.to_le_bytes());
        self.hasher
            .update(u32::from(instruction_scope_mask).to_le_bytes());
        self.hasher.update(transaction_ordinal.to_le_bytes());
        self.observed_item_count += 1;
        self.previous = Some((registry_id, transaction_ordinal));
        Ok(())
    }

    pub fn finish(self) -> Result<[u8; 32]> {
        ensure!(
            self.observed_item_count == self.declared_item_count,
            "semantic program posting count differs from its declaration"
        );
        Ok(self.hasher.finalize().into())
    }
}

pub fn program_postings_semantic_sha256(
    scope: ProgramInstructionScope,
    directory: &[PostingsDirectoryRecord],
    postings: &[ProgramPostingRecord],
    source_transaction_count: u64,
) -> Result<[u8; 32]> {
    validate_program_postings_data(scope, directory, postings, source_transaction_count)?;
    let item_count =
        u64::try_from(postings.len()).context("semantic program item count exceeds u64")?;
    let mut hasher = ProgramPostingsSemanticHasher::new(scope, item_count);
    for record in directory {
        let first = usize::try_from(record.first_posting_row)
            .context("semantic program posting range start exceeds usize")?;
        let end = usize::try_from(record.end_posting_row()?)
            .context("semantic program posting range end exceeds usize")?;
        for posting in &postings[first..end] {
            hasher.update(
                record.registry_id,
                posting.instruction_scope_mask,
                posting.transaction_ordinal,
            )?;
        }
    }
    hasher.finish()
}

/// Returns the semantic digest for one validated postings pair.
///
/// The digest is SHA-256 over the kind-specific domain above, the total item
/// count as a little-endian `u64`, and one canonical 16-byte tuple per posting:
/// `(registry_id: u32 LE, flags: u32 LE, transaction_ordinal: u64 LE)`. Tuples
/// are in ascending numeric `(registry_id, flags, transaction_ordinal)` order.
/// The validated directory and body already have this canonical order. Thus,
/// the digest depends on the semantic multiset, including duplicate count, and
/// not on directory offsets or input production order.
pub fn postings_semantic_sha256(
    kind: PostingsDirectoryKind,
    directory: &[PostingsDirectoryRecord],
    postings: &[PostingRecord],
    source_transaction_count: u64,
) -> Result<[u8; 32]> {
    validate_postings_data(kind, directory, postings, source_transaction_count)?;
    let item_count = u64::try_from(postings.len()).context("semantic item count exceeds u64")?;
    let mut hasher = PostingsSemanticHasher::new(kind, item_count);
    for record in directory {
        let first = usize::try_from(record.first_posting_row)
            .context("semantic posting range start exceeds usize")?;
        let end = usize::try_from(record.end_posting_row()?)
            .context("semantic posting range end exceeds usize")?;
        for posting in &postings[first..end] {
            hasher.update(
                record.registry_id,
                record.flags,
                posting.transaction_ordinal,
            )?;
        }
    }
    hasher.finish()
}

fn validate_file_binding(binding: &IndexFileBinding, kind: PostingsFileKind) -> Result<()> {
    ensure!(
        binding.file == kind.file_name()
            && binding.record_bytes == kind.record_bytes()
            && binding.bytes == kind.encoded_file_bytes(binding.records)?,
        "postings file binding differs from its fixed format"
    );
    parse_hex_digest(&binding.sha256, "postings file digest")?;
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 byte range"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 byte range"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 byte range"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST_DIGEST: [u8; 32] = [3; 32];
    const TRANSACTION_DIGEST: [u8; 32] = [5; 32];
    const DIGEST_HEX: &str = "0707070707070707070707070707070707070707070707070707070707070707";

    #[test]
    fn all_headers_round_trip_with_distinct_magic_and_zero_reserved_bytes() {
        let kinds = [
            PostingsFileKind::TargetAddressDirectory,
            PostingsFileKind::TargetAddressPostings,
            PostingsFileKind::ProgramDirectory,
            PostingsFileKind::ProgramPostings,
            PostingsFileKind::ProgramDirectDirectory,
            PostingsFileKind::ProgramDirectPostings,
            PostingsFileKind::ProgramInnerDirectory,
            PostingsFileKind::ProgramInnerPostings,
        ];
        for kind in kinds {
            let header = PostingsFileHeader {
                kind,
                complete: true,
                record_count: 42,
                source_manifest_sha256: MANIFEST_DIGEST,
                source_transaction_sha256: TRANSACTION_DIGEST,
            };
            let encoded = header.encode();
            assert_eq!(&encoded[0..8], &kind.magic());
            assert!(encoded[88..].iter().all(|byte| *byte == 0));
            assert_eq!(PostingsFileHeader::decode(&encoded, kind).unwrap(), header);
            for other in kinds {
                if other != kind {
                    assert!(PostingsFileHeader::decode(&encoded, other).is_err());
                }
            }
        }
    }

    #[test]
    fn header_rejects_flags_magic_and_reserved_bytes() {
        let header = PostingsFileHeader {
            kind: PostingsFileKind::ProgramPostings,
            complete: true,
            record_count: 2,
            source_manifest_sha256: MANIFEST_DIGEST,
            source_transaction_sha256: TRANSACTION_DIGEST,
        };
        let mut bytes = header.encode();
        bytes[0] ^= 1;
        assert!(PostingsFileHeader::decode(&bytes, header.kind).is_err());

        let mut bytes = header.encode();
        bytes[14..16].copy_from_slice(&2u16.to_le_bytes());
        assert!(PostingsFileHeader::decode(&bytes, header.kind).is_err());

        let mut bytes = header.encode();
        bytes[14..16].copy_from_slice(&0u16.to_le_bytes());
        assert!(
            !PostingsFileHeader::decode(&bytes, header.kind)
                .unwrap()
                .complete
        );

        let mut bytes = header.encode();
        bytes[127] = 1;
        assert!(PostingsFileHeader::decode(&bytes, header.kind).is_err());
    }

    #[test]
    fn directory_and_posting_rows_round_trip_and_validate_flags() {
        for flags in [TARGET_ADDRESS_FLAG_MINT, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT] {
            let row = PostingsDirectoryRecord {
                registry_id: 9,
                flags,
                first_posting_row: 10,
                posting_count: 2,
            };
            assert_eq!(
                PostingsDirectoryRecord::decode(
                    &row.encode(PostingsDirectoryKind::TargetAddress).unwrap(),
                    PostingsDirectoryKind::TargetAddress,
                )
                .unwrap(),
                row
            );
        }
        let program = PostingsDirectoryRecord {
            registry_id: 11,
            flags: 0,
            first_posting_row: 0,
            posting_count: 1,
        };
        assert_eq!(
            PostingsDirectoryRecord::decode(
                &program.encode(PostingsDirectoryKind::Program).unwrap(),
                PostingsDirectoryKind::Program,
            )
            .unwrap(),
            program
        );
        for flags in [0, 3, 4] {
            assert!(
                PostingsDirectoryRecord { flags, ..program }
                    .encode(PostingsDirectoryKind::TargetAddress)
                    .is_err()
            );
        }
        assert!(
            PostingsDirectoryRecord {
                flags: TARGET_ADDRESS_FLAG_MINT,
                ..program
            }
            .encode(PostingsDirectoryKind::Program)
            .is_err()
        );
        assert!(
            PostingsDirectoryRecord {
                registry_id: 0,
                ..program
            }
            .encode(PostingsDirectoryKind::Program)
            .is_err()
        );

        let posting = PostingRecord {
            transaction_ordinal: 8,
        };
        assert_eq!(PostingRecord::decode(&posting.encode()).unwrap(), posting);
        assert!(PostingRecord::decode(&[0; 7]).is_err());

        for instruction_scope_mask in [
            PROGRAM_INSTRUCTION_SCOPE_DIRECT,
            PROGRAM_INSTRUCTION_SCOPE_INNER,
            PROGRAM_INSTRUCTION_SCOPE_MASK,
        ] {
            let program_posting = ProgramPostingRecord {
                transaction_ordinal: 8,
                instruction_scope_mask,
            };
            assert_eq!(
                ProgramPostingRecord::decode(&program_posting.encode().unwrap()).unwrap(),
                program_posting
            );
        }
        assert!(
            ProgramPostingRecord {
                transaction_ordinal: 8,
                instruction_scope_mask: 0,
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn range_and_file_size_overflows_fail_closed() {
        assert!(
            PostingsDirectoryRecord {
                registry_id: 1,
                flags: 0,
                first_posting_row: u64::MAX,
                posting_count: 1,
            }
            .encode(PostingsDirectoryKind::Program)
            .is_err()
        );
        assert!(
            PostingsFileKind::ProgramPostings
                .encoded_file_bytes(u64::MAX)
                .is_err()
        );
        assert_eq!(
            PostingsFileKind::ProgramDirectory
                .encoded_file_bytes(2)
                .unwrap(),
            176
        );
    }

    #[test]
    fn complete_data_validation_checks_ranges_order_and_transaction_bounds() {
        let directory = [
            PostingsDirectoryRecord {
                registry_id: 2,
                flags: 0,
                first_posting_row: 0,
                posting_count: 2,
            },
            PostingsDirectoryRecord {
                registry_id: 7,
                flags: 0,
                first_posting_row: 2,
                posting_count: 1,
            },
        ];
        let postings = [
            PostingRecord {
                transaction_ordinal: 1,
            },
            PostingRecord {
                transaction_ordinal: 3,
            },
            PostingRecord {
                transaction_ordinal: 2,
            },
        ];
        validate_postings_data(PostingsDirectoryKind::Program, &directory, &postings, 4).unwrap();

        let mut invalid = postings;
        invalid[1].transaction_ordinal = 1;
        assert!(
            validate_postings_data(PostingsDirectoryKind::Program, &directory, &invalid, 4)
                .is_err()
        );
        let mut invalid = postings;
        invalid[2].transaction_ordinal = 4;
        assert!(
            validate_postings_data(PostingsDirectoryKind::Program, &directory, &invalid, 4)
                .is_err()
        );
        let mut invalid_directory = directory;
        invalid_directory[1].first_posting_row = 1;
        assert!(
            validate_postings_data(
                PostingsDirectoryKind::Program,
                &invalid_directory,
                &postings,
                4,
            )
            .is_err()
        );
    }

    #[test]
    fn semantic_digest_is_canonical_domain_separated_and_change_sensitive() {
        let target_directory = [
            PostingsDirectoryRecord {
                registry_id: 2,
                flags: TARGET_ADDRESS_FLAG_MINT,
                first_posting_row: 0,
                posting_count: 2,
            },
            PostingsDirectoryRecord {
                registry_id: 7,
                flags: TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT,
                first_posting_row: 2,
                posting_count: 1,
            },
        ];
        let postings = [
            PostingRecord {
                transaction_ordinal: 1,
            },
            PostingRecord {
                transaction_ordinal: 3,
            },
            PostingRecord {
                transaction_ordinal: 2,
            },
        ];
        let tuples = vec![
            (2, TARGET_ADDRESS_FLAG_MINT, 1),
            (2, TARGET_ADDRESS_FLAG_MINT, 3),
            (7, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT, 2),
        ];
        let target_digest = postings_semantic_sha256(
            PostingsDirectoryKind::TargetAddress,
            &target_directory,
            &postings,
            4,
        )
        .unwrap();
        assert_eq!(
            digest_hex(target_digest),
            "e34bb79288ed9610a30407eba49c8d8000c1a58dc344623accb795d563ad4b98"
        );
        assert_eq!(
            target_digest,
            reference_semantic_digest(PostingsDirectoryKind::TargetAddress, tuples.clone())
        );
        assert_eq!(
            target_digest,
            reference_semantic_digest(
                PostingsDirectoryKind::TargetAddress,
                vec![tuples[2], tuples[0], tuples[1]],
            )
        );

        let program_directory = [
            PostingsDirectoryRecord {
                flags: 0,
                ..target_directory[0]
            },
            PostingsDirectoryRecord {
                flags: 0,
                ..target_directory[1]
            },
        ];
        let program_digest = postings_semantic_sha256(
            PostingsDirectoryKind::Program,
            &program_directory,
            &postings,
            4,
        )
        .unwrap();
        assert_ne!(target_digest, program_digest);

        let mut changed = tuples.clone();
        changed[1].2 = 2;
        assert_ne!(
            target_digest,
            reference_semantic_digest(PostingsDirectoryKind::TargetAddress, changed)
        );
        let mut duplicate = tuples;
        duplicate.push(duplicate[0]);
        assert_ne!(
            target_digest,
            reference_semantic_digest(PostingsDirectoryKind::TargetAddress, duplicate)
        );
    }

    #[test]
    fn streaming_semantic_hasher_enforces_flags_order_and_declared_count() {
        let mut hasher = PostingsSemanticHasher::new(PostingsDirectoryKind::TargetAddress, 3);
        hasher.update(2, TARGET_ADDRESS_FLAG_MINT, 1).unwrap();
        hasher.update(2, TARGET_ADDRESS_FLAG_MINT, 3).unwrap();
        hasher
            .update(7, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT, 2)
            .unwrap();
        assert_eq!(
            digest_hex(hasher.finish().unwrap()),
            "e34bb79288ed9610a30407eba49c8d8000c1a58dc344623accb795d563ad4b98"
        );

        let mut invalid = PostingsSemanticHasher::new(PostingsDirectoryKind::TargetAddress, 2);
        assert!(invalid.update(2, 3, 1).is_err());
        invalid.update(2, TARGET_ADDRESS_FLAG_MINT, 3).unwrap();
        assert!(invalid.update(2, TARGET_ADDRESS_FLAG_MINT, 2).is_err());
        assert!(
            invalid
                .update(2, TARGET_ADDRESS_FLAG_TOKEN_ACCOUNT, 4)
                .is_err()
        );
        assert!(invalid.finish().is_err());

        let mut invalid = PostingsSemanticHasher::new(PostingsDirectoryKind::Program, 2);
        invalid.update(3, 0, 1).unwrap();
        assert!(invalid.update(2, 0, 2).is_err());
        assert!(invalid.finish().is_err());

        let mut invalid = PostingsSemanticHasher::new(PostingsDirectoryKind::Program, 1);
        assert!(invalid.update(0, 0, 1).is_err());
        assert!(invalid.update(2, TARGET_ADDRESS_FLAG_MINT, 1).is_err());
        invalid.update(2, 0, 1).unwrap();
        assert!(invalid.update(3, 0, 1).is_err());
        invalid.finish().unwrap();

        PostingsSemanticHasher::new(PostingsDirectoryKind::Program, 0)
            .finish()
            .unwrap();
    }

    #[test]
    fn manifest_round_trip_denies_unknown_fields_and_checks_file_sizes() {
        let manifest = fixture_manifest();
        manifest.validate().unwrap();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let decoded: PostingsManifest = serde_json::from_slice(&bytes).unwrap();
        decoded.validate().unwrap();

        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<PostingsManifest>(value).is_err());

        let mut invalid = fixture_manifest();
        invalid.program_postings.bytes += 1;
        assert!(invalid.validate().is_err());

        let mut invalid = fixture_manifest();
        invalid.target_address_semantic_sha256 = "A".repeat(64);
        assert!(invalid.validate().is_err());

        let mut empty_files = fixture_manifest();
        empty_files.target_address_directory = binding(PostingsFileKind::TargetAddressDirectory, 0);
        empty_files.target_address_postings = binding(PostingsFileKind::TargetAddressPostings, 0);
        empty_files.program_directory = binding(PostingsFileKind::ProgramDirectory, 0);
        empty_files.program_postings = binding(PostingsFileKind::ProgramPostings, 0);
        empty_files.program_direct_directory = binding(PostingsFileKind::ProgramDirectDirectory, 0);
        empty_files.program_direct_postings = binding(PostingsFileKind::ProgramDirectPostings, 0);
        empty_files.program_inner_directory = binding(PostingsFileKind::ProgramInnerDirectory, 0);
        empty_files.program_inner_postings = binding(PostingsFileKind::ProgramInnerPostings, 0);
        empty_files.validate().unwrap();

        let mut no_discovered_accounts = fixture_manifest();
        no_discovered_accounts.source.accounts = 0;
        no_discovered_accounts.validate().unwrap();

        let header = PostingsFileHeader {
            kind: PostingsFileKind::ProgramPostings,
            complete: true,
            record_count: invalid.program_postings.records,
            source_manifest_sha256: MANIFEST_DIGEST,
            source_transaction_sha256: TRANSACTION_DIGEST,
        };
        assert!(fixture_manifest().validate_header(header).is_ok());
    }

    #[test]
    fn manifest_and_headers_require_consistent_completion_markers() {
        let mut invalid_full = fixture_manifest();
        invalid_full.transactions = 9;
        assert!(invalid_full.validate().is_err());

        let mut invalid_full = fixture_manifest();
        invalid_full.canary_max_transactions = Some(4);
        assert!(invalid_full.validate().is_err());

        let mut canary = fixture_manifest();
        canary.complete = false;
        canary.canary_max_transactions = Some(4);
        canary.transactions = 4;
        canary.validate().unwrap();

        let incomplete_header = PostingsFileHeader {
            kind: PostingsFileKind::TargetAddressPostings,
            complete: false,
            record_count: canary.target_address_postings.records,
            source_manifest_sha256: MANIFEST_DIGEST,
            source_transaction_sha256: TRANSACTION_DIGEST,
        };
        canary.validate_header(incomplete_header).unwrap();
        assert!(
            canary
                .validate_header(PostingsFileHeader {
                    complete: true,
                    ..incomplete_header
                })
                .is_err()
        );

        canary.canary_max_transactions = None;
        assert!(canary.validate().is_err());
        canary.canary_max_transactions = Some(0);
        assert!(canary.validate().is_err());
    }

    fn fixture_manifest() -> PostingsManifest {
        PostingsManifest {
            schema_version: POSTINGS_SCHEMA_VERSION,
            artifact_kind: PostingsManifest::ARTIFACT_KIND.to_owned(),
            complete: true,
            canary_max_transactions: None,
            transactions: 10,
            created_unix_seconds: 1,
            source: PostingsSourceBinding {
                manifest_file: DUMP_MANIFEST_FILE.to_owned(),
                manifest_bytes: 100,
                manifest_sha256: digest_hex(MANIFEST_DIGEST),
                transaction_file: TRANSACTIONS_FILE.to_owned(),
                transaction_bytes: 200,
                transaction_sha256: digest_hex(TRANSACTION_DIGEST),
                registry_file: PUBKEY_REGISTRY_FILE.to_owned(),
                registry_bytes: 96,
                registry_sha256: DIGEST_HEX.to_owned(),
                accounts_file: ACCOUNTS_FILE.to_owned(),
                accounts_bytes: 50,
                accounts_sha256: DIGEST_HEX.to_owned(),
                transactions: 10,
                pubkeys: 3,
                accounts: 2,
            },
            target_address_semantic_sha256: DIGEST_HEX.to_owned(),
            program_semantic_sha256: DIGEST_HEX.to_owned(),
            program_direct_semantic_sha256: DIGEST_HEX.to_owned(),
            program_inner_semantic_sha256: DIGEST_HEX.to_owned(),
            target_address_directory: binding(PostingsFileKind::TargetAddressDirectory, 3),
            target_address_postings: binding(PostingsFileKind::TargetAddressPostings, 5),
            program_directory: binding(PostingsFileKind::ProgramDirectory, 2),
            program_postings: binding(PostingsFileKind::ProgramPostings, 4),
            program_direct_directory: binding(PostingsFileKind::ProgramDirectDirectory, 2),
            program_direct_postings: binding(PostingsFileKind::ProgramDirectPostings, 3),
            program_inner_directory: binding(PostingsFileKind::ProgramInnerDirectory, 2),
            program_inner_postings: binding(PostingsFileKind::ProgramInnerPostings, 2),
        }
    }

    fn binding(kind: PostingsFileKind, records: u64) -> IndexFileBinding {
        IndexFileBinding {
            file: kind.file_name().to_owned(),
            bytes: kind.encoded_file_bytes(records).unwrap(),
            sha256: DIGEST_HEX.to_owned(),
            records,
            record_bytes: kind.record_bytes(),
        }
    }

    fn digest_hex(bytes: [u8; 32]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::with_capacity(64);
        for byte in bytes {
            value.push(char::from(HEX[usize::from(byte >> 4)]));
            value.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        value
    }

    fn reference_semantic_digest(
        kind: PostingsDirectoryKind,
        mut tuples: Vec<(u32, u32, u64)>,
    ) -> [u8; 32] {
        tuples.sort_unstable();
        let mut hasher = Sha256::new();
        hasher.update(match kind {
            PostingsDirectoryKind::TargetAddress => TARGET_ADDRESS_SEMANTIC_DOMAIN,
            PostingsDirectoryKind::Program => PROGRAM_SEMANTIC_DOMAIN,
            PostingsDirectoryKind::Owner => OWNER_SEMANTIC_DOMAIN,
        });
        hasher.update(
            u64::try_from(tuples.len())
                .expect("test tuple count fits u64")
                .to_le_bytes(),
        );
        for (registry_id, flags, transaction_ordinal) in tuples {
            hasher.update(registry_id.to_le_bytes());
            hasher.update(flags.to_le_bytes());
            hasher.update(transaction_ordinal.to_le_bytes());
        }
        hasher.finalize().into()
    }
}

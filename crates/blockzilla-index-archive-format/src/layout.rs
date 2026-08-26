use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{catalog, dictionary, indexes, ledger, runtime, sidecars};

pub const FORMAT_ID: &str = "blockzilla-index-archive";
pub const FORMAT_MAJOR: u16 = 1;
pub const MANIFEST_SCHEMA: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ArchiveId([u8; 16]);

impl ArchiveId {
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(32);
        for byte in self.0 {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }

    pub fn from_hex(value: &str) -> Result<Self, ArchiveIdError> {
        if value.len() != 32 {
            return Err(ArchiveIdError::Length(value.len()));
        }
        let mut bytes = [0u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
        }
        Ok(Self(bytes))
    }
}

fn hex_nibble(byte: u8) -> Result<u8, ArchiveIdError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ArchiveIdError::Character(byte as char)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArchiveIdError {
    #[error("archive ID has {0} hexadecimal characters, expected 32")]
    Length(usize),
    #[error("archive ID contains invalid lowercase hexadecimal character {0:?}")]
    Character(char),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[repr(u16)]
pub enum ObjectRole {
    CatalogBlocks = 1,
    DictionaryPubkeys = 3,
    DictionaryBlockhashes = 5,
    LedgerTransactions = 6,
    RuntimeOutcomes = 11,
    RuntimeBalances = 12,
    RuntimeTokenBalances = 13,
    RuntimeLogs = 14,
    RuntimeRewards = 16,
    IndexSlots = 17,
    IndexAccounts = 19,
    IndexPrograms = 20,
    IndexSelectors = 21,
    SidecarSignatures = 22,
    SidecarPoh = 23,
    SidecarShredding = 24,
    SidecarGenesis = 26,
    RuntimeInnerInstructions = 27,
    RuntimeBlockRewards = 28,
    DictionaryAccountFlags = 30,
}

impl ObjectRole {
    pub const ALL: [Self; 20] = [
        Self::CatalogBlocks,
        Self::DictionaryPubkeys,
        Self::DictionaryBlockhashes,
        Self::LedgerTransactions,
        Self::RuntimeOutcomes,
        Self::RuntimeBalances,
        Self::RuntimeTokenBalances,
        Self::RuntimeLogs,
        Self::RuntimeRewards,
        Self::IndexSlots,
        Self::IndexAccounts,
        Self::IndexPrograms,
        Self::IndexSelectors,
        Self::SidecarSignatures,
        Self::SidecarPoh,
        Self::SidecarShredding,
        Self::SidecarGenesis,
        Self::RuntimeInnerInstructions,
        Self::RuntimeBlockRewards,
        Self::DictionaryAccountFlags,
    ];

    pub const fn code(self) -> u16 {
        self as u16
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogBlocks => "catalog-blocks",
            Self::DictionaryPubkeys => "dictionary-pubkeys",
            Self::LedgerTransactions => "ledger-transactions",
            Self::RuntimeOutcomes => "effect-outcomes",
            Self::RuntimeBalances => "effect-balances",
            Self::RuntimeTokenBalances => "effect-token-balances",
            Self::RuntimeLogs => "effect-logs",
            Self::RuntimeRewards => "effect-rewards",
            Self::IndexSlots => "index-slots",
            Self::IndexAccounts => "index-accounts",
            Self::IndexPrograms => "index-programs",
            Self::IndexSelectors => "index-selectors",
            Self::SidecarSignatures => "sidecar-signatures",
            Self::SidecarPoh => "sidecar-poh",
            Self::SidecarShredding => "sidecar-shredding",
            Self::SidecarGenesis => "sidecar-genesis",
            Self::DictionaryBlockhashes => "dictionary-blockhashes",
            Self::RuntimeInnerInstructions => "effect-inner-instructions",
            Self::RuntimeBlockRewards => "runtime-block-rewards",
            Self::DictionaryAccountFlags => "dictionary-account-flags",
        }
    }
}

impl TryFrom<u16> for ObjectRole {
    type Error = UnknownObjectRole;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(match value {
            1 => Self::CatalogBlocks,
            3 => Self::DictionaryPubkeys,
            5 => Self::DictionaryBlockhashes,
            6 => Self::LedgerTransactions,
            11 => Self::RuntimeOutcomes,
            12 => Self::RuntimeBalances,
            13 => Self::RuntimeTokenBalances,
            14 => Self::RuntimeLogs,
            16 => Self::RuntimeRewards,
            17 => Self::IndexSlots,
            19 => Self::IndexAccounts,
            20 => Self::IndexPrograms,
            21 => Self::IndexSelectors,
            22 => Self::SidecarSignatures,
            23 => Self::SidecarPoh,
            24 => Self::SidecarShredding,
            26 => Self::SidecarGenesis,
            27 => Self::RuntimeInnerInstructions,
            28 => Self::RuntimeBlockRewards,
            30 => Self::DictionaryAccountFlags,
            other => return Err(UnknownObjectRole(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("unknown archive object role {0}")]
pub struct UnknownObjectRole(pub u16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileClass {
    Control,
    Canonical,
    DerivedIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileEncoding {
    Json,
    HeaderedBinary,
    ExactBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Requirement {
    Always,
    EpochZero,
    Optional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalFact {
    Blocks,
    PubkeyBytes,
    NonPohHashBytes,
    LedgerCore,
    LedgerAccounts,
    LookupDescriptors,
    InstructionStructure,
    InstructionData,
    Outcomes,
    Balances,
    TokenBalances,
    Logs,
    ReturnData,
    TransactionRewards,
    SignatureBytes,
    Poh,
    Shredding,
    BlockRewards,
    GenesisBytes,
    InnerInstructions,
    InnerInstructionData,
}

impl CanonicalFact {
    pub const ALL: [Self; 21] = [
        Self::Blocks,
        Self::PubkeyBytes,
        Self::NonPohHashBytes,
        Self::LedgerCore,
        Self::LedgerAccounts,
        Self::LookupDescriptors,
        Self::InstructionStructure,
        Self::InstructionData,
        Self::Outcomes,
        Self::Balances,
        Self::TokenBalances,
        Self::Logs,
        Self::ReturnData,
        Self::TransactionRewards,
        Self::SignatureBytes,
        Self::Poh,
        Self::Shredding,
        Self::BlockRewards,
        Self::GenesisBytes,
        Self::InnerInstructions,
        Self::InnerInstructionData,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectSpec {
    pub path: &'static str,
    pub role: ObjectRole,
    pub schema: u16,
    pub class: FileClass,
    pub encoding: FileEncoding,
    pub requirement: Requirement,
    /// Every canonical fact physically owned by this object.
    pub canonical_facts: &'static [CanonicalFact],
    pub derived_from: &'static [CanonicalFact],
}

impl ObjectSpec {
    pub const fn required_for_epoch(self, epoch: u64) -> bool {
        match self.requirement {
            Requirement::Always => true,
            Requirement::EpochZero => epoch == 0,
            Requirement::Optional => false,
        }
    }
}

const NO_DEPENDENCIES: &[CanonicalFact] = &[];
const NO_FACTS: &[CanonicalFact] = &[];
const BLOCK_FACTS: &[CanonicalFact] = &[CanonicalFact::Blocks];
const PUBKEY_FACTS: &[CanonicalFact] = &[CanonicalFact::PubkeyBytes];
const BLOCKHASH_FACTS: &[CanonicalFact] = &[CanonicalFact::NonPohHashBytes];
const TRANSACTION_FACTS: &[CanonicalFact] = &[
    CanonicalFact::LedgerCore,
    CanonicalFact::LedgerAccounts,
    CanonicalFact::LookupDescriptors,
    CanonicalFact::InstructionStructure,
    CanonicalFact::InstructionData,
];
const INNER_INSTRUCTION_FACTS: &[CanonicalFact] = &[
    CanonicalFact::InnerInstructions,
    CanonicalFact::InnerInstructionData,
];
const OUTCOME_FACTS: &[CanonicalFact] = &[CanonicalFact::Outcomes, CanonicalFact::ReturnData];
const BALANCE_FACTS: &[CanonicalFact] = &[CanonicalFact::Balances];
const TOKEN_BALANCE_FACTS: &[CanonicalFact] = &[CanonicalFact::TokenBalances];
const LOG_FACTS: &[CanonicalFact] = &[CanonicalFact::Logs];
const REWARD_FACTS: &[CanonicalFact] = &[CanonicalFact::TransactionRewards];
const BLOCK_REWARD_FACTS: &[CanonicalFact] = &[CanonicalFact::BlockRewards];
const SIGNATURE_FACTS: &[CanonicalFact] = &[CanonicalFact::SignatureBytes];
const POH_FACTS: &[CanonicalFact] = &[CanonicalFact::Poh];
const SHREDDING_FACTS: &[CanonicalFact] = &[CanonicalFact::Shredding];
const GENESIS_FACTS: &[CanonicalFact] = &[CanonicalFact::GenesisBytes];
const SLOT_INDEX_DEPENDENCIES: &[CanonicalFact] = &[CanonicalFact::Blocks];
const ACCOUNT_INDEX_DEPENDENCIES: &[CanonicalFact] = &[
    CanonicalFact::LedgerCore,
    CanonicalFact::LedgerAccounts,
    CanonicalFact::InstructionStructure,
    CanonicalFact::InnerInstructions,
];
const PROGRAM_INDEX_DEPENDENCIES: &[CanonicalFact] = &[
    CanonicalFact::LedgerAccounts,
    CanonicalFact::InstructionStructure,
    CanonicalFact::InnerInstructions,
];
const SELECTOR_INDEX_DEPENDENCIES: &[CanonicalFact] = &[
    CanonicalFact::LedgerAccounts,
    CanonicalFact::InstructionStructure,
    CanonicalFact::InstructionData,
    CanonicalFact::InnerInstructions,
    CanonicalFact::InnerInstructionData,
];
const ACCOUNT_FLAGS_DEPENDENCIES: &[CanonicalFact] = &[
    CanonicalFact::LedgerCore,
    CanonicalFact::LedgerAccounts,
    CanonicalFact::InstructionStructure,
    CanonicalFact::InnerInstructions,
];

macro_rules! canonical {
    ($path:expr, $role:expr, $schema:expr, $facts:expr, $requirement:expr) => {
        ObjectSpec {
            path: $path,
            role: $role,
            schema: $schema,
            class: FileClass::Canonical,
            encoding: FileEncoding::HeaderedBinary,
            requirement: $requirement,
            canonical_facts: $facts,
            derived_from: NO_DEPENDENCIES,
        }
    };
}

macro_rules! derived {
    ($path:expr, $role:expr, $schema:expr, $dependencies:expr) => {
        ObjectSpec {
            path: $path,
            role: $role,
            schema: $schema,
            class: FileClass::DerivedIndex,
            encoding: FileEncoding::HeaderedBinary,
            requirement: Requirement::Always,
            canonical_facts: NO_FACTS,
            derived_from: $dependencies,
        }
    };
}

pub static LAYOUT: &[ObjectSpec] = &[
    canonical!(
        catalog::blocks::PATH,
        ObjectRole::CatalogBlocks,
        catalog::blocks::SCHEMA,
        BLOCK_FACTS,
        Requirement::Always
    ),
    canonical!(
        dictionary::pubkeys::PATH,
        ObjectRole::DictionaryPubkeys,
        dictionary::pubkeys::SCHEMA,
        PUBKEY_FACTS,
        Requirement::Always
    ),
    canonical!(
        dictionary::blockhashes::PATH,
        ObjectRole::DictionaryBlockhashes,
        dictionary::blockhashes::SCHEMA,
        BLOCKHASH_FACTS,
        Requirement::Always
    ),
    derived!(
        dictionary::account_flags::PATH,
        ObjectRole::DictionaryAccountFlags,
        dictionary::account_flags::SCHEMA,
        ACCOUNT_FLAGS_DEPENDENCIES
    ),
    canonical!(
        ledger::transactions::PATH,
        ObjectRole::LedgerTransactions,
        ledger::transactions::SCHEMA,
        TRANSACTION_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::outcomes::PATH,
        ObjectRole::RuntimeOutcomes,
        runtime::outcomes::SCHEMA,
        OUTCOME_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::balances::PATH,
        ObjectRole::RuntimeBalances,
        runtime::balances::SCHEMA,
        BALANCE_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::token_balances::PATH,
        ObjectRole::RuntimeTokenBalances,
        runtime::token_balances::SCHEMA,
        TOKEN_BALANCE_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::logs::PATH,
        ObjectRole::RuntimeLogs,
        runtime::logs::SCHEMA,
        LOG_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::rewards::PATH,
        ObjectRole::RuntimeRewards,
        runtime::rewards::SCHEMA,
        REWARD_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::inner_instructions::PATH,
        ObjectRole::RuntimeInnerInstructions,
        runtime::inner_instructions::SCHEMA,
        INNER_INSTRUCTION_FACTS,
        Requirement::Always
    ),
    canonical!(
        runtime::block_rewards::PATH,
        ObjectRole::RuntimeBlockRewards,
        runtime::block_rewards::SCHEMA,
        BLOCK_REWARD_FACTS,
        Requirement::Always
    ),
    derived!(
        indexes::slots::PATH,
        ObjectRole::IndexSlots,
        indexes::slots::SCHEMA,
        SLOT_INDEX_DEPENDENCIES
    ),
    derived!(
        indexes::accounts::PATH,
        ObjectRole::IndexAccounts,
        indexes::accounts::SCHEMA,
        ACCOUNT_INDEX_DEPENDENCIES
    ),
    derived!(
        indexes::programs::PATH,
        ObjectRole::IndexPrograms,
        indexes::programs::SCHEMA,
        PROGRAM_INDEX_DEPENDENCIES
    ),
    derived!(
        indexes::selectors::PATH,
        ObjectRole::IndexSelectors,
        indexes::selectors::SCHEMA,
        SELECTOR_INDEX_DEPENDENCIES
    ),
    canonical!(
        sidecars::signatures::PATH,
        ObjectRole::SidecarSignatures,
        sidecars::signatures::SCHEMA,
        SIGNATURE_FACTS,
        Requirement::Always
    ),
    canonical!(
        sidecars::poh::PATH,
        ObjectRole::SidecarPoh,
        sidecars::poh::SCHEMA,
        POH_FACTS,
        Requirement::Always
    ),
    canonical!(
        sidecars::shredding::PATH,
        ObjectRole::SidecarShredding,
        sidecars::shredding::SCHEMA,
        SHREDDING_FACTS,
        Requirement::Always
    ),
    ObjectSpec {
        path: sidecars::genesis::PATH,
        role: ObjectRole::SidecarGenesis,
        schema: sidecars::genesis::SCHEMA,
        class: FileClass::Canonical,
        encoding: FileEncoding::ExactBytes,
        requirement: Requirement::EpochZero,
        canonical_facts: GENESIS_FACTS,
        derived_from: NO_DEPENDENCIES,
    },
];

pub fn object_by_path(path: &str) -> Option<&'static ObjectSpec> {
    LAYOUT.iter().find(|spec| spec.path == path)
}

pub fn object_by_role(role: ObjectRole) -> Option<&'static ObjectSpec> {
    LAYOUT.iter().find(|spec| spec.role == role)
}

pub fn validate_archive_path(path: &str) -> Result<(), PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    if path.len() > 1024 {
        return Err(PathError::TooLong(path.len()));
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(PathError::NotRelativeFile);
    }
    if path.contains('\\') {
        return Err(PathError::Backslash);
    }
    if path
        .bytes()
        .any(|byte| byte == 0 || byte.is_ascii_control())
    {
        return Err(PathError::ControlCharacter);
    }
    for component in path.split('/') {
        if component.is_empty() {
            return Err(PathError::EmptyComponent);
        }
        if component == "." || component == ".." {
            return Err(PathError::TraversalComponent);
        }
        if component.len() > 255 {
            return Err(PathError::ComponentTooLong(component.len()));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PathError {
    #[error("archive path is empty")]
    Empty,
    #[error("archive path has {0} bytes, above the 1024-byte limit")]
    TooLong(usize),
    #[error("archive path must be a relative file path")]
    NotRelativeFile,
    #[error("archive path contains a backslash")]
    Backslash,
    #[error("archive path contains a control character")]
    ControlCharacter,
    #[error("archive path contains an empty component")]
    EmptyComponent,
    #[error("archive path contains a traversal component")]
    TraversalComponent,
    #[error("archive path component has {0} bytes, above the 255-byte limit")]
    ComponentTooLong(usize),
}

pub fn validate_layout(specs: &[ObjectSpec]) -> Result<(), LayoutError> {
    let mut paths = HashSet::with_capacity(specs.len());
    let mut roles = HashSet::with_capacity(specs.len());
    let mut owners = HashMap::with_capacity(CanonicalFact::ALL.len());

    for spec in specs {
        validate_archive_path(spec.path).map_err(|source| LayoutError::InvalidPath {
            path: spec.path,
            source,
        })?;
        if !paths.insert(spec.path) {
            return Err(LayoutError::DuplicatePath(spec.path));
        }
        if !roles.insert(spec.role) {
            return Err(LayoutError::DuplicateRole(spec.role));
        }
        if spec.schema == 0 {
            return Err(LayoutError::ZeroSchema(spec.path));
        }

        match spec.class {
            FileClass::Control | FileClass::Canonical => {
                if spec.canonical_facts.is_empty() {
                    return Err(LayoutError::CanonicalWithoutFact(spec.path));
                }
                if !spec.derived_from.is_empty() {
                    return Err(LayoutError::CanonicalWithDependencies(spec.path));
                }
                let mut object_facts = HashSet::with_capacity(spec.canonical_facts.len());
                for fact in spec.canonical_facts {
                    if !object_facts.insert(*fact) {
                        return Err(LayoutError::DuplicateOwnedFact {
                            path: spec.path,
                            fact: *fact,
                        });
                    }
                    if let Some(previous) = owners.insert(*fact, spec) {
                        return Err(LayoutError::DuplicateOwner {
                            fact: *fact,
                            first: previous.path,
                            second: spec.path,
                        });
                    }
                }
            }
            FileClass::DerivedIndex => {
                if !spec.canonical_facts.is_empty() {
                    return Err(LayoutError::IndexOwnsFact(spec.path));
                }
                if spec.derived_from.is_empty() {
                    return Err(LayoutError::IndexWithoutDependencies(spec.path));
                }
                let mut dependencies = HashSet::with_capacity(spec.derived_from.len());
                for dependency in spec.derived_from {
                    if !dependencies.insert(*dependency) {
                        return Err(LayoutError::DuplicateDependency {
                            path: spec.path,
                            fact: *dependency,
                        });
                    }
                }
            }
        }
    }

    for fact in CanonicalFact::ALL {
        if !owners.contains_key(&fact) {
            return Err(LayoutError::MissingOwner(fact));
        }
    }

    for spec in specs
        .iter()
        .filter(|spec| spec.class == FileClass::DerivedIndex)
    {
        for dependency in spec.derived_from {
            let owner = owners
                .get(dependency)
                .ok_or(LayoutError::MissingOwner(*dependency))?;
            if spec.requirement == Requirement::Always && owner.requirement != Requirement::Always {
                return Err(LayoutError::DependencyCanBeAbsent {
                    path: spec.path,
                    fact: *dependency,
                    owner: owner.path,
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LayoutError {
    #[error("invalid archive path {path}: {source}")]
    InvalidPath {
        path: &'static str,
        source: PathError,
    },
    #[error("duplicate archive path {0}")]
    DuplicatePath(&'static str),
    #[error("duplicate archive object role {0:?}")]
    DuplicateRole(ObjectRole),
    #[error("archive object {0} has schema zero")]
    ZeroSchema(&'static str),
    #[error("canonical archive object {0} does not own a fact")]
    CanonicalWithoutFact(&'static str),
    #[error("canonical archive object {0} declares index dependencies")]
    CanonicalWithDependencies(&'static str),
    #[error("canonical fact {fact:?} has two owners: {first} and {second}")]
    DuplicateOwner {
        fact: CanonicalFact,
        first: &'static str,
        second: &'static str,
    },
    #[error("canonical object {path} lists owned fact {fact:?} more than once")]
    DuplicateOwnedFact {
        path: &'static str,
        fact: CanonicalFact,
    },
    #[error("derived index {0} claims a canonical fact")]
    IndexOwnsFact(&'static str),
    #[error("derived index {0} has no canonical dependencies")]
    IndexWithoutDependencies(&'static str),
    #[error("derived index {path} lists canonical dependency {fact:?} more than once")]
    DuplicateDependency {
        path: &'static str,
        fact: CanonicalFact,
    },
    #[error("canonical fact {0:?} has no owner")]
    MissingOwner(CanonicalFact),
    #[error("required derived index {path} depends on {fact:?}, but owner {owner} can be absent")]
    DependencyCanBeAbsent {
        path: &'static str,
        fact: CanonicalFact,
        owner: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_layout_has_one_owner_per_fact() {
        validate_layout(LAYOUT).unwrap();
        let mut unique = HashSet::with_capacity(CanonicalFact::ALL.len());
        for fact in CanonicalFact::ALL {
            assert!(unique.insert(fact), "CanonicalFact::ALL repeats {fact:?}");
            let owners = LAYOUT
                .iter()
                .filter(|spec| spec.canonical_facts.contains(&fact))
                .count();
            assert_eq!(owners, 1, "{fact:?} has {owners} owners");
        }
    }

    #[test]
    fn poh_is_owned_only_by_the_poh_sidecar() {
        let owners: Vec<_> = LAYOUT
            .iter()
            .filter(|spec| spec.canonical_facts.contains(&CanonicalFact::Poh))
            .collect();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].path, sidecars::poh::PATH);
        assert_eq!(owners[0].role, ObjectRole::SidecarPoh);
    }

    #[test]
    fn no_materialized_block_access_object_exists() {
        assert!(
            LAYOUT
                .iter()
                .all(|spec| !spec.path.contains("block-access"))
        );
    }

    #[test]
    fn every_catalog_column_names_its_layout_object() {
        for path in catalog::blocks::COLUMNS {
            assert!(
                object_by_path(path).is_some(),
                "catalog column {path} has no layout object"
            );
        }
        assert_eq!(ledger::transactions::PATH, "ledger/transactions.wincode");
        assert_eq!(
            runtime::inner_instructions::PATH,
            "runtime/inner_instructions.wincode"
        );
        assert_eq!(runtime::outcomes::PATH, "runtime/outcomes.wincode");
        assert_eq!(
            runtime::block_rewards::PATH,
            "runtime/block_rewards.wincode"
        );
        assert_eq!(sidecars::signatures::PATH, "sidecars/signatures.bin");
        assert_eq!(
            dictionary::account_flags::PATH,
            "dictionary/account_flags.pages"
        );
    }

    #[test]
    fn nested_paths_are_safe_but_traversal_is_not() {
        validate_archive_path("ledger/accounts.pages").unwrap();
        assert!(validate_archive_path("../accounts.pages").is_err());
        assert!(validate_archive_path("transactions//accounts.pages").is_err());
        assert!(validate_archive_path("/ledger/accounts.pages").is_err());
        assert!(validate_archive_path("transactions\\accounts.pages").is_err());
    }

    #[test]
    fn archive_id_uses_exact_lowercase_hex() {
        let id = ArchiveId::new([0xab; 16]);
        assert_eq!(id.to_hex(), "abababababababababababababababab");
        assert_eq!(ArchiveId::from_hex(&id.to_hex()).unwrap(), id);
        assert!(ArchiveId::from_hex("ABABABABABABABABABABABABABABABAB").is_err());
    }

    #[test]
    fn object_role_wire_codes_and_names_are_frozen() {
        let expected = [
            (ObjectRole::CatalogBlocks, 1, "catalog-blocks"),
            (ObjectRole::DictionaryPubkeys, 3, "dictionary-pubkeys"),
            (
                ObjectRole::DictionaryBlockhashes,
                5,
                "dictionary-blockhashes",
            ),
            (ObjectRole::LedgerTransactions, 6, "ledger-transactions"),
            (ObjectRole::RuntimeOutcomes, 11, "effect-outcomes"),
            (ObjectRole::RuntimeBalances, 12, "effect-balances"),
            (
                ObjectRole::RuntimeTokenBalances,
                13,
                "effect-token-balances",
            ),
            (ObjectRole::RuntimeLogs, 14, "effect-logs"),
            (ObjectRole::RuntimeRewards, 16, "effect-rewards"),
            (ObjectRole::IndexSlots, 17, "index-slots"),
            (ObjectRole::IndexAccounts, 19, "index-accounts"),
            (ObjectRole::IndexPrograms, 20, "index-programs"),
            (ObjectRole::IndexSelectors, 21, "index-selectors"),
            (ObjectRole::SidecarSignatures, 22, "sidecar-signatures"),
            (ObjectRole::SidecarPoh, 23, "sidecar-poh"),
            (ObjectRole::SidecarShredding, 24, "sidecar-shredding"),
            (ObjectRole::SidecarGenesis, 26, "sidecar-genesis"),
            (
                ObjectRole::RuntimeInnerInstructions,
                27,
                "effect-inner-instructions",
            ),
            (ObjectRole::RuntimeBlockRewards, 28, "runtime-block-rewards"),
            (
                ObjectRole::DictionaryAccountFlags,
                30,
                "dictionary-account-flags",
            ),
        ];
        assert_eq!(ObjectRole::ALL.len(), expected.len());
        for (index, (role, code, name)) in expected.into_iter().enumerate() {
            assert_eq!(ObjectRole::ALL[index], role);
            assert_eq!(role.code(), code);
            assert_eq!(role.as_str(), name);
            assert_eq!(ObjectRole::try_from(code), Ok(role));
        }
        for retired in [7, 8, 9, 10, 15, 18, 25, 29, 31] {
            assert_eq!(
                ObjectRole::try_from(retired),
                Err(UnknownObjectRole(retired))
            );
        }
    }
}

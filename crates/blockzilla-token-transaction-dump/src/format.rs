use blockzilla_format::CompactPubkey;
use wincode::{SchemaRead, SchemaWrite};

pub const SPYX_MINT: &str = "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W";
pub const SPYX_MINT_SIGNATURE: &str =
    "51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R";
pub const SPYX_MINT_SLOT: u64 = 346_066_298;

pub const DUMP_SCHEMA_VERSION: u16 = 3;
pub const PUBKEY_REGISTRY_ID_BASE: u8 = 1;
pub const TRANSACTIONS_FILE: &str = "transactions.wincode";
pub const DISCOVERY_SHARDS_DIR: &str = "discoveries";
pub const CREATIONS_FILE: &str = "creations.wincode";
pub const ACCOUNTS_FILE: &str = "accounts.wincode";
pub const ACCOUNT_ID_LOG_FILE: &str = "account-ids.wincode";
pub const SIGNATURES_FILE: &str = "signatures.bin";
pub const PUBKEY_REGISTRY_FILE: &str = "registry.bin";
pub const DUMP_MANIFEST_FILE: &str = "manifest.json";
pub const EPOCH_SHARDS_DIR: &str = "epochs";
pub const REGISTRY_MAPS_DIR: &str = "maps";

#[derive(Debug, SchemaRead, SchemaWrite)]
#[allow(clippy::large_enum_variant)] // Keep the direct Wincode record shape allocation-free.
pub enum TokenTransactionDumpRecord {
    #[wincode(tag = 0)]
    Header(TokenTransactionDumpHeader),
    #[wincode(tag = 1)]
    Transaction(TokenTransactionRecord),
    #[wincode(tag = 2)]
    Footer(TokenTransactionDumpFooter),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum DumpStreamKind {
    #[wincode(tag = 0)]
    RawEpochShard,
    #[wincode(tag = 1)]
    Consolidated,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct TokenTransactionDumpHeader {
    pub schema_version: u16,
    pub stream_kind: DumpStreamKind,
    pub mint: [u8; 32],
    pub mint_slot: u64,
    pub mint_signature: [u8; 64],
    /// `Some` for an immutable raw epoch shard and `None` for the consolidated stream.
    pub source_epoch: Option<u64>,
    /// Present only for an immutable raw epoch shard. For published input, this is the
    /// published generation digest. For trusted-local input, it is a synthetic identity that
    /// binds admitted file names and sizes plus the asserted wire profile, but does not
    /// authenticate file contents.
    pub source_generation_digest: Option<[u8; 32]>,
    /// Present only for an immutable raw epoch shard. Consolidated transaction records retain
    /// their individual source profile.
    pub source_wire_profile: Option<DumpWireProfile>,
    pub pubkey_registry_id_base: u8,
}

/// One selected transaction.
///
/// Raw epoch shards copy `message_bytes` and `metadata_bytes` byte-for-byte from the admitted
/// Compact V2 generation. They keep only the source signature reference and set
/// `dump_signature_ordinal` to `None`. Consolidation rewrites only CompactPubkey references in
/// the two byte payloads, copies the selected source signatures byte-for-byte to the final
/// `signatures.bin`, and sets `dump_signature_ordinal` to that final sidecar position.
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct TokenTransactionRecord {
    pub source_epoch: u64,
    /// Binds all source locations in this record to the admitted source generation.
    pub source_generation_digest: [u8; 32],
    pub source_wire_profile: DumpWireProfile,
    pub source_block_id: u32,
    pub block: TokenTransactionBlockContext,
    pub tx_index: u32,
    pub flags: u32,
    pub source_first_signature_ordinal: u64,
    pub signature_count: u8,
    /// `None` in raw shards. In a consolidated stream, this is the zero-based first signature
    /// ordinal in the final `signatures.bin` sidecar.
    pub dump_signature_ordinal: Option<u64>,
    /// Exact source bytes in a raw shard. In the consolidated stream, only CompactPubkey
    /// references are rewritten to the dump registry.
    pub message_bytes: Vec<u8>,
    /// Exact source bytes in a raw shard. This is empty when the metadata-present flag is clear.
    /// In the consolidated stream, only CompactPubkey references are rewritten.
    pub metadata_bytes: Vec<u8>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenTransactionBlockContext {
    pub slot: u64,
    pub parent_slot: u64,
    /// Exact source registry ID bit pattern. A high-bit value can represent a signed
    /// cross-epoch reference in the source format.
    pub blockhash_id: u32,
    /// Exact source registry ID bit pattern.
    pub previous_blockhash_id: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    /// Total transactions in the source block, including transactions not selected for this dump.
    pub transaction_count: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TokenTransactionDumpFooter {
    pub epochs: u64,
    pub blocks_scanned: u64,
    pub transactions_scanned: u64,
    pub transactions_written: u64,
    /// Zero in raw extraction artifacts and the dedicated registry size in a consolidated dump.
    pub pubkeys: u64,
    /// Zero in raw extraction artifacts and the selected signature count in a consolidated dump.
    pub signatures: u64,
    pub owned_block_fallbacks: u64,
    pub raw_transaction_fallbacks: u64,
    pub raw_metadata_fallbacks: u64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    SchemaRead,
    SchemaWrite,
)]
pub struct SourceTransactionCoordinate {
    pub epoch: u64,
    pub slot: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
    /// Absolute ordinal in the admitted source epoch `signatures.bin`.
    pub source_first_signature_ordinal: u64,
    pub signature_count: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, SchemaRead, SchemaWrite)]
pub struct SourceInstructionCoordinate {
    pub epoch: u64,
    pub slot: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
    pub instruction_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct EpochCreationEntry {
    pub source_reference: CompactPubkey,
    pub raw_pubkey: [u8; 32],
    pub first_creation: SourceInstructionCoordinate,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct EpochCreationLog {
    pub schema_version: u16,
    pub epoch: u64,
    pub source_generation_digest: [u8; 32],
    /// Target mint for which these successful account initializations were discovered.
    pub mint: [u8; 32],
    /// Strictly sorted by `raw_pubkey` and unique. The earliest successful creation wins.
    pub entries: Vec<EpochCreationEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct DiscoveredAccount {
    pub raw_pubkey: [u8; 32],
    pub first_creation: SourceInstructionCoordinate,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct DiscoveredAccountList {
    pub schema_version: u16,
    pub mint: [u8; 32],
    /// The one transaction whose first source signature matches `mint_signature`.
    /// The file digest binds this location and signature reference for resume and Pass B.
    pub anchor_position: SourceTransactionCoordinate,
    /// Strictly sorted by `raw_pubkey` and unique.
    pub accounts: Vec<DiscoveredAccount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum AccountIdRole {
    #[wincode(tag = 0)]
    TargetMint,
    #[wincode(tag = 1)]
    TokenAccount,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct EpochAccountIdEntry {
    pub local_id: Option<u32>,
    pub raw_pubkey: [u8; 32],
    pub role: AccountIdRole,
    /// `None` for the target mint.
    pub first_creation: Option<SourceInstructionCoordinate>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct EpochAccountIdLog {
    pub schema_version: u16,
    pub epoch: u64,
    pub source_generation_digest: [u8; 32],
    /// Strictly sorted by `raw_pubkey` and unique.
    pub entries: Vec<EpochAccountIdEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DumpArtifactKind {
    RawExtractionRoot,
    RawEpochShard,
    Consolidated,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DumpManifest {
    pub schema_version: u16,
    pub artifact_kind: DumpArtifactKind,
    pub complete: bool,
    pub mint: String,
    pub mint_slot: u64,
    pub mint_signature: String,
    pub workers: usize,
    pub source_binding: DumpSourceBinding,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub transactions: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signatures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pubkeys: Option<u64>,
    pub transaction_stream: String,
    /// Present for a raw epoch shard or a consolidated stream. The raw extraction root points to
    /// the `epochs` directory and therefore has no single stream digest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_stream_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id_log: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id_log_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovered_accounts: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovered_accounts_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovered_account_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_stream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature_stream_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pubkey_registry: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pubkey_registry_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry_maps: Option<String>,
}

/// The integrity contract used to admit Compact V2 source generations.
///
/// This is present in extraction-root, epoch-shard, and consolidated manifests.
/// Consolidation requires every shard to use the same value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DumpSourceBinding {
    /// Manifest-free trusted local source identity used for manual local validation.
    /// The reader checks file identity, size, and final source stability.
    TrustedLocalSizesOnly {
        cluster_id: String,
        slots_per_epoch: u64,
        wire_profile: DumpWireProfile,
    },
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, SchemaRead, SchemaWrite,
)]
#[serde(rename_all = "snake_case")]
pub enum DumpWireProfile {
    #[wincode(tag = 0)]
    PostUnknownInstructionFallbacksV1,
    #[wincode(tag = 1)]
    PreUnknownInstructionFallbacksV1,
}

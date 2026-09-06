//! Immutable, market-bound display metadata for every mint in proven swaps.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use spl_token_2022_interface::{
    extension::{
        BaseStateWithExtensions, ExtensionType, StateWithExtensions,
        metadata_pointer::MetadataPointer,
    },
    state::Mint,
};
use spl_token_metadata_interface::state::TokenMetadata;

use crate::market_store::{MarketMint, MarketStore, RegistryKeyView};

pub const MINT_METADATA_SCHEMA_VERSION: u16 = 1;
pub const MINT_METADATA_FILE: &str = "mint-metadata-v1.json";
pub const DEFAULT_SOLANA_RPC_URL: &str = "https://api.mainnet-beta.solana.com";
pub const SOLANA_MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

const ARTIFACT_KIND: &str = "blockzilla_spyx_mint_metadata_v1";
const DECODER_VERSION: &str = "blockzilla_spyx_mint_metadata_decoder_v1";
const LEGACY_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const METAPLEX_METADATA_PROGRAM: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";
const MAX_RPC_BATCH_SIZE: usize = 100;
const MAX_ACCOUNT_DATA_BYTES: usize = 16 << 20;
const MAX_ARTIFACT_BYTES: u64 = 32 << 20;
const MAX_NAME_BYTES: usize = 512;
const MAX_SYMBOL_BYTES: usize = 128;
const MAX_URI_BYTES: usize = 2_048;
const MAX_METAPLEX_NAME_BYTES: usize = 32;
const MAX_METAPLEX_SYMBOL_BYTES: usize = 10;
const MAX_METAPLEX_URI_BYTES: usize = 200;
const METAPLEX_METADATA_V1_KEY: u8 = 4;

#[derive(Debug, Clone)]
pub struct MintMetadataBuildConfig {
    pub dump: PathBuf,
    pub market: PathBuf,
    pub output: PathBuf,
    pub rpc_url: String,
    pub expected_genesis_hash: String,
    pub batch_size: usize,
}

impl MintMetadataBuildConfig {
    pub fn mainnet(dump: PathBuf, market: PathBuf, output: PathBuf, rpc_url: String) -> Self {
        Self {
            dump,
            market,
            output,
            rpc_url,
            expected_genesis_hash: SOLANA_MAINNET_GENESIS_HASH.to_owned(),
            batch_size: MAX_RPC_BATCH_SIZE,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MintMetadataBuildSummary {
    pub output: String,
    pub artifact_sha256: String,
    pub mints: u64,
    pub valid_mint_accounts: u64,
    pub absent_mint_accounts: u64,
    pub invalid_mint_accounts: u64,
    pub named_mints: u64,
    pub symbol_mints: u64,
    pub token_2022_metadata: u64,
    pub metaplex_metadata: u64,
    pub unresolved_metadata: u64,
    pub minimum_context_slot: u64,
    pub maximum_context_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintMetadataArtifact {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    pub created_unix_seconds: u64,
    pub decoder_version: String,
    pub cluster_genesis_hash: String,
    pub rpc_endpoint_sha256: String,
    pub requested_min_context_slot: u64,
    pub minimum_context_slot: u64,
    pub maximum_context_slot: u64,
    pub market: MintMetadataMarketBinding,
    pub counts: MintMetadataCounts,
    pub mints: Vec<MintMetadataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintMetadataMarketBinding {
    pub market_schema_version: u16,
    pub market_manifest_sha256: String,
    pub market_trade_file_sha256: String,
    pub source_manifest_sha256: String,
    pub source_transaction_sha256: String,
    pub source_registry_sha256: String,
    pub target_mint: String,
    pub target_mint_id: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintMetadataCounts {
    pub mints: u64,
    pub valid_mint_accounts: u64,
    pub absent_mint_accounts: u64,
    pub invalid_mint_accounts: u64,
    pub named_mints: u64,
    pub symbol_mints: u64,
    pub token_2022_metadata: u64,
    pub metaplex_metadata: u64,
    pub unresolved_metadata: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MintAccountStatus {
    Valid,
    Absent,
    InvalidOwner,
    InvalidData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenProgramKind {
    Legacy,
    Token2022,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataPointerStatus {
    NotApplicable,
    Absent,
    Null,
    SelfAddressed,
    ExternalUnresolved,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisplayMetadataSource {
    Token2022,
    Metaplex,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintDisplayMetadata {
    pub source: DisplayMetadataSource,
    pub metadata_account: String,
    pub metadata_account_sha256: String,
    pub context_slot: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_authority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintMetadataRecord {
    pub mint: RegistryKeyView,
    pub market_decimals: u8,
    pub is_target: bool,
    pub direct_usd_quote: bool,
    pub trade_count: u64,
    pub mint_account_status: MintAccountStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_program: Option<TokenProgramKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_decimals: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint_account_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mint_context_slot: Option<u64>,
    pub metadata_pointer_status: MetadataPointerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_pointer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display: Option<MintDisplayMetadata>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MintMetadataHealth {
    pub available: bool,
    pub schema_version: u16,
    pub complete: bool,
    pub artifact_sha256: String,
    pub cluster_genesis_hash: String,
    pub minimum_context_slot: u64,
    pub maximum_context_slot: u64,
    pub counts: MintMetadataCounts,
}

pub struct MintMetadataStore {
    artifact_path: PathBuf,
    artifact_sha256: String,
    artifact: MintMetadataArtifact,
    by_address: BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
struct RpcAccountsResponse {
    jsonrpc: String,
    id: u64,
    result: Option<RpcAccountsResult>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcAccountsResult {
    context: RpcContext,
    value: Vec<Option<RpcAccount>>,
}

#[derive(Debug, Deserialize)]
struct RpcContext {
    slot: u64,
}

#[derive(Debug, Deserialize)]
struct RpcAccount {
    owner: String,
    data: Value,
    executable: bool,
    #[serde(default)]
    space: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RpcStringResponse {
    jsonrpc: String,
    id: u64,
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug)]
struct DecodedMint {
    program: TokenProgramKind,
    decimals: u8,
    pointer_status: MetadataPointerStatus,
    pointer: Option<String>,
    token_2022_display: Option<MintDisplayMetadata>,
    permit_metaplex_fallback: bool,
    warnings: Vec<String>,
}

#[derive(Debug)]
struct DecodedMetaplex {
    update_authority: [u8; 32],
    mint: [u8; 32],
    name: String,
    symbol: String,
    uri: String,
}

struct UntrustedDisplayMetadata {
    update_authority: Option<[u8; 32]>,
    name: String,
    symbol: String,
    uri: String,
}

pub async fn build_mint_metadata(
    config: &MintMetadataBuildConfig,
) -> Result<MintMetadataBuildSummary> {
    ensure!(!config.rpc_url.trim().is_empty(), "Solana RPC URL is empty");
    ensure!(
        (1..=MAX_RPC_BATCH_SIZE).contains(&config.batch_size),
        "metadata RPC batch size must be from 1 through {MAX_RPC_BATCH_SIZE}"
    );
    prepare_output_directory(&config.output)?;

    let market = MarketStore::open(&config.dump, &config.market)?;
    let market_mints = market.mint_summaries()?;
    ensure!(!market_mints.is_empty(), "market has no mint rows");
    let requested_min_context_slot = market.minimum_metadata_context_slot()?;
    let market_binding = market_binding(&market)?;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .context("build Solana RPC client")?;
    let genesis_hash = rpc_genesis_hash(&client, &config.rpc_url).await?;
    ensure!(
        genesis_hash == config.expected_genesis_hash,
        "RPC genesis hash differs: expected {}, got {}",
        config.expected_genesis_hash,
        genesis_hash
    );

    let mut records = market_mints.iter().map(empty_record).collect::<Vec<_>>();
    let mint_addresses = market_mints
        .iter()
        .map(|mint| mint.mint.address.clone())
        .collect::<Vec<_>>();
    let mut context_slots = Vec::new();
    let mut metaplex_targets = Vec::new();

    for (chunk_index, addresses) in mint_addresses.chunks(config.batch_size).enumerate() {
        let result = rpc_multiple_accounts(
            &client,
            &config.rpc_url,
            addresses,
            requested_min_context_slot,
        )
        .await
        .with_context(|| format!("fetch mint account batch {}", chunk_index + 1))?;
        ensure!(
            result.context.slot >= requested_min_context_slot,
            "mint RPC context predates the market source"
        );
        ensure!(
            result.value.len() == addresses.len(),
            "mint RPC result cardinality differs from its request"
        );
        context_slots.push(result.context.slot);
        let base = chunk_index
            .checked_mul(config.batch_size)
            .context("mint batch base overflow")?;
        for offset in 0..addresses.len() {
            let record_index = base.checked_add(offset).context("mint index overflow")?;
            let account = result
                .value
                .get(offset)
                .context("mint RPC response index is absent")?;
            let record = records
                .get_mut(record_index)
                .context("mint record index is absent")?;
            let Some(account) = account else {
                record.mint_account_status = MintAccountStatus::Absent;
                record
                    .warnings
                    .push("historical_account_unverified".to_owned());
                continue;
            };
            let data = rpc_account_data(account).context("decode mint account data")?;
            let mint_sha = hex_digest(Sha256::digest(&data).into());
            record.mint_account_sha256 = Some(mint_sha.clone());
            record.mint_context_slot = Some(result.context.slot);
            match decode_mint_account(
                &record.mint.address,
                &account.owner,
                &data,
                &mint_sha,
                result.context.slot,
            ) {
                Ok(decoded) => {
                    ensure!(
                        decoded.decimals == record.market_decimals,
                        "mint {} RPC decimals {} differ from market decimals {}",
                        record.mint.address,
                        decoded.decimals,
                        record.market_decimals
                    );
                    record.mint_account_status = MintAccountStatus::Valid;
                    record.token_program = Some(decoded.program);
                    record.rpc_decimals = Some(decoded.decimals);
                    record.metadata_pointer_status = decoded.pointer_status;
                    record.metadata_pointer = decoded.pointer;
                    record.display = decoded.token_2022_display;
                    record.warnings.extend(decoded.warnings);
                    if decoded.permit_metaplex_fallback {
                        metaplex_targets.push((record_index, metaplex_pda(&record.mint.address)?));
                    }
                }
                Err(error) if error.to_string().starts_with("unsupported mint owner") => {
                    record.mint_account_status = MintAccountStatus::InvalidOwner;
                    record.warnings.push(format!("invalid_mint_owner: {error}"));
                }
                Err(error) => {
                    record.mint_account_status = MintAccountStatus::InvalidData;
                    record
                        .warnings
                        .push(format!("invalid_mint_account: {error}"));
                }
            }
        }
    }

    for (chunk_index, targets) in metaplex_targets.chunks(config.batch_size).enumerate() {
        let addresses = targets
            .iter()
            .map(|(_, address)| address.clone())
            .collect::<Vec<_>>();
        let result = rpc_multiple_accounts(
            &client,
            &config.rpc_url,
            &addresses,
            requested_min_context_slot,
        )
        .await
        .with_context(|| format!("fetch Metaplex account batch {}", chunk_index + 1))?;
        ensure!(
            result.context.slot >= requested_min_context_slot,
            "metadata RPC context predates the market source"
        );
        ensure!(
            result.value.len() == targets.len(),
            "metadata RPC result cardinality differs from its request"
        );
        context_slots.push(result.context.slot);
        for offset in 0..targets.len() {
            let (record_index, metadata_address) = targets
                .get(offset)
                .context("metadata target index is absent")?;
            let record = records
                .get_mut(*record_index)
                .context("metadata record index is absent")?;
            let Some(account) = result
                .value
                .get(offset)
                .context("metadata RPC response index is absent")?
            else {
                record.warnings.push("metaplex_metadata_absent".to_owned());
                continue;
            };
            if account.owner != METAPLEX_METADATA_PROGRAM {
                record
                    .warnings
                    .push("metaplex_metadata_wrong_owner".to_owned());
                continue;
            }
            let data = match rpc_account_data(account) {
                Ok(data) => data,
                Err(error) => {
                    record
                        .warnings
                        .push(format!("metaplex_metadata_invalid_data: {error}"));
                    continue;
                }
            };
            let decoded = match decode_metaplex_metadata(&data) {
                Ok(decoded) => decoded,
                Err(error) => {
                    record
                        .warnings
                        .push(format!("metaplex_metadata_decode_failed: {error}"));
                    continue;
                }
            };
            let mint_bytes = decode_pubkey(&record.mint.address, "market mint")?;
            if decoded.mint != mint_bytes {
                record
                    .warnings
                    .push("metaplex_metadata_mint_mismatch".to_owned());
                continue;
            }
            record.display = build_display(
                DisplayMetadataSource::Metaplex,
                metadata_address,
                &data,
                result.context.slot,
                UntrustedDisplayMetadata {
                    update_authority: Some(decoded.update_authority),
                    name: decoded.name,
                    symbol: decoded.symbol,
                    uri: decoded.uri,
                },
                &mut record.warnings,
            )?;
        }
    }

    let minimum_context_slot = *context_slots
        .iter()
        .min()
        .context("metadata build recorded no RPC context")?;
    let maximum_context_slot = *context_slots
        .iter()
        .max()
        .context("metadata build recorded no RPC context")?;
    let counts = count_records(&records)?;
    let artifact = MintMetadataArtifact {
        schema_version: MINT_METADATA_SCHEMA_VERSION,
        artifact_kind: ARTIFACT_KIND.to_owned(),
        complete: true,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock predates the Unix epoch")?
            .as_secs(),
        decoder_version: DECODER_VERSION.to_owned(),
        cluster_genesis_hash: genesis_hash,
        rpc_endpoint_sha256: hex_digest(Sha256::digest(config.rpc_url.as_bytes()).into()),
        requested_min_context_slot,
        minimum_context_slot,
        maximum_context_slot,
        market: market_binding,
        counts,
        mints: records,
    };
    validate_artifact_against_market(&artifact, &market)?;
    let (artifact_path, artifact_sha256) = write_artifact(&config.output, &artifact)?;
    market.verify_identities()?;

    Ok(summary_from_artifact(
        &artifact,
        artifact_path,
        artifact_sha256,
    ))
}

impl MintMetadataStore {
    pub fn open(market: &MarketStore, root: &Path) -> Result<Self> {
        let artifact_path = if root.is_dir() {
            root.join(MINT_METADATA_FILE)
        } else {
            root.to_path_buf()
        };
        let metadata = fs::symlink_metadata(&artifact_path)
            .with_context(|| format!("inspect mint metadata {}", artifact_path.display()))?;
        ensure!(
            metadata.file_type().is_file() && metadata.len() <= MAX_ARTIFACT_BYTES,
            "mint metadata artifact is not a bounded regular file"
        );
        let bytes = fs::read(&artifact_path)
            .with_context(|| format!("read mint metadata {}", artifact_path.display()))?;
        let artifact_sha256 = hex_digest(Sha256::digest(&bytes).into());
        let artifact: MintMetadataArtifact =
            serde_json::from_slice(&bytes).context("parse mint metadata artifact")?;
        validate_artifact_against_market(&artifact, market)?;
        let by_address = artifact
            .mints
            .iter()
            .enumerate()
            .map(|(index, record)| (record.mint.address.clone(), index))
            .collect();
        Ok(Self {
            artifact_path,
            artifact_sha256,
            artifact,
            by_address,
        })
    }

    pub fn health(&self) -> MintMetadataHealth {
        MintMetadataHealth {
            available: true,
            schema_version: self.artifact.schema_version,
            complete: self.artifact.complete,
            artifact_sha256: self.artifact_sha256.clone(),
            cluster_genesis_hash: self.artifact.cluster_genesis_hash.clone(),
            minimum_context_slot: self.artifact.minimum_context_slot,
            maximum_context_slot: self.artifact.maximum_context_slot,
            counts: self.artifact.counts.clone(),
        }
    }

    pub fn mints(&self) -> &[MintMetadataRecord] {
        &self.artifact.mints
    }

    pub fn mint_by_address(&self, address: &str) -> Option<&MintMetadataRecord> {
        self.by_address
            .get(address)
            .and_then(|index| self.artifact.mints.get(*index))
    }

    pub fn verify_identity(&self) -> Result<()> {
        let bytes = fs::read(&self.artifact_path)
            .with_context(|| format!("read mint metadata {}", self.artifact_path.display()))?;
        ensure!(
            hex_digest(Sha256::digest(&bytes).into()) == self.artifact_sha256,
            "mint metadata artifact changed during use"
        );
        Ok(())
    }
}

pub fn validate_artifact_against_market(
    artifact: &MintMetadataArtifact,
    market: &MarketStore,
) -> Result<()> {
    ensure!(
        artifact.schema_version == MINT_METADATA_SCHEMA_VERSION
            && artifact.artifact_kind == ARTIFACT_KIND
            && artifact.complete
            && artifact.created_unix_seconds != 0
            && artifact.decoder_version == DECODER_VERSION,
        "invalid mint metadata artifact header"
    );
    ensure!(
        artifact.cluster_genesis_hash == SOLANA_MAINNET_GENESIS_HASH,
        "mint metadata artifact is not bound to Solana mainnet"
    );
    ensure!(
        artifact.requested_min_context_slot <= artifact.minimum_context_slot
            && artifact.minimum_context_slot <= artifact.maximum_context_slot,
        "mint metadata RPC context range is invalid"
    );
    ensure!(
        artifact.market == market_binding(market)?,
        "mint metadata market binding differs from the loaded market"
    );
    let expected = market.mint_summaries()?;
    ensure!(
        artifact.mints.len() == expected.len(),
        "mint metadata row count differs from the market mint set"
    );
    let mut ids = BTreeSet::new();
    let mut addresses = BTreeSet::new();
    for (record, mint) in artifact.mints.iter().zip(expected.iter()) {
        ensure!(
            record.mint == mint.mint
                && record.market_decimals == mint.decimals
                && record.is_target == mint.is_target
                && record.direct_usd_quote == mint.direct_usd_quote
                && record.trade_count == mint.trade_count,
            "mint metadata row differs from its exact market mint"
        );
        ensure!(
            ids.insert(record.mint.registry_id) && addresses.insert(&record.mint.address),
            "mint metadata contains a duplicate ID or address"
        );
        if record.mint_account_status == MintAccountStatus::Valid {
            ensure!(
                record.rpc_decimals == Some(record.market_decimals)
                    && record.token_program.is_some()
                    && record.mint_account_sha256.is_some()
                    && record.mint_context_slot.is_some_and(|slot| {
                        slot >= artifact.requested_min_context_slot
                            && slot <= artifact.maximum_context_slot
                    }),
                "valid mint account lacks its verified fields"
            );
        } else {
            ensure!(
                record.rpc_decimals.is_none() && record.token_program.is_none(),
                "invalid or absent mint account contains authoritative RPC fields"
            );
        }
        if let Some(display) = &record.display {
            ensure!(
                display.context_slot >= artifact.requested_min_context_slot
                    && display.context_slot <= artifact.maximum_context_slot
                    && (!display.metadata_account.is_empty())
                    && is_hex_digest(&display.metadata_account_sha256),
                "mint display metadata binding is invalid"
            );
            validate_optional_display_text(display.name.as_deref(), MAX_NAME_BYTES, "name")?;
            validate_optional_display_text(display.symbol.as_deref(), MAX_SYMBOL_BYTES, "symbol")?;
            validate_optional_uri(display.uri.as_deref())?;
        }
    }
    ensure!(
        artifact.counts == count_records(&artifact.mints)?,
        "mint metadata counters differ from the records"
    );
    Ok(())
}

fn empty_record(mint: &MarketMint) -> MintMetadataRecord {
    MintMetadataRecord {
        mint: mint.mint.clone(),
        market_decimals: mint.decimals,
        is_target: mint.is_target,
        direct_usd_quote: mint.direct_usd_quote,
        trade_count: mint.trade_count,
        mint_account_status: MintAccountStatus::Absent,
        token_program: None,
        rpc_decimals: None,
        mint_account_sha256: None,
        mint_context_slot: None,
        metadata_pointer_status: MetadataPointerStatus::NotApplicable,
        metadata_pointer: None,
        display: None,
        warnings: Vec::new(),
    }
}

fn decode_mint_account(
    mint_address: &str,
    owner: &str,
    data: &[u8],
    data_sha256: &str,
    context_slot: u64,
) -> Result<DecodedMint> {
    let program = match owner {
        LEGACY_TOKEN_PROGRAM => TokenProgramKind::Legacy,
        TOKEN_2022_PROGRAM => TokenProgramKind::Token2022,
        _ => bail!("unsupported mint owner {owner}"),
    };
    let state = StateWithExtensions::<Mint>::unpack(data)
        .map_err(|error| anyhow!("unpack initialized SPL mint: {error}"))?;
    ensure!(state.base.is_initialized, "SPL mint is not initialized");
    if program == TokenProgramKind::Legacy {
        ensure!(
            state
                .get_extension_types()
                .map_err(|error| anyhow!("read mint extensions: {error}"))?
                .is_empty(),
            "legacy SPL mint unexpectedly contains Token-2022 extensions"
        );
        return Ok(DecodedMint {
            program,
            decimals: state.base.decimals,
            pointer_status: MetadataPointerStatus::NotApplicable,
            pointer: None,
            token_2022_display: None,
            permit_metaplex_fallback: true,
            warnings: Vec::new(),
        });
    }

    let extension_types = state
        .get_extension_types()
        .map_err(|error| anyhow!("read Token-2022 extension types: {error}"))?;
    if !extension_types.contains(&ExtensionType::MetadataPointer) {
        return Ok(DecodedMint {
            program,
            decimals: state.base.decimals,
            pointer_status: MetadataPointerStatus::Absent,
            pointer: None,
            token_2022_display: None,
            permit_metaplex_fallback: true,
            warnings: Vec::new(),
        });
    }

    let pointer = match state.get_extension::<MetadataPointer>() {
        Ok(pointer) => pointer,
        Err(error) => {
            return Ok(DecodedMint {
                program,
                decimals: state.base.decimals,
                pointer_status: MetadataPointerStatus::Invalid,
                pointer: None,
                token_2022_display: None,
                permit_metaplex_fallback: false,
                warnings: vec![format!("metadata_pointer_invalid: {error}")],
            });
        }
    };
    let Some(pointer_address) = pointer.metadata_address.copied() else {
        return Ok(DecodedMint {
            program,
            decimals: state.base.decimals,
            pointer_status: MetadataPointerStatus::Null,
            pointer: None,
            token_2022_display: None,
            permit_metaplex_fallback: false,
            warnings: vec!["metadata_pointer_has_no_address".to_owned()],
        });
    };
    let pointer_bytes = pointer_address.to_bytes();
    let pointer_string = bs58::encode(pointer_bytes).into_string();
    let mint_bytes = decode_pubkey(mint_address, "mint")?;
    if pointer_bytes != mint_bytes {
        return Ok(DecodedMint {
            program,
            decimals: state.base.decimals,
            pointer_status: MetadataPointerStatus::ExternalUnresolved,
            pointer: Some(pointer_string),
            token_2022_display: None,
            permit_metaplex_fallback: false,
            warnings: vec!["external_metadata_pointer_unresolved".to_owned()],
        });
    }

    let mut warnings = Vec::new();
    let display = match state.get_variable_len_extension::<TokenMetadata>() {
        Ok(metadata) if metadata.mint.to_bytes() == mint_bytes => build_display(
            DisplayMetadataSource::Token2022,
            mint_address,
            data,
            context_slot,
            UntrustedDisplayMetadata {
                update_authority: metadata
                    .update_authority
                    .copied()
                    .map(|address| address.to_bytes()),
                name: metadata.name,
                symbol: metadata.symbol,
                uri: metadata.uri,
            },
            &mut warnings,
        )?,
        Ok(_) => {
            warnings.push("token_2022_metadata_mint_mismatch".to_owned());
            None
        }
        Err(error) => {
            warnings.push(format!("token_2022_metadata_decode_failed: {error}"));
            None
        }
    };
    ensure!(is_hex_digest(data_sha256), "mint account digest is invalid");
    Ok(DecodedMint {
        program,
        decimals: state.base.decimals,
        pointer_status: MetadataPointerStatus::SelfAddressed,
        pointer: Some(pointer_string),
        token_2022_display: display,
        permit_metaplex_fallback: false,
        warnings,
    })
}

fn build_display(
    source: DisplayMetadataSource,
    metadata_account: &str,
    account_data: &[u8],
    context_slot: u64,
    fields: UntrustedDisplayMetadata,
    warnings: &mut Vec<String>,
) -> Result<Option<MintDisplayMetadata>> {
    let name = clean_display_text(&fields.name, MAX_NAME_BYTES, "name", warnings);
    let symbol = clean_display_text(&fields.symbol, MAX_SYMBOL_BYTES, "symbol", warnings);
    let uri = clean_uri(&fields.uri, warnings);
    if name.is_none() && symbol.is_none() && uri.is_none() {
        warnings.push("display_metadata_empty".to_owned());
        return Ok(None);
    }
    Ok(Some(MintDisplayMetadata {
        source,
        metadata_account: metadata_account.to_owned(),
        metadata_account_sha256: hex_digest(Sha256::digest(account_data).into()),
        context_slot,
        update_authority: fields
            .update_authority
            .map(|key| bs58::encode(key).into_string()),
        name,
        symbol,
        uri,
    }))
}

fn clean_display_text(
    value: &str,
    maximum: usize,
    label: &str,
    warnings: &mut Vec<String>,
) -> Option<String> {
    let value = value.trim();
    let value = value.trim_end_matches('\0').trim_end();
    if value.is_empty() {
        return None;
    }
    if value.len() > maximum
        || value.contains('\0')
        || value.chars().any(|character| character.is_control())
    {
        warnings.push(format!("invalid_{label}"));
        return None;
    }
    Some(value.to_owned())
}

fn clean_uri(value: &str, warnings: &mut Vec<String>) -> Option<String> {
    let uri = clean_display_text(value, MAX_URI_BYTES, "uri", warnings)?;
    if is_permitted_uri(&uri) {
        Some(uri)
    } else {
        warnings.push("unsupported_metadata_uri".to_owned());
        None
    }
}

fn decode_metaplex_metadata(data: &[u8]) -> Result<DecodedMetaplex> {
    ensure!(
        data.first().copied() == Some(METAPLEX_METADATA_V1_KEY),
        "Metaplex account key is not MetadataV1"
    );
    let mut cursor = 1usize;
    let update_authority = read_pubkey(data, &mut cursor)?;
    let mint = read_pubkey(data, &mut cursor)?;
    let name = read_borsh_string(data, &mut cursor, MAX_METAPLEX_NAME_BYTES)?;
    let symbol = read_borsh_string(data, &mut cursor, MAX_METAPLEX_SYMBOL_BYTES)?;
    let uri = read_borsh_string(data, &mut cursor, MAX_METAPLEX_URI_BYTES)?;
    Ok(DecodedMetaplex {
        update_authority,
        mint,
        name,
        symbol,
        uri,
    })
}

fn read_pubkey(data: &[u8], cursor: &mut usize) -> Result<[u8; 32]> {
    let end = cursor.checked_add(32).context("pubkey offset overflow")?;
    let bytes = data
        .get(*cursor..end)
        .context("metadata pubkey is out of bounds")?;
    *cursor = end;
    Ok(bytes.try_into().expect("validated metadata pubkey length"))
}

fn read_borsh_string(data: &[u8], cursor: &mut usize, maximum: usize) -> Result<String> {
    let length_end = cursor.checked_add(4).context("string offset overflow")?;
    let length_bytes = data
        .get(*cursor..length_end)
        .context("metadata string length is out of bounds")?;
    *cursor = length_end;
    let length = u32::from_le_bytes(
        length_bytes
            .try_into()
            .expect("validated metadata string length"),
    ) as usize;
    ensure!(length <= maximum, "metadata string exceeds its size limit");
    let value_end = cursor
        .checked_add(length)
        .context("metadata string offset overflow")?;
    let bytes = data
        .get(*cursor..value_end)
        .context("metadata string is out of bounds")?;
    *cursor = value_end;
    std::str::from_utf8(bytes)
        .context("metadata string is not valid UTF-8")
        .map(str::to_owned)
}

fn metaplex_pda(mint: &str) -> Result<String> {
    let program =
        Pubkey::from_str(METAPLEX_METADATA_PROGRAM).context("parse Metaplex metadata program")?;
    let mint = Pubkey::from_str(mint).context("parse mint for Metaplex PDA")?;
    let (address, _) =
        Pubkey::find_program_address(&[b"metadata", program.as_ref(), mint.as_ref()], &program);
    Ok(address.to_string())
}

async fn rpc_genesis_hash(client: &reqwest::Client, rpc_url: &str) -> Result<String> {
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getGenesisHash"
        }))
        .send()
        .await
        .context("send getGenesisHash request")?
        .error_for_status()
        .context("getGenesisHash HTTP error")?
        .json::<RpcStringResponse>()
        .await
        .context("decode getGenesisHash response")?;
    ensure!(
        response.jsonrpc == "2.0" && response.id == 1,
        "getGenesisHash response envelope is invalid"
    );
    if let Some(error) = response.error {
        bail!("Solana RPC error {}: {}", error.code, error.message);
    }
    response.result.context("getGenesisHash result is absent")
}

async fn rpc_multiple_accounts(
    client: &reqwest::Client,
    rpc_url: &str,
    addresses: &[String],
    minimum_context_slot: u64,
) -> Result<RpcAccountsResult> {
    ensure!(
        !addresses.is_empty() && addresses.len() <= MAX_RPC_BATCH_SIZE,
        "invalid getMultipleAccounts batch size"
    );
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [
                addresses,
                {
                    "encoding": "base64",
                    "commitment": "finalized",
                    "minContextSlot": minimum_context_slot
                }
            ]
        }))
        .send()
        .await
        .context("send getMultipleAccounts request")?
        .error_for_status()
        .context("getMultipleAccounts HTTP error")?
        .json::<RpcAccountsResponse>()
        .await
        .context("decode getMultipleAccounts response")?;
    ensure!(
        response.jsonrpc == "2.0" && response.id == 1,
        "getMultipleAccounts response envelope is invalid"
    );
    if let Some(error) = response.error {
        bail!("Solana RPC error {}: {}", error.code, error.message);
    }
    response
        .result
        .context("getMultipleAccounts result is absent")
}

fn rpc_account_data(account: &RpcAccount) -> Result<Vec<u8>> {
    ensure!(
        !account.executable,
        "RPC token metadata account is executable"
    );
    let items = account
        .data
        .as_array()
        .context("RPC account data is not an encoded tuple")?;
    ensure!(
        items.len() == 2 && items.get(1).and_then(Value::as_str) == Some("base64"),
        "RPC account data encoding is not exact base64"
    );
    let encoded = items
        .first()
        .and_then(Value::as_str)
        .context("RPC account base64 value is absent")?;
    let maximum_encoded = MAX_ACCOUNT_DATA_BYTES
        .checked_mul(4)
        .and_then(|value| value.checked_div(3))
        .and_then(|value| value.checked_add(8))
        .context("account data bound overflow")?;
    ensure!(
        encoded.len() <= maximum_encoded,
        "RPC account data exceeds its size limit"
    );
    let data = BASE64
        .decode(encoded)
        .context("decode RPC account base64")?;
    ensure!(
        data.len() <= MAX_ACCOUNT_DATA_BYTES,
        "decoded RPC account exceeds its size limit"
    );
    if let Some(space) = account.space {
        ensure!(
            u64::try_from(data.len()).context("RPC account data length exceeds u64")? == space,
            "decoded RPC account size differs from its declared space"
        );
    }
    Ok(data)
}

fn market_binding(market: &MarketStore) -> Result<MintMetadataMarketBinding> {
    let provenance = market.provenance();
    let target = market
        .mint_summaries()?
        .into_iter()
        .find(|mint| mint.is_target)
        .context("market mint set has no target")?;
    Ok(MintMetadataMarketBinding {
        market_schema_version: provenance.schema_version,
        market_manifest_sha256: provenance.market_manifest_sha256.clone(),
        market_trade_file_sha256: provenance.market_trade_file_sha256.clone(),
        source_manifest_sha256: provenance.source.manifest_sha256.clone(),
        source_transaction_sha256: provenance.source.transaction_sha256.clone(),
        source_registry_sha256: provenance.source.registry_sha256.clone(),
        target_mint: target.mint.address,
        target_mint_id: target.mint.registry_id,
    })
}

fn count_records(records: &[MintMetadataRecord]) -> Result<MintMetadataCounts> {
    let mut counts = MintMetadataCounts {
        mints: u64::try_from(records.len()).context("mint metadata count exceeds u64")?,
        ..MintMetadataCounts::default()
    };
    for record in records {
        match record.mint_account_status {
            MintAccountStatus::Valid => counts.valid_mint_accounts += 1,
            MintAccountStatus::Absent => counts.absent_mint_accounts += 1,
            MintAccountStatus::InvalidOwner | MintAccountStatus::InvalidData => {
                counts.invalid_mint_accounts += 1;
            }
        }
        if let Some(display) = &record.display {
            counts.named_mints += u64::from(display.name.is_some());
            counts.symbol_mints += u64::from(display.symbol.is_some());
            match display.source {
                DisplayMetadataSource::Token2022 => counts.token_2022_metadata += 1,
                DisplayMetadataSource::Metaplex => counts.metaplex_metadata += 1,
            }
        } else {
            counts.unresolved_metadata += 1;
        }
    }
    Ok(counts)
}

fn prepare_output_directory(output: &Path) -> Result<()> {
    match fs::symlink_metadata(output) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_dir(),
                "mint metadata output exists and is not a directory"
            );
            ensure!(
                fs::read_dir(output)
                    .with_context(|| format!("read output directory {}", output.display()))?
                    .next()
                    .is_none(),
                "mint metadata output directory is not empty"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(output)
                .with_context(|| format!("create output directory {}", output.display()))?;
        }
        Err(error) => return Err(error).context("inspect mint metadata output"),
    }
    Ok(())
}

fn write_artifact(output: &Path, artifact: &MintMetadataArtifact) -> Result<(PathBuf, String)> {
    let partial = output.join(format!(".{MINT_METADATA_FILE}.partial"));
    let final_path = output.join(MINT_METADATA_FILE);
    let mut bytes = serde_json::to_vec_pretty(artifact).context("encode mint metadata artifact")?;
    bytes.push(b'\n');
    ensure!(
        u64::try_from(bytes.len()).context("mint metadata artifact exceeds u64")?
            <= MAX_ARTIFACT_BYTES,
        "mint metadata artifact exceeds its size limit"
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)
        .with_context(|| format!("create partial artifact {}", partial.display()))?;
    file.write_all(&bytes)
        .context("write mint metadata artifact")?;
    file.sync_all().context("sync mint metadata artifact")?;
    drop(file);
    fs::rename(&partial, &final_path).context("publish mint metadata artifact")?;
    File::open(output)
        .context("open mint metadata output directory")?
        .sync_all()
        .context("sync mint metadata output directory")?;
    Ok((final_path, hex_digest(Sha256::digest(&bytes).into())))
}

fn summary_from_artifact(
    artifact: &MintMetadataArtifact,
    path: PathBuf,
    artifact_sha256: String,
) -> MintMetadataBuildSummary {
    MintMetadataBuildSummary {
        output: path.display().to_string(),
        artifact_sha256,
        mints: artifact.counts.mints,
        valid_mint_accounts: artifact.counts.valid_mint_accounts,
        absent_mint_accounts: artifact.counts.absent_mint_accounts,
        invalid_mint_accounts: artifact.counts.invalid_mint_accounts,
        named_mints: artifact.counts.named_mints,
        symbol_mints: artifact.counts.symbol_mints,
        token_2022_metadata: artifact.counts.token_2022_metadata,
        metaplex_metadata: artifact.counts.metaplex_metadata,
        unresolved_metadata: artifact.counts.unresolved_metadata,
        minimum_context_slot: artifact.minimum_context_slot,
        maximum_context_slot: artifact.maximum_context_slot,
    }
}

fn validate_optional_display_text(value: Option<&str>, maximum: usize, label: &str) -> Result<()> {
    if let Some(value) = value {
        ensure!(
            !value.is_empty()
                && value.len() <= maximum
                && !value.contains('\0')
                && !value.chars().any(|character| character.is_control()),
            "mint metadata {label} is invalid"
        );
    }
    Ok(())
}

fn validate_optional_uri(uri: Option<&str>) -> Result<()> {
    validate_optional_display_text(uri, MAX_URI_BYTES, "URI")?;
    if let Some(uri) = uri {
        ensure!(is_permitted_uri(uri), "mint metadata URI is not permitted");
    }
    Ok(())
}

fn is_permitted_uri(uri: &str) -> bool {
    if uri.starts_with("https://") {
        return reqwest::Url::parse(uri).is_ok_and(|parsed| {
            parsed.scheme() == "https"
                && parsed.host_str().is_some()
                && parsed.username().is_empty()
                && parsed.password().is_none()
        });
    }
    ["ipfs://", "ar://"].iter().any(|prefix| {
        uri.strip_prefix(prefix)
            .is_some_and(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))
    })
}

fn decode_pubkey(value: &str, label: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode {label} as base58"))?;
    ensure!(bytes.len() == 32, "{label} is not 32 bytes");
    Ok(bytes.try_into().expect("validated public key length"))
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("write to String");
    }
    out
}

fn is_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_metaplex_decoder_checks_key_mint_and_utf8() {
        let mut data = vec![METAPLEX_METADATA_V1_KEY];
        data.extend_from_slice(&[7; 32]);
        data.extend_from_slice(&[9; 32]);
        write_borsh_string(&mut data, "Wrapped SOL\0\0");
        write_borsh_string(&mut data, "SOL");
        write_borsh_string(&mut data, "https://example.invalid/sol.json");
        let decoded = decode_metaplex_metadata(&data).expect("decode metadata");
        assert_eq!(decoded.mint, [9; 32]);
        assert_eq!(decoded.name, "Wrapped SOL\0\0");

        data[0] = 5;
        assert!(decode_metaplex_metadata(&data).is_err());
    }

    #[test]
    fn display_text_rejects_internal_nul_and_unsafe_uri() {
        let mut warnings = Vec::new();
        assert_eq!(
            clean_display_text(" SPYx\0\0 ", MAX_NAME_BYTES, "name", &mut warnings),
            Some("SPYx".to_owned())
        );
        assert_eq!(
            clean_display_text("bad\0name", MAX_NAME_BYTES, "name", &mut warnings),
            None
        );
        assert_eq!(clean_uri("http://example.invalid", &mut warnings), None);
    }

    fn write_borsh_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
}

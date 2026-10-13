use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    normalize_archive_v2_historical_source_metadata_owned,
};
use blockzilla_compact_v2_reader::{
    BorrowedDecodedBlock, CompactPubkey, CompactV2MessageProjector, CompactV2MessageSchema,
    CompactV2MetadataProjector, CompactV2MetadataSchema, CompiledPubkeyFilter, NetworkInputConfig,
    OrderedParallelBlockConfig, ProjectedCompactV2Message, ProjectedCompactV2Metadata,
    source_decode::decode_metadata_error_with_schema,
};
use blockzilla_dump::scan::{DumpArchive, DumpSource, SourceOptions, prepare_epoch};

use crate::{Args, Order, database::Database};

pub const STATE_ADDRESS: &str = "73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz";
pub const CLASSIC_ADDRESS: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022_ADDRESS: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

// The filter contains only State and the two token program keys, with IDs bound to one reader.
type StateCompiledPubkeyFilter = CompiledPubkeyFilter;
type Archive = DumpArchive;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Approval {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub outer_ix: u32,
    /// -1 denotes a top-level instruction; CPI ordinals start at 0 within their outer group.
    pub inner_ix: i64,
    /// -1 for a normal instruction; otherwise the child ordinal of Token Batch (255).
    pub batch_ix: i64,
    pub token_account: String,
    pub token_program: String,
    pub authority: String,
    pub tag: u8,
    /// Preserve the full u64 without SQLite signed-integer conversion.
    pub amount: String,
    pub mint: Option<String>,
    pub decimals: Option<u8>,
    pub signature: String,
    pub data: Vec<u8>,
    pub accounts: Vec<u8>,
    pub parent_data: Option<Vec<u8>>,
    pub parent_accounts: Option<Vec<u8>>,
}

pub struct BlockProjection {
    pub row: usize,
    pub slot: u64,
    pub block_id: u32,
    pub transactions: u64,
    pub events: Vec<Approval>,
}

struct Targets {
    state: [u8; 32],
    classic: [u8; 32],
    token_2022: [u8; 32],
}

impl Targets {
    fn new() -> Result<Self> {
        Ok(Self {
            state: key(STATE_ADDRESS)?,
            classic: key(CLASSIC_ADDRESS)?,
            token_2022: key(TOKEN_2022_ADDRESS)?,
        })
    }
}

#[derive(Clone, Copy)]
enum MetadataPolicy {
    Fixed(CompactV2MetadataSchema),
    HistoricalCompatibility,
}

fn schemas(args: &Args) -> Result<(CompactV2MessageSchema, MetadataPolicy)> {
    let message = match args.message_schema.as_str() {
        "current" | "post-unknown-instruction-fallbacks-v1" => CompactV2MessageSchema::Current,
        "may24-pre-unknown-fallbacks" | "pre-unknown-instruction-fallbacks-v1" => {
            CompactV2MessageSchema::May24PreUnknownFallbacks
        }
        other => bail!("unsupported message schema {other}"),
    };
    let metadata = match args.metadata_schema.as_str() {
        "current-typed-error" => MetadataPolicy::Fixed(CompactV2MetadataSchema::CurrentTypedError),
        "legacy-raw-error" => MetadataPolicy::Fixed(CompactV2MetadataSchema::LegacyRawError),
        "unmarked-historical-compatibility" => MetadataPolicy::HistoricalCompatibility,
        other => bail!("unsupported metadata schema {other}"),
    };
    Ok((message, metadata))
}

pub fn validate_source_arguments(args: &Args) -> Result<()> {
    match (&args.archive_root, &args.gateway) {
        (Some(_), None) => ensure!(
            args.source_generation_prefix
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty()),
            "source-generation-prefix is empty"
        ),
        (None, Some(gateway)) => {
            let parsed = url::Url::parse(gateway).context("parse gateway URL")?;
            ensure!(
                parsed.username().is_empty() && parsed.password().is_none(),
                "gateway URL must not contain credentials"
            );
            ensure!(
                parsed.fragment().is_none() && parsed.query().is_none(),
                "gateway URL must not contain a query or fragment"
            );
            if parsed.scheme() == "http" {
                let literal_loopback = matches!(parsed.host(),Some(url::Host::Ipv4(address)) if address == std::net::Ipv4Addr::LOCALHOST)
                    || matches!(parsed.host(),Some(url::Host::Ipv6(address)) if address == std::net::Ipv6Addr::LOCALHOST);
                ensure!(
                    args.allow_insecure_http && literal_loopback,
                    "HTTP requires --allow-insecure-http and a literal loopback gateway address"
                );
            } else {
                ensure!(
                    parsed.scheme() == "https",
                    "gateway must use HTTPS or explicit loopback HTTP"
                );
            }
        }
        _ => bail!("use exactly one archive-root or gateway"),
    }
    let (message, metadata) = schemas(args)?;
    source_options(args, message, metadata).validate()
}

fn source_options(
    args: &Args,
    message_schema: CompactV2MessageSchema,
    policy: MetadataPolicy,
) -> SourceOptions {
    SourceOptions {
        archive: args.archive_root.clone(),
        gateway: args.gateway.clone(),
        bearer_token: None,
        cache: args.cache.clone(),
        allow_insecure_http: args.allow_insecure_http,
        cluster_id: "mainnet-beta".into(),
        local_generation_prefix: args.source_generation_prefix.clone(),
        epoch_zero_first_slot: 0,
        slots_per_epoch: 432_000,
        message_schema,
        metadata_schema: match policy {
            MetadataPolicy::Fixed(schema) => schema,
            MetadataPolicy::HistoricalCompatibility => CompactV2MetadataSchema::CurrentTypedError,
        },
    }
}

pub fn run(args: &Args, db: &mut Database) -> Result<bool> {
    let (message_schema, metadata_policy) = schemas(args)?;
    let targets = Targets::new()?;
    let mut epochs: Vec<_> = (args.epoch_start..=args.epoch_end).collect();
    if matches!(args.order, Order::Descending) {
        epochs.reverse();
    }
    let mut remaining = args.max_blocks.unwrap_or(usize::MAX);
    for epoch in epochs {
        if remaining == 0 {
            return Ok(false);
        }
        let result = scan_epoch(
            args,
            db,
            epoch,
            &targets,
            message_schema,
            metadata_policy,
            &mut remaining,
        );
        if let Err(error) = result {
            db.fail_epoch(epoch, &format!("{error:#}"))?;
            return Err(error).with_context(|| {
                format!("coverage unknown in epoch {epoch}; committed candidates remain available")
            });
        }
        if !result? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn scan_epoch(
    args: &Args,
    db: &mut Database,
    epoch: u64,
    targets: &Targets,
    message_schema: CompactV2MessageSchema,
    metadata_policy: MetadataPolicy,
    remaining: &mut usize,
) -> Result<bool> {
    let prepared = prepare_epoch(
        &source_options(args, message_schema, metadata_policy),
        epoch,
    )
    .context("admit immutable Compact V2 source")?;
    let archive = &prepared.archive;
    let reader_metadata_schema = archive.metadata_schema();
    ensure!(
        archive.signatures_available(),
        "source does not contain signatures; event proof is incomplete"
    );
    let binding = archive.binding();
    let descriptor = archive
        .archive_descriptor()
        .context("missing admitted source descriptor")?;
    let binding_json = match archive.source() {
        DumpSource::Local(source) => serde_json::to_string(&serde_json::json!({
            "kind":"operator-pinned-local-compact-v2-v1", "root":prepared.source_root,
            "cluster_id":descriptor.identity.cluster_id, "epoch":epoch,
            "generation_id":descriptor.identity.generation_id,
            "first_slot":descriptor.identity.first_slot,"slots_per_epoch":descriptor.identity.slots_per_epoch,
            "message_schema":args.message_schema, "metadata_schema":args.metadata_schema,
            "reader_success_metadata_schema":format!("{reader_metadata_schema:?}"),
            "generation_digest":hex(&binding.generation_digest), "registry_sha256":hex(&binding.registry_sha256),
            "file_identities":source.pinned_object_identities()?,
        }))?,
        DumpSource::Gateway(_) => serde_json::to_string(&serde_json::json!({
            "kind":"strong-etag-compact-v2-v1", "source_identity":prepared.source_identity,
            "descriptor":serde_json::from_str::<serde_json::Value>(&prepared.source_descriptor_json)?,
            "message_schema":args.message_schema,"metadata_schema":args.metadata_schema,
            "generation_digest":hex(&binding.generation_digest),"registry_sha256":hex(&binding.registry_sha256),
        }))?,
    };
    let total = archive.index().rows.len();
    let first = archive
        .index()
        .rows
        .partition_point(|row| row.slot < args.start_slot);
    let start = db.begin_epoch(epoch, &binding_json, first, total)?;
    let end = total.min(start.saturating_add(*remaining));
    let filter: StateCompiledPubkeyFilter =
        archive.compile_pubkey_filter([targets.state, targets.classic, targets.token_2022])?;
    let programs: Vec<_> = [targets.classic, targets.token_2022]
        .into_iter()
        .flat_map(|program| {
            std::iter::once(CompactPubkey::Raw(program))
                .chain(filter.registry_id_for(&program).map(CompactPubkey::Id))
        })
        .collect();
    let config = scan_config(
        args.threads,
        matches!(archive.source(), DumpSource::Gateway(_)),
    );
    let mut pending = Vec::with_capacity(usize::from(args.checkpoint_blocks));
    let began = Instant::now();
    archive.process_borrowed_blocks_parallel_ordered(
        start..end,
        config,
        |_| Ok::<_, anyhow::Error>(()),
        |_, row, block| {
            project_block(
                &archive,
                &filter,
                targets,
                &programs,
                metadata_policy,
                row,
                block,
            )
        },
        |_, projection| {
            pending.push(projection);
            if pending.len() == usize::from(args.checkpoint_blocks) {
                if let DumpSource::Local(source) = archive.source() {
                    source
                        .verify_unchanged()
                        .context("source changed before durable commit")?;
                }
                db.commit(epoch, &pending)?;
                let next = pending.last().expect("non-empty batch").row + 1;
                eprintln!(
                    "epoch={epoch} next_row={next}/{total} new_blocks={} elapsed_s={:.1}",
                    next - start,
                    began.elapsed().as_secs_f64()
                );
                pending.clear();
                db.export(
                    &args
                        .candidates
                        .clone()
                        .unwrap_or_else(|| args.output.with_extension("candidates.txt")),
                    &args
                        .status
                        .clone()
                        .unwrap_or_else(|| args.output.with_extension("status.json")),
                    args.epoch_start,
                    args.epoch_end,
                    "scanning",
                    None,
                )?;
            }
            Ok(())
        },
    )?;
    prepared
        .verify_source_unchanged()
        .context("source changed at epoch boundary")?;
    db.commit(epoch, &pending)?;
    *remaining = remaining.saturating_sub(end - start);
    if end == total {
        db.finish_epoch(epoch)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn scan_config(threads: usize, gateway: bool) -> OrderedParallelBlockConfig {
    OrderedParallelBlockConfig {
        decode_workers: threads,
        compressed_buffer_count: threads.min(3),
        max_blocks_per_batch: 500,
        uncompressed_batch_budget_bytes: 128 * 1024 * 1024,
        retained_decompressed_bytes_per_worker: 32 * 1024 * 1024,
        discard_rewards: true,
        network_input: gateway.then(NetworkInputConfig::default),
        ..OrderedParallelBlockConfig::default()
    }
}

fn project_block(
    archive: &Archive,
    filter: &StateCompiledPubkeyFilter,
    targets: &Targets,
    programs: &[CompactPubkey],
    policy: MetadataPolicy,
    row_number: usize,
    block: BorrowedDecodedBlock<'_>,
) -> Result<BlockProjection> {
    let mut projection = BlockProjection {
        row: row_number,
        slot: block.header().slot,
        block_id: block.index_row.block_id,
        transactions: u64::from(block.tx_count()),
        events: Vec::new(),
    };
    let mut signature_ordinal = block.index_row.first_signature_ordinal;
    let message_projector =
        CompactV2MessageProjector::new(archive.message_schema(), archive.registry_entries());
    for row in block.tx_rows() {
        let first_ordinal = signature_ordinal;
        signature_ordinal = signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .context("signature ordinal overflow")?;
        let result = (|| {
            let messages = lane(block.message_bytes(), row.message_offset, row.message_len)?;
            let metadata = lane(
                block.metadata_bytes(),
                row.metadata_offset,
                row.metadata_len,
            )?;
            let mut events = project_transaction(
                message_projector,
                programs,
                policy,
                targets,
                row.flags,
                messages,
                metadata,
                |reference, key| filter.matches_reference(reference, key),
                |reference| {
                    archive
                        .resolve_pubkey(reference)
                        .map_err(anyhow::Error::from)
                },
                archive.epoch(),
                projection.slot,
                row.tx_index,
            )?;
            if !events.is_empty() {
                ensure!(
                    row.signature_count > 0,
                    "selected transaction has no signature"
                );
                let signature =
                    bs58::encode(archive.read_signature_ordinal(first_ordinal)?).into_string();
                for event in &mut events {
                    event.signature = signature.clone();
                }
            }
            Ok(events)
        })();
        projection.events.extend(
            result.with_context(|| {
                format!("slot {} transaction {}", projection.slot, row.tx_index)
            })?,
        );
    }
    Ok(projection)
}

#[allow(clippy::too_many_arguments)]
fn project_transaction(
    message_projector: CompactV2MessageProjector,
    programs: &[CompactPubkey],
    policy: MetadataPolicy,
    targets: &Targets,
    flags: u32,
    messages: &[u8],
    metadata_bytes: &[u8],
    matched: impl Fn(&CompactPubkey, &[u8; 32]) -> bool,
    resolve: impl Fn(&CompactPubkey) -> Result<[u8; 32]>,
    epoch: u64,
    slot: u64,
    tx_index: u32,
) -> Result<Vec<Approval>> {
    // HAS_ERROR is an archived execution outcome, not a token-state mutation. Its error
    // grammar is irrelevant to candidate enumeration, including failed raw fallbacks.
    if flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
        return Ok(Vec::new());
    }
    ensure!(
        flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0,
        "raw transaction fallback prevents exact account-space proof"
    );
    // Counts-only projection deliberately discards static keys. An empty selected
    // program set preserves the exact key space without retaining instruction data.
    let counts = message_projector
        .project_with_instruction_data_for_programs(messages, &[], None)
        .context("decode exact message key space")?;
    let static_state = counts
        .static_account_keys()
        .iter()
        .any(|r| matched(r, &targets.state));
    let loaded_count =
        counts.count_limits().total_message_accounts - counts.static_account_keys().len();
    if !static_state && loaded_count == 0 {
        return Ok(Vec::new());
    }
    ensure!(
        flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0,
        "metadata absent: execution or loaded key space unknown"
    );
    ensure!(
        flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0,
        "raw metadata fallback: execution or loaded key space unknown"
    );
    // For unmarked compatibility the error-free layout is identical in both schemas.
    // A present error with no HAS_ERROR flag is inconsistent and cannot be admitted.
    ensure!(
        !metadata_failed(metadata_bytes, policy)?,
        "metadata records failure but HAS_ERROR is absent"
    );
    let metadata_schema = match policy {
        MetadataPolicy::Fixed(schema) => schema,
        MetadataPolicy::HistoricalCompatibility => CompactV2MetadataSchema::CurrentTypedError,
    };
    let metadata =
        CompactV2MetadataProjector::new(metadata_schema, message_projector.registry_entries())
            .project(metadata_bytes, counts.count_limits())
            .context("decode exact metadata and loaded keys")?;
    ensure!(
        metadata.execution_status.is_success(),
        "selected transaction execution is not successful"
    );
    let all_keys: Vec<_> = counts
        .static_account_keys()
        .iter()
        .chain(metadata.loaded_writable_addresses.iter())
        .chain(metadata.loaded_readonly_addresses.iter())
        .copied()
        .collect();
    if !all_keys.iter().any(|r| matched(r, &targets.state)) {
        return Ok(Vec::new());
    }
    if !all_keys
        .iter()
        .any(|r| matched(r, &targets.classic) || matched(r, &targets.token_2022))
    {
        return Ok(Vec::new());
    }
    let message = message_projector
        .project_with_instruction_data_for_programs(messages, programs, None)
        .context("decode selected token instruction bytes")?;
    extract_transaction(
        &message, &metadata, &all_keys, targets, matched, resolve, epoch, slot, tx_index,
    )
}

fn metadata_failed(bytes: &[u8], policy: MetadataPolicy) -> Result<bool> {
    ensure!(!bytes.is_empty(), "empty metadata: execution unknown");
    match policy {
        MetadataPolicy::Fixed(schema) => {
            Ok(decode_metadata_error_with_schema(&mut &bytes[..], schema)
                .context("decode recorded execution status")?)
        }
        MetadataPolicy::HistoricalCompatibility => match bytes[0] {
            0 => Ok(false),
            1 => {
                let (normalized, _) = normalize_archive_v2_historical_source_metadata_owned(bytes)
                    .context("ambiguous or incomplete historical execution metadata")?;
                Ok(decode_metadata_error_with_schema(
                    &mut normalized.as_slice(),
                    CompactV2MetadataSchema::CurrentTypedError,
                )?)
            }
            _ => bail!("invalid historical execution option"),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn extract_transaction(
    message: &ProjectedCompactV2Message<'_>,
    metadata: &ProjectedCompactV2Metadata<'_>,
    keys: &[CompactPubkey],
    targets: &Targets,
    matched: impl Fn(&CompactPubkey, &[u8; 32]) -> bool,
    resolve: impl Fn(&CompactPubkey) -> Result<[u8; 32]>,
    epoch: u64,
    slot: u64,
    tx_index: u32,
) -> Result<Vec<Approval>> {
    if !metadata.execution_status.is_success() {
        return Ok(Vec::new());
    }
    let groups = metadata
        .inner_instructions
        .as_ref()
        .context("State and token program are present but CPI recording is absent")?;
    let mut events = Vec::new();
    let mut visit = |program_index: usize,
                     accounts: &[u8],
                     data: &[u8],
                     outer_ix: u32,
                     inner_ix: i64|
     -> Result<()> {
        let program = keys
            .get(program_index)
            .context("program index outside resolved account space")?;
        let program_address = if matched(program, &targets.classic) {
            CLASSIC_ADDRESS
        } else if matched(program, &targets.token_2022) {
            TOKEN_2022_ADDRESS
        } else {
            return Ok(());
        };
        visit_token_children(accounts, data, |batch_ix, accounts, child_data| {
            let Some(decoded) = decode_approval(child_data)? else {
                return Ok(());
            };
            let (delegate_index, authority_index, min_accounts) = if decoded.tag == 4 {
                (1, 2, 3)
            } else {
                (2, 3, 4)
            };
            ensure!(
                accounts.len() >= min_accounts,
                "approval account list is incomplete"
            );
            let account = |position: usize| -> Result<&CompactPubkey> {
                keys.get(usize::from(accounts[position]))
                    .context("approval account index outside resolved account space")
            };
            if !matched(account(delegate_index)?, &targets.state) {
                return Ok(());
            }
            events.push(Approval {
                epoch,
                slot,
                tx_index,
                outer_ix,
                inner_ix,
                batch_ix,
                token_account: bs58::encode(resolve(account(0)?)?).into_string(),
                token_program: program_address.into(),
                authority: bs58::encode(resolve(account(authority_index)?)?).into_string(),
                tag: decoded.tag,
                amount: decoded.amount.to_string(),
                mint: if decoded.tag == 13 {
                    Some(bs58::encode(resolve(account(1)?)?).into_string())
                } else {
                    None
                },
                decimals: decoded.decimals,
                signature: String::new(),
                data: child_data.to_vec(),
                accounts: accounts.to_vec(),
                parent_data: (batch_ix >= 0).then(|| data.to_vec()),
                parent_accounts: None,
            });
            Ok(())
        })?;
        // Preserve the exact parent account list for every matching Batch child.
        for event in events.iter_mut().rev().take_while(|event| {
            event.outer_ix == outer_ix && event.inner_ix == inner_ix && event.batch_ix >= 0
        }) {
            event.parent_accounts = Some(accounts.to_vec());
        }
        Ok(())
    };
    for (outer_ix, instruction) in message.instructions().iter().enumerate() {
        let program = keys
            .get(usize::from(instruction.program_id_index()))
            .context("outer program index out of bounds")?;
        if !matched(program, &targets.classic) && !matched(program, &targets.token_2022) {
            continue;
        }
        let candidates = instruction
            .data_candidates()
            .context("selected token instruction bytes unavailable")?;
        ensure!(
            candidates.len() == 1,
            "selected token instruction bytes are not exact"
        );
        visit(
            usize::from(instruction.program_id_index()),
            instruction.accounts(),
            &candidates[0].bytes,
            outer_ix as u32,
            -1,
        )?;
    }
    for group in groups {
        for (inner_ix, instruction) in group.instructions.iter().enumerate() {
            visit(
                instruction.program_id_index as usize,
                instruction.accounts,
                instruction.data,
                group.outer_instruction_index,
                inner_ix as i64,
            )?;
        }
    }
    Ok(events)
}

struct DecodedApproval {
    tag: u8,
    amount: u64,
    decimals: Option<u8>,
}
fn decode_approval(bytes: &[u8]) -> Result<Option<DecodedApproval>> {
    let Some(tag) = bytes.first().copied() else {
        return Ok(None);
    };
    if tag != 4 && tag != 13 {
        return Ok(None);
    }
    let expected = if tag == 4 { 9 } else { 10 };
    ensure!(
        bytes.len() >= expected,
        "approval data has fewer than the required {expected} bytes"
    );
    Ok(Some(DecodedApproval {
        tag,
        amount: u64::from_le_bytes(bytes[1..9].try_into().expect("validated length")),
        decimals: (tag == 13).then(|| bytes[9]),
    }))
}

/// Token Batch concatenates each child's account list. Headers are account-count,
/// data-length (one byte each), followed by that child's exact data. Nested Batch
/// is prohibited by both token processors. Extra parent accounts may be unused.
fn visit_token_children<'a>(
    accounts: &'a [u8],
    data: &'a [u8],
    mut visit: impl FnMut(i64, &'a [u8], &'a [u8]) -> Result<()>,
) -> Result<()> {
    if data.first() != Some(&255) {
        return visit(-1, accounts, data);
    }
    ensure!(
        data.len() <= u16::MAX as usize && accounts.len() <= u16::MAX as usize,
        "Token Batch source geometry exceeds protocol bounds"
    );
    ensure!(data.len() > 1, "empty Token Batch");
    let mut data_position = 1usize;
    let mut account_position = 0usize;
    let mut batch_ix = 0i64;
    while data_position < data.len() {
        let header = data
            .get(data_position..data_position + 2)
            .context("truncated Token Batch header")?;
        data_position += 2;
        let length = usize::from(header[1]);
        ensure!(length > 0, "empty Token Batch child data");
        let child = data
            .get(data_position..data_position + length)
            .context("Token Batch child data overrun")?;
        let child_accounts = accounts
            .get(account_position..account_position + usize::from(header[0]))
            .context("Token Batch child account overrun")?;
        ensure!(
            child.first() != Some(&255),
            "nested Token Batch is not permitted"
        );
        visit(batch_ix, child_accounts, child)?;
        batch_ix += 1;
        data_position += length;
        account_position += usize::from(header[0]);
    }
    Ok(())
}

fn lane(bytes: &[u8], offset: u32, length: u32) -> Result<&[u8]> {
    let end = (offset as usize)
        .checked_add(length as usize)
        .context("lane range overflow")?;
    bytes
        .get(offset as usize..end)
        .context("incomplete transaction lane")
}
fn key(address: &str) -> Result<[u8; 32]> {
    bs58::decode(address)
        .into_vec()?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid target public key"))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[cfg(test)]
mod tests;

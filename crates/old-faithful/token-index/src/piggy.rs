use crate::format::ProgramKind;
use crate::piggy_meta::{
    PiggyInnerInstruction, PiggyMeta, PiggyTokenBalance, decode_piggy_meta_from_protobuf,
    piggy_meta_from_owned,
};
use anyhow::{Context, Result, bail};
use of_car_reader::{
    CarBlockReader,
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::{
        ZstdReusableDecoder, decode_transaction_status_meta_into, slot_uses_protobuf_metadata,
    },
    node::{Node, decode_node, peek_node_type},
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const CAR_BUF: usize = 128 << 20;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const PROGRESS_TX_INTERVAL: usize = 1_000_000;

const DEFAULT_PIGGY_PROGRAMS: &[&str] = &[
    "Pig1CsXnfDwN1NuoeNRBojohbjc14dogmJCXeb2vL3Y",
    "Pig2ienhM3ukiTec3x8aCdnLASpU4z8yRPLgH9QxDvm",
];

const DEFAULT_PB_MINTS: &[&str] = &[
    "F35yYmTR6PqkbTx449P1eGhB57mRhWAdYs93eCo2dMZR",
    "E65CoK961Rs5LzKhGZxbKsB7xpFhYhXogH8nhr8zamTK",
    "FWLk7bGAqB8YW4HCbGbyXvL5HzUvfnHU2BwNi1kWc1Vt",
];

const DEFAULT_VAULT_ACCOUNTS: &[&str] = &[
    "91LYov1N1j62Q3ipuBi8w2e5GVqjVxWTdJ757roiWXdy",
    "5CgRTdywEQ7LK7SRM5NAgsuSWxnswREW6VeZ4i9jHCRf",
    "6mzgN6fyqap4Um7bNoH7DNCqtib3CU7TfGvVTMBhaDhf",
];

#[derive(Debug, Clone)]
pub struct PiggyScanConfig {
    pub car_path: PathBuf,
    pub out_path: PathBuf,
    pub mints: Vec<String>,
    pub programs: Vec<String>,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct PiggyScanSummary {
    pub scanned_txs: usize,
    pub skipped_txs: usize,
    pub decode_failures: usize,
    pub written_events: usize,
    pub candidate_transactions: usize,
    pub transaction_events: usize,
    pub token_instruction_events: usize,
    pub token_balance_events: usize,
    pub piggy_instruction_events: usize,
    pub discovered_users: usize,
    pub discovered_token_accounts: usize,
}

#[derive(Debug, Serialize)]
struct PiggyEvent {
    slot: u64,
    block_time: Option<i64>,
    signature: String,
    tx_error: bool,
    user: Option<String>,
    mint: Option<String>,
    token_account: Option<String>,
    event_type: String,
    amount: Option<String>,
    program_id: String,
    counterparty: Option<String>,
    outer_instruction_index: Option<u32>,
    inner_instruction_index: Option<u32>,
    stack_height: Option<u32>,
    tx_index: Option<u64>,
    raw_instruction_data_hex: Option<String>,
}

#[derive(Debug, Clone)]
struct ScanTargets {
    mint_strings: HashSet<String>,
    mint_pubkeys: HashSet<[u8; 32]>,
    program_pubkeys: HashSet<[u8; 32]>,
    vault_pubkeys: HashSet<[u8; 32]>,
}

pub fn scan_piggy(config: PiggyScanConfig) -> Result<PiggyScanSummary> {
    let targets = ScanTargets::from_config(&config)?;
    let out_file = File::create(&config.out_path)
        .with_context(|| format!("create {}", config.out_path.display()))?;
    let mut writer = BufWriter::new(out_file);
    let mut summary = PiggyScanSummary::default();

    scan_car(
        &config.car_path,
        is_zstd_path(&config.car_path),
        &config,
        &targets,
        &mut writer,
        &mut summary,
    )?;
    writer
        .flush()
        .with_context(|| format!("flush {}", config.out_path.display()))?;

    Ok(summary)
}

impl ScanTargets {
    fn from_config(config: &PiggyScanConfig) -> Result<Self> {
        let mint_values = if config.mints.is_empty() {
            DEFAULT_PB_MINTS
                .iter()
                .map(|value| value.to_string())
                .collect()
        } else {
            config.mints.clone()
        };
        let program_values = if config.programs.is_empty() {
            DEFAULT_PIGGY_PROGRAMS
                .iter()
                .map(|value| value.to_string())
                .collect()
        } else {
            config.programs.clone()
        };

        let mut mint_strings = HashSet::with_capacity(mint_values.len());
        let mut mint_pubkeys = HashSet::with_capacity(mint_values.len());
        for mint in mint_values {
            mint_pubkeys.insert(parse_pubkey(&mint)?);
            mint_strings.insert(mint);
        }

        let mut program_pubkeys = HashSet::with_capacity(program_values.len());
        for program in program_values {
            program_pubkeys.insert(parse_pubkey(&program)?);
        }

        let mut vault_pubkeys = HashSet::with_capacity(DEFAULT_VAULT_ACCOUNTS.len());
        for vault in DEFAULT_VAULT_ACCOUNTS {
            vault_pubkeys.insert(parse_pubkey(vault)?);
        }

        Ok(Self {
            mint_strings,
            mint_pubkeys,
            program_pubkeys,
            vault_pubkeys,
        })
    }
}

fn scan_car(
    path: &Path,
    compressed: bool,
    config: &PiggyScanConfig,
    targets: &ScanTargets,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    if compressed {
        let reader = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open zstd {}", path.display()))?;
        visit_reader(decoder, config, targets, writer, summary)
    } else {
        visit_reader(file, config, targets, writer, summary)
    }
}

fn visit_reader<R: Read>(
    reader: R,
    config: &PiggyScanConfig,
    targets: &ScanTargets,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;

    let mut scratch = Vec::with_capacity(1 << 20);
    let mut zstd = ZstdReusableDecoder::new();
    let mut slot_times = HashMap::<u64, i64>::new();
    let mut progress = ProgressLog::new();
    let mut discovered = DiscoveryState::new(targets);

    eprintln!(
        "piggy-scan: start car={} out={} start_slot={} end_slot={}",
        config.car_path.display(),
        config.out_path.display(),
        config
            .start_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "-".to_string()),
        config
            .end_slot
            .map(|slot| slot.to_string())
            .unwrap_or_else(|| "-".to_string())
    );

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .context("read CAR entry")?
    {
        let car_offset = reader.offset;
        let node_kind = peek_node_type(entry.payload).context("peek node type")?;
        if node_kind == 2 {
            if let Ok(Node::Block(block)) = decode_node(entry.payload)
                && let Some(block_time) = block.meta.blocktime
            {
                slot_times.insert(block.slot, block_time);
            }
            continue;
        }
        if node_kind != 0 {
            continue;
        }

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };

        if config.start_slot.is_some_and(|start| tx.slot < start)
            || config.end_slot.is_some_and(|end| tx.slot > end)
        {
            continue;
        }

        summary.scanned_txs += 1;
        progress.maybe_print(summary, tx.slot, car_offset);

        if tx.data.next.is_some() || tx.metadata.next.is_some() || tx.metadata.data.is_empty() {
            summary.skipped_txs += 1;
            continue;
        }

        let versioned_tx = match wincode::deserialize::<VersionedTransaction<'_>>(tx.data.data) {
            Ok(tx) => tx,
            Err(_) => {
                summary.skipped_txs += 1;
                summary.decode_failures += 1;
                continue;
            }
        };
        let Some(signature) = versioned_tx.signatures.first().copied() else {
            summary.skipped_txs += 1;
            summary.decode_failures += 1;
            continue;
        };

        let metadata_bytes = match zstd.decompress_if_zstd(tx.metadata.data) {
            Ok(true) => zstd.output(),
            Ok(false) => tx.metadata.data,
            Err(_) => {
                summary.skipped_txs += 1;
                summary.decode_failures += 1;
                continue;
            }
        };
        let meta = if slot_uses_protobuf_metadata(tx.slot) {
            match decode_piggy_meta_from_protobuf(metadata_bytes) {
                Ok(meta) => meta,
                Err(_) => {
                    summary.skipped_txs += 1;
                    summary.decode_failures += 1;
                    continue;
                }
            }
        } else {
            let mut owned_meta = TransactionStatusMeta::default();
            if decode_transaction_status_meta_into(tx.slot, metadata_bytes, &mut owned_meta)
                .is_err()
            {
                summary.skipped_txs += 1;
                summary.decode_failures += 1;
                continue;
            }
            piggy_meta_from_owned(&owned_meta)
        };

        let tx_ctx = TxContext {
            slot: tx.slot,
            block_time: slot_times.get(&tx.slot).copied(),
            signature: bs58::encode(signature).into_string(),
            tx_error: meta.tx_error,
            tx_index: tx.index,
        };
        emit_transaction_events(
            &versioned_tx,
            &meta,
            &tx_ctx,
            targets,
            &mut discovered,
            writer,
            summary,
        )?;
    }

    progress.print(summary, None, reader.offset, true);

    Ok(())
}

fn emit_transaction_events(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    tx_ctx: &TxContext,
    targets: &ScanTargets,
    discovered: &mut DiscoveryState,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let resolver = KeyResolver::new(tx, meta);
    let account_mints = collect_account_mints(meta);

    let instructions = match &tx.message {
        VersionedMessage::Legacy(message) => &message.instructions[..],
        VersionedMessage::V0(message) => &message.instructions[..],
    };

    let mut inner_groups: Vec<Vec<&PiggyInnerInstruction<'_>>> =
        vec![Vec::new(); instructions.len()];
    let mut trailing_inner_instructions = Vec::new();
    for instruction in &meta.inner_instructions {
        if let Some(slot) = inner_groups.get_mut(instruction.outer_instruction_index as usize) {
            slot.push(instruction);
        } else {
            trailing_inner_instructions.push(instruction);
        }
    }

    let candidate = is_candidate_transaction(
        tx,
        meta,
        instructions,
        &inner_groups,
        &trailing_inner_instructions,
        &resolver,
        &account_mints,
        targets,
        discovered,
    );
    if !candidate {
        return Ok(());
    }

    summary.candidate_transactions += 1;
    discover_from_token_balances(meta, &resolver, targets, discovered);
    summary.discovered_users = discovered.users.len();
    summary.discovered_token_accounts = discovered.token_accounts.len();

    emit_transaction_marker(tx_ctx, writer, summary)?;
    emit_token_balance_events(meta, tx_ctx, &resolver, writer, summary)?;

    for (outer_index, instruction) in instructions.iter().enumerate() {
        emit_instruction_event(
            InstructionView {
                outer_instruction_index: outer_index as u32,
                inner_instruction_index: None,
                stack_height: None,
                program_id_index: instruction.program_id_index as u32,
                accounts: &instruction.accounts,
                data: &instruction.data,
            },
            tx_ctx,
            targets,
            discovered,
            &resolver,
            &account_mints,
            writer,
            summary,
        )?;

        for inner_instruction in &inner_groups[outer_index] {
            emit_instruction_event(
                InstructionView {
                    outer_instruction_index: outer_index as u32,
                    inner_instruction_index: Some(inner_instruction.inner_instruction_index),
                    stack_height: inner_instruction.stack_height,
                    program_id_index: inner_instruction.program_id_index,
                    accounts: inner_instruction.accounts.as_ref(),
                    data: inner_instruction.data.as_ref(),
                },
                tx_ctx,
                targets,
                discovered,
                &resolver,
                &account_mints,
                writer,
                summary,
            )?;
        }
    }

    for inner_instruction in trailing_inner_instructions {
        emit_instruction_event(
            InstructionView {
                outer_instruction_index: inner_instruction.outer_instruction_index,
                inner_instruction_index: Some(inner_instruction.inner_instruction_index),
                stack_height: inner_instruction.stack_height,
                program_id_index: inner_instruction.program_id_index,
                accounts: inner_instruction.accounts.as_ref(),
                data: inner_instruction.data.as_ref(),
            },
            tx_ctx,
            targets,
            discovered,
            &resolver,
            &account_mints,
            writer,
            summary,
        )?;
    }

    Ok(())
}

fn emit_transaction_marker(
    tx_ctx: &TxContext,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let event = PiggyEvent {
        slot: tx_ctx.slot,
        block_time: tx_ctx.block_time,
        signature: tx_ctx.signature.clone(),
        tx_error: tx_ctx.tx_error,
        user: None,
        mint: None,
        token_account: None,
        event_type: "transaction".to_string(),
        amount: None,
        program_id: "transaction".to_string(),
        counterparty: None,
        outer_instruction_index: None,
        inner_instruction_index: None,
        stack_height: None,
        tx_index: tx_ctx.tx_index,
        raw_instruction_data_hex: None,
    };
    write_event(writer, &event)?;
    summary.written_events += 1;
    summary.transaction_events += 1;
    Ok(())
}

fn is_candidate_transaction(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    outer_instructions: &[of_car_reader::versioned_transaction::CompiledInstruction],
    inner_groups: &[Vec<&PiggyInnerInstruction<'_>>],
    trailing_inner_instructions: &[&PiggyInnerInstruction<'_>],
    resolver: &KeyResolver<'_>,
    account_mints: &HashMap<u32, [u8; 32]>,
    targets: &ScanTargets,
    discovered: &DiscoveryState,
) -> bool {
    if tx_account_keys(tx).any(|key| {
        targets.program_pubkeys.contains(&key)
            || targets.mint_pubkeys.contains(&key)
            || targets.vault_pubkeys.contains(&key)
            || discovered.users.contains(&key)
            || discovered.token_accounts.contains(&key)
    }) {
        return true;
    }

    if meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
        .any(|balance| token_balance_matches(balance, resolver, targets, discovered))
    {
        return true;
    }

    for (outer_index, instruction) in outer_instructions.iter().enumerate() {
        if instruction_matches(
            InstructionView {
                outer_instruction_index: outer_index as u32,
                inner_instruction_index: None,
                stack_height: None,
                program_id_index: instruction.program_id_index as u32,
                accounts: &instruction.accounts,
                data: &instruction.data,
            },
            resolver,
            account_mints,
            targets,
            discovered,
        ) {
            return true;
        }

        for (inner_index, inner_instruction) in inner_groups[outer_index].iter().enumerate() {
            if instruction_matches(
                InstructionView {
                    outer_instruction_index: outer_index as u32,
                    inner_instruction_index: Some(inner_index as u32),
                    stack_height: inner_instruction.stack_height,
                    program_id_index: inner_instruction.program_id_index,
                    accounts: inner_instruction.accounts.as_ref(),
                    data: inner_instruction.data.as_ref(),
                },
                resolver,
                account_mints,
                targets,
                discovered,
            ) {
                return true;
            }
        }
    }

    for inner_instruction in trailing_inner_instructions {
        if instruction_matches(
            InstructionView {
                outer_instruction_index: inner_instruction.outer_instruction_index,
                inner_instruction_index: Some(inner_instruction.inner_instruction_index),
                stack_height: inner_instruction.stack_height,
                program_id_index: inner_instruction.program_id_index,
                accounts: inner_instruction.accounts.as_ref(),
                data: inner_instruction.data.as_ref(),
            },
            resolver,
            account_mints,
            targets,
            discovered,
        ) {
            return true;
        }
    }

    false
}

fn instruction_matches(
    instruction: InstructionView<'_>,
    resolver: &KeyResolver<'_>,
    account_mints: &HashMap<u32, [u8; 32]>,
    targets: &ScanTargets,
    discovered: &DiscoveryState,
) -> bool {
    let Some(program_id) = resolver.resolve(instruction.program_id_index) else {
        return false;
    };
    if targets.program_pubkeys.contains(&program_id) {
        return true;
    }
    if ProgramKind::from_program_pubkey(program_id).is_none() {
        return false;
    }

    let Some(decoded) = decode_token_event(instruction.accounts, instruction.data, account_mints)
    else {
        return false;
    };
    let mint = decoded.mint.or_else(|| {
        decoded
            .mint_account_index
            .and_then(|index| resolver.resolve(index))
    });
    if mint.is_some_and(|mint| targets.mint_pubkeys.contains(&mint)) {
        return true;
    }

    [
        decoded.token_account_index,
        decoded.user_account_index,
        decoded.counterparty_account_index,
    ]
    .into_iter()
    .flatten()
    .filter_map(|index| resolver.resolve(index))
    .any(|key| discovered.users.contains(&key) || discovered.token_accounts.contains(&key))
}

fn token_balance_matches(
    balance: &PiggyTokenBalance<'_>,
    resolver: &KeyResolver<'_>,
    targets: &ScanTargets,
    discovered: &DiscoveryState,
) -> bool {
    if targets.mint_strings.contains(balance.mint.as_ref()) {
        return true;
    }
    if !balance.owner.is_empty()
        && parse_pubkey(balance.owner.as_ref())
            .map(|owner| {
                discovered.users.contains(&owner) || targets.vault_pubkeys.contains(&owner)
            })
            .unwrap_or(false)
    {
        return true;
    }
    resolver
        .resolve(balance.account_index)
        .map(|account| discovered.token_accounts.contains(&account))
        .unwrap_or(false)
}

fn discover_from_token_balances(
    meta: &PiggyMeta<'_>,
    resolver: &KeyResolver<'_>,
    targets: &ScanTargets,
    discovered: &mut DiscoveryState,
) {
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if !targets.mint_strings.contains(balance.mint.as_ref()) {
            continue;
        }
        if let Some(account) = resolver.resolve(balance.account_index) {
            discovered.token_accounts.insert(account);
        }
        if !balance.owner.is_empty()
            && let Ok(owner) = parse_pubkey(balance.owner.as_ref())
        {
            discovered.users.insert(owner);
        }
    }
}

fn emit_token_balance_events(
    meta: &PiggyMeta<'_>,
    tx_ctx: &TxContext,
    resolver: &KeyResolver<'_>,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let mut balances = HashMap::<
        (u32, String),
        (
            Option<&PiggyTokenBalance<'_>>,
            Option<&PiggyTokenBalance<'_>>,
        ),
    >::new();
    for pre in &meta.pre_token_balances {
        balances
            .entry((pre.account_index, pre.mint.to_string()))
            .or_default()
            .0 = Some(pre);
    }
    for post in &meta.post_token_balances {
        balances
            .entry((post.account_index, post.mint.to_string()))
            .or_default()
            .1 = Some(post);
    }

    for ((account_index, mint), (pre, post)) in balances {
        let pre_amount = pre.and_then(raw_token_amount).unwrap_or(0);
        let post_amount = post.and_then(raw_token_amount).unwrap_or(0);
        let delta = post_amount - pre_amount;
        if delta == 0 {
            continue;
        }

        let owner = post
            .or(pre)
            .and_then(|balance| (!balance.owner.is_empty()).then(|| balance.owner.to_string()));
        let program_id = post
            .or(pre)
            .map(|balance| balance.program_id.to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "token_balance".to_string());
        let event = PiggyEvent {
            slot: tx_ctx.slot,
            block_time: tx_ctx.block_time,
            signature: tx_ctx.signature.clone(),
            tx_error: tx_ctx.tx_error,
            user: owner,
            mint: Some(mint),
            token_account: resolver.resolve(account_index).map(encode_pubkey),
            event_type: "balance_change".to_string(),
            amount: Some(delta.to_string()),
            program_id,
            counterparty: None,
            outer_instruction_index: None,
            inner_instruction_index: None,
            stack_height: None,
            tx_index: tx_ctx.tx_index,
            raw_instruction_data_hex: None,
        };
        write_event(writer, &event)?;
        summary.written_events += 1;
        summary.token_balance_events += 1;
    }

    Ok(())
}

fn emit_instruction_event(
    instruction: InstructionView<'_>,
    tx_ctx: &TxContext,
    targets: &ScanTargets,
    discovered: &mut DiscoveryState,
    resolver: &KeyResolver<'_>,
    account_mints: &HashMap<u32, [u8; 32]>,
    writer: &mut BufWriter<File>,
    summary: &mut PiggyScanSummary,
) -> Result<()> {
    let Some(program_id) = resolver.resolve(instruction.program_id_index) else {
        return Ok(());
    };

    if targets.program_pubkeys.contains(&program_id) {
        let event = PiggyEvent {
            slot: tx_ctx.slot,
            block_time: tx_ctx.block_time,
            signature: tx_ctx.signature.clone(),
            tx_error: tx_ctx.tx_error,
            user: resolver.resolve(0).map(encode_pubkey),
            mint: None,
            token_account: None,
            event_type: "piggy_instruction".to_string(),
            amount: None,
            program_id: encode_pubkey(program_id),
            counterparty: None,
            outer_instruction_index: Some(instruction.outer_instruction_index),
            inner_instruction_index: instruction.inner_instruction_index,
            stack_height: instruction.stack_height,
            tx_index: tx_ctx.tx_index,
            raw_instruction_data_hex: Some(hex_encode(instruction.data)),
        };
        write_event(writer, &event)?;
        summary.written_events += 1;
        summary.piggy_instruction_events += 1;
        return Ok(());
    }

    if ProgramKind::from_program_pubkey(program_id).is_none() {
        return Ok(());
    }

    let Some(decoded) = decode_token_event(instruction.accounts, instruction.data, account_mints)
    else {
        return Ok(());
    };
    let Some(mint) = decoded.mint.or_else(|| {
        decoded
            .mint_account_index
            .and_then(|index| resolver.resolve(index))
    }) else {
        return Ok(());
    };

    if targets.mint_pubkeys.contains(&mint) {
        if let Some(user) = decoded
            .user_account_index
            .and_then(|index| resolver.resolve(index))
        {
            discovered.users.insert(user);
        }
        if let Some(token_account) = decoded
            .token_account_index
            .and_then(|index| resolver.resolve(index))
        {
            discovered.token_accounts.insert(token_account);
        }
    }

    let user = decoded
        .user_account_index
        .and_then(|index| resolver.resolve(index));
    let token_account = decoded
        .token_account_index
        .and_then(|index| resolver.resolve(index));
    let counterparty = decoded
        .counterparty_account_index
        .and_then(|index| resolver.resolve(index));
    let event = PiggyEvent {
        slot: tx_ctx.slot,
        block_time: tx_ctx.block_time,
        signature: tx_ctx.signature.clone(),
        tx_error: tx_ctx.tx_error,
        user: user.map(encode_pubkey),
        mint: Some(encode_pubkey(mint)),
        token_account: token_account.map(encode_pubkey),
        event_type: decoded.event_type.to_string(),
        amount: decoded.amount.map(|amount| amount.to_string()),
        program_id: encode_pubkey(program_id),
        counterparty: counterparty.map(encode_pubkey),
        outer_instruction_index: Some(instruction.outer_instruction_index),
        inner_instruction_index: instruction.inner_instruction_index,
        stack_height: instruction.stack_height,
        tx_index: tx_ctx.tx_index,
        raw_instruction_data_hex: None,
    };
    write_event(writer, &event)?;
    summary.written_events += 1;
    summary.token_instruction_events += 1;

    Ok(())
}

fn decode_token_event(
    accounts: &[u8],
    data: &[u8],
    account_mints: &HashMap<u32, [u8; 32]>,
) -> Option<DecodedTokenEvent> {
    let tag = *data.first()?;
    match tag {
        0 => Some(DecodedTokenEvent {
            event_type: "initialize_mint",
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 0),
            user_account_index: None,
            counterparty_account_index: None,
            amount: None,
        }),
        3 => {
            let amount = read_u64(data, 1)?;
            let source_index = *accounts.first()? as u32;
            let dest_index = *accounts.get(1)? as u32;
            let mint = mint_from_account_mints(account_mints, source_index)
                .or_else(|| mint_from_account_mints(account_mints, dest_index));
            Some(DecodedTokenEvent {
                event_type: "transfer",
                mint,
                mint_account_index: None,
                token_account_index: account_index(accounts, 0),
                user_account_index: account_index(accounts, 2),
                counterparty_account_index: account_index(accounts, 1),
                amount: Some(amount),
            })
        }
        7 => Some(DecodedTokenEvent {
            event_type: "mint_to",
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 1),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
            amount: Some(read_u64(data, 1)?),
        }),
        8 => Some(DecodedTokenEvent {
            event_type: "burn",
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
            amount: Some(read_u64(data, 1)?),
        }),
        9 => {
            let closing_account_index = *accounts.first()? as u32;
            Some(DecodedTokenEvent {
                event_type: "close_account",
                mint: mint_from_account_mints(account_mints, closing_account_index),
                mint_account_index: None,
                token_account_index: account_index(accounts, 0),
                user_account_index: account_index(accounts, 2),
                counterparty_account_index: account_index(accounts, 1),
                amount: None,
            })
        }
        12 => Some(DecodedTokenEvent {
            event_type: "transfer_checked",
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 3),
            counterparty_account_index: account_index(accounts, 2),
            amount: Some(read_u64(data, 1)?),
        }),
        14 => Some(DecodedTokenEvent {
            event_type: "mint_to_checked",
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 1),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
            amount: Some(read_u64(data, 1)?),
        }),
        15 => Some(DecodedTokenEvent {
            event_type: "burn_checked",
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
            amount: Some(read_u64(data, 1)?),
        }),
        20 => Some(DecodedTokenEvent {
            event_type: "initialize_mint2",
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 0),
            user_account_index: None,
            counterparty_account_index: None,
            amount: None,
        }),
        _ => None,
    }
}

fn collect_account_mints(meta: &PiggyMeta<'_>) -> HashMap<u32, [u8; 32]> {
    let mut out = HashMap::new();
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Ok(mint) = parse_pubkey(balance.mint.as_ref()) {
            out.insert(balance.account_index, mint);
        }
    }
    out
}

#[derive(Debug)]
struct DecodedTokenEvent {
    event_type: &'static str,
    mint: Option<[u8; 32]>,
    mint_account_index: Option<u32>,
    token_account_index: Option<u32>,
    user_account_index: Option<u32>,
    counterparty_account_index: Option<u32>,
    amount: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct InstructionView<'a> {
    outer_instruction_index: u32,
    inner_instruction_index: Option<u32>,
    stack_height: Option<u32>,
    program_id_index: u32,
    accounts: &'a [u8],
    data: &'a [u8],
}

#[derive(Debug)]
struct TxContext {
    slot: u64,
    block_time: Option<i64>,
    signature: String,
    tx_error: bool,
    tx_index: Option<u64>,
}

#[derive(Debug)]
struct DiscoveryState {
    users: HashSet<[u8; 32]>,
    token_accounts: HashSet<[u8; 32]>,
}

impl DiscoveryState {
    fn new(targets: &ScanTargets) -> Self {
        Self {
            users: targets.vault_pubkeys.clone(),
            token_accounts: HashSet::new(),
        }
    }
}

#[derive(Debug)]
struct ProgressLog {
    started_at: Instant,
    last_print_at: Instant,
    last_printed_txs: usize,
}

impl ProgressLog {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_print_at: now,
            last_printed_txs: 0,
        }
    }

    fn maybe_print(&mut self, summary: &PiggyScanSummary, slot: u64, file_offset: u64) {
        let now = Instant::now();
        let tx_delta = summary.scanned_txs.saturating_sub(self.last_printed_txs);
        if tx_delta >= PROGRESS_TX_INTERVAL
            || now.duration_since(self.last_print_at) >= PROGRESS_INTERVAL
        {
            self.print(summary, Some(slot), file_offset, false);
        }
    }

    fn print(
        &mut self,
        summary: &PiggyScanSummary,
        slot: Option<u64>,
        file_offset: u64,
        final_line: bool,
    ) {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let tx_per_sec = if elapsed > 0.0 {
            summary.scanned_txs as f64 / elapsed
        } else {
            0.0
        };
        let mib = file_offset as f64 / (1024.0 * 1024.0);
        eprintln!(
            "piggy-scan: {}txs={} candidates={} skipped={} decode_failures={} events={} tx_events={} token_ix={} balances={} piggy_ix={} users={} token_accounts={} slot={} read_mib={:.1} elapsed_s={:.0} tx_s={:.0}",
            if final_line { "done " } else { "" },
            summary.scanned_txs,
            summary.candidate_transactions,
            summary.skipped_txs,
            summary.decode_failures,
            summary.written_events,
            summary.transaction_events,
            summary.token_instruction_events,
            summary.token_balance_events,
            summary.piggy_instruction_events,
            summary.discovered_users,
            summary.discovered_token_accounts,
            slot.map(|slot| slot.to_string())
                .unwrap_or_else(|| "-".to_string()),
            mib,
            elapsed,
            tx_per_sec
        );
        self.last_print_at = Instant::now();
        self.last_printed_txs = summary.scanned_txs;
    }
}

struct KeyResolver<'a> {
    static_keys: &'a [&'a [u8; 32]],
    loaded_writable: &'a [std::borrow::Cow<'a, [u8]>],
    loaded_readonly: &'a [std::borrow::Cow<'a, [u8]>],
}

fn tx_account_keys<'a>(tx: &'a VersionedTransaction<'a>) -> impl Iterator<Item = [u8; 32]> + 'a {
    let keys = match &tx.message {
        VersionedMessage::Legacy(message) => &message.account_keys,
        VersionedMessage::V0(message) => &message.account_keys,
    };
    keys.iter().map(|key| **key)
}

impl<'a> KeyResolver<'a> {
    fn new(tx: &'a VersionedTransaction<'a>, meta: &'a PiggyMeta<'a>) -> Self {
        match &tx.message {
            VersionedMessage::Legacy(message) => Self {
                static_keys: &message.account_keys,
                loaded_writable: &[],
                loaded_readonly: &[],
            },
            VersionedMessage::V0(message) => Self {
                static_keys: &message.account_keys,
                loaded_writable: &meta.loaded_writable_addresses,
                loaded_readonly: &meta.loaded_readonly_addresses,
            },
        }
    }

    fn resolve(&self, index: u32) -> Option<[u8; 32]> {
        let index = index as usize;
        if let Some(key) = self.static_keys.get(index) {
            return Some(**key);
        }

        let loaded_index = index.checked_sub(self.static_keys.len())?;
        if let Some(key) = self.loaded_writable.get(loaded_index) {
            return slice_to_pubkey(key.as_ref());
        }

        let readonly_index = loaded_index.checked_sub(self.loaded_writable.len())?;
        slice_to_pubkey(self.loaded_readonly.get(readonly_index)?.as_ref())
    }
}

fn account_index(accounts: &[u8], account_position: usize) -> Option<u32> {
    accounts.get(account_position).map(|index| *index as u32)
}

fn mint_from_account_mints(
    account_mints: &HashMap<u32, [u8; 32]>,
    account_index: u32,
) -> Option<[u8; 32]> {
    account_mints.get(&account_index).copied()
}

fn raw_token_amount(balance: &PiggyTokenBalance<'_>) -> Option<i128> {
    balance.amount.as_ref()?.parse().ok()
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    let bytes = data.get(offset..offset + 8)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

fn write_event(writer: &mut BufWriter<File>, event: &PiggyEvent) -> Result<()> {
    serde_json::to_writer(&mut *writer, event).context("write piggy event")?;
    writer.write_all(b"\n").context("write piggy event newline")
}

fn parse_pubkey(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode pubkey {value}"))?;
    if bytes.len() != 32 {
        bail!(
            "pubkey {value} decoded to {} bytes, expected 32",
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn encode_pubkey(pubkey: [u8; 32]) -> String {
    bs58::encode(pubkey).into_string()
}

fn slice_to_pubkey(bytes: &[u8]) -> Option<[u8; 32]> {
    (bytes.len() == 32).then(|| {
        let mut out = [0u8; 32];
        out.copy_from_slice(bytes);
        out
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::decode_token_event;
    use std::collections::HashMap;

    #[test]
    fn decodes_burn_checked() {
        let amount = 123u64;
        let mut data = vec![15];
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(6);

        let decoded = decode_token_event(&[4, 5, 6], &data, &HashMap::new()).unwrap();
        assert_eq!(decoded.event_type, "burn_checked");
        assert_eq!(decoded.mint_account_index, Some(5));
        assert_eq!(decoded.token_account_index, Some(4));
        assert_eq!(decoded.user_account_index, Some(6));
        assert_eq!(decoded.amount, Some(amount));
    }
}

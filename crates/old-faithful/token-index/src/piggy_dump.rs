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
    node::{Node, decode_node, is_transaction_node},
    versioned_transaction::{VersionedMessage, VersionedTransaction},
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const CAR_BUF: usize = 128 << 20;
const DUMP_MAGIC: &[u8; 8] = b"PIGDMP2\n";
const RECORD_HEADER_LEN: u64 = 8 + 8 + 4 + 4 + 64 + 8 + 8;
const DUMP_FLAG_TX_ERROR: u32 = 1 << 0;
const DUMP_FLAG_HAS_COMPUTE_UNITS: u32 = 1 << 1;
const DUMP_FLAG_HAS_COST_UNITS: u32 = 1 << 2;
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
pub struct PiggyDumpConfig {
    pub car_path: PathBuf,
    pub out_dir: PathBuf,
    pub mints: Vec<String>,
    pub programs: Vec<String>,
    pub seed_user_files: Vec<PathBuf>,
    pub seed_token_account_files: Vec<PathBuf>,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct PiggyDumpSummary {
    pub scanned_txs: usize,
    pub skipped_txs: usize,
    pub decode_failures: usize,
    pub dumped_transactions: usize,
    pub dump_bytes: u64,
    pub signature_index_rows: usize,
    pub user_index_rows: usize,
    pub account_index_rows: usize,
    pub mint_index_rows: usize,
    pub program_index_rows: usize,
    pub discovered_users: usize,
    pub discovered_token_accounts: usize,
    pub dump_format_version: u32,
    pub dump_record_header_len: u64,
}

#[derive(Debug, Clone)]
struct DumpTargets {
    mint_strings: HashSet<String>,
    mint_pubkeys: HashSet<[u8; 32]>,
    program_pubkeys: HashSet<[u8; 32]>,
    vault_pubkeys: HashSet<[u8; 32]>,
}

pub fn dump_piggy(config: PiggyDumpConfig) -> Result<PiggyDumpSummary> {
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create {}", config.out_dir.display()))?;

    let targets = DumpTargets::from_config(&config)?;
    let mut writers = DumpWriters::create(&config.out_dir)?;
    let seed_users = read_seed_pubkey_files(&config.seed_user_files)?;
    let seed_token_accounts = read_seed_pubkey_files(&config.seed_token_account_files)?;
    let mut summary = PiggyDumpSummary {
        dump_format_version: 2,
        dump_record_header_len: RECORD_HEADER_LEN,
        ..PiggyDumpSummary::default()
    };

    scan_car(
        &config.car_path,
        is_zstd_path(&config.car_path),
        &config,
        &targets,
        &seed_users,
        &seed_token_accounts,
        &mut writers,
        &mut summary,
    )?;
    summary.dump_bytes = writers.dump_offset;
    writers.flush()?;

    fs::write(
        config.out_dir.join("meta.json"),
        serde_json::to_vec_pretty(&summary).context("serialize piggy dump meta")?,
    )
    .with_context(|| format!("write {}", config.out_dir.join("meta.json").display()))?;
    write_pubkey_set(
        &config.out_dir.join("discovered-users.txt"),
        &writers.users_seen,
    )?;
    write_pubkey_set(
        &config.out_dir.join("discovered-token-accounts.txt"),
        &writers.token_accounts_seen,
    )?;

    Ok(summary)
}

impl DumpTargets {
    fn from_config(config: &PiggyDumpConfig) -> Result<Self> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_record_preserves_compute_and_cost_units() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut writers = DumpWriters::create(tmp.path()).expect("create writers");
        let ctx = DumpContext {
            slot: 42,
            signature: "sig".to_string(),
            signature_bytes: [7; 64],
            tx_error: true,
            compute_units_consumed: Some(0),
            cost_units: Some(91),
            tx_index: None,
        };

        let location = writers
            .write_record(&ctx, &[1, 2, 3])
            .expect("write record");
        writers.flush().expect("flush");

        assert_eq!(location.offset, DUMP_MAGIC.len() as u64);
        assert_eq!(location.size, RECORD_HEADER_LEN + 3);

        let bytes = fs::read(tmp.path().join("transactions.bin")).expect("read dump");
        assert_eq!(&bytes[..DUMP_MAGIC.len()], DUMP_MAGIC);

        let base = DUMP_MAGIC.len();
        assert_eq!(
            u64::from_le_bytes(bytes[base..base + 8].try_into().unwrap()),
            42
        );
        assert_eq!(
            u64::from_le_bytes(bytes[base + 8..base + 16].try_into().unwrap()),
            u64::MAX
        );
        assert_eq!(
            u32::from_le_bytes(bytes[base + 16..base + 20].try_into().unwrap()),
            3
        );
        assert_eq!(
            u32::from_le_bytes(bytes[base + 20..base + 24].try_into().unwrap()),
            DUMP_FLAG_TX_ERROR | DUMP_FLAG_HAS_COMPUTE_UNITS | DUMP_FLAG_HAS_COST_UNITS
        );
        assert_eq!(&bytes[base + 24..base + 88], &[7; 64]);
        assert_eq!(
            u64::from_le_bytes(bytes[base + 88..base + 96].try_into().unwrap()),
            0
        );
        assert_eq!(
            u64::from_le_bytes(bytes[base + 96..base + 104].try_into().unwrap()),
            91
        );
        assert_eq!(&bytes[base + 104..], &[1, 2, 3]);
    }

    #[test]
    fn signature_index_includes_compute_and_cost_unit_columns() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut writers = DumpWriters::create(tmp.path()).expect("create writers");
        let mut summary = PiggyDumpSummary::default();
        let ctx = DumpContext {
            slot: 42,
            signature: "sig".to_string(),
            signature_bytes: [7; 64],
            tx_error: false,
            compute_units_consumed: Some(123),
            cost_units: None,
            tx_index: Some(9),
        };
        let keys = IndexKeys::default();

        write_indexes(
            &mut writers,
            &mut summary,
            &ctx,
            DumpLocation {
                offset: 8,
                size: RECORD_HEADER_LEN,
            },
            &keys,
        )
        .expect("write indexes");
        writers.flush().expect("flush");

        let index =
            fs::read_to_string(tmp.path().join("signature.index.tsv")).expect("read signature");
        let mut lines = index.lines();
        assert_eq!(
            lines.next(),
            Some(
                "signature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units",
            )
        );
        assert_eq!(lines.next(), Some("sig\t42\t9\t8\t104\tfalse\t123\t"));
    }
}

struct DumpWriters {
    dump: BufWriter<File>,
    dump_offset: u64,
    signature: BufWriter<File>,
    users: BufWriter<File>,
    accounts: BufWriter<File>,
    mints: BufWriter<File>,
    programs: BufWriter<File>,
    users_seen: HashSet<[u8; 32]>,
    token_accounts_seen: HashSet<[u8; 32]>,
}

impl DumpWriters {
    fn create(out_dir: &Path) -> Result<Self> {
        let mut dump = BufWriter::new(
            File::create(out_dir.join("transactions.bin")).with_context(|| {
                format!("create {}", out_dir.join("transactions.bin").display())
            })?,
        );
        dump.write_all(DUMP_MAGIC).context("write dump magic")?;

        let mut signature = tsv(out_dir, "signature.index.tsv")?;
        signature.write_all(
            b"signature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units\n",
        )?;
        let mut users = tsv(out_dir, "user.index.tsv")?;
        users.write_all(
            b"user\tsignature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units\n",
        )?;
        let mut accounts = tsv(out_dir, "account.index.tsv")?;
        accounts.write_all(
            b"account\tsignature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units\n",
        )?;
        let mut mints = tsv(out_dir, "mint.index.tsv")?;
        mints.write_all(
            b"mint\tsignature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units\n",
        )?;
        let mut programs = tsv(out_dir, "program.index.tsv")?;
        programs.write_all(
            b"program\tsignature\tslot\ttx_index\toffset\tsize\ttx_error\tcompute_units_consumed\tcost_units\n",
        )?;

        Ok(Self {
            dump,
            dump_offset: DUMP_MAGIC.len() as u64,
            signature,
            users,
            accounts,
            mints,
            programs,
            users_seen: HashSet::new(),
            token_accounts_seen: HashSet::new(),
        })
    }

    fn write_record(&mut self, ctx: &DumpContext, payload: &[u8]) -> Result<DumpLocation> {
        let payload_len: u32 = payload
            .len()
            .try_into()
            .context("transaction payload too large for dump record")?;
        let offset = self.dump_offset;
        let size = RECORD_HEADER_LEN + payload.len() as u64;

        self.dump.write_all(&ctx.slot.to_le_bytes())?;
        self.dump
            .write_all(&ctx.tx_index.unwrap_or(u64::MAX).to_le_bytes())?;
        self.dump.write_all(&payload_len.to_le_bytes())?;
        self.dump.write_all(&ctx.dump_flags().to_le_bytes())?;
        self.dump.write_all(&ctx.signature_bytes)?;
        self.dump
            .write_all(&ctx.compute_units_consumed.unwrap_or(0).to_le_bytes())?;
        self.dump
            .write_all(&ctx.cost_units.unwrap_or(0).to_le_bytes())?;
        self.dump.write_all(payload)?;
        self.dump_offset += size;

        Ok(DumpLocation { offset, size })
    }

    fn flush(&mut self) -> Result<()> {
        self.dump.flush().context("flush transactions.bin")?;
        self.signature.flush().context("flush signature index")?;
        self.users.flush().context("flush user index")?;
        self.accounts.flush().context("flush account index")?;
        self.mints.flush().context("flush mint index")?;
        self.programs.flush().context("flush program index")?;
        Ok(())
    }
}

fn tsv(out_dir: &Path, name: &str) -> Result<BufWriter<File>> {
    Ok(BufWriter::new(
        File::create(out_dir.join(name))
            .with_context(|| format!("create {}", out_dir.join(name).display()))?,
    ))
}

fn scan_car(
    path: &Path,
    compressed: bool,
    config: &PiggyDumpConfig,
    targets: &DumpTargets,
    seed_users: &HashSet<[u8; 32]>,
    seed_token_accounts: &HashSet<[u8; 32]>,
    writers: &mut DumpWriters,
    summary: &mut PiggyDumpSummary,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    if compressed {
        let reader = BufReader::with_capacity(CAR_BUF, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open zstd {}", path.display()))?;
        visit_reader(
            decoder,
            config,
            targets,
            seed_users,
            seed_token_accounts,
            writers,
            summary,
        )
    } else {
        visit_reader(
            file,
            config,
            targets,
            seed_users,
            seed_token_accounts,
            writers,
            summary,
        )
    }
}

fn visit_reader<R: Read>(
    reader: R,
    config: &PiggyDumpConfig,
    targets: &DumpTargets,
    seed_users: &HashSet<[u8; 32]>,
    seed_token_accounts: &HashSet<[u8; 32]>,
    writers: &mut DumpWriters,
    summary: &mut PiggyDumpSummary,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, CAR_BUF);
    reader.skip_header().context("skip CAR header")?;

    let mut scratch = Vec::with_capacity(1 << 20);
    let mut zstd = ZstdReusableDecoder::new();
    let mut progress = ProgressLog::new();
    let mut discovered = DiscoveryState::new(targets, seed_users, seed_token_accounts);
    summary.discovered_users = discovered.users.len();
    summary.discovered_token_accounts = discovered.token_accounts.len();

    eprintln!(
        "piggy-dump: start car={} out_dir={} start_slot={} end_slot={}",
        config.car_path.display(),
        config.out_dir.display(),
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
        .read_entry_payload_if_prefix_with_scratch(&mut scratch, 2, is_transaction_node)
        .context("read CAR entry")?
    {
        let car_offset = reader.offset;
        let Some(payload) = entry.payload else {
            continue;
        };

        let Node::Transaction(tx) = decode_node(payload).context("decode transaction node")? else {
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
        let Some(signature_bytes) = versioned_tx.signatures.first().copied() else {
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

        let ctx = DumpContext {
            slot: tx.slot,
            signature: bs58::encode(signature_bytes).into_string(),
            signature_bytes: *signature_bytes,
            tx_error: meta.tx_error,
            compute_units_consumed: meta.compute_units_consumed,
            cost_units: meta.cost_units,
            tx_index: tx.index,
        };

        maybe_dump_transaction(
            &versioned_tx,
            &meta,
            &ctx,
            payload,
            targets,
            &mut discovered,
            writers,
            summary,
        )?;
    }

    progress.print(summary, None, reader.offset, true);
    writers.users_seen = discovered.users;
    writers.token_accounts_seen = discovered.token_accounts;
    Ok(())
}

fn maybe_dump_transaction(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    ctx: &DumpContext,
    payload: &[u8],
    targets: &DumpTargets,
    discovered: &mut DiscoveryState,
    writers: &mut DumpWriters,
    summary: &mut PiggyDumpSummary,
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

    if !is_candidate_transaction(
        tx,
        meta,
        instructions,
        &inner_groups,
        &trailing_inner_instructions,
        &resolver,
        &account_mints,
        targets,
        discovered,
    ) {
        return Ok(());
    }

    discover_from_candidate(
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
    summary.discovered_users = discovered.users.len();
    summary.discovered_token_accounts = discovered.token_accounts.len();

    let keys = collect_index_keys(
        tx,
        meta,
        instructions,
        &inner_groups,
        &trailing_inner_instructions,
        &resolver,
        &account_mints,
        discovered,
    );
    let location = writers.write_record(ctx, payload)?;
    write_indexes(writers, summary, ctx, location, &keys)?;

    summary.dumped_transactions += 1;
    summary.dump_bytes = writers.dump_offset;
    Ok(())
}

fn write_indexes(
    writers: &mut DumpWriters,
    summary: &mut PiggyDumpSummary,
    ctx: &DumpContext,
    location: DumpLocation,
    keys: &IndexKeys,
) -> Result<()> {
    writeln!(
        writers.signature,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        ctx.signature,
        ctx.slot,
        tx_index_text(ctx.tx_index),
        location.offset,
        location.size,
        ctx.tx_error,
        optional_u64_text(ctx.compute_units_consumed),
        optional_u64_text(ctx.cost_units)
    )?;
    summary.signature_index_rows += 1;

    for key in sorted_pubkeys(&keys.users) {
        writeln!(
            writers.users,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            key,
            ctx.signature,
            ctx.slot,
            tx_index_text(ctx.tx_index),
            location.offset,
            location.size,
            ctx.tx_error,
            optional_u64_text(ctx.compute_units_consumed),
            optional_u64_text(ctx.cost_units)
        )?;
        summary.user_index_rows += 1;
    }
    for key in sorted_pubkeys(&keys.accounts) {
        writeln!(
            writers.accounts,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            key,
            ctx.signature,
            ctx.slot,
            tx_index_text(ctx.tx_index),
            location.offset,
            location.size,
            ctx.tx_error,
            optional_u64_text(ctx.compute_units_consumed),
            optional_u64_text(ctx.cost_units)
        )?;
        summary.account_index_rows += 1;
    }
    for key in sorted_pubkeys(&keys.mints) {
        writeln!(
            writers.mints,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            key,
            ctx.signature,
            ctx.slot,
            tx_index_text(ctx.tx_index),
            location.offset,
            location.size,
            ctx.tx_error,
            optional_u64_text(ctx.compute_units_consumed),
            optional_u64_text(ctx.cost_units)
        )?;
        summary.mint_index_rows += 1;
    }
    for key in sorted_pubkeys(&keys.programs) {
        writeln!(
            writers.programs,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            key,
            ctx.signature,
            ctx.slot,
            tx_index_text(ctx.tx_index),
            location.offset,
            location.size,
            ctx.tx_error,
            optional_u64_text(ctx.compute_units_consumed),
            optional_u64_text(ctx.cost_units)
        )?;
        summary.program_index_rows += 1;
    }

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
    targets: &DumpTargets,
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

        for inner_instruction in &inner_groups[outer_index] {
            if instruction_matches(
                InstructionView {
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
    targets: &DumpTargets,
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
    targets: &DumpTargets,
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

fn discover_from_candidate(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    outer_instructions: &[of_car_reader::versioned_transaction::CompiledInstruction],
    inner_groups: &[Vec<&PiggyInnerInstruction<'_>>],
    trailing_inner_instructions: &[&PiggyInnerInstruction<'_>],
    resolver: &KeyResolver<'_>,
    account_mints: &HashMap<u32, [u8; 32]>,
    targets: &DumpTargets,
    discovered: &mut DiscoveryState,
) {
    if tx_touches_program(
        tx,
        meta,
        outer_instructions,
        inner_groups,
        trailing_inner_instructions,
        targets,
    ) && let Some(payer) = resolver.resolve(0)
    {
        discovered.users.insert(payer);
    }

    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if targets.mint_strings.contains(balance.mint.as_ref()) {
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

    for instruction in instruction_views(
        outer_instructions,
        inner_groups,
        trailing_inner_instructions,
    ) {
        let Some(program_id) = resolver.resolve(instruction.program_id_index) else {
            continue;
        };
        if ProgramKind::from_program_pubkey(program_id).is_none() {
            continue;
        }
        let Some(decoded) =
            decode_token_event(instruction.accounts, instruction.data, account_mints)
        else {
            continue;
        };
        let mint = decoded.mint.or_else(|| {
            decoded
                .mint_account_index
                .and_then(|index| resolver.resolve(index))
        });
        let token_account = decoded
            .token_account_index
            .and_then(|index| resolver.resolve(index));
        let user = decoded
            .user_account_index
            .and_then(|index| resolver.resolve(index));

        if mint.is_some_and(|mint| targets.mint_pubkeys.contains(&mint))
            || token_account.is_some_and(|account| discovered.token_accounts.contains(&account))
            || user.is_some_and(|user| discovered.users.contains(&user))
        {
            if let Some(account) = token_account {
                discovered.token_accounts.insert(account);
            }
            if let Some(user) = user {
                discovered.users.insert(user);
            }
        }
    }
}

fn collect_index_keys(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    outer_instructions: &[of_car_reader::versioned_transaction::CompiledInstruction],
    inner_groups: &[Vec<&PiggyInnerInstruction<'_>>],
    trailing_inner_instructions: &[&PiggyInnerInstruction<'_>],
    resolver: &KeyResolver<'_>,
    account_mints: &HashMap<u32, [u8; 32]>,
    discovered: &DiscoveryState,
) -> IndexKeys {
    let mut keys = IndexKeys::default();

    for account in tx_all_account_keys(tx, meta) {
        keys.accounts.insert(account);
        if discovered.users.contains(&account) {
            keys.users.insert(account);
        }
    }
    if let Some(payer) = resolver.resolve(0) {
        keys.users.insert(payer);
    }

    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Ok(mint) = parse_pubkey(balance.mint.as_ref()) {
            keys.mints.insert(mint);
        }
        if !balance.owner.is_empty()
            && let Ok(owner) = parse_pubkey(balance.owner.as_ref())
            && discovered.users.contains(&owner)
        {
            keys.users.insert(owner);
        }
        if let Some(account) = resolver.resolve(balance.account_index) {
            keys.accounts.insert(account);
        }
    }

    for instruction in instruction_views(
        outer_instructions,
        inner_groups,
        trailing_inner_instructions,
    ) {
        if let Some(program_id) = resolver.resolve(instruction.program_id_index) {
            keys.programs.insert(program_id);
        }
        for account_index in instruction.accounts {
            if let Some(account) = resolver.resolve(*account_index as u32) {
                keys.accounts.insert(account);
                if discovered.users.contains(&account) {
                    keys.users.insert(account);
                }
            }
        }
        if let Some(decoded) =
            decode_token_event(instruction.accounts, instruction.data, account_mints)
        {
            let mint = decoded.mint.or_else(|| {
                decoded
                    .mint_account_index
                    .and_then(|index| resolver.resolve(index))
            });
            if let Some(mint) = mint {
                keys.mints.insert(mint);
            }
            if let Some(user) = decoded
                .user_account_index
                .and_then(|index| resolver.resolve(index))
            {
                keys.users.insert(user);
            }
        }
    }

    keys
}

fn tx_touches_program(
    tx: &VersionedTransaction<'_>,
    meta: &PiggyMeta<'_>,
    outer_instructions: &[of_car_reader::versioned_transaction::CompiledInstruction],
    inner_groups: &[Vec<&PiggyInnerInstruction<'_>>],
    trailing_inner_instructions: &[&PiggyInnerInstruction<'_>],
    targets: &DumpTargets,
) -> bool {
    let resolver = KeyResolver::new(tx, meta);
    instruction_views(
        outer_instructions,
        inner_groups,
        trailing_inner_instructions,
    )
    .any(|instruction| {
        resolver
            .resolve(instruction.program_id_index)
            .map(|program_id| targets.program_pubkeys.contains(&program_id))
            .unwrap_or(false)
    })
}

fn instruction_views<'a>(
    outer_instructions: &'a [of_car_reader::versioned_transaction::CompiledInstruction],
    inner_groups: &'a [Vec<&'a PiggyInnerInstruction<'a>>],
    trailing_inner_instructions: &'a [&'a PiggyInnerInstruction<'a>],
) -> impl Iterator<Item = InstructionView<'a>> + 'a {
    let outer = outer_instructions
        .iter()
        .enumerate()
        .flat_map(|(index, instruction)| {
            let inner = inner_groups
                .get(index)
                .into_iter()
                .flat_map(|group| group.iter())
                .map(|instruction| InstructionView {
                    program_id_index: instruction.program_id_index,
                    accounts: instruction.accounts.as_ref(),
                    data: instruction.data.as_ref(),
                });
            std::iter::once(InstructionView {
                program_id_index: instruction.program_id_index as u32,
                accounts: &instruction.accounts,
                data: &instruction.data,
            })
            .chain(inner)
        });
    let trailing = trailing_inner_instructions
        .iter()
        .map(|instruction| InstructionView {
            program_id_index: instruction.program_id_index,
            accounts: instruction.accounts.as_ref(),
            data: instruction.data.as_ref(),
        });
    outer.chain(trailing)
}

fn decode_token_event(
    accounts: &[u8],
    data: &[u8],
    account_mints: &HashMap<u32, [u8; 32]>,
) -> Option<DecodedTokenEvent> {
    let tag = *data.first()?;
    match tag {
        0 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 0),
            user_account_index: None,
            counterparty_account_index: None,
        }),
        3 => {
            let source_index = *accounts.first()? as u32;
            let dest_index = *accounts.get(1)? as u32;
            let mint = mint_from_account_mints(account_mints, source_index)
                .or_else(|| mint_from_account_mints(account_mints, dest_index));
            Some(DecodedTokenEvent {
                mint,
                mint_account_index: None,
                token_account_index: account_index(accounts, 0),
                user_account_index: account_index(accounts, 2),
                counterparty_account_index: account_index(accounts, 1),
            })
        }
        7 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 1),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
        }),
        8 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
        }),
        9 => {
            let closing_account_index = *accounts.first()? as u32;
            Some(DecodedTokenEvent {
                mint: mint_from_account_mints(account_mints, closing_account_index),
                mint_account_index: None,
                token_account_index: account_index(accounts, 0),
                user_account_index: account_index(accounts, 2),
                counterparty_account_index: account_index(accounts, 1),
            })
        }
        12 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 3),
            counterparty_account_index: account_index(accounts, 2),
        }),
        14 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 1),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
        }),
        15 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 1),
            token_account_index: account_index(accounts, 0),
            user_account_index: account_index(accounts, 2),
            counterparty_account_index: None,
        }),
        20 => Some(DecodedTokenEvent {
            mint: None,
            mint_account_index: account_index(accounts, 0),
            token_account_index: account_index(accounts, 0),
            user_account_index: None,
            counterparty_account_index: None,
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
    mint: Option<[u8; 32]>,
    mint_account_index: Option<u32>,
    token_account_index: Option<u32>,
    user_account_index: Option<u32>,
    counterparty_account_index: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct InstructionView<'a> {
    program_id_index: u32,
    accounts: &'a [u8],
    data: &'a [u8],
}

#[derive(Debug)]
struct DumpContext {
    slot: u64,
    signature: String,
    signature_bytes: [u8; 64],
    tx_error: bool,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
    tx_index: Option<u64>,
}

impl DumpContext {
    fn dump_flags(&self) -> u32 {
        let mut flags = 0u32;
        if self.tx_error {
            flags |= DUMP_FLAG_TX_ERROR;
        }
        if self.compute_units_consumed.is_some() {
            flags |= DUMP_FLAG_HAS_COMPUTE_UNITS;
        }
        if self.cost_units.is_some() {
            flags |= DUMP_FLAG_HAS_COST_UNITS;
        }
        flags
    }
}

#[derive(Debug, Clone, Copy)]
struct DumpLocation {
    offset: u64,
    size: u64,
}

#[derive(Debug)]
struct DiscoveryState {
    users: HashSet<[u8; 32]>,
    token_accounts: HashSet<[u8; 32]>,
}

impl DiscoveryState {
    fn new(
        targets: &DumpTargets,
        seed_users: &HashSet<[u8; 32]>,
        seed_token_accounts: &HashSet<[u8; 32]>,
    ) -> Self {
        let mut users = targets.vault_pubkeys.clone();
        users.extend(seed_users.iter().copied());
        Self {
            users,
            token_accounts: seed_token_accounts.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct IndexKeys {
    users: HashSet<[u8; 32]>,
    accounts: HashSet<[u8; 32]>,
    mints: HashSet<[u8; 32]>,
    programs: HashSet<[u8; 32]>,
}

struct KeyResolver<'a> {
    static_keys: &'a [&'a [u8; 32]],
    loaded_writable: &'a [std::borrow::Cow<'a, [u8]>],
    loaded_readonly: &'a [std::borrow::Cow<'a, [u8]>],
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

fn tx_account_keys<'a>(tx: &'a VersionedTransaction<'a>) -> impl Iterator<Item = [u8; 32]> + 'a {
    let keys = match &tx.message {
        VersionedMessage::Legacy(message) => &message.account_keys,
        VersionedMessage::V0(message) => &message.account_keys,
    };
    keys.iter().map(|key| **key)
}

fn tx_all_account_keys<'a>(
    tx: &'a VersionedTransaction<'a>,
    meta: &'a PiggyMeta<'a>,
) -> impl Iterator<Item = [u8; 32]> + 'a {
    let static_keys = tx_account_keys(tx);
    let loaded_writable = meta
        .loaded_writable_addresses
        .iter()
        .filter_map(|key| slice_to_pubkey(key.as_ref()));
    let loaded_readonly = meta
        .loaded_readonly_addresses
        .iter()
        .filter_map(|key| slice_to_pubkey(key.as_ref()));
    static_keys.chain(loaded_writable).chain(loaded_readonly)
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

fn sorted_pubkeys(keys: &HashSet<[u8; 32]>) -> Vec<String> {
    let mut out: Vec<_> = keys.iter().copied().map(encode_pubkey).collect();
    out.sort_unstable();
    out
}

fn read_seed_pubkey_files(paths: &[PathBuf]) -> Result<HashSet<[u8; 32]>> {
    let mut out = HashSet::new();
    for path in paths {
        let file =
            File::open(path).with_context(|| format!("open seed file {}", path.display()))?;
        let reader = BufReader::new(file);
        for (line_index, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("read seed file {}", path.display()))?;
            let value = line.trim();
            if value.is_empty() || value.starts_with('#') {
                continue;
            }
            out.insert(
                parse_pubkey(value)
                    .with_context(|| format!("parse {} line {}", path.display(), line_index + 1))?,
            );
        }
    }
    Ok(out)
}

fn write_pubkey_set(path: &Path, keys: &HashSet<[u8; 32]>) -> Result<()> {
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    for key in sorted_pubkeys(keys) {
        writeln!(writer, "{key}")?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))
}

fn tx_index_text(tx_index: Option<u64>) -> String {
    tx_index
        .map(|index| index.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn optional_u64_text(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

#[derive(Debug)]
struct ProgressLog {
    started_at: Instant,
    last_print_at: Instant,
    last_printed_txs: usize,
    first_slot: Option<u64>,
}

impl ProgressLog {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_print_at: now,
            last_printed_txs: 0,
            first_slot: None,
        }
    }

    fn maybe_print(&mut self, summary: &PiggyDumpSummary, slot: u64, file_offset: u64) {
        if self.first_slot.is_none() {
            self.first_slot = Some(slot);
        }
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
        summary: &PiggyDumpSummary,
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
        let mib_per_sec = if elapsed > 0.0 { mib / elapsed } else { 0.0 };
        let slot_per_sec = match (self.first_slot, slot) {
            (Some(first_slot), Some(slot)) if elapsed > 0.0 => {
                slot.saturating_sub(first_slot) as f64 / elapsed
            }
            _ => 0.0,
        };
        eprintln!(
            "piggy-dump: {}txs={} dumped={} skipped={} decode_failures={} users={} token_accounts={} dump_mib={:.1} slot={} read_mib={:.1} read_mib_s={:.1} elapsed_s={:.0} tx_s={:.0} slot_s={:.1}",
            if final_line { "done " } else { "" },
            summary.scanned_txs,
            summary.dumped_transactions,
            summary.skipped_txs,
            summary.decode_failures,
            summary.discovered_users,
            summary.discovered_token_accounts,
            summary.dump_bytes as f64 / (1024.0 * 1024.0),
            slot.map(|slot| slot.to_string())
                .unwrap_or_else(|| "-".to_string()),
            mib,
            mib_per_sec,
            elapsed,
            tx_per_sec,
            slot_per_sec
        );
        self.last_print_at = Instant::now();
        self.last_printed_txs = summary.scanned_txs;
    }
}

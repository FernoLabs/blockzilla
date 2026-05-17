use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, UNIX_EPOCH},
};

const SLOTS_PER_SECOND: f64 = 2.5;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const PROGRESS_ROW_INTERVAL: usize = 1_000_000;
const FLOW_CANDIDATE_HEADER: &[&str] = &[
    "burn_id",
    "withdraw_signature",
    "user",
    "withdraw_slot",
    "withdraw_tx_index",
    "pb_mint",
    "pb_amount_raw",
    "signature",
    "slot",
    "tx_index",
    "epoch",
    "tx_size",
    "compute_units_consumed",
    "cost_units",
    "tx_error",
];
const WORK_SCHEMA_VERSION: u32 = 2;
const ANNOTATED_FLOW_HEADER: &[&str] = &[
    "burn_id",
    "withdraw_signature",
    "user",
    "withdraw_slot",
    "withdraw_tx_index",
    "pb_mint",
    "pb_amount_raw",
    "signature",
    "slot",
    "slot_delta",
    "tx_index",
    "epoch",
    "classification",
    "protocol_guess",
    "programs",
    "mints",
    "tx_size",
    "compute_units_consumed",
    "cost_units",
    "tx_error",
];

const PIGGY_PROGRAMS: &[&str] = &[
    "Pig1CsXnfDwN1NuoeNRBojohbjc14dogmJCXeb2vL3Y",
    "Pig2ienhM3ukiTec3x8aCdnLASpU4z8yRPLgH9QxDvm",
];

const PB_MINTS: &[(&str, &str)] = &[
    ("F35yYmTR6PqkbTx449P1eGhB57mRhWAdYs93eCo2dMZR", "pbUSDC"),
    ("E65CoK961Rs5LzKhGZxbKsB7xpFhYhXogH8nhr8zamTK", "pbSPYx"),
    ("FWLk7bGAqB8YW4HCbGbyXvL5HzUvfnHU2BwNi1kWc1Vt", "pbJitoSOL"),
];

pub const DEFAULT_MAX_FLOWS_PER_BURN: usize = 8;

const IGNORED_PROGRAMS: &[&str] = &[
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "Sysvar1nstructions1111111111111111111111111",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLCyzxA6Pgs6YtUKiG",
];

const DEFAULT_PROGRAM_LABELS: &[(&str, &str, &str)] = &[
    (
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        "Jupiter",
        "swap",
    ),
    (
        "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
        "Jupiter v4",
        "swap",
    ),
    (
        "675kPX9MHTjS2zt1qfr1NY5gQTvneKk2hHfhaU9fG",
        "Raydium AMM",
        "swap",
    ),
    (
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "Raydium CLMM",
        "swap",
    ),
    (
        "whirLbMiicVdio4qvUfM5KAg6CtJm5Fh6LauYUW2M8",
        "Orca Whirlpool",
        "swap",
    ),
    (
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        "Meteora DLMM",
        "swap",
    ),
    (
        "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        "Phoenix",
        "swap",
    ),
    (
        "opnb2LAfJYbR3sA6bfUsn6yA5RFyXaKfQhXvzo3f9HY",
        "OpenBook v2",
        "swap",
    ),
    (
        "9xQeWvG816bUx9EPfMaT23yvVM2ZW1M5D55F5X9yE",
        "Serum/OpenBook",
        "swap",
    ),
    (
        "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo",
        "Solend",
        "deposit_other_defi",
    ),
    (
        "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA",
        "Marginfi v2",
        "deposit_other_defi",
    ),
    (
        "worm2ZoG2kUd4vFXhvjh93UU596ayRfgQ2MgjNMTth",
        "Wormhole Core",
        "bridge",
    ),
    (
        "wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb",
        "Wormhole Token Bridge",
        "bridge",
    ),
];

#[derive(Debug, Clone)]
pub struct PiggyAnalyzeConfig {
    pub dump_dir: PathBuf,
    pub event_paths: Vec<PathBuf>,
    pub out_dir: PathBuf,
    pub windows: Vec<String>,
    pub completed_summary: Option<PathBuf>,
    pub program_label_file: Option<PathBuf>,
    pub max_top: usize,
    pub max_flows_per_burn: usize,
    pub collect_top_users: bool,
    pub collect_holder_stats: bool,
}

#[derive(Debug, Clone)]
pub struct PiggyAnalyzeSummary {
    pub withdraw_burns: usize,
    pub withdraw_users: usize,
    pub epoch_dirs: usize,
    pub dumped_txs: usize,
    pub users_in_dump_index: usize,
    pub out_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct Burn {
    burn_id: usize,
    user: String,
    slot: u64,
    signature: String,
    mint: String,
    amount_raw: Option<u128>,
    block_time: Option<i64>,
    tx_index: Option<u64>,
}

#[derive(Debug, Default)]
struct HolderStats {
    deposits: usize,
    withdrawals: usize,
    deposited_raw: u128,
    withdrawn_raw: u128,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
}

impl HolderStats {
    fn touch(&mut self, slot: u64) {
        self.first_slot = Some(self.first_slot.map_or(slot, |first| first.min(slot)));
        self.last_slot = Some(self.last_slot.map_or(slot, |last| last.max(slot)));
    }
}

#[derive(Debug, Default)]
struct UserActivity {
    txs: usize,
    bytes: u64,
    compute_units: u64,
    cost_units: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
}

impl UserActivity {
    fn add(&mut self, slot: u64, size: u64, compute_units: u64, cost_units: u64) {
        self.txs += 1;
        self.bytes += size;
        self.compute_units += compute_units;
        self.cost_units += cost_units;
        self.first_slot = Some(self.first_slot.map_or(slot, |first| first.min(slot)));
        self.last_slot = Some(self.last_slot.map_or(slot, |last| last.max(slot)));
    }
}

#[derive(Debug, Default)]
struct ValueStats {
    counts: BTreeMap<u64, u64>,
    count: u64,
    sum: u128,
}

impl ValueStats {
    fn add(&mut self, value: u64) {
        *self.counts.entry(value).or_default() += 1;
        self.count += 1;
        self.sum += value as u128;
    }

    fn percentile(&self, pct: u64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64) * (pct as f64 / 100.0)).ceil() as u64;
        let target = target.max(1);
        let mut seen = 0u64;
        for (value, count) in &self.counts {
            seen += *count;
            if seen >= target {
                return *value;
            }
        }
        self.counts.keys().next_back().copied().unwrap_or(0)
    }

    fn mean(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        (self.sum / self.count as u128) as u64
    }

    fn unique_values(&self) -> usize {
        self.counts.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FlowKey {
    burn_id: usize,
    signature: String,
}

#[derive(Debug, Clone)]
struct FlowTx {
    burn: Burn,
    signature: String,
    slot: u64,
    tx_index: String,
    size: u64,
    tx_error: String,
    compute_units: u64,
    cost_units: u64,
    epoch: String,
    programs: HashSet<String>,
    mints: HashSet<String>,
}

#[derive(Debug)]
struct OutcomeAgg {
    first_signature: String,
    first_slot_delta: u64,
    first_sort_key: (u64, u64, String),
    first_classification: String,
    first_protocol: String,
    all_classes: BTreeMap<String, usize>,
    tx_count: usize,
}

impl OutcomeAgg {
    fn add(
        &mut self,
        signature: &str,
        tx_index: &str,
        slot_delta: u64,
        classification: &str,
        protocol: &str,
    ) {
        self.tx_count += 1;
        *self
            .all_classes
            .entry(classification.to_string())
            .or_default() += 1;
        let sort_key = flow_sort_key(slot_delta, tx_index, signature);
        if sort_key < self.first_sort_key {
            self.first_signature = signature.to_string();
            self.first_slot_delta = slot_delta;
            self.first_sort_key = sort_key;
            self.first_classification = classification.to_string();
            self.first_protocol = protocol.to_string();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileFingerprint {
    path: String,
    len: u64,
    modified_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkManifest {
    schema_version: u32,
    dump_dir: String,
    epoch_dirs: Vec<String>,
    event_files: Vec<FileFingerprint>,
    completed_summary: Option<FileFingerprint>,
    program_label_file: Option<FileFingerprint>,
    windows: Vec<String>,
    max_flows_per_burn: usize,
    pb_mints: Vec<String>,
    piggy_programs: Vec<String>,
    default_program_labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PiggyScanEvent {
    slot: u64,
    block_time: Option<i64>,
    signature: String,
    tx_error: bool,
    tx_index: Option<u64>,
    user: Option<String>,
    mint: Option<String>,
    event_type: String,
    amount: Option<String>,
}

#[derive(Debug)]
struct TsvRow<'a> {
    fields: &'a [&'a str],
    header: &'a HashMap<String, usize>,
}

impl TsvRow<'_> {
    fn get(&self, name: &str) -> &str {
        self.header
            .get(name)
            .and_then(|index| self.fields.get(*index))
            .copied()
            .unwrap_or("")
    }
}

pub fn analyze_piggy_flows(config: PiggyAnalyzeConfig) -> Result<PiggyAnalyzeSummary> {
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("create {}", config.out_dir.display()))?;

    let started_at = Instant::now();
    eprintln!(
        "piggy-analyze: start dump_dir={} out_dir={} event_files={} completed_summary={}",
        config.dump_dir.display(),
        config.out_dir.display(),
        config.event_paths.len(),
        config
            .completed_summary
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string())
    );

    let labels = load_program_labels(config.program_label_file.as_deref())?;
    let windows = if config.windows.is_empty() {
        vec![
            "1h".to_string(),
            "24h".to_string(),
            "7d".to_string(),
            "30d".to_string(),
        ]
    } else {
        config.windows.clone()
    };
    let mut window_slots = BTreeMap::new();
    for window in &windows {
        window_slots.insert(window.clone(), parse_duration_slots(window)?);
    }
    let max_window = window_slots.values().copied().max().unwrap_or(0);
    let max_flows_per_burn = if config.max_flows_per_burn == 0 {
        usize::MAX
    } else {
        config.max_flows_per_burn
    };
    eprintln!(
        "piggy-analyze: windows={} max_window_slots={} max_flows_per_burn={}",
        windows.join(","),
        max_window,
        if max_flows_per_burn == usize::MAX {
            "unlimited".to_string()
        } else {
            max_flows_per_burn.to_string()
        }
    );

    let (burns, holders, event_counts) =
        load_scan_events(&config.event_paths, config.collect_holder_stats)?;
    eprintln!(
        "piggy-analyze: loaded events withdraw_burns={} holder_pairs={} holder_stats={} event_types={}",
        burns.len(),
        holders.len(),
        if config.collect_holder_stats {
            "enabled"
        } else {
            "disabled"
        },
        event_counts.len()
    );
    let mut burns_by_user: HashMap<String, Vec<Burn>> = HashMap::new();
    for burn in &burns {
        burns_by_user
            .entry(burn.user.clone())
            .or_default()
            .push(burn.clone());
    }
    for user_burns in burns_by_user.values_mut() {
        user_burns.sort_by_key(|burn| burn.slot);
    }

    let epoch_dirs = epoch_dirs(&config.dump_dir, config.completed_summary.as_deref())?;
    if epoch_dirs.is_empty() {
        bail!(
            "no epoch-* directories found under {}",
            config.dump_dir.display()
        );
    }
    eprintln!(
        "piggy-analyze: discovered {} epoch dump dirs under {}",
        epoch_dirs.len(),
        config.dump_dir.display()
    );

    let work_dir = config.out_dir.join("_work");
    let candidate_dir = work_dir.join("flow-candidates");
    let annotated_dir = work_dir.join("annotated");
    fs::create_dir_all(&candidate_dir)
        .with_context(|| format!("create {}", candidate_dir.display()))?;
    fs::create_dir_all(&annotated_dir)
        .with_context(|| format!("create {}", annotated_dir.display()))?;
    let work_manifest =
        WorkManifest::from_config(&config, &epoch_dirs, &windows, max_flows_per_burn)?;
    validate_or_write_work_manifest(&work_dir, &work_manifest)?;

    let mut active_users: HashMap<String, UserActivity> = HashMap::new();
    let mut signature_sizes = ValueStats::default();
    let mut signature_compute_units = ValueStats::default();
    let mut signature_cost_units = ValueStats::default();
    let mut signature_error_count = 0usize;
    let mut signature_count = 0usize;
    let mut dump_bytes = 0u64;
    let mut flows_per_burn: HashMap<usize, usize> = HashMap::new();
    let mut skipped_flow_candidates_by_cap = 0usize;
    let mut candidate_flow_txs = 0usize;

    for (epoch_index, epoch_dir) in epoch_dirs.iter().enumerate() {
        eprintln!(
            "piggy-analyze: pass1 epoch {}/{} {}",
            epoch_index + 1,
            epoch_dirs.len(),
            epoch_dir.display()
        );
        let signature_path = epoch_dir.join("signature.index.tsv");
        if signature_path.exists() {
            read_tsv_rows_progress(&signature_path, "signature index", |row| {
                signature_count += 1;
                let size = parse_u64(row.get("size"));
                let compute_units = parse_u64(row.get("compute_units_consumed"));
                let cost_units = parse_u64(row.get("cost_units"));
                signature_sizes.add(size);
                if compute_units > 0 {
                    signature_compute_units.add(compute_units);
                }
                if cost_units > 0 {
                    signature_cost_units.add(cost_units);
                }
                if row.get("tx_error") == "true" {
                    signature_error_count += 1;
                }
                dump_bytes += size;
                Ok(())
            })?;
        }

        let user_path = epoch_dir.join("user.index.tsv");
        if !user_path.exists() {
            eprintln!(
                "piggy-analyze: pass1 epoch {}/{} skipped missing {}",
                epoch_index + 1,
                epoch_dirs.len(),
                user_path.display()
            );
            continue;
        }
        let epoch = epoch_id(epoch_dir);
        let candidate_path = candidate_dir.join(format!("epoch-{epoch}.tsv"));
        if candidate_path.exists() {
            let rows = load_candidate_counts_file(&candidate_path, &mut flows_per_burn)?;
            candidate_flow_txs += rows;
            eprintln!(
                "piggy-analyze: pass1 epoch {}/{} reusing {} rows={}",
                epoch_index + 1,
                epoch_dirs.len(),
                candidate_path.display(),
                rows
            );
            if config.collect_top_users {
                read_user_index_top_users(&user_path, &mut active_users)?;
            }
            continue;
        }
        let candidate_tmp_path = candidate_path.with_extension("tsv.tmp");
        if candidate_tmp_path.exists() {
            fs::remove_file(&candidate_tmp_path)
                .with_context(|| format!("remove {}", candidate_tmp_path.display()))?;
        }

        eprintln!(
            "piggy-analyze: pass1 epoch {}/{} reading {}",
            epoch_index + 1,
            epoch_dirs.len(),
            user_path.display()
        );
        {
            let mut writer = BufWriter::new(
                File::create(&candidate_tmp_path)
                    .with_context(|| format!("create {}", candidate_tmp_path.display()))?,
            );
            writeln!(writer, "{}", FLOW_CANDIDATE_HEADER.join("\t"))?;
            let mut epoch_seen = HashSet::new();
            let mut epoch_candidates = 0usize;
            read_user_index_rows_progress(
                &user_path,
                &burns_by_user,
                config.collect_top_users,
                |row| {
                    let user = row.get("user");
                    let user_burns = burns_by_user.get(user);

                    let slot = parse_u64(row.get("slot"));
                    let row_tx_index = parse_optional_u64(row.get("tx_index"));
                    let size = parse_u64(row.get("size"));
                    let compute_units = parse_u64(row.get("compute_units_consumed"));
                    let cost_units = parse_u64(row.get("cost_units"));

                    if config.collect_top_users {
                        active_users.entry(user.to_string()).or_default().add(
                            slot,
                            size,
                            compute_units,
                            cost_units,
                        );
                    }

                    let Some(user_burns) = user_burns else {
                        return Ok(());
                    };
                    let lo = user_burns
                        .partition_point(|burn| burn.slot.saturating_add(max_window) < slot);
                    let hi = user_burns.partition_point(|burn| burn.slot <= slot);
                    for burn in &user_burns[lo..hi] {
                        if !is_after_burn(slot, row_tx_index, burn) {
                            continue;
                        }
                        let burn_flow_count = flows_per_burn.entry(burn.burn_id).or_default();
                        if *burn_flow_count >= max_flows_per_burn {
                            skipped_flow_candidates_by_cap += 1;
                            continue;
                        }
                        let signature = row.get("signature");
                        let key = FlowKey {
                            burn_id: burn.burn_id,
                            signature: signature.to_string(),
                        };
                        if !epoch_seen.insert(key) {
                            continue;
                        }
                        write_flow_candidate(&mut writer, burn, &row, &epoch)?;
                        *burn_flow_count += 1;
                        candidate_flow_txs += 1;
                        epoch_candidates += 1;
                    }
                    Ok(())
                },
            )?;
            writer.flush()?;
            eprintln!(
                "piggy-analyze: pass1 epoch {}/{} wrote candidate_flow_txs={} file={}",
                epoch_index + 1,
                epoch_dirs.len(),
                epoch_candidates,
                candidate_tmp_path.display()
            );
        }
        fs::rename(&candidate_tmp_path, &candidate_path).with_context(|| {
            format!(
                "rename {} to {}",
                candidate_tmp_path.display(),
                candidate_path.display()
            )
        })?;
    }

    eprintln!(
        "piggy-analyze: pass1 done dumped_txs={} active_users={} candidate_flow_txs={} skipped_flow_candidates_by_cap={} unique_tx_sizes={} unique_compute_units={} unique_cost_units={}",
        signature_count,
        active_users.len(),
        candidate_flow_txs,
        skipped_flow_candidates_by_cap,
        signature_sizes.unique_values(),
        signature_compute_units.unique_values(),
        signature_cost_units.unique_values()
    );

    let mut annotated_flow_txs = 0usize;
    for (epoch_index, epoch_dir) in epoch_dirs.iter().enumerate() {
        eprintln!(
            "piggy-analyze: pass2 epoch {}/{} {}",
            epoch_index + 1,
            epoch_dirs.len(),
            epoch_dir.display()
        );
        let epoch = epoch_id(epoch_dir);
        let candidate_path = candidate_dir.join(format!("epoch-{epoch}.tsv"));
        if !candidate_path.exists() {
            eprintln!(
                "piggy-analyze: pass2 epoch {}/{} skipped missing {}",
                epoch_index + 1,
                epoch_dirs.len(),
                candidate_path.display()
            );
            continue;
        }
        let annotated_path = annotated_dir.join(format!("epoch-{epoch}.tsv"));
        if annotated_path.exists() {
            let rows = count_tsv_data_rows(&annotated_path)?;
            annotated_flow_txs += rows;
            eprintln!(
                "piggy-analyze: pass2 epoch {}/{} reusing {} rows={}",
                epoch_index + 1,
                epoch_dirs.len(),
                annotated_path.display(),
                rows
            );
            continue;
        }
        let annotated_tmp_path = annotated_path.with_extension("tsv.tmp");
        if annotated_tmp_path.exists() {
            fs::remove_file(&annotated_tmp_path)
                .with_context(|| format!("remove {}", annotated_tmp_path.display()))?;
        }

        let mut epoch_flow_txs = read_flow_candidates(&candidate_path)?;
        annotated_flow_txs += epoch_flow_txs.len();
        let mut flows_by_signature: HashMap<String, Vec<usize>> = HashMap::new();
        for (index, tx) in epoch_flow_txs.iter().enumerate() {
            flows_by_signature
                .entry(tx.signature.clone())
                .or_default()
                .push(index);
        }
        eprintln!(
            "piggy-analyze: pass2 epoch {}/{} loaded candidate_flow_txs={} signatures={}",
            epoch_index + 1,
            epoch_dirs.len(),
            epoch_flow_txs.len(),
            flows_by_signature.len()
        );
        let program_path = epoch_dir.join("program.index.tsv");
        if program_path.exists() {
            read_tsv_rows_progress(&program_path, "program index", |row| {
                let Some(indexes) = flows_by_signature.get(row.get("signature")) else {
                    return Ok(());
                };
                let program = row.get("program");
                for index in indexes {
                    if let Some(tx) = epoch_flow_txs.get_mut(*index) {
                        tx.programs.insert(program.to_string());
                    }
                }
                Ok(())
            })?;
        }

        let mint_path = epoch_dir.join("mint.index.tsv");
        if mint_path.exists() {
            read_tsv_rows_progress(&mint_path, "mint index", |row| {
                let Some(indexes) = flows_by_signature.get(row.get("signature")) else {
                    return Ok(());
                };
                let mint = row.get("mint");
                for index in indexes {
                    if let Some(tx) = epoch_flow_txs.get_mut(*index) {
                        tx.mints.insert(mint.to_string());
                    }
                }
                Ok(())
            })?;
        }
        {
            let mut writer = BufWriter::new(
                File::create(&annotated_tmp_path)
                    .with_context(|| format!("create {}", annotated_tmp_path.display()))?,
            );
            writeln!(writer, "{}", ANNOTATED_FLOW_HEADER.join("\t"))?;
            for tx in &epoch_flow_txs {
                write_annotated_flow(&mut writer, tx, &labels)?;
            }
            writer.flush()?;
        }
        fs::rename(&annotated_tmp_path, &annotated_path).with_context(|| {
            format!(
                "rename {} to {}",
                annotated_tmp_path.display(),
                annotated_path.display()
            )
        })?;
    }
    eprintln!(
        "piggy-analyze: pass2 done annotated_flow_txs={}",
        annotated_flow_txs
    );

    let post_withdraw_path = config.out_dir.join("post_withdraw_flows.tsv");
    let mut flow_writer = BufWriter::new(
        File::create(&post_withdraw_path)
            .with_context(|| format!("create {}", post_withdraw_path.display()))?,
    );
    writeln!(
        flow_writer,
        "{}",
        [
            "burn_id",
            "withdraw_signature",
            "user",
            "withdraw_slot",
            "pb_mint",
            "pb_amount_raw",
            "signature",
            "slot",
            "slot_delta",
            "epoch",
            "classification",
            "protocol_guess",
            "programs",
            "mints",
            "tx_size",
            "compute_units_consumed",
            "cost_units",
            "tx_error",
        ]
        .join("\t")
    )?;

    let mut outcomes_by_burn_window: HashMap<(usize, usize), OutcomeAgg> = HashMap::new();
    let mut flow_tx_classification_counts: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut flow_tx_classification_counts_by_token: BTreeMap<(String, String, String), usize> =
        BTreeMap::new();
    let mut protocol_counts: HashMap<String, usize> = HashMap::new();
    let mut protocol_counts_by_token: HashMap<(String, String), usize> = HashMap::new();
    let mut mint_counts: HashMap<String, usize> = HashMap::new();
    let mut mint_counts_by_token: HashMap<(String, String), usize> = HashMap::new();
    let window_slot_vec = windows
        .iter()
        .map(|window| (window.clone(), *window_slots.get(window).unwrap_or(&0)))
        .collect::<Vec<_>>();

    for (epoch_index, epoch_dir) in epoch_dirs.iter().enumerate() {
        let epoch = epoch_id(epoch_dir);
        let annotated_path = annotated_dir.join(format!("epoch-{epoch}.tsv"));
        if !annotated_path.exists() {
            continue;
        }
        eprintln!(
            "piggy-analyze: reduce epoch {}/{} {}",
            epoch_index + 1,
            epoch_dirs.len(),
            annotated_path.display()
        );
        read_tsv_rows_progress(&annotated_path, "annotated flow", |row| {
            let burn_id = parse_u64(row.get("burn_id")) as usize;
            let slot_delta = parse_u64(row.get("slot_delta"));
            let classification = row.get("classification");
            let protocol = row.get("protocol_guess");
            let signature = row.get("signature");
            let tx_index = row.get("tx_index");
            let pb_mint = row.get("pb_mint");

            writeln!(
                flow_writer,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                row.get("burn_id"),
                row.get("withdraw_signature"),
                row.get("user"),
                row.get("withdraw_slot"),
                row.get("pb_mint"),
                row.get("pb_amount_raw"),
                signature,
                row.get("slot"),
                row.get("slot_delta"),
                row.get("epoch"),
                classification,
                protocol,
                row.get("programs"),
                row.get("mints"),
                row.get("tx_size"),
                row.get("compute_units_consumed"),
                row.get("cost_units"),
                row.get("tx_error"),
            )?;

            for (window_index, (window, slots)) in window_slot_vec.iter().enumerate() {
                if slot_delta <= *slots {
                    *flow_tx_classification_counts
                        .entry((window.clone(), classification.to_string()))
                        .or_default() += 1;
                    *flow_tx_classification_counts_by_token
                        .entry((
                            pb_mint.to_string(),
                            window.clone(),
                            classification.to_string(),
                        ))
                        .or_default() += 1;
                    let outcome = outcomes_by_burn_window
                        .entry((burn_id, window_index))
                        .or_insert_with(|| {
                            let mut all_classes = BTreeMap::new();
                            all_classes.insert(classification.to_string(), 0);
                            OutcomeAgg {
                                first_signature: signature.to_string(),
                                first_slot_delta: slot_delta,
                                first_sort_key: flow_sort_key(slot_delta, tx_index, signature),
                                first_classification: classification.to_string(),
                                first_protocol: protocol.to_string(),
                                all_classes,
                                tx_count: 0,
                            }
                        });
                    outcome.add(signature, tx_index, slot_delta, classification, protocol);
                }
            }
            for protocol in protocol.split(',').filter(|value| !value.is_empty()) {
                *protocol_counts.entry(protocol.to_string()).or_default() += 1;
                *protocol_counts_by_token
                    .entry((pb_mint.to_string(), protocol.to_string()))
                    .or_default() += 1;
            }
            for mint in row
                .get("mints")
                .split(',')
                .filter(|value| !value.is_empty())
            {
                *mint_counts.entry(mint.to_string()).or_default() += 1;
                *mint_counts_by_token
                    .entry((pb_mint.to_string(), mint.to_string()))
                    .or_default() += 1;
            }
            Ok(())
        })?;
    }
    flow_writer.flush()?;

    let withdraw_outcomes_path = config.out_dir.join("withdraw_outcomes.tsv");
    let mut outcome_writer = BufWriter::new(
        File::create(&withdraw_outcomes_path)
            .with_context(|| format!("create {}", withdraw_outcomes_path.display()))?,
    );
    writeln!(
        outcome_writer,
        "{}",
        [
            "burn_id",
            "withdraw_signature",
            "user",
            "withdraw_slot",
            "pb_mint",
            "pb_amount_raw",
            "window",
            "first_action_classification",
            "first_protocol_guess",
            "first_signature",
            "first_slot_delta",
            "all_classifications",
            "tx_count",
        ]
        .join("\t")
    )?;
    let mut outcome_counts: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut outcome_counts_by_token: BTreeMap<(String, String, String), usize> = BTreeMap::new();
    for burn in &burns {
        for (window_index, (window, _)) in window_slot_vec.iter().enumerate() {
            let Some(outcome) = outcomes_by_burn_window.remove(&(burn.burn_id, window_index))
            else {
                *outcome_counts
                    .entry((window.clone(), "wallet_idle".to_string()))
                    .or_default() += 1;
                *outcome_counts_by_token
                    .entry((burn.mint.clone(), window.clone(), "wallet_idle".to_string()))
                    .or_default() += 1;
                writeln!(
                    outcome_writer,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\twallet_idle\t\t\t\t\t0",
                    burn.burn_id,
                    burn.signature,
                    burn.user,
                    burn.slot,
                    burn.mint,
                    option_u128_text(burn.amount_raw),
                    window,
                )?;
                continue;
            };
            *outcome_counts
                .entry((window.clone(), outcome.first_classification.clone()))
                .or_default() += 1;
            *outcome_counts_by_token
                .entry((
                    burn.mint.clone(),
                    window.clone(),
                    outcome.first_classification.clone(),
                ))
                .or_default() += 1;
            let all_classifications = outcome
                .all_classes
                .into_iter()
                .map(|(class, count)| format!("{class}:{count}"))
                .collect::<Vec<_>>()
                .join(",");
            writeln!(
                outcome_writer,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                burn.burn_id,
                burn.signature,
                burn.user,
                burn.slot,
                burn.mint,
                option_u128_text(burn.amount_raw),
                window,
                outcome.first_classification,
                outcome.first_protocol,
                outcome.first_signature,
                outcome.first_slot_delta,
                all_classifications,
                outcome.tx_count,
            )?;
        }
    }
    outcome_writer.flush()?;

    write_rows(
        &config.out_dir.join("withdraws.tsv"),
        &[
            "burn_id",
            "signature",
            "user",
            "slot",
            "tx_index",
            "block_time",
            "pb_mint",
            "amount_raw",
        ],
        burns.iter().map(|burn| {
            vec![
                burn.burn_id.to_string(),
                burn.signature.clone(),
                burn.user.clone(),
                burn.slot.to_string(),
                burn.tx_index
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                burn.block_time
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                burn.mint.clone(),
                option_u128_text(burn.amount_raw),
            ]
        }),
    )?;
    write_token_summary(&config.out_dir.join("token_summary.tsv"), &burns)?;

    write_counter_rows(
        &config.out_dir.join("classification_counts.tsv"),
        &["window", "classification", "withdraws"],
        outcome_counts
            .iter()
            .map(|((window, class), count)| vec![window.clone(), class.clone(), count.to_string()]),
    )?;
    write_counter_rows(
        &config.out_dir.join("flow_tx_classification_counts.tsv"),
        &["window", "classification", "txs"],
        flow_tx_classification_counts
            .iter()
            .map(|((window, class), count)| vec![window.clone(), class.clone(), count.to_string()]),
    )?;
    write_counter_rows(
        &config.out_dir.join("classification_counts_by_token.tsv"),
        &["pb_mint", "symbol", "window", "classification", "withdraws"],
        outcome_counts_by_token
            .iter()
            .map(|((mint, window, class), count)| {
                vec![
                    mint.clone(),
                    pb_symbol(mint).unwrap_or("").to_string(),
                    window.clone(),
                    class.clone(),
                    count.to_string(),
                ]
            }),
    )?;
    write_counter_rows(
        &config
            .out_dir
            .join("flow_tx_classification_counts_by_token.tsv"),
        &["pb_mint", "symbol", "window", "classification", "txs"],
        flow_tx_classification_counts_by_token
            .iter()
            .map(|((mint, window, class), count)| {
                vec![
                    mint.clone(),
                    pb_symbol(mint).unwrap_or("").to_string(),
                    window.clone(),
                    class.clone(),
                    count.to_string(),
                ]
            }),
    )?;

    if config.collect_holder_stats {
        write_holder_stats(&config.out_dir.join("holder_stats.tsv"), &holders)?;
    } else {
        write_rows(
            &config.out_dir.join("holder_stats.tsv"),
            &[
                "user",
                "mint",
                "symbol",
                "deposits",
                "withdrawals",
                "deposited_raw",
                "withdrawn_raw",
                "net_raw",
                "first_slot",
                "last_slot",
            ],
            std::iter::empty(),
        )?;
    }
    if config.collect_top_users {
        write_top_users(
            &config.out_dir.join("top_users.tsv"),
            &active_users,
            config.max_top,
        )?;
    } else {
        write_rows(
            &config.out_dir.join("top_users.tsv"),
            &[
                "user",
                "txs",
                "bytes",
                "compute_units_consumed",
                "cost_units",
                "first_slot",
                "last_slot",
            ],
            std::iter::empty(),
        )?;
    }
    write_ranked_counter(
        &config.out_dir.join("top_protocols_after_withdraw.tsv"),
        &["protocol_guess", "txs"],
        &protocol_counts,
        config.max_top,
        |_| None,
    )?;
    write_ranked_counter(
        &config.out_dir.join("top_mints_after_withdraw.tsv"),
        &["mint", "symbol", "txs"],
        &mint_counts,
        config.max_top,
        |mint| Some(pb_symbol(mint).unwrap_or("").to_string()),
    )?;
    write_ranked_pair_counter(
        &config
            .out_dir
            .join("top_protocols_after_withdraw_by_token.tsv"),
        &["pb_mint", "symbol", "protocol_guess", "txs"],
        &protocol_counts_by_token,
        config.max_top,
        |_| None,
    )?;
    write_ranked_pair_counter(
        &config.out_dir.join("top_mints_after_withdraw_by_token.tsv"),
        &["pb_mint", "symbol", "mint", "mint_symbol", "txs"],
        &mint_counts_by_token,
        config.max_top,
        |mint| Some(pb_symbol(mint).unwrap_or("").to_string()),
    )?;

    let tx_stats = vec![
        ("dumped_txs".to_string(), signature_count.to_string()),
        ("dump_bytes_sum".to_string(), dump_bytes.to_string()),
        (
            "tx_error_count".to_string(),
            signature_error_count.to_string(),
        ),
        (
            "tx_size_p50".to_string(),
            signature_sizes.percentile(50).to_string(),
        ),
        (
            "tx_size_p90".to_string(),
            signature_sizes.percentile(90).to_string(),
        ),
        (
            "tx_size_p99".to_string(),
            signature_sizes.percentile(99).to_string(),
        ),
        (
            "tx_size_mean".to_string(),
            signature_sizes.mean().to_string(),
        ),
        (
            "compute_units_p50".to_string(),
            signature_compute_units.percentile(50).to_string(),
        ),
        (
            "compute_units_p90".to_string(),
            signature_compute_units.percentile(90).to_string(),
        ),
        (
            "compute_units_p99".to_string(),
            signature_compute_units.percentile(99).to_string(),
        ),
        (
            "cost_units_p50".to_string(),
            signature_cost_units.percentile(50).to_string(),
        ),
        (
            "cost_units_p90".to_string(),
            signature_cost_units.percentile(90).to_string(),
        ),
        (
            "cost_units_p99".to_string(),
            signature_cost_units.percentile(99).to_string(),
        ),
    ];
    write_rows(
        &config.out_dir.join("transaction_size_stats.tsv"),
        &["metric", "value"],
        tx_stats
            .iter()
            .map(|(name, value)| vec![name.clone(), value.clone()]),
    )?;

    write_summary(
        &config,
        &event_counts,
        &outcome_counts,
        &outcome_counts_by_token,
        &tx_stats,
        &burns,
        burns.len(),
        burns
            .iter()
            .map(|burn| &burn.user)
            .collect::<HashSet<_>>()
            .len(),
        epoch_dirs.len(),
        signature_count,
        active_users.len(),
    )?;
    eprintln!(
        "piggy-analyze: done withdraw_burns={} users={} epoch_dirs={} dumped_txs={} elapsed_s={:.0} out_dir={}",
        burns.len(),
        burns
            .iter()
            .map(|burn| &burn.user)
            .collect::<HashSet<_>>()
            .len(),
        epoch_dirs.len(),
        signature_count,
        started_at.elapsed().as_secs_f64(),
        config.out_dir.display()
    );

    Ok(PiggyAnalyzeSummary {
        withdraw_burns: burns.len(),
        withdraw_users: burns
            .iter()
            .map(|burn| &burn.user)
            .collect::<HashSet<_>>()
            .len(),
        epoch_dirs: epoch_dirs.len(),
        dumped_txs: signature_count,
        users_in_dump_index: active_users.len(),
        out_dir: config.out_dir,
    })
}

type LoadedScanEvents = (
    Vec<Burn>,
    HashMap<(String, String), HolderStats>,
    BTreeMap<String, usize>,
);

fn load_scan_events(paths: &[PathBuf], collect_holder_stats: bool) -> Result<LoadedScanEvents> {
    let mut burns = Vec::new();
    let mut holders: HashMap<(String, String), HolderStats> = HashMap::new();
    let mut event_counts = BTreeMap::new();
    let mut seen_burns = HashSet::new();

    for path in paths {
        eprintln!("piggy-analyze: loading events {}", path.display());
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut progress = RowProgress::new(path, "events");
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            progress.maybe_print(line_index + 1);
            let line = line.with_context(|| format!("read {}", path.display()))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let event: PiggyScanEvent = serde_json::from_str(line)
                .with_context(|| format!("parse {}:{}", path.display(), line_index + 1))?;
            *event_counts.entry(event.event_type.clone()).or_default() += 1;
            if event.tx_error {
                continue;
            }
            let (Some(user), Some(mint)) = (event.user, event.mint) else {
                continue;
            };
            if pb_symbol(&mint).is_none() {
                continue;
            }
            let amount = event.amount.as_deref().and_then(parse_u128_option);

            match event.event_type.as_str() {
                "mint_to" | "mint_to_checked" if collect_holder_stats => {
                    let stats = holders.entry((user, mint)).or_default();
                    stats.touch(event.slot);
                    stats.deposits += 1;
                    stats.deposited_raw += amount.unwrap_or(0);
                }
                "burn" | "burn_checked" => {
                    if collect_holder_stats {
                        let stats = holders.entry((user.clone(), mint.clone())).or_default();
                        stats.touch(event.slot);
                        stats.withdrawals += 1;
                        stats.withdrawn_raw += amount.unwrap_or(0);
                    }
                    let key = (
                        event.signature.clone(),
                        user.clone(),
                        mint.clone(),
                        amount.unwrap_or(0),
                    );
                    if seen_burns.insert(key) {
                        burns.push(Burn {
                            burn_id: 0,
                            user,
                            slot: event.slot,
                            signature: event.signature,
                            mint,
                            amount_raw: amount,
                            block_time: event.block_time,
                            tx_index: event.tx_index,
                        });
                    }
                }
                _ => {}
            }
        }
        progress.done();
    }

    burns.sort_by(|a, b| (a.slot, &a.signature, &a.user).cmp(&(b.slot, &b.signature, &b.user)));
    for (index, burn) in burns.iter_mut().enumerate() {
        burn.burn_id = index + 1;
    }
    Ok((burns, holders, event_counts))
}

fn epoch_dirs(root: &Path, completed_summary: Option<&Path>) -> Result<Vec<PathBuf>> {
    let completed_epochs = completed_summary.map(read_completed_epochs).transpose()?;
    let mut out = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let entry = entry?;
        if !entry
            .metadata()
            .with_context(|| format!("stat {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(epoch) = name
            .strip_prefix("epoch-")
            .and_then(|epoch| epoch.parse::<u64>().ok())
        else {
            continue;
        };
        if completed_epochs
            .as_ref()
            .is_some_and(|epochs| !epochs.contains(&epoch))
        {
            continue;
        }
        out.push(entry.path());
    }
    out.sort_by_key(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("epoch-"))
            .and_then(|epoch| epoch.parse::<u64>().ok())
            .unwrap_or(0)
    });
    Ok(out)
}

fn read_completed_epochs(path: &Path) -> Result<HashSet<u64>> {
    let mut epochs = HashSet::new();
    read_tsv_rows(path, |row| {
        if row.get("car") == "MISSING" {
            return Ok(());
        }
        if parse_u64(row.get("dumped_txs")) == 0 {
            return Ok(());
        }
        if let Ok(epoch) = row.get("epoch").parse::<u64>() {
            epochs.insert(epoch);
        }
        Ok(())
    })
    .with_context(|| format!("read completed epoch summary {}", path.display()))?;
    Ok(epochs)
}

impl WorkManifest {
    fn from_config(
        config: &PiggyAnalyzeConfig,
        epoch_dirs: &[PathBuf],
        windows: &[String],
        max_flows_per_burn: usize,
    ) -> Result<Self> {
        Ok(Self {
            schema_version: WORK_SCHEMA_VERSION,
            dump_dir: stable_path_text(&config.dump_dir),
            epoch_dirs: epoch_dirs
                .iter()
                .map(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect(),
            event_files: config
                .event_paths
                .iter()
                .map(|path| file_fingerprint(path))
                .collect::<Result<Vec<_>>>()?,
            completed_summary: config
                .completed_summary
                .as_deref()
                .map(file_fingerprint)
                .transpose()?,
            program_label_file: config
                .program_label_file
                .as_deref()
                .map(file_fingerprint)
                .transpose()?,
            windows: windows.to_vec(),
            max_flows_per_burn,
            pb_mints: PB_MINTS
                .iter()
                .map(|(mint, symbol)| format!("{symbol}:{mint}"))
                .collect(),
            piggy_programs: PIGGY_PROGRAMS
                .iter()
                .map(|program| (*program).to_string())
                .collect(),
            default_program_labels: DEFAULT_PROGRAM_LABELS
                .iter()
                .map(|(program, protocol, class)| format!("{program}:{protocol}:{class}"))
                .collect(),
        })
    }
}

fn validate_or_write_work_manifest(work_dir: &Path, expected: &WorkManifest) -> Result<()> {
    let path = work_dir.join("manifest.json");
    if path.exists() {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let actual: WorkManifest =
            serde_json::from_reader(file).with_context(|| format!("parse {}", path.display()))?;
        if actual != *expected {
            bail!(
                "existing piggy-analyze work manifest does not match this run; use a fresh --out-dir or remove {} before retrying",
                work_dir.display()
            );
        }
        return Ok(());
    }

    if work_dir_has_checkpoints(work_dir)? {
        bail!(
            "existing piggy-analyze checkpoints under {} have no manifest; use a fresh --out-dir or remove that _work directory",
            work_dir.display()
        );
    }

    let mut writer =
        BufWriter::new(File::create(&path).with_context(|| format!("create {}", path.display()))?);
    serde_json::to_writer_pretty(&mut writer, expected)
        .with_context(|| format!("write {}", path.display()))?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn work_dir_has_checkpoints(work_dir: &Path) -> Result<bool> {
    for subdir in ["flow-candidates", "annotated"] {
        let path = work_dir.join(subdir);
        if !path.exists() {
            continue;
        }
        for entry in fs::read_dir(&path).with_context(|| format!("read {}", path.display()))? {
            let entry = entry?;
            if entry
                .metadata()
                .with_context(|| format!("stat {}", entry.path().display()))?
                .is_file()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn file_fingerprint(path: &Path) -> Result<FileFingerprint> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    let modified_secs = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    Ok(FileFingerprint {
        path: stable_path_text(path),
        len: metadata.len(),
        modified_secs,
    })
}

fn stable_path_text(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn epoch_id(epoch_dir: &Path) -> String {
    epoch_dir
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("epoch-"))
        .unwrap_or("")
        .to_string()
}

fn load_candidate_counts_file(
    path: &Path,
    flows_per_burn: &mut HashMap<usize, usize>,
) -> Result<usize> {
    let mut rows = 0usize;
    read_tsv_rows(path, |row| {
        let burn_id = parse_u64(row.get("burn_id")) as usize;
        if burn_id > 0 {
            *flows_per_burn.entry(burn_id).or_default() += 1;
        }
        rows += 1;
        Ok(())
    })
    .with_context(|| format!("load candidate counts {}", path.display()))?;
    Ok(rows)
}

fn write_flow_candidate(
    writer: &mut impl Write,
    burn: &Burn,
    row: &TsvRow<'_>,
    epoch: &str,
) -> Result<()> {
    writeln!(
        writer,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        burn.burn_id,
        burn.signature,
        burn.user,
        burn.slot,
        burn.tx_index
            .map(|value| value.to_string())
            .unwrap_or_default(),
        burn.mint,
        option_u128_text(burn.amount_raw),
        row.get("signature"),
        row.get("slot"),
        row.get("tx_index"),
        epoch,
        row.get("size"),
        row.get("compute_units_consumed"),
        row.get("cost_units"),
        row.get("tx_error"),
    )?;
    Ok(())
}

fn read_flow_candidates(path: &Path) -> Result<Vec<FlowTx>> {
    let mut txs = Vec::new();
    read_tsv_rows_progress(path, "flow candidate", |row| {
        txs.push(FlowTx {
            burn: Burn {
                burn_id: parse_u64(row.get("burn_id")) as usize,
                user: row.get("user").to_string(),
                slot: parse_u64(row.get("withdraw_slot")),
                signature: row.get("withdraw_signature").to_string(),
                mint: row.get("pb_mint").to_string(),
                amount_raw: parse_u128_option(row.get("pb_amount_raw")),
                block_time: None,
                tx_index: parse_optional_u64(row.get("withdraw_tx_index")),
            },
            signature: row.get("signature").to_string(),
            slot: parse_u64(row.get("slot")),
            tx_index: row.get("tx_index").to_string(),
            size: parse_u64(row.get("tx_size")),
            tx_error: row.get("tx_error").to_string(),
            compute_units: parse_u64(row.get("compute_units_consumed")),
            cost_units: parse_u64(row.get("cost_units")),
            epoch: row.get("epoch").to_string(),
            programs: HashSet::new(),
            mints: HashSet::new(),
        });
        Ok(())
    })?;
    Ok(txs)
}

fn write_annotated_flow(
    writer: &mut impl Write,
    tx: &FlowTx,
    labels: &HashMap<String, (String, String)>,
) -> Result<()> {
    let (classification, protocol) = classify_flow(tx, labels);
    writeln!(
        writer,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        tx.burn.burn_id,
        tx.burn.signature,
        tx.burn.user,
        tx.burn.slot,
        tx.burn
            .tx_index
            .map(|value| value.to_string())
            .unwrap_or_default(),
        tx.burn.mint,
        option_u128_text(tx.burn.amount_raw),
        tx.signature,
        tx.slot,
        tx.slot.saturating_sub(tx.burn.slot),
        tx.tx_index,
        tx.epoch,
        classification,
        protocol,
        sorted_join(&tx.programs),
        sorted_join(&tx.mints),
        tx.size,
        optional_nonzero(tx.compute_units),
        optional_nonzero(tx.cost_units),
        tx.tx_error,
    )?;
    Ok(())
}

fn count_tsv_data_rows(path: &Path) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(0);
    }
    let mut rows = 0usize;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if !line.trim_end().is_empty() {
            rows += 1;
        }
    }
    Ok(rows)
}

fn read_user_index_top_users(
    path: &Path,
    active_users: &mut HashMap<String, UserActivity>,
) -> Result<()> {
    read_tsv_rows_progress(path, "user index top-users", |row| {
        let slot = parse_u64(row.get("slot"));
        active_users
            .entry(row.get("user").to_string())
            .or_default()
            .add(
                slot,
                parse_u64(row.get("size")),
                parse_u64(row.get("compute_units_consumed")),
                parse_u64(row.get("cost_units")),
            );
        Ok(())
    })
}

fn read_user_index_rows_progress(
    path: &Path,
    burns_by_user: &HashMap<String, Vec<Burn>>,
    collect_top_users: bool,
    mut f: impl FnMut(TsvRow<'_>) -> Result<()>,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut header_line = String::new();
    if reader.read_line(&mut header_line)? == 0 {
        return Ok(());
    }
    let header_names = header_line.trim_end().split('\t').collect::<Vec<_>>();
    let header = header_names
        .iter()
        .enumerate()
        .map(|(index, name)| ((*name).to_string(), index))
        .collect::<HashMap<_, _>>();
    let user_is_first = header_names.first() == Some(&"user");

    let mut line = String::new();
    let mut rows = 0usize;
    let mut progress = RowProgress::new(path, "user index");
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }
        rows += 1;
        progress.maybe_print(rows);

        if user_is_first && !collect_top_users {
            let user = line.split_once('\t').map_or(line, |(user, _)| user);
            if !burns_by_user.contains_key(user) {
                continue;
            }
        }

        let fields = line.split('\t').collect::<Vec<_>>();
        if !collect_top_users && !user_is_first {
            let user = header
                .get("user")
                .and_then(|index| fields.get(*index))
                .copied()
                .unwrap_or("");
            if !burns_by_user.contains_key(user) {
                continue;
            }
        }
        f(TsvRow {
            fields: &fields,
            header: &header,
        })?;
    }
    progress.done();
    Ok(())
}

fn read_tsv_rows(path: &Path, mut f: impl FnMut(TsvRow<'_>) -> Result<()>) -> Result<()> {
    read_tsv_rows_inner(path, None, &mut f)
}

fn read_tsv_rows_progress(
    path: &Path,
    label: &'static str,
    mut f: impl FnMut(TsvRow<'_>) -> Result<()>,
) -> Result<()> {
    read_tsv_rows_inner(path, Some(label), &mut f)
}

fn read_tsv_rows_inner(
    path: &Path,
    label: Option<&'static str>,
    f: &mut impl FnMut(TsvRow<'_>) -> Result<()>,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut header_line = String::new();
    if reader.read_line(&mut header_line)? == 0 {
        return Ok(());
    }
    let header = header_line
        .trim_end()
        .split('\t')
        .enumerate()
        .map(|(index, name)| (name.to_string(), index))
        .collect::<HashMap<_, _>>();

    let mut line = String::new();
    let mut rows = 0usize;
    let mut progress = label.map(|label| RowProgress::new(path, label));
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }
        rows += 1;
        if let Some(progress) = progress.as_mut() {
            progress.maybe_print(rows);
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        f(TsvRow {
            fields: &fields,
            header: &header,
        })?;
    }
    if let Some(progress) = progress.as_mut() {
        progress.done();
    }
    Ok(())
}

struct RowProgress<'a> {
    path: &'a Path,
    label: &'static str,
    started_at: Instant,
    last_print_at: Instant,
    last_printed_rows: usize,
    rows: usize,
}

impl<'a> RowProgress<'a> {
    fn new(path: &'a Path, label: &'static str) -> Self {
        let now = Instant::now();
        Self {
            path,
            label,
            started_at: now,
            last_print_at: now,
            last_printed_rows: 0,
            rows: 0,
        }
    }

    fn maybe_print(&mut self, rows: usize) {
        self.rows = rows;
        let now = Instant::now();
        let row_delta = rows.saturating_sub(self.last_printed_rows);
        if row_delta >= PROGRESS_ROW_INTERVAL
            || now.duration_since(self.last_print_at) >= PROGRESS_INTERVAL
        {
            self.print(false);
        }
    }

    fn done(&mut self) {
        self.print(true);
    }

    fn print(&mut self, final_line: bool) {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let rows_per_sec = if elapsed > 0.0 {
            self.rows as f64 / elapsed
        } else {
            0.0
        };
        eprintln!(
            "piggy-analyze: {}{} rows={} elapsed_s={:.0} rows_s={:.0} file={}",
            if final_line { "done " } else { "" },
            self.label,
            self.rows,
            elapsed,
            rows_per_sec,
            self.path.display()
        );
        self.last_print_at = Instant::now();
        self.last_printed_rows = self.rows;
    }
}

fn load_program_labels(path: Option<&Path>) -> Result<HashMap<String, (String, String)>> {
    let mut labels = DEFAULT_PROGRAM_LABELS
        .iter()
        .map(|(program, protocol, class)| {
            (
                (*program).to_string(),
                ((*protocol).to_string(), (*class).to_string()),
            )
        })
        .collect::<HashMap<_, _>>();
    let Some(path) = path else {
        return Ok(labels);
    };

    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for (line_index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.first() == Some(&"program") {
            continue;
        }
        if fields.len() < 3 {
            bail!(
                "{}:{} expected program<TAB>protocol_guess<TAB>classification",
                path.display(),
                line_index + 1
            );
        }
        labels.insert(
            fields[0].to_string(),
            (fields[1].to_string(), fields[2].to_string()),
        );
    }
    Ok(labels)
}

fn classify_flow(tx: &FlowTx, labels: &HashMap<String, (String, String)>) -> (String, String) {
    if tx
        .programs
        .iter()
        .any(|program| PIGGY_PROGRAMS.contains(&program.as_str()))
        || tx.mints.iter().any(|mint| pb_symbol(mint).is_some())
    {
        return ("redeposit_piggy".to_string(), "PiggyBank".to_string());
    }

    let mut guesses = tx
        .programs
        .iter()
        .filter_map(|program| labels.get(program))
        .cloned()
        .collect::<Vec<_>>();
    if !guesses.is_empty() {
        guesses.sort_by_key(|(_, class)| classification_priority(class));
        let mut protocols = guesses
            .iter()
            .map(|(protocol, _)| protocol.clone())
            .collect::<Vec<_>>();
        protocols.sort();
        protocols.dedup();
        return (guesses[0].1.clone(), protocols.join(","));
    }

    let mut non_ignored = tx
        .programs
        .iter()
        .filter(|program| !IGNORED_PROGRAMS.contains(&program.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    non_ignored.sort();
    if !non_ignored.is_empty() {
        non_ignored.truncate(8);
        return ("unknown_protocol".to_string(), non_ignored.join(","));
    }

    if !tx.mints.is_empty() {
        return ("unknown_transfer".to_string(), String::new());
    }

    ("other_activity".to_string(), String::new())
}

fn classification_priority(class: &str) -> u8 {
    match class {
        "bridge" => 0,
        "swap" => 1,
        "deposit_other_defi" => 2,
        "cex_transfer" => 3,
        "unknown_transfer" => 4,
        _ => 99,
    }
}

fn parse_duration_slots(value: &str) -> Result<u64> {
    let Some(unit) = value.chars().last() else {
        bail!("empty window");
    };
    let amount = value[..value.len() - unit.len_utf8()]
        .parse::<f64>()
        .with_context(|| format!("parse window {value}"))?;
    let seconds = match unit.to_ascii_lowercase() {
        's' => amount,
        'm' => amount * 60.0,
        'h' => amount * 60.0 * 60.0,
        'd' => amount * 24.0 * 60.0 * 60.0,
        _ => bail!("unsupported window {value}; use s/m/h/d"),
    };
    Ok((seconds * SLOTS_PER_SECOND).ceil() as u64)
}

fn parse_u64(value: &str) -> u64 {
    value.parse().unwrap_or(0)
}

fn parse_optional_u64(value: &str) -> Option<u64> {
    if value.is_empty() {
        None
    } else {
        value.parse().ok()
    }
}

fn parse_u128_option(value: &str) -> Option<u128> {
    value.parse().ok()
}

fn flow_sort_key(slot_delta: u64, tx_index: &str, signature: &str) -> (u64, u64, String) {
    (
        slot_delta,
        parse_optional_u64(tx_index).unwrap_or(u64::MAX),
        signature.to_string(),
    )
}

fn is_after_burn(slot: u64, tx_index: Option<u64>, burn: &Burn) -> bool {
    if slot > burn.slot {
        return true;
    }
    if slot < burn.slot {
        return false;
    }
    match (tx_index, burn.tx_index) {
        (Some(tx_index), Some(burn_tx_index)) => tx_index > burn_tx_index,
        _ => false,
    }
}

fn option_u128_text(value: Option<u128>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn optional_nonzero(value: u64) -> String {
    if value == 0 {
        String::new()
    } else {
        value.to_string()
    }
}

fn pb_symbol(mint: &str) -> Option<&'static str> {
    PB_MINTS
        .iter()
        .find_map(|(candidate, symbol)| (*candidate == mint).then_some(*symbol))
}

fn sorted_join(values: &HashSet<String>) -> String {
    let mut values = values.iter().cloned().collect::<Vec<_>>();
    values.sort();
    values.join(",")
}

fn write_rows<I>(path: &Path, header: &[&str], rows: I) -> Result<()>
where
    I: IntoIterator<Item = Vec<String>>,
{
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "{}", header.join("\t"))?;
    for row in rows {
        writeln!(writer, "{}", row.join("\t"))?;
    }
    Ok(())
}

fn write_counter_rows<I>(path: &Path, header: &[&str], rows: I) -> Result<()>
where
    I: IntoIterator<Item = Vec<String>>,
{
    write_rows(path, header, rows)
}

fn write_holder_stats(path: &Path, holders: &HashMap<(String, String), HolderStats>) -> Result<()> {
    let mut rows = holders
        .iter()
        .map(|((user, mint), stats)| {
            (
                mint.clone(),
                stats.deposited_raw.saturating_sub(stats.withdrawn_raw),
                user.clone(),
                vec![
                    user.clone(),
                    mint.clone(),
                    pb_symbol(mint).unwrap_or("").to_string(),
                    stats.deposits.to_string(),
                    stats.withdrawals.to_string(),
                    stats.deposited_raw.to_string(),
                    stats.withdrawn_raw.to_string(),
                    stats
                        .deposited_raw
                        .saturating_sub(stats.withdrawn_raw)
                        .to_string(),
                    stats
                        .first_slot
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    stats
                        .last_slot
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                ],
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        (&a.0, std::cmp::Reverse(a.1), &a.2).cmp(&(&b.0, std::cmp::Reverse(b.1), &b.2))
    });
    write_rows(
        path,
        &[
            "user",
            "mint",
            "symbol",
            "deposits",
            "withdrawals",
            "deposited_raw",
            "withdrawn_raw",
            "net_raw",
            "first_slot",
            "last_slot",
        ],
        rows.into_iter().map(|(_, _, _, row)| row),
    )
}

fn write_top_users(
    path: &Path,
    users: &HashMap<String, UserActivity>,
    max_top: usize,
) -> Result<()> {
    let mut rows = users.iter().collect::<Vec<_>>();
    rows.sort_by(|(a_user, a), (b_user, b)| b.txs.cmp(&a.txs).then_with(|| a_user.cmp(b_user)));
    write_rows(
        path,
        &[
            "user",
            "txs",
            "bytes",
            "compute_units_consumed",
            "cost_units",
            "first_slot",
            "last_slot",
        ],
        rows.into_iter().take(max_top).map(|(user, stats)| {
            vec![
                user.clone(),
                stats.txs.to_string(),
                stats.bytes.to_string(),
                optional_nonzero(stats.compute_units),
                optional_nonzero(stats.cost_units),
                stats
                    .first_slot
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                stats
                    .last_slot
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ]
        }),
    )
}

fn write_token_summary(path: &Path, burns: &[Burn]) -> Result<()> {
    let token_rows = token_burn_summary(burns);
    write_rows(
        path,
        &[
            "pb_mint",
            "symbol",
            "withdraw_burns",
            "withdraw_users",
            "amount_raw_sum",
        ],
        token_rows
            .into_iter()
            .map(|(mint, (withdraws, users, amount_sum))| {
                vec![
                    mint.clone(),
                    pb_symbol(&mint).unwrap_or("").to_string(),
                    withdraws.to_string(),
                    users.len().to_string(),
                    amount_sum.to_string(),
                ]
            }),
    )
}

fn token_burn_summary(burns: &[Burn]) -> BTreeMap<String, (usize, HashSet<String>, u128)> {
    let mut token_rows: BTreeMap<String, (usize, HashSet<String>, u128)> = BTreeMap::new();
    for burn in burns {
        let (withdraws, users, amount_sum) = token_rows.entry(burn.mint.clone()).or_default();
        *withdraws += 1;
        users.insert(burn.user.clone());
        *amount_sum += burn.amount_raw.unwrap_or(0);
    }
    token_rows
}

fn write_ranked_counter(
    path: &Path,
    header: &[&str],
    counter: &HashMap<String, usize>,
    max_top: usize,
    extra: impl Fn(&str) -> Option<String>,
) -> Result<()> {
    let mut rows = counter.iter().collect::<Vec<_>>();
    rows.sort_by(|(a_key, a), (b_key, b)| b.cmp(a).then_with(|| a_key.cmp(b_key)));
    write_rows(
        path,
        header,
        rows.into_iter().take(max_top).map(|(key, count)| {
            let mut row = vec![key.clone()];
            if let Some(value) = extra(key) {
                row.push(value);
            }
            row.push(count.to_string());
            row
        }),
    )
}

fn write_ranked_pair_counter(
    path: &Path,
    header: &[&str],
    counter: &HashMap<(String, String), usize>,
    max_top: usize,
    extra: impl Fn(&str) -> Option<String>,
) -> Result<()> {
    let mut rows = counter
        .iter()
        .map(|((mint, value), count)| (mint.clone(), value.clone(), *count))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.1.cmp(&b.1))
    });
    let mut seen_by_mint: HashMap<String, usize> = HashMap::new();
    write_rows(
        path,
        header,
        rows.into_iter().filter_map(|(mint, value, count)| {
            let seen = seen_by_mint.entry(mint.clone()).or_default();
            if *seen >= max_top {
                return None;
            }
            *seen += 1;
            let mut row = vec![
                mint.clone(),
                pb_symbol(&mint).unwrap_or("").to_string(),
                value.clone(),
            ];
            if let Some(extra_value) = extra(&value) {
                row.push(extra_value);
            }
            row.push(count.to_string());
            Some(row)
        }),
    )
}

fn write_summary(
    config: &PiggyAnalyzeConfig,
    event_counts: &BTreeMap<String, usize>,
    outcome_counts: &BTreeMap<(String, String), usize>,
    outcome_counts_by_token: &BTreeMap<(String, String, String), usize>,
    tx_stats: &[(String, String)],
    burns_by_token_source: &[Burn],
    burns: usize,
    burn_users: usize,
    epoch_dirs: usize,
    signature_count: usize,
    active_users: usize,
) -> Result<()> {
    let path = config.out_dir.join("summary.md");
    let mut writer =
        BufWriter::new(File::create(&path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "# Piggy Flow Summary")?;
    writeln!(writer)?;
    writeln!(writer, "- dump_dir: `{}`", config.dump_dir.display())?;
    writeln!(
        writer,
        "- events: {}",
        config
            .event_paths
            .iter()
            .map(|path| format!("`{}`", path.display()))
            .collect::<Vec<_>>()
            .join(", ")
    )?;
    writeln!(writer, "- epoch dirs read: {epoch_dirs}")?;
    writeln!(
        writer,
        "- pb mints: {}",
        PB_MINTS
            .iter()
            .map(|(mint, symbol)| format!("{symbol}={mint}"))
            .collect::<Vec<_>>()
            .join(", ")
    )?;
    writeln!(writer, "- raw scan event counts: {event_counts:?}")?;
    writeln!(writer, "- pb withdraw burns: {burns}")?;
    writeln!(writer, "- pb withdraw users: {burn_users}")?;
    writeln!(writer, "- dumped tx index rows: {signature_count}")?;
    writeln!(writer, "- users in dump index: {active_users}")?;
    writeln!(
        writer,
        "- top users aggregation: {}",
        if config.collect_top_users {
            "enabled"
        } else {
            "disabled"
        }
    )?;
    writeln!(
        writer,
        "- max flows per burn: {}",
        if config.max_flows_per_burn == 0 {
            "unlimited".to_string()
        } else {
            config.max_flows_per_burn.to_string()
        }
    )?;
    writeln!(
        writer,
        "- holder stats aggregation: {}",
        if config.collect_holder_stats {
            "enabled"
        } else {
            "disabled"
        }
    )?;
    writeln!(writer)?;

    writeln!(writer, "## Withdraw Outcomes")?;
    writeln!(writer)?;
    writeln!(writer, "| window | classification | withdraws |")?;
    writeln!(writer, "|---|---:|---:|")?;
    for ((window, classification), count) in outcome_counts {
        writeln!(writer, "| {window} | {classification} | {count} |")?;
    }

    writeln!(writer)?;
    writeln!(writer, "## Token Breakdown")?;
    writeln!(writer)?;
    writeln!(
        writer,
        "| token | withdraw burns | withdraw users | amount raw sum |"
    )?;
    writeln!(writer, "|---|---:|---:|---:|")?;
    for (mint, (withdraws, users, amount_sum)) in token_burn_summary(burns_by_token_source) {
        writeln!(
            writer,
            "| {} | {} | {} | {} |",
            pb_symbol(&mint).map_or(mint.as_str(), |symbol| symbol),
            withdraws,
            users.len(),
            amount_sum
        )?;
    }

    writeln!(writer)?;
    writeln!(writer, "| token | window | classification | withdraws |")?;
    writeln!(writer, "|---|---|---:|---:|")?;
    for ((mint, window, classification), count) in outcome_counts_by_token {
        writeln!(
            writer,
            "| {} | {} | {} | {} |",
            pb_symbol(mint).map_or(mint.as_str(), |symbol| symbol),
            window,
            classification,
            count
        )?;
    }

    writeln!(writer)?;
    writeln!(writer, "## Transaction Shape")?;
    writeln!(writer)?;
    writeln!(writer, "| metric | value |")?;
    writeln!(writer, "|---|---:|")?;
    for (metric, value) in tx_stats {
        writeln!(writer, "| {metric} | {value} |")?;
    }

    writeln!(writer)?;
    writeln!(writer, "## Notes")?;
    writeln!(writer)?;
    writeln!(
        writer,
        "- `wallet_idle` means no dumped tracked-user transaction was found in that window."
    )?;
    writeln!(
        writer,
        "- CEX detection needs an address-label file or decoded destination labels; this index-only pass cannot prove CEX deposits by itself."
    )?;
    writeln!(
        writer,
        "- Holder net is mint/burn net inside the provided piggy-scan event files, not necessarily lifetime balance if the scan is partial."
    )?;
    writeln!(
        writer,
        "- Classification is program/mint based; inspect `post_withdraw_flows.tsv` for raw programs and mints."
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parses_duration_windows_to_slots() {
        assert_eq!(parse_duration_slots("1h").unwrap(), 9_000);
        assert_eq!(parse_duration_slots("24h").unwrap(), 216_000);
        assert_eq!(parse_duration_slots("7d").unwrap(), 1_512_000);
    }

    #[test]
    fn classifies_piggy_mint_as_redeposit() {
        let mut tx = FlowTx {
            burn: Burn {
                burn_id: 1,
                user: "user".to_string(),
                slot: 1,
                signature: "burn".to_string(),
                mint: PB_MINTS[0].0.to_string(),
                amount_raw: None,
                block_time: None,
                tx_index: None,
            },
            signature: "sig".to_string(),
            slot: 2,
            tx_index: String::new(),
            size: 0,
            tx_error: "false".to_string(),
            compute_units: 0,
            cost_units: 0,
            epoch: "1".to_string(),
            programs: HashSet::new(),
            mints: HashSet::new(),
        };
        tx.mints.insert(PB_MINTS[0].0.to_string());
        let labels = load_program_labels(None).unwrap();
        assert_eq!(classify_flow(&tx, &labels).0, "redeposit_piggy");
    }

    #[test]
    fn analysis_filters_withdraws_to_pb_mints() {
        let temp = tempfile::tempdir().unwrap();
        let dump = temp.path().join("dump");
        let epoch = dump.join("epoch-1");
        fs::create_dir_all(&epoch).unwrap();
        fs::write(
            temp.path().join("events.jsonl"),
            format!(
                "{{\"slot\":10,\"block_time\":1,\"signature\":\"pb_burn\",\"tx_error\":false,\"tx_index\":1,\"user\":\"user1\",\"mint\":\"{}\",\"event_type\":\"burn\",\"amount\":\"100\"}}\n{{\"slot\":11,\"block_time\":1,\"signature\":\"other_burn\",\"tx_error\":false,\"tx_index\":1,\"user\":\"user2\",\"mint\":\"So11111111111111111111111111111111111111112\",\"event_type\":\"burn\",\"amount\":\"100\"}}\n",
                PB_MINTS[0].0
            ),
        )
        .unwrap();
        fs::write(
            epoch.join("signature.index.tsv"),
            "signature\tslot\ttx_index\tsize\ttx_error\tcompute_units_consumed\tcost_units\nflow1\t12\t1\t120\tfalse\t500\t600\nflow2\t13\t1\t121\tfalse\t501\t601\n",
        )
        .unwrap();
        fs::write(
            epoch.join("user.index.tsv"),
            "user\tsignature\tslot\ttx_index\tsize\ttx_error\tcompute_units_consumed\tcost_units\nuser1\tflow1\t12\t1\t120\tfalse\t500\t600\nuser2\tflow2\t13\t1\t121\tfalse\t501\t601\n",
        )
        .unwrap();
        fs::write(
            epoch.join("program.index.tsv"),
            "program\tsignature\tslot\ttx_index\nJUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4\tflow1\t12\t1\n",
        )
        .unwrap();
        fs::write(
            epoch.join("mint.index.tsv"),
            "mint\tsignature\tslot\ttx_index\nSo11111111111111111111111111111111111111112\tflow1\t12\t1\n",
        )
        .unwrap();

        let out_dir = temp.path().join("out");
        let summary = analyze_piggy_flows(PiggyAnalyzeConfig {
            dump_dir: dump,
            event_paths: vec![temp.path().join("events.jsonl")],
            out_dir: out_dir.clone(),
            windows: vec!["1h".to_string()],
            completed_summary: None,
            program_label_file: None,
            max_top: 50,
            max_flows_per_burn: 1,
            collect_top_users: false,
            collect_holder_stats: false,
        })
        .unwrap();

        assert_eq!(summary.withdraw_burns, 1);
        let withdraws = fs::read_to_string(out_dir.join("withdraws.tsv")).unwrap();
        assert!(withdraws.contains("pb_burn"));
        assert!(!withdraws.contains("other_burn"));
    }

    #[test]
    fn analysis_refuses_incompatible_checkpoint_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let dump = temp.path().join("dump");
        let epoch = dump.join("epoch-1");
        fs::create_dir_all(&epoch).unwrap();
        fs::write(
            temp.path().join("events.jsonl"),
            format!(
                "{{\"slot\":10,\"block_time\":1,\"signature\":\"pb_burn\",\"tx_error\":false,\"tx_index\":1,\"user\":\"user1\",\"mint\":\"{}\",\"event_type\":\"burn\",\"amount\":\"100\"}}\n",
                PB_MINTS[0].0
            ),
        )
        .unwrap();
        fs::write(
            epoch.join("signature.index.tsv"),
            "signature\tslot\ttx_index\tsize\ttx_error\tcompute_units_consumed\tcost_units\nflow1\t12\t1\t120\tfalse\t500\t600\n",
        )
        .unwrap();
        fs::write(
            epoch.join("user.index.tsv"),
            "user\tsignature\tslot\ttx_index\tsize\ttx_error\tcompute_units_consumed\tcost_units\nuser1\tflow1\t12\t1\t120\tfalse\t500\t600\n",
        )
        .unwrap();
        fs::write(
            epoch.join("program.index.tsv"),
            "program\tsignature\tslot\ttx_index\nJUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4\tflow1\t12\t1\n",
        )
        .unwrap();
        fs::write(
            epoch.join("mint.index.tsv"),
            "mint\tsignature\tslot\ttx_index\nSo11111111111111111111111111111111111111112\tflow1\t12\t1\n",
        )
        .unwrap();

        let out_dir = temp.path().join("out");
        let mut config = PiggyAnalyzeConfig {
            dump_dir: dump,
            event_paths: vec![temp.path().join("events.jsonl")],
            out_dir,
            windows: vec!["1h".to_string()],
            completed_summary: None,
            program_label_file: None,
            max_top: 50,
            max_flows_per_burn: 1,
            collect_top_users: false,
            collect_holder_stats: false,
        };
        analyze_piggy_flows(config.clone()).unwrap();
        config.max_flows_per_burn = 2;
        let err = analyze_piggy_flows(config).unwrap_err().to_string();
        assert!(err.contains("work manifest does not match"));
    }
}

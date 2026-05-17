use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
};

use anyhow::{Context, Result};
use blockzilla_format::program_logs::{
    official_program_log_has_known_binary_form, program_log_has_known_binary_form, system_program,
};
use blockzilla_log_parser::{
    ParsedLogLine, parse_custom_program_error_reason, parse_line, parse_program_log_error_payload,
};
use clap::Parser;
use data_encoding::BASE64;
use of_car_reader::{
    CarBlockReader,
    car_block_group::{CarBlockGroup, TxMetadataVisit},
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::TransactionStatusMetaVisitor,
};
use solana_pubkey::Pubkey;
use tracing::{Level, info};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "car-log-audit",
    about = "Decode CAR transaction metadata and summarize log parser misses"
)]
struct Args {
    /// Input CAR file path (.car or .car.zst), or a directory containing epoch CAR files
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Report directory for directory scans
    #[arg(long, default_value = "car-log-audit-report")]
    output_dir: PathBuf,

    /// Recurse into subdirectories when INPUT is a directory
    #[arg(long)]
    recursive: bool,

    /// Number of epoch files to scan concurrently in directory mode
    #[arg(long, default_value_t = 1)]
    jobs: usize,

    /// Also build merged global report files in directory mode
    #[arg(long)]
    global_report: bool,

    /// Stop after N blocks
    #[arg(long)]
    limit_blocks: Option<u64>,

    /// Print progress every N blocks (0 disables)
    #[arg(long, default_value_t = 10_000)]
    progress_every: u64,

    /// Number of rows to print per top-N section
    #[arg(long, default_value_t = 50)]
    top: usize,

    /// Maximum unique strings retained per counter before dropping new keys
    #[arg(long, default_value_t = 200_000)]
    max_unique: usize,

    /// Reader buffer size in bytes
    #[arg(long, default_value_t = 128 << 20)]
    buf_size: usize,

    /// Stream exact unparsed strings as kind<TAB>escaped_string
    #[arg(long)]
    dump_unparsed: Option<PathBuf>,

    /// Stream exact Program log payloads as kind<TAB>program<TAB>escaped_payload
    #[arg(long)]
    dump_program_payloads: Option<PathBuf>,
}

struct AuditReport {
    max_unique: usize,
    blocks: u64,
    txs: u64,
    txs_without_metadata: u64,
    metadata: u64,
    metadata_visitor: u64,
    metadata_legacy_decoded: u64,
    metadata_decode_errors: u64,
    log_messages_none: u64,
    log_lines: u64,
    top_level_unparsed: u64,
    unparsed_program: u64,
    invalid_pubkeys: u64,
    invalid_base64: u64,
    streamed_unparsed: u64,
    streamed_program_payloads: u64,
    dropped_logs_due_to_decode_error: u64,
    first_errors: Vec<String>,
    kinds: BTreeMap<&'static str, u64>,
    unparsed_exact: Counter,
    unparsed_shapes: Counter,
    invalid_pubkey_exact: Counter,
    invalid_base64_exact: Counter,
    program_payload_exact: Counter,
    program_payload_shapes: Counter,
    program_payload_programs: Counter,
    unknown_program_payload_shapes: Counter,
    program_id_payload_shapes: Counter,
    failure_reason_shapes: Counter,
}

impl AuditReport {
    fn new(max_unique: usize) -> Self {
        Self {
            max_unique,
            blocks: 0,
            txs: 0,
            txs_without_metadata: 0,
            metadata: 0,
            metadata_visitor: 0,
            metadata_legacy_decoded: 0,
            metadata_decode_errors: 0,
            log_messages_none: 0,
            log_lines: 0,
            top_level_unparsed: 0,
            unparsed_program: 0,
            invalid_pubkeys: 0,
            invalid_base64: 0,
            streamed_unparsed: 0,
            streamed_program_payloads: 0,
            dropped_logs_due_to_decode_error: 0,
            first_errors: Vec::with_capacity(16),
            kinds: BTreeMap::new(),
            unparsed_exact: Counter::new(max_unique),
            unparsed_shapes: Counter::new(max_unique),
            invalid_pubkey_exact: Counter::new(max_unique),
            invalid_base64_exact: Counter::new(max_unique),
            program_payload_exact: Counter::new(max_unique),
            program_payload_shapes: Counter::new(max_unique),
            program_payload_programs: Counter::new(max_unique),
            unknown_program_payload_shapes: Counter::new(max_unique),
            program_id_payload_shapes: Counter::new(max_unique),
            failure_reason_shapes: Counter::new(max_unique),
        }
    }

    fn add_error(&mut self, msg: String) {
        if self.first_errors.len() < 16 {
            self.first_errors.push(msg);
        }
    }

    fn print(&self, args: &Args, elapsed_secs: f64) {
        println!("input={}", args.input.display());
        println!(
            "elapsed={elapsed_secs:.2}s blocks={} txs={} metadata={} metadata_visitor={} metadata_legacy_decoded={} txs_without_metadata={} metadata_decode_errors={} log_messages_none={} log_lines={}",
            self.blocks,
            self.txs,
            self.metadata,
            self.metadata_visitor,
            self.metadata_legacy_decoded,
            self.txs_without_metadata,
            self.metadata_decode_errors,
            self.log_messages_none,
            self.log_lines
        );
        println!(
            "parser_misses top_level_unparsed={} unparsed_program={} invalid_pubkeys={} invalid_base64={} streamed_unparsed={} streamed_program_payloads={} dropped_logs_due_to_decode_error={}",
            self.top_level_unparsed,
            self.unparsed_program,
            self.invalid_pubkeys,
            self.invalid_base64,
            self.streamed_unparsed,
            self.streamed_program_payloads,
            self.dropped_logs_due_to_decode_error
        );
        println!("max_unique_per_counter={}", self.max_unique);

        print_kinds("parsed log line kinds", &self.kinds);
        self.unparsed_shapes
            .print_top("top normalized unparsed shapes", args.top);
        self.unparsed_exact
            .print_top("top exact unparsed strings", args.top);
        self.program_payload_shapes
            .print_top("top normalized Program log payload shapes", args.top);
        self.program_payload_programs
            .print_top("top Program log payload programs", args.top);
        self.unknown_program_payload_shapes
            .print_top("top unknown Program log payload shapes", args.top);
        self.program_id_payload_shapes
            .print_top("top normalized Program <id> log payload shapes", args.top);
        self.program_payload_exact
            .print_top("top exact Program log payloads", args.top);
        self.failure_reason_shapes
            .print_top("top normalized failure reasons", args.top);
        self.invalid_pubkey_exact
            .print_top("invalid pubkey references", args.top);
        self.invalid_base64_exact
            .print_top("invalid base64 payloads", args.top);

        if !self.first_errors.is_empty() {
            println!("first decode/audit errors:");
            for error in &self.first_errors {
                println!("  {error}");
            }
        }
    }

    fn merge_from(&mut self, other: &AuditReport) {
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.txs_without_metadata += other.txs_without_metadata;
        self.metadata += other.metadata;
        self.metadata_visitor += other.metadata_visitor;
        self.metadata_legacy_decoded += other.metadata_legacy_decoded;
        self.metadata_decode_errors += other.metadata_decode_errors;
        self.log_messages_none += other.log_messages_none;
        self.log_lines += other.log_lines;
        self.top_level_unparsed += other.top_level_unparsed;
        self.unparsed_program += other.unparsed_program;
        self.invalid_pubkeys += other.invalid_pubkeys;
        self.invalid_base64 += other.invalid_base64;
        self.streamed_unparsed += other.streamed_unparsed;
        self.streamed_program_payloads += other.streamed_program_payloads;
        self.dropped_logs_due_to_decode_error += other.dropped_logs_due_to_decode_error;

        for error in &other.first_errors {
            self.add_error(error.clone());
        }
        for (kind, count) in &other.kinds {
            *self.kinds.entry(*kind).or_default() += count;
        }

        self.unparsed_exact.merge(&other.unparsed_exact);
        self.unparsed_shapes.merge(&other.unparsed_shapes);
        self.invalid_pubkey_exact.merge(&other.invalid_pubkey_exact);
        self.invalid_base64_exact.merge(&other.invalid_base64_exact);
        self.program_payload_exact
            .merge(&other.program_payload_exact);
        self.program_payload_shapes
            .merge(&other.program_payload_shapes);
        self.program_payload_programs
            .merge(&other.program_payload_programs);
        self.unknown_program_payload_shapes
            .merge(&other.unknown_program_payload_shapes);
        self.program_id_payload_shapes
            .merge(&other.program_id_payload_shapes);
        self.failure_reason_shapes
            .merge(&other.failure_reason_shapes);
    }
}

struct Counter {
    counts: HashMap<String, u64>,
    max_unique: usize,
    dropped_new_keys: u64,
}

impl Counter {
    fn new(max_unique: usize) -> Self {
        Self {
            counts: HashMap::new(),
            max_unique,
            dropped_new_keys: 0,
        }
    }

    fn add(&mut self, value: &str) {
        if let Some(count) = self.counts.get_mut(value) {
            *count += 1;
            return;
        }
        if self.counts.len() >= self.max_unique {
            self.dropped_new_keys += 1;
            return;
        }
        self.counts.insert(value.to_owned(), 1);
    }

    fn add_count(&mut self, value: &str, count: u64) {
        if count == 0 {
            return;
        }
        if let Some(existing) = self.counts.get_mut(value) {
            *existing += count;
            return;
        }
        if self.counts.len() >= self.max_unique {
            self.dropped_new_keys += 1;
            return;
        }
        self.counts.insert(value.to_owned(), count);
    }

    fn merge(&mut self, other: &Counter) {
        for (value, count) in &other.counts {
            self.add_count(value, *count);
        }
        self.dropped_new_keys += other.dropped_new_keys;
    }

    fn sorted(&self) -> Vec<(&String, &u64)> {
        let mut items = self.counts.iter().collect::<Vec<_>>();
        items.sort_unstable_by(|(a_key, a_count), (b_key, b_count)| {
            b_count.cmp(a_count).then_with(|| a_key.cmp(b_key))
        });
        items
    }

    fn print_top(&self, title: &str, top: usize) {
        println!("{title}:");
        for (value, count) in self.sorted().into_iter().take(top) {
            println!("  {count:>12}  {value}");
        }
        if self.dropped_new_keys > 0 {
            println!("  dropped_new_unique_keys={}", self.dropped_new_keys);
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = Args::parse();
    let start = Instant::now();
    if args.input.is_dir() {
        scan_directory(&args)?;
    } else {
        let report = scan_input(&args)?;
        let elapsed = start.elapsed().as_secs_f64();
        report.print(&args, elapsed);
    }

    Ok(())
}

fn scan_directory(args: &Args) -> Result<()> {
    anyhow::ensure!(
        args.dump_unparsed.is_none() && args.dump_program_payloads.is_none(),
        "--dump-unparsed/--dump-program-payloads are single-file options; directory mode writes aggregated report TSVs"
    );

    let inputs = collect_car_inputs(&args.input, args.recursive)?;
    anyhow::ensure!(
        !inputs.is_empty(),
        "no .car or .car.zst files found in {}",
        args.input.display()
    );

    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create report dir {}", args.output_dir.display()))?;
    let epoch_dir = args.output_dir.join("epochs");
    fs::create_dir_all(&epoch_dir)
        .with_context(|| format!("create epoch report dir {}", epoch_dir.display()))?;

    let mut summary = BufWriter::new(File::create(args.output_dir.join("epochs.tsv"))?);
    write_tsv_record(
        &mut summary,
        &[
            "epoch",
            "input",
            "elapsed_s",
            "blocks",
            "txs",
            "metadata",
            "metadata_visitor",
            "metadata_legacy_decoded",
            "metadata_decode_errors",
            "log_lines",
            "top_level_unparsed",
            "unparsed_program",
            "unknown_program_payloads",
            "invalid_pubkeys",
            "invalid_base64",
        ],
    )?;

    let started = Instant::now();
    let mut global = args
        .global_report
        .then(|| AuditReport::new(args.max_unique));
    let input_count = inputs.len();
    let jobs = args.jobs.max(1).min(inputs.len());

    if jobs == 1 {
        for (index, input) in inputs.iter().cloned().enumerate() {
            let result = scan_epoch_input(args, &epoch_dir, inputs.len(), index, input)?;
            record_epoch_result(&mut summary, global.as_mut(), result)?;
        }
    } else {
        info!(
            jobs,
            epochs = inputs.len(),
            "car-log-audit directory scan starting concurrent workers"
        );

        let total_count = inputs.len();
        let queue = Arc::new(Mutex::new(
            inputs.into_iter().enumerate().collect::<VecDeque<_>>(),
        ));
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::with_capacity(jobs);

        for worker_id in 0..jobs {
            let queue = Arc::clone(&queue);
            let tx = tx.clone();
            let worker_args = args.clone();
            let worker_epoch_dir = epoch_dir.clone();
            let total = total_count;

            handles.push(thread::spawn(move || {
                loop {
                    let next = {
                        let mut queue = queue.lock().expect("epoch queue poisoned");
                        queue.pop_front()
                    };
                    let Some((index, input)) = next else {
                        break;
                    };
                    let result =
                        scan_epoch_input(&worker_args, &worker_epoch_dir, total, index, input)
                            .map_err(|error| format!("{error:#}"));
                    if tx.send((worker_id, result)).is_err() {
                        break;
                    }
                }
            }));
        }
        drop(tx);

        let mut errors = Vec::new();
        for (worker_id, result) in rx {
            match result {
                Ok(result) => record_epoch_result(&mut summary, global.as_mut(), result)?,
                Err(error) => {
                    errors.push(format!("worker {worker_id}: {error}"));
                }
            }
        }

        for handle in handles {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("car-log-audit worker thread panicked"))?;
        }

        anyhow::ensure!(
            errors.is_empty(),
            "directory scan errors:\n{}",
            errors.join("\n")
        );
    }

    let elapsed = started.elapsed().as_secs_f64();
    if let Some(global) = &global {
        write_report_files(&args.output_dir.join("global"), global, args.top, elapsed)?;
    }

    println!(
        "scanned_epochs={} elapsed={elapsed:.2}s report_dir={}",
        input_count,
        args.output_dir.display()
    );
    println!("summary={}", args.output_dir.join("epochs.tsv").display());
    if args.global_report {
        println!("global={}", args.output_dir.join("global").display());
    }

    Ok(())
}

struct EpochScanResult {
    input: PathBuf,
    epoch_name: String,
    elapsed: f64,
    report: AuditReport,
}

fn scan_epoch_input(
    args: &Args,
    epoch_dir: &Path,
    total: usize,
    index: usize,
    input: PathBuf,
) -> Result<EpochScanResult> {
    let epoch_name = report_name_for_input(&input);
    let mut file_args = args.clone();
    file_args.input = input.clone();
    file_args.dump_unparsed = None;
    file_args.dump_program_payloads = None;

    info!(
        input = %input.display(),
        epoch = %epoch_name,
        ordinal = index + 1,
        total,
        "car-log-audit directory scan starting epoch"
    );

    let epoch_started = Instant::now();
    let report =
        scan_input(&file_args).with_context(|| format!("scan epoch input {}", input.display()))?;
    let elapsed = epoch_started.elapsed().as_secs_f64();

    let per_epoch_dir = epoch_dir.join(&epoch_name);
    fs::create_dir_all(&per_epoch_dir)
        .with_context(|| format!("create {}", per_epoch_dir.display()))?;
    write_report_files(&per_epoch_dir, &report, args.top, elapsed)?;

    info!(
        epoch = %epoch_name,
        blocks = report.blocks,
        txs = report.txs,
        log_lines = report.log_lines,
        top_level_unparsed = report.top_level_unparsed,
        unparsed_program = report.unparsed_program,
        unknown_program_payloads = report.program_payload_programs.counts.values().sum::<u64>(),
        elapsed_s = elapsed,
        "car-log-audit directory scan finished epoch"
    );

    Ok(EpochScanResult {
        input,
        epoch_name,
        elapsed,
        report,
    })
}

fn record_epoch_result(
    summary: &mut impl Write,
    global: Option<&mut AuditReport>,
    result: EpochScanResult,
) -> Result<()> {
    write_epoch_summary_row(
        summary,
        &result.epoch_name,
        &result.input,
        &result.report,
        result.elapsed,
    )?;
    summary.flush()?;
    if let Some(global) = global {
        global.merge_from(&result.report);
    }
    Ok(())
}

fn collect_car_inputs(input: &Path, recursive: bool) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_car_inputs_into(input, recursive, &mut out)?;
    out.sort_by(compare_epoch_inputs);
    Ok(out)
}

fn compare_epoch_inputs(a: &PathBuf, b: &PathBuf) -> std::cmp::Ordering {
    match (epoch_number_from_path(a), epoch_number_from_path(b)) {
        (Some(a_epoch), Some(b_epoch)) => a_epoch.cmp(&b_epoch).then_with(|| a.cmp(b)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(b),
    }
}

fn epoch_number_from_path(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let name = name
        .strip_suffix(".car.zst")
        .or_else(|| name.strip_suffix(".car"))?;
    name.strip_prefix("epoch-")?.parse().ok()
}

fn collect_car_inputs_into(dir: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if recursive {
                collect_car_inputs_into(&path, recursive, out)?;
            }
        } else if file_type.is_file() && is_car_input(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_car_input(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.ends_with(".car") || name.ends_with(".car.zst")
}

fn report_name_for_input(path: &Path) -> String {
    let mut name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("epoch")
        .to_string();
    if let Some(stripped) = name.strip_suffix(".car.zst") {
        name = stripped.to_string();
    } else if let Some(stripped) = name.strip_suffix(".car") {
        name = stripped.to_string();
    }
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn write_epoch_summary_row(
    out: &mut impl Write,
    epoch_name: &str,
    input: &Path,
    report: &AuditReport,
    elapsed: f64,
) -> Result<()> {
    let elapsed = format!("{elapsed:.3}");
    let blocks = report.blocks.to_string();
    let txs = report.txs.to_string();
    let metadata = report.metadata.to_string();
    let metadata_visitor = report.metadata_visitor.to_string();
    let metadata_legacy_decoded = report.metadata_legacy_decoded.to_string();
    let metadata_decode_errors = report.metadata_decode_errors.to_string();
    let log_lines = report.log_lines.to_string();
    let top_level_unparsed = report.top_level_unparsed.to_string();
    let unparsed_program = report.unparsed_program.to_string();
    let unknown_program_payloads = report
        .program_payload_programs
        .counts
        .values()
        .sum::<u64>()
        .to_string();
    let invalid_pubkeys = report.invalid_pubkeys.to_string();
    let invalid_base64 = report.invalid_base64.to_string();
    write_tsv_record(
        out,
        &[
            epoch_name,
            &input.display().to_string(),
            &elapsed,
            &blocks,
            &txs,
            &metadata,
            &metadata_visitor,
            &metadata_legacy_decoded,
            &metadata_decode_errors,
            &log_lines,
            &top_level_unparsed,
            &unparsed_program,
            &unknown_program_payloads,
            &invalid_pubkeys,
            &invalid_base64,
        ],
    )?;
    Ok(())
}

fn write_report_files(dir: &Path, report: &AuditReport, top: usize, elapsed: f64) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    write_summary_txt(&dir.join("summary.txt"), report, elapsed)?;
    write_kinds_tsv(&dir.join("kinds.tsv"), &report.kinds)?;
    write_counter_tsv(&dir.join("unparsed-exact.tsv"), &report.unparsed_exact, top)?;
    write_counter_tsv(
        &dir.join("unparsed-shapes.tsv"),
        &report.unparsed_shapes,
        top,
    )?;
    write_counter_tsv(
        &dir.join("program-payload-programs.tsv"),
        &report.program_payload_programs,
        top,
    )?;
    write_counter_tsv(
        &dir.join("unknown-program-payload-shapes.tsv"),
        &report.unknown_program_payload_shapes,
        top,
    )?;
    write_counter_tsv(
        &dir.join("program-payload-shapes.tsv"),
        &report.program_payload_shapes,
        top,
    )?;
    write_counter_tsv(
        &dir.join("program-id-payload-shapes.tsv"),
        &report.program_id_payload_shapes,
        top,
    )?;
    write_counter_tsv(
        &dir.join("program-payload-exact.tsv"),
        &report.program_payload_exact,
        top,
    )?;
    write_counter_tsv(
        &dir.join("failure-reason-shapes.tsv"),
        &report.failure_reason_shapes,
        top,
    )?;
    write_counter_tsv(
        &dir.join("invalid-pubkeys.tsv"),
        &report.invalid_pubkey_exact,
        top,
    )?;
    write_counter_tsv(
        &dir.join("invalid-base64.tsv"),
        &report.invalid_base64_exact,
        top,
    )?;
    Ok(())
}

fn write_summary_txt(path: &Path, report: &AuditReport, elapsed: f64) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    writeln!(out, "elapsed_s={elapsed:.3}")?;
    writeln!(
        out,
        "blocks={} txs={} metadata={} metadata_visitor={} metadata_legacy_decoded={} txs_without_metadata={} metadata_decode_errors={} log_messages_none={} log_lines={}",
        report.blocks,
        report.txs,
        report.metadata,
        report.metadata_visitor,
        report.metadata_legacy_decoded,
        report.txs_without_metadata,
        report.metadata_decode_errors,
        report.log_messages_none,
        report.log_lines
    )?;
    writeln!(
        out,
        "parser_misses top_level_unparsed={} unparsed_program={} unknown_program_payloads={} invalid_pubkeys={} invalid_base64={} dropped_logs_due_to_decode_error={}",
        report.top_level_unparsed,
        report.unparsed_program,
        report.program_payload_programs.counts.values().sum::<u64>(),
        report.invalid_pubkeys,
        report.invalid_base64,
        report.dropped_logs_due_to_decode_error
    )?;
    if !report.first_errors.is_empty() {
        writeln!(out, "first_errors:")?;
        for error in &report.first_errors {
            writeln!(out, "  {error}")?;
        }
    }
    Ok(())
}

fn write_kinds_tsv(path: &Path, kinds: &BTreeMap<&'static str, u64>) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    write_tsv_record(&mut out, &["count", "kind"])?;
    let mut rows = kinds.iter().collect::<Vec<_>>();
    rows.sort_unstable_by(|(a_key, a_count), (b_key, b_count)| {
        b_count.cmp(a_count).then_with(|| a_key.cmp(b_key))
    });
    for (kind, count) in rows {
        let count = count.to_string();
        write_tsv_record(&mut out, &[&count, kind])?;
    }
    Ok(())
}

fn write_counter_tsv(path: &Path, counter: &Counter, top: usize) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    write_tsv_record(&mut out, &["count", "value"])?;
    for (value, count) in counter.sorted().into_iter().take(top) {
        let count = count.to_string();
        write_tsv_record(&mut out, &[&count, value])?;
    }
    if counter.dropped_new_keys > 0 {
        let dropped = counter.dropped_new_keys.to_string();
        write_tsv_record(&mut out, &[&dropped, "<dropped-new-unique-keys>"])?;
    }
    Ok(())
}

fn scan_input(args: &Args) -> Result<AuditReport> {
    let file = File::open(&args.input).with_context(|| format!("open {}", args.input.display()))?;
    let file = BufReader::with_capacity(args.buf_size, file);

    if has_zst_extension(&args.input) {
        let zstd = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("open zstd stream {}", args.input.display()))?;
        scan_reader(zstd, args)
    } else {
        scan_reader(file, args)
    }
}

fn scan_reader<R: Read>(reader: R, args: &Args) -> Result<AuditReport> {
    let mut car = CarBlockReader::with_capacity(reader, args.buf_size);
    car.skip_header().context("skip CAR header")?;

    let mut group = CarBlockGroup::without_rewards();
    let mut report = AuditReport::new(args.max_unique);
    let mut streams = AuditStreams::new(args)?;
    let started = Instant::now();

    loop {
        if let Some(limit) = args.limit_blocks
            && report.blocks >= limit
        {
            break;
        }

        let has_block = car
            .read_until_block_into(&mut group)
            .with_context(|| format!("read block after {} complete blocks", report.blocks))?;
        if !has_block {
            break;
        }

        report.blocks += 1;
        scan_group(&mut group, &mut report, &mut streams)?;

        if args.progress_every > 0 && report.blocks.is_multiple_of(args.progress_every) {
            streams.flush()?;
            let elapsed = started.elapsed().as_secs_f64().max(1e-9);
            info!(
                blocks = report.blocks,
                txs = report.txs,
                metadata = report.metadata,
                metadata_visitor = report.metadata_visitor,
                metadata_legacy_decoded = report.metadata_legacy_decoded,
                log_lines = report.log_lines,
                streamed_unparsed = report.streamed_unparsed,
                streamed_program_payloads = report.streamed_program_payloads,
                blocks_per_s = report.blocks as f64 / elapsed,
                "car-log-audit progress"
            );
        }
    }

    streams.flush()?;
    Ok(report)
}

struct AuditStreams {
    unparsed: Option<BufWriter<File>>,
    program_payloads: Option<BufWriter<File>>,
}

impl AuditStreams {
    fn new(args: &Args) -> Result<Self> {
        Ok(Self {
            unparsed: open_stream(&args.dump_unparsed)?,
            program_payloads: open_stream(&args.dump_program_payloads)?,
        })
    }

    fn has_unparsed(&self) -> bool {
        self.unparsed.is_some()
    }

    fn has_program_payloads(&self) -> bool {
        self.program_payloads.is_some()
    }

    fn write_unparsed(&mut self, kind: &str, value: &str) -> Result<()> {
        if let Some(out) = &mut self.unparsed {
            write_tsv_record(out, &[kind, value]).context("write streamed unparsed log")?;
        }
        Ok(())
    }

    fn write_program_payload(
        &mut self,
        kind: &str,
        program: Option<&str>,
        value: &str,
    ) -> Result<()> {
        if let Some(out) = &mut self.program_payloads {
            write_tsv_record(out, &[kind, program.unwrap_or(""), value])
                .context("write streamed program payload")?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(out) = &mut self.unparsed {
            out.flush().context("flush streamed unparsed log")?;
        }
        if let Some(out) = &mut self.program_payloads {
            out.flush().context("flush streamed program payloads")?;
        }
        Ok(())
    }
}

fn open_stream(path: &Option<PathBuf>) -> Result<Option<BufWriter<File>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    Ok(Some(BufWriter::with_capacity(8 << 20, file)))
}

fn write_tsv_record(out: &mut impl Write, fields: &[&str]) -> std::io::Result<()> {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.write_all(b"\t")?;
        }
        write_escaped_tsv_field(out, field)?;
    }
    out.write_all(b"\n")
}

fn write_escaped_tsv_field(out: &mut impl Write, value: &str) -> std::io::Result<()> {
    for ch in value.chars() {
        match ch {
            '\n' => out.write_all(b"\\n")?,
            '\r' => out.write_all(b"\\r")?,
            '\t' => out.write_all(b"\\t")?,
            '\\' => out.write_all(b"\\\\")?,
            _ => write!(out, "{ch}")?,
        }
    }
    Ok(())
}

fn scan_group(
    group: &mut CarBlockGroup,
    report: &mut AuditReport,
    streams: &mut AuditStreams,
) -> Result<()> {
    let block_slot = group.slot;
    let mut tx_index = 0u64;
    let mut metadata = group.transaction_metadata();

    loop {
        let next = {
            let mut visitor = AuditLogVisitor {
                report,
                streams,
                error: None,
                program_stack: Vec::new(),
            };
            let next = metadata.next_metadata_visit(&mut visitor);
            if let Some(error) = visitor.error.take() {
                return Err(error);
            }
            next
        };

        match next {
            Ok(Some(tx)) => {
                report.txs += 1;
                tx_index += 1;
                match tx {
                    TxMetadataVisit::Missing { .. } => {
                        report.txs_without_metadata += 1;
                    }
                    TxMetadataVisit::ProtobufVisited { .. } => {
                        report.metadata += 1;
                        report.metadata_visitor += 1;
                    }
                    TxMetadataVisit::LegacyDecoded { metadata, .. } => {
                        report.metadata += 1;
                        report.metadata_legacy_decoded += 1;
                        audit_materialized_metadata_logs(metadata, report, streams)?;
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                report.metadata_decode_errors += 1;
                report.dropped_logs_due_to_decode_error += 1;
                report.add_error(format!(
                    "block_slot={:?} tx_index={} metadata decode failed: {}",
                    block_slot, tx_index, err
                ));
                break;
            }
        }
    }

    Ok(())
}

struct AuditLogVisitor<'report, 'streams> {
    report: &'report mut AuditReport,
    streams: &'streams mut AuditStreams,
    error: Option<anyhow::Error>,
    program_stack: Vec<String>,
}

impl<'a> TransactionStatusMetaVisitor<'a> for AuditLogVisitor<'_, '_> {
    #[inline]
    fn wants_log_messages(&self) -> bool {
        true
    }

    #[inline]
    fn log_message(&mut self, message: &'a str) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) =
            audit_log_line(message, self.report, self.streams, &mut self.program_stack)
        {
            self.error = Some(error);
        }
    }

    #[inline]
    fn log_messages_none(&mut self, none: bool) {
        if none {
            self.report.log_messages_none += 1;
        }
    }
}

fn audit_materialized_metadata_logs(
    meta: &TransactionStatusMeta,
    report: &mut AuditReport,
    streams: &mut AuditStreams,
) -> Result<()> {
    if meta.log_messages_none {
        report.log_messages_none += 1;
        return Ok(());
    }

    let mut program_stack = Vec::new();
    for line in &meta.log_messages {
        audit_log_line(line, report, streams, &mut program_stack)?;
    }
    Ok(())
}

fn audit_log_line(
    line: &str,
    report: &mut AuditReport,
    streams: &mut AuditStreams,
    program_stack: &mut Vec<String>,
) -> Result<()> {
    let line = line.trim_end();
    report.log_lines += 1;

    let parsed = parse_line(line);
    *report.kinds.entry(kind_name(parsed)).or_default() += 1;

    match parsed {
        ParsedLogLine::Plain { text } => {
            let active_program = program_stack.last().map(String::as_str);
            if !plain_log_has_known_binary_form(active_program, text) {
                report.top_level_unparsed += 1;
                if streams.has_unparsed() {
                    streams.write_unparsed("Plain", text)?;
                    report.streamed_unparsed += 1;
                } else {
                    report.unparsed_exact.add(text);
                }
                report.unparsed_shapes.add(&normalize_shape(text));
            }
        }
        ParsedLogLine::UnparsedProgram => {
            report.unparsed_program += 1;
            if streams.has_unparsed() {
                streams.write_unparsed("UnparsedProgram", line)?;
                report.streamed_unparsed += 1;
            } else {
                report.unparsed_exact.add(line);
            }
            report.unparsed_shapes.add(&normalize_shape(line));
        }
        ParsedLogLine::UnknownProgram { program } => validate_pubkey(program, line, report),
        ParsedLogLine::UnknownAccount { account } => validate_pubkey(account, line, report),
        ParsedLogLine::ProgramIdLog { program, text } => {
            validate_pubkey(program, line, report);
            let is_unknown_payload = !program_log_payload_has_generic_binary_form(text)
                && !program_log_has_known_binary_form(Some(program), text);
            if streams.has_program_payloads() && is_unknown_payload {
                streams.write_program_payload("ProgramIdLog", Some(program), text)?;
                report.streamed_program_payloads += 1;
            } else if !streams.has_program_payloads() {
                report.program_payload_exact.add(text);
            }
            if is_unknown_payload {
                report.program_payload_programs.add(program);
                report.unknown_program_payload_shapes.add(&format!(
                    "program={} shape={}",
                    program,
                    normalize_shape(text)
                ));
            }
            report.program_payload_shapes.add(&normalize_shape(text));
            report.program_id_payload_shapes.add(&format!(
                "{} {}",
                normalize_token(program),
                normalize_shape(text)
            ));
        }
        ParsedLogLine::ProgramLog { text } => {
            let active_program = program_stack.last().map(String::as_str);
            let is_unknown_payload = !program_log_payload_has_generic_binary_form(text)
                && !program_log_has_known_binary_form(active_program, text);
            if streams.has_program_payloads() && is_unknown_payload {
                streams.write_program_payload("ProgramLog", active_program, text)?;
                report.streamed_program_payloads += 1;
            } else if !streams.has_program_payloads() {
                report.program_payload_exact.add(text);
            }
            if is_unknown_payload {
                let program = active_program.unwrap_or("<missing-active-program>");
                report.program_payload_programs.add(program);
                report.unknown_program_payload_shapes.add(&format!(
                    "program={} shape={}",
                    program,
                    normalize_shape(text)
                ));
            }
            report.program_payload_shapes.add(&normalize_shape(text));
        }
        ParsedLogLine::ProgramReturn { program, data } => {
            validate_pubkey(program, line, report);
            validate_base64_chunks(data, line, report);
        }
        ParsedLogLine::ProgramData { data } => validate_base64_chunks(data, line, report),
        ParsedLogLine::ProgramNotCached {
            program: Some(program),
        }
        | ParsedLogLine::ProgramNotDeployed {
            program: Some(program),
        }
        | ParsedLogLine::LoaderUpgradedProgram { program }
        | ParsedLogLine::LoaderFinalizedAccount { account: program }
        | ParsedLogLine::RuntimeWritablePrivilegeEscalated { account: program }
        | ParsedLogLine::RuntimeSignerPrivilegeEscalated { account: program }
        | ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { account: program }
        | ParsedLogLine::Invoke { program, .. }
        | ParsedLogLine::BpfInvoke { program }
        | ParsedLogLine::Success { program }
        | ParsedLogLine::BpfSuccess { program }
        | ParsedLogLine::Failure { program, .. }
        | ParsedLogLine::BpfFailure { program, .. }
        | ParsedLogLine::Consumed { program, .. }
        | ParsedLogLine::CbRequestUnits { program, .. } => {
            validate_pubkey(program, line, report);
            if let ParsedLogLine::Failure { reason, .. }
            | ParsedLogLine::BpfFailure { reason, .. } = parsed
            {
                report.failure_reason_shapes.add(&normalize_shape(reason));
            }
        }
        ParsedLogLine::SystemAllocateAccountAlreadyInUse { account }
        | ParsedLogLine::SystemCreateAccountAlreadyInUse { account } => {
            validate_pubkey(account.address, line, report);
            if let Some(base) = account.base {
                validate_pubkey(base, line, report);
            }
        }
        ParsedLogLine::FailedToComplete { reason } => {
            report.failure_reason_shapes.add(&normalize_shape(reason));
        }
        ParsedLogLine::CustomProgramError { .. }
        | ParsedLogLine::LogTruncated
        | ParsedLogLine::VerifyEd25519
        | ParsedLogLine::VerifySecp256k1
        | ParsedLogLine::CloseContextState
        | ParsedLogLine::ProgramAccountNotWritable
        | ParsedLogLine::ProgramIdMismatch
        | ParsedLogLine::ProgramNotUpgradeable
        | ParsedLogLine::ProgramAndProgramDataAccountMismatch
        | ParsedLogLine::ProgramWasExtendedInThisBlockAlready
        | ParsedLogLine::StakeMergingAccounts
        | ParsedLogLine::SystemTransferFromMustNotCarryData
        | ParsedLogLine::SystemCreateAccountDataSizeLimited { .. }
        | ParsedLogLine::SystemTransferInsufficient { .. }
        | ParsedLogLine::ProgramConsumption { .. }
        | ParsedLogLine::BpfConsumed { .. }
        | ParsedLogLine::ProgramNotCached { program: None }
        | ParsedLogLine::ProgramNotDeployed { program: None } => {}
    }

    update_program_stack(parsed, program_stack);
    Ok(())
}

fn program_log_payload_has_generic_binary_form(text: &str) -> bool {
    parse_custom_program_error_reason(text).is_some()
        || parse_program_log_error_payload(text).is_some()
}

fn plain_log_has_known_binary_form(active_program: Option<&str>, text: &str) -> bool {
    system_program::has_known_binary_form(text)
        || program_log_has_known_binary_form(None, text)
        || matches!(
            parse_line(text),
            ParsedLogLine::SystemTransferFromMustNotCarryData
                | ParsedLogLine::SystemCreateAccountDataSizeLimited { .. }
                | ParsedLogLine::RuntimeWritablePrivilegeEscalated { .. }
                | ParsedLogLine::RuntimeSignerPrivilegeEscalated { .. }
                | ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { .. }
                | ParsedLogLine::LogTruncated
        )
        || active_program
            .is_some_and(|program| official_program_log_has_known_binary_form(program, text))
}

fn update_program_stack(parsed: ParsedLogLine<'_>, stack: &mut Vec<String>) {
    match parsed {
        ParsedLogLine::Invoke { program, depth } => {
            let depth = depth as usize;
            if depth == 0 {
                stack.clear();
                stack.push(program.to_string());
                return;
            }
            stack.truncate(depth.saturating_sub(1));
            stack.push(program.to_string());
        }
        ParsedLogLine::BpfInvoke { program } => {
            stack.push(program.to_string());
        }
        ParsedLogLine::Success { program }
        | ParsedLogLine::Failure { program, .. }
        | ParsedLogLine::BpfSuccess { program }
        | ParsedLogLine::BpfFailure { program, .. } => {
            if stack.last().is_some_and(|active| active == program) {
                stack.pop();
            } else if let Some(position) = stack.iter().rposition(|active| active == program) {
                stack.truncate(position);
            } else {
                stack.clear();
            }
        }
        _ => {}
    }
}

fn validate_pubkey(value: &str, line: &str, report: &mut AuditReport) {
    if Pubkey::from_str(value).is_err() {
        report.invalid_pubkeys += 1;
        report.invalid_pubkey_exact.add(line);
    }
}

fn validate_base64_chunks(value: &str, line: &str, report: &mut AuditReport) {
    for chunk in value.split_whitespace() {
        if BASE64.decode(chunk.as_bytes()).is_err() {
            report.invalid_base64 += 1;
            report.invalid_base64_exact.add(line);
            return;
        }
    }
}

fn kind_name(parsed: ParsedLogLine<'_>) -> &'static str {
    match parsed {
        ParsedLogLine::CustomProgramError { .. } => "CustomProgramError",
        ParsedLogLine::FailedToComplete { .. } => "FailedToComplete",
        ParsedLogLine::UnknownProgram { .. } => "UnknownProgram",
        ParsedLogLine::UnknownAccount { .. } => "UnknownAccount",
        ParsedLogLine::LogTruncated => "LogTruncated",
        ParsedLogLine::VerifyEd25519 => "VerifyEd25519",
        ParsedLogLine::VerifySecp256k1 => "VerifySecp256k1",
        ParsedLogLine::CloseContextState => "CloseContextState",
        ParsedLogLine::ProgramAccountNotWritable => "ProgramAccountNotWritable",
        ParsedLogLine::ProgramIdMismatch => "ProgramIdMismatch",
        ParsedLogLine::ProgramNotUpgradeable => "ProgramNotUpgradeable",
        ParsedLogLine::ProgramAndProgramDataAccountMismatch => {
            "ProgramAndProgramDataAccountMismatch"
        }
        ParsedLogLine::ProgramWasExtendedInThisBlockAlready => {
            "ProgramWasExtendedInThisBlockAlready"
        }
        ParsedLogLine::StakeMergingAccounts => "StakeMergingAccounts",
        ParsedLogLine::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
        ParsedLogLine::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
        ParsedLogLine::RuntimeWritablePrivilegeEscalated { .. } => {
            "RuntimeWritablePrivilegeEscalated"
        }
        ParsedLogLine::RuntimeSignerPrivilegeEscalated { .. } => "RuntimeSignerPrivilegeEscalated",
        ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
            "RuntimeAccountOwnerBalanceVerificationFailed"
        }
        ParsedLogLine::SystemTransferInsufficient { .. } => "SystemTransferInsufficient",
        ParsedLogLine::SystemTransferFromMustNotCarryData => "SystemTransferFromMustNotCarryData",
        ParsedLogLine::SystemCreateAccountDataSizeLimited { .. } => {
            "SystemCreateAccountDataSizeLimited"
        }
        ParsedLogLine::SystemAllocateAccountAlreadyInUse { .. } => {
            "SystemAllocateAccountAlreadyInUse"
        }
        ParsedLogLine::SystemCreateAccountAlreadyInUse { .. } => "SystemCreateAccountAlreadyInUse",
        ParsedLogLine::ProgramLog { .. } => "ProgramLog",
        ParsedLogLine::ProgramIdLog { .. } => "ProgramIdLog",
        ParsedLogLine::ProgramData { .. } => "ProgramData",
        ParsedLogLine::ProgramReturn { .. } => "ProgramReturn",
        ParsedLogLine::ProgramConsumption { .. } => "ProgramConsumption",
        ParsedLogLine::ProgramNotCached { .. } => "ProgramNotCached",
        ParsedLogLine::ProgramNotDeployed { .. } => "ProgramNotDeployed",
        ParsedLogLine::Invoke { .. } => "Invoke",
        ParsedLogLine::BpfInvoke { .. } => "BpfInvoke",
        ParsedLogLine::Success { .. } => "Success",
        ParsedLogLine::BpfSuccess { .. } => "BpfSuccess",
        ParsedLogLine::Failure { .. } => "Failure",
        ParsedLogLine::BpfFailure { .. } => "BpfFailure",
        ParsedLogLine::Consumed { .. } => "Consumed",
        ParsedLogLine::BpfConsumed { .. } => "BpfConsumed",
        ParsedLogLine::CbRequestUnits { .. } => "CbRequestUnits",
        ParsedLogLine::UnparsedProgram => "UnparsedProgram",
        ParsedLogLine::Plain { .. } => "Plain",
    }
}

fn normalize_shape(value: &str) -> String {
    value
        .split_whitespace()
        .map(normalize_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_token(token: &str) -> String {
    let core = token.trim_matches(|c: char| {
        matches!(
            c,
            '[' | ']' | '(' | ')' | '{' | '}' | ',' | '.' | ':' | ';' | '\'' | '"' | '`'
        )
    });

    if core.is_empty() {
        return token.to_owned();
    }
    if Pubkey::from_str(core).is_ok() {
        return "<PUBKEY>".to_string();
    }
    if is_decimal(core) {
        return "<N>".to_string();
    }
    if is_hex(core) {
        return "<HEX>".to_string();
    }
    if looks_like_base64(core) {
        return "<B64>".to_string();
    }

    core.to_owned()
}

fn is_decimal(value: &str) -> bool {
    let mut saw_digit = false;
    for b in value.bytes() {
        match b {
            b'0'..=b'9' => saw_digit = true,
            b',' => {}
            _ => return false,
        }
    }
    saw_digit
}

fn is_hex(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("0x") else {
        return false;
    };
    !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

fn looks_like_base64(value: &str) -> bool {
    value.len() >= 20
        && value
            .bytes()
            .any(|b| b.is_ascii_digit() || matches!(b, b'+' | b'/' | b'='))
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'='))
        && BASE64.decode(value.as_bytes()).is_ok()
}

fn has_zst_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
}

fn print_kinds(title: &str, kinds: &BTreeMap<&'static str, u64>) {
    println!("{title}:");
    let mut rows = kinds.iter().collect::<Vec<_>>();
    rows.sort_unstable_by(|(a_key, a_count), (b_key, b_count)| {
        b_count.cmp(a_count).then_with(|| a_key.cmp(b_key))
    });
    for (kind, count) in rows {
        println!("  {count:>12}  {kind}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_inputs_sort_by_numeric_epoch() {
        let mut inputs = vec![
            PathBuf::from("epoch-720.car.zst"),
            PathBuf::from("epoch-72.car.zst"),
            PathBuf::from("epoch-9.car"),
            PathBuf::from("other.car.zst"),
        ];

        inputs.sort_by(compare_epoch_inputs);

        assert_eq!(
            inputs,
            vec![
                PathBuf::from("epoch-9.car"),
                PathBuf::from("epoch-72.car.zst"),
                PathBuf::from("epoch-720.car.zst"),
                PathBuf::from("other.car.zst"),
            ]
        );
    }

    #[test]
    fn historical_bpf_failures_are_not_unparsed() {
        let program = "3qyCAxkCQEaix78hmYeU4k9fUXvhywnxE5U6SyLds328";
        let mut report = AuditReport::new(100);
        let mut streams = AuditStreams {
            unparsed: None,
            program_payloads: None,
        };
        let mut stack = Vec::new();

        for line in [
            format!("BPF program {program} failed custom program error: 0x1"),
            format!("BPF program {program} failed invalid account data for instruction"),
            format!("BPF program {program} failed insufficient account keys for instruction"),
        ] {
            audit_log_line(&line, &mut report, &mut streams, &mut stack).unwrap();
        }

        assert_eq!(report.top_level_unparsed, 0);
        assert_eq!(report.unparsed_program, 0);
        assert!(report.unparsed_shapes.counts.is_empty());
        assert_eq!(report.kinds.get("BpfFailure"), Some(&3));
        assert_eq!(
            report
                .failure_reason_shapes
                .counts
                .get("custom program error <HEX>"),
            Some(&1)
        );
    }
}

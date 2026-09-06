use std::{
    collections::{BTreeMap, VecDeque},
    env,
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
};

use minicbor::Decoder;

const CAR_BUF: usize = 128 << 20;
const CID_LEN: usize = 36;
const PREFIX_LEN: usize = 32;
const MAX_UVARINT_LEN_64: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Prefer {
    Raw,
    Zstd,
}

#[derive(Debug)]
struct Config {
    inputs: Vec<PathBuf>,
    jobs: usize,
    prefer: Prefer,
    gaps_only: bool,
}

#[derive(Debug)]
struct EpochInput {
    epoch: Option<u64>,
    path: PathBuf,
}

#[derive(Debug)]
struct Gap {
    prev_slot: u64,
    next_slot: u64,
    skipped: u64,
}

#[derive(Debug)]
struct ScanSummary {
    epoch: Option<u64>,
    path: PathBuf,
    entries: u64,
    blocks: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    skipped_slots: u64,
    gaps: Vec<Gap>,
    elapsed_ms: u128,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let inputs = discover_inputs(&config.inputs, config.prefer)?;
    if inputs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no epoch CAR files found",
        ));
    }

    let jobs = config.jobs.max(1).min(inputs.len());
    let work = Arc::new(Mutex::new(VecDeque::from(inputs)));
    let (tx, rx) = mpsc::channel();

    for _ in 0..jobs {
        let work = Arc::clone(&work);
        let tx = tx.clone();
        thread::spawn(move || {
            loop {
                let item = {
                    let mut work = work.lock().expect("work mutex poisoned");
                    work.pop_front()
                };
                let Some(input) = item else {
                    break;
                };
                let result = scan_epoch(input);
                if tx.send(result).is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);

    let mut summaries = Vec::new();
    let mut failed = false;
    for result in rx {
        match result {
            Ok(summary) => summaries.push(summary),
            Err(err) => {
                failed = true;
                eprintln!("car_skipped_slots: {err}");
            }
        }
    }

    summaries.sort_by(|a, b| match (a.epoch, b.epoch) {
        (Some(left), Some(right)) => left.cmp(&right).then_with(|| a.path.cmp(&b.path)),
        _ => a.path.cmp(&b.path),
    });

    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    if !config.gaps_only {
        writeln!(
            out,
            "type\tepoch\tpath\tentries\tblocks\tfirst_slot\tlast_slot\tskipped_slots\tgaps\telapsed_ms"
        )?;
    }
    for summary in &summaries {
        if !config.gaps_only {
            writeln!(
                out,
                "summary\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                display_epoch(summary.epoch),
                summary.path.display(),
                summary.entries,
                summary.blocks,
                display_opt_u64(summary.first_slot),
                display_opt_u64(summary.last_slot),
                summary.skipped_slots,
                summary.gaps.len(),
                summary.elapsed_ms
            )?;
        }
        for gap in &summary.gaps {
            writeln!(
                out,
                "gap\t{}\t{}\t\t\t{}\t{}\t{}\t\t",
                display_epoch(summary.epoch),
                summary.path.display(),
                gap.prev_slot,
                gap.next_slot,
                gap.skipped
            )?;
        }
    }
    out.flush()?;

    if failed {
        std::process::exit(1);
    }
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let mut inputs = Vec::new();
    let mut jobs = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let mut prefer = Prefer::Raw;
    let mut gaps_only = false;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "-j" | "--jobs" => {
                let Some(value) = args.next() else {
                    return invalid_input("--jobs requires a value");
                };
                jobs = value.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid --jobs value")
                })?;
            }
            "--prefer-zst" => prefer = Prefer::Zstd,
            "--prefer-raw" => prefer = Prefer::Raw,
            "--gaps-only" => gaps_only = true,
            _ if arg.starts_with('-') => {
                return invalid_input(format!("unknown option {arg}"));
            }
            _ => inputs.push(PathBuf::from(arg)),
        }
    }

    if inputs.is_empty() {
        print_usage();
        return invalid_input("missing input file or directory");
    }

    Ok(Config {
        inputs,
        jobs,
        prefer,
        gaps_only,
    })
}

fn print_usage() {
    eprintln!(
        "usage: car_skipped_slots [--jobs N] [--prefer-raw|--prefer-zst] [--gaps-only] <epoch.car|epoch.car.zst|dir>..."
    );
}

fn discover_inputs(inputs: &[PathBuf], prefer: Prefer) -> io::Result<Vec<EpochInput>> {
    let mut direct = Vec::new();
    let mut discovered: BTreeMap<u64, EpochInput> = BTreeMap::new();

    for input in inputs {
        if input.is_dir() {
            for entry in fs::read_dir(input)? {
                let path = entry?.path();
                let Some(epoch) = epoch_from_path(&path) else {
                    continue;
                };
                let candidate = EpochInput {
                    epoch: Some(epoch),
                    path,
                };
                match discovered.get(&epoch) {
                    Some(existing) if keep_existing(&existing.path, &candidate.path, prefer) => {}
                    _ => {
                        discovered.insert(epoch, candidate);
                    }
                }
            }
        } else {
            direct.push(EpochInput {
                epoch: epoch_from_path(input),
                path: input.clone(),
            });
        }
    }

    direct.extend(discovered.into_values());
    direct.sort_by(|a, b| match (a.epoch, b.epoch) {
        (Some(left), Some(right)) => left.cmp(&right).then_with(|| a.path.cmp(&b.path)),
        _ => a.path.cmp(&b.path),
    });
    Ok(direct)
}

fn keep_existing(existing: &Path, candidate: &Path, prefer: Prefer) -> bool {
    let existing_zst = is_zstd_path(existing);
    let candidate_zst = is_zstd_path(candidate);
    match prefer {
        Prefer::Raw => !existing_zst || candidate_zst,
        Prefer::Zstd => existing_zst || !candidate_zst,
    }
}

fn scan_epoch(input: EpochInput) -> io::Result<ScanSummary> {
    let start = Instant::now();
    let mut summary = if is_zstd_path(&input.path) {
        let file = File::open(&input.path)?;
        let file = BufReader::with_capacity(CAR_BUF, file);
        let zstd = zstd::Decoder::with_buffer(file)?;
        let reader = BufReader::with_capacity(CAR_BUF, zstd);
        scan_reader(reader, input.epoch, input.path.clone())?
    } else {
        let file = File::open(&input.path)?;
        let reader = BufReader::with_capacity(CAR_BUF, file);
        scan_reader(reader, input.epoch, input.path.clone())?
    };
    summary.elapsed_ms = start.elapsed().as_millis();
    Ok(summary)
}

fn scan_reader<R: Read>(
    mut reader: BufReader<R>,
    epoch: Option<u64>,
    path: PathBuf,
) -> io::Result<ScanSummary> {
    let Some(header_len) = try_read_uvarint64(&mut reader)? else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{} is empty", path.display()),
        ));
    };
    skip_bytes(&mut reader, header_len as usize)?;

    let mut summary = ScanSummary {
        epoch,
        path,
        entries: 0,
        blocks: 0,
        first_slot: None,
        last_slot: None,
        skipped_slots: 0,
        gaps: Vec::new(),
        elapsed_ms: 0,
    };
    let mut cid = [0u8; CID_LEN];
    let mut prefix = [0u8; PREFIX_LEN];

    while let Some(entry_len) = try_read_uvarint64(&mut reader)? {
        let entry_len = usize::try_from(entry_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "entry length does not fit usize",
            )
        })?;
        if entry_len < CID_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "entry {} length {entry_len} is smaller than cid size",
                    summary.entries
                ),
            ));
        }

        reader.read_exact(&mut cid)?;
        let payload_len = entry_len - CID_LEN;
        let prefix_len = payload_len.min(PREFIX_LEN);
        reader.read_exact(&mut prefix[..prefix_len])?;

        if let Some(slot) = decode_block_slot_prefix(&prefix[..prefix_len])? {
            record_block_slot(&mut summary, slot);
        }

        skip_bytes(&mut reader, payload_len - prefix_len)?;
        summary.entries += 1;
    }

    Ok(summary)
}

fn record_block_slot(summary: &mut ScanSummary, slot: u64) {
    summary.blocks += 1;
    if summary.first_slot.is_none() {
        summary.first_slot = Some(slot);
    }
    if let Some(prev_slot) = summary.last_slot
        && slot > prev_slot + 1
    {
        let skipped = slot - prev_slot - 1;
        summary.skipped_slots += skipped;
        summary.gaps.push(Gap {
            prev_slot,
            next_slot: slot,
            skipped,
        });
    }
    summary.last_slot = Some(slot);
}

fn decode_block_slot_prefix(prefix: &[u8]) -> io::Result<Option<u64>> {
    if prefix.len() < 3 || !looks_like_array(prefix[0]) {
        return Ok(None);
    }

    let mut decoder = Decoder::new(prefix);
    let Some(array_len) = decoder.array().map_err(invalid_data)? else {
        return Ok(None);
    };
    if array_len < 2 {
        return Ok(None);
    }

    let kind = decoder.u64().map_err(invalid_data)?;
    if kind != 2 {
        return Ok(None);
    }

    let slot = decoder.u64().map_err(invalid_data)?;
    Ok(Some(slot))
}

fn looks_like_array(byte: u8) -> bool {
    (0x80..0xa0).contains(&byte)
}

fn skip_bytes<R: BufRead>(reader: &mut R, mut len: usize) -> io::Result<()> {
    while len > 0 {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected eof while skipping bytes",
            ));
        }
        let consumed = available.len().min(len);
        reader.consume(consumed);
        len -= consumed;
    }
    Ok(())
}

fn try_read_uvarint64<R: BufRead>(reader: &mut R) -> io::Result<Option<u64>> {
    let mut value = 0u64;
    let mut shift = 0u32;
    let mut total = 0usize;
    let mut started = false;

    loop {
        if total >= MAX_UVARINT_LEN_64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uvarint overflow",
            ));
        }

        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            if started {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof while reading uvarint",
                ));
            }
            return Ok(None);
        }

        let mut consumed = 0usize;
        for &byte in buf {
            started = true;
            total += 1;
            consumed += 1;

            if byte < 0x80 {
                if total == MAX_UVARINT_LEN_64 && byte > 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "uvarint overflow",
                    ));
                }
                value |= (byte as u64) << shift;
                reader.consume(consumed);
                return Ok(Some(value));
            }

            value |= ((byte & 0x7f) as u64) << shift;
            shift += 7;
            if shift > 63 {
                reader.consume(consumed);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uvarint too long",
                ));
            }
        }

        reader.consume(consumed);
    }
}

fn epoch_from_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    let rest = filename.strip_prefix("epoch-")?;
    if !(rest.ends_with(".car") || rest.ends_with(".car.zst")) {
        return None;
    }
    let digits_len = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits_len == 0 {
        return None;
    }
    rest[..digits_len].parse().ok()
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
}

fn display_epoch(epoch: Option<u64>) -> String {
    epoch
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn display_opt_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn invalid_input<T>(message: impl Into<String>) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}

fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

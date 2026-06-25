use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const SLOTS_PER_EPOCH: u64 = 432_000;
const KEY_LEN: usize = 32;
const EPOCH_LEN: usize = 4;
const RECORD_LEN: usize = KEY_LEN + EPOCH_LEN;

#[derive(Clone, Debug)]
struct EpochRow {
    epoch: u32,
    registry: PathBuf,
    account_count: u64,
    tx_count: String,
    blockhash_count: u64,
}

#[derive(Debug)]
struct Config {
    registry_root: PathBuf,
    out_csv: PathBuf,
    work_dir: PathBuf,
    metadata_csv: Option<PathBuf>,
    bucket_bits: u32,
    start_epoch: u32,
    end_epoch: Option<u32>,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let rows = iter_epochs(&config)?;
    if rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no epoch-N/registry.bin files under {}", config.registry_root.display()),
        ));
    }
    fs::create_dir_all(&config.work_dir)?;
    let new_by_epoch = if let Some(done) = read_reduced(&config.work_dir, rows.last().unwrap().epoch)? {
        done
    } else {
        bucketize(&config, &rows)?;
        reduce_buckets(&config.work_dir, config.bucket_bits, rows.last().unwrap().epoch)?
    };
    write_csv(&config.out_csv, &rows, &new_by_epoch)?;
    eprintln!("wrote {}", config.out_csv.display());
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let mut registry_root = None;
    let mut out_csv = None;
    let mut work_dir = None;
    let mut metadata_csv = None;
    let mut bucket_bits = 12u32;
    let mut start_epoch = 0u32;
    let mut end_epoch = None;
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--registry-root" => {
                i += 1;
                registry_root = args.get(i).map(PathBuf::from);
            }
            "--out-csv" => {
                i += 1;
                out_csv = args.get(i).map(PathBuf::from);
            }
            "--work-dir" => {
                i += 1;
                work_dir = args.get(i).map(PathBuf::from);
            }
            "--metadata-csv" => {
                i += 1;
                metadata_csv = args.get(i).map(PathBuf::from);
            }
            "--bucket-bits" => {
                i += 1;
                bucket_bits = args
                    .get(i)
                    .ok_or_else(|| invalid("--bucket-bits requires a value"))?
                    .parse()
                    .map_err(|_| invalid("--bucket-bits must be an integer"))?;
            }
            "--start-epoch" => {
                i += 1;
                start_epoch = args
                    .get(i)
                    .ok_or_else(|| invalid("--start-epoch requires a value"))?
                    .parse()
                    .map_err(|_| invalid("--start-epoch must be an integer"))?;
            }
            "--end-epoch" => {
                i += 1;
                end_epoch = Some(
                    args.get(i)
                        .ok_or_else(|| invalid("--end-epoch requires a value"))?
                        .parse()
                        .map_err(|_| invalid("--end-epoch must be an integer"))?,
                );
            }
            other => return Err(invalid(&format!("unknown argument {other}"))),
        }
        i += 1;
    }
    let bucket_count = 1u64
        .checked_shl(bucket_bits)
        .ok_or_else(|| invalid("--bucket-bits is too large"))?;
    if bucket_count == 0 || bucket_count > 65_536 {
        return Err(invalid("--bucket-bits must produce 1..65536 buckets"));
    }
    Ok(Config {
        registry_root: registry_root.ok_or_else(|| invalid("--registry-root is required"))?,
        out_csv: out_csv.ok_or_else(|| invalid("--out-csv is required"))?,
        work_dir: work_dir.ok_or_else(|| invalid("--work-dir is required"))?,
        metadata_csv,
        bucket_bits,
        start_epoch,
        end_epoch,
    })
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn iter_epochs(config: &Config) -> io::Result<Vec<EpochRow>> {
    let metadata = if let Some(path) = &config.metadata_csv {
        read_metadata(path)?
    } else {
        HashMap::new()
    };
    let mut rows = Vec::new();
    for entry in fs::read_dir(&config.registry_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(epoch) = parse_epoch_dir(&name.to_string_lossy()) else {
            continue;
        };
        if epoch < config.start_epoch || config.end_epoch.is_some_and(|end| epoch > end) {
            continue;
        }
        let registry = entry.path().join("registry.bin");
        if !registry.is_file() {
            continue;
        }
        let account_count = count_registry(&registry)?;
        let (tx_count, blockhash_count) = metadata
            .get(&epoch)
            .cloned()
            .unwrap_or_else(|| (String::new(), count_blockhashes(&entry.path()).unwrap_or(0)));
        rows.push(EpochRow {
            epoch,
            registry,
            account_count,
            tx_count,
            blockhash_count,
        });
    }
    rows.sort_by_key(|row| row.epoch);
    Ok(rows)
}

fn parse_epoch_dir(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("epoch-")?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

fn read_metadata(path: &Path) -> io::Result<HashMap<u32, (String, u64)>> {
    let mut out = HashMap::new();
    let text = fs::read_to_string(path)?;
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Ok(out);
    };
    let headers: Vec<&str> = header.split(',').collect();
    let epoch_i = headers.iter().position(|h| *h == "epoch");
    let tx_i = headers.iter().position(|h| *h == "tx_count");
    let blockhash_i = headers.iter().position(|h| *h == "blockhash_count");
    let (Some(epoch_i), Some(tx_i), Some(blockhash_i)) = (epoch_i, tx_i, blockhash_i) else {
        return Ok(out);
    };
    for line in lines {
        let cols: Vec<&str> = line.split(',').collect();
        if let Some(epoch) = cols.get(epoch_i).and_then(|v| v.parse::<u32>().ok()) {
            let tx = cols.get(tx_i).copied().unwrap_or("").to_string();
            let blockhash = cols
                .get(blockhash_i)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            out.insert(epoch, (tx, blockhash));
        }
    }
    Ok(out)
}

fn count_registry(path: &Path) -> io::Result<u64> {
    let size = path.metadata()?.len();
    if size % KEY_LEN as u64 != 0 {
        return Err(invalid(&format!("{} is not key-aligned", path.display())));
    }
    Ok(size / KEY_LEN as u64)
}

fn count_blockhashes(epoch_dir: &Path) -> io::Result<u64> {
    let path = epoch_dir.join("blockhash_registry.bin");
    if !path.is_file() {
        return Ok(0);
    }
    let size = path.metadata()?.len();
    if size % KEY_LEN as u64 != 0 {
        return Ok(0);
    }
    Ok(size / KEY_LEN as u64)
}

fn bucketize(config: &Config, rows: &[EpochRow]) -> io::Result<()> {
    let done_path = config.work_dir.join("bucketize.done");
    if done_path.exists() {
        return Ok(());
    }
    let bucket_dir = config.work_dir.join("buckets");
    if bucket_dir.exists() {
        fs::remove_dir_all(&bucket_dir)?;
    }
    fs::create_dir_all(&bucket_dir)?;
    let bucket_count = 1usize << config.bucket_bits;
    let mask = bucket_count - 1;
    let mut buffers = vec![Vec::<u8>::new(); bucket_count];
    let flush_limit = 64 * 1024;
    let mut chunk = vec![0u8; 64 * 1024 * 1024];
    for row in rows {
        eprintln!("bucketize epoch={} accounts={}", row.epoch, row.account_count);
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&row.registry)?);
        let epoch_bytes = row.epoch.to_le_bytes();
        loop {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            if n % KEY_LEN != 0 {
                return Err(invalid(&format!("{} read was not key-aligned", row.registry.display())));
            }
            for key in chunk[..n].chunks_exact(KEY_LEN) {
                let bucket = bucket_for_key(key) & mask;
                let buf = &mut buffers[bucket];
                buf.extend_from_slice(key);
                buf.extend_from_slice(&epoch_bytes);
                if buf.len() >= flush_limit {
                    flush_bucket(&bucket_dir, bucket, buf)?;
                }
            }
        }
    }
    for (bucket, buf) in buffers.iter_mut().enumerate() {
        flush_bucket(&bucket_dir, bucket, buf)?;
    }
    fs::write(done_path, b"ok\n")?;
    Ok(())
}

fn bucket_for_key(key: &[u8]) -> usize {
    u32::from_le_bytes([key[0], key[1], key[2], key[3]]) as usize
}

fn flush_bucket(bucket_dir: &Path, bucket: usize, buf: &mut Vec<u8>) -> io::Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let shard = bucket >> 8;
    let dir = bucket_dir.join(format!("{shard:02x}"));
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("bucket-{bucket:04x}.bin"));
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(buf)?;
    buf.clear();
    Ok(())
}

fn reduce_buckets(work_dir: &Path, bucket_bits: u32, max_epoch: u32) -> io::Result<Vec<u64>> {
    let reduced_path = work_dir.join("new_accounts_by_epoch.tsv");
    let bucket_count = 1usize << bucket_bits;
    let mut new_by_epoch = vec![0u64; max_epoch as usize + 1];
    let mut record = vec![0u8; RECORD_LEN * 256 * 1024];
    for bucket in 0..bucket_count {
        let shard = bucket >> 8;
        let path = work_dir
            .join("buckets")
            .join(format!("{shard:02x}"))
            .join(format!("bucket-{bucket:04x}.bin"));
        if !path.is_file() {
            continue;
        }
        let records = path.metadata()?.len() / RECORD_LEN as u64;
        eprintln!("reduce bucket={}/{} records={}", bucket + 1, bucket_count, records);
        let mut first_seen: HashMap<[u8; KEY_LEN], u32> = HashMap::new();
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&path)?);
        let mut carry = Vec::with_capacity(RECORD_LEN);
        loop {
            let n = reader.read(&mut record)?;
            if n == 0 {
                break;
            }
            let mut start = 0;
            if !carry.is_empty() {
                let needed = RECORD_LEN - carry.len();
                if n < needed {
                    carry.extend_from_slice(&record[..n]);
                    continue;
                }
                carry.extend_from_slice(&record[..needed]);
                add_record(&carry, &mut first_seen);
                carry.clear();
                start = needed;
            }
            let usable = start + ((n - start) / RECORD_LEN) * RECORD_LEN;
            for rec in record[start..usable].chunks_exact(RECORD_LEN) {
                add_record(rec, &mut first_seen);
            }
            if usable < n {
                carry.extend_from_slice(&record[usable..n]);
            }
        }
        if !carry.is_empty() {
            return Err(invalid(&format!(
                "{} ended with a partial record",
                path.display()
            )));
        }
        for epoch in first_seen.values().copied() {
            if let Some(slot) = new_by_epoch.get_mut(epoch as usize) {
                *slot += 1;
            }
        }
    }
    let mut writer = BufWriter::new(File::create(&reduced_path)?);
    for (epoch, count) in new_by_epoch.iter().enumerate() {
        if *count != 0 {
            writeln!(writer, "{epoch}\t{count}")?;
        }
    }
    Ok(new_by_epoch)
}

fn add_record(rec: &[u8], first_seen: &mut HashMap<[u8; KEY_LEN], u32>) {
    let mut key = [0u8; KEY_LEN];
    key.copy_from_slice(&rec[..KEY_LEN]);
    let epoch = u32::from_le_bytes([
        rec[KEY_LEN],
        rec[KEY_LEN + 1],
        rec[KEY_LEN + 2],
        rec[KEY_LEN + 3],
    ]);
    first_seen
        .entry(key)
        .and_modify(|old| {
            if epoch < *old {
                *old = epoch;
            }
        })
        .or_insert(epoch);
}

fn read_reduced(work_dir: &Path, max_epoch: u32) -> io::Result<Option<Vec<u64>>> {
    let path = work_dir.join("new_accounts_by_epoch.tsv");
    if !path.is_file() {
        return Ok(None);
    }
    let mut out = vec![0u64; max_epoch as usize + 1];
    for line in fs::read_to_string(path)?.lines() {
        let mut parts = line.split('\t');
        if let (Some(epoch), Some(count)) = (parts.next(), parts.next()) {
            if let (Ok(epoch), Ok(count)) = (epoch.parse::<usize>(), count.parse::<u64>()) {
                if let Some(slot) = out.get_mut(epoch) {
                    *slot = count;
                }
            }
        }
    }
    Ok(Some(out))
}

fn write_csv(path: &Path, rows: &[EpochRow], new_by_epoch: &[u64]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "epoch,first_slot,last_slot,account_count,active_account_count,tx_count,blockhash_count,unique_account_growth,global_unique_accounts,new_account_ratio,active_to_global_unique_ratio,registry_path"
    )?;
    let mut cumulative = 0u64;
    for row in rows {
        let growth = new_by_epoch.get(row.epoch as usize).copied().unwrap_or(0);
        cumulative += growth;
        let new_ratio = if row.account_count == 0 {
            0.0
        } else {
            growth as f64 / row.account_count as f64
        };
        let active_ratio = if cumulative == 0 {
            0.0
        } else {
            row.account_count as f64 / cumulative as f64
        };
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{:.12},{:.12},{}",
            row.epoch,
            row.epoch as u64 * SLOTS_PER_EPOCH,
            row.epoch as u64 * SLOTS_PER_EPOCH + SLOTS_PER_EPOCH - 1,
            row.account_count,
            row.account_count,
            row.tx_count,
            row.blockhash_count,
            growth,
            cumulative,
            new_ratio,
            active_ratio,
            row.registry.display()
        )?;
    }
    Ok(())
}

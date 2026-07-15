use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const KEY_LEN: usize = 32;

#[derive(Debug)]
struct Config {
    registry_root: PathBuf,
    top_k: usize,
    start_epoch: Option<u32>,
    end_epoch: Option<u32>,
    out_csv: Option<PathBuf>,
}

#[derive(Debug)]
struct RegistryTop {
    label: String,
    epoch: Option<u32>,
    path: PathBuf,
    keys: Vec<[u8; KEY_LEN]>,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let rows = iter_registries(&config)?;
    if rows.is_empty() {
        return Err(invalid(&format!(
            "no registry.bin files found under {}",
            config.registry_root.display()
        )));
    }
    print_summary(&config, &rows)?;
    if let Some(path) = &config.out_csv {
        write_csv(path, &rows, config.top_k)?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let args = env::args().collect::<Vec<_>>();
    let mut registry_root = None;
    let mut top_k = 1000usize;
    let mut start_epoch = None;
    let mut end_epoch = None;
    let mut out_csv = None;
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--registry-root" => {
                i += 1;
                registry_root = args.get(i).map(PathBuf::from);
            }
            "--top-k" => {
                i += 1;
                top_k = args
                    .get(i)
                    .ok_or_else(|| invalid("--top-k requires a value"))?
                    .parse()
                    .map_err(|_| invalid("--top-k must be an integer"))?;
            }
            "--start-epoch" => {
                i += 1;
                start_epoch = Some(
                    args.get(i)
                        .ok_or_else(|| invalid("--start-epoch requires a value"))?
                        .parse()
                        .map_err(|_| invalid("--start-epoch must be an integer"))?,
                );
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
            "--out-csv" => {
                i += 1;
                out_csv = args.get(i).map(PathBuf::from);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(invalid(&format!("unknown argument {other}"))),
        }
        i += 1;
    }
    if top_k == 0 {
        return Err(invalid("--top-k must be greater than zero"));
    }
    Ok(Config {
        registry_root: registry_root.ok_or_else(|| invalid("--registry-root is required"))?,
        top_k,
        start_epoch,
        end_epoch,
        out_csv,
    })
}

fn print_usage() {
    eprintln!(
        "usage: rustc -O scripts/registry_topk_overlap.rs -o target/registry_topk_overlap && target/registry_topk_overlap --registry-root <archive-root> [--top-k 1000] [--out-csv path]"
    );
}

fn iter_registries(config: &Config) -> io::Result<Vec<RegistryTop>> {
    let direct = config.registry_root.join("registry.bin");
    if direct.is_file() {
        return Ok(vec![RegistryTop {
            label: config
                .registry_root
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| config.registry_root.display().to_string()),
            epoch: parse_epoch_dir_name(&config.registry_root),
            keys: read_top_keys(&direct, config.top_k)?,
            path: direct,
        }]);
    }

    let mut rows = Vec::new();
    for entry in fs::read_dir(&config.registry_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let epoch = parse_epoch_dir_name(&entry.path());
        if let Some(epoch) = epoch {
            if config.start_epoch.is_some_and(|start| epoch < start)
                || config.end_epoch.is_some_and(|end| epoch > end)
            {
                continue;
            }
        }
        let path = entry.path().join("registry.bin");
        if !path.is_file() {
            continue;
        }
        let label = entry.file_name().to_string_lossy().into_owned();
        rows.push(RegistryTop {
            label,
            epoch,
            keys: read_top_keys(&path, config.top_k)?,
            path,
        });
    }
    rows.sort_by(|left, right| match (left.epoch, right.epoch) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.label.cmp(&right.label),
    });
    Ok(rows)
}

fn parse_epoch_dir_name(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_string_lossy();
    let rest = name.strip_prefix("epoch-")?;
    if rest.is_empty() || !rest.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

fn read_top_keys(path: &Path, top_k: usize) -> io::Result<Vec<[u8; KEY_LEN]>> {
    let mut file = File::open(path)?;
    let mut keys = Vec::with_capacity(top_k);
    for _ in 0..top_k {
        let mut key = [0u8; KEY_LEN];
        match file.read_exact(&mut key) {
            Ok(()) => keys.push(key),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err),
        }
    }
    Ok(keys)
}

fn print_summary(config: &Config, rows: &[RegistryTop]) -> io::Result<()> {
    println!(
        "registries={} top_k={} root={}",
        rows.len(),
        config.top_k,
        config.registry_root.display()
    );
    for row in rows {
        println!(
            "registry={} epoch={} top_keys={} path={}",
            row.label,
            row.epoch
                .map(|epoch| epoch.to_string())
                .unwrap_or_else(|| "-".to_string()),
            row.keys.len(),
            row.path.display()
        );
    }

    if rows.len() < 2 {
        println!("adjacent_pairs=0 average_common=0.00 average_ratio=0.0000");
        println!("all_registry_intersection={}", rows[0].keys.len());
        return Ok(());
    }

    let mut sum = 0u64;
    let mut min = u64::MAX;
    let mut max = 0u64;
    for pair in rows.windows(2) {
        let common = intersection_count(&pair[0].keys, &pair[1].keys) as u64;
        sum += common;
        min = min.min(common);
        max = max.max(common);
        println!(
            "adjacent left={} right={} common={} ratio={:.4}",
            pair[0].label,
            pair[1].label,
            common,
            common as f64 / config.top_k as f64
        );
    }
    let pairs = rows.len() - 1;
    println!(
        "adjacent_pairs={} average_common={:.2} min_common={} max_common={} average_ratio={:.4}",
        pairs,
        sum as f64 / pairs as f64,
        min,
        max,
        sum as f64 / pairs as f64 / config.top_k as f64
    );
    println!("all_registry_intersection={}", all_intersection_count(rows));
    Ok(())
}

fn write_csv(path: &Path, rows: &[RegistryTop], top_k: usize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(
        writer,
        "left_label,right_label,left_epoch,right_epoch,top_k,common,ratio,left_path,right_path"
    )?;
    for pair in rows.windows(2) {
        let common = intersection_count(&pair[0].keys, &pair[1].keys);
        writeln!(
            writer,
            "{},{},{},{},{},{},{:.8},{},{}",
            pair[0].label,
            pair[1].label,
            pair[0]
                .epoch
                .map(|epoch| epoch.to_string())
                .unwrap_or_default(),
            pair[1]
                .epoch
                .map(|epoch| epoch.to_string())
                .unwrap_or_default(),
            top_k,
            common,
            common as f64 / top_k as f64,
            pair[0].path.display(),
            pair[1].path.display()
        )?;
    }
    Ok(())
}

fn intersection_count(left: &[[u8; KEY_LEN]], right: &[[u8; KEY_LEN]]) -> usize {
    let right = right.iter().copied().collect::<HashSet<_>>();
    left.iter().filter(|key| right.contains(*key)).count()
}

fn all_intersection_count(rows: &[RegistryTop]) -> usize {
    let Some(first) = rows.first() else {
        return 0;
    };
    let mut intersection = first.keys.iter().copied().collect::<HashSet<_>>();
    for row in &rows[1..] {
        let keys = row.keys.iter().copied().collect::<HashSet<_>>();
        intersection.retain(|key| keys.contains(key));
    }
    intersection.len()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

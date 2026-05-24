use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::GzDecoder;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

const BUFFER_SIZE: usize = 256 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Fast concurrent diff for saved JSON-RPC response corpora")]
struct Args {
    #[arg(long)]
    left_rows: PathBuf,
    #[arg(long)]
    right_rows: PathBuf,
    #[arg(long, default_value = "left")]
    left_label: String,
    #[arg(long, default_value = "right")]
    right_label: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 0)]
    threads: usize,
    #[arg(long, default_value_t = 500)]
    progress_every: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct CorpusRow {
    slot: u64,
    call: String,
    body_path: PathBuf,
    #[serde(default)]
    body_bytes: Option<usize>,
    #[serde(default)]
    rpc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    slot: u64,
    call: String,
}

#[derive(Debug)]
struct CompareResult {
    key: Key,
    matched: bool,
    diff_path: String,
    diff: String,
    left_body_path: PathBuf,
    right_body_path: PathBuf,
    left_rpc: Option<String>,
    right_rpc: Option<String>,
    compare_error: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(args.threads)
            .build_global()
            .context("configure rayon thread pool")?;
    }

    let started = Instant::now();
    std::fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("create {}", args.out_dir.display()))?;

    let left = load_rows(&args.left_rows)
        .with_context(|| format!("load left rows from {}", args.left_rows.display()))?;
    let right = load_rows(&args.right_rows)
        .with_context(|| format!("load right rows from {}", args.right_rows.display()))?;

    let left_keys = left.keys().cloned().collect::<BTreeSet<_>>();
    let right_keys = right.keys().cloned().collect::<BTreeSet<_>>();
    let common = left_keys
        .intersection(&right_keys)
        .cloned()
        .collect::<Vec<_>>();
    let missing_left = right_keys.difference(&left_keys).count();
    let missing_right = left_keys.difference(&right_keys).count();

    let results = common
        .par_iter()
        .enumerate()
        .map(|(index, key)| {
            if args.progress_every > 0 && (index + 1) % args.progress_every == 0 {
                eprintln!("compared={}/{}", index + 1, common.len());
            }
            compare_one(key, &left[key], &right[key])
        })
        .collect::<Vec<_>>();

    let mut matched = 0usize;
    let mut mismatches = Vec::new();
    let mut diff_paths = BTreeMap::<String, usize>::new();
    let mut compare_errors = BTreeMap::<String, usize>::new();

    for result in results {
        if result.matched {
            matched += 1;
        } else {
            *diff_paths.entry(result.diff_path.clone()).or_default() += 1;
            if let Some(error) = &result.compare_error {
                *compare_errors.entry(error.clone()).or_default() += 1;
            }
            mismatches.push(result);
        }
    }
    mismatches.sort_by(|left, right| left.key.cmp(&right.key));

    let mismatch_path = args.out_dir.join("mismatches.jsonl");
    write_mismatches(&mismatch_path, &mismatches)?;

    let summary = json!({
        "left_label": args.left_label,
        "right_label": args.right_label,
        "common": common.len(),
        "matched": matched,
        "mismatched": mismatches.len(),
        "missing_left": missing_left,
        "missing_right": missing_right,
        "diff_paths": diff_paths,
        "compare_errors": compare_errors,
        "elapsed_s": started.elapsed().as_secs_f64(),
        "threads": rayon::current_num_threads(),
        "artifacts": {
            "mismatches": mismatch_path.display().to_string(),
        },
    });
    let summary_path = args.out_dir.join("summary.json");
    std::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("write {}", summary_path.display()))?;
    println!("{}", serde_json::to_string_pretty(&summary)?);

    Ok(())
}

fn load_rows(path: &Path) -> Result<BTreeMap<Key, CorpusRow>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut rows = BTreeMap::new();
    for (line_index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("read {} line {}", path.display(), line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<CorpusRow>(&line)
            .with_context(|| format!("parse {} line {}", path.display(), line_index + 1))?;
        let key = Key {
            slot: row.slot,
            call: row.call.clone(),
        };
        rows.insert(key, row);
    }
    Ok(rows)
}

fn compare_one(key: &Key, left: &CorpusRow, right: &CorpusRow) -> CompareResult {
    let left_path = resolve_body_path(&left.body_path);
    let right_path = resolve_body_path(&right.body_path);

    let decoded = (|| -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((
            read_decoded_body(&left_path, left.body_bytes)?,
            read_decoded_body(&right_path, right.body_bytes)?,
        ))
    })();

    match decoded {
        Ok((left_bytes, right_bytes)) if left_bytes == right_bytes => CompareResult {
            key: key.clone(),
            matched: true,
            diff_path: String::new(),
            diff: String::new(),
            left_body_path: left_path,
            right_body_path: right_path,
            left_rpc: left.rpc.clone(),
            right_rpc: right.rpc.clone(),
            compare_error: None,
        },
        Ok((left_bytes, right_bytes)) => json_diff_result(
            key,
            left,
            right,
            left_path,
            right_path,
            &left_bytes,
            &right_bytes,
        ),
        Err(err) => CompareResult {
            key: key.clone(),
            matched: false,
            diff_path: "$".to_string(),
            diff: format!("compare error: {err:#}"),
            left_body_path: left_path,
            right_body_path: right_path,
            left_rpc: left.rpc.clone(),
            right_rpc: right.rpc.clone(),
            compare_error: Some(err.root_cause().to_string()),
        },
    }
}

fn json_diff_result(
    key: &Key,
    left: &CorpusRow,
    right: &CorpusRow,
    left_path: PathBuf,
    right_path: PathBuf,
    left_bytes: &[u8],
    right_bytes: &[u8],
) -> CompareResult {
    let result = (|| -> Result<(String, String)> {
        let left_json = serde_json::from_slice::<Value>(left_bytes)
            .with_context(|| format!("parse {}", left_path.display()))?;
        let right_json = serde_json::from_slice::<Value>(right_bytes)
            .with_context(|| format!("parse {}", right_path.display()))?;
        Ok(first_diff(
            &rpc_payload(&left_json),
            &rpc_payload(&right_json),
            "$",
        ))
    })();

    match result {
        Ok((diff_path, diff)) => CompareResult {
            key: key.clone(),
            matched: diff_path.is_empty(),
            diff_path,
            diff,
            left_body_path: left_path,
            right_body_path: right_path,
            left_rpc: left.rpc.clone(),
            right_rpc: right.rpc.clone(),
            compare_error: None,
        },
        Err(err) => CompareResult {
            key: key.clone(),
            matched: false,
            diff_path: "$".to_string(),
            diff: format!("compare error: {err:#}"),
            left_body_path: left_path,
            right_body_path: right_path,
            left_rpc: left.rpc.clone(),
            right_rpc: right.rpc.clone(),
            compare_error: Some(err.root_cause().to_string()),
        },
    }
}

fn resolve_body_path(path: &Path) -> PathBuf {
    if path.exists() {
        return path.to_path_buf();
    }
    let gz_path = PathBuf::from(format!("{}.gz", path.display()));
    if gz_path.exists() {
        return gz_path;
    }
    path.to_path_buf()
}

fn read_decoded_body(path: &Path, expected_len: Option<usize>) -> Result<Vec<u8>> {
    let mut reader = open_decoded_body(path)?;
    let mut out = Vec::with_capacity(expected_len.unwrap_or(0));
    reader
        .read_to_end(&mut out)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(out)
}

fn open_decoded_body(path: &Path) -> Result<Box<dyn Read + Send>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(BUFFER_SIZE, file);
    if path.extension().is_some_and(|extension| extension == "gz") {
        Ok(Box::new(GzDecoder::new(reader)))
    } else {
        Ok(Box::new(reader))
    }
}

fn rpc_payload(value: &Value) -> Value {
    match value {
        Value::Object(map) if map.contains_key("error") => {
            json!({"error": map.get("error").cloned().unwrap_or(Value::Null)})
        }
        Value::Object(map) => map.get("result").cloned().unwrap_or(Value::Null),
        other => other.clone(),
    }
}

fn first_diff(left: &Value, right: &Value, path: &str) -> (String, String) {
    match (left, right) {
        (Value::Object(left_map), Value::Object(right_map)) => {
            let left_keys = left_map.keys().collect::<BTreeSet<_>>();
            let right_keys = right_map.keys().collect::<BTreeSet<_>>();
            if left_keys != right_keys {
                if let Some(key) = right_keys.difference(&left_keys).next() {
                    return (format!("{path}.{key}"), "missing in left".to_string());
                }
                if let Some(key) = left_keys.difference(&right_keys).next() {
                    return (format!("{path}.{key}"), "extra in left".to_string());
                }
            }
            for key in left_keys {
                let left_value = left_map.get(key.as_str()).expect("left key exists");
                let right_value = right_map.get(key.as_str()).expect("right key exists");
                let (diff_path, diff) =
                    first_diff(left_value, right_value, &format!("{path}.{key}"));
                if !diff_path.is_empty() {
                    return (diff_path, diff);
                }
            }
            (String::new(), String::new())
        }
        (Value::Array(left_items), Value::Array(right_items)) => {
            if left_items.len() != right_items.len() {
                return (
                    path.to_string(),
                    format!(
                        "list length left={} right={}",
                        left_items.len(),
                        right_items.len()
                    ),
                );
            }
            for (index, (left_item, right_item)) in left_items.iter().zip(right_items).enumerate() {
                let (diff_path, diff) =
                    first_diff(left_item, right_item, &format!("{path}[{index}]"));
                if !diff_path.is_empty() {
                    return (diff_path, diff);
                }
            }
            (String::new(), String::new())
        }
        _ if left == right => (String::new(), String::new()),
        _ if value_kind(left) != value_kind(right) => (
            path.to_string(),
            format!("type left={} right={}", value_kind(left), value_kind(right)),
        ),
        _ => (
            path.to_string(),
            truncate(&format!("left={left:?} right={right:?}"), 500),
        ),
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut out = value.chars().take(max_chars + 1).collect::<String>();
    if out.chars().count() <= max_chars {
        out
    } else {
        out.truncate(
            out.char_indices()
                .nth(max_chars)
                .map_or(out.len(), |(index, _)| index),
        );
        out.push_str("...");
        out
    }
}

fn write_mismatches(path: &Path, mismatches: &[CompareResult]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    for mismatch in mismatches {
        serde_json::to_writer(
            &mut file,
            &json!({
                "slot": mismatch.key.slot,
                "call": mismatch.key.call,
                "diff_path": mismatch.diff_path,
                "diff": mismatch.diff,
                "left_body_path": mismatch.left_body_path.display().to_string(),
                "right_body_path": mismatch.right_body_path.display().to_string(),
                "left_rpc": mismatch.left_rpc,
                "right_rpc": mismatch.right_rpc,
                "compare_error": mismatch.compare_error,
            }),
        )?;
        writeln!(file)?;
    }
    Ok(())
}

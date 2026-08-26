use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    CarBlockReader,
    compact_index::decode_offset_and_size,
    node::{Node, decode_node},
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, SlotRange, SlotRangeWithPreviousBlockhash,
        decode_slot_range_entry, epoch_for_slot, slot_in_epoch,
        write_slot_ranges_raw as write_slot_ranges_raw_entries,
        write_slot_ranges_v2_raw as write_slot_ranges_v2_raw_entries,
    },
};
use of_slot_ranges::{
    AmbiguousBlockCidError, AsyncCompactIndex, BlockSlotCandidate, BuildSlotRangesConfig,
    LocalFileRangeReader, RangeReader, build_block_slot_candidates_from_slot_index,
    build_slot_ranges_from_indexes, build_slot_ranges_from_indexes_with_block_slots,
    decode_block_slot_from_car_frame, decode_car_header_total_size,
};
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_RANGE, HeaderValue, RANGE};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::future::{Ready, ready};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_BUCKET_SIZE: usize = 64 * 1024 * 1024;

// We only need up to 10 bytes to decode a uvarint64.
// We read a few extra bytes (16) to be robust against short reads and proxies,
// but we still only *use* the first 10 for decoding.
const CAR_HEADER_PREFIX_READ_LEN: usize = 16;
const OLD_FAITHFUL_CAR_HEADER_TOTAL_SIZE: u64 = 59;
const ZSTD_LONG_WINDOW_LOG_MAX: u32 = 31;
const DEFAULT_BASE_URL: &str = "https://files.old-faithful.net";

#[derive(Parser, Debug)]
#[command(name = "of-slot-ranges")]
struct Cli {
    /// First epoch to process, inclusive.
    #[arg(long)]
    start_epoch: u64,

    /// Last epoch to process, inclusive.
    #[arg(long)]
    end_epoch: u64,

    /// Root directory containing per-epoch compact index files.
    #[arg(long = "indexes-dir", default_value = "indexes", alias = "index-dir")]
    indexes_dir: PathBuf,

    /// Optional directory containing local plain `.car` files.
    /// If a CAR is missing, only the remote CAR header is fetched in memory.
    #[arg(long = "cars-dir", alias = "car-dir")]
    cars_dir: Option<PathBuf>,

    /// Base URL for remote Old Faithful compact indexes and plain CAR files.
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,

    /// Root containing epoch-N/blockhash_registry.bin.
    ///
    /// Ordered block slots come from `--slot-list-dir` when set. Otherwise,
    /// they come from the Old Faithful slot-to-CID index under indexes-dir.
    /// Archive V2 files are not read in this mode.
    #[arg(long = "blockhash-dir", conflicts_with = "archive_v2_dir")]
    blockhash_dir: Option<PathBuf>,

    /// Local directory containing `epoch-N.slots.txt` or `N.slots.txt`.
    ///
    /// With `--blockhash-dir`, this uses the Old Faithful slot list as the
    /// ordered block-membership source. It requires existing raw range files
    /// and does not read compact indexes, CID indexes, CAR files, or Archive V2.
    #[arg(
        long = "slot-list-dir",
        requires = "blockhash_dir",
        conflicts_with_all = [
            "archive_v2_dir",
            "indexes_dir",
            "cars_dir",
            "base_url",
            "raw_only",
            "overwrite"
        ]
    )]
    slot_list_dir: Option<PathBuf>,

    /// Optional legacy root containing Archive V2 per-epoch sidecars.
    ///
    /// When present, v2 slot ranges are rebuilt from `blockhash_registry.bin`,
    /// using the 12-byte slot range output for CAR offsets. If
    /// `archive-v2-blocks.index` exists it is used as the slot/order source.
    /// Prefer blockhash-dir for builds based on Old Faithful compact indexes.
    #[arg(
        long = "archive-v2-dir",
        alias = "block-index-dir",
        conflicts_with = "blockhash_dir"
    )]
    archive_v2_dir: Option<PathBuf>,

    /// Base58 previous blockhash for the first epoch in this run.
    ///
    /// Epoch 0 needs this value when its registry does not contain the
    /// mainnet genesis hash as a prefix. Later epochs normally read the last
    /// hash from the preceding epoch registry.
    #[arg(long = "seed-previous-blockhash")]
    seed_previous_blockhash: Option<String>,

    /// Directory where `epoch-*-slot-ranges.raw` files are written.
    #[arg(long = "output-dir", default_value = "out", alias = "out-dir")]
    output_dir: PathBuf,

    /// Rebuild output files even when they already exist.
    #[arg(long)]
    overwrite: bool,

    /// Rebuild only the v2 output file when it already exists.
    #[arg(long)]
    overwrite_v2: bool,

    /// Write only `epoch-N-slot-ranges.raw`.
    ///
    /// This skips v2 output and avoids reading local CAR bodies when `--cars-dir`
    /// is provided only to decode the CAR header length.
    #[arg(long)]
    raw_only: bool,
}

fn main() -> Result<()> {
    run(Cli::parse())
}

fn run(cli: Cli) -> Result<()> {
    let configured_seed = cli
        .seed_previous_blockhash
        .as_deref()
        .map(decode_base58_hash)
        .transpose()?;
    fs::create_dir_all(&cli.output_dir)?;

    let http = Client::builder()
        .user_agent("of-slot-ranges/1.0")
        .build()
        .context("build reqwest client")?;

    let mut previous_epoch_last_blockhash: Option<[u8; 32]> = None;
    let allow_archive_root_fallback = cli.blockhash_dir.is_none()
        && cli.archive_v2_dir.is_some()
        && cli.start_epoch == cli.end_epoch;

    for epoch in cli.start_epoch..=cli.end_epoch {
        let out_path = cli
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges.raw"));
        let out_v2_path = cli
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges-v2.raw"));

        if out_v2_path.exists() && !cli.overwrite && !cli.overwrite_v2 {
            eprintln!("skip epoch={epoch} exists: {}", out_v2_path.display());
            if let Some(last) = last_blockhash_from_registry(
                epoch,
                cli.blockhash_dir
                    .as_deref()
                    .or(cli.archive_v2_dir.as_deref()),
                allow_archive_root_fallback,
            )? {
                previous_epoch_last_blockhash = Some(last);
            } else {
                previous_epoch_last_blockhash = None;
            }
            continue;
        }

        let mut index_block_slots = None;

        if out_path.exists() && !cli.overwrite {
            eprintln!("epoch={epoch}: keep existing {}", out_path.display());
        } else {
            if let Some(slot_list_root) = cli.slot_list_dir.as_deref() {
                return Err(anyhow!(
                    "epoch={epoch}: --slot-list-dir {} requires the existing raw range file {}; it cannot build or overwrite raw ranges",
                    slot_list_root.display(),
                    out_path.display()
                ));
            }
            let build_result = (|| -> Result<Vec<u64>> {
                let epoch_dir = cli.indexes_dir.join(epoch.to_string());
                let cid_path = epoch_dir.join(format!("epoch-{epoch}.cid"));
                let epoch_cid = fs::read_to_string(&cid_path)
                    .with_context(|| format!("read {}", cid_path.display()))?;
                let epoch_cid = epoch_cid.trim().to_string();

                let slot_idx_name = format!("epoch-{epoch}-{epoch_cid}-mainnet-slot-to-cid.index");
                let cid_idx_name =
                    format!("epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");

                let slot_idx_path = epoch_dir.join(&slot_idx_name);
                let cid_idx_path = epoch_dir.join(&cid_idx_name);

                eprintln!(
                    "epoch={epoch}: open slot-to-cid.index: {}",
                    slot_idx_path.display()
                );
                eprintln!(
                    "epoch={epoch}: open cid-to-offset-and-size.index: {}",
                    cid_idx_path.display()
                );

                let slot_reader = LocalFileRangeReader::open(&slot_idx_path)?;
                let cid_reader = LocalFileRangeReader::open(&cid_idx_path)?;
                let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
                    slot_reader,
                    slot_idx_path.display().to_string(),
                ))?;
                let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(
                    cid_reader,
                    cid_idx_path.display().to_string(),
                ))?;

                // IMPORTANT: We do NOT download CAR files.
                // Prefer local .car/.car.zst headers, then a tiny remote prefix, then a logged
                // Old Faithful-specific fallback.
                let (car_hdr, default_log) =
                    car_header_total_size(&http, epoch, cli.cars_dir.as_deref(), &cli.base_url)?;
                if let Some(default_log) = default_log {
                    append_header_default_log(&cli.output_dir, epoch, car_hdr, &default_log)?;
                }
                eprintln!("epoch={epoch}: car_header_size={car_hdr}");

                let t0 = std::time::Instant::now();
                let build_config = BuildSlotRangesConfig {
                    max_bucket_payload_bytes: MAX_BUCKET_SIZE,
                    allow_node_read_fallback: true,
                };
                let first_build = futures::executor::block_on(build_slot_ranges_from_indexes(
                    epoch,
                    car_hdr,
                    &mut slot_index,
                    &mut cid_index,
                    build_config,
                ));
                let output = match first_build {
                    Ok(output) => output,
                    Err(error) if error.downcast_ref::<AmbiguousBlockCidError>().is_some() => {
                        eprintln!(
                            "epoch={epoch}: verify duplicate slot-to-CID candidates from exact CAR frames"
                        );
                        let candidates = futures::executor::block_on(
                            build_block_slot_candidates_from_slot_index(
                                epoch,
                                &mut slot_index,
                                build_config,
                            ),
                        )?;
                        let resolved_slots =
                            futures::executor::block_on(resolve_block_slot_candidates_from_car(
                                epoch,
                                &candidates.candidates,
                                &mut cid_index,
                                &http,
                                cli.cars_dir.as_deref(),
                                &cli.base_url,
                            ))?;
                        futures::executor::block_on(
                            build_slot_ranges_from_indexes_with_block_slots(
                                epoch,
                                car_hdr,
                                &mut slot_index,
                                &mut cid_index,
                                build_config,
                                &resolved_slots,
                            ),
                        )?
                    }
                    Err(error) => return Err(error),
                };
                eprintln!(
                    "epoch={epoch}: done build ranges in {:.2}s present_slots={} slot_bucket_read={} MiB cid_bucket_read={} MiB max_slot_bucket={} MiB max_cid_bucket={} MiB slot_node_fallbacks={} cid_node_fallbacks={}",
                    t0.elapsed().as_secs_f64(),
                    output.stats.present_slots,
                    output.stats.slot_bucket_payload_bytes_read / (1024 * 1024),
                    output.stats.cid_bucket_payload_bytes_read / (1024 * 1024),
                    output.stats.max_slot_bucket_payload_bytes / (1024 * 1024),
                    output.stats.max_cid_bucket_payload_bytes / (1024 * 1024),
                    output.stats.slot_node_read_fallbacks,
                    output.stats.cid_node_read_fallbacks,
                );

                eprintln!("epoch={epoch}: write {}", out_path.display());
                write_slot_ranges_raw_file(&out_path, &output.ranges)?;
                Ok(output.block_slots)
            })();

            match build_result {
                Ok(block_slots) => index_block_slots = Some(block_slots),
                Err(err) if cli.raw_only => {
                    eprintln!("epoch={epoch}: SKIP raw-only slot ranges after error: {err:#}");
                    if out_path.exists() {
                        if let Err(remove_err) = fs::remove_file(&out_path) {
                            eprintln!(
                                "epoch={epoch}: warning: failed to remove stale raw output {}: {remove_err:#}",
                                out_path.display()
                            );
                        }
                    }
                    if let Err(log_err) = append_epoch_skip_log(&cli.output_dir, epoch, &err) {
                        eprintln!("epoch={epoch}: warning: failed to append skip log: {log_err:#}");
                    }
                    previous_epoch_last_blockhash = None;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        if cli.raw_only {
            previous_epoch_last_blockhash = None;
            eprintln!("epoch={epoch}: raw-only, skip slot ranges v2");
        } else if out_v2_path.exists() && !cli.overwrite && !cli.overwrite_v2 {
            eprintln!("epoch={epoch}: keep existing {}", out_v2_path.display());
            if let Some(last) = last_blockhash_from_registry(
                epoch,
                cli.blockhash_dir
                    .as_deref()
                    .or(cli.archive_v2_dir.as_deref()),
                allow_archive_root_fallback,
            )? {
                previous_epoch_last_blockhash = Some(last);
            } else {
                previous_epoch_last_blockhash = None;
            }
        } else if let Some(blockhash_root) = cli.blockhash_dir.as_deref() {
            let epoch_dir = find_archive_v2_blockhash_dir(blockhash_root, epoch, false)
                .ok_or_else(|| {
                    anyhow!(
                        "epoch={epoch}: blockhash registry directory not found under {}",
                        blockhash_root.display()
                    )
                })?;
            let raw_ranges = read_slot_ranges_raw_file(&out_path)
                .with_context(|| format!("read {}", out_path.display()))?;
            let block_slots = match index_block_slots.take() {
                Some(block_slots) => block_slots,
                None => match cli.slot_list_dir.as_deref() {
                    Some(slot_list_root) => {
                        read_block_slots_from_old_faithful_slot_list(epoch, slot_list_root)?
                    }
                    None => read_block_slots_from_old_faithful_index(
                        epoch,
                        &cli.indexes_dir,
                        &http,
                        cli.cars_dir.as_deref(),
                        &cli.base_url,
                    )?,
                },
            };
            let run_seed = (epoch == cli.start_epoch)
                .then_some(configured_seed)
                .flatten();
            let initial_previous_blockhash = match previous_epoch_last_blockhash {
                Some(hash) => Some(hash),
                None if epoch > 0 => {
                    last_blockhash_from_registry(epoch - 1, Some(blockhash_root), false)?
                        .or(run_seed)
                }
                None => run_seed,
            };
            eprintln!(
                "epoch={epoch}: build slot ranges v2 from {} and registry in {}",
                if cli.slot_list_dir.is_some() {
                    "Old Faithful slots.txt membership"
                } else {
                    "Old Faithful slot-to-CID index"
                },
                epoch_dir.display()
            );
            let v2 = build_slot_ranges_v2_from_blockhash_registry_sidecar(
                &epoch_dir,
                epoch,
                &raw_ranges,
                Some(&block_slots),
                initial_previous_blockhash,
            )?;
            previous_epoch_last_blockhash = v2.last_blockhash;
            eprintln!("epoch={epoch}: write {}", out_v2_path.display());
            write_slot_ranges_v2_raw_file(&out_v2_path, &v2.ranges)?;
        } else if let Some(archive_v2_root) = cli.archive_v2_dir.as_deref() {
            let epoch_dir =
                find_archive_v2_blockhash_dir(archive_v2_root, epoch, allow_archive_root_fallback)
                    .ok_or_else(|| {
                        anyhow!(
                            "epoch={epoch}: Archive V2 blockhash sidecar dir not found under {}",
                            archive_v2_root.display()
                        )
                    })?;
            eprintln!(
                "epoch={epoch}: build slot ranges v2 from Archive V2 sidecars in {}",
                epoch_dir.display()
            );
            let raw_ranges = read_slot_ranges_raw_file(&out_path)
                .with_context(|| format!("read {}", out_path.display()))?;
            let run_seed = (epoch == cli.start_epoch)
                .then_some(configured_seed)
                .flatten();
            let initial_previous_blockhash = match previous_epoch_last_blockhash {
                Some(hash) => Some(hash),
                None if epoch > 0 => {
                    last_blockhash_from_registry(epoch - 1, Some(archive_v2_root), false)?
                        .or(run_seed)
                }
                None => run_seed,
            };
            let v2 = build_slot_ranges_v2_from_archive_v2_sidecars(
                &epoch_dir,
                epoch,
                &raw_ranges,
                index_block_slots.as_deref(),
                initial_previous_blockhash,
            )?;
            previous_epoch_last_blockhash = v2.last_blockhash;
            eprintln!("epoch={epoch}: write {}", out_v2_path.display());
            write_slot_ranges_v2_raw_file(&out_v2_path, &v2.ranges)?;
        } else if let Some(local_car_path) = find_local_car(epoch, cli.cars_dir.as_deref()) {
            eprintln!(
                "epoch={epoch}: build slot ranges v2 with previous blockhash from {}",
                local_car_path.display()
            );
            let v2 = build_slot_ranges_v2_from_local_car(
                &local_car_path,
                epoch,
                previous_epoch_last_blockhash.or_else(|| {
                    (epoch == cli.start_epoch)
                        .then_some(configured_seed)
                        .flatten()
                }),
            )?;
            previous_epoch_last_blockhash = v2.last_blockhash;
            eprintln!("epoch={epoch}: write {}", out_v2_path.display());
            write_slot_ranges_v2_raw_file(&out_v2_path, &v2.ranges)?;
        } else {
            previous_epoch_last_blockhash = None;
            eprintln!(
                "epoch={epoch}: skip slot ranges v2 (requires local plain epoch-{epoch}.car via --cars-dir)"
            );
        }

        eprintln!("epoch={epoch}: done");
    }

    Ok(())
}

fn read_block_slots_from_old_faithful_slot_list(
    epoch: u64,
    slot_list_dir: &Path,
) -> Result<Vec<u64>> {
    let candidates = [
        slot_list_dir.join(format!("epoch-{epoch}.slots.txt")),
        slot_list_dir.join(format!("{epoch}.slots.txt")),
    ];
    let existing = candidates
        .into_iter()
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    let path = match existing.as_slice() {
        [path] => path,
        [] => {
            return Err(anyhow!(
                "epoch={epoch}: missing epoch-{epoch}.slots.txt or {epoch}.slots.txt under {}",
                slot_list_dir.display()
            ));
        }
        paths => {
            return Err(anyhow!(
                "epoch={epoch}: multiple Old Faithful slot lists found: {}",
                paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read Old Faithful slot list {}", path.display()))?;
    let block_slots = decode_old_faithful_slot_list(path, epoch, &contents)?;
    eprintln!(
        "epoch={epoch}: ordered block slots={} from {}",
        block_slots.len(),
        path.display()
    );
    Ok(block_slots)
}

fn decode_old_faithful_slot_list(path: &Path, epoch: u64, contents: &str) -> Result<Vec<u64>> {
    let mut raw_lines = contents.split('\n').collect::<Vec<_>>();
    if raw_lines.last() == Some(&"") {
        raw_lines.pop();
    }
    if raw_lines.is_empty() {
        return Err(anyhow!("{} has no slot lines", path.display()));
    }

    let mut slots = Vec::with_capacity(raw_lines.len());
    for (line_index, raw_line) in raw_lines.into_iter().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() {
            return Err(anyhow!("{} line {line_number} is blank", path.display()));
        }
        if !line.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(anyhow!(
                "{} line {line_number} is not a decimal u64: {line:?}",
                path.display()
            ));
        }
        slots.push(line.parse::<u64>().with_context(|| {
            format!(
                "parse decimal slot on line {line_number} of {}",
                path.display()
            )
        })?);
    }

    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    let current_slots = if epoch == 0 {
        slots.as_slice()
    } else {
        let predecessor = slots[0];
        let expected_predecessor = epoch_start - 1;
        if predecessor != expected_predecessor {
            return Err(anyhow!(
                "{} line 1 predecessor slot is {predecessor}, expected epoch boundary slot {expected_predecessor}",
                path.display()
            ));
        }
        &slots[1..]
    };
    if current_slots.is_empty() {
        return Err(anyhow!(
            "{} has no current-epoch block slots for epoch {epoch}",
            path.display()
        ));
    }

    let mut previous = None;
    for (position, slot) in current_slots.iter().copied().enumerate() {
        if !(epoch_start..epoch_end).contains(&slot) {
            return Err(anyhow!(
                "{} current slot {slot} at position {position} is outside epoch {epoch} range {epoch_start}..{epoch_end}",
                path.display()
            ));
        }
        if previous.is_some_and(|previous_slot| slot <= previous_slot) {
            return Err(anyhow!(
                "{} current slots are not strictly increasing at position {position}: {slot} follows {}",
                path.display(),
                previous.unwrap()
            ));
        }
        previous = Some(slot);
    }
    Ok(current_slots.to_vec())
}

fn read_block_slots_from_old_faithful_index(
    epoch: u64,
    indexes_dir: &Path,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<Vec<u64>> {
    let epoch_dir = indexes_dir.join(epoch.to_string());
    let cid_path = epoch_dir.join(format!("epoch-{epoch}.cid"));
    let epoch_cid =
        fs::read_to_string(&cid_path).with_context(|| format!("read {}", cid_path.display()))?;
    let slot_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-slot-to-cid.index",
        epoch_cid.trim()
    ));
    let cid_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-cid-to-offset-and-size.index",
        epoch_cid.trim()
    ));
    eprintln!(
        "epoch={epoch}: read ordered block slots from {}",
        slot_index_path.display()
    );
    let reader = LocalFileRangeReader::open(&slot_index_path)?;
    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        reader,
        slot_index_path.display().to_string(),
    ))?;
    let output = futures::executor::block_on(build_block_slot_candidates_from_slot_index(
        epoch,
        &mut slot_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes: MAX_BUCKET_SIZE,
            allow_node_read_fallback: true,
        },
    ))?;
    let block_slots = if has_duplicate_block_cids(&output.candidates) {
        let (reader, source) = if cid_index_path.is_file() {
            (
                IndexRangeReader::Local(LocalFileRangeReader::open(&cid_index_path)?),
                cid_index_path.display().to_string(),
            )
        } else {
            let file_name = cid_index_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("CID index path has no UTF-8 file name"))?;
            let url = format!("{}/{epoch}/{file_name}", base_url.trim_end_matches('/'));
            (
                IndexRangeReader::Http(HttpRangeReader::new(http.clone(), url.clone())),
                url,
            )
        };
        let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(reader, &source))
            .with_context(|| format!("open {source}"))?;
        futures::executor::block_on(resolve_block_slot_candidates_from_car(
            epoch,
            &output.candidates,
            &mut cid_index,
            http,
            cars_dir,
            base_url,
        ))?
    } else {
        let mut slots = output
            .candidates
            .iter()
            .map(|candidate| candidate.slot)
            .collect::<Vec<_>>();
        slots.sort_unstable();
        slots
    };
    eprintln!(
        "epoch={epoch}: ordered unique block slots={} slot_index_present_slots={}",
        block_slots.len(),
        output.stats.present_slots
    );
    Ok(block_slots)
}

fn has_duplicate_block_cids(candidates: &[BlockSlotCandidate]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(candidates.len());
    candidates
        .iter()
        .any(|candidate| !seen.insert(candidate.cid))
}

async fn resolve_block_slot_candidates_from_car<C>(
    epoch: u64,
    candidates: &[BlockSlotCandidate],
    cid_index: &mut AsyncCompactIndex<C>,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<Vec<u64>>
where
    C: RangeReader,
{
    if cid_index.value_size() != 9 {
        return Err(anyhow!(
            "{} has value_size {}, expected 9",
            cid_index.source(),
            cid_index.value_size()
        ));
    }
    let mut groups: HashMap<[u8; 36], Vec<&BlockSlotCandidate>> = HashMap::new();
    for candidate in candidates {
        groups.entry(candidate.cid).or_default().push(candidate);
    }

    let mut selected = Vec::with_capacity(groups.len());
    for group in groups.values_mut() {
        group.sort_unstable_by_key(|candidate| candidate.slot);
        if group.len() == 1 {
            selected.push(group[0].slot);
            continue;
        }

        let mut value = [0u8; 9];
        if !cid_index
            .lookup_into_node_reads(&group[0].cid, &mut value)
            .await?
        {
            return Err(anyhow!(
                "CID-to-offset index has no exact entry for duplicate candidate slots {}",
                display_candidate_slots(group)
            ));
        }
        let (offset, size) = decode_offset_and_size(&value)?;
        let frame = read_exact_car_range(
            http,
            epoch,
            cars_dir,
            base_url,
            offset,
            usize::try_from(size).context("CAR frame size exceeds address space")?,
        )?;
        let decoded_slot = decode_block_slot_from_car_frame(&group[0].cid, &frame)
            .context("verify exact CID-index CAR frame")?;
        if group
            .iter()
            .filter(|candidate| candidate.slot == decoded_slot)
            .count()
            != 1
        {
            return Err(anyhow!(
                "exact CID-index CAR frame decodes block slot {decoded_slot}, which is not exactly one of candidate slots {}",
                display_candidate_slots(group)
            ));
        }
        eprintln!(
            "epoch={epoch}: resolved duplicate CID candidate slots {} to slot {decoded_slot} from the exact CAR frame",
            display_candidate_slots(group)
        );
        selected.push(decoded_slot);
    }
    selected.sort_unstable();
    Ok(selected)
}

fn display_candidate_slots(candidates: &[&BlockSlotCandidate]) -> String {
    candidates
        .iter()
        .map(|candidate| candidate.slot.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

enum IndexRangeReader {
    Local(LocalFileRangeReader),
    Http(HttpRangeReader),
}

impl RangeReader for IndexRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        match self {
            Self::Local(reader) => reader.read_exact_at(offset, out),
            Self::Http(reader) => reader.read_exact_at(offset, out),
        }
    }
}

struct HttpRangeReader {
    client: Client,
    url: String,
}

impl HttpRangeReader {
    fn new(client: Client, url: String) -> Self {
        Self { client, url }
    }
}

impl RangeReader for HttpRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        ready(
            http_range_get_exact(&self.client, &self.url, offset, out.len()).and_then(|bytes| {
                out.copy_from_slice(&bytes);
                Ok(())
            }),
        )
    }
}

/* ---------------- output writer ---------------- */

fn write_slot_ranges_raw_file(path: &Path, ranges: &[SlotRange]) -> Result<()> {
    if ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("ranges wrong length"));
    }

    let mut f = std::io::BufWriter::with_capacity(256 * 1024, std::fs::File::create(path)?);
    write_slot_ranges_raw_entries(&mut f, ranges)?;
    f.flush()?;
    Ok(())
}

fn read_slot_ranges_raw_file(path: &Path) -> Result<Vec<SlotRange>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let expected_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if bytes.len() != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len}",
            path.display(),
            bytes.len()
        ));
    }

    bytes
        .chunks_exact(SLOT_RANGE_ENTRY_SIZE)
        .map(|chunk| decode_slot_range_entry(chunk).map_err(Into::into))
        .collect()
}

fn write_slot_ranges_v2_raw_file(
    path: &Path,
    ranges: &[SlotRangeWithPreviousBlockhash],
) -> Result<()> {
    if ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("v2 ranges wrong length"));
    }

    let mut f = std::io::BufWriter::with_capacity(256 * 1024, std::fs::File::create(path)?);
    write_slot_ranges_v2_raw_entries(&mut f, ranges)?;
    f.flush()?;
    Ok(())
}

#[derive(Debug)]
struct SlotRangesV2Build {
    ranges: Vec<SlotRangeWithPreviousBlockhash>,
    last_blockhash: Option<[u8; 32]>,
}

const ARCHIVE_V2_BLOCK_INDEX_FILE: &str = "archive-v2-blocks.index";
const ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const ARCHIVE_V2_LEGACY_INDEX_MAGIC: &[u8; 8] = b"BZV2IDX1";
const ARCHIVE_V2_LEGACY_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;
const ARCHIVE_V2_LEGACY_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
const ARCHIVE_V2_HOT_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_HOT_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4;
const MAINNET_GENESIS_HASH_BASE58: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

#[derive(Debug, Clone, Copy)]
struct ArchiveV2BlockIndexRow {
    block_id: u32,
    slot: u64,
}

fn build_slot_ranges_v2_from_archive_v2_sidecars(
    epoch_dir: &Path,
    epoch: u64,
    raw_ranges: &[SlotRange],
    index_block_slots: Option<&[u64]>,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    if raw_ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("raw ranges wrong length"));
    }
    if epoch > 0 {
        require_epoch_seed(epoch, initial_previous_blockhash)?;
    }

    let block_index_path = epoch_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    if !block_index_path.is_file() {
        return build_slot_ranges_v2_from_blockhash_registry_sidecar(
            epoch_dir,
            epoch,
            raw_ranges,
            index_block_slots,
            initial_previous_blockhash,
        );
    }

    let mut rows = read_archive_v2_block_index_rows(&block_index_path)?;
    rows.sort_by_key(|row| row.block_id);
    for (position, row) in rows.iter().enumerate() {
        let expected_block_id = u32::try_from(position).context("block index exceeds u32")?;
        if row.block_id != expected_block_id {
            return Err(anyhow!(
                "Archive V2 block index row {position} has block_id {}, expected {expected_block_id}",
                row.block_id
            ));
        }
    }
    let row_slots = rows.iter().map(|row| row.slot).collect::<Vec<_>>();
    validate_ordered_block_slots(epoch, raw_ranges, &row_slots)?;
    let blockhashes = read_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?;
    let alignment =
        blockhash_registry_alignment(epoch, rows.len(), &blockhashes, initial_previous_blockhash)?;

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut previous_blockhash = alignment.initial_previous_blockhash;
    let mut last_blockhash = None;
    let mut present_slots = 0u64;

    for row in rows {
        let hash_index = row
            .block_id
            .checked_add(u32::try_from(alignment.registry_offset)?)
            .ok_or_else(|| anyhow!("blockhash id overflow for block_id {}", row.block_id))?
            as usize;
        let blockhash = *blockhashes.get(hash_index).ok_or_else(|| {
            anyhow!(
                "missing blockhash id {hash_index} for block_id {} slot {}",
                row.block_id,
                row.slot
            )
        })?;

        let idx =
            usize::try_from(slot_in_epoch(row.slot)).context("slot-in-epoch exceeds usize")?;
        let range = raw_ranges[idx];
        if range.is_empty() {
            eprintln!(
                "epoch={epoch}: warning slot {} is present in Archive V2 block index but empty in raw slot ranges",
                row.slot
            );
        }
        ranges[idx] = SlotRangeWithPreviousBlockhash {
            range,
            previous_blockhash,
        };
        previous_blockhash = blockhash;
        last_blockhash = Some(blockhash);
        present_slots += 1;
    }

    eprintln!(
        "epoch={epoch}: built v2 from Archive V2 block index present_slots={present_slots} blockhash_id_offset={}",
        alignment.registry_offset
    );

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

fn build_slot_ranges_v2_from_blockhash_registry_sidecar(
    epoch_dir: &Path,
    epoch: u64,
    raw_ranges: &[SlotRange],
    index_block_slots: Option<&[u64]>,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    if raw_ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("raw ranges wrong length"));
    }
    if epoch > 0 {
        require_epoch_seed(epoch, initial_previous_blockhash)?;
    }
    let blockhashes = read_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?;
    let raw_present_slots = raw_ranges
        .iter()
        .copied()
        .filter(|range| !range.is_empty())
        .count();
    let raw_block_slots: Vec<u64>;
    let block_slots = if let Some(slots) = index_block_slots {
        slots
    } else {
        raw_block_slots = raw_ranges
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot_in_epoch, range)| {
                (!range.is_empty()).then(|| epoch * SLOTS_PER_EPOCH + slot_in_epoch as u64)
            })
            .collect();
        &raw_block_slots
    };
    validate_ordered_block_slots(epoch, raw_ranges, block_slots)?;
    let alignment = blockhash_registry_alignment(
        epoch,
        block_slots.len(),
        &blockhashes,
        initial_previous_blockhash,
    )?;

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut previous_blockhash = alignment.initial_previous_blockhash;
    let mut last_blockhash = None;
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;

    for (block_i, slot) in block_slots.iter().copied().enumerate() {
        let slot_in_epoch =
            usize::try_from(slot_in_epoch(slot)).context("slot-in-epoch exceeds usize")?;
        let range = raw_ranges[slot_in_epoch];

        let hash_index = block_i
            .checked_add(alignment.registry_offset)
            .ok_or_else(|| anyhow!("blockhash id overflow for block index {block_i}"))?;
        let blockhash = *blockhashes.get(hash_index).ok_or_else(|| {
            anyhow!("missing blockhash id {hash_index} for block index {block_i}")
        })?;
        ranges[slot_in_epoch] = SlotRangeWithPreviousBlockhash {
            range,
            previous_blockhash,
        };
        previous_blockhash = blockhash;
        last_blockhash = Some(blockhash);
    }

    eprintln!(
        "epoch={epoch}: built v2 from ordered block slots and blockhash registry block_slots={} raw_present_slots={} blockhash_id_offset={} first_slot={} last_slot={}",
        block_slots.len(),
        raw_present_slots,
        alignment.registry_offset,
        block_slots.first().copied().unwrap_or(epoch_start),
        block_slots.last().copied().unwrap_or(epoch_start),
    );

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

fn validate_ordered_block_slots(
    epoch: u64,
    raw_ranges: &[SlotRange],
    block_slots: &[u64],
) -> Result<()> {
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    let mut member = vec![false; SLOTS_PER_EPOCH as usize];
    let mut previous_slot = None;
    for (position, slot) in block_slots.iter().copied().enumerate() {
        if !(epoch_start..epoch_end).contains(&slot) {
            return Err(anyhow!(
                "ordered block slot {slot} at position {position} is outside epoch {epoch} range {epoch_start}..{epoch_end}"
            ));
        }
        if previous_slot.is_some_and(|previous| slot <= previous) {
            return Err(anyhow!(
                "ordered block slots are not strictly increasing at position {position}: {slot} follows {}",
                previous_slot.unwrap()
            ));
        }
        let slot_in_epoch =
            usize::try_from(slot - epoch_start).context("slot-in-epoch exceeds address space")?;
        member[slot_in_epoch] = true;
        previous_slot = Some(slot);
    }
    for (slot_in_epoch, range) in raw_ranges.iter().enumerate() {
        if !range.is_empty() && !member[slot_in_epoch] {
            return Err(anyhow!(
                "raw-present slot {} is absent from the ordered block slot list",
                epoch_start + slot_in_epoch as u64
            ));
        }
    }
    Ok(())
}

fn last_blockhash_from_registry(
    epoch: u64,
    archive_v2_root: Option<&Path>,
    allow_root_fallback: bool,
) -> Result<Option<[u8; 32]>> {
    let Some(root) = archive_v2_root else {
        return Ok(None);
    };
    let Some(epoch_dir) = find_archive_v2_blockhash_dir(root, epoch, allow_root_fallback) else {
        return Ok(None);
    };
    read_last_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).map(Some)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct BlockhashRegistryAlignment {
    registry_offset: usize,
    initial_previous_blockhash: [u8; 32],
}

fn blockhash_registry_alignment(
    epoch: u64,
    row_count: usize,
    blockhashes: &[[u8; 32]],
    explicit_seed: Option<[u8; 32]>,
) -> Result<BlockhashRegistryAlignment> {
    let blockhash_count = blockhashes.len();
    for (record, blockhash) in blockhashes.iter().enumerate() {
        if *blockhash == [0; 32] {
            return Err(anyhow!("blockhash registry record {record} is zero"));
        }
    }
    let registry_offset = blockhash_count.checked_sub(row_count).ok_or_else(|| {
        anyhow!(
            "blockhash registry has {blockhash_count} hashes but ordered block slot list has {row_count} rows"
        )
    })?;

    if epoch > 0 {
        if registry_offset != 0 {
            return Err(anyhow!(
                "epoch {epoch} blockhash registry has {blockhash_count} hashes for {row_count} ordered block slots; only epoch 0 can contain a genesis prefix"
            ));
        }
        return Ok(BlockhashRegistryAlignment {
            registry_offset,
            initial_previous_blockhash: require_epoch_seed(epoch, explicit_seed)?,
        });
    }

    match registry_offset {
        0 => Ok(BlockhashRegistryAlignment {
            registry_offset,
            initial_previous_blockhash: require_epoch_seed(epoch, explicit_seed)?,
        }),
        1 => {
            let registry_genesis = blockhashes[0];
            let expected_genesis = explicit_seed.unwrap_or(mainnet_genesis_hash()?);
            if registry_genesis != expected_genesis {
                return Err(anyhow!(
                    "epoch 0 blockhash registry genesis prefix does not match {}",
                    if explicit_seed.is_some() {
                        "--seed-previous-blockhash"
                    } else {
                        "the mainnet genesis hash"
                    }
                ));
            }
            Ok(BlockhashRegistryAlignment {
                registry_offset,
                initial_previous_blockhash: registry_genesis,
            })
        }
        extra => Err(anyhow!(
            "epoch 0 blockhash registry has {blockhash_count} hashes for {row_count} ordered block slots (offset {extra}); expected one genesis-prefixed hash or equal length with --seed-previous-blockhash"
        )),
    }
}

fn mainnet_genesis_hash() -> Result<[u8; 32]> {
    decode_base58_hash(MAINNET_GENESIS_HASH_BASE58)
}

fn decode_base58_hash(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode base58 blockhash {value}"))?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow!("blockhash must be 32 bytes, got {}", bytes.len()))
}

fn read_blockhash_registry(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() % 32 != 0 {
        return Err(anyhow!(
            "{} has invalid length {} (not divisible by 32)",
            path.display(),
            bytes.len()
        ));
    }
    bytes
        .chunks_exact(32)
        .enumerate()
        .map(|(record, chunk)| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            if hash == [0; 32] {
                return Err(anyhow!(
                    "{} blockhash registry record {record} is zero",
                    path.display()
                ));
            }
            Ok(hash)
        })
        .collect()
}

fn read_last_blockhash_registry(path: &Path) -> Result<[u8; 32]> {
    read_blockhash_registry(path)?
        .last()
        .copied()
        .ok_or_else(|| anyhow!("{} blockhash registry is empty", path.display()))
}

fn read_archive_v2_block_index_rows(path: &Path) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() < 8 {
        return Err(anyhow!(
            "{} is shorter than Archive V2 index magic",
            path.display()
        ));
    }

    match &bytes[..8] {
        magic if magic == ARCHIVE_V2_LEGACY_INDEX_MAGIC => {
            read_archive_v2_legacy_block_index_rows(path, &bytes)
        }
        magic if magic == ARCHIVE_V2_HOT_INDEX_MAGIC => {
            read_archive_v2_hot_block_index_rows(path, &bytes)
        }
        _ => Err(anyhow!(
            "{} is not an Archive V2 block index",
            path.display()
        )),
    }
}

fn read_archive_v2_legacy_block_index_rows(
    path: &Path,
    bytes: &[u8],
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN {
        return Err(anyhow!(
            "{} is shorter than Archive V2 legacy index header",
            path.display()
        ));
    }
    let header = &bytes[..ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN];
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_LEGACY_INDEX_VERSION {
        return Err(anyhow!(
            "{} has unsupported Archive V2 legacy block index version {version}",
            path.display()
        ));
    }
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let expected_len = ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN as u64
        + row_count * ARCHIVE_V2_LEGACY_INDEX_ROW_LEN as u64;
    if bytes.len() as u64 != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len} for {row_count} rows",
            path.display(),
            bytes.len()
        ));
    }

    let mut rows = Vec::with_capacity(usize::try_from(row_count)?);
    for chunk in
        bytes[ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN..].chunks_exact(ARCHIVE_V2_LEGACY_INDEX_ROW_LEN)
    {
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    Ok(rows)
}

fn read_archive_v2_hot_block_index_rows(
    path: &Path,
    bytes: &[u8],
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < ARCHIVE_V2_HOT_INDEX_HEADER_LEN {
        return Err(anyhow!(
            "{} is shorter than Archive V2 hot-block index header",
            path.display()
        ));
    }
    let header = &bytes[..ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_HOT_INDEX_VERSION {
        return Err(anyhow!(
            "{} has unsupported Archive V2 hot-block index version {version}",
            path.display()
        ));
    }
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let expected_len =
        ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64 + row_count * ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64;
    if bytes.len() as u64 != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len} for {row_count} hot rows",
            path.display(),
            bytes.len()
        ));
    }

    let mut rows = Vec::with_capacity(usize::try_from(row_count)?);
    for chunk in bytes[ARCHIVE_V2_HOT_INDEX_HEADER_LEN..].chunks_exact(ARCHIVE_V2_HOT_INDEX_ROW_LEN)
    {
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    Ok(rows)
}

fn find_archive_v2_blockhash_dir(
    root: &Path,
    epoch: u64,
    allow_root_fallback: bool,
) -> Option<PathBuf> {
    let mut candidates = vec![
        root.join(format!("epoch-{epoch}")),
        root.join(epoch.to_string()),
    ];
    if allow_root_fallback {
        candidates.push(root.to_path_buf());
    }
    candidates
        .into_iter()
        .find(|path| path.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE).is_file())
}

fn build_slot_ranges_v2_from_local_car(
    path: &Path,
    epoch: u64,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    let initial_previous_blockhash = require_epoch_seed(epoch, initial_previous_blockhash)?;
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = CarBlockReader::with_capacity(file, 16 << 20);
    reader
        .skip_header()
        .with_context(|| format!("read CAR header from {}", path.display()))?;

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut scratch = Vec::with_capacity(256 << 10);
    let mut pending_start: Option<u64> = None;
    let mut pending_blockhash = [0u8; 32];
    let mut previous_blockhash = initial_previous_blockhash;
    let mut last_blockhash = None;

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .with_context(|| format!("read CAR entry from {}", path.display()))?
    {
        pending_start.get_or_insert(entry.location.car_offset);

        let node = decode_node(entry.payload).with_context(|| {
            format!(
                "decode CAR node at entry {} offset {}",
                entry.location.entry_index, entry.location.car_offset
            )
        })?;

        match node {
            Node::Entry(entry_node) => {
                if entry_node.hash.len() != 32 {
                    return Err(anyhow!(
                        "entry hash length {} at CAR offset {}",
                        entry_node.hash.len(),
                        entry.location.car_offset
                    ));
                }
                pending_blockhash.copy_from_slice(entry_node.hash);
            }
            Node::Block(block) => {
                let start = pending_start.unwrap_or(entry.location.car_offset);
                let end = entry
                    .location
                    .car_offset
                    .checked_add(entry.total_len as u64)
                    .ok_or_else(|| anyhow!("CAR range end overflow"))?;

                if epoch_for_slot(block.slot) == epoch {
                    let len = u32::try_from(end.saturating_sub(start))
                        .context("CAR block range exceeds u32")?;
                    let idx = usize::try_from(slot_in_epoch(block.slot))
                        .context("slot-in-epoch exceeds usize")?;
                    ranges[idx] = SlotRangeWithPreviousBlockhash {
                        range: SlotRange { offset: start, len },
                        previous_blockhash,
                    };
                }

                previous_blockhash = pending_blockhash;
                last_blockhash = Some(pending_blockhash);
                pending_start = None;
                pending_blockhash = [0; 32];
            }
            Node::Transaction(_)
            | Node::Rewards(_)
            | Node::DataFrame(_)
            | Node::Subset(_)
            | Node::Epoch(_) => {}
        }
    }

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

fn require_epoch_seed(
    epoch: u64,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<[u8; 32]> {
    let hash = initial_previous_blockhash.ok_or_else(|| {
        if epoch == 0 {
            anyhow!(
                "epoch 0 v2 index requires a genesis-prefixed blockhash registry or --seed-previous-blockhash"
            )
        } else {
            anyhow!(
                "epoch {epoch} v2 index requires the last blockhash from epoch {}; provide the predecessor blockhash registry",
                epoch - 1
            )
        }
    })?;
    if hash == [0; 32] {
        if epoch == 0 {
            return Err(anyhow!("epoch 0 v2 index genesis seed is zero"));
        }
        return Err(anyhow!(
            "epoch {epoch} v2 index predecessor blockhash from epoch {} is zero",
            epoch - 1
        ));
    }
    Ok(hash)
}

/* ---------------- CAR header size ---------------- */

fn car_header_total_size(
    http: &Client,
    epoch: u64,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<(u64, Option<String>)> {
    if let Some(local_car_path) = find_local_car(epoch, cars_dir) {
        eprintln!(
            "epoch={epoch}: read CAR header from local file: {}",
            local_car_path.display()
        );
        return car_header_total_size_from_local_car(&local_car_path).map(|size| (size, None));
    }

    match car_header_total_size_from_remote_car(http, epoch, base_url) {
        Ok(size) => Ok((size, None)),
        Err(err) => {
            let log = format!(
                "url={}/{epoch}/epoch-{epoch}.car error={err:#}",
                base_url.trim_end_matches('/')
            );
            eprintln!(
                "epoch={epoch}: warning: remote CAR header fetch failed ({err:#}); using Old Faithful default car_header_size={OLD_FAITHFUL_CAR_HEADER_TOTAL_SIZE}"
            );
            Ok((OLD_FAITHFUL_CAR_HEADER_TOTAL_SIZE, Some(log)))
        }
    }
}

fn find_local_car(epoch: u64, cars_dir: Option<&Path>) -> Option<PathBuf> {
    let cars_dir = cars_dir?;
    let file_name = format!("epoch-{epoch}.car");
    let zst_file_name = format!("epoch-{epoch}.car.zst");
    let candidates = [
        cars_dir.join(&file_name),
        cars_dir.join(epoch.to_string()).join(&file_name),
        cars_dir.join(&zst_file_name),
        cars_dir.join(epoch.to_string()).join(&zst_file_name),
    ];

    candidates.into_iter().find(|path| path.is_file())
}

fn find_local_plain_car(epoch: u64, cars_dir: Option<&Path>) -> Option<PathBuf> {
    let root = cars_dir?;
    let name = format!("epoch-{epoch}.car");
    [
        root.join(&name),
        root.join(epoch.to_string()).join(&name),
        root.join(format!("epoch-{epoch}")).join(&name),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

fn read_exact_car_range(
    http: &Client,
    epoch: u64,
    cars_dir: Option<&Path>,
    base_url: &str,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    if len == 0 {
        return Err(anyhow!("cannot read an empty CAR range"));
    }
    if let Some(path) = find_local_plain_car(epoch, cars_dir) {
        let mut file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek {} to {offset}", path.display()))?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes)
            .with_context(|| format!("read {len} bytes at {offset} from {}", path.display()))?;
        return Ok(bytes);
    }
    let url = format!(
        "{}/{epoch}/epoch-{epoch}.car",
        base_url.trim_end_matches('/')
    );
    http_range_get_exact(http, &url, offset, len)
}

fn car_header_total_size_from_local_car(path: &Path) -> Result<u64> {
    if is_zstd_path(path) {
        return car_header_total_size_from_local_zstd_car(path);
    }

    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut prefix = [0u8; CAR_HEADER_PREFIX_READ_LEN];
    let prefix_len = file
        .read(&mut prefix)
        .with_context(|| format!("read {}", path.display()))?;
    let source = path.display().to_string();

    decode_car_header_total_size(&prefix[..prefix_len], &source)
}

fn car_header_total_size_from_local_zstd_car(path: &Path) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut dctx = zstd::zstd_safe::DCtx::create();
    dctx.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
        ZSTD_LONG_WINDOW_LOG_MAX,
    ))
    .map_err(|code| {
        anyhow!(
            "set zstd windowLogMax={ZSTD_LONG_WINDOW_LOG_MAX} for {}: {}",
            path.display(),
            zstd::zstd_safe::get_error_name(code)
        )
    })?;
    let mut decoder = zstd::Decoder::with_context(reader, &mut dctx);
    let mut prefix = [0u8; CAR_HEADER_PREFIX_READ_LEN];
    let prefix_len = decoder
        .read(&mut prefix)
        .with_context(|| format!("read zstd {}", path.display()))?;
    let source = path.display().to_string();

    decode_car_header_total_size(&prefix[..prefix_len], &source)
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| matches!(ext, "zst" | "zstd"))
        .unwrap_or(false)
}

fn append_header_default_log(output_dir: &Path, epoch: u64, size: u64, reason: &str) -> Result<()> {
    let path = output_dir.join("car-header-default-59.log");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    writeln!(file, "epoch={epoch}\tcar_header_size={size}\t{reason}")
        .with_context(|| format!("write {}", path.display()))
}

fn append_epoch_skip_log(output_dir: &Path, epoch: u64, err: &anyhow::Error) -> Result<()> {
    let path = output_dir.join("slot-range-skipped-epochs.log");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    writeln!(file, "epoch={epoch}\terror={err:#}")
        .with_context(|| format!("write {}", path.display()))
}

fn car_header_total_size_from_remote_car(http: &Client, epoch: u64, base_url: &str) -> Result<u64> {
    // NOTE: This uses the *plain* .car, because you cannot get the uncompressed CAR header
    // out of a .car.zst with a simple Range request.
    let url_car = format!(
        "{}/{epoch}/epoch-{epoch}.car",
        base_url.trim_end_matches('/')
    );

    eprintln!(
        "epoch={epoch}: range fetch remote CAR prefix ({} bytes): {}",
        CAR_HEADER_PREFIX_READ_LEN, url_car
    );
    let prefix = http_range_get_exact(http, &url_car, 0, CAR_HEADER_PREFIX_READ_LEN)
        .with_context(|| format!("range GET {url_car}"))?;

    decode_car_header_total_size(&prefix, &url_car)
}

fn http_range_get_exact(http: &Client, url: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        return Err(anyhow!("cannot request an empty HTTP range from {url}"));
    }
    let end = offset
        .checked_add(u64::try_from(len).context("HTTP range length exceeds u64")? - 1)
        .ok_or_else(|| anyhow!("HTTP range end overflow for {url}"))?;
    let response = http
        .get(url)
        .header(
            RANGE,
            HeaderValue::from_str(&format!("bytes={offset}-{end}"))?,
        )
        .send()
        .with_context(|| format!("range GET {url}"))?;
    if response.status().as_u16() != 206 {
        return Err(anyhow!(
            "range GET {url} returned HTTP {}, expected 206",
            response.status().as_u16()
        ));
    }
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| anyhow!("range GET {url} has no Content-Range header"))?
        .to_str()
        .with_context(|| format!("decode Content-Range from {url}"))?;
    let expected_prefix = format!("bytes {offset}-{end}/");
    let total = content_range.strip_prefix(&expected_prefix).ok_or_else(|| {
        anyhow!(
            "range GET {url} returned Content-Range {content_range:?}, expected {expected_prefix}..."
        )
    })?;
    if total.is_empty() {
        return Err(anyhow!(
            "range GET {url} returned an empty Content-Range total"
        ));
    }
    let bytes = response
        .bytes()
        .with_context(|| format!("read range response from {url}"))?;
    if bytes.len() != len {
        return Err(anyhow!(
            "range GET {url} returned {} bytes, expected {len}",
            bytes.len()
        ));
    }
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_test_registry(root: &Path, epoch: u64, blockhashes: &[[u8; 32]]) {
        let epoch_dir = root.join(format!("epoch-{epoch}"));
        fs::create_dir_all(&epoch_dir).expect("create blockhash epoch directory");
        let bytes = blockhashes
            .iter()
            .flat_map(|blockhash| blockhash.iter().copied())
            .collect::<Vec<_>>();
        fs::write(epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), bytes)
            .expect("write blockhash registry");
    }

    #[test]
    fn slot_list_build_reuses_raw_and_keeps_empty_member_in_hash_chain() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let output_dir = temporary.path().join("slot-index");
        let slot_list_dir = temporary.path().join("slot-lists");
        let blockhash_dir = temporary.path().join("blockhash-registry");
        fs::create_dir_all(&output_dir).expect("create output directory");
        fs::create_dir_all(&slot_list_dir).expect("create slot-list directory");

        let epoch = 4;
        let epoch_start = epoch * SLOTS_PER_EPOCH;
        let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        raw_ranges[0] = SlotRange {
            offset: 59,
            len: 100,
        };
        raw_ranges[2] = SlotRange {
            offset: 159,
            len: 120,
        };
        let raw_path = output_dir.join(format!("epoch-{epoch}-slot-ranges.raw"));
        write_slot_ranges_raw_file(&raw_path, &raw_ranges).expect("write existing raw ranges");
        fs::write(
            slot_list_dir.join(format!("{epoch}.slots.txt")),
            format!(
                "{}\n{}\n{}\n{}\n",
                epoch_start - 1,
                epoch_start,
                epoch_start + 1,
                epoch_start + 2
            ),
        )
        .expect("write Old Faithful slot list");
        let predecessor = [6; 32];
        let blockhashes = [[7; 32], [8; 32], [9; 32]];
        write_test_registry(&blockhash_dir, epoch - 1, &[predecessor]);
        write_test_registry(&blockhash_dir, epoch, &blockhashes);

        let missing_inputs = temporary.path().join("must-not-be-read");
        run(Cli {
            start_epoch: epoch,
            end_epoch: epoch,
            indexes_dir: missing_inputs.clone(),
            cars_dir: Some(missing_inputs.clone()),
            base_url: "http://127.0.0.1:1".to_owned(),
            blockhash_dir: Some(blockhash_dir),
            slot_list_dir: Some(slot_list_dir),
            archive_v2_dir: None,
            seed_previous_blockhash: None,
            output_dir: output_dir.clone(),
            overwrite: false,
            overwrite_v2: true,
            raw_only: false,
        })
        .expect("direct slot-list build must not read compact indexes, CID indexes, or CARs");

        let v2_path = output_dir.join(format!("epoch-{epoch}-slot-ranges-v2.raw"));
        let v2 = fs::read(v2_path).expect("read v2 output");
        assert_eq!(
            v2.len(),
            SLOTS_PER_EPOCH as usize * of_car_reader::slot_ranges::SLOT_RANGE_V2_ENTRY_SIZE
        );
        let entry = |slot_in_epoch: usize| {
            let start = slot_in_epoch * of_car_reader::slot_ranges::SLOT_RANGE_V2_ENTRY_SIZE;
            of_car_reader::slot_ranges::decode_slot_range_v2_entry(
                &v2[start..start + of_car_reader::slot_ranges::SLOT_RANGE_V2_ENTRY_SIZE],
            )
            .expect("decode v2 row")
        };
        assert_eq!(entry(0).range, raw_ranges[0]);
        assert_eq!(entry(0).previous_blockhash, predecessor);
        assert!(entry(1).range.is_empty());
        assert_eq!(entry(1).previous_blockhash, blockhashes[0]);
        assert_eq!(entry(2).range, raw_ranges[2]);
        assert_eq!(entry(2).previous_blockhash, blockhashes[1]);
        assert_eq!(entry(3), SlotRangeWithPreviousBlockhash::EMPTY);
    }

    #[test]
    fn slot_list_names_and_format_are_strict() {
        let epoch = 4;
        let epoch_start = epoch * SLOTS_PER_EPOCH;
        let valid = format!(
            "{}\n{}\n{}\n",
            epoch_start - 1,
            epoch_start,
            epoch_start + 2
        );
        assert_eq!(
            decode_old_faithful_slot_list(Path::new("fixture.slots.txt"), epoch, &valid)
                .expect("decode valid list"),
            vec![epoch_start, epoch_start + 2]
        );
        assert_eq!(
            decode_old_faithful_slot_list(Path::new("0.slots.txt"), 0, "0\n2\n")
                .expect("epoch zero has no predecessor line"),
            vec![0, 2]
        );

        for (contents, expected) in [
            ("", "no slot lines"),
            ("1727999\n\n1728000\n", "line 2 is blank"),
            ("1727999\nslot\n", "not a decimal u64"),
            ("1728000\n1728001\n", "predecessor slot"),
            ("1727999\n1728001\n1728000\n", "not strictly increasing"),
            ("1727999\n2160000\n", "outside epoch 4"),
        ] {
            let error = decode_old_faithful_slot_list(Path::new("bad.slots.txt"), epoch, contents)
                .expect_err("malformed slot list must fail");
            assert!(
                error.to_string().contains(expected),
                "error {error:#} does not contain {expected:?}"
            );
        }

        for name in ["epoch-4.slots.txt", "4.slots.txt"] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            fs::write(temporary.path().join(name), &valid).expect("write named slot list");
            assert_eq!(
                read_block_slots_from_old_faithful_slot_list(epoch, temporary.path())
                    .expect("read supported local name"),
                vec![epoch_start, epoch_start + 2]
            );
        }
    }

    #[test]
    fn independent_registry_stitch_uses_ordered_old_faithful_slots() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let epoch_dir = temporary.path().join("epoch-4");
        fs::create_dir_all(&epoch_dir).expect("create epoch directory");
        let predecessor_last_hash = [6; 32];
        let blockhashes = [[7; 32], [8; 32], [9; 32]];
        let registry = blockhashes.concat();
        fs::write(epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), registry)
            .expect("write blockhash registry");
        fs::write(epoch_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE), b"not used")
            .expect("write ignored Archive V2 index");

        let epoch_start = 4 * SLOTS_PER_EPOCH;
        let ordered_block_slots = [epoch_start, epoch_start + 1, epoch_start + 2];
        let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        raw_ranges[0] = SlotRange {
            offset: 59,
            len: 100,
        };
        raw_ranges[2] = SlotRange {
            offset: 159,
            len: 120,
        };

        let output = build_slot_ranges_v2_from_blockhash_registry_sidecar(
            &epoch_dir,
            4,
            &raw_ranges,
            Some(&ordered_block_slots),
            Some(predecessor_last_hash),
        )
        .expect("stitch from Old Faithful slot order");

        assert_eq!(output.ranges[0].previous_blockhash, predecessor_last_hash);
        assert!(output.ranges[1].range.is_empty());
        assert_eq!(output.ranges[1].previous_blockhash, blockhashes[0]);
        assert_eq!(output.ranges[2].previous_blockhash, blockhashes[1]);
        assert_eq!(output.last_blockhash, Some(blockhashes[2]));
    }

    #[test]
    fn independent_stitch_rejects_invalid_ordered_slots() {
        let epoch = 4;
        let epoch_start = epoch * SLOTS_PER_EPOCH;
        let raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];

        for slots in [
            vec![epoch_start, epoch_start],
            vec![epoch_start + 1, epoch_start],
            vec![epoch_start - 1],
            vec![epoch_start + SLOTS_PER_EPOCH],
        ] {
            let error = validate_ordered_block_slots(epoch, &raw_ranges, &slots)
                .expect_err("invalid ordered slots must fail");
            assert!(
                error.to_string().contains("strictly increasing")
                    || error.to_string().contains("outside epoch")
            );
        }
    }

    #[test]
    fn independent_stitch_rejects_raw_slot_missing_from_ordered_slots() {
        let epoch = 4;
        let epoch_start = epoch * SLOTS_PER_EPOCH;
        let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        raw_ranges[2] = SlotRange { offset: 59, len: 1 };
        let error = validate_ordered_block_slots(epoch, &raw_ranges, &[epoch_start])
            .expect_err("raw-present slot must be in the compact slot list");
        assert!(error.to_string().contains("raw-present slot"));
    }

    #[test]
    fn builder_rejects_zero_blockhash_registry_record() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
        let mut registry = Vec::new();
        registry.extend_from_slice(&[7; 32]);
        registry.extend_from_slice(&[0; 32]);
        fs::write(&path, registry).expect("write registry");
        let error = read_blockhash_registry(&path).expect_err("zero hash must fail");
        assert!(error.to_string().contains("record 1 is zero"));
    }

    #[test]
    fn registry_alignment_limits_genesis_prefix_to_epoch_zero() {
        let genesis = mainnet_genesis_hash().expect("mainnet genesis hash");
        let epoch_zero = blockhash_registry_alignment(0, 1, &[genesis, [7; 32]], None)
            .expect("epoch zero genesis prefix");
        assert_eq!(epoch_zero.registry_offset, 1);
        assert_eq!(epoch_zero.initial_previous_blockhash, genesis);

        let wrong_seed = blockhash_registry_alignment(0, 1, &[genesis, [7; 32]], Some([9; 32]))
            .expect_err("a present genesis prefix must match the explicit seed");
        assert!(wrong_seed.to_string().contains("--seed-previous-blockhash"));

        let missing_seed = blockhash_registry_alignment(0, 1, &[[7; 32]], None)
            .expect_err("unprefixed epoch zero needs a seed");
        assert!(
            missing_seed
                .to_string()
                .contains("--seed-previous-blockhash")
        );

        let later_prefix = blockhash_registry_alignment(1, 1, &[[7; 32], [8; 32]], Some([6; 32]))
            .expect_err("later epoch cannot have a registry prefix");
        assert!(later_prefix.to_string().contains("only epoch 0"));
    }

    #[test]
    fn normal_epoch_zero_accepts_unprefixed_registry_with_explicit_seed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::write(
            temporary.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [7; 32],
        )
        .expect("write unprefixed registry");
        let raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        let seed = mainnet_genesis_hash().expect("mainnet genesis hash");
        let output = build_slot_ranges_v2_from_blockhash_registry_sidecar(
            temporary.path(),
            0,
            &raw_ranges,
            Some(&[0]),
            Some(seed),
        )
        .expect("explicit seed makes an unprefixed epoch-zero registry safe");
        assert_eq!(output.ranges[0].previous_blockhash, seed);
        assert_eq!(output.last_blockhash, Some([7; 32]));
    }

    #[test]
    fn normal_registry_lookup_never_reuses_root_file() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        fs::write(
            temporary.path().join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE),
            [7; 32],
        )
        .expect("write root registry");
        assert!(find_archive_v2_blockhash_dir(temporary.path(), 4, false).is_none());
        assert_eq!(
            find_archive_v2_blockhash_dir(temporary.path(), 4, true),
            Some(temporary.path().to_path_buf())
        );
    }

    #[test]
    fn blockhash_dir_is_an_explicit_independent_mode() {
        let cli = Cli::try_parse_from([
            "of-slot-ranges",
            "--start-epoch",
            "4",
            "--end-epoch",
            "4",
            "--blockhash-dir",
            "/registry",
        ])
        .expect("independent blockhash mode");
        assert_eq!(cli.blockhash_dir, Some(PathBuf::from("/registry")));
        assert!(cli.slot_list_dir.is_none());
        assert!(cli.archive_v2_dir.is_none());

        let direct = Cli::try_parse_from([
            "of-slot-ranges",
            "--start-epoch",
            "4",
            "--end-epoch",
            "4",
            "--blockhash-dir",
            "/registry",
            "--slot-list-dir",
            "/slot-lists",
            "--overwrite-v2",
        ])
        .expect("direct slot-list mode");
        assert_eq!(direct.slot_list_dir, Some(PathBuf::from("/slot-lists")));

        assert!(
            Cli::try_parse_from([
                "of-slot-ranges",
                "--start-epoch",
                "4",
                "--end-epoch",
                "4",
                "--blockhash-dir",
                "/registry",
                "--archive-v2-dir",
                "/archive",
            ])
            .is_err()
        );
        for conflicting_args in [
            vec!["--slot-list-dir", "/slot-lists"],
            vec![
                "--blockhash-dir",
                "/registry",
                "--slot-list-dir",
                "/slot-lists",
                "--overwrite",
            ],
            vec![
                "--blockhash-dir",
                "/registry",
                "--slot-list-dir",
                "/slot-lists",
                "--raw-only",
            ],
            vec![
                "--blockhash-dir",
                "/registry",
                "--slot-list-dir",
                "/slot-lists",
                "--indexes-dir",
                "/indexes",
            ],
            vec![
                "--blockhash-dir",
                "/registry",
                "--slot-list-dir",
                "/slot-lists",
                "--cars-dir",
                "/cars",
            ],
        ] {
            let mut args = vec!["of-slot-ranges", "--start-epoch", "4", "--end-epoch", "4"];
            args.extend(conflicting_args);
            assert!(
                Cli::try_parse_from(args).is_err(),
                "conflicting slot-list arguments must fail"
            );
        }
    }

    #[test]
    fn reused_raw_v2_build_reads_slots_from_old_faithful_index() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let epoch_dir = temporary.path().join("4");
        fs::create_dir_all(&epoch_dir).expect("create epoch directory");
        fs::write(epoch_dir.join("epoch-4.cid"), "fixture-cid\n").expect("write CID file");
        let expected_slot = 4 * SLOTS_PER_EPOCH + 1;
        let slot_index = tiny_compact_index(&expected_slot.to_le_bytes(), &[1; 36]);
        fs::write(
            epoch_dir.join("epoch-4-fixture-cid-mainnet-slot-to-cid.index"),
            slot_index,
        )
        .expect("write slot-to-CID index");

        assert_eq!(
            read_block_slots_from_old_faithful_index(
                4,
                temporary.path(),
                &Client::new(),
                None,
                DEFAULT_BASE_URL,
            )
            .expect("read ordered block slots"),
            vec![expected_slot]
        );
    }

    #[test]
    fn duplicate_cid_is_resolved_from_exact_car_frame() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let payload = test_block_payload(7);
        let cid = *of_car_reader::reconstruct::Cid36::compute(&payload).car_bytes();
        let frame = test_car_frame(cid, &payload);
        let offset = 59u64;
        let mut car = vec![0; offset as usize];
        car.extend_from_slice(&frame);
        fs::write(temporary.path().join("epoch-0.car"), car).expect("write CAR fixture");

        let cid_index_path = temporary.path().join("cid.index");
        fs::write(
            &cid_index_path,
            tiny_compact_index(&cid, &offset_size_value(offset, frame.len() as u32)),
        )
        .expect("write CID index");
        let reader = LocalFileRangeReader::open(&cid_index_path).expect("open CID index file");
        let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(
            reader,
            cid_index_path.display().to_string(),
        ))
        .expect("open CID index");
        let candidates = [
            BlockSlotCandidate { slot: 7, cid },
            BlockSlotCandidate { slot: 8, cid },
        ];
        let slots = futures::executor::block_on(resolve_block_slot_candidates_from_car(
            0,
            &candidates,
            &mut cid_index,
            &Client::new(),
            Some(temporary.path()),
            DEFAULT_BASE_URL,
        ))
        .expect("resolve duplicate CID");
        assert_eq!(slots, vec![7]);
    }

    fn test_block_payload(slot: u8) -> Vec<u8> {
        assert!(slot < 24);
        vec![0x86, 0x02, slot, 0x80, 0x80, 0x83, 0xf6, 0xf6, 0xf6, 0xf6]
    }

    fn test_car_frame(cid: [u8; 36], payload: &[u8]) -> Vec<u8> {
        let entry_len = cid.len() + payload.len();
        assert!(entry_len < 128);
        let mut frame = Vec::with_capacity(entry_len + 1);
        frame.push(entry_len as u8);
        frame.extend_from_slice(&cid);
        frame.extend_from_slice(payload);
        frame
    }

    fn offset_size_value(offset: u64, size: u32) -> [u8; 9] {
        assert!(offset < (1u64 << 48));
        assert!(size < (1u32 << 24));
        let mut value = [0; 9];
        value[..6].copy_from_slice(&offset.to_le_bytes()[..6]);
        value[6..].copy_from_slice(&size.to_le_bytes()[..3]);
        value
    }

    fn tiny_compact_index(key: &[u8], value: &[u8]) -> Vec<u8> {
        use of_car_reader::compact_index::{
            BUCKET_HEADER_SIZE, COMPACT_INDEX_FIXED_HEADER_SIZE, COMPACT_INDEX_MAGIC,
            truncate_entry_hash,
        };

        let hash_domain = 11u32;
        let hash_len = 8u8;
        let header_len = 13u32;
        let bucket_count = 1u32;
        let data_offset = COMPACT_INDEX_FIXED_HEADER_SIZE + BUCKET_HEADER_SIZE;
        let target = truncate_entry_hash(hash_domain, key, hash_len as usize);
        let mut out = Vec::new();
        out.extend_from_slice(COMPACT_INDEX_MAGIC);
        out.extend_from_slice(&header_len.to_le_bytes());
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(&bucket_count.to_le_bytes());
        out.push(1);
        let mut bucket_header = [0u8; BUCKET_HEADER_SIZE];
        bucket_header[0..4].copy_from_slice(&hash_domain.to_le_bytes());
        bucket_header[4..8].copy_from_slice(&1u32.to_le_bytes());
        bucket_header[8] = hash_len;
        bucket_header[10..16].copy_from_slice(&(data_offset as u64).to_le_bytes()[..6]);
        out.extend_from_slice(&bucket_header);
        out.extend_from_slice(&target.to_le_bytes());
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn sidecar_stitch_uses_predecessor_last_hash_before_current_first_hash() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let epoch_dir = temporary.path().join("epoch-1");
        fs::create_dir_all(&epoch_dir).expect("create epoch directory");

        let predecessor_last_hash = [7; 32];
        let current_first_hash = [8; 32];
        let current_second_hash = [9; 32];
        let mut registry = Vec::new();
        registry.extend_from_slice(&current_first_hash);
        registry.extend_from_slice(&current_second_hash);
        fs::write(epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), registry)
            .expect("write blockhash registry");

        let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        raw_ranges[0] = SlotRange {
            offset: 59,
            len: 100,
        };
        raw_ranges[2] = SlotRange {
            offset: 159,
            len: 120,
        };

        let output = build_slot_ranges_v2_from_archive_v2_sidecars(
            &epoch_dir,
            1,
            &raw_ranges,
            None,
            Some(predecessor_last_hash),
        )
        .expect("stitch v2 sidecar");

        assert_eq!(output.ranges[0].previous_blockhash, predecessor_last_hash);
        assert_ne!(output.ranges[0].previous_blockhash, current_first_hash);
        assert_eq!(output.ranges[2].previous_blockhash, current_first_hash);
        assert_eq!(output.last_blockhash, Some(current_second_hash));
    }

    #[test]
    fn sidecar_stitch_rejects_missing_non_genesis_seed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        let error = build_slot_ranges_v2_from_archive_v2_sidecars(
            temporary.path(),
            1,
            &raw_ranges,
            None,
            None,
        )
        .expect_err("missing predecessor seed must fail");
        assert!(error.to_string().contains("last blockhash from epoch 0"));

        let error = build_slot_ranges_v2_from_archive_v2_sidecars(
            temporary.path(),
            1,
            &raw_ranges,
            None,
            Some([0; 32]),
        )
        .expect_err("zero predecessor seed must fail");
        assert!(error.to_string().contains("predecessor blockhash"));
    }
}

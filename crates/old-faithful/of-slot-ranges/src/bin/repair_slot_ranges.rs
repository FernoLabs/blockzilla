use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use of_car_reader::{
    CarBlockReader,
    node::{Node, decode_node, peek_node_type},
    reader::CarPayloadRead,
    reconstruct::Cid36,
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, SlotRange, decode_slot_range_entry,
        write_slot_ranges_raw,
    },
};
use reqwest::{
    StatusCode,
    blocking::{Client, Response},
    header::{
        ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, ETAG, HeaderMap,
        IF_RANGE, RANGE,
    },
};
use sha2::{Digest, Sha256};
use std::{
    collections::hash_map::RandomState,
    fs::{self, File, OpenOptions},
    hash::BuildHasher,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_BLOCK_RANGE_BYTES: u64 = 64 * 1024 * 1024;
const CAR_READER_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const NODE_KIND_PREFIX_LEN: usize = 16;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const HTTP_FULL_CAR_REQUEST_TIMEOUT: Duration = Duration::from_secs(48 * 60 * 60);
const TEMP_FILE_ATTEMPTS: usize = 128;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Parser)]
#[command(name = "of-repair-slot-ranges")]
#[command(about = "Repair Old Faithful raw slot ranges from slots.txt and bounded plain-CAR reads")]
struct Cli {
    /// Epoch to repair.
    #[arg(long)]
    epoch: u64,

    /// Raw index file, or a directory that contains epoch-N-slot-ranges.raw.
    #[arg(long)]
    raw: PathBuf,

    /// N.slots.txt file, or a directory that contains N.slots.txt or epoch-N.slots.txt.
    #[arg(long)]
    slots: PathBuf,

    /// Plain CAR path, directory, exact HTTP URL, URL ending in '/', or URL with {epoch}.
    #[arg(long)]
    car: String,

    /// Candidate raw output file. Required unless --plan is set.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Replace an existing output file.
    #[arg(long)]
    overwrite: bool,

    /// Permit an HTTP CAR without a strong ETag. Every repair segment
    /// is fetched twice and compared by SHA-256. Use only for immutable URLs.
    #[arg(long)]
    assume_immutable_http: bool,

    /// Expected SHA-256 of the complete plain CAR. This forces one full-CAR
    /// pass and replaces the second HTTP read with a whole-object digest check.
    #[arg(long, value_parser = parse_sha256_hex)]
    expected_car_sha256: Option<[u8; 32]>,

    /// Print repair segments and transfer bytes without reading them or writing output.
    #[arg(long)]
    plan: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SlotMembership {
    before: Option<u64>,
    current: Vec<u64>,
    after: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RepairStats {
    segments: usize,
    repaired_rows: usize,
    cleared_nonmember_rows: usize,
    kept_rows: usize,
    car_bytes_read: u64,
    car_bytes_rechecked: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairOutput {
    ranges: Vec<SlotRange>,
    stats: RepairStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RepairPlan {
    ranges: Vec<SlotRange>,
    member_rows: Vec<usize>,
    segments: Vec<RepairSegment>,
    stats: RepairStats,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RepairSegment {
    first: usize,
    last: usize,
    start: u64,
    end: u64,
    includes_header: bool,
    allows_trailing_nodes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentExpectation {
    exact: Vec<(u64, bool)>,
    epoch_start: u64,
    epoch_end: u64,
    allow_decoded_before: bool,
    allow_decoded_after: bool,
}

#[derive(Debug)]
struct ScannedSegment {
    ranges: Vec<SlotRange>,
    sha256: [u8; 32],
}

trait CarSource {
    fn len(&self) -> u64;
    fn label(&self) -> &str;
    fn open_segment(&self, start: u64, end: u64) -> Result<Box<dyn Read>>;

    fn requires_recheck(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct LocalCarSource {
    file: File,
    path: PathBuf,
    label: String,
    len: u64,
    modified: Option<std::time::SystemTime>,
}

impl LocalCarSource {
    fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?;
        if !metadata.is_file() {
            bail!("{} is not a plain CAR file", path.display());
        }
        let len = metadata.len();
        if len == 0 {
            bail!("{} is empty", path.display());
        }
        let label = path.display().to_string();
        let modified = metadata.modified().ok();
        Ok(Self {
            file,
            path,
            label,
            len,
            modified,
        })
    }
}

impl CarSource for LocalCarSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn label(&self) -> &str {
        &self.label
    }

    fn open_segment(&self, start: u64, end: u64) -> Result<Box<dyn Read>> {
        validate_segment_bounds(start, end, self.len, &self.label)?;
        let metadata = self
            .file
            .metadata()
            .with_context(|| format!("stat {}", self.path.display()))?;
        let current_len = metadata.len();
        if current_len != self.len {
            bail!(
                "{} changed size during repair: initial={} current={current_len}",
                self.path.display(),
                self.len
            );
        }
        if self.modified.is_some() && metadata.modified().ok() != self.modified {
            bail!("{} changed during repair", self.path.display());
        }
        let mut file = self
            .file
            .try_clone()
            .with_context(|| format!("clone handle for {}", self.path.display()))?;
        file.seek(SeekFrom::Start(start))
            .with_context(|| format!("seek {} to {start}", self.path.display()))?;
        Ok(Box::new(file.take(end - start)))
    }
}

#[derive(Debug)]
struct HttpCarSource {
    client: Client,
    url: String,
    len: u64,
    validator: Option<HttpValidator>,
    accept_full_body_200: bool,
}

#[derive(Clone, Debug)]
struct HttpValidator {
    header_name: &'static str,
    value: String,
}

impl HttpCarSource {
    fn open(url: String, allow_unvalidated: bool, full_car_pass: bool) -> Result<Self> {
        let request_timeout = if full_car_pass {
            HTTP_FULL_CAR_REQUEST_TIMEOUT
        } else {
            HTTP_REQUEST_TIMEOUT
        };
        let client = Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(request_timeout)
            .tcp_nodelay(true)
            .user_agent("of-repair-slot-ranges/0.1")
            .build()
            .context("build HTTP client")?;
        let response = client
            .get(&url)
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, "bytes=0-0")
            .send()
            .with_context(|| format!("range probe {url}"))?;
        let len = validate_http_range_headers(&response, 0, 1, None, &url)?;
        let validator = response_validator(&response, &url)?;
        if validator.is_none() && !allow_unvalidated {
            bail!(
                "HTTP CAR {url} has no strong ETag; use a local plain CAR, a versioned source, or explicitly pass --assume-immutable-http"
            );
        }
        let bytes = response
            .bytes()
            .with_context(|| format!("read range probe from {url}"))?;
        if bytes.len() != 1 {
            bail!(
                "range probe {url} returned {} bytes, expected 1",
                bytes.len()
            );
        }
        Ok(Self {
            client,
            url,
            len,
            validator,
            accept_full_body_200: full_car_pass,
        })
    }
}

impl CarSource for HttpCarSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn label(&self) -> &str {
        &self.url
    }

    fn open_segment(&self, start: u64, end: u64) -> Result<Box<dyn Read>> {
        validate_segment_bounds(start, end, self.len, &self.url)?;
        let inclusive_end = end - 1;
        let mut request = self
            .client
            .get(&self.url)
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, format!("bytes={start}-{inclusive_end}"));
        if let Some(validator) = &self.validator {
            request = request.header(IF_RANGE, &validator.value);
        }
        let response = request
            .send()
            .with_context(|| format!("range GET {} bytes={start}-{inclusive_end}", self.url))?;
        if response.status() == StatusCode::OK {
            if !self.accept_full_body_200 || start != 0 || end != self.len {
                bail!(
                    "range GET {} bytes={start}-{inclusive_end} returned HTTP 200 for a non-full-CAR request",
                    self.url
                );
            }
            validate_http_full_body_headers(
                response.status(),
                response.headers(),
                self.len,
                &self.url,
            )?;
        } else {
            validate_http_range_headers(&response, start, end - start, Some(self.len), &self.url)?;
        }
        if let Some(validator) = &self.validator {
            validate_response_validator(&response, validator, &self.url)?;
        }
        Ok(Box::new(response.take(end - start)))
    }

    fn requires_recheck(&self) -> bool {
        self.validator.is_none()
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let full_car_digest = cli.expected_car_sha256;
    let raw_path = resolve_raw_path(&cli.raw, cli.epoch)?;
    let slots_path = resolve_slots_path(&cli.slots, cli.epoch)?;
    let car_name = resolve_car_name(&cli.car, cli.epoch)?;
    reject_compressed_car(&car_name)?;

    let ranges = read_raw_ranges(&raw_path)?;
    let membership = read_slot_membership_with_neighbors(
        &slots_path,
        cli.slots.is_dir().then_some(cli.slots.as_path()),
        cli.epoch,
    )?;
    let source: Box<dyn CarSource> = if is_http_url(&car_name) {
        Box::new(HttpCarSource::open(
            car_name,
            cli.assume_immutable_http || full_car_digest.is_some(),
            full_car_digest.is_some(),
        )?)
    } else {
        Box::new(LocalCarSource::open(PathBuf::from(car_name))?)
    };

    let mut plan = build_repair_plan(cli.epoch, &ranges, &membership, source.len())?;
    if full_car_digest.is_some() {
        force_full_car_plan(&mut plan, &membership, source.len())?;
    }
    if cli.plan {
        if cli.overwrite {
            bail!("--plan does not accept --overwrite");
        }
        print_repair_plan(
            cli.epoch,
            &membership,
            &plan,
            source.label(),
            source.requires_recheck() && full_car_digest.is_none(),
        )?;
        return Ok(());
    }
    let output_path = cli
        .output
        .as_deref()
        .ok_or_else(|| anyhow!("--output is required unless --plan is set"))?;
    if output_path.exists() && !cli.overwrite {
        bail!(
            "output {} already exists; pass --overwrite to replace it",
            output_path.display()
        );
    }

    let repaired = execute_repair_plan_with_digest(
        cli.epoch,
        &membership,
        source.as_ref(),
        plan,
        full_car_digest,
    )?;
    write_raw_atomic(output_path, &repaired.ranges, cli.overwrite)?;
    eprintln!(
        "epoch={}: repaired_segments={} repaired_rows={} cleared_nonmember_rows={} kept_rows={} car_bytes_read={} car_bytes_rechecked={} output={}",
        cli.epoch,
        repaired.stats.segments,
        repaired.stats.repaired_rows,
        repaired.stats.cleared_nonmember_rows,
        repaired.stats.kept_rows,
        repaired.stats.car_bytes_read,
        repaired.stats.car_bytes_rechecked,
        output_path.display()
    );
    Ok(())
}

#[cfg(test)]
fn repair_ranges(
    epoch: u64,
    input: &[SlotRange],
    membership: &SlotMembership,
    source: &dyn CarSource,
) -> Result<RepairOutput> {
    let plan = build_repair_plan(epoch, input, membership, source.len())?;
    execute_repair_plan(epoch, membership, source, plan)
}

fn force_full_car_plan(
    plan: &mut RepairPlan,
    membership: &SlotMembership,
    source_len: u64,
) -> Result<()> {
    if source_len == 0 {
        bail!("cannot plan a full pass over an empty CAR");
    }
    let last = membership
        .current
        .len()
        .checked_sub(1)
        .ok_or_else(|| anyhow!("cannot plan a full CAR pass without current-epoch Blocks"))?;
    plan.segments = vec![RepairSegment {
        first: 0,
        last,
        start: 0,
        end: source_len,
        includes_header: true,
        allows_trailing_nodes: true,
    }];
    plan.stats.segments = 1;
    plan.stats.repaired_rows = membership.current.len();
    plan.stats.kept_rows = 0;
    Ok(())
}

fn build_repair_plan(
    epoch: u64,
    input: &[SlotRange],
    membership: &SlotMembership,
    source_len: u64,
) -> Result<RepairPlan> {
    if input.len() != SLOTS_PER_EPOCH as usize {
        bail!(
            "epoch {epoch} raw index has {} rows, expected {}",
            input.len(),
            SLOTS_PER_EPOCH
        );
    }
    validate_membership(epoch, membership)?;

    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let mut member_at_row = vec![false; SLOTS_PER_EPOCH as usize];
    let member_rows = membership
        .current
        .iter()
        .map(|slot| {
            let row = usize::try_from(*slot - epoch_start)
                .context("slot-in-epoch exceeds address space")?;
            member_at_row[row] = true;
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut output = input.to_vec();
    let mut stats = RepairStats::default();
    for (row, is_member) in member_at_row.iter().copied().enumerate() {
        if !is_member && output[row] != SlotRange::EMPTY {
            output[row] = SlotRange::EMPTY;
            stats.cleared_nonmember_rows += 1;
        }
    }

    let mut locally_valid = Vec::with_capacity(member_rows.len());
    for row in &member_rows {
        locally_valid.push(is_valid_present_range(output[*row], source_len));
    }
    let mut suspect = locally_valid.iter().map(|valid| !valid).collect::<Vec<_>>();
    for index in 0..member_rows.len().saturating_sub(1) {
        if !locally_valid[index] || !locally_valid[index + 1] {
            continue;
        }
        let current = output[member_rows[index]];
        let next = output[member_rows[index + 1]];
        if current.end_exclusive() != Some(next.offset) {
            // Either side can be wrong. Repair both instead of selecting an
            // unproved boundary.
            suspect[index] = true;
            suspect[index + 1] = true;
        }
    }
    // A malformed row can hide that its left neighbor extends into its CAR
    // group. Include that neighbor so its terminal Block proves the left
    // boundary and its range is reconstructed too.
    let initially_suspect = suspect.clone();
    for (index, is_suspect) in initially_suspect.into_iter().enumerate() {
        if is_suspect && index > 0 {
            suspect[index - 1] = true;
        }
    }

    let segments = repair_segments(&suspect, &member_rows, &output, source_len)?;
    stats.segments = segments.len();
    stats.repaired_rows = segments
        .iter()
        .map(|segment| segment.last - segment.first + 1)
        .sum();
    stats.kept_rows = membership.current.len() - stats.repaired_rows;
    Ok(RepairPlan {
        ranges: output,
        member_rows,
        segments,
        stats,
    })
}

#[cfg(test)]
fn execute_repair_plan(
    epoch: u64,
    membership: &SlotMembership,
    source: &dyn CarSource,
    plan: RepairPlan,
) -> Result<RepairOutput> {
    execute_repair_plan_with_digest(epoch, membership, source, plan, None)
}

fn execute_repair_plan_with_digest(
    epoch: u64,
    membership: &SlotMembership,
    source: &dyn CarSource,
    plan: RepairPlan,
    expected_car_sha256: Option<[u8; 32]>,
) -> Result<RepairOutput> {
    if expected_car_sha256.is_some() {
        let expected_last = membership
            .current
            .len()
            .checked_sub(1)
            .ok_or_else(|| anyhow!("expected CAR SHA-256 requires current-epoch Blocks"))?;
        let is_full_pass = matches!(
            plan.segments.as_slice(),
            [segment]
                if segment.first == 0
                    && segment.last == expected_last
                    && segment.start == 0
                    && segment.end == source.len()
                    && segment.includes_header
                    && segment.allows_trailing_nodes
        );
        if !is_full_pass {
            bail!("expected CAR SHA-256 requires exactly one complete CAR repair segment");
        }
    }
    let RepairPlan {
        ranges: mut output,
        member_rows,
        segments,
        mut stats,
    } = plan;
    let mut rechecks = Vec::new();
    for segment in segments {
        let expected = expected_segment_slots(epoch, membership, segment)?;
        let scanned = scan_segment(source, segment, &expected)?;
        if let Some(expected_sha256) = expected_car_sha256
            && scanned.sha256 != expected_sha256
        {
            bail!(
                "CAR {} SHA-256 mismatch: expected={} actual={}",
                source.label(),
                sha256_hex(expected_sha256),
                sha256_hex(scanned.sha256)
            );
        }
        let expected_current = segment.last - segment.first + 1;
        if scanned.ranges.len() != expected_current {
            bail!(
                "{} segment {}..{} returned {} current-epoch ranges, expected {expected_current}",
                source.label(),
                segment.start,
                segment.end,
                scanned.ranges.len()
            );
        }
        for (member_index, range) in (segment.first..=segment.last).zip(scanned.ranges) {
            output[member_rows[member_index]] = range;
        }
        stats.car_bytes_read = stats
            .car_bytes_read
            .checked_add(segment.end - segment.start)
            .ok_or_else(|| anyhow!("CAR byte counter overflow"))?;
        if expected_car_sha256.is_none() && source.requires_recheck() {
            rechecks.push((segment, scanned.sha256));
        }
    }

    for (segment, expected_sha256) in rechecks.into_iter().rev() {
        let actual_sha256 = hash_source_segment(source, segment.start, segment.end)?;
        if actual_sha256 != expected_sha256 {
            bail!(
                "HTTP CAR segment {} bytes {}..{} changed between bounded reads",
                source.label(),
                segment.start,
                segment.end
            );
        }
        stats.car_bytes_rechecked = stats
            .car_bytes_rechecked
            .checked_add(segment.end - segment.start)
            .ok_or_else(|| anyhow!("CAR recheck byte counter overflow"))?;
    }

    validate_complete_output(epoch, &output, membership, source.len())?;
    Ok(RepairOutput {
        ranges: output,
        stats,
    })
}

fn print_repair_plan(
    epoch: u64,
    membership: &SlotMembership,
    plan: &RepairPlan,
    source: &str,
    recheck: bool,
) -> Result<()> {
    let selected_bytes = plan.segments.iter().try_fold(0u64, |total, segment| {
        total
            .checked_add(segment.end - segment.start)
            .ok_or_else(|| anyhow!("selected CAR byte count overflow"))
    })?;
    let transfer_bytes = selected_bytes
        .checked_mul(if recheck { 2 } else { 1 })
        .ok_or_else(|| anyhow!("planned transfer byte count overflow"))?;
    println!(
        "epoch={epoch} source={source} segments={} repaired_rows={} cleared_nonmember_rows={} kept_rows={} selected_car_bytes={selected_bytes} recheck={} planned_transfer_bytes={transfer_bytes}",
        plan.stats.segments,
        plan.stats.repaired_rows,
        plan.stats.cleared_nonmember_rows,
        plan.stats.kept_rows,
        recheck
    );
    for (index, segment) in plan.segments.iter().enumerate() {
        println!(
            "segment={index} first_slot={} last_slot={} start={} end={} bytes={} includes_header={} includes_car_tail={}",
            membership.current[segment.first],
            membership.current[segment.last],
            segment.start,
            segment.end,
            segment.end - segment.start,
            segment.includes_header,
            segment.allows_trailing_nodes
        );
    }
    Ok(())
}

fn repair_segments(
    suspect: &[bool],
    member_rows: &[usize],
    ranges: &[SlotRange],
    source_len: u64,
) -> Result<Vec<RepairSegment>> {
    let mut segments = Vec::new();
    let mut index = 0usize;
    while index < suspect.len() {
        if !suspect[index] {
            index += 1;
            continue;
        }
        let first = index;
        while index + 1 < suspect.len() && suspect[index + 1] {
            index += 1;
        }
        let mut last = index;
        let start = if first == 0 {
            0
        } else {
            ranges[member_rows[first - 1]]
                .end_exclusive()
                .ok_or_else(|| anyhow!("left trusted boundary overflows"))?
        };
        let end = loop {
            let candidate_end = if last + 1 == suspect.len() {
                source_len
            } else {
                ranges[member_rows[last + 1]].offset
            };
            // A displaced right anchor can start exactly where the first
            // repaired row ends. Such a segment has no byte space for any
            // later repaired row, even though its numeric bounds are ordered.
            // Rebuild the nominal anchor too so that the next boundary proves
            // all expected Blocks.
            let right_anchor_is_covered_by_first_range = first < last
                && ranges[member_rows[first]]
                    .end_exclusive()
                    .is_some_and(|first_end| first_end >= candidate_end);
            if start < candidate_end
                && candidate_end <= source_len
                && !right_anchor_is_covered_by_first_range
            {
                break candidate_end;
            }
            if last + 1 < suspect.len() {
                // The nominal right anchor is not usable. Include it in the
                // repair and continue to the next possible anchor. Absorb any
                // following suspect run too.
                last += 1;
                while last + 1 < suspect.len() && suspect[last + 1] {
                    last += 1;
                }
                continue;
            }
            // A structurally coherent prefix can still be globally displaced.
            // With no usable right anchor, scan the whole epoch CAR rather than
            // guess which earlier boundary is correct.
            return Ok(vec![RepairSegment {
                first: 0,
                last: suspect.len() - 1,
                start: 0,
                end: source_len,
                includes_header: true,
                allows_trailing_nodes: true,
            }]);
        };
        segments.push(RepairSegment {
            first,
            last,
            start,
            end,
            includes_header: first == 0,
            allows_trailing_nodes: last + 1 == suspect.len(),
        });
        index = last + 1;
    }
    Ok(segments)
}

fn expected_segment_slots(
    epoch: u64,
    membership: &SlotMembership,
    segment: RepairSegment,
) -> Result<SegmentExpectation> {
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    let mut exact = Vec::with_capacity(segment.last - segment.first + 3);
    let mut allow_decoded_before = false;
    if segment.includes_header && epoch > 0 {
        if let Some(slot) = membership.before {
            exact.push((slot, false));
        } else {
            allow_decoded_before = true;
        }
    }
    exact.extend(
        membership.current[segment.first..=segment.last]
            .iter()
            .copied()
            .map(|slot| (slot, true)),
    );
    let mut allow_decoded_after = false;
    if segment.allows_trailing_nodes {
        if let Some(slot) = membership.after {
            exact.push((slot, false));
        } else {
            allow_decoded_after = true;
        }
    }
    Ok(SegmentExpectation {
        exact,
        epoch_start,
        epoch_end,
        allow_decoded_before,
        allow_decoded_after,
    })
}

fn scan_segment(
    source: &dyn CarSource,
    segment: RepairSegment,
    expected: &SegmentExpectation,
) -> Result<ScannedSegment> {
    let reader = DigestingReader::new(source.open_segment(segment.start, segment.end)?);
    let mut car = CarBlockReader::with_capacity(reader, CAR_READER_BUFFER_BYTES);
    if segment.includes_header {
        car.skip_header()
            .with_context(|| format!("read CAR header from {}", source.label()))?;
    }

    let segment_len = segment.end - segment.start;
    let mut scratch = Vec::with_capacity(256 * 1024);
    let mut pending_start = None;
    let mut expected_index = 0usize;
    let mut decoded_before = false;
    let mut decoded_after = false;
    let mut repaired = Vec::new();
    while let Some(entry) = car
        .read_entry_payload_select_with_scratch(&mut scratch, NODE_KIND_PREFIX_LEN, |prefix| {
            if matches!(peek_node_type(prefix), Ok(2)) {
                CarPayloadRead::Full
            } else {
                CarPayloadRead::Skip
            }
        })
        .with_context(|| {
            format!(
                "parse CAR segment {} bytes {}..{}",
                source.label(),
                segment.start,
                segment.end
            )
        })?
    {
        let absolute_start = segment
            .start
            .checked_add(entry.location.car_offset)
            .ok_or_else(|| anyhow!("absolute CAR entry offset overflow"))?;
        pending_start.get_or_insert(absolute_start);
        let entry_end = absolute_start
            .checked_add(entry.total_len as u64)
            .ok_or_else(|| anyhow!("CAR entry range end overflow"))?;
        let group_len = entry_end
            .checked_sub(pending_start.expect("set before entry processing"))
            .ok_or_else(|| anyhow!("CAR entry ends before the current group starts"))?;
        if group_len > MAX_BLOCK_RANGE_BYTES {
            bail!(
                "CAR group at {} offset {} exceeded {MAX_BLOCK_RANGE_BYTES} bytes before a Block boundary",
                source.label(),
                pending_start.unwrap()
            );
        }
        if !matches!(peek_node_type(entry.prefix), Ok(2)) {
            continue;
        }
        let payload = entry
            .payload
            .ok_or_else(|| anyhow!("Block candidate payload was not loaded in full"))?;
        let actual_cid = Cid36::compute(payload);
        if actual_cid != entry.cid {
            bail!(
                "CAR segment {} entry {} at offset {} has Block CID mismatch: expected={} actual={actual_cid}",
                source.label(),
                entry.location.entry_index,
                absolute_start,
                entry.cid
            );
        }
        let slot = match decode_node(payload).with_context(|| {
            format!(
                "decode full Block node from {} entry {} at offset {}",
                source.label(),
                entry.location.entry_index,
                absolute_start
            )
        })? {
            Node::Block(block) => block.slot,
            _ => bail!(
                "CAR segment {} entry {} at offset {} was selected as a Block but decoded as another node kind",
                source.label(),
                entry.location.entry_index,
                absolute_start
            ),
        };
        // Old Faithful slot lists can include one verified Block from the
        // previous epoch even when that boundary Block is not stored in this
        // CAR. If the CAR starts with the expected current-epoch sequence,
        // treat the listed predecessor as absent. A decoded predecessor must
        // still match the listed slot exactly below.
        if let Some((optional_before, false)) = expected.exact.get(expected_index).copied()
            && optional_before < expected.epoch_start
            && (expected.epoch_start..expected.epoch_end).contains(&slot)
        {
            expected_index += 1;
        }
        if expected.allow_decoded_before
            && expected_index == 0
            && !decoded_before
            && slot < expected.epoch_start
        {
            let previous_epoch_start = expected.epoch_start - SLOTS_PER_EPOCH;
            if slot < previous_epoch_start {
                bail!(
                    "CAR segment {} has decoded predecessor Block slot {slot}, before adjacent epoch range {previous_epoch_start}..{}",
                    source.label(),
                    expected.epoch_start
                );
            }
            decoded_before = true;
            pending_start = None;
            continue;
        }
        if expected_index == expected.exact.len()
            && expected.allow_decoded_after
            && !decoded_after
            && slot >= expected.epoch_end
        {
            let next_epoch_end = expected
                .epoch_end
                .checked_add(SLOTS_PER_EPOCH)
                .ok_or_else(|| anyhow!("next epoch end overflow"))?;
            if slot >= next_epoch_end {
                bail!(
                    "CAR segment {} has decoded successor Block slot {slot}, outside adjacent epoch range {}..{next_epoch_end}",
                    source.label(),
                    expected.epoch_end
                );
            }
            decoded_after = true;
            pending_start = None;
            continue;
        }
        let Some((expected_slot, keep)) = expected.exact.get(expected_index).copied() else {
            bail!(
                "CAR segment {} bytes {}..{} has unexpected Block slot {slot} after all expected slots",
                source.label(),
                segment.start,
                segment.end
            );
        };
        if slot != expected_slot {
            bail!(
                "CAR segment {} bytes {}..{} has Block slot {slot} at position {expected_index}, expected {expected_slot}",
                source.label(),
                segment.start,
                segment.end
            );
        }

        let start = pending_start.expect("set before Block processing");
        let end = entry_end;
        let len = end
            .checked_sub(start)
            .ok_or_else(|| anyhow!("CAR block range starts after its end for slot {slot}"))?;
        validate_reconstructed_len(slot, len)?;
        if keep {
            repaired.push(SlotRange {
                offset: start,
                len: u32::try_from(len).expect("64 MiB range fits u32"),
            });
        }
        expected_index += 1;
        pending_start = None;
    }

    if car.offset != segment_len {
        bail!(
            "CAR segment {} consumed {} bytes, expected {segment_len}",
            source.label(),
            car.offset
        );
    }
    // The listed next-epoch boundary is also optional in the CAR. At EOF it
    // can be the only unconsumed expectation. If a successor Block is present,
    // the normal exact-slot comparison above still proves its identity.
    while let Some((optional_after, false)) = expected.exact.get(expected_index).copied()
        && optional_after >= expected.epoch_end
    {
        expected_index += 1;
    }
    if expected_index != expected.exact.len() {
        let missing = expected.exact[expected_index].0;
        bail!(
            "CAR segment {} bytes {}..{} ended before expected Block slot {missing} at position {expected_index}",
            source.label(),
            segment.start,
            segment.end
        );
    }
    if !segment.allows_trailing_nodes && pending_start.is_some() {
        bail!(
            "CAR segment {} bytes {}..{} has non-Block frames after its last expected Block; right boundary is not a block boundary",
            source.label(),
            segment.start,
            segment.end
        );
    }
    let sha256 = car.reader.get_ref().sha256();
    Ok(ScannedSegment {
        ranges: repaired,
        sha256,
    })
}

struct DigestingReader {
    inner: Box<dyn Read>,
    hasher: Sha256,
}

impl DigestingReader {
    fn new(inner: Box<dyn Read>) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn sha256(&self) -> [u8; 32] {
        self.hasher.clone().finalize().into()
    }
}

impl Read for DigestingReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(out)?;
        self.hasher.update(&out[..read]);
        Ok(read)
    }
}

fn hash_source_segment(source: &dyn CarSource, start: u64, end: u64) -> Result<[u8; 32]> {
    let mut reader = source.open_segment(start, end)?;
    let expected_len = end - start;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    let mut read_total = 0u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("recheck {} bytes {start}..{end}", source.label()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        read_total = read_total
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("CAR recheck byte count overflow"))?;
    }
    if read_total != expected_len {
        bail!(
            "CAR recheck {} bytes {start}..{end} returned {read_total} bytes, expected {expected_len}",
            source.label()
        );
    }
    Ok(hasher.finalize().into())
}

fn validate_complete_output(
    epoch: u64,
    ranges: &[SlotRange],
    membership: &SlotMembership,
    source_len: u64,
) -> Result<()> {
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let mut member_at_row = vec![false; SLOTS_PER_EPOCH as usize];
    let mut previous_end = None;
    for slot in &membership.current {
        let row =
            usize::try_from(*slot - epoch_start).context("slot-in-epoch exceeds address space")?;
        member_at_row[row] = true;
        let range = ranges[row];
        if !is_valid_present_range(range, source_len) {
            bail!(
                "listed Block slot {slot} has invalid CAR range offset={} len={} (required: nonzero and at most {MAX_BLOCK_RANGE_BYTES} bytes)",
                range.offset,
                range.len
            );
        }
        if previous_end.is_some_and(|end| range.offset != end) {
            bail!(
                "listed Block slot {slot} starts at {}, expected contiguous boundary {}",
                range.offset,
                previous_end.unwrap()
            );
        }
        previous_end = range.end_exclusive();
    }
    for (row, range) in ranges.iter().copied().enumerate() {
        if !member_at_row[row] && range != SlotRange::EMPTY {
            bail!(
                "nonmember slot {} has CAR range offset={} len={}",
                epoch_start + row as u64,
                range.offset,
                range.len
            );
        }
    }
    Ok(())
}

fn is_valid_present_range(range: SlotRange, source_len: u64) -> bool {
    range.offset != 0
        && range.len != 0
        && u64::from(range.len) <= MAX_BLOCK_RANGE_BYTES
        && range.end_exclusive().is_some_and(|end| end <= source_len)
}

fn validate_reconstructed_len(slot: u64, len: u64) -> Result<()> {
    if len == 0 {
        bail!("Block slot {slot} reconstructed to an empty CAR range");
    }
    if len > MAX_BLOCK_RANGE_BYTES {
        bail!(
            "Block slot {slot} reconstructed CAR range is {len} bytes, over {MAX_BLOCK_RANGE_BYTES}"
        );
    }
    Ok(())
}

fn read_raw_ranges(path: &Path) -> Result<Vec<SlotRange>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let expected_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if bytes.len() != expected_len {
        bail!(
            "{} has {} bytes, expected {expected_len} ({} rows of {SLOT_RANGE_ENTRY_SIZE} bytes)",
            path.display(),
            bytes.len(),
            SLOTS_PER_EPOCH
        );
    }
    bytes
        .chunks_exact(SLOT_RANGE_ENTRY_SIZE)
        .map(|row| decode_slot_range_entry(row).map_err(anyhow::Error::from))
        .collect()
}

fn read_slot_membership(path: &Path, epoch: u64) -> Result<SlotMembership> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read Old Faithful slot list {}", path.display()))?;
    let mut lines = contents.split('\n').collect::<Vec<_>>();
    if lines.last() == Some(&"") {
        lines.pop();
    }
    if lines.is_empty() {
        bail!("{} has no slot lines", path.display());
    }
    let mut slots = Vec::with_capacity(lines.len());
    for (line_index, raw_line) in lines.into_iter().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() || !line.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!(
                "{} line {line_number} is not a decimal u64: {line:?}",
                path.display()
            );
        }
        slots.push(
            line.parse::<u64>()
                .with_context(|| format!("parse line {line_number} from {}", path.display()))?,
        );
    }

    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    for pair in slots.windows(2) {
        if pair[1] <= pair[0] {
            bail!(
                "{} slots are not strictly increasing: {} follows {}",
                path.display(),
                pair[1],
                pair[0]
            );
        }
    }
    let before = slots
        .iter()
        .copied()
        .filter(|slot| *slot < epoch_start)
        .collect::<Vec<_>>();
    let current = slots
        .iter()
        .copied()
        .filter(|slot| (epoch_start..epoch_end).contains(slot))
        .collect::<Vec<_>>();
    let after = slots
        .iter()
        .copied()
        .filter(|slot| *slot >= epoch_end)
        .collect::<Vec<_>>();
    if before.len() > 1 || after.len() > 1 {
        bail!(
            "{} has {} before-epoch and {} after-epoch boundary slots; at most one of each is allowed",
            path.display(),
            before.len(),
            after.len()
        );
    }
    let membership = SlotMembership {
        before: before.first().copied(),
        current,
        after: after.first().copied(),
    };
    validate_membership(epoch, &membership)
        .with_context(|| format!("validate {}", path.display()))?;
    Ok(membership)
}

fn read_slot_membership_with_neighbors(
    path: &Path,
    slot_list_dir: Option<&Path>,
    epoch: u64,
) -> Result<SlotMembership> {
    let membership = read_slot_membership(path, epoch)?;
    let Some(root) = slot_list_dir else {
        return Ok(membership);
    };
    if let Some(before) = membership.before {
        let previous_epoch = epoch
            .checked_sub(1)
            .ok_or_else(|| anyhow!("epoch 0 cannot have a before-epoch boundary"))?;
        if let Some(previous_path) = find_slots_path(root, previous_epoch)? {
            let previous = read_slot_membership(&previous_path, previous_epoch)?;
            let expected = previous.current.last().copied().ok_or_else(|| {
                anyhow!(
                    "{} has no current-epoch Block slots",
                    previous_path.display()
                )
            })?;
            if before != expected {
                bail!(
                    "{} before-epoch boundary {before} differs from the last current slot {expected} in {}",
                    path.display(),
                    previous_path.display()
                );
            }
        }
    }
    if let Some(after) = membership.after {
        let next_epoch = epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("next epoch overflow for epoch {epoch}"))?;
        if let Some(next_path) = find_slots_path(root, next_epoch)? {
            let next = read_slot_membership(&next_path, next_epoch)?;
            let expected = next.current.first().copied().ok_or_else(|| {
                anyhow!("{} has no current-epoch Block slots", next_path.display())
            })?;
            if after != expected {
                bail!(
                    "{} after-epoch boundary {after} differs from the first current slot {expected} in {}",
                    path.display(),
                    next_path.display()
                );
            }
        }
    }
    Ok(membership)
}

fn validate_membership(epoch: u64, membership: &SlotMembership) -> Result<()> {
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    if epoch == 0 && membership.before.is_some() {
        bail!("epoch 0 slot list must not have a before-epoch boundary");
    }
    if let Some(before) = membership.before {
        let previous_epoch_start = epoch_start
            .checked_sub(SLOTS_PER_EPOCH)
            .ok_or_else(|| anyhow!("epoch 0 cannot have a predecessor"))?;
        if !(previous_epoch_start..epoch_start).contains(&before) {
            bail!(
                "epoch {epoch} before-epoch boundary {before} is outside adjacent epoch range {previous_epoch_start}..{epoch_start}"
            );
        }
    }
    if let Some(after) = membership.after {
        let next_epoch_end = epoch_end
            .checked_add(SLOTS_PER_EPOCH)
            .ok_or_else(|| anyhow!("next epoch end overflow for epoch {epoch}"))?;
        if !(epoch_end..next_epoch_end).contains(&after) {
            bail!(
                "epoch {epoch} after-epoch boundary {after} is outside adjacent epoch range {epoch_end}..{next_epoch_end}"
            );
        }
    }
    if membership.current.is_empty() {
        bail!("epoch {epoch} slot list has no current-epoch Block slots");
    }
    let mut previous = None;
    for (position, slot) in membership.current.iter().copied().enumerate() {
        if !(epoch_start..epoch_end).contains(&slot) {
            bail!(
                "epoch {epoch} membership slot {slot} at position {position} is outside {epoch_start}..{epoch_end}"
            );
        }
        if previous.is_some_and(|prior| slot <= prior) {
            bail!(
                "epoch {epoch} membership is not strictly increasing at position {position}: {slot} follows {}",
                previous.unwrap()
            );
        }
        previous = Some(slot);
    }
    Ok(())
}

fn resolve_raw_path(path: &Path, epoch: u64) -> Result<PathBuf> {
    let resolved = if path.is_dir() {
        path.join(format!("epoch-{epoch}-slot-ranges.raw"))
    } else {
        path.to_path_buf()
    };
    if !resolved.is_file() {
        bail!("raw index {} is not a file", resolved.display());
    }
    Ok(resolved)
}

fn resolve_slots_path(path: &Path, epoch: u64) -> Result<PathBuf> {
    if !path.is_dir() {
        if !path.is_file() {
            bail!("slot list {} is not a file", path.display());
        }
        return Ok(path.to_path_buf());
    }
    find_slots_path(path, epoch)?.ok_or_else(|| {
        anyhow!(
            "no {epoch}.slots.txt or epoch-{epoch}.slots.txt in {}",
            path.display()
        )
    })
}

fn find_slots_path(path: &Path, epoch: u64) -> Result<Option<PathBuf>> {
    let found = [
        path.join(format!("{epoch}.slots.txt")),
        path.join(format!("epoch-{epoch}.slots.txt")),
    ]
    .into_iter()
    .filter(|candidate| candidate.is_file())
    .collect::<Vec<_>>();
    match found.as_slice() {
        [only] => Ok(Some(only.clone())),
        [] => Ok(None),
        _ => bail!(
            "both {epoch}.slots.txt and epoch-{epoch}.slots.txt exist in {}; select one file",
            path.display()
        ),
    }
}

fn resolve_car_name(input: &str, epoch: u64) -> Result<String> {
    if input.contains("{epoch}") {
        return Ok(input.replace("{epoch}", &epoch.to_string()));
    }
    if is_http_url(input) {
        if input.ends_with('/') {
            return Ok(format!("{input}{epoch}/epoch-{epoch}.car"));
        }
        return Ok(input.to_string());
    }
    let path = PathBuf::from(input);
    let path = if path.is_dir() {
        path.join(format!("epoch-{epoch}.car"))
    } else {
        path
    };
    if !path.is_file() {
        bail!("plain CAR {} is not a file", path.display());
    }
    Ok(path.display().to_string())
}

fn reject_compressed_car(name: &str) -> Result<()> {
    let clean = name.split(['?', '#']).next().unwrap_or(name);
    if clean.ends_with(".zst") {
        bail!(
            "{name} is compressed; repair requires a seekable plain .car source for bounded reads"
        );
    }
    Ok(())
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn parse_sha256_hex(value: &str) -> std::result::Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err(format!(
            "SHA-256 must have exactly 64 hexadecimal characters, got {}",
            value.len()
        ));
    }
    let mut digest = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or_else(|| {
            format!(
                "SHA-256 has a non-hexadecimal character at position {}",
                index * 2 + 1
            )
        })?;
        let low = hex_nibble(pair[1]).ok_or_else(|| {
            format!(
                "SHA-256 has a non-hexadecimal character at position {}",
                index * 2 + 2
            )
        })?;
        digest[index] = high << 4 | low;
    }
    Ok(digest)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn sha256_hex(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_segment_bounds(start: u64, end: u64, source_len: u64, label: &str) -> Result<()> {
    if start >= end || end > source_len {
        bail!("invalid CAR segment for {label}: start={start} end={end} source_len={source_len}");
    }
    Ok(())
}

fn validate_http_range_headers(
    response: &Response,
    start: u64,
    len: u64,
    expected_total: Option<u64>,
    url: &str,
) -> Result<u64> {
    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "range GET {url} returned HTTP {}, expected 206",
            response.status()
        );
    }
    let end = start
        .checked_add(len)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| anyhow!("HTTP range end overflow for {url}"))?;
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| anyhow!("range GET {url} has no Content-Range"))?
        .to_str()
        .with_context(|| format!("decode Content-Range from {url}"))?;
    let prefix = format!("bytes {start}-{end}/");
    let total = content_range
        .strip_prefix(&prefix)
        .ok_or_else(|| {
            anyhow!(
                "range GET {url} returned Content-Range {content_range:?}, expected {prefix}TOTAL"
            )
        })?
        .parse::<u64>()
        .with_context(|| format!("parse Content-Range total from {url}"))?;
    if total == 0 || end >= total {
        bail!("range GET {url} returned invalid total length {total}");
    }
    if expected_total.is_some_and(|expected| total != expected) {
        bail!(
            "HTTP CAR {url} changed size during repair: initial={} current={total}",
            expected_total.unwrap()
        );
    }
    if let Some(content_length) = response.headers().get(CONTENT_LENGTH) {
        let actual = content_length
            .to_str()
            .with_context(|| format!("decode Content-Length from {url}"))?
            .parse::<u64>()
            .with_context(|| format!("parse Content-Length from {url}"))?;
        if actual != len {
            bail!("range GET {url} returned Content-Length {actual}, expected {len}");
        }
    }
    Ok(total)
}

fn validate_http_full_body_headers(
    status: StatusCode,
    headers: &HeaderMap,
    expected_len: u64,
    url: &str,
) -> Result<()> {
    if status != StatusCode::OK {
        bail!("full GET {url} returned HTTP {status}, expected 200");
    }
    if headers.contains_key(CONTENT_RANGE) {
        bail!("full GET {url} returned an unexpected Content-Range header");
    }
    let actual_len = headers
        .get(CONTENT_LENGTH)
        .ok_or_else(|| anyhow!("full GET {url} has no Content-Length"))?
        .to_str()
        .with_context(|| format!("decode Content-Length from {url}"))?
        .parse::<u64>()
        .with_context(|| format!("parse Content-Length from {url}"))?;
    if actual_len != expected_len {
        bail!("full GET {url} returned Content-Length {actual_len}, expected {expected_len}");
    }
    if let Some(encoding) = headers.get(CONTENT_ENCODING) {
        let encoding = encoding
            .to_str()
            .with_context(|| format!("decode Content-Encoding from {url}"))?;
        if !encoding.eq_ignore_ascii_case("identity") {
            bail!("full GET {url} returned unsupported Content-Encoding {encoding:?}");
        }
    }
    Ok(())
}

fn response_validator(response: &Response, url: &str) -> Result<Option<HttpValidator>> {
    if let Some(value) = response.headers().get(ETAG) {
        let value = value
            .to_str()
            .with_context(|| format!("decode ETag from {url}"))?;
        if is_strong_etag(value) {
            return Ok(Some(HttpValidator {
                header_name: "etag",
                value: value.to_string(),
            }));
        }
    }
    Ok(None)
}

fn is_strong_etag(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2
        && bytes[0] == b'"'
        && bytes[bytes.len() - 1] == b'"'
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte))
}

fn validate_response_validator(
    response: &Response,
    expected: &HttpValidator,
    url: &str,
) -> Result<()> {
    let actual = response
        .headers()
        .get(ETAG)
        .ok_or_else(|| anyhow!("range GET {url} omitted {}", expected.header_name))?
        .to_str()
        .with_context(|| format!("decode {} from {url}", expected.header_name))?;
    if actual != expected.value {
        bail!(
            "HTTP CAR {url} changed {} during repair: initial={:?} current={actual:?}",
            expected.header_name,
            expected.value
        );
    }
    Ok(())
}

fn write_raw_atomic(path: &Path, ranges: &[SlotRange], overwrite: bool) -> Result<()> {
    if ranges.len() != SLOTS_PER_EPOCH as usize {
        bail!(
            "refuse to write {} rows; expected {}",
            ranges.len(),
            SLOTS_PER_EPOCH
        );
    }
    let parent = output_parent(path);
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let (temporary, file) = create_temporary_output(path, parent)?;
    let cleanup = TemporaryOutput::new(temporary);
    let mut writer = BufWriter::with_capacity(256 * 1024, file);
    write_slot_ranges_raw(&mut writer, ranges)
        .with_context(|| format!("write {}", cleanup.path.display()))?;
    writer
        .flush()
        .with_context(|| format!("flush {}", cleanup.path.display()))?;
    let file = writer
        .into_inner()
        .map_err(|error| anyhow!("flush {}: {}", cleanup.path.display(), error.error()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", cleanup.path.display()))?;
    let actual_len = file
        .metadata()
        .with_context(|| format!("stat {}", cleanup.path.display()))?
        .len();
    let expected_len = (SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE) as u64;
    if actual_len != expected_len {
        bail!(
            "candidate {} has {actual_len} bytes, expected {expected_len}",
            cleanup.path.display()
        );
    }
    drop(file);

    if overwrite {
        fs::rename(&cleanup.path, path).with_context(|| {
            format!(
                "atomically replace {} with {}",
                path.display(),
                cleanup.path.display()
            )
        })?;
    } else {
        match fs::hard_link(&cleanup.path, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                bail!(
                    "output {} already exists; pass --overwrite to replace it",
                    path.display()
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "atomically publish {} without replacing {}",
                        cleanup.path.display(),
                        path.display()
                    )
                });
            }
        }
        fs::remove_file(&cleanup.path)
            .with_context(|| format!("remove {} after publish", cleanup.path.display()))?;
    }
    sync_parent_directory(parent)?;
    Ok(())
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn create_temporary_output(path: &Path, parent: &Path) -> Result<(PathBuf, File)> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("slot-ranges.raw");
    for _ in 0..TEMP_FILE_ATTEMPTS {
        let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let nonce = RandomState::new().hash_one((std::process::id(), counter, timestamp));
        let temporary = parent.join(format!(".{file_name}.tmp-{nonce:016x}"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create temporary output in {}", parent.display()));
            }
        }
    }
    bail!(
        "could not create a unique temporary output in {} after {TEMP_FILE_ATTEMPTS} attempts",
        parent.display()
    )
}

struct TemporaryOutput {
    path: PathBuf,
}

impl TemporaryOutput {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TemporaryOutput {
    fn drop(&mut self) {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<()> {
    File::open(parent)
        .with_context(|| format!("open parent directory {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("sync parent directory {}", parent.display()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        sync::{Arc, Barrier},
        thread,
    };
    use tempfile::TempDir;

    struct Fixture {
        temporary: TempDir,
        source: LocalCarSource,
        correct: Vec<SlotRange>,
        membership: SlotMembership,
        car: Vec<u8>,
    }

    impl Fixture {
        fn new(epoch: u64, current_slots: &[u64], trailing_node: bool) -> Self {
            let before = (epoch > 0).then(|| epoch * SLOTS_PER_EPOCH - 1);
            Self::with_boundaries(epoch, current_slots, before, None, trailing_node)
        }

        fn with_boundaries(
            epoch: u64,
            current_slots: &[u64],
            before: Option<u64>,
            after: Option<u64>,
            trailing_node: bool,
        ) -> Self {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let path = temporary.path().join(format!("epoch-{epoch}.car"));
            let mut car = vec![0]; // A zero-byte header is sufficient for CarBlockReader.
            if let Some(slot) = before {
                append_block_group(&mut car, slot);
            }
            let mut correct = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
            let epoch_start = epoch * SLOTS_PER_EPOCH;
            for slot in current_slots {
                let start = car.len() as u64;
                append_block_group(&mut car, *slot);
                let len = car.len() as u64 - start;
                correct[usize::try_from(*slot - epoch_start).unwrap()] = SlotRange {
                    offset: start,
                    len: u32::try_from(len).unwrap(),
                };
            }
            if let Some(slot) = after {
                append_block_group(&mut car, slot);
            }
            if trailing_node {
                append_frame(&mut car, &[0x81, 0x03]);
            }
            fs::write(&path, &car).expect("write CAR fixture");
            let source = LocalCarSource::open(path).expect("open CAR fixture");
            Self {
                temporary,
                source,
                correct,
                membership: SlotMembership {
                    before,
                    current: current_slots.to_vec(),
                    after,
                },
                car,
            }
        }
    }

    struct MemoryUnvalidatedSource {
        bytes: Vec<u8>,
        opens: Cell<usize>,
    }

    impl MemoryUnvalidatedSource {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                opens: Cell::new(0),
            }
        }
    }

    impl CarSource for MemoryUnvalidatedSource {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }

        fn label(&self) -> &str {
            "memory-unvalidated.car"
        }

        fn open_segment(&self, start: u64, end: u64) -> Result<Box<dyn Read>> {
            validate_segment_bounds(start, end, self.len(), self.label())?;
            self.opens.set(self.opens.get() + 1);
            let start = usize::try_from(start).context("test start fits usize")?;
            let end = usize::try_from(end).context("test end fits usize")?;
            Ok(Box::new(std::io::Cursor::new(
                self.bytes[start..end].to_vec(),
            )))
        }

        fn requires_recheck(&self) -> bool {
            true
        }
    }

    #[test]
    fn repairs_epoch_4_like_giant_overlap_and_keeps_coherent_rows() {
        let epoch = 4;
        let start = epoch * SLOTS_PER_EPOCH;
        let fixture = Fixture::new(epoch, &[start, start + 1, start + 2, start + 3], false);
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange {
            offset: malformed[0].offset,
            len: u32::try_from(MAX_BLOCK_RANGE_BYTES + 1).unwrap(),
        };

        let repaired = repair_ranges(epoch, &malformed, &fixture.membership, &fixture.source)
            .expect("repair giant overlap");

        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.segments, 1);
        assert_eq!(repaired.stats.repaired_rows, 2);
        assert_eq!(repaired.ranges[0], malformed[0]);
        assert_eq!(repaired.ranges[2], malformed[2]);
    }

    #[test]
    fn repairs_missing_member_and_clears_false_nonmember_row() {
        let fixture = Fixture::new(0, &[0, 1, 2], false);
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange::EMPTY;
        malformed[17] = fixture.correct[1];

        let repaired = repair_ranges(0, &malformed, &fixture.membership, &fixture.source)
            .expect("repair missing and false rows");

        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.repaired_rows, 2);
        assert_eq!(repaired.stats.cleared_nonmember_rows, 1);
    }

    #[test]
    fn reconstructs_left_anchor_when_it_consumes_missing_blocks_prefix() {
        let fixture = Fixture::new(0, &[0, 1, 2], false);
        let mut filler = Vec::new();
        append_frame(&mut filler, &[0x81, 0x00]);
        let mut malformed = fixture.correct.clone();
        malformed[0].len += u32::try_from(filler.len()).unwrap();
        malformed[1] = SlotRange::EMPTY;

        let repaired = repair_ranges(0, &malformed, &fixture.membership, &fixture.source)
            .expect("repair must prove and reconstruct left anchor");

        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.repaired_rows, 2);
    }

    #[test]
    fn repairs_first_current_block_after_verified_predecessor() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let fixture =
            Fixture::with_boundaries(epoch, &[start, start + 2], Some(start - 17), None, false);
        let mut malformed = fixture.correct.clone();
        malformed[0] = SlotRange::EMPTY;

        let repaired = repair_ranges(epoch, &malformed, &fixture.membership, &fixture.source)
            .expect("repair first block");

        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.segments, 1);
    }

    #[test]
    fn repairs_first_current_block_when_listed_predecessor_is_absent_from_car() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let fixture = Fixture::with_boundaries(epoch, &[start, start + 2], None, None, false);
        let mut membership = fixture.membership.clone();
        membership.before = Some(start - 17);
        let mut malformed = fixture.correct.clone();
        malformed[0] = SlotRange::EMPTY;

        let repaired = repair_ranges(epoch, &malformed, &membership, &fixture.source)
            .expect("listed predecessor is optional in the CAR");

        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn rejects_wrong_predecessor_when_listed_predecessor_is_present() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let fixture =
            Fixture::with_boundaries(epoch, &[start, start + 2], Some(start - 16), None, false);
        let mut membership = fixture.membership.clone();
        membership.before = Some(start - 17);
        let mut malformed = fixture.correct.clone();
        malformed[0] = SlotRange::EMPTY;

        let error = repair_ranges(epoch, &malformed, &membership, &fixture.source)
            .expect_err("decoded predecessor must match the listed boundary");

        assert!(
            error
                .to_string()
                .contains(&format!("expected {}", start - 17)),
            "{error:#}"
        );
    }

    #[test]
    fn verifies_listed_next_epoch_boundary_when_repairing_tail() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let fixture = Fixture::with_boundaries(
            epoch,
            &[start, start + 2],
            Some(start - 17),
            Some(end + 11),
            true,
        );
        let mut malformed = fixture.correct.clone();
        malformed[2] = SlotRange::EMPTY;

        let repaired = repair_ranges(epoch, &malformed, &fixture.membership, &fixture.source)
            .expect("repair tail with successor boundary");

        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn repairs_tail_when_listed_successor_is_absent_from_car() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let fixture = Fixture::with_boundaries(epoch, &[start, start + 2], None, None, true);
        let mut membership = fixture.membership.clone();
        membership.after = Some(end + 11);
        let mut malformed = fixture.correct.clone();
        malformed[2] = SlotRange::EMPTY;

        let repaired = repair_ranges(epoch, &malformed, &membership, &fixture.source)
            .expect("listed successor is optional at CAR EOF");

        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn rejects_wrong_successor_when_listed_successor_is_present() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let fixture =
            Fixture::with_boundaries(epoch, &[start, start + 2], None, Some(end + 7), true);
        let mut membership = fixture.membership.clone();
        membership.after = Some(end + 11);
        let mut malformed = fixture.correct.clone();
        malformed[2] = SlotRange::EMPTY;

        let error = repair_ranges(epoch, &malformed, &membership, &fixture.source)
            .expect_err("decoded successor must match the listed boundary");

        assert!(
            error
                .to_string()
                .contains(&format!("expected {}", end + 11)),
            "{error:#}"
        );
    }

    #[test]
    fn decodes_unlisted_adjacent_epoch_boundaries_from_car() {
        let epoch = 5;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let mut fixture = Fixture::with_boundaries(
            epoch,
            &[start + 1, start + 3],
            Some(start - 23),
            Some(end + 7),
            true,
        );
        fixture.membership.before = None;
        fixture.membership.after = None;
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange::EMPTY;
        malformed[3] = SlotRange::EMPTY;

        let repaired = repair_ranges(epoch, &malformed, &fixture.membership, &fixture.source)
            .expect("decode unlisted boundary Blocks");

        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn repairs_tail_to_exact_car_length_and_allows_trailing_root_nodes() {
        let fixture = Fixture::new(0, &[0, 1, 2], true);
        let mut malformed = fixture.correct.clone();
        malformed[2] = SlotRange::EMPTY;

        let repaired = repair_ranges(0, &malformed, &fixture.membership, &fixture.source)
            .expect("repair tail");

        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(
            repaired.stats.car_bytes_read,
            fixture.car.len() as u64 - malformed[0].end_exclusive().unwrap()
        );
    }

    #[test]
    fn rejects_segment_when_car_block_slot_does_not_match_membership() {
        let fixture = Fixture::new(0, &[0, 99, 2], false);
        let membership = SlotMembership {
            before: None,
            current: vec![0, 1, 2],
            after: None,
        };
        let mut malformed = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        malformed[0] = fixture.correct[0];
        malformed[2] = fixture.correct[2];

        let error = repair_ranges(0, &malformed, &membership, &fixture.source)
            .expect_err("wrong Block slot must fail");
        assert!(error.to_string().contains("expected 1"), "{error:#}");
    }

    #[test]
    fn rejects_wrong_raw_file_size() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("epoch-0-slot-ranges.raw");
        fs::write(&path, vec![0; 100]).unwrap();
        let error = read_raw_ranges(&path).expect_err("short raw must fail");
        assert!(error.to_string().contains("expected 5184000"));
    }

    #[test]
    fn classifies_real_boundary_slots_and_checks_adjacent_lists() {
        let temporary = tempfile::tempdir().unwrap();
        let epoch = 4;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let before = start - 17;
        let after = end + 11;
        fs::write(
            temporary.path().join("3.slots.txt"),
            format!("{}\n{before}\n", start - SLOTS_PER_EPOCH),
        )
        .unwrap();
        let current_path = temporary.path().join("4.slots.txt");
        fs::write(
            &current_path,
            format!("{before}\n{start}\n{}\n{after}\n", start + 2),
        )
        .unwrap();
        fs::write(
            temporary.path().join("5.slots.txt"),
            format!("{after}\n{}\n", end + 23),
        )
        .unwrap();

        let membership =
            read_slot_membership_with_neighbors(&current_path, Some(temporary.path()), epoch)
                .expect("read list with actual adjacent boundary Blocks");
        assert_eq!(
            membership,
            SlotMembership {
                before: Some(before),
                current: vec![start, start + 2],
                after: Some(after),
            }
        );

        fs::write(
            temporary.path().join("3.slots.txt"),
            format!("{}\n{}\n", start - SLOTS_PER_EPOCH, before - 1),
        )
        .unwrap();
        let error =
            read_slot_membership_with_neighbors(&current_path, Some(temporary.path()), epoch)
                .expect_err("mismatched neighbor must fail");
        assert!(
            error
                .to_string()
                .contains("differs from the last current slot")
        );
    }

    #[test]
    fn keeps_fully_coherent_index_without_reading_car_segments() {
        let fixture = Fixture::new(0, &[0, 1, 2], false);
        let repaired = repair_ranges(0, &fixture.correct, &fixture.membership, &fixture.source)
            .expect("validate coherent index");
        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.car_bytes_read, 0);
        assert_eq!(repaired.stats.kept_rows, 3);
        assert!(fixture.temporary.path().is_dir());
    }

    #[test]
    fn plans_selected_bytes_without_opening_car_segments() {
        let fixture = Fixture::new(0, &[0, 1, 2], false);
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange::EMPTY;

        let plan = build_repair_plan(0, &malformed, &fixture.membership, fixture.source.len())
            .expect("plan bounded repair");

        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.stats.repaired_rows, 2);
        assert_eq!(plan.segments[0].start, 0);
        assert_eq!(plan.segments[0].end, fixture.correct[2].offset);
    }

    #[test]
    fn expands_past_a_reversed_nominal_right_anchor() {
        let fixture = Fixture::new(0, &[0, 1, 2, 3, 4, 5, 6], false);
        let mut malformed = fixture.correct.clone();
        malformed[3] = SlotRange::EMPTY;
        let displaced_start = fixture.correct[1].offset;
        malformed[4] = SlotRange {
            offset: displaced_start,
            len: u32::try_from(fixture.correct[5].offset - displaced_start).unwrap(),
        };

        let plan = build_repair_plan(0, &malformed, &fixture.membership, fixture.source.len())
            .expect("plan must absorb a reversed nominal anchor");
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].first, 2);
        assert_eq!(plan.segments[0].last, 4);
        assert_eq!(plan.segments[0].start, fixture.correct[2].offset);
        assert_eq!(plan.segments[0].end, fixture.correct[5].offset);

        let repaired = execute_repair_plan(0, &fixture.membership, &fixture.source, plan)
            .expect("repair expanded segment");
        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn expands_past_a_displaced_right_anchor_at_the_left_range_end() {
        let fixture = Fixture::new(0, &[0, 1, 2, 3], false);
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange::EMPTY;
        let displaced_start = fixture.correct[1].offset;
        malformed[2] = SlotRange {
            offset: displaced_start,
            len: u32::try_from(fixture.correct[3].offset - displaced_start).unwrap(),
        };

        let plan = build_repair_plan(0, &malformed, &fixture.membership, fixture.source.len())
            .expect("plan must absorb a displaced nominal anchor");
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].first, 0);
        assert_eq!(plan.segments[0].last, 2);
        assert_eq!(plan.segments[0].start, 0);
        assert_eq!(plan.segments[0].end, fixture.correct[3].offset);

        let repaired = execute_repair_plan(0, &fixture.membership, &fixture.source, plan)
            .expect("repair expanded segment");
        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn full_car_checksum_mode_reads_once_and_rebuilds_all_members() {
        let fixture = Fixture::new(0, &[0, 1, 2], true);
        let source = MemoryUnvalidatedSource::new(fixture.car.clone());
        let mut malformed = fixture.correct.clone();
        malformed[1] = SlotRange::EMPTY;
        malformed[17] = fixture.correct[1];
        let mut plan = build_repair_plan(0, &malformed, &fixture.membership, source.len())
            .expect("build initial plan");
        force_full_car_plan(&mut plan, &fixture.membership, source.len())
            .expect("force one full-CAR segment");
        let expected_digest: [u8; 32] = Sha256::digest(&fixture.car).into();

        let repaired = execute_repair_plan_with_digest(
            0,
            &fixture.membership,
            &source,
            plan,
            Some(expected_digest),
        )
        .expect("verify full CAR and repair");

        assert_eq!(source.opens.get(), 1);
        assert_eq!(repaired.ranges, fixture.correct);
        assert_eq!(repaired.stats.segments, 1);
        assert_eq!(repaired.stats.repaired_rows, 3);
        assert_eq!(repaired.stats.kept_rows, 0);
        assert_eq!(repaired.stats.cleared_nonmember_rows, 1);
        assert_eq!(repaired.stats.car_bytes_read, source.len());
        assert_eq!(repaired.stats.car_bytes_rechecked, 0);
    }

    #[test]
    fn full_car_checksum_mode_allows_listed_boundaries_absent_from_car() {
        let epoch = 3;
        let start = epoch * SLOTS_PER_EPOCH;
        let end = start + SLOTS_PER_EPOCH;
        let fixture = Fixture::with_boundaries(epoch, &[start, start + 2], None, None, true);
        let source = MemoryUnvalidatedSource::new(fixture.car.clone());
        let membership = SlotMembership {
            before: Some(start - 17),
            current: fixture.membership.current.clone(),
            after: Some(end + 11),
        };
        let mut plan = build_repair_plan(epoch, &fixture.correct, &membership, source.len())
            .expect("build initial plan");
        force_full_car_plan(&mut plan, &membership, source.len())
            .expect("force one full-CAR segment");
        let expected_digest: [u8; 32] = Sha256::digest(&fixture.car).into();

        let repaired = execute_repair_plan_with_digest(
            epoch,
            &membership,
            &source,
            plan,
            Some(expected_digest),
        )
        .expect("verified slot-list boundaries can be absent from the full CAR");

        assert_eq!(source.opens.get(), 1);
        assert_eq!(repaired.ranges, fixture.correct);
    }

    #[test]
    fn full_car_checksum_mode_rejects_wrong_digest_after_one_read() {
        let fixture = Fixture::new(0, &[0, 1, 2], false);
        let source = MemoryUnvalidatedSource::new(fixture.car.clone());
        let mut plan = build_repair_plan(0, &fixture.correct, &fixture.membership, source.len())
            .expect("build initial plan");
        force_full_car_plan(&mut plan, &fixture.membership, source.len())
            .expect("force one full-CAR segment");

        let error = execute_repair_plan_with_digest(
            0,
            &fixture.membership,
            &source,
            plan,
            Some([0x55; 32]),
        )
        .expect_err("wrong whole-CAR digest must fail");

        assert_eq!(source.opens.get(), 1);
        assert!(error.to_string().contains("SHA-256 mismatch"), "{error:#}");
    }

    #[test]
    fn parses_upper_and_lower_case_sha256() {
        let lower = "7cd069372272ea081de4f3b2755ec56023669c062be50882bcfed19d667ede21";
        let upper = lower.to_ascii_uppercase();
        assert_eq!(
            parse_sha256_hex(lower).unwrap(),
            parse_sha256_hex(&upper).unwrap()
        );
        assert!(parse_sha256_hex(&lower[..63]).is_err());
        let mut invalid = lower.to_string();
        invalid.replace_range(12..13, "g");
        assert!(parse_sha256_hex(&invalid).is_err());
    }

    #[test]
    fn rejects_truncated_full_block_payload() {
        let mut car = vec![0];
        append_frame(&mut car, &[0x81, 0x00]);
        append_frame(&mut car, &[0x85, 0x02, 0x00]);
        let (_temporary, source) = local_source_for_car(&car);
        let membership = SlotMembership {
            before: None,
            current: vec![0],
            after: None,
        };
        let input = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];

        let error = repair_ranges(0, &input, &membership, &source)
            .expect_err("a truncated Block payload must fail full CBOR decoding");

        assert!(
            format!("{error:#}").contains("decode full Block node"),
            "{error:#}"
        );
    }

    #[test]
    fn rejects_block_payload_with_wrong_cid() {
        let mut car = vec![0];
        append_frame(&mut car, &[0x81, 0x00]);
        let cid_offset = append_frame(&mut car, &block_payload(0));
        car[cid_offset + 4] ^= 0x01;
        let (_temporary, source) = local_source_for_car(&car);
        let membership = SlotMembership {
            before: None,
            current: vec![0],
            after: None,
        };
        let input = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];

        let error = repair_ranges(0, &input, &membership, &source)
            .expect_err("a Block payload with the wrong CID must fail");

        assert!(
            error.to_string().contains("Block CID mismatch"),
            "{error:#}"
        );
    }

    #[test]
    fn no_clobber_keeps_existing_output_and_cleans_temporary_file() {
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("candidate.raw");
        let sentinel = b"keep this file";
        fs::write(&output, sentinel).unwrap();
        let ranges = marked_ranges(11);

        let error = write_raw_atomic(&output, &ranges, false)
            .expect_err("no-clobber publish must reject an existing output");

        assert!(error.to_string().contains("already exists"), "{error:#}");
        assert_eq!(fs::read(&output).unwrap(), sentinel);
        assert!(temporary_output_names(temporary.path()).is_empty());
    }

    #[test]
    fn overwrite_atomically_replaces_existing_output() {
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("candidate.raw");
        fs::write(&output, b"old contents").unwrap();
        let ranges = marked_ranges(12);

        write_raw_atomic(&output, &ranges, true).expect("replace an existing output");

        assert_eq!(read_raw_ranges(&output).unwrap(), ranges);
        assert!(temporary_output_names(temporary.path()).is_empty());
    }

    #[test]
    fn concurrent_no_clobber_writers_publish_exactly_one_complete_file() {
        let temporary = tempfile::tempdir().unwrap();
        let output = Arc::new(temporary.path().join("candidate.raw"));
        let first = Arc::new(marked_ranges(21));
        let second = Arc::new(marked_ranges(22));
        let barrier = Arc::new(Barrier::new(2));

        let spawn_writer = |ranges: Arc<Vec<SlotRange>>| {
            let output = Arc::clone(&output);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                write_raw_atomic(&output, &ranges, false)
            })
        };
        let left = spawn_writer(Arc::clone(&first));
        let right = spawn_writer(Arc::clone(&second));
        let left = left.join().expect("first writer did not panic");
        let right = right.join().expect("second writer did not panic");

        assert_ne!(left.is_ok(), right.is_ok());
        let published = read_raw_ranges(&output).unwrap();
        assert!(published == *first || published == *second);
        assert!(temporary_output_names(temporary.path()).is_empty());
    }

    #[test]
    fn stale_temporary_file_does_not_block_publish_or_get_removed() {
        let temporary = tempfile::tempdir().unwrap();
        let output = temporary.path().join("candidate.raw");
        let stale = temporary.path().join(".candidate.raw.tmp-stale");
        fs::write(&stale, b"stale sentinel").unwrap();
        let ranges = marked_ranges(31);

        write_raw_atomic(&output, &ranges, false).expect("ignore an unrelated stale temp file");

        assert_eq!(read_raw_ranges(&output).unwrap(), ranges);
        assert_eq!(fs::read(&stale).unwrap(), b"stale sentinel");
        assert_eq!(
            temporary_output_names(temporary.path()),
            vec![".candidate.raw.tmp-stale".to_string()]
        );
    }

    #[test]
    fn accepts_only_syntactically_strong_etags() {
        assert!(is_strong_etag("\"abc\""));
        assert!(is_strong_etag("\"\""));
        assert!(!is_strong_etag("W/\"abc\""));
        assert!(!is_strong_etag("abc"));
        assert!(!is_strong_etag("\"bad value\""));
        assert!(!is_strong_etag("\"bad\\\"value\""));
    }

    #[test]
    fn accepts_exact_full_body_200_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "123".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "identity".parse().unwrap());

        validate_http_full_body_headers(StatusCode::OK, &headers, 123, "fixture.car")
            .expect("accept exact unencoded full body");
    }

    #[test]
    fn rejects_ambiguous_full_body_200_headers() {
        let missing_length = HeaderMap::new();
        let error =
            validate_http_full_body_headers(StatusCode::OK, &missing_length, 123, "fixture.car")
                .expect_err("Content-Length is required");
        assert!(error.to_string().contains("no Content-Length"), "{error:#}");

        let mut wrong_length = HeaderMap::new();
        wrong_length.insert(CONTENT_LENGTH, "122".parse().unwrap());
        let error =
            validate_http_full_body_headers(StatusCode::OK, &wrong_length, 123, "fixture.car")
                .expect_err("Content-Length must match the probed CAR length");
        assert!(error.to_string().contains("expected 123"), "{error:#}");

        let mut ranged = HeaderMap::new();
        ranged.insert(CONTENT_LENGTH, "123".parse().unwrap());
        ranged.insert(CONTENT_RANGE, "bytes 0-122/123".parse().unwrap());
        let error = validate_http_full_body_headers(StatusCode::OK, &ranged, 123, "fixture.car")
            .expect_err("a full 200 response must not claim a range");
        assert!(
            error.to_string().contains("unexpected Content-Range"),
            "{error:#}"
        );

        let mut encoded = HeaderMap::new();
        encoded.insert(CONTENT_LENGTH, "123".parse().unwrap());
        encoded.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let error = validate_http_full_body_headers(StatusCode::OK, &encoded, 123, "fixture.car")
            .expect_err("encoded response is not the plain CAR byte stream");
        assert!(
            error.to_string().contains("unsupported Content-Encoding"),
            "{error:#}"
        );
    }

    fn append_block_group(car: &mut Vec<u8>, slot: u64) {
        append_frame(car, &[0x81, 0x00]);
        append_frame(car, &block_payload(slot));
    }

    fn append_frame(car: &mut Vec<u8>, payload: &[u8]) -> usize {
        append_uvarint(car, (36 + payload.len()) as u64);
        let cid_offset = car.len();
        car.extend_from_slice(Cid36::compute(payload).car_bytes());
        car.extend_from_slice(payload);
        cid_offset
    }

    fn local_source_for_car(car: &[u8]) -> (TempDir, LocalCarSource) {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("fixture.car");
        fs::write(&path, car).expect("write CAR fixture");
        let source = LocalCarSource::open(path).expect("open CAR fixture");
        (temporary, source)
    }

    fn marked_ranges(marker: u32) -> Vec<SlotRange> {
        let mut ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
        ranges[0] = SlotRange {
            offset: u64::from(marker),
            len: marker,
        };
        ranges
    }

    fn temporary_output_names(directory: &Path) -> Vec<String> {
        let mut names = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".candidate.raw.tmp-"))
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn block_payload(slot: u64) -> Vec<u8> {
        let mut payload = vec![0x85, 0x02];
        append_cbor_u64(&mut payload, slot);
        payload.extend_from_slice(&[
            0x80, // shredding
            0x80, // entries
            0x83, 0xf6, 0xf6, 0xf6, // SlotMeta
        ]);
        payload
    }

    fn append_cbor_u64(out: &mut Vec<u8>, value: u64) {
        match value {
            0..=23 => out.push(value as u8),
            24..=0xff => out.extend_from_slice(&[0x18, value as u8]),
            0x100..=0xffff => {
                out.push(0x19);
                out.extend_from_slice(&(value as u16).to_be_bytes());
            }
            0x1_0000..=0xffff_ffff => {
                out.push(0x1a);
                out.extend_from_slice(&(value as u32).to_be_bytes());
            }
            _ => {
                out.push(0x1b);
                out.extend_from_slice(&value.to_be_bytes());
            }
        }
    }

    fn append_uvarint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }
}

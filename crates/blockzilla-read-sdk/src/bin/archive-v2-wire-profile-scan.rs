//! Read-only Archive V2 instruction-enum profile scan.
//!
//! This tool does not hash or publish archive objects. It reads the minimum
//! generation objects needed to walk hot messages, validates the canonical
//! program family for structured instruction data, and reports whether the
//! bytes are already canonical Post, legacy Pre, mixed, ambiguous, or invalid.

use std::{
    env, fs,
    ops::Range,
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use blockzilla_format::{ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, CompactPubkey};
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2InstructionProgramSemantics, ArchiveV2MessageProjector,
    ArchiveV2WireProfile, CompiledPubkeyFilter, HashVerification, OpenOptions,
    PinnedLocalRangeSource, WireProfileAuditOutcome, manifest::TrustedGenerationIdentity,
};
use serde::Serialize;

const DEFAULT_SLOTS_PER_EPOCH: u64 = 432_000;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_PROGRESS_BLOCKS: u64 = 10_000;
const DEFAULT_WORKERS: usize = 4;

#[derive(Debug)]
struct Args {
    archive: PathBuf,
    epoch: u64,
    slots_per_epoch: u64,
    max_message_bytes: usize,
    progress_blocks: u64,
    workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Classification {
    CanonicalPost,
    CanonicalEquivalent,
    LegacyPre,
    MixedNeedsRepair,
    AmbiguousNeedsRepair,
    InvalidNeedsRepair,
    ScanError,
}

#[derive(Debug, Default, Serialize)]
struct Counts {
    blocks: u64,
    compressed_block_bytes: u64,
    uncompressed_block_bytes: u64,
    typed_messages: u64,
    raw_transaction_fallbacks: u64,
    post_only: u64,
    pre_only: u64,
    both_equivalent: u64,
    both_divergent: u64,
    invalid: u64,
}

impl Counts {
    fn merge(&mut self, other: Self) -> Result<(), String> {
        merge_count(&mut self.blocks, other.blocks, "block count")?;
        merge_count(
            &mut self.compressed_block_bytes,
            other.compressed_block_bytes,
            "compressed block-byte count",
        )?;
        merge_count(
            &mut self.uncompressed_block_bytes,
            other.uncompressed_block_bytes,
            "uncompressed block-byte count",
        )?;
        merge_count(
            &mut self.typed_messages,
            other.typed_messages,
            "typed message count",
        )?;
        merge_count(
            &mut self.raw_transaction_fallbacks,
            other.raw_transaction_fallbacks,
            "raw transaction fallback count",
        )?;
        merge_count(&mut self.post_only, other.post_only, "Post-only count")?;
        merge_count(&mut self.pre_only, other.pre_only, "Pre-only count")?;
        merge_count(
            &mut self.both_equivalent,
            other.both_equivalent,
            "equivalent count",
        )?;
        merge_count(
            &mut self.both_divergent,
            other.both_divergent,
            "divergent count",
        )?;
        merge_count(&mut self.invalid, other.invalid, "invalid count")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct Location {
    slot: u64,
    transaction_index: u32,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidLocation {
    slot: u64,
    transaction_index: u32,
    post_error: String,
    pre_error: String,
}

#[derive(Debug, Default, Serialize)]
struct FirstEvidence {
    post_only: Option<Location>,
    pre_only: Option<Location>,
    both_divergent: Option<Location>,
    invalid: Option<InvalidLocation>,
}

impl FirstEvidence {
    fn merge(&mut self, other: Self) {
        merge_location(&mut self.post_only, other.post_only);
        merge_location(&mut self.pre_only, other.pre_only);
        merge_location(&mut self.both_divergent, other.both_divergent);
        merge_invalid_location(&mut self.invalid, other.invalid);
    }
}

struct Progress {
    epoch: u64,
    interval: u64,
    blocks: AtomicU64,
    typed_messages: AtomicU64,
}

impl Progress {
    fn new(epoch: u64, interval: u64) -> Self {
        Self {
            epoch,
            interval,
            blocks: AtomicU64::new(0),
            typed_messages: AtomicU64::new(0),
        }
    }

    fn add_block(&self, typed_messages: u64) {
        let typed_messages = self
            .typed_messages
            .fetch_add(typed_messages, Ordering::Relaxed)
            + typed_messages;
        let blocks = self.blocks.fetch_add(1, Ordering::Relaxed) + 1;
        if self.interval != 0 && blocks.is_multiple_of(self.interval) {
            eprintln!(
                "epoch {}: {blocks} blocks, at least {typed_messages} typed messages",
                self.epoch
            );
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    kind: &'static str,
    archive: String,
    epoch: u64,
    workers: usize,
    classification: Classification,
    action: &'static str,
    counts: Counts,
    first_evidence: FirstEvidence,
    error: Option<String>,
    elapsed_seconds: f64,
    completed_unix_seconds: u64,
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}\n\n{}", usage());
            process::exit(2);
        }
    };
    let started = Instant::now();
    let archive = match fs::canonicalize(&args.archive) {
        Ok(archive) => archive,
        Err(error) => {
            print_report(error_report(
                &args,
                args.archive.display().to_string(),
                format!("cannot open archive directory: {error}"),
                started.elapsed().as_secs_f64(),
            ));
            return;
        }
    };
    let archive_display = archive.display().to_string();
    let report = match scan(&args, archive) {
        Ok((counts, first_evidence)) => {
            let classification = classify(&counts);
            Report {
                schema_version: 1,
                kind: "archive-v2-wire-profile-scan",
                archive: archive_display,
                epoch: args.epoch,
                workers: args.workers,
                classification,
                action: action(classification),
                counts,
                first_evidence,
                error: None,
                elapsed_seconds: started.elapsed().as_secs_f64(),
                completed_unix_seconds: unix_seconds(),
            }
        }
        Err(error) => error_report(
            &args,
            archive_display,
            error,
            started.elapsed().as_secs_f64(),
        ),
    };
    print_report(report);
}

fn scan(args: &Args, archive: PathBuf) -> Result<(Counts, FirstEvidence), String> {
    let source = PinnedLocalRangeSource::new(&archive);
    // Post is used only as the generation-wide reader binding. Block-envelope
    // decoding is profile-neutral. Each typed message is classified below.
    let reader = ArchiveReader::open_trusted(
        source.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".into(),
            epoch: args.epoch,
            generation_id: "read-only-wire-profile-scan".into(),
            slots_per_epoch: args.slots_per_epoch,
            wire_profile: ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .map_err(|error| format!("cannot validate Archive V2 container structure: {error}"))?;

    let system_program = solana_pubkey::pubkey!("11111111111111111111111111111111").to_bytes();
    let compute_budget_program =
        solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes();
    let vote_program =
        solana_pubkey::pubkey!("Vote111111111111111111111111111111111111111").to_bytes();
    let known_programs = reader
        .compile_pubkey_filter([system_program, compute_budget_program, vote_program])
        .map_err(|error| format!("cannot resolve canonical program IDs: {error}"))?;
    let programs = ProgramKeys {
        filter: &known_programs,
        system: &system_program,
        compute_budget: &compute_budget_program,
        vote: &vote_program,
    };
    let row_count = reader.index().rows.len();
    let ranges = block_ranges(row_count, args.workers);
    let progress = Progress::new(args.epoch, args.progress_blocks);
    let results = thread::scope(|scope| {
        let mut ranges = ranges.into_iter();
        let first_range = ranges.next().expect("block_ranges is never empty");
        let handles: Vec<_> = ranges
            .map(|range| {
                let reader = &reader;
                let programs = &programs;
                let progress = &progress;
                scope.spawn(move || scan_range(reader, programs, args, range, progress))
            })
            .collect();
        let mut results = vec![scan_range(&reader, &programs, args, first_range, &progress)];
        for handle in handles {
            results.push(
                handle
                    .join()
                    .map_err(|_| "a scan worker panicked".to_owned())?,
            );
        }
        Ok::<_, String>(results)
    })?;
    let mut counts = Counts::default();
    let mut first = FirstEvidence::default();
    for result in results {
        let (worker_counts, worker_first) = result?;
        counts.merge(worker_counts)?;
        first.merge(worker_first);
    }
    counts.compressed_block_bytes = reader.index().blob_file_bytes;
    counts.uncompressed_block_bytes = reader.index().rows.iter().try_fold(0u64, |total, row| {
        total
            .checked_add(u64::from(row.uncompressed_len))
            .ok_or_else(|| "uncompressed block-byte count overflow".to_owned())
    })?;
    source
        .verify_unchanged()
        .map_err(|error| format!("archive files changed during the scan: {error}"))?;
    Ok((counts, first))
}

fn scan_range(
    reader: &ArchiveReader<PinnedLocalRangeSource>,
    programs: &ProgramKeys<'_>,
    args: &Args,
    range: Range<usize>,
    progress: &Progress,
) -> Result<(Counts, FirstEvidence), String> {
    let post =
        ArchiveV2MessageProjector::new(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1);
    let pre =
        ArchiveV2MessageProjector::new(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1);
    let mut counts = Counts::default();
    let mut first = FirstEvidence::default();
    let mut blocks = reader
        .borrowed_blocks_without_rewards_range(range)
        .map_err(|error| format!("cannot start block scan: {error}"))?;

    while let Some(block) = blocks.next_block() {
        let block = block.map_err(|error| format!("cannot decode block frame: {error}"))?;
        increment(&mut counts.blocks, "block count")?;
        let typed_before = counts.typed_messages;
        let slot = block.header().slot;
        for row in block.tx_rows() {
            if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
                increment(
                    &mut counts.raw_transaction_fallbacks,
                    "raw transaction fallback count",
                )?;
                continue;
            }
            increment(&mut counts.typed_messages, "typed message count")?;
            let message_length = row.message_len as usize;
            if message_length > args.max_message_bytes {
                record_invalid(
                    &mut counts,
                    &mut first,
                    slot,
                    row.tx_index,
                    format!(
                        "message has {message_length} bytes, above the {} byte scan limit",
                        args.max_message_bytes
                    ),
                    "not attempted".into(),
                )?;
                continue;
            }
            let start = row.message_offset as usize;
            let Some(end) = start.checked_add(message_length) else {
                record_invalid(
                    &mut counts,
                    &mut first,
                    slot,
                    row.tx_index,
                    "message range overflows usize".into(),
                    "not attempted".into(),
                )?;
                continue;
            };
            let Some(bytes) = block.message_bytes().get(start..end) else {
                record_invalid(
                    &mut counts,
                    &mut first,
                    slot,
                    row.tx_index,
                    "message range is outside its block".into(),
                    "not attempted".into(),
                )?;
                continue;
            };
            classify_message(
                post,
                pre,
                programs,
                bytes,
                slot,
                row.tx_index,
                &mut counts,
                &mut first,
            )?;
        }
        progress.add_block(counts.typed_messages - typed_before);
    }
    Ok((counts, first))
}

#[allow(clippy::too_many_arguments)]
fn classify_message(
    post: ArchiveV2MessageProjector,
    pre: ArchiveV2MessageProjector,
    programs: &ProgramKeys<'_>,
    bytes: &[u8],
    slot: u64,
    transaction_index: u32,
    counts: &mut Counts,
    first: &mut FirstEvidence,
) -> Result<(), String> {
    let post_result = post
        .audit_alternate_profile_with_program_oracle(bytes, |program, semantics| {
            programs.matches(program, semantics)
        });
    match post_result {
        Ok(WireProfileAuditOutcome::SelectedOnly) => {
            increment(&mut counts.post_only, "Post-only message count")?;
            first.post_only.get_or_insert(Location {
                slot,
                transaction_index,
            });
        }
        Ok(WireProfileAuditOutcome::BothSemanticallyEquivalent) => {
            increment(&mut counts.both_equivalent, "equivalent message count")?;
        }
        Ok(WireProfileAuditOutcome::BothSemanticallyDivergent) => {
            increment(&mut counts.both_divergent, "divergent message count")?;
            first.both_divergent.get_or_insert(Location {
                slot,
                transaction_index,
            });
        }
        Err(post_error) => {
            let pre_result = pre
                .audit_alternate_profile_with_program_oracle(bytes, |program, semantics| {
                    programs.matches(program, semantics)
                });
            match pre_result {
                Ok(WireProfileAuditOutcome::SelectedOnly) => {
                    increment(&mut counts.pre_only, "Pre-only message count")?;
                    first.pre_only.get_or_insert(Location {
                        slot,
                        transaction_index,
                    });
                }
                Ok(other) => {
                    record_invalid(
                        counts,
                        first,
                        slot,
                        transaction_index,
                        post_error.to_string(),
                        format!("inconsistent alternate result: {other:?}"),
                    )?;
                }
                Err(pre_error) => {
                    record_invalid(
                        counts,
                        first,
                        slot,
                        transaction_index,
                        post_error.to_string(),
                        pre_error.to_string(),
                    )?;
                }
            }
        }
    }
    Ok(())
}

struct ProgramKeys<'a> {
    filter: &'a CompiledPubkeyFilter,
    system: &'a [u8; 32],
    compute_budget: &'a [u8; 32],
    vote: &'a [u8; 32],
}

impl ProgramKeys<'_> {
    fn matches(
        &self,
        program: CompactPubkey,
        semantics: ArchiveV2InstructionProgramSemantics,
    ) -> bool {
        match semantics {
            ArchiveV2InstructionProgramSemantics::Raw => true,
            ArchiveV2InstructionProgramSemantics::ComputeBudget => {
                self.filter.matches_reference(program, self.compute_budget)
            }
            ArchiveV2InstructionProgramSemantics::System => {
                self.filter.matches_reference(program, self.system)
            }
            ArchiveV2InstructionProgramSemantics::Vote => {
                self.filter.matches_reference(program, self.vote)
            }
        }
    }
}

fn record_invalid(
    counts: &mut Counts,
    first: &mut FirstEvidence,
    slot: u64,
    transaction_index: u32,
    post_error: String,
    pre_error: String,
) -> Result<(), String> {
    increment(&mut counts.invalid, "invalid message count")?;
    first.invalid.get_or_insert(InvalidLocation {
        slot,
        transaction_index,
        post_error,
        pre_error,
    });
    Ok(())
}

fn increment(value: &mut u64, label: &'static str) -> Result<(), String> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| format!("{label} overflow"))?;
    Ok(())
}

fn merge_count(target: &mut u64, value: u64, label: &'static str) -> Result<(), String> {
    *target = target
        .checked_add(value)
        .ok_or_else(|| format!("{label} overflow"))?;
    Ok(())
}

fn merge_location(target: &mut Option<Location>, candidate: Option<Location>) {
    let Some(candidate) = candidate else {
        return;
    };
    if target.as_ref().is_none_or(|current| {
        (candidate.slot, candidate.transaction_index) < (current.slot, current.transaction_index)
    }) {
        *target = Some(candidate);
    }
}

fn merge_invalid_location(
    target: &mut Option<InvalidLocation>,
    candidate: Option<InvalidLocation>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    if target.as_ref().is_none_or(|current| {
        (candidate.slot, candidate.transaction_index) < (current.slot, current.transaction_index)
    }) {
        *target = Some(candidate);
    }
}

fn block_ranges(row_count: usize, requested_workers: usize) -> Vec<Range<usize>> {
    let workers = requested_workers.max(1).min(row_count.max(1));
    let base = row_count / workers;
    let remainder = row_count % workers;
    let mut start = 0usize;
    let mut ranges = Vec::with_capacity(workers);
    for worker in 0..workers {
        let length = base + usize::from(worker < remainder);
        let end = start + length;
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn classify(counts: &Counts) -> Classification {
    if counts.invalid != 0 {
        Classification::InvalidNeedsRepair
    } else if counts.post_only != 0 && counts.pre_only != 0 {
        Classification::MixedNeedsRepair
    } else if counts.both_divergent != 0 {
        // Tags 5 and 6 are Vote instructions under both layouts but carry
        // different meanings. Without external producer evidence, accepting
        // either meaning would not be canonical.
        Classification::AmbiguousNeedsRepair
    } else if counts.pre_only != 0 {
        Classification::LegacyPre
    } else if counts.post_only != 0 {
        Classification::CanonicalPost
    } else {
        Classification::CanonicalEquivalent
    }
}

fn action(classification: Classification) -> &'static str {
    match classification {
        Classification::CanonicalPost | Classification::CanonicalEquivalent => "none",
        Classification::LegacyPre => "convert-to-post",
        Classification::MixedNeedsRepair
        | Classification::AmbiguousNeedsRepair
        | Classification::InvalidNeedsRepair => "repair-before-conversion",
        Classification::ScanError => "rescan",
    }
}

fn error_report(args: &Args, archive: String, error: String, elapsed_seconds: f64) -> Report {
    Report {
        schema_version: 1,
        kind: "archive-v2-wire-profile-scan",
        archive,
        epoch: args.epoch,
        workers: args.workers,
        classification: Classification::ScanError,
        action: action(Classification::ScanError),
        counts: Counts::default(),
        first_evidence: FirstEvidence::default(),
        error: Some(error),
        elapsed_seconds,
        completed_unix_seconds: unix_seconds(),
    }
}

fn print_report(report: Report) {
    match serde_json::to_string(&report) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("cannot serialize scan report: {error}");
            process::exit(1);
        }
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_args() -> Result<Args, String> {
    let mut archive = None;
    let mut epoch = None;
    let mut slots_per_epoch = DEFAULT_SLOTS_PER_EPOCH;
    let mut max_message_bytes = DEFAULT_MAX_MESSAGE_BYTES;
    let mut progress_blocks = DEFAULT_PROGRESS_BLOCKS;
    let mut workers = DEFAULT_WORKERS;
    let mut args = env::args_os().skip(1);
    while let Some(argument) = args.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "arguments must be valid UTF-8".to_owned())?;
        if argument == "--help" || argument == "-h" {
            println!("{}", usage());
            process::exit(0);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?;
        match argument.as_str() {
            "--archive" => archive = Some(PathBuf::from(value)),
            "--epoch" => epoch = Some(parse_number(value, "epoch")?),
            "--slots-per-epoch" => {
                slots_per_epoch = parse_number(value, "slots per epoch")?;
            }
            "--max-message-bytes" => {
                max_message_bytes = parse_number(value, "maximum message bytes")?;
            }
            "--progress-blocks" => {
                progress_blocks = parse_number(value, "progress block interval")?;
            }
            "--workers" => workers = parse_number(value, "worker count")?,
            _ => return Err(format!("unknown argument {argument:?}")),
        }
    }
    let archive = archive.ok_or_else(|| "--archive is required".to_owned())?;
    let epoch = epoch.ok_or_else(|| "--epoch is required".to_owned())?;
    if slots_per_epoch == 0 {
        return Err("--slots-per-epoch must be positive".into());
    }
    if max_message_bytes == 0 || max_message_bytes > DEFAULT_MAX_MESSAGE_BYTES {
        return Err(format!(
            "--max-message-bytes must be between 1 and {DEFAULT_MAX_MESSAGE_BYTES}"
        ));
    }
    if !(1..=64).contains(&workers) {
        return Err("--workers must be between 1 and 64".into());
    }
    Ok(Args {
        archive,
        epoch,
        slots_per_epoch,
        max_message_bytes,
        progress_blocks,
        workers,
    })
}

fn parse_number<T: std::str::FromStr>(value: std::ffi::OsString, label: &str) -> Result<T, String> {
    value
        .into_string()
        .map_err(|_| format!("{label} must be valid UTF-8"))?
        .parse()
        .map_err(|_| format!("{label} is not a valid number"))
}

fn usage() -> &'static str {
    "Usage: archive-v2-wire-profile-scan --archive ABSOLUTE_EPOCH_DIR --epoch N \\
     [--slots-per-epoch 432000] [--max-message-bytes 16777216] \\
     [--progress-blocks 10000] [--workers 4]"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(post: u64, pre: u64, equivalent: u64, divergent: u64, invalid: u64) -> Counts {
        Counts {
            post_only: post,
            pre_only: pre,
            both_equivalent: equivalent,
            both_divergent: divergent,
            invalid,
            ..Counts::default()
        }
    }

    #[test]
    fn classification_is_post_canonical_and_fail_closed() {
        assert_eq!(
            classify(&counts(1, 0, 2, 0, 0)),
            Classification::CanonicalPost
        );
        assert_eq!(
            classify(&counts(0, 0, 2, 0, 0)),
            Classification::CanonicalEquivalent
        );
        assert_eq!(classify(&counts(0, 1, 2, 0, 0)), Classification::LegacyPre);
        assert_eq!(
            classify(&counts(1, 1, 0, 0, 0)),
            Classification::MixedNeedsRepair
        );
        assert_eq!(
            classify(&counts(1, 0, 0, 1, 0)),
            Classification::AmbiguousNeedsRepair
        );
        assert_eq!(
            classify(&counts(1, 0, 0, 0, 1)),
            Classification::InvalidNeedsRepair
        );
    }

    #[test]
    fn worker_ranges_are_contiguous_complete_and_bounded() {
        assert_eq!(block_ranges(0, 4), vec![0..0]);
        assert_eq!(block_ranges(3, 8), vec![0..1, 1..2, 2..3]);
        assert_eq!(block_ranges(10, 4), vec![0..3, 3..6, 6..8, 8..10]);
    }
}

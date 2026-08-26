//! Build every required derived index with bounded aggregate sort memory.

use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use blockzilla_index_archive_format::ArchiveId;

use crate::{
    account_index::{AccountIndexBuildOptions, AccountIndexBuildReport, build_account_index},
    basic_indexes::{BasicIndexBuildOptions, BasicIndexBuildReport, build_basic_indexes},
    program_index::{ProgramIndexBuildOptions, ProgramIndexBuildReport, build_program_index},
    selector_index::{SelectorIndexBuildOptions, SelectorIndexBuildReport, build_selector_index},
};

const BUILDER_COUNT: usize = 4;
const SORT_BUILDER_COUNT: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedIndexBuildOptions {
    /// Maximum builders that can run at the same time. Values above four are
    /// accepted but clamped because there are four independent builder jobs.
    pub workers: usize,
    /// Aggregate memory declared for external-sort buffers across active jobs.
    pub total_sort_memory_bytes: usize,
}

impl Default for DerivedIndexBuildOptions {
    fn default() -> Self {
        Self {
            workers: 1,
            total_sort_memory_bytes: 512 << 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BasicDerivedIndexReport {
    pub slots_bytes: u64,
    pub transactions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountDerivedIndexReport {
    pub object_bytes: u64,
    pub blocks: u64,
    pub transactions: u64,
    pub postings: u64,
    pub distinct_accounts: u64,
    pub sort_runs: u64,
    pub merge_passes: u32,
    pub pages: u64,
    pub continuation_pages: u64,
    pub max_postings_per_page: usize,
    pub peak_page_postings: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramDerivedIndexReport {
    pub object_bytes: u64,
    pub postings: u64,
    pub distinct_programs: u64,
    pub sort_runs: u64,
    pub continuation_pages: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectorDerivedIndexReport {
    pub object_bytes: u64,
    pub postings: u64,
    pub sort_runs: u64,
    pub merge_passes: u32,
    pub continuation_pages: u64,
}

/// Plain summary for logs, JSON adapters, or publication validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedIndexBuildReport {
    pub archive_id: ArchiveId,
    pub workers_used: usize,
    pub total_sort_memory_bytes: usize,
    pub sort_memory_per_active_builder_bytes: usize,
    pub blocks: u64,
    pub elapsed: Duration,
    pub basic: BasicDerivedIndexReport,
    pub accounts: AccountDerivedIndexReport,
    pub programs: ProgramDerivedIndexReport,
    pub selectors: SelectorDerivedIndexReport,
}

#[derive(Debug)]
enum BuilderReport {
    Basic(BasicIndexBuildReport),
    Accounts(AccountIndexBuildReport),
    Programs(ProgramIndexBuildReport),
    Selectors(SelectorIndexBuildReport),
}

#[derive(Debug, Clone, Copy)]
enum BuilderKind {
    Basic,
    Accounts,
    Programs,
    Selectors,
}

fn run_builder(root: &Path, kind: BuilderKind, sort_memory_bytes: usize) -> Result<BuilderReport> {
    Ok(match kind {
        BuilderKind::Basic => {
            BuilderReport::Basic(build_basic_indexes(root, BasicIndexBuildOptions)?)
        }
        BuilderKind::Accounts => BuilderReport::Accounts(build_account_index(
            root,
            AccountIndexBuildOptions { sort_memory_bytes },
        )?),
        BuilderKind::Programs => BuilderReport::Programs(build_program_index(
            root,
            ProgramIndexBuildOptions {
                sort_memory_bytes,
                ..Default::default()
            },
        )?),
        BuilderKind::Selectors => BuilderReport::Selectors(build_selector_index(
            root,
            SelectorIndexBuildOptions {
                sort_memory_bytes,
                ..Default::default()
            },
        )?),
    })
}

fn run_batch(
    root: &Path,
    kinds: &[BuilderKind],
    sort_memory_bytes: usize,
) -> Result<Vec<BuilderReport>> {
    if kinds.len() == 1 {
        return Ok(vec![run_builder(root, kinds[0], sort_memory_bytes)?]);
    }
    thread::scope(|scope| {
        let handles = kinds
            .iter()
            .copied()
            .map(|kind| scope.spawn(move || run_builder(root, kind, sort_memory_bytes)))
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("derived-index builder thread panicked"))?
            })
            .collect()
    })
}

/// Build slots, accounts, programs, and selectors.
///
/// The slot builder does not sort. The caller's total sort-memory budget is
/// divided by the maximum number of active sorting jobs, which bounds the
/// aggregate declared sort buffers without charging the slot job.
pub fn build_all_derived_indexes(
    root: &Path,
    options: DerivedIndexBuildOptions,
) -> Result<DerivedIndexBuildReport> {
    ensure!(
        options.workers > 0,
        "derived-index worker count must be positive"
    );
    let workers_used = options.workers.min(BUILDER_COUNT);
    let active_sort_builders = workers_used.min(SORT_BUILDER_COUNT);
    ensure!(
        options.total_sort_memory_bytes >= active_sort_builders,
        "derived-index sort-memory budget is too small for {active_sort_builders} active sorting builders"
    );
    let per_builder = options.total_sort_memory_bytes / active_sort_builders;
    // The strictest fixed-width run record is the selector's 40-byte record.
    ensure!(
        per_builder >= 40,
        "each active derived-index builder needs at least 40 sort-memory bytes"
    );
    ensure!(
        root.is_dir(),
        "{} is not an archive directory",
        root.display()
    );

    let started = Instant::now();
    let kinds = [
        BuilderKind::Basic,
        BuilderKind::Accounts,
        BuilderKind::Programs,
        BuilderKind::Selectors,
    ];
    let mut reports = Vec::with_capacity(BUILDER_COUNT);
    for batch in kinds.chunks(workers_used) {
        reports.extend(run_batch(root, batch, per_builder)?);
    }

    let mut basic = None;
    let mut accounts = None;
    let mut programs = None;
    let mut selectors = None;
    for report in reports {
        match report {
            BuilderReport::Basic(report) => basic = Some(report),
            BuilderReport::Accounts(report) => accounts = Some(report),
            BuilderReport::Programs(report) => programs = Some(report),
            BuilderReport::Selectors(report) => selectors = Some(report),
        }
    }
    let basic = basic.context("basic derived-index builder returned no report")?;
    let accounts = accounts.context("account derived-index builder returned no report")?;
    let programs = programs.context("program derived-index builder returned no report")?;
    let selectors = selectors.context("selector derived-index builder returned no report")?;
    for (name, archive_id) in [
        ("accounts", accounts.archive_id),
        ("programs", programs.archive_id),
        ("selectors", selectors.archive_id),
    ] {
        ensure!(
            archive_id == basic.archive_id,
            "{name} derived-index builder reports a different archive ID"
        );
    }
    ensure!(
        accounts.blocks == basic.blocks
            && programs.blocks == basic.blocks
            && selectors.blocks == basic.blocks,
        "derived-index builders report different block counts"
    );
    ensure!(
        accounts.transactions == basic.transactions
            && programs.transactions == basic.transactions
            && selectors.transactions == basic.transactions,
        "derived-index builders report different transaction counts"
    );

    Ok(DerivedIndexBuildReport {
        archive_id: basic.archive_id,
        workers_used,
        total_sort_memory_bytes: options.total_sort_memory_bytes,
        sort_memory_per_active_builder_bytes: per_builder,
        blocks: basic.blocks,
        elapsed: started.elapsed(),
        basic: BasicDerivedIndexReport {
            slots_bytes: basic.slots_object_bytes,
            transactions: basic.transactions,
        },
        accounts: AccountDerivedIndexReport {
            object_bytes: accounts.object_bytes,
            blocks: accounts.blocks,
            transactions: accounts.transactions,
            postings: accounts.postings,
            distinct_accounts: accounts.distinct_accounts,
            sort_runs: accounts.sort_runs,
            merge_passes: accounts.merge_passes,
            pages: accounts.pages,
            continuation_pages: accounts.continuation_pages,
            max_postings_per_page: accounts.max_postings_per_page,
            peak_page_postings: accounts.peak_page_postings,
        },
        programs: ProgramDerivedIndexReport {
            object_bytes: programs.object_bytes,
            postings: programs.postings,
            distinct_programs: programs.distinct_programs,
            sort_runs: programs.sort_runs,
            continuation_pages: programs.continuation_pages,
        },
        selectors: SelectorDerivedIndexReport {
            object_bytes: selectors.object_bytes,
            postings: selectors.postings,
            sort_runs: selectors.sort_runs,
            merge_passes: selectors.merge_passes,
            continuation_pages: selectors.continuation_pages,
        },
    })
}

/// Owned path helper for callers that dispatch the operation to another task.
pub fn build_all_derived_indexes_owned(
    root: PathBuf,
    options: DerivedIndexBuildOptions,
) -> Result<DerivedIndexBuildReport> {
    build_all_derived_indexes(&root, options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_count_and_memory_split_are_validated() {
        let missing = Path::new("/definitely-not-an-index-archive");
        let error = build_all_derived_indexes(
            missing,
            DerivedIndexBuildOptions {
                workers: 0,
                total_sort_memory_bytes: 1,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("worker count must be positive"));

        let error = build_all_derived_indexes(
            missing,
            DerivedIndexBuildOptions {
                workers: 4,
                total_sort_memory_bytes: 119,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("at least 40"));
    }

    #[test]
    fn default_budget_is_large_enough_for_all_sort_builders() {
        let options = DerivedIndexBuildOptions::default();
        assert_eq!(options.workers, 1);
        assert!(options.total_sort_memory_bytes >= 40);
        let parallel = DerivedIndexBuildOptions {
            workers: usize::MAX,
            total_sort_memory_bytes: 160,
        };
        assert_eq!(parallel.workers.min(BUILDER_COUNT), 4);
        assert_eq!(parallel.total_sort_memory_bytes / SORT_BUILDER_COUNT, 53);
    }
}

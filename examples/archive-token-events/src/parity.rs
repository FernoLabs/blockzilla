//! Exact, bounded-memory comparison of three token-event SQLite databases.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, Rows, types::ValueRef};
use serde::Serialize;
use serde_json::{Map, Value};
use solana_sha256_hasher::Hasher as Sha256Hasher;

use blockzilla_dump::{TokenEventAudit, TokenEventDatabase};
use blockzilla_query_sdk::token::HistoryCoverage;

const DATASET_DIGEST_DOMAIN: &[u8] = b"blockzilla.token-event-parity.dataset.v1\0";
const FORMAT_NAMES: [&str; 3] = ["car", "compact-v2", "indexer-v3"];

#[derive(Debug, Clone, Serialize)]
pub struct DatabaseSummary {
    pub path: PathBuf,
    pub complete: bool,
    pub next_block_ordinal: u32,
    pub expected_end_block_ordinal: u32,
    pub digest_head: String,
    pub tracker_digest: String,
    pub row_counts: BTreeMap<String, u64>,
    pub coverage: CoverageStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct CoverageStatus {
    pub tracker_history: String,
    pub issue_count: u64,
    pub issues_by_kind: BTreeMap<String, u64>,
    pub has_gaps: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComparisonReport {
    pub schema: &'static str,
    pub status: &'static str,
    pub comparison_method: &'static str,
    pub mismatch_sample_limit: usize,
    pub all_databases_complete: bool,
    pub databases: BTreeMap<String, DatabaseSummary>,
    pub source_projection_parity: SourceProjectionParity,
    pub canonical_source_digest_parity: CategoryReport,
    pub token_event_parity: CategoryReport,
    pub coverage_parity: CategoryReport,
    pub all_ledger_parity: CategoryReport,
    pub datasets: Vec<DatasetReport>,
    pub mismatch_samples: Vec<MismatchSample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CategoryReport {
    pub exact_equal: bool,
    pub datasets: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceProjectionParity {
    pub status: &'static str,
    pub full_canonical_values_compared: bool,
    pub canonical_block_view_digest_equal: bool,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetReport {
    pub name: &'static str,
    pub category: DatasetCategory,
    pub exact_equal: bool,
    pub formats: BTreeMap<String, DatasetSummary>,
    pub pairwise: Vec<PairwiseReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DatasetCategory {
    CanonicalSourceDigest,
    TokenEvent,
    Coverage,
    Tracker,
    LedgerControl,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetSummary {
    pub rows: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairwiseReport {
    pub pair: String,
    pub exact_equal: bool,
    pub matching_rows: u64,
    pub differing_values: u64,
    pub left_only_rows: u64,
    pub right_only_rows: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MismatchSample {
    pub dataset: &'static str,
    pub pair: String,
    pub kind: &'static str,
    pub key: Value,
    pub left: Option<Value>,
    pub right: Option<Value>,
}

struct ValidatedDatabase {
    connection: Connection,
    summary: DatabaseSummary,
}

#[derive(Clone, Copy)]
struct DatasetSpec {
    name: &'static str,
    category: DatasetCategory,
    key_columns: usize,
    sql: &'static str,
}

struct CanonicalRow {
    key: Vec<u8>,
    encoded: Vec<u8>,
    key_display: Value,
    display: Value,
}

/// Compare the current durable rows of all three databases. Completion is
/// reported separately, so an equal partial prefix cannot be mistaken for a
/// complete archive result.
pub fn compare_output_databases(
    car: &Path,
    compact_v2: &Path,
    indexer_v3: &Path,
    mismatch_limit: usize,
) -> Result<ComparisonReport> {
    ensure!(mismatch_limit <= 20, "mismatch limit must be in 0..=20");
    let databases = [
        open_validated_database(car)?,
        open_validated_database(compact_v2)?,
        open_validated_database(indexer_v3)?,
    ];

    let mut database_reports = BTreeMap::new();
    for (name, database) in FORMAT_NAMES.iter().zip(&databases) {
        database_reports.insert((*name).to_owned(), database.summary.clone());
    }

    let mut datasets = Vec::with_capacity(DATASETS.len());
    let mut samples = Vec::new();
    for spec in DATASETS {
        let mut summaries = BTreeMap::new();
        for (name, database) in FORMAT_NAMES.iter().zip(&databases) {
            summaries.insert(
                (*name).to_owned(),
                summarize_dataset(&database.connection, spec)?,
            );
        }

        let mut pairwise = Vec::with_capacity(3);
        for (left, right) in [(0, 1), (0, 2), (1, 2)] {
            pairwise.push(compare_pair(
                &databases[left].connection,
                FORMAT_NAMES[left],
                &databases[right].connection,
                FORMAT_NAMES[right],
                spec,
                mismatch_limit,
                &mut samples,
            )?);
        }
        let exact_equal = pairwise.iter().all(|pair| pair.exact_equal);
        datasets.push(DatasetReport {
            name: spec.name,
            category: spec.category,
            exact_equal,
            formats: summaries,
            pairwise,
        });
    }

    let canonical_source_digest_parity = category_report(&datasets, |category| {
        category == DatasetCategory::CanonicalSourceDigest
    });
    let source_projection_parity = SourceProjectionParity {
        status: "not-proved-full-row",
        full_canonical_values_compared: false,
        canonical_block_view_digest_equal: canonical_source_digest_parity.exact_equal,
        reason: "the SQLite ledger keeps one SHA-256 source digest per full canonical BlockView; this command compares those digests but does not retain a second full source-projection table",
    };
    let token_event_parity = category_report(&datasets, |category| {
        category == DatasetCategory::TokenEvent
    });
    let coverage_parity =
        category_report(&datasets, |category| category == DatasetCategory::Coverage);
    let all_ledger_parity = category_report(&datasets, |_| true);
    let all_databases_complete = databases.iter().all(|database| database.summary.complete);

    Ok(ComparisonReport {
        schema: "blockzilla-archive-token-events/comparison-v1",
        status: "complete",
        comparison_method: "token, coverage, tracker, and ledger rows are merge-compared as full canonical values with raw 32-byte public-key addresses instead of database-local IDs; full BlockView values are represented only by stored per-block source digests",
        mismatch_sample_limit: mismatch_limit,
        all_databases_complete,
        databases: database_reports,
        source_projection_parity,
        canonical_source_digest_parity,
        token_event_parity,
        coverage_parity,
        all_ledger_parity,
        datasets,
        mismatch_samples: samples,
    })
}

/// Open one database read-only and return its validated operational summary.
pub fn database_summary(path: &Path) -> Result<DatabaseSummary> {
    Ok(open_validated_database(path)?.summary)
}

fn category_report(
    datasets: &[DatasetReport],
    select: impl Fn(DatasetCategory) -> bool,
) -> CategoryReport {
    let selected = datasets
        .iter()
        .filter(|dataset| select(dataset.category))
        .collect::<Vec<_>>();
    CategoryReport {
        exact_equal: selected.iter().all(|dataset| dataset.exact_equal),
        datasets: selected.iter().map(|dataset| dataset.name).collect(),
    }
}

fn open_validated_database(path: &Path) -> Result<ValidatedDatabase> {
    let audit = TokenEventDatabase::audit_read_only(path)
        .with_context(|| format!("audit token-event database {} read-only", path.display()))?;
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .with_context(|| format!("open token-event database {} read-only", path.display()))?;
    connection
        .pragma_update(None, "query_only", "ON")
        .context("enable SQLite query-only mode")?;
    connection
        .pragma_update(None, "trusted_schema", "OFF")
        .context("disable trusted SQLite schema")?;
    let summary = load_database_summary(&connection, path, &audit)?;
    Ok(ValidatedDatabase {
        connection,
        summary,
    })
}

fn load_database_summary(
    connection: &Connection,
    path: &Path,
    audit: &TokenEventAudit,
) -> Result<DatabaseSummary> {
    let next = audit.resume.next_block_ordinal;
    let first = audit.spec.range.first_block;
    let count = audit.spec.range.block_count.get();
    let expected_end = first
        .checked_add(count)
        .context("token-event range end exceeds u32")?;
    ensure!(
        next >= first && next <= expected_end,
        "token-event checkpoint is outside its run range"
    );

    let mut row_counts = BTreeMap::new();
    for table in PHYSICAL_TABLES {
        let sql = format!("SELECT count(*) FROM \"{table}\"");
        let count: i64 = connection.query_row(&sql, [], |row| row.get(0))?;
        let count = u64::try_from(count).context("negative SQLite row count")?;
        row_counts.insert((*table).to_owned(), count);
    }

    let tracker_history = match audit.resume.tracker.history_coverage() {
        HistoryCoverage::Complete => "complete",
        HistoryCoverage::Partial => "partial",
    }
    .to_owned();
    let mut issues_by_kind = BTreeMap::new();
    let mut statement = connection.prepare(
        "SELECT issue_kind, count(*)
           FROM coverage_issues
          GROUP BY issue_kind
          ORDER BY issue_kind",
    )?;
    let mut rows = statement.query([])?;
    let mut issue_count = 0u64;
    while let Some(row) = rows.next()? {
        let kind: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        let count = u64::try_from(count).context("negative coverage issue count")?;
        issue_count = issue_count
            .checked_add(count)
            .context("coverage issue count overflow")?;
        issues_by_kind.insert(kind, count);
    }
    let has_gaps = tracker_history != "complete" || issue_count != 0;

    Ok(DatabaseSummary {
        path: path.to_path_buf(),
        complete: next == expected_end,
        next_block_ordinal: next,
        expected_end_block_ordinal: expected_end,
        digest_head: hex_lower(&audit.digest_head),
        tracker_digest: hex_lower(&audit.tracker_digest),
        row_counts,
        coverage: CoverageStatus {
            tracker_history,
            issue_count,
            issues_by_kind,
            has_gaps,
        },
    })
}

fn summarize_dataset(connection: &Connection, spec: &DatasetSpec) -> Result<DatasetSummary> {
    let mut statement = connection.prepare(spec.sql)?;
    let column_count = statement.column_count();
    ensure!(
        spec.key_columns > 0 && spec.key_columns <= column_count,
        "dataset {} has an invalid key width",
        spec.name
    );
    let mut rows = statement.query([])?;
    let mut digest = Sha256Hasher::default();
    digest.hash(DATASET_DIGEST_DOMAIN);
    digest.hash(&(spec.name.len() as u64).to_le_bytes());
    digest.hash(spec.name.as_bytes());
    let mut count = 0u64;
    let mut previous_key = None;
    while let Some(row) = next_canonical_row(&mut rows, column_count, spec.key_columns)? {
        require_increasing_key(spec, &mut previous_key, &row.key)?;
        digest.hash(&(row.encoded.len() as u64).to_le_bytes());
        digest.hash(&row.encoded);
        count = count.checked_add(1).context("dataset row count overflow")?;
    }
    Ok(DatasetSummary {
        rows: count,
        sha256: hex_lower(digest.result().as_ref()),
    })
}

#[allow(clippy::too_many_arguments)]
fn compare_pair(
    left: &Connection,
    left_name: &str,
    right: &Connection,
    right_name: &str,
    spec: &DatasetSpec,
    sample_limit: usize,
    samples: &mut Vec<MismatchSample>,
) -> Result<PairwiseReport> {
    let mut left_statement = left.prepare(spec.sql)?;
    let mut right_statement = right.prepare(spec.sql)?;
    let left_columns = left_statement.column_count();
    let right_columns = right_statement.column_count();
    ensure!(
        left_columns == right_columns,
        "dataset column count differs"
    );
    let mut left_rows = left_statement.query([])?;
    let mut right_rows = right_statement.query([])?;
    let mut previous_left = None;
    let mut previous_right = None;
    let mut left_row = next_checked_row(&mut left_rows, left_columns, spec, &mut previous_left)?;
    let mut right_row =
        next_checked_row(&mut right_rows, right_columns, spec, &mut previous_right)?;
    let mut matching = 0u64;
    let mut differing = 0u64;
    let mut left_only = 0u64;
    let mut right_only = 0u64;
    let pair = format!("{left_name}-vs-{right_name}");

    while left_row.is_some() || right_row.is_some() {
        match (&left_row, &right_row) {
            (Some(left_value), Some(right_value)) if left_value.key == right_value.key => {
                if left_value.encoded == right_value.encoded {
                    matching = checked_increment(matching, "matching row count")?;
                } else {
                    differing = checked_increment(differing, "differing row count")?;
                    push_sample(
                        samples,
                        sample_limit,
                        spec.name,
                        &pair,
                        "different-values",
                        left_value.key_display.clone(),
                        Some(left_value.display.clone()),
                        Some(right_value.display.clone()),
                    );
                }
                left_row =
                    next_checked_row(&mut left_rows, left_columns, spec, &mut previous_left)?;
                right_row =
                    next_checked_row(&mut right_rows, right_columns, spec, &mut previous_right)?;
            }
            (Some(left_value), Some(right_value)) if left_value.key < right_value.key => {
                left_only = checked_increment(left_only, "left-only row count")?;
                push_sample(
                    samples,
                    sample_limit,
                    spec.name,
                    &pair,
                    "left-only",
                    left_value.key_display.clone(),
                    Some(left_value.display.clone()),
                    None,
                );
                left_row =
                    next_checked_row(&mut left_rows, left_columns, spec, &mut previous_left)?;
            }
            (Some(_), Some(right_value)) => {
                right_only = checked_increment(right_only, "right-only row count")?;
                push_sample(
                    samples,
                    sample_limit,
                    spec.name,
                    &pair,
                    "right-only",
                    right_value.key_display.clone(),
                    None,
                    Some(right_value.display.clone()),
                );
                right_row =
                    next_checked_row(&mut right_rows, right_columns, spec, &mut previous_right)?;
            }
            (Some(left_value), None) => {
                left_only = checked_increment(left_only, "left-only row count")?;
                push_sample(
                    samples,
                    sample_limit,
                    spec.name,
                    &pair,
                    "left-only",
                    left_value.key_display.clone(),
                    Some(left_value.display.clone()),
                    None,
                );
                left_row =
                    next_checked_row(&mut left_rows, left_columns, spec, &mut previous_left)?;
            }
            (None, Some(right_value)) => {
                right_only = checked_increment(right_only, "right-only row count")?;
                push_sample(
                    samples,
                    sample_limit,
                    spec.name,
                    &pair,
                    "right-only",
                    right_value.key_display.clone(),
                    None,
                    Some(right_value.display.clone()),
                );
                right_row =
                    next_checked_row(&mut right_rows, right_columns, spec, &mut previous_right)?;
            }
            (None, None) => break,
        }
    }

    Ok(PairwiseReport {
        pair,
        exact_equal: differing == 0 && left_only == 0 && right_only == 0,
        matching_rows: matching,
        differing_values: differing,
        left_only_rows: left_only,
        right_only_rows: right_only,
    })
}

fn next_checked_row(
    rows: &mut Rows<'_>,
    column_count: usize,
    spec: &DatasetSpec,
    previous: &mut Option<Vec<u8>>,
) -> Result<Option<CanonicalRow>> {
    let row = next_canonical_row(rows, column_count, spec.key_columns)?;
    if let Some(row) = &row {
        require_increasing_key(spec, previous, &row.key)?;
    }
    Ok(row)
}

fn require_increasing_key(
    spec: &DatasetSpec,
    previous: &mut Option<Vec<u8>>,
    current: &[u8],
) -> Result<()> {
    if let Some(previous) = previous {
        ensure!(
            previous.as_slice() < current,
            "dataset {} has a duplicate or unordered semantic key",
            spec.name
        );
    }
    *previous = Some(current.to_vec());
    Ok(())
}

fn checked_increment(value: u64, label: &str) -> Result<u64> {
    value
        .checked_add(1)
        .with_context(|| format!("{label} overflow"))
}

#[allow(clippy::too_many_arguments)]
fn push_sample(
    samples: &mut Vec<MismatchSample>,
    limit: usize,
    dataset: &'static str,
    pair: &str,
    kind: &'static str,
    key: Value,
    left: Option<Value>,
    right: Option<Value>,
) {
    if samples.len() < limit {
        samples.push(MismatchSample {
            dataset,
            pair: pair.to_owned(),
            kind,
            key,
            left,
            right,
        });
    }
}

fn next_canonical_row(
    rows: &mut Rows<'_>,
    column_count: usize,
    key_columns: usize,
) -> Result<Option<CanonicalRow>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut key = Vec::new();
    let mut encoded = Vec::new();
    let mut key_display = Vec::with_capacity(key_columns);
    let mut display = Vec::with_capacity(column_count);
    for column in 0..column_count {
        let value = row.get_ref(column)?;
        encode_value(&mut encoded, value);
        let shown = display_value(value);
        if column < key_columns {
            encode_value(&mut key, value);
            key_display.push(shown.clone());
        }
        display.push(shown);
    }
    Ok(Some(CanonicalRow {
        key,
        encoded,
        key_display: Value::Array(key_display),
        display: Value::Array(display),
    }))
}

fn encode_value(output: &mut Vec<u8>, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => output.push(0),
        ValueRef::Integer(value) => {
            output.push(1);
            output.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
        }
        ValueRef::Real(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        ValueRef::Text(value) => {
            output.push(3);
            output.extend_from_slice(&(value.len() as u64).to_be_bytes());
            output.extend_from_slice(value);
        }
        ValueRef::Blob(value) => {
            output.push(4);
            output.extend_from_slice(&(value.len() as u64).to_be_bytes());
            output.extend_from_slice(value);
        }
    }
}

fn display_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::from(value),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        ValueRef::Text(value) => match std::str::from_utf8(value) {
            Ok(value) => Value::String(value.to_owned()),
            Err(_) => blob_value(value),
        },
        ValueRef::Blob(value) => blob_value(value),
    }
}

fn blob_value(value: &[u8]) -> Value {
    const DISPLAY_BYTES: usize = 64;
    let mut object = Map::new();
    object.insert("bytes".into(), Value::from(value.len() as u64));
    if value.len() <= DISPLAY_BYTES {
        object.insert("hex".into(), Value::String(hex_lower(value)));
    } else {
        object.insert(
            "hex_prefix".into(),
            Value::String(hex_lower(&value[..DISPLAY_BYTES])),
        );
        object.insert("truncated".into(), Value::Bool(true));
    }
    Value::Object(object)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

const DATASETS: &[DatasetSpec] = &[
    DatasetSpec {
        name: "run-semantics",
        category: DatasetCategory::LedgerControl,
        key_columns: 1,
        sql: "SELECT r.singleton, mint.address, program.address,
                     r.first_block_ordinal, r.range_block_count
                FROM run_identity r
                JOIN pubkeys mint ON mint.pubkey_id = r.target_mint_pubkey_id
                JOIN pubkeys program ON program.pubkey_id = r.token_program_pubkey_id
               ORDER BY r.singleton",
    },
    DatasetSpec {
        name: "public-key-universe",
        category: DatasetCategory::LedgerControl,
        key_columns: 1,
        sql: "SELECT address FROM pubkeys ORDER BY address",
    },
    DatasetSpec {
        name: "opening-tracker-state",
        category: DatasetCategory::Tracker,
        key_columns: 1,
        sql: "SELECT singleton, history_coverage, certainty_revision_le,
                     certainty_revision_text
                FROM opening_tracker_state ORDER BY singleton",
    },
    DatasetSpec {
        name: "opening-tracker-accounts",
        category: DatasetCategory::Tracker,
        key_columns: 1,
        sql: "SELECT account.address, a.generation_le, a.generation_text,
                     a.account_state, mint.address, a.confirmed_revision_le,
                     a.confirmed_revision_text
                FROM opening_tracker_accounts a
                JOIN pubkeys account ON account.pubkey_id = a.pubkey_id
                LEFT JOIN pubkeys mint ON mint.pubkey_id = a.state_mint_pubkey_id
               ORDER BY account.address",
    },
    DatasetSpec {
        name: "checkpoint",
        category: DatasetCategory::LedgerControl,
        key_columns: 1,
        sql: "SELECT singleton, next_block_ordinal, digest_head, tracker_digest
                FROM checkpoint ORDER BY singleton",
    },
    DatasetSpec {
        name: "block-universe",
        category: DatasetCategory::LedgerControl,
        key_columns: 1,
        sql: "SELECT block_ordinal, epoch_le, epoch_text, slot_le, slot_text,
                     transaction_count
                FROM blocks ORDER BY block_ordinal",
    },
    DatasetSpec {
        name: "transaction-universe",
        category: DatasetCategory::LedgerControl,
        key_columns: 2,
        sql: "SELECT block_ordinal, tx_index, execution_status, status_reason,
                     failed_outer_index, primary_signature,
                     tracker_history_after, tracker_revision_after_le,
                     tracker_revision_after_text
                FROM transactions ORDER BY block_ordinal, tx_index",
    },
    DatasetSpec {
        name: "block-source-digests",
        category: DatasetCategory::CanonicalSourceDigest,
        key_columns: 1,
        sql: "SELECT block_ordinal, source_digest
                FROM blocks ORDER BY block_ordinal",
    },
    DatasetSpec {
        name: "block-tracker-and-integrity",
        category: DatasetCategory::Tracker,
        key_columns: 1,
        sql: "SELECT block_ordinal, tracker_history_after,
                     tracker_revision_after_le, tracker_revision_after_text,
                     tracker_digest_after, durable_rows_digest, chain_digest
                FROM blocks ORDER BY block_ordinal",
    },
    DatasetSpec {
        name: "account-lifetimes",
        category: DatasetCategory::Tracker,
        key_columns: 2,
        sql: "SELECT account.address, l.generation_text, l.generation_le,
                     l.account_state, mint.address, l.confirmed_revision_le,
                     l.confirmed_revision_text
                FROM account_lifetimes l
                JOIN pubkeys account ON account.pubkey_id = l.pubkey_id
                LEFT JOIN pubkeys mint ON mint.pubkey_id = l.state_mint_pubkey_id
               ORDER BY account.address, length(l.generation_text), l.generation_text",
    },
    DatasetSpec {
        name: "tracker-state",
        category: DatasetCategory::Tracker,
        key_columns: 1,
        sql: "SELECT singleton, history_coverage, certainty_revision_le,
                     certainty_revision_text
                FROM tracker_state ORDER BY singleton",
    },
    DatasetSpec {
        name: "tracker-accounts",
        category: DatasetCategory::Tracker,
        key_columns: 1,
        sql: "SELECT account.address, a.generation_le
                FROM tracker_accounts a
                JOIN pubkeys account ON account.pubkey_id = a.pubkey_id
               ORDER BY account.address",
    },
    DatasetSpec {
        name: "tracker-account-updates",
        category: DatasetCategory::Tracker,
        key_columns: 3,
        sql: "SELECT u.block_ordinal, u.tx_index, u.update_index,
                     account.address, u.generation_le, u.generation_text,
                     u.account_state, mint.address, u.confirmed_revision_le,
                     u.confirmed_revision_text
                FROM tracker_account_updates u
                JOIN pubkeys account ON account.pubkey_id = u.pubkey_id
                LEFT JOIN pubkeys mint ON mint.pubkey_id = u.state_mint_pubkey_id
               ORDER BY u.block_ordinal, u.tx_index, u.update_index",
    },
    DatasetSpec {
        name: "events",
        category: DatasetCategory::TokenEvent,
        key_columns: 3,
        sql: "SELECT e.block_ordinal, e.tx_index, e.event_index,
                     e.instruction_order, e.outer_index, e.inner_index,
                     e.stack_height, e.batch_index, e.invocation_state,
                     e.commit_state, program.address, e.raw_kind, e.token_tag,
                     e.data_coverage, e.data_coverage_reason, e.raw_data,
                     e.trailing_data, e.amount_le, e.amount_text, e.decimals,
                     e.required_signers, e.authority_type, embedded_a.address,
                     embedded_b.address, e.optional_value_present, e.ui_amount
                FROM events e
                JOIN pubkeys program ON program.pubkey_id = e.program_pubkey_id
                LEFT JOIN pubkeys embedded_a ON embedded_a.pubkey_id = e.embedded_pubkey_a
                LEFT JOIN pubkeys embedded_b ON embedded_b.pubkey_id = e.embedded_pubkey_b
               ORDER BY e.block_ordinal, e.tx_index, e.event_index",
    },
    DatasetSpec {
        name: "event-accounts",
        category: DatasetCategory::TokenEvent,
        key_columns: 4,
        sql: "SELECT e.block_ordinal, e.tx_index, e.event_index, a.binding_index,
                     a.account_index, account.address, a.semantic_role
                FROM event_accounts a
                JOIN events e ON e.event_id = a.event_id
                JOIN pubkeys account ON account.pubkey_id = a.pubkey_id
               ORDER BY e.block_ordinal, e.tx_index, e.event_index, a.binding_index",
    },
    DatasetSpec {
        name: "event-effects",
        category: DatasetCategory::TokenEvent,
        key_columns: 4,
        sql: "SELECT e.block_ordinal, e.tx_index, e.event_index, f.effect_index,
                     f.effect_kind, f.amount_le, f.amount_text, f.decimals,
                     f.checked
                FROM event_effects f
                JOIN events e ON e.event_id = f.event_id
               ORDER BY e.block_ordinal, e.tx_index, e.event_index, f.effect_index",
    },
    DatasetSpec {
        name: "lifecycle-effects",
        category: DatasetCategory::TokenEvent,
        key_columns: 4,
        sql: "SELECT e.block_ordinal, e.tx_index, e.event_index, l.effect_index,
                     account.address, l.before_generation_le,
                     l.before_generation_text, l.before_state, before_mint.address,
                     l.after_generation_le, l.after_generation_text, l.after_state,
                     after_mint.address, l.cause
                FROM lifecycle_effects l
                JOIN events e ON e.event_id = l.event_id
                JOIN pubkeys account ON account.pubkey_id = l.account_pubkey_id
                LEFT JOIN pubkeys before_mint ON before_mint.pubkey_id = l.before_state_mint_pubkey_id
                LEFT JOIN pubkeys after_mint ON after_mint.pubkey_id = l.after_state_mint_pubkey_id
               ORDER BY e.block_ordinal, e.tx_index, e.event_index, l.effect_index",
    },
    DatasetSpec {
        name: "delta-legs",
        category: DatasetCategory::TokenEvent,
        key_columns: 5,
        sql: "SELECT e.block_ordinal, e.tx_index, e.event_index, d.effect_index,
                     d.leg_index, account.address, d.generation_le,
                     d.generation_text, d.direction, d.transfer_role,
                     d.amount_le, d.amount_text
                FROM delta_legs d
                JOIN events e ON e.event_id = d.event_id
                JOIN pubkeys account ON account.pubkey_id = d.account_pubkey_id
               ORDER BY e.block_ordinal, e.tx_index, e.event_index,
                        d.effect_index, d.leg_index",
    },
    DatasetSpec {
        name: "coverage-issues",
        category: DatasetCategory::Coverage,
        key_columns: 3,
        sql: "SELECT c.block_ordinal, c.tx_index, c.issue_index,
                     c.instruction_order, c.outer_index, c.inner_index,
                     c.stack_height, c.issue_kind, c.detail, c.data_coverage,
                     c.coverage_reason, first_key.address, second_key.address,
                     known_mint.address, observed_mint.address,
                     c.expected_index, c.actual_index
                FROM coverage_issues c
                LEFT JOIN pubkeys first_key ON first_key.pubkey_id = c.first_pubkey_id
                LEFT JOIN pubkeys second_key ON second_key.pubkey_id = c.second_pubkey_id
                LEFT JOIN pubkeys known_mint ON known_mint.pubkey_id = c.known_mint_pubkey_id
                LEFT JOIN pubkeys observed_mint ON observed_mint.pubkey_id = c.observed_mint_pubkey_id
               ORDER BY c.block_ordinal, c.tx_index, c.issue_index",
    },
];

/// Used only for row-count reporting. The exact schema, constraints, indexes,
/// resources, tracker transitions, and digest chain are checked by
/// `TokenEventDatabase::audit_read_only` before these queries run.
const PHYSICAL_TABLES: &[&str] = &[
    "pubkeys",
    "run_identity",
    "opening_tracker_state",
    "opening_tracker_accounts",
    "checkpoint",
    "blocks",
    "transactions",
    "account_lifetimes",
    "tracker_state",
    "tracker_accounts",
    "tracker_account_updates",
    "events",
    "event_accounts",
    "event_effects",
    "lifecycle_effects",
    "delta_legs",
    "coverage_issues",
];

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32};

    #[cfg(unix)]
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    use blockzilla_dump::{TokenEventDatabase, TokenEventRunSpec};
    use blockzilla_query_sdk::{
        ArchiveFormat, BlockHeader, CanonicalBlock, ScanRange, SourceIdentity, SourceVerification,
        token::TargetMintTracker,
    };

    use super::*;

    fn fixture_database(path: &Path, mint: [u8; 32], format: ArchiveFormat) {
        let source = SourceIdentity {
            format,
            label: format!("fixture-{format}"),
            cluster_id: Some("fixture".into()),
            epoch: 0,
            first_slot: 0,
            slots_per_epoch: 32,
            block_count: 1,
            verification: match format {
                ArchiveFormat::Car => SourceVerification::OperatorTrusted,
                ArchiveFormat::CompactV2 => SourceVerification::PublishedManifest,
                ArchiveFormat::IndexerV3 => SourceVerification::InternalBindingOnly,
            },
            binding: Some(format!("binding-{format}")),
        };
        let opening = TargetMintTracker::from_sparse_start(mint).snapshot();
        let spec = TokenEventRunSpec::classic(
            source,
            mint,
            ScanRange {
                first_block: 0,
                block_count: NonZeroU32::new(1).unwrap(),
            },
            opening,
        );
        let mut database = TokenEventDatabase::create(path, spec).unwrap();
        database
            .track_and_commit_block(
                CanonicalBlock {
                    header: BlockHeader {
                        epoch: 0,
                        block_ordinal: 0,
                        slot: 0,
                    },
                    transactions: Vec::new(),
                }
                .as_view(),
            )
            .unwrap();
        database.checkpoint_wal().unwrap();
    }

    fn private_dir(root: &Path, name: &str) -> PathBuf {
        let path = root.join(name);
        #[cfg(unix)]
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        #[cfg(not(unix))]
        fs::create_dir(&path).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    #[test]
    fn equal_and_mismatched_full_rows_are_reported() {
        let root = tempfile::tempdir().unwrap();
        let canonical_root = std::fs::canonicalize(root.path()).unwrap();
        let car_dir = private_dir(&canonical_root, "car");
        let compact_dir = private_dir(&canonical_root, "compact");
        let v3_dir = private_dir(&canonical_root, "v3");
        let car = car_dir.join("events.sqlite");
        let compact = compact_dir.join("events.sqlite");
        let v3 = v3_dir.join("events.sqlite");
        fixture_database(&car, [7; 32], ArchiveFormat::Car);
        fixture_database(&compact, [7; 32], ArchiveFormat::CompactV2);
        fixture_database(&v3, [7; 32], ArchiveFormat::IndexerV3);

        let equal = compare_output_databases(&car, &compact, &v3, 20).unwrap();
        assert!(equal.all_ledger_parity.exact_equal);
        assert!(equal.all_databases_complete);
        assert!(equal.mismatch_samples.is_empty());

        let other_dir = private_dir(&canonical_root, "other");
        let other = other_dir.join("events.sqlite");
        fixture_database(&other, [8; 32], ArchiveFormat::IndexerV3);
        let mismatch = compare_output_databases(&car, &compact, &other, 20).unwrap();
        assert!(!mismatch.all_ledger_parity.exact_equal);
        assert!(!mismatch.mismatch_samples.is_empty());
        assert!(mismatch.mismatch_samples.len() <= 20);
    }
}

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version 2 names the checkpoint payload as a runtime source descriptor.
/// Existing version-1 databases are rejected instead of being resumed under a
/// changed source contract.
pub const SCHEMA_VERSION: i64 = 2;
const APPLICATION_ID: i64 = 0x425a_4450; // "BZDP"

const SCHEMA: &str = r#"
CREATE TABLE dump (
    singleton              INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version         INTEGER NOT NULL,
    kind                   TEXT NOT NULL CHECK (kind IN ('program', 'token')),
    target_pubkey          BLOB NOT NULL CHECK (length(target_pubkey) = 32),
    target_pubkey_base58   TEXT NOT NULL CHECK (length(target_pubkey_base58) > 0),
    source                 TEXT NOT NULL,
    on_indeterminate       TEXT NOT NULL CHECK (on_indeterminate IN ('fail', 'record', 'skip')),
    state                  TEXT NOT NULL CHECK (state IN ('building', 'complete', 'complete-with-gaps', 'failed')),
    created_unix_seconds   INTEGER NOT NULL,
    updated_unix_seconds   INTEGER NOT NULL,
    error                  TEXT
) STRICT;

CREATE TABLE epochs (
    epoch                       INTEGER PRIMARY KEY CHECK (epoch >= 0),
    state                       TEXT NOT NULL CHECK (state IN ('pending', 'scanning', 'complete', 'complete-with-gaps', 'failed')),
    source_identity             TEXT,
    cluster_id                  TEXT,
    generation_id              TEXT,
    slots_per_epoch             INTEGER CHECK (slots_per_epoch IS NULL OR slots_per_epoch > 0),
    message_schema              TEXT,
    metadata_schema             TEXT,
    source_descriptor_json      TEXT,
    block_rows_total            INTEGER CHECK (block_rows_total IS NULL OR block_rows_total >= 0),
    next_block_row              INTEGER NOT NULL DEFAULT 0 CHECK (next_block_row >= 0),
    scanned_blocks              INTEGER NOT NULL DEFAULT 0 CHECK (scanned_blocks >= 0),
    scanned_transactions        INTEGER NOT NULL DEFAULT 0 CHECK (scanned_transactions >= 0),
    matched_transactions        INTEGER NOT NULL DEFAULT 0 CHECK (matched_transactions >= 0),
    indeterminate_transactions  INTEGER NOT NULL DEFAULT 0 CHECK (indeterminate_transactions >= 0),
    updated_unix_seconds        INTEGER NOT NULL,
    error                       TEXT,
    CHECK (
        (source_identity IS NULL AND cluster_id IS NULL AND generation_id IS NULL
            AND slots_per_epoch IS NULL AND message_schema IS NULL AND metadata_schema IS NULL
            AND source_descriptor_json IS NULL AND block_rows_total IS NULL)
        OR
        (length(source_identity) > 0 AND cluster_id IS NOT NULL AND generation_id IS NOT NULL
            AND slots_per_epoch IS NOT NULL AND length(message_schema) > 0
            AND length(metadata_schema) > 0 AND source_descriptor_json IS NOT NULL
            AND block_rows_total IS NOT NULL)
    ),
    CHECK (block_rows_total IS NULL OR next_block_row <= block_rows_total)
) STRICT;

CREATE TABLE transactions (
    epoch                    INTEGER NOT NULL REFERENCES epochs(epoch) ON DELETE RESTRICT,
    slot                     INTEGER NOT NULL CHECK (slot >= 0),
    block_id                 INTEGER NOT NULL CHECK (block_id >= 0),
    tx_index                 INTEGER NOT NULL CHECK (tx_index >= 0),
    source_flags             INTEGER NOT NULL CHECK (source_flags >= 0),
    first_signature_ordinal  INTEGER NOT NULL CHECK (first_signature_ordinal >= 0),
    signature_count          INTEGER NOT NULL CHECK (signature_count BETWEEN 1 AND 255),
    primary_signature        BLOB NOT NULL CHECK (length(primary_signature) = 64),
    primary_signature_base58 TEXT NOT NULL UNIQUE CHECK (length(primary_signature_base58) > 0),
    signatures               BLOB NOT NULL CHECK (length(signatures) = signature_count * 64),
    message_state            TEXT NOT NULL CHECK (message_state IN ('decoded', 'raw_fallback')),
    message_bytes            BLOB NOT NULL,
    metadata_state           TEXT NOT NULL CHECK (metadata_state IN ('absent', 'decoded', 'raw_fallback')),
    metadata_wincode         BLOB,
    CHECK ((metadata_state = 'absent' AND metadata_wincode IS NULL)
        OR (metadata_state IN ('decoded', 'raw_fallback') AND metadata_wincode IS NOT NULL)),
    PRIMARY KEY (epoch, slot, tx_index),
    UNIQUE (epoch, primary_signature)
) STRICT, WITHOUT ROWID;

CREATE TABLE transaction_signatures (
    epoch              INTEGER NOT NULL,
    slot               INTEGER NOT NULL,
    tx_index           INTEGER NOT NULL,
    signature_index    INTEGER NOT NULL CHECK (signature_index BETWEEN 0 AND 254),
    signature          BLOB NOT NULL CHECK (length(signature) = 64),
    signature_base58   TEXT NOT NULL CHECK (length(signature_base58) > 0),
    PRIMARY KEY (epoch, slot, tx_index, signature_index),
    FOREIGN KEY (epoch, slot, tx_index)
        REFERENCES transactions(epoch, slot, tx_index) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE transaction_accounts (
    epoch              INTEGER NOT NULL,
    slot               INTEGER NOT NULL,
    tx_index           INTEGER NOT NULL,
    account_index      INTEGER NOT NULL CHECK (account_index >= 0),
    pubkey             BLOB NOT NULL CHECK (length(pubkey) = 32),
    pubkey_base58      TEXT NOT NULL CHECK (length(pubkey_base58) > 0),
    source             TEXT NOT NULL CHECK (source IN ('static', 'loaded_writable', 'loaded_readonly')),
    is_signer          INTEGER NOT NULL CHECK (is_signer IN (0, 1)),
    is_writable        INTEGER NOT NULL CHECK (is_writable IN (0, 1)),
    PRIMARY KEY (epoch, slot, tx_index, account_index),
    FOREIGN KEY (epoch, slot, tx_index)
        REFERENCES transactions(epoch, slot, tx_index) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE program_matches (
    epoch             INTEGER NOT NULL,
    slot              INTEGER NOT NULL,
    tx_index          INTEGER NOT NULL,
    direct_count      INTEGER NOT NULL CHECK (direct_count >= 0),
    cpi_count         INTEGER NOT NULL CHECK (cpi_count >= 0),
    CHECK (direct_count + cpi_count > 0),
    PRIMARY KEY (epoch, slot, tx_index),
    FOREIGN KEY (epoch, slot, tx_index)
        REFERENCES transactions(epoch, slot, tx_index) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE token_matches (
    epoch             INTEGER NOT NULL,
    slot              INTEGER NOT NULL,
    tx_index          INTEGER NOT NULL,
    pre_count         INTEGER NOT NULL CHECK (pre_count >= 0),
    post_count        INTEGER NOT NULL CHECK (post_count >= 0),
    CHECK (pre_count + post_count > 0),
    PRIMARY KEY (epoch, slot, tx_index),
    FOREIGN KEY (epoch, slot, tx_index)
        REFERENCES transactions(epoch, slot, tx_index) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE token_balances (
    epoch             INTEGER NOT NULL,
    slot              INTEGER NOT NULL,
    tx_index          INTEGER NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('pre', 'post')),
    balance_index     INTEGER NOT NULL CHECK (balance_index >= 0),
    account_index     INTEGER NOT NULL CHECK (account_index >= 0),
    mint              BLOB NOT NULL CHECK (length(mint) = 32),
    mint_base58       TEXT NOT NULL CHECK (length(mint_base58) > 0),
    owner             BLOB CHECK (owner IS NULL OR length(owner) = 32),
    owner_base58      TEXT,
    token_program     BLOB CHECK (token_program IS NULL OR length(token_program) = 32),
    token_program_base58 TEXT,
    amount_u64        TEXT NOT NULL CHECK (
        amount_u64 = '0' OR (
            length(amount_u64) BETWEEN 1 AND 20
            AND amount_u64 NOT LIKE '0%'
            AND amount_u64 NOT GLOB '*[^0-9]*'
        )
    ),
    amount_le         BLOB NOT NULL CHECK (length(amount_le) = 8),
    decimals          INTEGER NOT NULL CHECK (decimals BETWEEN 0 AND 255),
    PRIMARY KEY (epoch, slot, tx_index, side, balance_index),
    FOREIGN KEY (epoch, slot, tx_index)
        REFERENCES transactions(epoch, slot, tx_index) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE coverage_issues (
    epoch          INTEGER NOT NULL REFERENCES epochs(epoch) ON DELETE RESTRICT,
    slot           INTEGER NOT NULL CHECK (slot >= 0),
    tx_index       INTEGER NOT NULL CHECK (tx_index >= 0),
    reason         TEXT NOT NULL,
    detail         TEXT,
    PRIMARY KEY (epoch, slot, tx_index)
) STRICT, WITHOUT ROWID;

CREATE INDEX transactions_by_primary_signature ON transactions(primary_signature);
CREATE INDEX transactions_by_slot ON transactions(epoch, slot, tx_index);
CREATE INDEX transaction_signatures_by_base58 ON transaction_signatures(signature_base58);
CREATE INDEX transaction_accounts_by_pubkey ON transaction_accounts(pubkey_base58, epoch, slot, tx_index);
CREATE INDEX token_balances_by_mint ON token_balances(mint_base58, epoch, slot, tx_index);
"#;

#[derive(Debug, Error)]
pub enum DumpError {
    #[error("SQLite dump already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("SQLite dump does not exist: {0}")]
    NotFound(PathBuf),
    #[error("not a Blockzilla dump database (application_id={0})")]
    WrongApplication(i64),
    #[error("unsupported Blockzilla dump schema version {0}")]
    UnsupportedSchema(i64),
    #[error("dump specification does not match the existing database: {0}")]
    SpecificationMismatch(String),
    #[error("invalid dump specification: {0}")]
    InvalidSpecification(String),
    #[error("epoch {epoch} is not part of this dump")]
    UnknownEpoch { epoch: u64 },
    #[error("epoch {epoch} generation binding differs from the existing checkpoint")]
    GenerationMismatch { epoch: u64 },
    #[error("invalid checkpoint for epoch {epoch}: {message}")]
    InvalidCheckpoint { epoch: u64, message: String },
    #[error("cannot complete dump while an epoch has not finished")]
    IncompleteEpochs,
    #[error("system clock is before the Unix epoch")]
    InvalidClock,
    #[error("cannot create SQLite dump file {path}: {source}")]
    CreateFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{field} value {value} does not fit SQLite INTEGER")]
    ValueOutOfRange { field: &'static str, value: u64 },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub type Result<T> = std::result::Result<T, DumpError>;

macro_rules! text_enum {
    ($type:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "kebab-case")]
        pub enum $type { $($variant),+ }

        impl $type {
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),+ }
            }
        }

        impl rusqlite::types::ToSql for $type {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::Borrowed(ValueRef::Text(self.as_str().as_bytes())))
            }
        }

        impl FromSql for $type {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                let text = value.as_str()?;
                match text {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(FromSqlError::Other(
                        format!("invalid {} value {text:?}", stringify!($type)).into(),
                    )),
                }
            }
        }
    };
}

text_enum!(DumpKind { Program => "program", Token => "token" });
text_enum!(OnIndeterminate { Fail => "fail", Record => "record", Skip => "skip" });
text_enum!(DumpState {
    Building => "building",
    Complete => "complete",
    CompleteWithGaps => "complete-with-gaps",
    Failed => "failed",
});
text_enum!(EpochState {
    Pending => "pending",
    Scanning => "scanning",
    Complete => "complete",
    CompleteWithGaps => "complete-with-gaps",
    Failed => "failed",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpSpec {
    pub kind: DumpKind,
    pub target_pubkey: [u8; 32],
    pub source: String,
    pub on_indeterminate: OnIndeterminate,
    pub epochs: Vec<u64>,
}

impl DumpSpec {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.source.is_empty() {
            return Err("source must not be empty".into());
        }
        if self.epochs.is_empty() {
            return Err("at least one epoch is required".into());
        }
        let unique = self.epochs.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != self.epochs.len() {
            return Err("epochs must not contain duplicates".into());
        }
        Ok(())
    }

    fn sorted_epochs(&self) -> Vec<u64> {
        let mut epochs = self.epochs.clone();
        epochs.sort_unstable();
        epochs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochBinding {
    pub epoch: u64,
    pub source_identity: String,
    pub cluster_id: String,
    pub generation_id: String,
    pub slots_per_epoch: u64,
    pub message_schema: String,
    pub metadata_schema: String,
    pub source_descriptor_json: String,
    pub block_rows_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Checkpoint {
    pub next_block_row: u64,
    pub scanned_blocks: u64,
    pub scanned_transactions: u64,
    pub matched_transactions: u64,
    pub indeterminate_transactions: u64,
}

text_enum!(MessageState { Decoded => "decoded", RawFallback => "raw_fallback" });
text_enum!(MetadataState {
    Absent => "absent",
    Decoded => "decoded",
    RawFallback => "raw_fallback",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchRecord {
    pub epoch: u64,
    pub slot: u64,
    pub block_id: u32,
    pub tx_index: u32,
    pub source_flags: u32,
    pub first_signature_ordinal: u64,
    pub signatures: Vec<[u8; 64]>,
    pub message_state: MessageState,
    pub message_bytes: Vec<u8>,
    pub metadata_state: MetadataState,
    pub metadata_wincode: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramMatch {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub direct_count: u32,
    pub cpi_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenMatch {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub pre_count: u32,
    pub post_count: u32,
}

text_enum!(TokenBalanceSide { Pre => "pre", Post => "post" });

text_enum!(TransactionAccountSource {
    Static => "static",
    LoadedWritable => "loaded_writable",
    LoadedReadonly => "loaded_readonly",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionAccountRecord {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub account_index: u32,
    pub pubkey: [u8; 32],
    pub source: TransactionAccountSource,
    pub is_signer: bool,
    pub is_writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalanceRecord {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub side: TokenBalanceSide,
    pub balance_index: u32,
    pub account_index: u32,
    pub mint: [u8; 32],
    pub owner: Option<[u8; 32]>,
    pub token_program: Option<[u8; 32]>,
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoverageIssue {
    pub epoch: u64,
    pub slot: u64,
    pub tx_index: u32,
    pub reason: String,
    pub detail: Option<String>,
}

#[derive(Debug, Default)]
pub struct CheckpointBatch {
    pub checkpoint: Checkpoint,
    pub transactions: Vec<MatchRecord>,
    pub transaction_accounts: Vec<TransactionAccountRecord>,
    pub program_matches: Vec<ProgramMatch>,
    pub token_matches: Vec<TokenMatch>,
    pub token_balances: Vec<TokenBalanceRecord>,
    pub coverage_issues: Vec<CoverageIssue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CheckpointStatus {
    pub next_block_row: u64,
    pub scanned_blocks: u64,
    pub scanned_transactions: u64,
    pub matched_transactions: u64,
    pub indeterminate_transactions: u64,
}

impl From<Checkpoint> for CheckpointStatus {
    fn from(value: Checkpoint) -> Self {
        Self {
            next_block_row: value.next_block_row,
            scanned_blocks: value.scanned_blocks,
            scanned_transactions: value.scanned_transactions,
            matched_transactions: value.matched_transactions,
            indeterminate_transactions: value.indeterminate_transactions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EpochStatus {
    pub epoch: u64,
    pub state: EpochState,
    pub source_identity: Option<String>,
    pub source_descriptor_json: Option<String>,
    pub message_schema: Option<String>,
    pub metadata_schema: Option<String>,
    pub block_rows_total: Option<u64>,
    pub checkpoint: CheckpointStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DumpStatus {
    pub schema_version: i64,
    pub kind: DumpKind,
    pub target_pubkey_hex: String,
    pub target_pubkey_base58: String,
    pub source: String,
    pub on_indeterminate: OnIndeterminate,
    pub state: DumpState,
    pub error: Option<String>,
    pub transaction_rows: u64,
    pub coverage_issue_rows: u64,
    pub epochs: Vec<EpochStatus>,
}

pub struct DumpDatabase {
    path: PathBuf,
    connection: Connection,
}

impl DumpDatabase {
    /// Create a new immutable-scope dump. Existing paths are never overwritten.
    pub fn create(path: impl AsRef<Path>, spec: &DumpSpec) -> Result<Self> {
        spec.validate().map_err(DumpError::InvalidSpecification)?;
        let path = path.as_ref().to_path_buf();
        let reserved = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path);
        match reserved {
            Ok(file) => drop(file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(DumpError::AlreadyExists(path));
            }
            Err(source) => {
                return Err(DumpError::CreateFile {
                    path: path.clone(),
                    source,
                });
            }
        }
        let mut create_guard = NewFileGuard::new(path.clone());
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut connection = Connection::open_with_flags(&path, flags)?;
        configure_writer(&connection)?;
        connection.pragma_update(None, "application_id", APPLICATION_ID)?;
        connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;

        let now = unix_seconds()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(SCHEMA)?;
        transaction.execute(
            "INSERT INTO dump (
                singleton, schema_version, kind, target_pubkey, target_pubkey_base58,
                source, on_indeterminate, state, created_unix_seconds, updated_unix_seconds
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                SCHEMA_VERSION,
                spec.kind,
                &spec.target_pubkey[..],
                bs58::encode(spec.target_pubkey).into_string(),
                spec.source,
                spec.on_indeterminate,
                DumpState::Building,
                now,
            ],
        )?;
        for epoch in spec.sorted_epochs() {
            transaction.execute(
                "INSERT INTO epochs (epoch, state, updated_unix_seconds) VALUES (?1, ?2, ?3)",
                params![sqlite_u64(epoch, "epoch")?, EpochState::Pending, now],
            )?;
        }
        transaction.commit()?;
        validate_database(&connection)?;
        create_guard.keep();
        Ok(Self { path, connection })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.is_file() {
            return Err(DumpError::NotFound(path));
        }
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        configure_writer(&connection)?;
        validate_database(&connection)?;
        Ok(Self { path, connection })
    }

    pub fn open_or_create(path: impl AsRef<Path>, spec: &DumpSpec) -> Result<Self> {
        let path = path.as_ref();
        let database = if path.exists() {
            Self::open(path)?
        } else {
            Self::create(path, spec)?
        };
        database.ensure_spec(spec)?;
        Ok(database)
    }

    pub fn read_status(path: impl AsRef<Path>) -> Result<DumpStatus> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(DumpError::NotFound(path.to_path_buf()));
        }
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.pragma_update(None, "foreign_keys", true)?;
        validate_database(&connection)?;
        status_from_connection(&connection)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn status(&self) -> Result<DumpStatus> {
        status_from_connection(&self.connection)
    }

    /// Bind an epoch to one exact source and return its resume point.
    pub fn begin_epoch(&mut self, binding: &EpochBinding) -> Result<Checkpoint> {
        validate_binding(binding)?;
        let now = unix_seconds()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = stored_epoch(&transaction, binding.epoch)?;
        match &stored.binding {
            Some(existing) if !same_binding(existing, binding) => {
                return Err(DumpError::GenerationMismatch {
                    epoch: binding.epoch,
                });
            }
            Some(_) => {}
            None => {
                transaction.execute(
                    "UPDATE epochs SET
                        source_identity = ?2, cluster_id = ?3, generation_id = ?4,
                        slots_per_epoch = ?5, message_schema = ?6,
                        metadata_schema = ?7, source_descriptor_json = ?8, block_rows_total = ?9
                     WHERE epoch = ?1",
                    params![
                        sqlite_u64(binding.epoch, "epoch")?,
                        binding.source_identity,
                        binding.cluster_id,
                        binding.generation_id,
                        sqlite_u64(binding.slots_per_epoch, "slots_per_epoch")?,
                        binding.message_schema,
                        binding.metadata_schema,
                        binding.source_descriptor_json,
                        sqlite_u64(binding.block_rows_total, "block_rows_total")?,
                    ],
                )?;
            }
        }
        transaction.execute(
            "UPDATE epochs SET state = ?2, error = NULL, updated_unix_seconds = ?3 WHERE epoch = ?1",
            params![
                sqlite_u64(binding.epoch, "epoch")?,
                EpochState::Scanning,
                now
            ],
        )?;
        transaction.execute(
            "UPDATE dump SET state = ?1, error = NULL, updated_unix_seconds = ?2 WHERE singleton = 1",
            params![DumpState::Building, now],
        )?;
        transaction.commit()?;
        Ok(stored.checkpoint)
    }

    /// Commit rows and the absolute resume checkpoint in one durable transaction.
    pub fn commit_checkpoint(&mut self, epoch: u64, batch: &CheckpointBatch) -> Result<()> {
        let stored = stored_epoch(&self.connection, epoch)?;
        let binding = stored
            .binding
            .as_ref()
            .ok_or_else(|| DumpError::InvalidCheckpoint {
                epoch,
                message: "epoch has no generation binding; call begin_epoch first".into(),
            })?;
        if stored.state != EpochState::Scanning {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: format!(
                    "epoch state is {}, expected scanning",
                    stored.state.as_str()
                ),
            });
        }
        validate_checkpoint(epoch, stored.checkpoint, binding.block_rows_total, batch)?;
        let (kind, on_indeterminate): (DumpKind, OnIndeterminate) = self.connection.query_row(
            "SELECT kind, on_indeterminate FROM dump WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        validate_batch_shape(epoch, kind, on_indeterminate, batch)?;

        let now = unix_seconds()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for record in &batch.transactions {
            insert_transaction(&transaction, record)?;
        }
        for account in &batch.transaction_accounts {
            insert_transaction_account(&transaction, account)?;
        }
        for summary in &batch.program_matches {
            transaction.execute(
                "INSERT INTO program_matches
                    (epoch, slot, tx_index, direct_count, cpi_count)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sqlite_u64(summary.epoch, "epoch")?,
                    sqlite_u64(summary.slot, "slot")?,
                    i64::from(summary.tx_index),
                    i64::from(summary.direct_count),
                    i64::from(summary.cpi_count),
                ],
            )?;
        }
        for summary in &batch.token_matches {
            transaction.execute(
                "INSERT INTO token_matches (epoch, slot, tx_index, pre_count, post_count)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sqlite_u64(summary.epoch, "epoch")?,
                    sqlite_u64(summary.slot, "slot")?,
                    i64::from(summary.tx_index),
                    i64::from(summary.pre_count),
                    i64::from(summary.post_count),
                ],
            )?;
        }
        for balance in &batch.token_balances {
            insert_token_balance(&transaction, balance)?;
        }
        for issue in &batch.coverage_issues {
            transaction.execute(
                "INSERT INTO coverage_issues (epoch, slot, tx_index, reason, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sqlite_u64(issue.epoch, "epoch")?,
                    sqlite_u64(issue.slot, "slot")?,
                    i64::from(issue.tx_index),
                    issue.reason,
                    issue.detail,
                ],
            )?;
        }

        let stored_matches = count_for_epoch(&transaction, "transactions", epoch)?;
        if stored_matches != batch.checkpoint.matched_transactions {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: format!(
                    "matched_transactions is {}, but the database contains {stored_matches}",
                    batch.checkpoint.matched_transactions
                ),
            });
        }
        let stored_issues = count_for_epoch(&transaction, "coverage_issues", epoch)?;
        if on_indeterminate == OnIndeterminate::Record
            && stored_issues != batch.checkpoint.indeterminate_transactions
        {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: format!(
                    "indeterminate_transactions is {}, but the database contains {stored_issues} recorded issues",
                    batch.checkpoint.indeterminate_transactions
                ),
            });
        }

        transaction.execute(
            "UPDATE epochs SET
                next_block_row = ?2, scanned_blocks = ?3, scanned_transactions = ?4,
                matched_transactions = ?5, indeterminate_transactions = ?6,
                updated_unix_seconds = ?7
             WHERE epoch = ?1",
            params![
                sqlite_u64(epoch, "epoch")?,
                sqlite_u64(batch.checkpoint.next_block_row, "next_block_row")?,
                sqlite_u64(batch.checkpoint.scanned_blocks, "scanned_blocks")?,
                sqlite_u64(
                    batch.checkpoint.scanned_transactions,
                    "scanned_transactions"
                )?,
                sqlite_u64(
                    batch.checkpoint.matched_transactions,
                    "matched_transactions"
                )?,
                sqlite_u64(
                    batch.checkpoint.indeterminate_transactions,
                    "indeterminate_transactions"
                )?,
                now,
            ],
        )?;
        transaction.execute(
            "UPDATE dump SET updated_unix_seconds = ?1 WHERE singleton = 1",
            [now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn complete_epoch(&mut self, epoch: u64) -> Result<EpochState> {
        let stored = stored_epoch(&self.connection, epoch)?;
        let binding = stored
            .binding
            .as_ref()
            .ok_or_else(|| DumpError::InvalidCheckpoint {
                epoch,
                message: "epoch has no generation binding".into(),
            })?;
        if stored.checkpoint.next_block_row != binding.block_rows_total {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: format!(
                    "checkpoint is at block row {}, but the epoch contains {} rows",
                    stored.checkpoint.next_block_row, binding.block_rows_total
                ),
            });
        }
        let state = if stored.checkpoint.indeterminate_transactions == 0 {
            EpochState::Complete
        } else {
            EpochState::CompleteWithGaps
        };
        let now = unix_seconds()?;
        self.connection.execute(
            "UPDATE epochs SET state = ?2, error = NULL, updated_unix_seconds = ?3
             WHERE epoch = ?1",
            params![sqlite_u64(epoch, "epoch")?, state, now],
        )?;
        Ok(state)
    }

    pub fn complete_dump(&mut self) -> Result<DumpState> {
        let unfinished: i64 = self.connection.query_row(
            "SELECT count(*) FROM epochs WHERE state NOT IN ('complete', 'complete-with-gaps')",
            [],
            |row| row.get(0),
        )?;
        if unfinished != 0 {
            return Err(DumpError::IncompleteEpochs);
        }
        let gaps: i64 = self.connection.query_row(
            "SELECT count(*) FROM epochs WHERE state = 'complete-with-gaps'",
            [],
            |row| row.get(0),
        )?;
        let state = if gaps == 0 {
            DumpState::Complete
        } else {
            DumpState::CompleteWithGaps
        };
        self.connection.execute(
            "UPDATE dump SET state = ?1, error = NULL, updated_unix_seconds = ?2
             WHERE singleton = 1",
            params![state, unix_seconds()?],
        )?;
        Ok(state)
    }

    pub fn fail_epoch(&mut self, epoch: u64, message: impl AsRef<str>) -> Result<()> {
        ensure_epoch(&self.connection, epoch)?;
        let message = message.as_ref().trim();
        if message.is_empty() {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: "failure message must not be empty".into(),
            });
        }
        let now = unix_seconds()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "UPDATE epochs SET state = ?2, error = ?3, updated_unix_seconds = ?4
             WHERE epoch = ?1",
            params![
                sqlite_u64(epoch, "epoch")?,
                EpochState::Failed,
                message,
                now
            ],
        )?;
        transaction.execute(
            "UPDATE dump SET state = ?1, error = ?2, updated_unix_seconds = ?3
             WHERE singleton = 1",
            params![DumpState::Failed, message, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn integrity_check(&self) -> Result<()> {
        let result: String = self
            .connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(DumpError::Sqlite(rusqlite::Error::InvalidQuery));
        }
        let foreign_key_error: Option<i64> = self
            .connection
            .query_row(
                "SELECT 1 FROM pragma_foreign_key_check LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if foreign_key_error.is_some() {
            return Err(DumpError::Sqlite(rusqlite::Error::InvalidQuery));
        }
        Ok(())
    }

    fn ensure_spec(&self, expected: &DumpSpec) -> Result<()> {
        expected
            .validate()
            .map_err(DumpError::InvalidSpecification)?;
        let (kind, target, source, policy): (DumpKind, Vec<u8>, String, OnIndeterminate) =
            self.connection.query_row(
                "SELECT kind, target_pubkey, source, on_indeterminate FROM dump WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
        if kind != expected.kind {
            return Err(DumpError::SpecificationMismatch("kind differs".into()));
        }
        if target.as_slice() != expected.target_pubkey {
            return Err(DumpError::SpecificationMismatch(
                "target pubkey differs".into(),
            ));
        }
        if source != expected.source {
            return Err(DumpError::SpecificationMismatch("source differs".into()));
        }
        if policy != expected.on_indeterminate {
            return Err(DumpError::SpecificationMismatch(
                "on-indeterminate policy differs".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare("SELECT epoch FROM epochs ORDER BY epoch")?;
        let actual = statement
            .query_map([], |row| sql_u64(row, 0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if actual != expected.sorted_epochs() {
            return Err(DumpError::SpecificationMismatch("epoch set differs".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredEpochBinding {
    source_identity: String,
    cluster_id: String,
    generation_id: String,
    slots_per_epoch: u64,
    message_schema: String,
    metadata_schema: String,
    source_descriptor_json: String,
    block_rows_total: u64,
}

struct NewFileGuard {
    path: PathBuf,
    keep: bool,
}

impl NewFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for NewFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Debug, Clone)]
struct StoredEpoch {
    state: EpochState,
    binding: Option<StoredEpochBinding>,
    checkpoint: Checkpoint,
}

fn configure_writer(connection: &Connection) -> Result<()> {
    connection.busy_timeout(std::time::Duration::from_secs(30))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    Ok(())
}

fn validate_database(connection: &Connection) -> Result<()> {
    let application_id: i64 =
        connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    if application_id != APPLICATION_ID {
        return Err(DumpError::WrongApplication(application_id));
    }
    let user_version: i64 =
        connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if user_version != SCHEMA_VERSION {
        return Err(DumpError::UnsupportedSchema(user_version));
    }
    let schema_version: i64 = connection
        .query_row(
            "SELECT schema_version FROM dump WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| DumpError::WrongApplication(application_id))?;
    if schema_version != SCHEMA_VERSION {
        return Err(DumpError::UnsupportedSchema(schema_version));
    }
    let (target, target_base58): (Vec<u8>, String) = connection.query_row(
        "SELECT target_pubkey, target_pubkey_base58 FROM dump WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if target.len() != 32 || bs58::encode(&target).into_string() != target_base58 {
        return Err(DumpError::WrongApplication(application_id));
    }
    Ok(())
}

fn status_from_connection(connection: &Connection) -> Result<DumpStatus> {
    let (schema_version, kind, target, source, on_indeterminate, state, error): (
        i64,
        DumpKind,
        Vec<u8>,
        String,
        OnIndeterminate,
        DumpState,
        Option<String>,
    ) = connection.query_row(
        "SELECT schema_version, kind, target_pubkey, source, on_indeterminate, state, error
         FROM dump WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    let transaction_rows = count_all(connection, "transactions")?;
    let coverage_issue_rows = count_all(connection, "coverage_issues")?;
    let mut statement = connection.prepare(
        "SELECT epoch, state, source_identity, source_descriptor_json, message_schema, metadata_schema,
                block_rows_total, next_block_row, scanned_blocks, scanned_transactions,
                matched_transactions, indeterminate_transactions, error
         FROM epochs ORDER BY epoch",
    )?;
    let epochs = statement
        .query_map([], |row| {
            Ok(EpochStatus {
                epoch: sql_u64(row, 0)?,
                state: row.get(1)?,
                source_identity: row.get(2)?,
                source_descriptor_json: row.get(3)?,
                message_schema: row.get(4)?,
                metadata_schema: row.get(5)?,
                block_rows_total: sql_optional_u64(row, 6)?,
                checkpoint: CheckpointStatus {
                    next_block_row: sql_u64(row, 7)?,
                    scanned_blocks: sql_u64(row, 8)?,
                    scanned_transactions: sql_u64(row, 9)?,
                    matched_transactions: sql_u64(row, 10)?,
                    indeterminate_transactions: sql_u64(row, 11)?,
                },
                error: row.get(12)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(DumpStatus {
        schema_version,
        kind,
        target_pubkey_hex: hex_lower(&target),
        target_pubkey_base58: bs58::encode(&target).into_string(),
        source,
        on_indeterminate,
        state,
        error,
        transaction_rows,
        coverage_issue_rows,
        epochs,
    })
}

fn validate_binding(binding: &EpochBinding) -> Result<()> {
    if binding.source_identity.is_empty()
        || binding.cluster_id.is_empty()
        || binding.generation_id.is_empty()
        || binding.message_schema.is_empty()
        || binding.metadata_schema.is_empty()
    {
        return Err(DumpError::InvalidCheckpoint {
            epoch: binding.epoch,
            message: "generation and schema names must not be empty".into(),
        });
    }
    if binding.slots_per_epoch == 0 {
        return Err(DumpError::InvalidCheckpoint {
            epoch: binding.epoch,
            message: "slots_per_epoch must be greater than zero".into(),
        });
    }
    serde_json::from_str::<serde_json::Value>(&binding.source_descriptor_json).map_err(
        |error| DumpError::InvalidCheckpoint {
            epoch: binding.epoch,
            message: format!("source_descriptor_json is not valid JSON: {error}"),
        },
    )?;
    Ok(())
}

fn stored_epoch(connection: &Connection, epoch: u64) -> Result<StoredEpoch> {
    let row = connection
        .query_row(
            "SELECT state, source_identity, cluster_id, generation_id,
                    slots_per_epoch, message_schema, metadata_schema, source_descriptor_json, block_rows_total,
                    next_block_row, scanned_blocks, scanned_transactions,
                    matched_transactions, indeterminate_transactions
             FROM epochs WHERE epoch = ?1",
            [sqlite_u64(epoch, "epoch")?],
            |row| {
                let source_identity: Option<String> = row.get(1)?;
                let binding = match source_identity {
                    Some(source_identity) => Some(StoredEpochBinding {
                        source_identity,
                        cluster_id: row.get(2)?,
                        generation_id: row.get(3)?,
                        slots_per_epoch: sql_u64(row, 4)?,
                        message_schema: row.get(5)?,
                        metadata_schema: row.get(6)?,
                        source_descriptor_json: row.get(7)?,
                        block_rows_total: sql_u64(row, 8)?,
                    }),
                    None => None,
                };
                Ok(StoredEpoch {
                    state: row.get(0)?,
                    binding,
                    checkpoint: Checkpoint {
                        next_block_row: sql_u64(row, 9)?,
                        scanned_blocks: sql_u64(row, 10)?,
                        scanned_transactions: sql_u64(row, 11)?,
                        matched_transactions: sql_u64(row, 12)?,
                        indeterminate_transactions: sql_u64(row, 13)?,
                    },
                })
            },
        )
        .optional()?;
    row.ok_or(DumpError::UnknownEpoch { epoch })
}

fn same_binding(stored: &StoredEpochBinding, candidate: &EpochBinding) -> bool {
    stored.source_identity == candidate.source_identity
        && stored.cluster_id == candidate.cluster_id
        && stored.generation_id == candidate.generation_id
        && stored.slots_per_epoch == candidate.slots_per_epoch
        && stored.message_schema == candidate.message_schema
        && stored.metadata_schema == candidate.metadata_schema
        && stored.source_descriptor_json == candidate.source_descriptor_json
        && stored.block_rows_total == candidate.block_rows_total
}

fn validate_checkpoint(
    epoch: u64,
    previous: Checkpoint,
    block_rows_total: u64,
    batch: &CheckpointBatch,
) -> Result<()> {
    let next = batch.checkpoint;
    if next.next_block_row <= previous.next_block_row {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: "next_block_row must advance".into(),
        });
    }
    if next.next_block_row > block_rows_total {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: format!(
                "next_block_row {} exceeds block row count {block_rows_total}",
                next.next_block_row
            ),
        });
    }
    if next.scanned_blocks != next.next_block_row {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: "scanned_blocks must equal next_block_row".into(),
        });
    }
    let counters = [
        (
            "scanned_blocks",
            previous.scanned_blocks,
            next.scanned_blocks,
        ),
        (
            "scanned_transactions",
            previous.scanned_transactions,
            next.scanned_transactions,
        ),
        (
            "matched_transactions",
            previous.matched_transactions,
            next.matched_transactions,
        ),
        (
            "indeterminate_transactions",
            previous.indeterminate_transactions,
            next.indeterminate_transactions,
        ),
    ];
    for (name, old, new) in counters {
        if new < old {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: format!("{name} moved backwards from {old} to {new}"),
            });
        }
    }
    if next.matched_transactions + next.indeterminate_transactions > next.scanned_transactions {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: "matched plus indeterminate transactions exceeds scanned transactions".into(),
        });
    }
    Ok(())
}

fn validate_batch_shape(
    epoch: u64,
    kind: DumpKind,
    policy: OnIndeterminate,
    batch: &CheckpointBatch,
) -> Result<()> {
    let wrong_epoch = batch.transactions.iter().any(|row| row.epoch != epoch)
        || batch
            .transaction_accounts
            .iter()
            .any(|row| row.epoch != epoch)
        || batch.program_matches.iter().any(|row| row.epoch != epoch)
        || batch.token_matches.iter().any(|row| row.epoch != epoch)
        || batch.token_balances.iter().any(|row| row.epoch != epoch)
        || batch.coverage_issues.iter().any(|row| row.epoch != epoch);
    if wrong_epoch {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: "a batch row belongs to a different epoch".into(),
        });
    }
    let summary_count = match kind {
        DumpKind::Program if batch.token_matches.is_empty() && batch.token_balances.is_empty() => {
            batch.program_matches.len()
        }
        DumpKind::Token if batch.program_matches.is_empty() => batch.token_matches.len(),
        _ => {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: "batch match rows do not agree with dump kind".into(),
            });
        }
    };
    if summary_count != batch.transactions.len() {
        return Err(DumpError::InvalidCheckpoint {
            epoch,
            message: "each stored transaction must have one match summary".into(),
        });
    }
    match policy {
        OnIndeterminate::Fail
            if batch.checkpoint.indeterminate_transactions != 0
                || !batch.coverage_issues.is_empty() =>
        {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: "fail policy cannot commit an indeterminate transaction".into(),
            });
        }
        OnIndeterminate::Skip if !batch.coverage_issues.is_empty() => {
            return Err(DumpError::InvalidCheckpoint {
                epoch,
                message: "skip policy must not store coverage issue details".into(),
            });
        }
        OnIndeterminate::Record | OnIndeterminate::Skip | OnIndeterminate::Fail => {}
    }
    Ok(())
}

fn insert_transaction(transaction: &Transaction<'_>, record: &MatchRecord) -> Result<()> {
    if record.signatures.is_empty() || record.signatures.len() > 255 {
        return Err(DumpError::InvalidCheckpoint {
            epoch: record.epoch,
            message: "a transaction must contain 1 to 255 signatures".into(),
        });
    }
    if record.message_bytes.is_empty() {
        return Err(DumpError::InvalidCheckpoint {
            epoch: record.epoch,
            message: "message bytes must not be empty".into(),
        });
    }
    let metadata_valid = matches!(
        (record.metadata_state, record.metadata_wincode.as_ref()),
        (MetadataState::Absent, None)
            | (MetadataState::Decoded | MetadataState::RawFallback, Some(_))
    );
    if !metadata_valid {
        return Err(DumpError::InvalidCheckpoint {
            epoch: record.epoch,
            message: "metadata state and bytes do not agree".into(),
        });
    }
    let mut signatures = Vec::with_capacity(record.signatures.len() * 64);
    for signature in &record.signatures {
        signatures.extend_from_slice(signature);
    }
    let primary_base58 = bs58::encode(record.signatures[0]).into_string();
    transaction.execute(
        "INSERT INTO transactions (
            epoch, slot, block_id, tx_index, source_flags, first_signature_ordinal,
            signature_count, primary_signature, primary_signature_base58, signatures,
            message_state, message_bytes, metadata_state, metadata_wincode
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            sqlite_u64(record.epoch, "epoch")?,
            sqlite_u64(record.slot, "slot")?,
            i64::from(record.block_id),
            i64::from(record.tx_index),
            i64::from(record.source_flags),
            sqlite_u64(record.first_signature_ordinal, "first_signature_ordinal")?,
            i64::try_from(record.signatures.len()).expect("signature count is at most 255"),
            &record.signatures[0][..],
            primary_base58,
            signatures,
            record.message_state,
            record.message_bytes,
            record.metadata_state,
            record.metadata_wincode,
        ],
    )?;
    for (index, signature) in record.signatures.iter().enumerate() {
        transaction.execute(
            "INSERT INTO transaction_signatures (
                epoch, slot, tx_index, signature_index, signature, signature_base58
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                sqlite_u64(record.epoch, "epoch")?,
                sqlite_u64(record.slot, "slot")?,
                i64::from(record.tx_index),
                i64::try_from(index).expect("signature index is at most 254"),
                &signature[..],
                bs58::encode(signature).into_string(),
            ],
        )?;
    }
    Ok(())
}

fn insert_transaction_account(
    transaction: &Transaction<'_>,
    account: &TransactionAccountRecord,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO transaction_accounts (
            epoch, slot, tx_index, account_index, pubkey, pubkey_base58,
            source, is_signer, is_writable
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            sqlite_u64(account.epoch, "epoch")?,
            sqlite_u64(account.slot, "slot")?,
            i64::from(account.tx_index),
            i64::from(account.account_index),
            &account.pubkey[..],
            bs58::encode(account.pubkey).into_string(),
            account.source,
            i64::from(account.is_signer),
            i64::from(account.is_writable),
        ],
    )?;
    Ok(())
}

fn insert_token_balance(transaction: &Transaction<'_>, balance: &TokenBalanceRecord) -> Result<()> {
    let owner_base58 = balance.owner.map(|value| bs58::encode(value).into_string());
    let token_program_base58 = balance
        .token_program
        .map(|value| bs58::encode(value).into_string());
    transaction.execute(
        "INSERT INTO token_balances (
            epoch, slot, tx_index, side, balance_index, account_index,
            mint, mint_base58, owner, owner_base58, token_program,
            token_program_base58, amount_u64, amount_le, decimals
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            sqlite_u64(balance.epoch, "epoch")?,
            sqlite_u64(balance.slot, "slot")?,
            i64::from(balance.tx_index),
            balance.side,
            i64::from(balance.balance_index),
            i64::from(balance.account_index),
            &balance.mint[..],
            bs58::encode(balance.mint).into_string(),
            balance.owner.as_ref().map(|value| &value[..]),
            owner_base58,
            balance.token_program.as_ref().map(|value| &value[..]),
            token_program_base58,
            balance.amount.to_string(),
            &balance.amount.to_le_bytes()[..],
            i64::from(balance.decimals),
        ],
    )?;
    Ok(())
}

fn ensure_epoch(connection: &Connection, epoch: u64) -> Result<()> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM epochs WHERE epoch = ?1",
            [sqlite_u64(epoch, "epoch")?],
            |row| row.get(0),
        )
        .optional()?;
    if exists.is_none() {
        return Err(DumpError::UnknownEpoch { epoch });
    }
    Ok(())
}

fn count_all(connection: &Connection, table: &'static str) -> Result<u64> {
    let sql = match table {
        "transactions" => "SELECT count(*) FROM transactions",
        "coverage_issues" => "SELECT count(*) FROM coverage_issues",
        _ => unreachable!("fixed internal table name"),
    };
    let value: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(u64::try_from(value).expect("SQLite count is non-negative"))
}

fn count_for_epoch(connection: &Connection, table: &'static str, epoch: u64) -> Result<u64> {
    let sql = match table {
        "transactions" => "SELECT count(*) FROM transactions WHERE epoch = ?1",
        "coverage_issues" => "SELECT count(*) FROM coverage_issues WHERE epoch = ?1",
        _ => unreachable!("fixed internal table name"),
    };
    let value: i64 = connection.query_row(sql, [sqlite_u64(epoch, "epoch")?], |row| row.get(0))?;
    Ok(u64::try_from(value).expect("SQLite count is non-negative"))
}

fn sqlite_u64(value: u64, field: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| DumpError::ValueOutOfRange { field, value })
}

fn sql_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

fn sql_optional_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    let value: Option<i64> = row.get(index)?;
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
        })
        .transpose()
}

fn unix_seconds() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DumpError::InvalidClock)?
        .as_secs();
    sqlite_u64(seconds, "unix time")
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex nibble"));
        output.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("hex nibble"));
    }
    output
}

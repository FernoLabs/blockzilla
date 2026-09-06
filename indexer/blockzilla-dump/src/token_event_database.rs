//! Deterministic SQLite storage for instruction-derived classic Token events.
//!
//! This module is separate from the legacy pre/post-balance dump. One file is
//! bound to one exact source identity, target mint, Token program, scan range,
//! and opening tracker checkpoint.
//!
//! The database must be in an existing private directory that is owned by the
//! current user and is not writable by the group or by other users. That
//! directory is the safety boundary for SQLite journal, WAL, and SHM files.
//!
//! The path checks prevent mutation by a different operating-system user. A
//! process with the same effective user ID can still rename files or change
//! directory entries. Such concurrent same-user mutation is outside this
//! module's threat model; callers must keep the private directory exclusive to
//! the writer while it is open. No custom SQLite VFS is used.
//!
//! The digest chain detects accidental changes to a private database. It is
//! not an authentication mechanism. A malicious process with the same user ID
//! can coordinate changes to rows and their in-database digests.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    ops::Deref,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use blockzilla_model::{
    ArchiveFormat, BlockView, CoverageReason, CpiCoverage, ExecutionStatus, InstructionCoverage,
    InstructionDataCoverage, ScanRange, SourceIdentity, SourceVerification, TransactionView,
    token::{
        AccountLifecycleChange, BalanceDirection, CLASSIC_SPL_TOKEN_PROGRAM_ID,
        ClassicTokenInstruction, DecodedClassicTokenBatch, HistoryCoverage, LifecycleCause,
        MAX_EXPANDED_TOKEN_LEAVES, MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION,
        MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION, MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION,
        ObservedTokenInstruction, PubkeyBytes, RawUnknownTokenInstruction, TargetAccountSnapshot,
        TargetMintEffect, TargetMintTracker, TargetMintTrackerError, TargetMintTrackerSnapshot,
        TokenAccountLifecycle, TokenAccountRole, TokenAccountState, TokenAuthorityType,
        TokenCommitState, TokenCoverageIssue, TokenCoverageIssueKind, TokenInvocationEvidence,
        TrackedTokenEvent, TrackedTokenTransaction, TransferLeg, TransferLegRole,
        decode_classic_token_batch, decode_classic_token_instruction,
    },
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
    types::ValueRef,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// SQLite application ID for an instruction-only Token event database.
pub const TOKEN_EVENT_APPLICATION_ID: i64 = 0x425a_5445; // "BZTE"
/// Current instruction-only Token event schema version.
///
/// Version 2 adds the `object-set-bound` source-verification value. An
/// existing version-1 database keeps its exact schema and is rejected before
/// resume instead of being accepted under a changed version-1 contract.
pub const TOKEN_EVENT_SCHEMA_VERSION: i64 = 2;

const MAX_EVENTS_PER_TRANSACTION: usize = MAX_EXPANDED_TOKEN_LEAVES;
const MAX_ACCOUNT_UPDATES_PER_TRANSACTION: usize = MAX_TOKEN_ACCOUNT_UPDATES_PER_TRANSACTION;
const MAX_EFFECTS_PER_TRANSACTION: usize = MAX_EXPANDED_TOKEN_LEAVES * 3;
// A terminal raw Batch event can repeat the parent accounts and bytes after
// valid child-prefix events. Two input-size budgets cover that exact output.
const MAX_TRACKED_EVENT_RESOURCE_BYTES: usize = MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION * 2;
const MAX_SOURCE_IDENTITY_TEXT_BYTES: usize = 4 * 1024;
const MAX_DURABLE_TEXT_BYTES: usize = 64 * 1024;
const DIGEST_BYTES: usize = 32;
const EMPTY_DIGEST_HEAD: [u8; DIGEST_BYTES] = [0; DIGEST_BYTES];

const SCHEMA: &str = r#"
CREATE TABLE pubkeys (
    pubkey_id  INTEGER PRIMARY KEY,
    address    BLOB NOT NULL UNIQUE CHECK (length(address) = 32)
) STRICT;

CREATE TABLE run_identity (
    singleton                 INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version            INTEGER NOT NULL CHECK (schema_version = 2),
    source_format             TEXT NOT NULL CHECK (source_format IN ('car', 'compact-v2', 'indexer-v3')),
    source_label              TEXT NOT NULL,
    source_cluster_id         TEXT,
    source_epoch_le           BLOB NOT NULL CHECK (length(source_epoch_le) = 8),
    source_epoch_text         TEXT NOT NULL CHECK (source_epoch_text = '0' OR (length(source_epoch_text) BETWEEN 1 AND 20 AND substr(source_epoch_text, 1, 1) BETWEEN '1' AND '9' AND source_epoch_text NOT GLOB '*[^0-9]*')),
    source_first_slot_le      BLOB NOT NULL CHECK (length(source_first_slot_le) = 8),
    source_first_slot_text    TEXT NOT NULL CHECK (source_first_slot_text = '0' OR (length(source_first_slot_text) BETWEEN 1 AND 20 AND substr(source_first_slot_text, 1, 1) BETWEEN '1' AND '9' AND source_first_slot_text NOT GLOB '*[^0-9]*')),
    source_slots_per_epoch_le BLOB NOT NULL CHECK (length(source_slots_per_epoch_le) = 8),
    source_slots_per_epoch_text TEXT NOT NULL CHECK (source_slots_per_epoch_text = '0' OR (length(source_slots_per_epoch_text) BETWEEN 1 AND 20 AND substr(source_slots_per_epoch_text, 1, 1) BETWEEN '1' AND '9' AND source_slots_per_epoch_text NOT GLOB '*[^0-9]*')),
    source_block_count        INTEGER NOT NULL CHECK (source_block_count BETWEEN 0 AND 4294967295),
    source_verification       TEXT NOT NULL CHECK (source_verification IN ('object-set-bound', 'operator-trusted', 'internal-binding-only', 'unverified')),
    source_binding            TEXT,
    target_mint_pubkey_id     INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    token_program_pubkey_id   INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    first_block_ordinal       INTEGER NOT NULL CHECK (first_block_ordinal BETWEEN 0 AND 4294967295),
    range_block_count         INTEGER NOT NULL CHECK (range_block_count BETWEEN 1 AND 4294967295)
) STRICT;

CREATE TABLE opening_tracker_state (
    singleton          INTEGER PRIMARY KEY CHECK (singleton = 1),
    history_coverage   TEXT NOT NULL CHECK (history_coverage IN ('complete', 'partial')),
    certainty_revision_le BLOB NOT NULL CHECK (length(certainty_revision_le) = 8),
    certainty_revision_text TEXT NOT NULL CHECK (certainty_revision_text = '0' OR (length(certainty_revision_text) BETWEEN 1 AND 20 AND substr(certainty_revision_text, 1, 1) BETWEEN '1' AND '9' AND certainty_revision_text NOT GLOB '*[^0-9]*'))
) STRICT;

CREATE TABLE opening_tracker_accounts (
    pubkey_id              INTEGER PRIMARY KEY REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    generation_le          BLOB NOT NULL CHECK (length(generation_le) = 8),
    generation_text        TEXT NOT NULL CHECK (generation_text = '0' OR (length(generation_text) BETWEEN 1 AND 20 AND substr(generation_text, 1, 1) BETWEEN '1' AND '9' AND generation_text NOT GLOB '*[^0-9]*')),
    account_state          TEXT NOT NULL CHECK (account_state IN ('active-target', 'active-other', 'closed')),
    state_mint_pubkey_id   INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    confirmed_revision_le  BLOB NOT NULL CHECK (length(confirmed_revision_le) = 8),
    confirmed_revision_text TEXT NOT NULL CHECK (confirmed_revision_text = '0' OR (length(confirmed_revision_text) BETWEEN 1 AND 20 AND substr(confirmed_revision_text, 1, 1) BETWEEN '1' AND '9' AND confirmed_revision_text NOT GLOB '*[^0-9]*')),
    CHECK ((account_state = 'active-other' AND state_mint_pubkey_id IS NOT NULL)
        OR (account_state = 'active-target' AND state_mint_pubkey_id IS NULL)
        OR account_state = 'closed')
) STRICT;

CREATE TABLE checkpoint (
    singleton            INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_block_ordinal   INTEGER NOT NULL CHECK (next_block_ordinal BETWEEN 0 AND 4294967295),
    digest_head          BLOB NOT NULL CHECK (length(digest_head) = 32),
    tracker_digest       BLOB NOT NULL CHECK (length(tracker_digest) = 32)
) STRICT;

CREATE TABLE blocks (
    block_ordinal  INTEGER PRIMARY KEY CHECK (block_ordinal BETWEEN 0 AND 4294967295),
    epoch_le       BLOB NOT NULL CHECK (length(epoch_le) = 8),
    epoch_text     TEXT NOT NULL CHECK (epoch_text = '0' OR (length(epoch_text) BETWEEN 1 AND 20 AND substr(epoch_text, 1, 1) BETWEEN '1' AND '9' AND epoch_text NOT GLOB '*[^0-9]*')),
    slot_le        BLOB NOT NULL CHECK (length(slot_le) = 8),
    slot_text      TEXT NOT NULL CHECK (slot_text = '0' OR (length(slot_text) BETWEEN 1 AND 20 AND substr(slot_text, 1, 1) BETWEEN '1' AND '9' AND slot_text NOT GLOB '*[^0-9]*')),
    transaction_count INTEGER NOT NULL CHECK (transaction_count BETWEEN 0 AND 4294967295),
    tracker_history_after TEXT NOT NULL CHECK (tracker_history_after IN ('complete', 'partial')),
    tracker_revision_after_le BLOB NOT NULL CHECK (length(tracker_revision_after_le) = 8),
    tracker_revision_after_text TEXT NOT NULL CHECK (tracker_revision_after_text = '0' OR (length(tracker_revision_after_text) BETWEEN 1 AND 20 AND substr(tracker_revision_after_text, 1, 1) BETWEEN '1' AND '9' AND tracker_revision_after_text NOT GLOB '*[^0-9]*')),
    tracker_digest_after BLOB NOT NULL CHECK (length(tracker_digest_after) = 32),
    source_digest BLOB NOT NULL CHECK (length(source_digest) = 32),
    durable_rows_digest BLOB NOT NULL CHECK (length(durable_rows_digest) = 32),
    chain_digest BLOB NOT NULL CHECK (length(chain_digest) = 32)
) STRICT;

CREATE TABLE transactions (
    block_ordinal       INTEGER NOT NULL REFERENCES blocks(block_ordinal) ON DELETE RESTRICT,
    tx_index            INTEGER NOT NULL CHECK (tx_index BETWEEN 0 AND 4294967295),
    execution_status    TEXT NOT NULL CHECK (execution_status IN ('succeeded', 'failed', 'unknown')),
    status_reason       TEXT,
    failed_outer_index  INTEGER CHECK (failed_outer_index IS NULL OR failed_outer_index BETWEEN 0 AND 4294967295),
    primary_signature   BLOB CHECK (primary_signature IS NULL OR length(primary_signature) = 64),
    tracker_history_after TEXT NOT NULL CHECK (tracker_history_after IN ('complete', 'partial')),
    tracker_revision_after_le BLOB NOT NULL CHECK (length(tracker_revision_after_le) = 8),
    tracker_revision_after_text TEXT NOT NULL CHECK (tracker_revision_after_text = '0' OR (length(tracker_revision_after_text) BETWEEN 1 AND 20 AND substr(tracker_revision_after_text, 1, 1) BETWEEN '1' AND '9' AND tracker_revision_after_text NOT GLOB '*[^0-9]*')),
    CHECK ((execution_status = 'unknown' AND status_reason IS NOT NULL) OR (execution_status != 'unknown' AND status_reason IS NULL)),
    CHECK (execution_status = 'failed' OR failed_outer_index IS NULL),
    PRIMARY KEY (block_ordinal, tx_index)
) STRICT, WITHOUT ROWID;

CREATE TABLE account_lifetimes (
    pubkey_id               INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    generation_le           BLOB NOT NULL CHECK (length(generation_le) = 8),
    generation_text         TEXT NOT NULL CHECK (generation_text = '0' OR (length(generation_text) BETWEEN 1 AND 20 AND substr(generation_text, 1, 1) BETWEEN '1' AND '9' AND generation_text NOT GLOB '*[^0-9]*')),
    account_state           TEXT NOT NULL CHECK (account_state IN ('active-target', 'active-other', 'closed')),
    state_mint_pubkey_id    INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    confirmed_revision_le   BLOB NOT NULL CHECK (length(confirmed_revision_le) = 8),
    confirmed_revision_text TEXT NOT NULL CHECK (confirmed_revision_text = '0' OR (length(confirmed_revision_text) BETWEEN 1 AND 20 AND substr(confirmed_revision_text, 1, 1) BETWEEN '1' AND '9' AND confirmed_revision_text NOT GLOB '*[^0-9]*')),
    CHECK ((account_state = 'active-other' AND state_mint_pubkey_id IS NOT NULL)
        OR (account_state = 'active-target' AND state_mint_pubkey_id IS NULL)
        OR account_state = 'closed'),
    PRIMARY KEY (pubkey_id, generation_le),
    UNIQUE (pubkey_id, generation_text)
) STRICT, WITHOUT ROWID;

CREATE TABLE tracker_state (
    singleton                 INTEGER PRIMARY KEY CHECK (singleton = 1),
    history_coverage          TEXT NOT NULL CHECK (history_coverage IN ('complete', 'partial')),
    certainty_revision_le     BLOB NOT NULL CHECK (length(certainty_revision_le) = 8),
    certainty_revision_text   TEXT NOT NULL CHECK (certainty_revision_text = '0' OR (length(certainty_revision_text) BETWEEN 1 AND 20 AND substr(certainty_revision_text, 1, 1) BETWEEN '1' AND '9' AND certainty_revision_text NOT GLOB '*[^0-9]*'))
) STRICT;

CREATE TABLE tracker_accounts (
    pubkey_id       INTEGER PRIMARY KEY REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    generation_le   BLOB NOT NULL CHECK (length(generation_le) = 8),
    FOREIGN KEY (pubkey_id, generation_le) REFERENCES account_lifetimes(pubkey_id, generation_le) ON DELETE RESTRICT
) STRICT;

CREATE TABLE tracker_account_updates (
    block_ordinal             INTEGER NOT NULL,
    tx_index                  INTEGER NOT NULL,
    update_index              INTEGER NOT NULL CHECK (update_index BETWEEN 0 AND 4294967295),
    pubkey_id                 INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    generation_le             BLOB NOT NULL CHECK (length(generation_le) = 8),
    generation_text           TEXT NOT NULL CHECK (generation_text = '0' OR (length(generation_text) BETWEEN 1 AND 20 AND substr(generation_text, 1, 1) BETWEEN '1' AND '9' AND generation_text NOT GLOB '*[^0-9]*')),
    account_state             TEXT NOT NULL CHECK (account_state IN ('active-target', 'active-other', 'closed')),
    state_mint_pubkey_id      INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    confirmed_revision_le     BLOB NOT NULL CHECK (length(confirmed_revision_le) = 8),
    confirmed_revision_text   TEXT NOT NULL CHECK (confirmed_revision_text = '0' OR (length(confirmed_revision_text) BETWEEN 1 AND 20 AND substr(confirmed_revision_text, 1, 1) BETWEEN '1' AND '9' AND confirmed_revision_text NOT GLOB '*[^0-9]*')),
    CHECK ((account_state = 'active-other' AND state_mint_pubkey_id IS NOT NULL)
        OR (account_state = 'active-target' AND state_mint_pubkey_id IS NULL)
        OR account_state = 'closed'),
    PRIMARY KEY (block_ordinal, tx_index, update_index),
    FOREIGN KEY (block_ordinal, tx_index) REFERENCES transactions(block_ordinal, tx_index) ON DELETE RESTRICT,
    FOREIGN KEY (pubkey_id, generation_le) REFERENCES account_lifetimes(pubkey_id, generation_le) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE events (
    event_id                 INTEGER PRIMARY KEY,
    block_ordinal            INTEGER NOT NULL,
    tx_index                 INTEGER NOT NULL,
    event_index              INTEGER NOT NULL CHECK (event_index BETWEEN 0 AND 4294967295),
    instruction_order        INTEGER NOT NULL CHECK (instruction_order BETWEEN 0 AND 4294967295),
    outer_index              INTEGER NOT NULL CHECK (outer_index BETWEEN 0 AND 4294967295),
    inner_index              INTEGER CHECK (inner_index IS NULL OR inner_index BETWEEN 0 AND 4294967295),
    stack_height             INTEGER CHECK (stack_height IS NULL OR stack_height BETWEEN 1 AND 4294967295),
    batch_index              INTEGER CHECK (batch_index IS NULL OR batch_index BETWEEN 0 AND 4294967295),
    invocation_state         TEXT NOT NULL CHECK (invocation_state IN ('invoked', 'not-invoked', 'unknown')),
    commit_state             TEXT NOT NULL CHECK (commit_state IN ('committed', 'rolled-back', 'not-committed', 'unknown')),
    program_pubkey_id        INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    raw_kind                 TEXT NOT NULL CHECK (raw_kind IN ('classic', 'unknown')),
    token_tag                INTEGER CHECK (token_tag IS NULL OR token_tag BETWEEN 0 AND 255),
    data_coverage            TEXT NOT NULL CHECK (data_coverage IN ('exact', 'not-requested', 'unknown')),
    data_coverage_reason     TEXT,
    raw_data                 BLOB,
    trailing_data            BLOB,
    amount_le                BLOB CHECK (amount_le IS NULL OR length(amount_le) = 8),
    amount_text              TEXT CHECK (amount_text IS NULL OR amount_text = '0' OR (length(amount_text) BETWEEN 1 AND 20 AND substr(amount_text, 1, 1) BETWEEN '1' AND '9' AND amount_text NOT GLOB '*[^0-9]*')),
    decimals                 INTEGER CHECK (decimals IS NULL OR decimals BETWEEN 0 AND 255),
    required_signers         INTEGER CHECK (required_signers IS NULL OR required_signers BETWEEN 0 AND 255),
    authority_type           TEXT CHECK (authority_type IS NULL OR authority_type IN ('mint-tokens', 'freeze-account', 'account-owner', 'close-account')),
    embedded_pubkey_a        INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    embedded_pubkey_b        INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    optional_value_present   INTEGER CHECK (optional_value_present IS NULL OR optional_value_present IN (0, 1)),
    ui_amount                TEXT,
    CHECK ((data_coverage = 'unknown' AND data_coverage_reason IS NOT NULL) OR (data_coverage != 'unknown' AND data_coverage_reason IS NULL)),
    CHECK ((amount_le IS NULL) = (amount_text IS NULL)),
    CHECK ((raw_kind = 'classic' AND token_tag IS NOT NULL AND data_coverage = 'exact' AND raw_data IS NULL AND trailing_data IS NOT NULL) OR raw_kind = 'unknown'),
    UNIQUE (block_ordinal, tx_index, event_index),
    FOREIGN KEY (block_ordinal, tx_index) REFERENCES transactions(block_ordinal, tx_index) ON DELETE RESTRICT
) STRICT;

CREATE TABLE event_accounts (
    event_id       INTEGER NOT NULL REFERENCES events(event_id) ON DELETE RESTRICT,
    binding_index  INTEGER NOT NULL CHECK (binding_index BETWEEN 0 AND 4294967295),
    account_index  INTEGER NOT NULL CHECK (account_index BETWEEN 0 AND 4294967295),
    pubkey_id      INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    semantic_role  TEXT CHECK (semantic_role IS NULL OR semantic_role IN ('mint', 'token-account', 'multisig-account', 'source', 'destination', 'lamport-destination', 'owner', 'delegate', 'authority', 'authority-subject', 'rent-sysvar', 'multisig-signer', 'additional')),
    PRIMARY KEY (event_id, binding_index)
) STRICT, WITHOUT ROWID;

CREATE TABLE event_effects (
    event_id       INTEGER NOT NULL REFERENCES events(event_id) ON DELETE RESTRICT,
    effect_index   INTEGER NOT NULL CHECK (effect_index BETWEEN 0 AND 4294967295),
    effect_kind    TEXT NOT NULL CHECK (effect_kind IN ('lifecycle', 'transfer', 'mint', 'burn')),
    amount_le      BLOB CHECK (amount_le IS NULL OR length(amount_le) = 8),
    amount_text    TEXT CHECK (amount_text IS NULL OR amount_text = '0' OR (length(amount_text) BETWEEN 1 AND 20 AND substr(amount_text, 1, 1) BETWEEN '1' AND '9' AND amount_text NOT GLOB '*[^0-9]*')),
    decimals       INTEGER CHECK (decimals IS NULL OR decimals BETWEEN 0 AND 255),
    checked        INTEGER CHECK (checked IS NULL OR checked IN (0, 1)),
    CHECK ((amount_le IS NULL) = (amount_text IS NULL)),
    CHECK ((effect_kind = 'lifecycle' AND amount_le IS NULL AND decimals IS NULL AND checked IS NULL) OR (effect_kind != 'lifecycle' AND amount_le IS NOT NULL)),
    CHECK ((effect_kind = 'transfer' AND checked IS NOT NULL) OR (effect_kind != 'transfer' AND checked IS NULL)),
    PRIMARY KEY (event_id, effect_index)
) STRICT, WITHOUT ROWID;

CREATE TABLE lifecycle_effects (
    event_id                  INTEGER NOT NULL,
    effect_index              INTEGER NOT NULL,
    account_pubkey_id         INTEGER NOT NULL REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    before_generation_le      BLOB CHECK (before_generation_le IS NULL OR length(before_generation_le) = 8),
    before_generation_text    TEXT CHECK (before_generation_text IS NULL OR before_generation_text = '0' OR (length(before_generation_text) BETWEEN 1 AND 20 AND substr(before_generation_text, 1, 1) BETWEEN '1' AND '9' AND before_generation_text NOT GLOB '*[^0-9]*')),
    before_state              TEXT CHECK (before_state IS NULL OR before_state IN ('active-target', 'active-other', 'closed')),
    before_state_mint_pubkey_id INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    after_generation_le       BLOB NOT NULL CHECK (length(after_generation_le) = 8),
    after_generation_text     TEXT NOT NULL CHECK (after_generation_text = '0' OR (length(after_generation_text) BETWEEN 1 AND 20 AND substr(after_generation_text, 1, 1) BETWEEN '1' AND '9' AND after_generation_text NOT GLOB '*[^0-9]*')),
    after_state               TEXT NOT NULL CHECK (after_state IN ('active-target', 'active-other', 'closed')),
    after_state_mint_pubkey_id INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    cause                     TEXT NOT NULL CHECK (cause IN ('initialize-account', 'explicit-mint-instruction', 'checked-transfer', 'unchecked-transfer', 'close-account')),
    CHECK ((before_generation_le IS NULL) = (before_generation_text IS NULL)),
    CHECK ((before_generation_le IS NULL) = (before_state IS NULL)),
    CHECK ((before_state IS NULL AND before_state_mint_pubkey_id IS NULL)
        OR (before_state = 'active-other' AND before_state_mint_pubkey_id IS NOT NULL)
        OR (before_state = 'active-target' AND before_state_mint_pubkey_id IS NULL)
        OR before_state = 'closed'),
    CHECK ((after_state = 'active-other' AND after_state_mint_pubkey_id IS NOT NULL)
        OR (after_state = 'active-target' AND after_state_mint_pubkey_id IS NULL)
        OR after_state = 'closed'),
    PRIMARY KEY (event_id, effect_index),
    FOREIGN KEY (event_id, effect_index) REFERENCES event_effects(event_id, effect_index) ON DELETE RESTRICT,
    FOREIGN KEY (account_pubkey_id, before_generation_le) REFERENCES account_lifetimes(pubkey_id, generation_le) ON DELETE RESTRICT,
    FOREIGN KEY (account_pubkey_id, after_generation_le) REFERENCES account_lifetimes(pubkey_id, generation_le) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE delta_legs (
    event_id        INTEGER NOT NULL,
    effect_index    INTEGER NOT NULL,
    leg_index       INTEGER NOT NULL CHECK (leg_index BETWEEN 0 AND 1),
    account_pubkey_id INTEGER NOT NULL,
    generation_le   BLOB NOT NULL CHECK (length(generation_le) = 8),
    generation_text TEXT NOT NULL CHECK (generation_text = '0' OR (length(generation_text) BETWEEN 1 AND 20 AND substr(generation_text, 1, 1) BETWEEN '1' AND '9' AND generation_text NOT GLOB '*[^0-9]*')),
    direction       TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    transfer_role   TEXT CHECK (transfer_role IS NULL OR transfer_role IN ('source', 'destination')),
    amount_le       BLOB NOT NULL CHECK (length(amount_le) = 8),
    amount_text     TEXT NOT NULL CHECK (amount_text = '0' OR (length(amount_text) BETWEEN 1 AND 20 AND substr(amount_text, 1, 1) BETWEEN '1' AND '9' AND amount_text NOT GLOB '*[^0-9]*')),
    PRIMARY KEY (event_id, effect_index, leg_index),
    FOREIGN KEY (event_id, effect_index) REFERENCES event_effects(event_id, effect_index) ON DELETE RESTRICT,
    FOREIGN KEY (account_pubkey_id, generation_le) REFERENCES account_lifetimes(pubkey_id, generation_le) ON DELETE RESTRICT
) STRICT, WITHOUT ROWID;

CREATE TABLE coverage_issues (
    issue_id                   INTEGER PRIMARY KEY,
    block_ordinal              INTEGER NOT NULL,
    tx_index                   INTEGER NOT NULL,
    issue_index                INTEGER NOT NULL CHECK (issue_index BETWEEN 0 AND 4294967295),
    instruction_order          INTEGER CHECK (instruction_order IS NULL OR instruction_order BETWEEN 0 AND 4294967295),
    outer_index                INTEGER CHECK (outer_index IS NULL OR outer_index BETWEEN 0 AND 4294967295),
    inner_index                INTEGER CHECK (inner_index IS NULL OR inner_index BETWEEN 0 AND 4294967295),
    stack_height               INTEGER CHECK (stack_height IS NULL OR stack_height BETWEEN 1 AND 4294967295),
    issue_kind                 TEXT NOT NULL CHECK (issue_kind IN ('decode', 'instruction-data-unavailable', 'insufficient-history', 'conflicting-mint-evidence', 'sync-native-on-target', 'invalid-instruction-order', 'incomplete-instructions', 'incomplete-cpi', 'cpi-not-recorded', 'unknown-execution')),
    detail                     TEXT,
    data_coverage              TEXT,
    coverage_reason            TEXT,
    first_pubkey_id            INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    second_pubkey_id           INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    known_mint_pubkey_id       INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    observed_mint_pubkey_id    INTEGER REFERENCES pubkeys(pubkey_id) ON DELETE RESTRICT,
    expected_index             INTEGER CHECK (expected_index IS NULL OR expected_index BETWEEN 0 AND 4294967295),
    actual_index               INTEGER CHECK (actual_index IS NULL OR actual_index BETWEEN 0 AND 4294967295),
    UNIQUE (block_ordinal, tx_index, issue_index),
    FOREIGN KEY (block_ordinal, tx_index) REFERENCES transactions(block_ordinal, tx_index) ON DELETE RESTRICT
) STRICT;

CREATE INDEX events_by_coordinate ON events(block_ordinal, tx_index, instruction_order, batch_index);
CREATE INDEX event_accounts_by_pubkey ON event_accounts(pubkey_id, event_id);
CREATE INDEX delta_legs_by_account_lifetime ON delta_legs(account_pubkey_id, generation_le, event_id, effect_index, leg_index);
CREATE INDEX coverage_issues_by_transaction ON coverage_issues(block_ordinal, tx_index, issue_index);
"#;

/// An exact run binding for one instruction-only token event database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEventRunSpec {
    pub source: SourceIdentity,
    pub target_mint: PubkeyBytes,
    pub token_program: PubkeyBytes,
    pub range: ScanRange,
    pub opening_tracker: TargetMintTrackerSnapshot,
}

impl TokenEventRunSpec {
    /// Create a run binding for the classic SPL Token program.
    pub fn classic(
        source: SourceIdentity,
        target_mint: PubkeyBytes,
        range: ScanRange,
        opening_tracker: TargetMintTrackerSnapshot,
    ) -> Self {
        Self {
            source,
            target_mint,
            token_program: CLASSIC_SPL_TOKEN_PROGRAM_ID,
            range,
            opening_tracker,
        }
    }

    fn end_block_exclusive(&self) -> Result<u32> {
        self.range
            .first_block
            .checked_add(self.range.block_count.get())
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidSpecification(
                    "the scan range end exceeds u32".into(),
                )
            })
    }

    fn validate(&self) -> Result<()> {
        if self.token_program != CLASSIC_SPL_TOKEN_PROGRAM_ID {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "the token event tracker supports only the classic SPL Token program".into(),
            ));
        }
        if self.opening_tracker.target_mint() != self.target_mint {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "the opening tracker target mint differs from the run target".into(),
            ));
        }
        if self.source.label.is_empty() {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "source label must not be empty".into(),
            ));
        }
        if self.source.label.len() > MAX_SOURCE_IDENTITY_TEXT_BYTES {
            return Err(TokenEventDatabaseError::InvalidSpecification(format!(
                "source label exceeds {MAX_SOURCE_IDENTITY_TEXT_BYTES} bytes"
            )));
        }
        if self
            .source
            .cluster_id
            .as_ref()
            .is_some_and(String::is_empty)
        {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "source cluster ID must not be empty".into(),
            ));
        }
        if self
            .source
            .cluster_id
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SOURCE_IDENTITY_TEXT_BYTES)
        {
            return Err(TokenEventDatabaseError::InvalidSpecification(format!(
                "source cluster ID exceeds {MAX_SOURCE_IDENTITY_TEXT_BYTES} bytes"
            )));
        }
        if self.source.slots_per_epoch == 0 {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "source slots_per_epoch must be greater than zero".into(),
            ));
        }
        if u64::from(self.source.block_count) > self.source.slots_per_epoch {
            return Err(TokenEventDatabaseError::InvalidSpecification(format!(
                "source block count {} is greater than slots per epoch {}",
                self.source.block_count, self.source.slots_per_epoch
            )));
        }
        if self
            .source
            .binding
            .as_ref()
            .is_none_or(|binding| binding.is_empty())
        {
            return Err(TokenEventDatabaseError::InvalidSpecification(
                "a resumable source must have a nonempty immutable binding".into(),
            ));
        }
        if self
            .source
            .binding
            .as_ref()
            .is_some_and(|value| value.len() > MAX_SOURCE_IDENTITY_TEXT_BYTES)
        {
            return Err(TokenEventDatabaseError::InvalidSpecification(format!(
                "source binding exceeds {MAX_SOURCE_IDENTITY_TEXT_BYTES} bytes"
            )));
        }
        self.source
            .first_slot
            .checked_add(self.source.slots_per_epoch - 1)
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidSpecification(
                    "the source slot range exceeds u64".into(),
                )
            })?;
        let end = self.end_block_exclusive()?;
        if end > self.source.block_count {
            return Err(TokenEventDatabaseError::InvalidSpecification(format!(
                "the scan range ends at block {end}, after source block count {}",
                self.source.block_count
            )));
        }
        Ok(())
    }
}

/// Durable state needed to continue an ordered token scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEventResume {
    pub next_block_ordinal: u32,
    pub tracker: TargetMintTrackerSnapshot,
}

/// A complete read-only validation result for one token event database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenEventAudit {
    pub spec: TokenEventRunSpec,
    pub resume: TokenEventResume,
    pub digest_head: [u8; 32],
    pub tracker_digest: [u8; 32],
}

/// Result of one complete-block commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockCommitOutcome {
    Committed,
    AlreadyCommitted,
}

/// In-process phase measurements for one token-event database writer.
///
/// These counters are diagnostic data only. They are not stored in SQLite and
/// do not take part in the durable digest chain. Durations are exclusive to
/// the named phase unless the field documentation says otherwise.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenEventDatabaseMetrics {
    /// Calls to [`TokenEventDatabase::track_and_commit_block`].
    pub block_operations: u64,
    /// New blocks whose SQLite transaction committed successfully.
    pub committed_blocks: u64,
    /// Previously committed blocks that were validated successfully.
    pub validated_replay_blocks: u64,
    /// Source transactions passed to the token tracker.
    pub visited_transactions: u64,
    /// Transactions that changed and wrote the singleton tracker state.
    pub tracker_state_updates: u64,
    /// Transactions whose unchanged singleton tracker-state write was skipped.
    pub tracker_state_noop_writes_skipped: u64,
    /// Public-key ID lookups served from the committed or transaction cache.
    pub pubkey_cache_hits: u64,
    /// Cache hits served by an ID first allocated in the current transaction.
    pub pubkey_pending_hits: u64,
    /// Public-key ID lookups that required SQLite insertion and selection.
    pub pubkey_sql_misses: u64,
    /// Wall time inside all block operations, including error recovery.
    pub block_operation_elapsed: Duration,
    /// Source-shape validation and canonical source-block hashing.
    pub source_validation_and_digest_elapsed: Duration,
    /// SQLite transaction creation and cached-checkpoint validation.
    pub sqlite_transaction_setup_elapsed: Duration,
    /// Token decoding and tracker state transitions.
    pub token_tracking_elapsed: Duration,
    /// Validation and SQLite row writes for tracked transactions.
    pub tracked_row_write_elapsed: Duration,
    /// Block-row writes before per-transaction processing.
    pub block_header_write_elapsed: Duration,
    /// Durable-row hashing, tracker hashing, and checkpoint-row updates.
    pub durable_digest_and_checkpoint_elapsed: Duration,
    /// SQLite transaction commit time for new and replayed blocks.
    pub sqlite_commit_elapsed: Duration,
    /// Durable reload and audit work after failed new-block operations.
    pub error_recovery_elapsed: Duration,
    /// Calls to [`TokenEventDatabase::checkpoint_wal`].
    pub wal_checkpoint_calls: u64,
    /// Successful calls to [`TokenEventDatabase::checkpoint_wal`].
    pub wal_checkpoint_successes: u64,
    /// Wall time in all WAL checkpoint attempts, including failed attempts.
    pub wal_checkpoint_elapsed: Duration,
}

impl TokenEventDatabaseMetrics {
    pub(crate) fn delta_since(self, earlier: Self) -> Self {
        Self {
            block_operations: self
                .block_operations
                .saturating_sub(earlier.block_operations),
            committed_blocks: self
                .committed_blocks
                .saturating_sub(earlier.committed_blocks),
            validated_replay_blocks: self
                .validated_replay_blocks
                .saturating_sub(earlier.validated_replay_blocks),
            visited_transactions: self
                .visited_transactions
                .saturating_sub(earlier.visited_transactions),
            tracker_state_updates: self
                .tracker_state_updates
                .saturating_sub(earlier.tracker_state_updates),
            tracker_state_noop_writes_skipped: self
                .tracker_state_noop_writes_skipped
                .saturating_sub(earlier.tracker_state_noop_writes_skipped),
            pubkey_cache_hits: self
                .pubkey_cache_hits
                .saturating_sub(earlier.pubkey_cache_hits),
            pubkey_pending_hits: self
                .pubkey_pending_hits
                .saturating_sub(earlier.pubkey_pending_hits),
            pubkey_sql_misses: self
                .pubkey_sql_misses
                .saturating_sub(earlier.pubkey_sql_misses),
            block_operation_elapsed: self
                .block_operation_elapsed
                .saturating_sub(earlier.block_operation_elapsed),
            source_validation_and_digest_elapsed: self
                .source_validation_and_digest_elapsed
                .saturating_sub(earlier.source_validation_and_digest_elapsed),
            sqlite_transaction_setup_elapsed: self
                .sqlite_transaction_setup_elapsed
                .saturating_sub(earlier.sqlite_transaction_setup_elapsed),
            token_tracking_elapsed: self
                .token_tracking_elapsed
                .saturating_sub(earlier.token_tracking_elapsed),
            tracked_row_write_elapsed: self
                .tracked_row_write_elapsed
                .saturating_sub(earlier.tracked_row_write_elapsed),
            block_header_write_elapsed: self
                .block_header_write_elapsed
                .saturating_sub(earlier.block_header_write_elapsed),
            durable_digest_and_checkpoint_elapsed: self
                .durable_digest_and_checkpoint_elapsed
                .saturating_sub(earlier.durable_digest_and_checkpoint_elapsed),
            sqlite_commit_elapsed: self
                .sqlite_commit_elapsed
                .saturating_sub(earlier.sqlite_commit_elapsed),
            error_recovery_elapsed: self
                .error_recovery_elapsed
                .saturating_sub(earlier.error_recovery_elapsed),
            wal_checkpoint_calls: self
                .wal_checkpoint_calls
                .saturating_sub(earlier.wal_checkpoint_calls),
            wal_checkpoint_successes: self
                .wal_checkpoint_successes
                .saturating_sub(earlier.wal_checkpoint_successes),
            wal_checkpoint_elapsed: self
                .wal_checkpoint_elapsed
                .saturating_sub(earlier.wal_checkpoint_elapsed),
        }
    }

    fn add_assign(&mut self, other: Self) {
        self.block_operations = self.block_operations.saturating_add(other.block_operations);
        self.committed_blocks = self.committed_blocks.saturating_add(other.committed_blocks);
        self.validated_replay_blocks = self
            .validated_replay_blocks
            .saturating_add(other.validated_replay_blocks);
        self.visited_transactions = self
            .visited_transactions
            .saturating_add(other.visited_transactions);
        self.tracker_state_updates = self
            .tracker_state_updates
            .saturating_add(other.tracker_state_updates);
        self.tracker_state_noop_writes_skipped = self
            .tracker_state_noop_writes_skipped
            .saturating_add(other.tracker_state_noop_writes_skipped);
        self.pubkey_cache_hits = self
            .pubkey_cache_hits
            .saturating_add(other.pubkey_cache_hits);
        self.pubkey_pending_hits = self
            .pubkey_pending_hits
            .saturating_add(other.pubkey_pending_hits);
        self.pubkey_sql_misses = self
            .pubkey_sql_misses
            .saturating_add(other.pubkey_sql_misses);
        self.block_operation_elapsed += other.block_operation_elapsed;
        self.source_validation_and_digest_elapsed += other.source_validation_and_digest_elapsed;
        self.sqlite_transaction_setup_elapsed += other.sqlite_transaction_setup_elapsed;
        self.token_tracking_elapsed += other.token_tracking_elapsed;
        self.tracked_row_write_elapsed += other.tracked_row_write_elapsed;
        self.block_header_write_elapsed += other.block_header_write_elapsed;
        self.durable_digest_and_checkpoint_elapsed += other.durable_digest_and_checkpoint_elapsed;
        self.sqlite_commit_elapsed += other.sqlite_commit_elapsed;
        self.error_recovery_elapsed += other.error_recovery_elapsed;
        self.wal_checkpoint_calls = self
            .wal_checkpoint_calls
            .saturating_add(other.wal_checkpoint_calls);
        self.wal_checkpoint_successes = self
            .wal_checkpoint_successes
            .saturating_add(other.wal_checkpoint_successes);
        self.wal_checkpoint_elapsed += other.wal_checkpoint_elapsed;
    }
}

#[derive(Debug, Error)]
pub enum TokenEventDatabaseError {
    #[error("token event database already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("token event database does not exist: {0}")]
    NotFound(PathBuf),
    #[error("cannot create token event database file {path}: {source}")]
    CreateFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("unsafe token event database path {path}: {reason}")]
    UnsafePath { path: PathBuf, reason: String },
    #[error("not a Blockzilla token event database (application_id={0})")]
    WrongApplication(i64),
    #[error("unsupported token event database schema version {0}")]
    UnsupportedSchema(i64),
    #[error("token event run specification differs from the database: {0}")]
    SpecificationMismatch(String),
    #[error("invalid token event run specification: {0}")]
    InvalidSpecification(String),
    #[error("invalid token event checkpoint: {0}")]
    InvalidCheckpoint(String),
    #[error("invalid token event block: {0}")]
    InvalidBlock(String),
    #[error("token event database writer is poisoned: {0}")]
    Poisoned(String),
    #[error(transparent)]
    Tracker(#[from] TargetMintTrackerError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub type Result<T> = std::result::Result<T, TokenEventDatabaseError>;

/// One deterministic SQLite writer for a bound token event run.
pub struct TokenEventDatabase {
    connection: Connection,
    pubkey_ids: HashMap<PubkeyBytes, i64>,
    spec: TokenEventRunSpec,
    tracker: TargetMintTracker,
    next_block_ordinal: u32,
    tracker_history: HistoryCoverage,
    tracker_revision: u64,
    digest_head: [u8; DIGEST_BYTES],
    tracker_digest: [u8; DIGEST_BYTES],
    poisoned: Option<String>,
    metrics: Cell<TokenEventDatabaseMetrics>,
}

/// One SQLite transaction with a rollback-safe public-key ID write buffer.
///
/// IDs loaded from committed rows are shared read-only. IDs allocated by this
/// transaction stay in `pending` until SQLite accepts the commit. Thus, a
/// failed block cannot leave an in-memory ID for a row that was rolled back.
struct PubkeyCachingTransaction<'connection, 'cache> {
    transaction: Transaction<'connection>,
    committed: &'cache HashMap<PubkeyBytes, i64>,
    pending: RefCell<HashMap<PubkeyBytes, i64>>,
    cache_hits: Cell<u64>,
    pending_hits: Cell<u64>,
    sql_misses: Cell<u64>,
}

struct PubkeyTransactionCommit {
    pending: HashMap<PubkeyBytes, i64>,
    cache_hits: u64,
    pending_hits: u64,
    sql_misses: u64,
}

impl<'connection, 'cache> PubkeyCachingTransaction<'connection, 'cache> {
    fn new(
        transaction: Transaction<'connection>,
        committed: &'cache HashMap<PubkeyBytes, i64>,
    ) -> Self {
        Self {
            transaction,
            committed,
            pending: RefCell::new(HashMap::new()),
            cache_hits: Cell::new(0),
            pending_hits: Cell::new(0),
            sql_misses: Cell::new(0),
        }
    }

    fn commit(self) -> Result<PubkeyTransactionCommit> {
        let Self {
            transaction,
            pending,
            cache_hits,
            pending_hits,
            sql_misses,
            ..
        } = self;
        transaction.commit()?;
        Ok(PubkeyTransactionCommit {
            pending: pending.into_inner(),
            cache_hits: cache_hits.get(),
            pending_hits: pending_hits.get(),
            sql_misses: sql_misses.get(),
        })
    }
}

impl<'connection> Deref for PubkeyCachingTransaction<'connection, '_> {
    type Target = Transaction<'connection>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

#[derive(Debug)]
struct AnchoredDatabasePath {
    path: PathBuf,
    parent_owner: u32,
    parent_device: u64,
    parent_inode: u64,
}

#[cfg(unix)]
unsafe extern "C" {
    fn geteuid() -> u32;
}

#[cfg(unix)]
fn effective_user_id() -> u32 {
    // SAFETY: `geteuid` has no parameters and has no memory-safety contract.
    unsafe { geteuid() }
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(unix)]
fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(value) => normalized.push(value),
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            std::path::Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

#[cfg(unix)]
fn anchor_database_path(path: &Path) -> Result<AnchoredDatabasePath> {
    let file_name = path
        .file_name()
        .ok_or_else(|| TokenEventDatabaseError::UnsafePath {
            path: path.to_path_buf(),
            reason: "the database path has no file name".into(),
        })?;
    let supplied_parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let supplied_parent = if supplied_parent.is_absolute() {
        supplied_parent.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| TokenEventDatabaseError::UnsafePath {
                path: path.to_path_buf(),
                reason: format!("cannot anchor the current directory: {error}"),
            })?
            .join(supplied_parent)
    };
    let supplied_parent = normalize_absolute_path(&supplied_parent).ok_or_else(|| {
        TokenEventDatabaseError::UnsafePath {
            path: supplied_parent.clone(),
            reason: "the database parent cannot be normalized as an absolute path".into(),
        }
    })?;
    let supplied_metadata = fs::symlink_metadata(&supplied_parent).map_err(|error| {
        TokenEventDatabaseError::UnsafePath {
            path: supplied_parent.clone(),
            reason: format!("the database parent is not accessible: {error}"),
        }
    })?;
    if supplied_metadata.file_type().is_symlink() || !supplied_metadata.is_dir() {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: supplied_parent,
            reason: "the database parent must be an existing non-symlink directory".into(),
        });
    }

    let parent = fs::canonicalize(&supplied_parent).map_err(|error| {
        TokenEventDatabaseError::UnsafePath {
            path: supplied_parent.clone(),
            reason: format!("cannot anchor the database parent: {error}"),
        }
    })?;
    if parent != supplied_parent {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: supplied_parent,
            reason: "the database path contains a symbolic-link component".into(),
        });
    }
    let parent_metadata =
        fs::symlink_metadata(&parent).map_err(|error| TokenEventDatabaseError::UnsafePath {
            path: parent.clone(),
            reason: format!("cannot inspect the anchored database parent: {error}"),
        })?;
    if !same_file_identity(&supplied_metadata, &parent_metadata) {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: supplied_parent,
            reason: "the database parent changed while it was anchored".into(),
        });
    }
    if parent_metadata.uid() != effective_user_id() {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: parent,
            reason: "the database parent is not owned by the current user".into(),
        });
    }
    if parent_metadata.mode() & 0o077 != 0 {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: parent,
            reason: "the database parent must have owner-only permissions".into(),
        });
    }
    Ok(AnchoredDatabasePath {
        path: parent.join(file_name),
        parent_owner: parent_metadata.uid(),
        parent_device: parent_metadata.dev(),
        parent_inode: parent_metadata.ino(),
    })
}

#[cfg(not(unix))]
fn anchor_database_path(path: &Path) -> Result<AnchoredDatabasePath> {
    Err(TokenEventDatabaseError::UnsafePath {
        path: path.to_path_buf(),
        reason: "private SQLite path validation is available only on Unix".into(),
    })
}

#[cfg(unix)]
fn verify_parent_anchor(anchor: &AnchoredDatabasePath) -> Result<()> {
    let parent = anchor
        .path
        .parent()
        .ok_or_else(|| TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: "the anchored database path has no parent".into(),
        })?;
    let metadata =
        fs::symlink_metadata(parent).map_err(|error| TokenEventDatabaseError::UnsafePath {
            path: parent.to_path_buf(),
            reason: format!("cannot recheck the database parent: {error}"),
        })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != anchor.parent_owner
        || metadata.dev() != anchor.parent_device
        || metadata.ino() != anchor.parent_inode
        || metadata.mode() & 0o077 != 0
    {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: parent.to_path_buf(),
            reason: "the private database parent changed after it was anchored".into(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_parent_anchor(_anchor: &AnchoredDatabasePath) -> Result<()> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn validate_sidecars(anchor: &AnchoredDatabasePath, creating: bool) -> Result<()> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let sidecar = sidecar_path(&anchor.path, suffix);
        let metadata = match fs::symlink_metadata(&sidecar) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(TokenEventDatabaseError::UnsafePath {
                    path: sidecar,
                    reason: format!("cannot inspect an SQLite sidecar: {error}"),
                });
            }
        };
        if creating {
            return Err(TokenEventDatabaseError::UnsafePath {
                path: sidecar,
                reason: "an SQLite sidecar exists for a new database".into(),
            });
        }
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != anchor.parent_owner
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
        {
            return Err(TokenEventDatabaseError::UnsafePath {
                path: sidecar,
                reason: "an SQLite sidecar is not a private regular file".into(),
            });
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_sidecars(_anchor: &AnchoredDatabasePath, _creating: bool) -> Result<()> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

#[cfg(unix)]
fn validate_existing_database_file(anchor: &AnchoredDatabasePath) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(&anchor.path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            TokenEventDatabaseError::NotFound(anchor.path.clone())
        } else {
            TokenEventDatabaseError::UnsafePath {
                path: anchor.path.clone(),
                reason: format!("cannot inspect the database file: {error}"),
            }
        }
    })?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != anchor.parent_owner
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
    {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: "the database must be a private regular file".into(),
        });
    }
    Ok(metadata)
}

#[cfg(not(unix))]
fn validate_existing_database_file(_anchor: &AnchoredDatabasePath) -> Result<fs::Metadata> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

#[cfg(unix)]
fn create_database_anchor(anchor: &AnchoredDatabasePath) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&anchor.path)
        .map_err(|source| {
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                match fs::symlink_metadata(&anchor.path) {
                    Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                        TokenEventDatabaseError::AlreadyExists(anchor.path.clone())
                    }
                    _ => TokenEventDatabaseError::UnsafePath {
                        path: anchor.path.clone(),
                        reason: "the new database path collided with a non-regular entry".into(),
                    },
                }
            } else {
                TokenEventDatabaseError::CreateFile {
                    path: anchor.path.clone(),
                    source,
                }
            }
        })
}

#[cfg(unix)]
fn require_absent_database_target(anchor: &AnchoredDatabasePath) -> Result<()> {
    match fs::symlink_metadata(&anchor.path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: format!("cannot inspect the new database path: {error}"),
        }),
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            Err(TokenEventDatabaseError::AlreadyExists(anchor.path.clone()))
        }
        Ok(_) => Err(TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: "the new database path is an existing non-regular entry".into(),
        }),
    }
}

#[cfg(not(unix))]
fn require_absent_database_target(_anchor: &AnchoredDatabasePath) -> Result<()> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

#[cfg(not(unix))]
fn create_database_anchor(_anchor: &AnchoredDatabasePath) -> Result<File> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

#[cfg(unix)]
fn verify_created_identity(anchor: &AnchoredDatabasePath, file: &File) -> Result<()> {
    let held = file
        .metadata()
        .map_err(|error| TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: format!("cannot inspect the held database file: {error}"),
        })?;
    let named = validate_existing_database_file(anchor)?;
    if !same_file_identity(&held, &named) {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: "the new database file changed before SQLite opened it".into(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_created_identity(_anchor: &AnchoredDatabasePath, _file: &File) -> Result<()> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

#[cfg(unix)]
fn verify_opened_identity(anchor: &AnchoredDatabasePath, before: &fs::Metadata) -> Result<()> {
    let after = validate_existing_database_file(anchor)?;
    if !same_file_identity(before, &after) {
        return Err(TokenEventDatabaseError::UnsafePath {
            path: anchor.path.clone(),
            reason: "the database file changed while SQLite opened it".into(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_opened_identity(_anchor: &AnchoredDatabasePath, _before: &fs::Metadata) -> Result<()> {
    unreachable!("non-Unix database paths are rejected while anchoring")
}

fn database_open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn database_read_only_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn audit_read_only_path(
    path: &Path,
    expected: Option<&TokenEventRunSpec>,
) -> Result<TokenEventAudit> {
    let anchor = anchor_database_path(path)?;
    verify_parent_anchor(&anchor)?;
    let before = validate_existing_database_file(&anchor)?;
    validate_sidecars(&anchor, false)?;
    let connection = Connection::open_with_flags(&anchor.path, database_read_only_flags())?;
    verify_opened_identity(&anchor, &before)?;
    verify_parent_anchor(&anchor)?;
    configure_safety(&connection)?;
    connection.pragma_update(None, "query_only", "ON")?;
    let query_only: i64 = connection.pragma_query_value(None, "query_only", |row| row.get(0))?;
    if query_only != 1 {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "SQLite query_only is {query_only}, expected ON (1)"
        )));
    }
    verify_writer_configuration(&connection)?;
    audit_connection(&connection, expected)
}

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"blockzilla-token-event-digest\0");
        digest.update((domain.len() as u64).to_le_bytes());
        digest.update(domain);
        Self(digest)
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_le_bytes());
    }

    fn encoded(&mut self, value: &[u8]) {
        self.0.update(value);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.update(value);
    }

    fn optional_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u32(value);
            }
            None => self.u8(0),
        }
    }

    fn finish(self) -> [u8; DIGEST_BYTES] {
        self.0.finalize().into()
    }
}

fn usize_as_u64(value: usize, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        TokenEventDatabaseError::InvalidBlock(format!("{field} exceeds the canonical u64 limit"))
    })
}

fn hash_coverage_reason(digest: &mut CanonicalHasher, reason: CoverageReason) {
    digest.bytes(coverage_reason_text(reason).as_bytes());
}

fn hash_execution_status(digest: &mut CanonicalHasher, status: ExecutionStatus) {
    match status {
        ExecutionStatus::Succeeded => digest.u8(0),
        ExecutionStatus::Failed => digest.u8(1),
        ExecutionStatus::Unknown(reason) => {
            digest.u8(2);
            hash_coverage_reason(digest, reason);
        }
    }
}

fn hash_instruction_coverage(digest: &mut CanonicalHasher, coverage: InstructionCoverage) {
    match coverage {
        InstructionCoverage::Complete => digest.u8(0),
        InstructionCoverage::Unknown(reason) => {
            digest.u8(1);
            hash_coverage_reason(digest, reason);
        }
    }
}

fn hash_cpi_coverage(digest: &mut CanonicalHasher, coverage: CpiCoverage) {
    match coverage {
        CpiCoverage::Complete => digest.u8(0),
        CpiCoverage::NotRecorded => digest.u8(1),
        CpiCoverage::Unknown(reason) => {
            digest.u8(2);
            hash_coverage_reason(digest, reason);
        }
    }
}

fn hash_data_coverage(digest: &mut CanonicalHasher, coverage: InstructionDataCoverage) {
    match coverage {
        InstructionDataCoverage::Exact => digest.u8(0),
        InstructionDataCoverage::NotRequested => digest.u8(1),
        InstructionDataCoverage::Unknown(reason) => {
            digest.u8(2);
            hash_coverage_reason(digest, reason);
        }
    }
}

fn source_block_digest(block: BlockView<'_>) -> Result<[u8; DIGEST_BYTES]> {
    let mut digest = CanonicalHasher::new(b"source-block-v1");
    digest.u64(block.header.epoch);
    digest.u32(block.header.block_ordinal);
    digest.u64(block.header.slot);
    digest.u64(usize_as_u64(block.transactions.len(), "transaction count")?);
    for transaction in block.transaction_views() {
        digest.u32(transaction.header.tx_index);
        hash_execution_status(&mut digest, transaction.header.status);
        digest.optional_u32(transaction.header.failed_outer_instruction_index);
        hash_instruction_coverage(&mut digest, transaction.header.instruction_coverage);
        hash_cpi_coverage(&mut digest, transaction.header.cpi_coverage);
        match transaction.primary_signature {
            Some(signature) => {
                digest.u8(1);
                digest.bytes(signature);
            }
            None => digest.u8(0),
        }
        digest.u64(usize_as_u64(
            transaction.required_signers.len(),
            "required signer count",
        )?);
        for signer in transaction.required_signers {
            digest.bytes(signer);
        }
        digest.u64(usize_as_u64(
            transaction.instructions.len(),
            "instruction count",
        )?);
        for instruction in transaction.instructions {
            digest.u32(instruction.coordinate.order);
            digest.u32(instruction.coordinate.outer_index);
            digest.optional_u32(instruction.coordinate.inner_index);
            digest.optional_u32(instruction.coordinate.stack_height);
            digest.bytes(&instruction.program_id.ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(
                    "token replay requires instruction program identities".into(),
                )
            })?);
            digest.u64(usize_as_u64(
                instruction.accounts.len(),
                "instruction account count",
            )?);
            for account in &instruction.accounts {
                digest.bytes(account);
            }
            hash_data_coverage(&mut digest, instruction.data_coverage);
            digest.bytes(&instruction.data);
        }
    }
    Ok(digest.finish())
}

fn hash_account_snapshot(
    digest: &mut CanonicalHasher,
    account: &PubkeyBytes,
    snapshot: &TargetAccountSnapshot,
) {
    digest.bytes(account);
    digest.u64(snapshot.lifecycle.generation);
    match snapshot.lifecycle.state {
        TokenAccountState::ActiveTarget => digest.u8(0),
        TokenAccountState::ActiveOther { mint } => {
            digest.u8(1);
            digest.bytes(&mint);
        }
        TokenAccountState::Closed { last_mint } => {
            digest.u8(2);
            match last_mint {
                Some(mint) => {
                    digest.u8(1);
                    digest.bytes(&mint);
                }
                None => digest.u8(0),
            }
        }
    }
    digest.u64(snapshot.confirmed_revision);
}

fn opening_tracker_digest(snapshot: &TargetMintTrackerSnapshot) -> Result<[u8; DIGEST_BYTES]> {
    let mut digest = CanonicalHasher::new(b"opening-tracker-v1");
    digest.bytes(&snapshot.target_mint());
    digest.bytes(history_text(snapshot.history_coverage()).as_bytes());
    digest.u64(snapshot.certainty_revision());
    digest.u64(usize_as_u64(
        snapshot.accounts().len(),
        "opening tracker account count",
    )?);
    for (account, state) in snapshot.accounts() {
        hash_account_snapshot(&mut digest, account, state);
    }
    Ok(digest.finish())
}

fn digest_sql_query(
    connection: &Connection,
    digest: &mut CanonicalHasher,
    domain: &[u8],
    sql: &str,
    block_ordinal: u32,
) -> Result<()> {
    digest.bytes(domain);
    let mut statement = connection.prepare_cached(sql)?;
    let column_count = statement.column_count();
    let mut rows = statement.query([i64::from(block_ordinal)])?;
    let mut row_count = 0u64;
    while let Some(row) = rows.next()? {
        digest.u8(0xff);
        for column in 0..column_count {
            hash_sql_value(digest, row.get_ref(column)?);
        }
        row_count = row_count.checked_add(1).ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint("durable row count exceeds u64".into())
        })?;
    }
    digest.u8(0xfe);
    digest.u64(row_count);
    Ok(())
}

fn hash_sql_value(digest: &mut CanonicalHasher, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => digest.u8(0),
        ValueRef::Integer(value) => {
            digest.u8(1);
            digest.i64(value);
        }
        ValueRef::Real(value) => {
            digest.u8(2);
            digest.u64(value.to_bits());
        }
        ValueRef::Text(value) => {
            digest.u8(3);
            digest.bytes(value);
        }
        ValueRef::Blob(value) => {
            digest.u8(4);
            digest.bytes(value);
        }
    }
}

fn hash_sql_row(
    digest: &mut CanonicalHasher,
    row: &rusqlite::Row<'_>,
    first_column: usize,
    column_count: usize,
) -> Result<()> {
    digest.u8(0xff);
    for column in first_column..column_count {
        hash_sql_value(digest, row.get_ref(column)?);
    }
    Ok(())
}

fn encoded_sql_value_len(value: ValueRef<'_>) -> Result<usize> {
    let payload_len = match value {
        ValueRef::Null => 0,
        ValueRef::Integer(_) | ValueRef::Real(_) => std::mem::size_of::<u64>(),
        ValueRef::Text(value) | ValueRef::Blob(value) => std::mem::size_of::<u64>()
            .checked_add(value.len())
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidCheckpoint(
                    "an encoded digest value exceeds the platform size limit".into(),
                )
            })?,
    };
    1usize.checked_add(payload_len).ok_or_else(|| {
        TokenEventDatabaseError::InvalidCheckpoint(
            "an encoded digest value exceeds the platform size limit".into(),
        )
    })
}

fn encode_sql_value(output: &mut Vec<u8>, value: ValueRef<'_>) -> Result<()> {
    match value {
        ValueRef::Null => output.push(0),
        ValueRef::Integer(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
        }
        ValueRef::Real(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        ValueRef::Text(value) => {
            output.push(3);
            let length = u64::try_from(value.len()).map_err(|_| {
                TokenEventDatabaseError::InvalidCheckpoint(
                    "digest text length exceeds the canonical u64 limit".into(),
                )
            })?;
            output.extend_from_slice(&length.to_le_bytes());
            output.extend_from_slice(value);
        }
        ValueRef::Blob(value) => {
            output.push(4);
            let length = u64::try_from(value.len()).map_err(|_| {
                TokenEventDatabaseError::InvalidCheckpoint(
                    "digest blob length exceeds the canonical u64 limit".into(),
                )
            })?;
            output.extend_from_slice(&length.to_le_bytes());
            output.extend_from_slice(value);
        }
    }
    Ok(())
}

struct EncodedDigestRow {
    block_ordinal: u32,
    bytes: Vec<u8>,
}

struct DigestDomainCursor<'statement> {
    rows: rusqlite::Rows<'statement>,
    group_column: usize,
    first_digest_column: usize,
    column_count: usize,
    lookahead: Option<EncodedDigestRow>,
}

impl<'statement> DigestDomainCursor<'statement> {
    fn new(
        statement: &'statement mut rusqlite::Statement<'_>,
        group_column: usize,
        first_digest_column: usize,
    ) -> Result<Self> {
        let column_count = statement.column_count();
        let rows = statement.query([])?;
        let mut cursor = Self {
            rows,
            group_column,
            first_digest_column,
            column_count,
            lookahead: None,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn advance(&mut self) -> Result<()> {
        let Some(row) = self.rows.next()? else {
            self.lookahead = None;
            return Ok(());
        };
        let block_ordinal = u32_from_i64(
            row.get(self.group_column)?,
            "global digest-domain block ordinal",
        )?;
        let mut encoded_len = 1usize;
        for column in self.first_digest_column..self.column_count {
            encoded_len = encoded_len
                .checked_add(encoded_sql_value_len(row.get_ref(column)?)?)
                .ok_or_else(|| {
                    TokenEventDatabaseError::InvalidCheckpoint(
                        "an encoded digest row exceeds the platform size limit".into(),
                    )
                })?;
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(encoded_len).map_err(|error| {
            TokenEventDatabaseError::InvalidCheckpoint(format!(
                "cannot allocate the bounded digest-row buffer: {error}"
            ))
        })?;
        bytes.push(0xff);
        for column in self.first_digest_column..self.column_count {
            encode_sql_value(&mut bytes, row.get_ref(column)?)?;
        }
        self.lookahead = Some(EncodedDigestRow {
            block_ordinal,
            bytes,
        });
        Ok(())
    }

    fn feed_block(
        &mut self,
        digest: &mut CanonicalHasher,
        domain: &[u8],
        block_ordinal: u32,
    ) -> Result<()> {
        digest.bytes(domain);
        if self
            .lookahead
            .as_ref()
            .is_some_and(|row| row.block_ordinal < block_ordinal)
        {
            return Err(invalid_historical_row(
                "a digest-domain row precedes its ordered block",
            ));
        }
        let mut row_count = 0u64;
        while self
            .lookahead
            .as_ref()
            .is_some_and(|row| row.block_ordinal == block_ordinal)
        {
            let row = self
                .lookahead
                .take()
                .ok_or_else(|| invalid_historical_row("digest-domain lookahead disappeared"))?;
            digest.encoded(&row.bytes);
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| invalid_historical_row("digest-domain row count exceeds u64"))?;
            self.advance()?;
        }
        digest.u8(0xfe);
        digest.u64(row_count);
        Ok(())
    }

    fn ensure_exhausted(&self) -> Result<()> {
        if self.lookahead.is_some() {
            return Err(invalid_historical_row(
                "a digest-domain row has no ordered block",
            ));
        }
        Ok(())
    }
}

fn durable_block_digest(connection: &Connection, block_ordinal: u32) -> Result<[u8; DIGEST_BYTES]> {
    let mut digest = CanonicalHasher::new(b"durable-block-rows-v1");
    for (domain, sql) in [
        (
            b"block".as_slice(),
            "SELECT block_ordinal, epoch_le, epoch_text, slot_le, slot_text,
                    transaction_count, tracker_history_after,
                    tracker_revision_after_le, tracker_revision_after_text,
                    tracker_digest_after
               FROM blocks WHERE block_ordinal = ?1",
        ),
        (
            b"transactions".as_slice(),
            "SELECT block_ordinal, tx_index, execution_status, status_reason,
                    failed_outer_index, primary_signature, tracker_history_after,
                    tracker_revision_after_le, tracker_revision_after_text
               FROM transactions WHERE block_ordinal = ?1 ORDER BY tx_index",
        ),
        (
            b"tracker-updates".as_slice(),
            "SELECT u.block_ordinal, u.tx_index, u.update_index, u.pubkey_id,
                    p.address, u.generation_le, u.generation_text, u.account_state,
                    u.state_mint_pubkey_id, m.address, u.confirmed_revision_le,
                    u.confirmed_revision_text
               FROM tracker_account_updates AS u
               JOIN pubkeys AS p ON p.pubkey_id = u.pubkey_id
               LEFT JOIN pubkeys AS m ON m.pubkey_id = u.state_mint_pubkey_id
              WHERE u.block_ordinal = ?1
              ORDER BY u.tx_index, u.update_index",
        ),
        (
            b"events".as_slice(),
            "SELECT e.event_id, e.block_ordinal, e.tx_index, e.event_index,
                    e.instruction_order, e.outer_index, e.inner_index,
                    e.stack_height, e.batch_index, e.invocation_state,
                    e.commit_state, e.program_pubkey_id, p.address, e.raw_kind,
                    e.token_tag, e.data_coverage, e.data_coverage_reason,
                    e.raw_data, e.trailing_data, e.amount_le, e.amount_text,
                    e.decimals, e.required_signers, e.authority_type,
                    e.embedded_pubkey_a, a.address, e.embedded_pubkey_b,
                    b.address, e.optional_value_present, e.ui_amount
               FROM events AS e
               JOIN pubkeys AS p ON p.pubkey_id = e.program_pubkey_id
               LEFT JOIN pubkeys AS a ON a.pubkey_id = e.embedded_pubkey_a
               LEFT JOIN pubkeys AS b ON b.pubkey_id = e.embedded_pubkey_b
              WHERE e.block_ordinal = ?1
              ORDER BY e.tx_index, e.event_index",
        ),
        (
            b"event-accounts".as_slice(),
            "SELECT a.event_id, e.tx_index, e.event_index, a.binding_index,
                    a.account_index, a.pubkey_id, p.address, a.semantic_role
               FROM event_accounts AS a
               JOIN events AS e ON e.event_id = a.event_id
               JOIN pubkeys AS p ON p.pubkey_id = a.pubkey_id
              WHERE e.block_ordinal = ?1
              ORDER BY e.tx_index, e.event_index, a.binding_index",
        ),
        (
            b"effects".as_slice(),
            "SELECT f.event_id, e.tx_index, e.event_index, f.effect_index,
                    f.effect_kind, f.amount_le, f.amount_text, f.decimals, f.checked
               FROM event_effects AS f
               JOIN events AS e ON e.event_id = f.event_id
              WHERE e.block_ordinal = ?1
              ORDER BY e.tx_index, e.event_index, f.effect_index",
        ),
        (
            b"lifecycle".as_slice(),
            "SELECT l.event_id, e.tx_index, e.event_index, l.effect_index,
                    l.account_pubkey_id, p.address, l.before_generation_le,
                    l.before_generation_text, l.before_state,
                    l.before_state_mint_pubkey_id, bm.address,
                    l.after_generation_le, l.after_generation_text,
                    l.after_state, l.after_state_mint_pubkey_id, am.address, l.cause
               FROM lifecycle_effects AS l
               JOIN events AS e ON e.event_id = l.event_id
               JOIN pubkeys AS p ON p.pubkey_id = l.account_pubkey_id
               LEFT JOIN pubkeys AS bm ON bm.pubkey_id = l.before_state_mint_pubkey_id
               LEFT JOIN pubkeys AS am ON am.pubkey_id = l.after_state_mint_pubkey_id
              WHERE e.block_ordinal = ?1
              ORDER BY e.tx_index, e.event_index, l.effect_index",
        ),
        (
            b"delta-legs".as_slice(),
            "SELECT d.event_id, e.tx_index, e.event_index, d.effect_index,
                    d.leg_index, d.account_pubkey_id, p.address, d.generation_le,
                    d.generation_text, d.direction, d.transfer_role,
                    d.amount_le, d.amount_text
               FROM delta_legs AS d
               JOIN events AS e ON e.event_id = d.event_id
               JOIN pubkeys AS p ON p.pubkey_id = d.account_pubkey_id
              WHERE e.block_ordinal = ?1
              ORDER BY e.tx_index, e.event_index, d.effect_index, d.leg_index",
        ),
        (
            b"coverage".as_slice(),
            "SELECT c.issue_id, c.block_ordinal, c.tx_index, c.issue_index,
                    c.instruction_order, c.outer_index, c.inner_index,
                    c.stack_height, c.issue_kind, c.detail, c.data_coverage,
                    c.coverage_reason, c.first_pubkey_id, fp.address,
                    c.second_pubkey_id, sp.address, c.known_mint_pubkey_id,
                    kp.address, c.observed_mint_pubkey_id, op.address,
                    c.expected_index, c.actual_index
               FROM coverage_issues AS c
               LEFT JOIN pubkeys AS fp ON fp.pubkey_id = c.first_pubkey_id
               LEFT JOIN pubkeys AS sp ON sp.pubkey_id = c.second_pubkey_id
               LEFT JOIN pubkeys AS kp ON kp.pubkey_id = c.known_mint_pubkey_id
               LEFT JOIN pubkeys AS op ON op.pubkey_id = c.observed_mint_pubkey_id
              WHERE c.block_ordinal = ?1
              ORDER BY c.tx_index, c.issue_index",
        ),
    ] {
        digest_sql_query(connection, &mut digest, domain, sql, block_ordinal)?;
    }
    Ok(digest.finish())
}

fn tracker_after_digest(
    connection: &Connection,
    block_ordinal: u32,
    previous: &[u8; DIGEST_BYTES],
    history_after: HistoryCoverage,
    revision_after: u64,
) -> Result<[u8; DIGEST_BYTES]> {
    let mut digest = CanonicalHasher::new(b"tracker-after-v1");
    digest.bytes(previous);
    digest.u32(block_ordinal);
    digest.bytes(history_text(history_after).as_bytes());
    digest.u64(revision_after);
    for (domain, sql) in [
        (
            b"transaction-trackers".as_slice(),
            "SELECT tx_index, tracker_history_after,
                    tracker_revision_after_le, tracker_revision_after_text
               FROM transactions WHERE block_ordinal = ?1 ORDER BY tx_index",
        ),
        (
            b"tracker-updates".as_slice(),
            "SELECT u.tx_index, u.update_index, u.pubkey_id, p.address,
                    u.generation_le, u.generation_text, u.account_state,
                    u.state_mint_pubkey_id, m.address, u.confirmed_revision_le,
                    u.confirmed_revision_text
               FROM tracker_account_updates AS u
               JOIN pubkeys AS p ON p.pubkey_id = u.pubkey_id
               LEFT JOIN pubkeys AS m ON m.pubkey_id = u.state_mint_pubkey_id
              WHERE u.block_ordinal = ?1
              ORDER BY u.tx_index, u.update_index",
        ),
    ] {
        digest_sql_query(connection, &mut digest, domain, sql, block_ordinal)?;
    }
    Ok(digest.finish())
}

fn chained_block_digest(
    previous: &[u8; DIGEST_BYTES],
    source: &[u8; DIGEST_BYTES],
    durable: &[u8; DIGEST_BYTES],
) -> [u8; DIGEST_BYTES] {
    let mut digest = CanonicalHasher::new(b"block-chain-v1");
    digest.bytes(previous);
    digest.bytes(source);
    digest.bytes(durable);
    digest.finish()
}

fn digest_from_blob(value: Vec<u8>, field: &str) -> Result<[u8; DIGEST_BYTES]> {
    value.try_into().map_err(|value: Vec<u8>| {
        TokenEventDatabaseError::InvalidCheckpoint(format!(
            "{field} has {} bytes instead of {DIGEST_BYTES}",
            value.len()
        ))
    })
}

impl TokenEventDatabase {
    /// Create a new database and bind it to `spec`.
    pub fn create(path: impl AsRef<Path>, spec: TokenEventRunSpec) -> Result<Self> {
        spec.validate()?;
        let anchor = anchor_database_path(path.as_ref())?;
        verify_parent_anchor(&anchor)?;
        require_absent_database_target(&anchor)?;
        validate_sidecars(&anchor, true)?;
        let created_file = create_database_anchor(&anchor)?;
        verify_parent_anchor(&anchor)?;
        let mut connection = Connection::open_with_flags(&anchor.path, database_open_flags())?;
        verify_created_identity(&anchor, &created_file)?;
        verify_parent_anchor(&anchor)?;
        configure_safety(&connection)?;
        configure_writer(&connection)?;
        connection.pragma_update(None, "application_id", TOKEN_EVENT_APPLICATION_ID)?;

        let tracker_digest = {
            let committed_pubkey_ids = HashMap::new();
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let transaction = PubkeyCachingTransaction::new(transaction, &committed_pubkey_ids);
            transaction.execute_batch(SCHEMA)?;
            let tracker_digest = insert_run(&transaction, &spec)?;
            transaction.commit()?;
            tracker_digest
        };
        let pubkey_ids = load_pubkey_ids(&connection)?;

        let next_block_ordinal = spec.range.first_block;
        let tracker_history = spec.opening_tracker.history_coverage();
        let tracker_revision = spec.opening_tracker.certainty_revision();
        let tracker = TargetMintTracker::from_snapshot(spec.opening_tracker.clone());
        Ok(Self {
            connection,
            pubkey_ids,
            spec,
            tracker,
            next_block_ordinal,
            tracker_history,
            tracker_revision,
            digest_head: EMPTY_DIGEST_HEAD,
            tracker_digest,
            poisoned: None,
            metrics: Cell::new(TokenEventDatabaseMetrics::default()),
        })
    }

    /// Open an existing database and require its complete run binding to match.
    pub fn open(path: impl AsRef<Path>, spec: TokenEventRunSpec) -> Result<Self> {
        spec.validate()?;
        let anchor = anchor_database_path(path.as_ref())?;
        verify_parent_anchor(&anchor)?;
        let before = validate_existing_database_file(&anchor)?;
        validate_sidecars(&anchor, false)?;
        let connection = Connection::open_with_flags(&anchor.path, database_open_flags())?;
        verify_opened_identity(&anchor, &before)?;
        verify_parent_anchor(&anchor)?;
        configure_safety(&connection)?;
        configure_writer(&connection)?;
        let audit = audit_connection(&connection, Some(&spec))?;
        let pubkey_ids = load_pubkey_ids(&connection)?;
        let TokenEventAudit {
            spec,
            resume,
            digest_head,
            tracker_digest,
        } = audit;
        let tracker_history = resume.tracker.history_coverage();
        let tracker_revision = resume.tracker.certainty_revision();
        Ok(Self {
            connection,
            pubkey_ids,
            spec,
            tracker: TargetMintTracker::from_snapshot(resume.tracker),
            next_block_ordinal: resume.next_block_ordinal,
            tracker_history,
            tracker_revision,
            digest_head,
            tracker_digest,
            poisoned: None,
            metrics: Cell::new(TokenEventDatabaseMetrics::default()),
        })
    }

    /// Validate an existing database without opening a writer.
    ///
    /// The stored run specification is validated and returned. This call does
    /// not change the journal mode or checkpoint the WAL.
    pub fn audit_read_only(path: impl AsRef<Path>) -> Result<TokenEventAudit> {
        audit_read_only_path(path.as_ref(), None)
    }

    /// Validate an existing database and require an exact run specification.
    ///
    /// This call uses the same audit as [`Self::open`], but it opens SQLite in
    /// read-only and query-only modes and does not change the journal mode.
    pub fn audit_read_only_expected(
        path: impl AsRef<Path>,
        expected: &TokenEventRunSpec,
    ) -> Result<TokenEventAudit> {
        expected.validate()?;
        audit_read_only_path(path.as_ref(), Some(expected))
    }

    /// Create the database when absent, or open and validate it when present.
    pub fn create_or_open(path: impl AsRef<Path>, spec: TokenEventRunSpec) -> Result<Self> {
        let path = path.as_ref();
        match Self::create(path, spec.clone()) {
            Err(TokenEventDatabaseError::AlreadyExists(_)) => Self::open(path, spec),
            result => result,
        }
    }

    /// Return the validated durable checkpoint and full tracker state.
    pub fn resume_state(&self) -> Result<TokenEventResume> {
        self.ensure_not_poisoned()?;
        validate_database_integrity(&self.connection)?;
        validate_historical_rows(&self.connection, &self.spec)?;
        let resume = load_resume_state(&self.connection, &self.spec)?;
        let digest_head = load_digest_head(&self.connection)?;
        let tracker_digest = load_checkpoint_tracker_digest(&self.connection)?;
        if resume.next_block_ordinal != self.next_block_ordinal
            || resume.tracker != self.tracker.snapshot()
            || digest_head != self.digest_head
            || tracker_digest != self.tracker_digest
        {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(
                "the in-memory writer state differs from the durable checkpoint".into(),
            ));
        }
        Ok(resume)
    }

    /// Return the next block ordinal without cloning the tracker state.
    pub const fn next_block_ordinal(&self) -> u32 {
        self.next_block_ordinal
    }

    /// Borrow the current tracker without cloning its retained account map.
    pub const fn tracker(&self) -> &TargetMintTracker {
        &self.tracker
    }

    /// Borrow the immutable run binding used by this database.
    pub const fn run_spec(&self) -> &TokenEventRunSpec {
        &self.spec
    }

    /// Return a snapshot of the writer's in-process performance counters.
    ///
    /// A reopened writer starts with zero counters. The values describe work
    /// done by this process, not the complete durable history in the file.
    pub fn metrics(&self) -> TokenEventDatabaseMetrics {
        self.metrics.get()
    }

    /// Number of committed public-key rows held by the writer ID cache.
    pub fn committed_pubkey_cache_entries(&self) -> usize {
        self.pubkey_ids.len()
    }

    fn record_metrics(&self, delta: TokenEventDatabaseMetrics) {
        let mut total = self.metrics.get();
        total.add_assign(delta);
        self.metrics.set(total);
    }

    /// Track and commit one complete source block as one atomic state change.
    ///
    /// This method derives all events from `block`. It never accepts
    /// caller-made event, effect, or account-update rows.
    pub fn track_and_commit_block(&mut self, block: BlockView<'_>) -> Result<BlockCommitOutcome> {
        self.ensure_not_poisoned()?;
        let operation_started = Instant::now();
        let mut delta = TokenEventDatabaseMetrics {
            block_operations: 1,
            ..TokenEventDatabaseMetrics::default()
        };
        let replay = block.header.block_ordinal < self.next_block_ordinal;
        let result = match self.try_track_and_commit_block(block, &mut delta) {
            Ok(outcome) => Ok(outcome),
            Err(error) if replay && matches!(&error, TokenEventDatabaseError::InvalidBlock(_)) => {
                Err(error)
            }
            Err(error) if replay => {
                let reason = format!("replay validation failed ({error})");
                self.poisoned = Some(reason.clone());
                Err(TokenEventDatabaseError::Poisoned(reason))
            }
            Err(error) => {
                let recovery_started = Instant::now();
                let recovery = self.reload_after_error();
                delta.error_recovery_elapsed += recovery_started.elapsed();
                match recovery {
                    Ok(()) => Err(error),
                    Err(recovery) => {
                        let reason = format!(
                            "block operation failed ({error}); durable recovery failed ({recovery})"
                        );
                        self.poisoned = Some(reason.clone());
                        Err(TokenEventDatabaseError::Poisoned(reason))
                    }
                }
            }
        };
        match &result {
            Ok(BlockCommitOutcome::Committed) => delta.committed_blocks = 1,
            Ok(BlockCommitOutcome::AlreadyCommitted) => delta.validated_replay_blocks = 1,
            Err(_) => {}
        }
        delta.block_operation_elapsed = operation_started.elapsed();
        self.record_metrics(delta);
        result
    }

    fn try_track_and_commit_block(
        &mut self,
        block: BlockView<'_>,
        timing: &mut TokenEventDatabaseMetrics,
    ) -> Result<BlockCommitOutcome> {
        let source_started = Instant::now();
        let source_digest = (|| {
            validate_source_block(&self.spec, block)?;
            source_block_digest(block)
        })();
        timing.source_validation_and_digest_elapsed += source_started.elapsed();
        let source_digest = source_digest?;
        let Self {
            connection,
            pubkey_ids,
            spec,
            tracker,
            next_block_ordinal,
            tracker_history,
            tracker_revision,
            digest_head,
            tracker_digest,
            ..
        } = self;
        let next = *next_block_ordinal;
        let setup_started = Instant::now();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate);
        let transaction = match transaction {
            Ok(transaction) => PubkeyCachingTransaction::new(transaction, pubkey_ids),
            Err(error) => {
                timing.sqlite_transaction_setup_elapsed += setup_started.elapsed();
                return Err(error.into());
            }
        };
        let checkpoint_validation = validate_cached_checkpoint(
            &transaction,
            next,
            *tracker_history,
            *tracker_revision,
            digest_head,
            tracker_digest,
        );
        timing.sqlite_transaction_setup_elapsed += setup_started.elapsed();
        checkpoint_validation?;

        if block.header.block_ordinal < next {
            let replay_started = Instant::now();
            let replay_validation = validate_already_committed(
                &transaction,
                spec,
                block,
                &source_digest,
                next,
                digest_head,
                tracker_digest,
            );
            timing.durable_digest_and_checkpoint_elapsed += replay_started.elapsed();
            replay_validation?;
            let commit_started = Instant::now();
            let commit = transaction.commit();
            timing.sqlite_commit_elapsed += commit_started.elapsed();
            let pubkey_commit = commit?;
            timing.pubkey_cache_hits = timing
                .pubkey_cache_hits
                .saturating_add(pubkey_commit.cache_hits);
            timing.pubkey_pending_hits = timing
                .pubkey_pending_hits
                .saturating_add(pubkey_commit.pending_hits);
            timing.pubkey_sql_misses = timing
                .pubkey_sql_misses
                .saturating_add(pubkey_commit.sql_misses);
            pubkey_ids.extend(pubkey_commit.pending);
            return Ok(BlockCommitOutcome::AlreadyCommitted);
        }
        if block.header.block_ordinal != next {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "block ordinal {} is not the next checkpoint {next}",
                block.header.block_ordinal
            )));
        }
        if next >= spec.end_block_exclusive()? {
            return Err(TokenEventDatabaseError::InvalidBlock(
                "the bound scan range is already complete".into(),
            ));
        }
        let block_header_started = Instant::now();
        let block_header_write = (|| {
            validate_previous_slot(&transaction, spec, next, block.header.slot)?;
            insert_block_header(
                &transaction,
                block,
                *tracker_history,
                *tracker_revision,
                tracker_digest,
                &source_digest,
            )
        })();
        timing.block_header_write_elapsed += block_header_started.elapsed();
        block_header_write?;
        for source in block.transaction_views() {
            timing.visited_transactions = timing.visited_transactions.saturating_add(1);
            let tracking_started = Instant::now();
            let tracked = tracker.process_transaction(source);
            timing.token_tracking_elapsed += tracking_started.elapsed();
            let tracked = tracked?;

            let row_write_started = Instant::now();
            let row_write: Result<(TrackerStateAfter, bool)> = (|| {
                validate_tracked_transaction(source, &tracked)?;
                // The block transaction validated this cached state before it
                // processed any source transaction. Each successful loop
                // iteration advances both the row and these two scalars.
                let tracker_before = TrackerStateAfter {
                    history: *tracker_history,
                    certainty_revision: *tracker_revision,
                };
                let tracker_after = validate_tracker_transition(
                    &transaction,
                    spec.target_mint,
                    tracker_before,
                    std::slice::from_ref(&tracked),
                )?;
                let tracker_state_changed = tracker_after != tracker_before;
                insert_transaction(&transaction, source, &tracked)?;
                apply_current_checkpoint(
                    &transaction,
                    std::slice::from_ref(&tracked),
                    tracker_before,
                    tracker_after,
                )?;
                Ok((tracker_after, tracker_state_changed))
            })();
            timing.tracked_row_write_elapsed += row_write_started.elapsed();
            let (tracker_after, tracker_state_changed) = row_write?;
            if tracker_state_changed {
                timing.tracker_state_updates = timing.tracker_state_updates.saturating_add(1);
            } else {
                timing.tracker_state_noop_writes_skipped =
                    timing.tracker_state_noop_writes_skipped.saturating_add(1);
            }
            *tracker_history = tracker_after.history;
            *tracker_revision = tracker_after.certainty_revision;
        }
        let finalization_started = Instant::now();
        let finalization = (|| {
            let tracker_digest_after = tracker_after_digest(
                &transaction,
                block.header.block_ordinal,
                tracker_digest,
                *tracker_history,
                *tracker_revision,
            )?;
            finalize_block_header(
                &transaction,
                block.header.block_ordinal,
                *tracker_history,
                *tracker_revision,
                &tracker_digest_after,
            )?;
            let durable_digest = durable_block_digest(&transaction, block.header.block_ordinal)?;
            let chain_digest = chained_block_digest(digest_head, &source_digest, &durable_digest);
            let finalized = execute_cached(
                &transaction,
                "UPDATE blocks SET durable_rows_digest = ?1, chain_digest = ?2
                  WHERE block_ordinal = ?3",
                params![
                    durable_digest.as_slice(),
                    chain_digest.as_slice(),
                    i64::from(block.header.block_ordinal),
                ],
            )?;
            if finalized != 1 {
                return Err(TokenEventDatabaseError::InvalidCheckpoint(
                    "the pending block disappeared during digest finalization".into(),
                ));
            }
            let next_after = next.checked_add(1).ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock("next block ordinal exceeds u32".into())
            })?;
            let advanced = execute_cached(
                &transaction,
                "UPDATE checkpoint
                    SET next_block_ordinal = ?1, digest_head = ?2, tracker_digest = ?3
                  WHERE singleton = 1 AND next_block_ordinal = ?4
                    AND digest_head = ?5 AND tracker_digest = ?6",
                params![
                    i64::from(next_after),
                    chain_digest.as_slice(),
                    tracker_digest_after.as_slice(),
                    i64::from(next),
                    digest_head.as_slice(),
                    tracker_digest.as_slice(),
                ],
            )?;
            if advanced != 1 {
                return Err(TokenEventDatabaseError::InvalidCheckpoint(
                    "the next-block checkpoint changed during the block commit".into(),
                ));
            }
            Ok((next_after, tracker_digest_after, chain_digest))
        })();
        timing.durable_digest_and_checkpoint_elapsed += finalization_started.elapsed();
        let (next_after, tracker_digest_after, chain_digest) = finalization?;
        let commit_started = Instant::now();
        let commit = transaction.commit();
        timing.sqlite_commit_elapsed += commit_started.elapsed();
        let pubkey_commit = commit?;
        timing.pubkey_cache_hits = timing
            .pubkey_cache_hits
            .saturating_add(pubkey_commit.cache_hits);
        timing.pubkey_pending_hits = timing
            .pubkey_pending_hits
            .saturating_add(pubkey_commit.pending_hits);
        timing.pubkey_sql_misses = timing
            .pubkey_sql_misses
            .saturating_add(pubkey_commit.sql_misses);
        pubkey_ids.extend(pubkey_commit.pending);
        *next_block_ordinal = next_after;
        *digest_head = chain_digest;
        *tracker_digest = tracker_digest_after;
        Ok(BlockCommitOutcome::Committed)
    }

    fn reload_after_error(&mut self) -> Result<()> {
        validate_database_integrity(&self.connection)?;
        validate_historical_rows(&self.connection, &self.spec)?;
        let resume = load_resume_state(&self.connection, &self.spec)?;
        let digest_head = load_digest_head(&self.connection)?;
        let tracker_digest = load_checkpoint_tracker_digest(&self.connection)?;
        let pubkey_ids = load_pubkey_ids(&self.connection)?;
        self.next_block_ordinal = resume.next_block_ordinal;
        self.tracker_history = resume.tracker.history_coverage();
        self.tracker_revision = resume.tracker.certainty_revision();
        self.tracker = TargetMintTracker::from_snapshot(resume.tracker);
        self.digest_head = digest_head;
        self.tracker_digest = tracker_digest;
        self.pubkey_ids = pubkey_ids;
        Ok(())
    }

    fn ensure_not_poisoned(&self) -> Result<()> {
        match &self.poisoned {
            Some(reason) => Err(TokenEventDatabaseError::Poisoned(reason.clone())),
            None => Ok(()),
        }
    }

    /// Flush all committed WAL pages into the main database file.
    pub fn checkpoint_wal(&self) -> Result<()> {
        let started = Instant::now();
        let result = (|| {
            self.ensure_not_poisoned()?;
            verify_writer_configuration(&self.connection)?;
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = self
                .connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
            if busy != 0 || log_frames != checkpointed_frames {
                return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                    "WAL checkpoint is incomplete (busy={busy}, log={log_frames}, checkpointed={checkpointed_frames})"
                )));
            }
            Ok(())
        })();
        let mut delta = TokenEventDatabaseMetrics {
            wal_checkpoint_calls: 1,
            wal_checkpoint_elapsed: started.elapsed(),
            ..TokenEventDatabaseMetrics::default()
        };
        if result.is_ok() {
            delta.wal_checkpoint_successes = 1;
        }
        self.record_metrics(delta);
        result
    }
}

impl blockzilla_model::BlockSink for TokenEventDatabase {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_model::Result<()> {
        self.track_and_commit_block(block)
            .map(|_| ())
            .map_err(blockzilla_model::Error::sink)
    }
}

fn validate_database_integrity(connection: &Connection) -> Result<()> {
    let quick_check: String =
        connection.pragma_query_value(None, "quick_check", |row| row.get(0))?;
    if quick_check != "ok" {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "SQLite quick_check failed: {quick_check}"
        )));
    }
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    if statement.exists([])? {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "SQLite foreign-key validation failed".into(),
        ));
    }
    Ok(())
}

fn load_resume_state(
    connection: &Connection,
    spec: &TokenEventRunSpec,
) -> Result<TokenEventResume> {
    let next_i64: i64 = connection.query_row(
        "SELECT next_block_ordinal FROM checkpoint WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let next_block_ordinal = u32_from_i64(next_i64, "next block ordinal")?;
    let end = spec.end_block_exclusive()?;
    if next_block_ordinal < spec.range.first_block || next_block_ordinal > end {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "next block ordinal {next_block_ordinal} is outside {}..={end}",
            spec.range.first_block
        )));
    }
    validate_committed_universe(connection, spec, next_block_ordinal)?;
    let tracker = load_current_snapshot(connection, spec.target_mint)?;
    Ok(TokenEventResume {
        next_block_ordinal,
        tracker,
    })
}

fn load_digest_head(connection: &Connection) -> Result<[u8; DIGEST_BYTES]> {
    let value: Vec<u8> = connection.query_row(
        "SELECT digest_head FROM checkpoint WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    digest_from_blob(value, "checkpoint digest head")
}

fn load_checkpoint_tracker_digest(connection: &Connection) -> Result<[u8; DIGEST_BYTES]> {
    let value: Vec<u8> = connection.query_row(
        "SELECT tracker_digest FROM checkpoint WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    digest_from_blob(value, "checkpoint tracker digest")
}

fn validate_cached_checkpoint(
    connection: &Connection,
    expected_next: u32,
    expected_history: HistoryCoverage,
    expected_revision: u64,
    expected_digest_head: &[u8; DIGEST_BYTES],
    expected_tracker_digest: &[u8; DIGEST_BYTES],
) -> Result<()> {
    let (stored_next, stored_digest, stored_tracker_digest): (i64, Vec<u8>, Vec<u8>) = {
        let mut statement = connection.prepare_cached(
            "SELECT next_block_ordinal, digest_head, tracker_digest
               FROM checkpoint WHERE singleton = 1",
        )?;
        statement.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
    };
    let stored_next = u32_from_i64(stored_next, "next block ordinal")?;
    let stored_digest = digest_from_blob(stored_digest, "checkpoint digest head")?;
    let stored_tracker_digest =
        digest_from_blob(stored_tracker_digest, "checkpoint tracker digest")?;
    let (stored_history, stored_revision) = load_tracker_state(connection)?;
    if stored_next != expected_next
        || stored_history != expected_history
        || stored_revision != expected_revision
        || &stored_digest != expected_digest_head
        || &stored_tracker_digest != expected_tracker_digest
    {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "the durable checkpoint changed outside this writer".into(),
        ));
    }
    Ok(())
}

fn validate_historical_rows(connection: &Connection, spec: &TokenEventRunSpec) -> Result<()> {
    validate_schema_topology(connection)?;
    validate_run_identity_bounds(connection)?;
    validate_durable_resource_bounds(connection)?;
    validate_all_u64_pairs(connection)?;
    validate_transaction_domains(connection)?;
    validate_event_domains(connection)?;
    validate_effect_domains(connection)?;
    validate_coverage_issue_domains(connection)?;
    validate_dense_historical_children(connection)?;
    validate_historical_tracker(connection, spec)?;
    validate_exact_lifetime_materialization(connection)?;
    validate_no_unreferenced_pubkeys(connection)?;
    validate_digest_chain(connection, spec)?;
    Ok(())
}

fn validate_digest_chain(connection: &Connection, spec: &TokenEventRunSpec) -> Result<()> {
    let mut block_statement = connection.prepare(
        "SELECT block_ordinal, epoch_le, epoch_text, slot_le, slot_text,
                transaction_count, tracker_history_after,
                tracker_revision_after_le, tracker_revision_after_text,
                tracker_digest_after, source_digest, durable_rows_digest,
                chain_digest
           FROM blocks ORDER BY block_ordinal",
    )?;
    let mut transaction_statement = connection.prepare(
        "SELECT block_ordinal, tx_index, execution_status, status_reason,
                failed_outer_index, primary_signature, tracker_history_after,
                tracker_revision_after_le, tracker_revision_after_text
           FROM transactions ORDER BY block_ordinal, tx_index",
    )?;
    let mut update_statement = connection.prepare(
        "SELECT u.block_ordinal, u.tx_index, u.update_index, u.pubkey_id,
                p.address, u.generation_le, u.generation_text, u.account_state,
                u.state_mint_pubkey_id, m.address, u.confirmed_revision_le,
                u.confirmed_revision_text
           FROM tracker_account_updates AS u
           JOIN pubkeys AS p ON p.pubkey_id = u.pubkey_id
           LEFT JOIN pubkeys AS m ON m.pubkey_id = u.state_mint_pubkey_id
          ORDER BY u.block_ordinal, u.tx_index, u.update_index",
    )?;
    let mut event_statement = connection.prepare(
        "SELECT e.event_id, e.block_ordinal, e.tx_index, e.event_index,
                e.instruction_order, e.outer_index, e.inner_index,
                e.stack_height, e.batch_index, e.invocation_state,
                e.commit_state, e.program_pubkey_id, p.address, e.raw_kind,
                e.token_tag, e.data_coverage, e.data_coverage_reason,
                e.raw_data, e.trailing_data, e.amount_le, e.amount_text,
                e.decimals, e.required_signers, e.authority_type,
                e.embedded_pubkey_a, a.address, e.embedded_pubkey_b,
                b.address, e.optional_value_present, e.ui_amount
           FROM events AS e
           JOIN pubkeys AS p ON p.pubkey_id = e.program_pubkey_id
           LEFT JOIN pubkeys AS a ON a.pubkey_id = e.embedded_pubkey_a
           LEFT JOIN pubkeys AS b ON b.pubkey_id = e.embedded_pubkey_b
          ORDER BY e.block_ordinal, e.tx_index, e.event_index",
    )?;
    let mut event_account_statement = connection.prepare(
        "SELECT e.block_ordinal, a.event_id, e.tx_index, e.event_index,
                a.binding_index, a.account_index, a.pubkey_id, p.address,
                a.semantic_role
           FROM event_accounts AS a
           JOIN events AS e ON e.event_id = a.event_id
           JOIN pubkeys AS p ON p.pubkey_id = a.pubkey_id
          ORDER BY e.block_ordinal, e.tx_index, e.event_index, a.binding_index",
    )?;
    let mut effect_statement = connection.prepare(
        "SELECT e.block_ordinal, f.event_id, e.tx_index, e.event_index,
                f.effect_index, f.effect_kind, f.amount_le, f.amount_text,
                f.decimals, f.checked
           FROM event_effects AS f
           JOIN events AS e ON e.event_id = f.event_id
          ORDER BY e.block_ordinal, e.tx_index, e.event_index, f.effect_index",
    )?;
    let mut lifecycle_statement = connection.prepare(
        "SELECT e.block_ordinal, l.event_id, e.tx_index, e.event_index,
                l.effect_index, l.account_pubkey_id, p.address,
                l.before_generation_le, l.before_generation_text,
                l.before_state, l.before_state_mint_pubkey_id, bm.address,
                l.after_generation_le, l.after_generation_text,
                l.after_state, l.after_state_mint_pubkey_id, am.address,
                l.cause
           FROM lifecycle_effects AS l
           JOIN events AS e ON e.event_id = l.event_id
           JOIN pubkeys AS p ON p.pubkey_id = l.account_pubkey_id
           LEFT JOIN pubkeys AS bm ON bm.pubkey_id = l.before_state_mint_pubkey_id
           LEFT JOIN pubkeys AS am ON am.pubkey_id = l.after_state_mint_pubkey_id
          ORDER BY e.block_ordinal, e.tx_index, e.event_index, l.effect_index",
    )?;
    let mut delta_statement = connection.prepare(
        "SELECT e.block_ordinal, d.event_id, e.tx_index, e.event_index,
                d.effect_index, d.leg_index, d.account_pubkey_id, p.address,
                d.generation_le, d.generation_text, d.direction,
                d.transfer_role, d.amount_le, d.amount_text
           FROM delta_legs AS d
           JOIN events AS e ON e.event_id = d.event_id
           JOIN pubkeys AS p ON p.pubkey_id = d.account_pubkey_id
          ORDER BY e.block_ordinal, e.tx_index, e.event_index,
                   d.effect_index, d.leg_index",
    )?;
    let mut coverage_statement = connection.prepare(
        "SELECT c.issue_id, c.block_ordinal, c.tx_index, c.issue_index,
                c.instruction_order, c.outer_index, c.inner_index,
                c.stack_height, c.issue_kind, c.detail, c.data_coverage,
                c.coverage_reason, c.first_pubkey_id, fp.address,
                c.second_pubkey_id, sp.address, c.known_mint_pubkey_id,
                kp.address, c.observed_mint_pubkey_id, op.address,
                c.expected_index, c.actual_index
           FROM coverage_issues AS c
           LEFT JOIN pubkeys AS fp ON fp.pubkey_id = c.first_pubkey_id
           LEFT JOIN pubkeys AS sp ON sp.pubkey_id = c.second_pubkey_id
           LEFT JOIN pubkeys AS kp ON kp.pubkey_id = c.known_mint_pubkey_id
           LEFT JOIN pubkeys AS op ON op.pubkey_id = c.observed_mint_pubkey_id
          ORDER BY c.block_ordinal, c.tx_index, c.issue_index",
    )?;
    let mut tracker_transaction_statement = connection.prepare(
        "SELECT block_ordinal, tx_index, tracker_history_after,
                tracker_revision_after_le, tracker_revision_after_text
           FROM transactions ORDER BY block_ordinal, tx_index",
    )?;
    let mut tracker_update_statement = connection.prepare(
        "SELECT u.block_ordinal, u.tx_index, u.update_index, u.pubkey_id,
                p.address, u.generation_le, u.generation_text, u.account_state,
                u.state_mint_pubkey_id, m.address, u.confirmed_revision_le,
                u.confirmed_revision_text
           FROM tracker_account_updates AS u
           JOIN pubkeys AS p ON p.pubkey_id = u.pubkey_id
           LEFT JOIN pubkeys AS m ON m.pubkey_id = u.state_mint_pubkey_id
          ORDER BY u.block_ordinal, u.tx_index, u.update_index",
    )?;

    let mut transactions = DigestDomainCursor::new(&mut transaction_statement, 0, 0)?;
    let mut updates = DigestDomainCursor::new(&mut update_statement, 0, 0)?;
    let mut events = DigestDomainCursor::new(&mut event_statement, 1, 0)?;
    let mut event_accounts = DigestDomainCursor::new(&mut event_account_statement, 0, 1)?;
    let mut effects = DigestDomainCursor::new(&mut effect_statement, 0, 1)?;
    let mut lifecycles = DigestDomainCursor::new(&mut lifecycle_statement, 0, 1)?;
    let mut deltas = DigestDomainCursor::new(&mut delta_statement, 0, 1)?;
    let mut coverage = DigestDomainCursor::new(&mut coverage_statement, 1, 0)?;
    let mut tracker_transactions =
        DigestDomainCursor::new(&mut tracker_transaction_statement, 0, 1)?;
    let mut tracker_updates = DigestDomainCursor::new(&mut tracker_update_statement, 0, 1)?;
    let mut rows = block_statement.query([])?;
    let mut previous = EMPTY_DIGEST_HEAD;
    let mut previous_tracker = opening_tracker_digest(&spec.opening_tracker)?;
    let mut expected_ordinal = spec.range.first_block;
    while let Some(row) = rows.next()? {
        let ordinal = u32_from_i64(row.get(0)?, "digest-chain block ordinal")?;
        if ordinal != expected_ordinal {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "digest-chain block {ordinal} is not the expected block {expected_ordinal}"
            )));
        }
        let history_after = parse_history(&row.get::<_, String>(6)?)?;
        let revision_after = parse_u64_pair(
            &row.get::<_, Vec<u8>>(7)?,
            &row.get::<_, String>(8)?,
            "block tracker revision",
        )?;
        let stored_tracker = digest_from_blob(row.get(9)?, "stored block tracker digest")?;
        let source = digest_from_blob(row.get(10)?, "stored source block digest")?;
        let stored_durable = digest_from_blob(row.get(11)?, "stored durable row digest")?;
        let stored_chain = digest_from_blob(row.get(12)?, "stored block chain digest")?;

        let mut tracker_digest = CanonicalHasher::new(b"tracker-after-v1");
        tracker_digest.bytes(&previous_tracker);
        tracker_digest.u32(ordinal);
        tracker_digest.bytes(history_text(history_after).as_bytes());
        tracker_digest.u64(revision_after);
        tracker_transactions.feed_block(&mut tracker_digest, b"transaction-trackers", ordinal)?;
        tracker_updates.feed_block(&mut tracker_digest, b"tracker-updates", ordinal)?;
        let tracker = tracker_digest.finish();
        if tracker != stored_tracker {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "tracker-after digest differs for block {ordinal}"
            )));
        }

        let mut durable_digest = CanonicalHasher::new(b"durable-block-rows-v1");
        durable_digest.bytes(b"block");
        hash_sql_row(&mut durable_digest, row, 0, 10)?;
        durable_digest.u8(0xfe);
        durable_digest.u64(1);
        transactions.feed_block(&mut durable_digest, b"transactions", ordinal)?;
        updates.feed_block(&mut durable_digest, b"tracker-updates", ordinal)?;
        events.feed_block(&mut durable_digest, b"events", ordinal)?;
        event_accounts.feed_block(&mut durable_digest, b"event-accounts", ordinal)?;
        effects.feed_block(&mut durable_digest, b"effects", ordinal)?;
        lifecycles.feed_block(&mut durable_digest, b"lifecycle", ordinal)?;
        deltas.feed_block(&mut durable_digest, b"delta-legs", ordinal)?;
        coverage.feed_block(&mut durable_digest, b"coverage", ordinal)?;
        let durable = durable_digest.finish();
        if durable != stored_durable {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "durable row digest differs for block {ordinal}"
            )));
        }
        let chain = chained_block_digest(&previous, &source, &durable);
        if chain != stored_chain {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "digest chain differs at block {ordinal}"
            )));
        }
        previous = chain;
        previous_tracker = tracker;
        expected_ordinal = expected_ordinal.checked_add(1).ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint(
                "digest-chain block ordinal exceeds u32".into(),
            )
        })?;
    }
    transactions.ensure_exhausted()?;
    updates.ensure_exhausted()?;
    events.ensure_exhausted()?;
    event_accounts.ensure_exhausted()?;
    effects.ensure_exhausted()?;
    lifecycles.ensure_exhausted()?;
    deltas.ensure_exhausted()?;
    coverage.ensure_exhausted()?;
    tracker_transactions.ensure_exhausted()?;
    tracker_updates.ensure_exhausted()?;
    if load_digest_head(connection)? != previous {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "checkpoint digest head differs from the final block digest".into(),
        ));
    }
    if load_checkpoint_tracker_digest(connection)? != previous_tracker {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "checkpoint tracker digest differs from the final tracker transition".into(),
        ));
    }
    Ok(())
}

fn validate_all_u64_pairs(connection: &Connection) -> Result<()> {
    const REQUIRED: &[(&str, &str, &str)] = &[
        ("run_identity", "source_epoch_le", "source_epoch_text"),
        (
            "run_identity",
            "source_first_slot_le",
            "source_first_slot_text",
        ),
        (
            "run_identity",
            "source_slots_per_epoch_le",
            "source_slots_per_epoch_text",
        ),
        (
            "opening_tracker_state",
            "certainty_revision_le",
            "certainty_revision_text",
        ),
        (
            "opening_tracker_accounts",
            "generation_le",
            "generation_text",
        ),
        (
            "opening_tracker_accounts",
            "confirmed_revision_le",
            "confirmed_revision_text",
        ),
        ("blocks", "epoch_le", "epoch_text"),
        ("blocks", "slot_le", "slot_text"),
        (
            "blocks",
            "tracker_revision_after_le",
            "tracker_revision_after_text",
        ),
        (
            "transactions",
            "tracker_revision_after_le",
            "tracker_revision_after_text",
        ),
        ("account_lifetimes", "generation_le", "generation_text"),
        (
            "account_lifetimes",
            "confirmed_revision_le",
            "confirmed_revision_text",
        ),
        (
            "tracker_state",
            "certainty_revision_le",
            "certainty_revision_text",
        ),
        (
            "tracker_account_updates",
            "generation_le",
            "generation_text",
        ),
        (
            "tracker_account_updates",
            "confirmed_revision_le",
            "confirmed_revision_text",
        ),
        (
            "lifecycle_effects",
            "after_generation_le",
            "after_generation_text",
        ),
        ("delta_legs", "generation_le", "generation_text"),
        ("delta_legs", "amount_le", "amount_text"),
    ];
    const OPTIONAL: &[(&str, &str, &str)] = &[
        ("events", "amount_le", "amount_text"),
        ("event_effects", "amount_le", "amount_text"),
        (
            "lifecycle_effects",
            "before_generation_le",
            "before_generation_text",
        ),
    ];

    for &(table, bytes_column, text_column) in REQUIRED {
        let sql = format!("SELECT {bytes_column}, {text_column} FROM {table}");
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (bytes, text) = row?;
            parse_u64_pair(&bytes, &text, &format!("{table}.{bytes_column}"))?;
        }
    }
    for &(table, bytes_column, text_column) in OPTIONAL {
        let sql = format!("SELECT {bytes_column}, {text_column} FROM {table}");
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })?;
        for row in rows {
            match row? {
                (Some(bytes), Some(text)) => {
                    parse_u64_pair(&bytes, &text, &format!("{table}.{bytes_column}"))?;
                }
                (None, None) => {}
                _ => {
                    return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                        "{table}.{bytes_column} has an incomplete BLOB/text pair"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn invalid_historical_row(detail: impl Into<String>) -> TokenEventDatabaseError {
    TokenEventDatabaseError::InvalidCheckpoint(detail.into())
}

fn is_coverage_reason(value: &str) -> bool {
    matches!(
        value,
        "metadata-absent"
            | "raw-transaction"
            | "raw-metadata"
            | "projection-not-requested"
            | "invalid-reference"
            | "ambiguous-instruction-data"
            | "instruction-data-unavailable"
            | "unsupported-instruction"
            | "source-unverified"
            | "non-contiguous-history"
            | "other"
    )
}

fn validate_transaction_domains(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT block_ordinal, tx_index, execution_status, status_reason,
                failed_outer_index
           FROM transactions",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
        ))
    })?;
    for row in rows {
        let (block, transaction, status, reason, failed_outer) = row?;
        let valid = match status.as_str() {
            "succeeded" => reason.is_none() && failed_outer.is_none(),
            "failed" => reason.is_none(),
            "unknown" => {
                failed_outer.is_none() && reason.as_deref().is_some_and(is_coverage_reason)
            }
            _ => false,
        };
        if !valid {
            return Err(invalid_historical_row(format!(
                "transaction {block}:{transaction} has an invalid status variant"
            )));
        }
    }
    Ok(())
}

struct StoredEventDomainRow {
    event_id: i64,
    execution_status: String,
    failed_outer_index: Option<i64>,
    outer_index: i64,
    inner_index: Option<i64>,
    batch_index: Option<i64>,
    invocation_state: String,
    commit_state: String,
    program: Vec<u8>,
    raw_kind: String,
    token_tag: Option<i64>,
    data_coverage: String,
    data_coverage_reason: Option<String>,
    raw_data: Option<Vec<u8>>,
    trailing_data: Option<Vec<u8>>,
    amount: Option<Vec<u8>>,
    decimals: Option<i64>,
    required_signers: Option<i64>,
    authority_type: Option<String>,
    embedded_a: Option<i64>,
    embedded_b: Option<i64>,
    optional_value_present: Option<i64>,
    ui_amount: Option<String>,
    effect_count: i64,
}

fn validate_event_domains(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT e.event_id, t.execution_status, t.failed_outer_index,
                e.outer_index, e.inner_index, e.batch_index,
                e.invocation_state, e.commit_state, program.address,
                e.raw_kind, e.token_tag, e.data_coverage,
                e.data_coverage_reason, e.raw_data, e.trailing_data,
                e.amount_le, e.decimals, e.required_signers, e.authority_type,
                e.embedded_pubkey_a, e.embedded_pubkey_b,
                e.optional_value_present, e.ui_amount,
                (SELECT count(*) FROM event_effects effect
                  WHERE effect.event_id = e.event_id)
           FROM events e
           JOIN transactions t
             ON t.block_ordinal = e.block_ordinal AND t.tx_index = e.tx_index
           JOIN pubkeys program ON program.pubkey_id = e.program_pubkey_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(StoredEventDomainRow {
            event_id: row.get(0)?,
            execution_status: row.get(1)?,
            failed_outer_index: row.get(2)?,
            outer_index: row.get(3)?,
            inner_index: row.get(4)?,
            batch_index: row.get(5)?,
            invocation_state: row.get(6)?,
            commit_state: row.get(7)?,
            program: row.get(8)?,
            raw_kind: row.get(9)?,
            token_tag: row.get(10)?,
            data_coverage: row.get(11)?,
            data_coverage_reason: row.get(12)?,
            raw_data: row.get(13)?,
            trailing_data: row.get(14)?,
            amount: row.get(15)?,
            decimals: row.get(16)?,
            required_signers: row.get(17)?,
            authority_type: row.get(18)?,
            embedded_a: row.get(19)?,
            embedded_b: row.get(20)?,
            optional_value_present: row.get(21)?,
            ui_amount: row.get(22)?,
            effect_count: row.get(23)?,
        })
    })?;
    for row in rows {
        let row = row?;
        if row.program.as_slice() != CLASSIC_SPL_TOKEN_PROGRAM_ID {
            return Err(invalid_historical_row(format!(
                "event {} has a non-Token program",
                row.event_id
            )));
        }
        let expected_evidence = match row.execution_status.as_str() {
            "succeeded" => ("committed", "invoked"),
            "failed" => match row.failed_outer_index {
                Some(failed) if row.outer_index < failed => ("rolled-back", "invoked"),
                Some(failed) if row.outer_index > failed => ("not-committed", "not-invoked"),
                Some(_) if row.batch_index.is_some() => ("not-committed", "unknown"),
                Some(_) => ("rolled-back", "invoked"),
                None if row.batch_index.is_some() => ("not-committed", "unknown"),
                None if row.inner_index.is_some() => ("rolled-back", "invoked"),
                None => ("not-committed", "unknown"),
            },
            "unknown" if row.batch_index.is_none() && row.inner_index.is_some() => {
                ("unknown", "invoked")
            }
            "unknown" => ("unknown", "unknown"),
            _ => {
                return Err(invalid_historical_row(format!(
                    "event {} has an unknown transaction status",
                    row.event_id
                )));
            }
        };
        if row.commit_state != expected_evidence.0
            || row.invocation_state != expected_evidence.1
            || (row.commit_state != "committed" && row.effect_count != 0)
        {
            return Err(invalid_historical_row(format!(
                "event {} has invalid invocation, commit, or effect evidence",
                row.event_id
            )));
        }
        validate_stored_event_variant(&row)?;
    }
    Ok(())
}

fn validate_stored_event_variant(row: &StoredEventDomainRow) -> Result<()> {
    let coverage_valid = match row.data_coverage.as_str() {
        "exact" | "not-requested" => row.data_coverage_reason.is_none(),
        "unknown" => row
            .data_coverage_reason
            .as_deref()
            .is_some_and(is_coverage_reason),
        _ => false,
    };
    if !coverage_valid {
        return Err(invalid_historical_row(format!(
            "event {} has invalid instruction-data coverage",
            row.event_id
        )));
    }

    let decoded_fields_are_empty = row.amount.is_none()
        && row.decimals.is_none()
        && row.required_signers.is_none()
        && row.authority_type.is_none()
        && row.embedded_a.is_none()
        && row.embedded_b.is_none()
        && row.optional_value_present.is_none()
        && row.ui_amount.is_none();
    let valid = match row.raw_kind.as_str() {
        "unknown" => {
            let tag = if row.data_coverage == "exact" {
                row.raw_data
                    .as_deref()
                    .and_then(|data| data.first())
                    .copied()
                    .map(i64::from)
            } else {
                None
            };
            row.raw_data.is_some()
                && row.trailing_data.is_none()
                && decoded_fields_are_empty
                && row.token_tag == tag
        }
        "classic" => {
            row.data_coverage == "exact"
                && row.data_coverage_reason.is_none()
                && row.raw_data.is_none()
                && row.trailing_data.is_some()
                && validate_classic_event_fields(row)
        }
        _ => false,
    };
    if !valid {
        return Err(invalid_historical_row(format!(
            "event {} has an invalid raw instruction variant",
            row.event_id
        )));
    }
    Ok(())
}

fn validate_classic_event_fields(row: &StoredEventDomainRow) -> bool {
    let no_amount = row.amount.is_none();
    let no_decimals = row.decimals.is_none();
    let no_signers = row.required_signers.is_none();
    let no_authority = row.authority_type.is_none();
    let no_embedded = row.embedded_a.is_none() && row.embedded_b.is_none();
    let no_optional = row.optional_value_present.is_none();
    let no_ui = row.ui_amount.is_none();
    match row.token_tag {
        Some(0 | 20) => {
            no_amount
                && row.decimals.is_some()
                && no_signers
                && no_authority
                && row.embedded_a.is_some()
                && row.optional_value_present == Some(i64::from(row.embedded_b.is_some()))
                && no_ui
        }
        Some(2 | 19) => {
            no_amount
                && no_decimals
                && row.required_signers.is_some()
                && no_authority
                && no_embedded
                && no_optional
                && no_ui
        }
        Some(3 | 4 | 7 | 8 | 23) => {
            row.amount.is_some()
                && no_decimals
                && no_signers
                && no_authority
                && no_embedded
                && no_optional
                && no_ui
        }
        Some(6) => {
            no_amount
                && no_decimals
                && no_signers
                && row.authority_type.is_some()
                && row.embedded_b.is_none()
                && row.optional_value_present == Some(i64::from(row.embedded_a.is_some()))
                && no_ui
        }
        Some(12..=15) => {
            row.amount.is_some()
                && row.decimals.is_some()
                && no_signers
                && no_authority
                && no_embedded
                && no_optional
                && no_ui
        }
        Some(16 | 18) => {
            no_amount
                && no_decimals
                && no_signers
                && no_authority
                && row.embedded_a.is_some()
                && row.embedded_b.is_none()
                && no_optional
                && no_ui
        }
        Some(24) => {
            no_amount
                && no_decimals
                && no_signers
                && no_authority
                && no_embedded
                && no_optional
                && row.ui_amount.is_some()
        }
        Some(45) => {
            no_decimals
                && no_signers
                && no_authority
                && no_embedded
                && row.optional_value_present == Some(i64::from(row.amount.is_some()))
                && no_ui
        }
        Some(1 | 5 | 9 | 10 | 11 | 17 | 21 | 22 | 38 | 255) => {
            decoded_fields_are_empty_for_classic(row)
        }
        _ => false,
    }
}

fn decoded_fields_are_empty_for_classic(row: &StoredEventDomainRow) -> bool {
    row.amount.is_none()
        && row.decimals.is_none()
        && row.required_signers.is_none()
        && row.authority_type.is_none()
        && row.embedded_a.is_none()
        && row.embedded_b.is_none()
        && row.optional_value_present.is_none()
        && row.ui_amount.is_none()
}

fn validate_effect_domains(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT effect.event_id, effect.effect_index, effect.effect_kind,
                effect.amount_le, effect.decimals, effect.checked,
                (SELECT count(*) FROM lifecycle_effects lifecycle
                  WHERE lifecycle.event_id = effect.event_id
                    AND lifecycle.effect_index = effect.effect_index),
                (SELECT count(*) FROM delta_legs leg
                  WHERE leg.event_id = effect.event_id
                    AND leg.effect_index = effect.effect_index)
           FROM event_effects effect",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    for row in rows {
        let (event, effect, kind, amount, decimals, checked, lifecycles, legs) = row?;
        let valid = match kind.as_str() {
            "lifecycle" => {
                amount.is_none()
                    && decimals.is_none()
                    && checked.is_none()
                    && lifecycles == 1
                    && legs == 0
            }
            "transfer" => {
                amount.is_some()
                    && checked == Some(i64::from(decimals.is_some()))
                    && lifecycles == 0
                    && legs == 2
            }
            "mint" | "burn" => {
                amount.is_some() && checked.is_none() && lifecycles == 0 && legs == 1
            }
            _ => false,
        };
        if !valid {
            return Err(invalid_historical_row(format!(
                "effect {event}:{effect} has an invalid durable variant"
            )));
        }
    }

    let mut statement = connection.prepare(
        "SELECT leg.event_id, leg.effect_index, leg.leg_index,
                effect.effect_kind, effect.amount_le, effect.amount_text,
                leg.direction, leg.transfer_role, leg.amount_le, leg.amount_text
           FROM delta_legs leg
           JOIN event_effects effect
             ON effect.event_id = leg.event_id
            AND effect.effect_index = leg.effect_index",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Vec<u8>>(8)?,
            row.get::<_, String>(9)?,
        ))
    })?;
    for row in rows {
        let (
            event,
            effect,
            leg,
            kind,
            effect_amount_le,
            effect_amount_text,
            direction,
            role,
            leg_amount_le,
            leg_amount_text,
        ) = row?;
        let geometry_valid = match (kind.as_str(), leg) {
            ("transfer", 0) => role.as_deref() == Some("source") && direction == "debit",
            ("transfer", 1) => role.as_deref() == Some("destination") && direction == "credit",
            ("mint", 0) => role.is_none() && direction == "credit",
            ("burn", 0) => role.is_none() && direction == "debit",
            _ => false,
        };
        if !geometry_valid
            || effect_amount_le != leg_amount_le
            || effect_amount_text != leg_amount_text
        {
            return Err(invalid_historical_row(format!(
                "delta leg {event}:{effect}:{leg} does not match its effect"
            )));
        }
    }
    Ok(())
}

struct StoredCoverageDomainRow {
    issue_id: i64,
    kind: String,
    instruction_order: Option<i64>,
    outer_index: Option<i64>,
    inner_index: Option<i64>,
    stack_height: Option<i64>,
    detail: Option<String>,
    data_coverage: Option<String>,
    coverage_reason: Option<String>,
    first_pubkey: Option<i64>,
    second_pubkey: Option<i64>,
    known_mint: Option<i64>,
    observed_mint: Option<i64>,
    expected_index: Option<i64>,
    actual_index: Option<i64>,
    transaction_status: String,
    transaction_reason: Option<String>,
}

fn validate_coverage_issue_domains(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT issue.issue_id, issue.issue_kind, issue.instruction_order,
                issue.outer_index, issue.inner_index, issue.stack_height,
                issue.detail, issue.data_coverage, issue.coverage_reason,
                issue.first_pubkey_id, issue.second_pubkey_id,
                issue.known_mint_pubkey_id, issue.observed_mint_pubkey_id,
                issue.expected_index, issue.actual_index,
                tx.execution_status, tx.status_reason
           FROM coverage_issues issue
           JOIN transactions tx
             ON tx.block_ordinal = issue.block_ordinal
            AND tx.tx_index = issue.tx_index",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(StoredCoverageDomainRow {
            issue_id: row.get(0)?,
            kind: row.get(1)?,
            instruction_order: row.get(2)?,
            outer_index: row.get(3)?,
            inner_index: row.get(4)?,
            stack_height: row.get(5)?,
            detail: row.get(6)?,
            data_coverage: row.get(7)?,
            coverage_reason: row.get(8)?,
            first_pubkey: row.get(9)?,
            second_pubkey: row.get(10)?,
            known_mint: row.get(11)?,
            observed_mint: row.get(12)?,
            expected_index: row.get(13)?,
            actual_index: row.get(14)?,
            transaction_status: row.get(15)?,
            transaction_reason: row.get(16)?,
        })
    })?;
    for row in rows {
        let row = row?;
        let has_coordinate = row.instruction_order.is_some();
        if has_coordinate != row.outer_index.is_some()
            || (!has_coordinate && (row.inner_index.is_some() || row.stack_height.is_some()))
        {
            return Err(invalid_historical_row(format!(
                "coverage issue {} has an incomplete coordinate",
                row.issue_id
            )));
        }
        let no_detail = row.detail.is_none();
        let no_coverage = row.data_coverage.is_none() && row.coverage_reason.is_none();
        let no_pubkeys = row.first_pubkey.is_none()
            && row.second_pubkey.is_none()
            && row.known_mint.is_none()
            && row.observed_mint.is_none();
        let no_indices = row.expected_index.is_none() && row.actual_index.is_none();
        let valid = match row.kind.as_str() {
            "decode" => {
                has_coordinate
                    && row.detail.as_ref().is_some_and(|detail| !detail.is_empty())
                    && no_coverage
                    && no_pubkeys
                    && no_indices
            }
            "instruction-data-unavailable" => {
                let coverage = match row.data_coverage.as_deref() {
                    Some("not-requested") => row.coverage_reason.is_none(),
                    Some("unknown") => row
                        .coverage_reason
                        .as_deref()
                        .is_some_and(is_coverage_reason),
                    _ => false,
                };
                has_coordinate && no_detail && coverage && no_pubkeys && no_indices
            }
            "insufficient-history" => {
                has_coordinate
                    && no_detail
                    && no_coverage
                    && row.first_pubkey.is_some()
                    && row.known_mint.is_none()
                    && row.observed_mint.is_none()
                    && no_indices
            }
            "conflicting-mint-evidence" => {
                has_coordinate
                    && no_detail
                    && no_coverage
                    && row.first_pubkey.is_some()
                    && row.second_pubkey.is_none()
                    && row.known_mint.is_some()
                    && row.observed_mint.is_some()
                    && no_indices
            }
            "sync-native-on-target" => {
                has_coordinate
                    && no_detail
                    && no_coverage
                    && row.first_pubkey.is_some()
                    && row.second_pubkey.is_none()
                    && row.known_mint.is_none()
                    && row.observed_mint.is_none()
                    && no_indices
            }
            "invalid-instruction-order" => {
                has_coordinate
                    && no_detail
                    && no_coverage
                    && no_pubkeys
                    && row.expected_index.is_some()
                    && row.actual_index.is_some()
            }
            "incomplete-instructions" | "incomplete-cpi" => {
                !has_coordinate
                    && no_detail
                    && row.data_coverage.is_none()
                    && row
                        .coverage_reason
                        .as_deref()
                        .is_some_and(is_coverage_reason)
                    && no_pubkeys
                    && no_indices
            }
            "cpi-not-recorded" => {
                !has_coordinate && no_detail && no_coverage && no_pubkeys && no_indices
            }
            "unknown-execution" => {
                !has_coordinate
                    && no_detail
                    && row.data_coverage.is_none()
                    && row.coverage_reason == row.transaction_reason
                    && row
                        .coverage_reason
                        .as_deref()
                        .is_some_and(is_coverage_reason)
                    && row.transaction_status == "unknown"
                    && no_pubkeys
                    && no_indices
            }
            _ => false,
        };
        if !valid {
            return Err(invalid_historical_row(format!(
                "coverage issue {} has an invalid variant",
                row.issue_id
            )));
        }
    }
    Ok(())
}

fn validate_dense_historical_children(connection: &Connection) -> Result<()> {
    const DENSE_QUERIES: &[(&str, &str)] = &[
        (
            "events",
            "SELECT EXISTS(
                SELECT 1 FROM (
                    SELECT min(event_index) AS minimum, max(event_index) AS maximum,
                           count(*) AS item_count
                      FROM events GROUP BY block_ordinal, tx_index
                ) WHERE minimum != 0 OR maximum + 1 != item_count
             )",
        ),
        (
            "event effects",
            "SELECT EXISTS(
                SELECT 1 FROM (
                    SELECT min(effect_index) AS minimum, max(effect_index) AS maximum,
                           count(*) AS item_count
                      FROM event_effects GROUP BY event_id
                ) WHERE minimum != 0 OR maximum + 1 != item_count
             )",
        ),
        (
            "event account bindings",
            "SELECT EXISTS(
                SELECT 1 FROM (
                    SELECT min(binding_index) AS minimum, max(binding_index) AS maximum,
                           count(*) AS item_count
                      FROM event_accounts GROUP BY event_id
                ) WHERE minimum != 0 OR maximum + 1 != item_count
             )",
        ),
        (
            "tracker account updates",
            "SELECT EXISTS(
                SELECT 1 FROM (
                    SELECT min(update_index) AS minimum, max(update_index) AS maximum,
                           count(*) AS item_count
                      FROM tracker_account_updates GROUP BY block_ordinal, tx_index
                ) WHERE minimum != 0 OR maximum + 1 != item_count
             )",
        ),
        (
            "coverage issues",
            "SELECT EXISTS(
                SELECT 1 FROM (
                    SELECT min(issue_index) AS minimum, max(issue_index) AS maximum,
                           count(*) AS item_count
                      FROM coverage_issues GROUP BY block_ordinal, tx_index
                ) WHERE minimum != 0 OR maximum + 1 != item_count
             )",
        ),
    ];
    for &(name, sql) in DENSE_QUERIES {
        let invalid: bool = connection.query_row(sql, [], |row| row.get(0))?;
        if invalid {
            return Err(invalid_historical_row(format!(
                "stored {name} are not dense"
            )));
        }
    }

    let invalid_event_order: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1
              FROM events current
              JOIN events previous
                ON previous.block_ordinal = current.block_ordinal
               AND previous.tx_index = current.tx_index
               AND previous.event_index + 1 = current.event_index
             WHERE current.instruction_order < previous.instruction_order
                OR (current.instruction_order = previous.instruction_order
                    AND ((previous.batch_index IS NULL)
                         OR (current.batch_index IS NOT NULL
                             AND current.batch_index <= previous.batch_index)))
         )",
        [],
        |row| row.get(0),
    )?;
    if invalid_event_order {
        return Err(invalid_historical_row(
            "stored events are not in tracker order",
        ));
    }

    Ok(())
}

fn validate_historical_tracker(connection: &Connection, spec: &TokenEventRunSpec) -> Result<()> {
    let opening = load_opening_snapshot(connection, spec.target_mint)?;
    let mut history = opening.history_coverage();
    let mut certainty_revision = opening.certainty_revision();
    let mut accounts = opening.accounts().clone();
    let mut transaction_statement = connection.prepare(
        "SELECT block_ordinal, tx_index, tracker_history_after,
                tracker_revision_after_le, tracker_revision_after_text
           FROM transactions ORDER BY block_ordinal, tx_index",
    )?;
    let mut update_statement = connection.prepare(
        "SELECT update_row.block_ordinal, update_row.tx_index,
                update_row.update_index, account.address,
                update_row.generation_le, update_row.generation_text,
                update_row.account_state, mint.address,
                update_row.confirmed_revision_le,
                update_row.confirmed_revision_text
           FROM tracker_account_updates update_row
           JOIN pubkeys account ON account.pubkey_id = update_row.pubkey_id
           LEFT JOIN pubkeys mint
             ON mint.pubkey_id = update_row.state_mint_pubkey_id
          ORDER BY update_row.block_ordinal, update_row.tx_index,
                   update_row.update_index",
    )?;
    let mut lifecycle_statement = connection.prepare(
        "SELECT event.block_ordinal, event.tx_index, event.event_index,
                effect.effect_index, account.address,
                effect.before_generation_le, effect.before_generation_text,
                effect.before_state, before_mint.address,
                effect.after_generation_le, effect.after_generation_text,
                effect.after_state, after_mint.address, effect.cause
           FROM lifecycle_effects effect
           JOIN events event ON event.event_id = effect.event_id
           JOIN pubkeys account
             ON account.pubkey_id = effect.account_pubkey_id
           LEFT JOIN pubkeys before_mint
             ON before_mint.pubkey_id = effect.before_state_mint_pubkey_id
           LEFT JOIN pubkeys after_mint
             ON after_mint.pubkey_id = effect.after_state_mint_pubkey_id
          ORDER BY event.block_ordinal, event.tx_index, event.event_index,
                   effect.effect_index",
    )?;
    let mut transaction_rows = transaction_statement.query([])?;
    let mut update_rows = update_statement.query([])?;
    let mut lifecycle_rows = lifecycle_statement.query([])?;
    let mut next_update = next_historical_tracker_update(&mut update_rows)?;
    let mut next_lifecycle = next_historical_lifecycle_effect(&mut lifecycle_rows)?;
    while let Some(row) = transaction_rows.next()? {
        let block = row.get::<_, i64>(0)?;
        let transaction = row.get::<_, i64>(1)?;
        let next_history = row.get::<_, String>(2)?;
        let revision_le = row.get::<_, Vec<u8>>(3)?;
        let revision_text = row.get::<_, String>(4)?;
        let next_history = parse_history(&next_history)?;
        let next_revision =
            parse_u64_pair(&revision_le, &revision_text, "historical tracker revision")?;
        if (history == HistoryCoverage::Partial && next_history == HistoryCoverage::Complete)
            || next_revision < certainty_revision
            || (history == HistoryCoverage::Complete
                && next_history == HistoryCoverage::Complete
                && next_revision != certainty_revision)
        {
            return Err(invalid_historical_row(format!(
                "transaction {block}:{transaction} has an invalid tracker-state transition"
            )));
        }

        if next_update
            .as_ref()
            .is_some_and(|update| (update.block, update.transaction) < (block, transaction))
        {
            return Err(invalid_historical_row(
                "a tracker update precedes its ordered transaction row",
            ));
        }
        if next_lifecycle
            .as_ref()
            .is_some_and(|effect| (effect.block, effect.transaction) < (block, transaction))
        {
            return Err(invalid_historical_row(
                "a lifecycle effect precedes its ordered transaction row",
            ));
        }
        let mut lifecycle_overlay = BTreeMap::new();
        while next_lifecycle
            .as_ref()
            .is_some_and(|effect| (effect.block, effect.transaction) == (block, transaction))
        {
            let effect = next_lifecycle
                .take()
                .ok_or_else(|| invalid_historical_row("lifecycle effect lookahead disappeared"))?;
            let current = lifecycle_overlay
                .get(&effect.change.account)
                .copied()
                .or_else(|| {
                    accounts
                        .get(&effect.change.account)
                        .map(|state| state.lifecycle)
                });
            if effect.change.before != current {
                return Err(invalid_historical_row(format!(
                    "transaction {block}:{transaction} lifecycle effect {}:{} does not continue the reconstructed account state",
                    effect.event, effect.effect
                )));
            }
            validate_lifecycle_change(&effect.change, spec.target_mint).map_err(|error| {
                invalid_historical_row(format!(
                    "transaction {block}:{transaction} lifecycle effect {}:{} is invalid: {error}",
                    effect.event, effect.effect
                ))
            })?;
            lifecycle_overlay.insert(effect.change.account, effect.change.after);
            next_lifecycle = next_historical_lifecycle_effect(&mut lifecycle_rows)?;
        }
        let mut expected_index = 0u32;
        let mut previous_account = None;
        while next_update
            .as_ref()
            .is_some_and(|update| (update.block, update.transaction) == (block, transaction))
        {
            let update = next_update
                .take()
                .ok_or_else(|| invalid_historical_row("tracker update lookahead disappeared"))?;
            if u32_from_i64(update.index, "tracker update index")? != expected_index {
                return Err(invalid_historical_row(format!(
                    "transaction {block}:{transaction} has a non-dense tracker update"
                )));
            }
            expected_index = expected_index.checked_add(1).ok_or_else(|| {
                invalid_historical_row("historical tracker update count exceeds u32")
            })?;
            let (account, state) = parse_stored_account_row(&update.account)?;
            if previous_account.is_some_and(|previous| account <= previous) {
                return Err(invalid_historical_row(format!(
                    "transaction {block}:{transaction} tracker updates are not ordered"
                )));
            }
            previous_account = Some(account);
            if state.confirmed_revision > next_revision
                || (next_history == HistoryCoverage::Complete
                    && state.confirmed_revision != next_revision)
                || matches!(state.lifecycle.state, TokenAccountState::ActiveOther { mint } if mint == spec.target_mint)
            {
                return Err(invalid_historical_row(format!(
                    "transaction {block}:{transaction} has an invalid tracker account update"
                )));
            }
            let expected_lifecycle = match (
                lifecycle_overlay.remove(&account),
                accounts.get(&account).copied(),
            ) {
                (Some(lifecycle), _) => lifecycle,
                (None, Some(current)) => current.lifecycle,
                (None, None) => {
                    return Err(invalid_historical_row(format!(
                        "transaction {block}:{transaction} has a new tracker account update without a lifecycle effect"
                    )));
                }
            };
            if state.lifecycle != expected_lifecycle {
                return Err(invalid_historical_row(format!(
                    "transaction {block}:{transaction} tracker account update does not match its lifecycle effects"
                )));
            }
            accounts.insert(account, state);
            next_update = next_historical_tracker_update(&mut update_rows)?;
        }
        if !lifecycle_overlay.is_empty() {
            return Err(invalid_historical_row(format!(
                "transaction {block}:{transaction} has a lifecycle effect without a final tracker account update"
            )));
        }
        history = next_history;
        certainty_revision = next_revision;
    }
    if next_update.is_some() {
        return Err(invalid_historical_row(
            "a tracker update has no ordered transaction row",
        ));
    }
    if next_lifecycle.is_some() {
        return Err(invalid_historical_row(
            "a lifecycle effect has no ordered transaction row",
        ));
    }

    let rebuilt = TargetMintTrackerSnapshot::from_parts(
        spec.target_mint,
        history,
        certainty_revision,
        accounts,
    )
    .map_err(|error| {
        invalid_historical_row(format!("historical tracker state is invalid: {error:?}"))
    })?;
    let current = load_current_snapshot(connection, spec.target_mint)?;
    if rebuilt != current {
        return Err(invalid_historical_row(
            "historical tracker updates do not reconstruct the current tracker state",
        ));
    }
    Ok(())
}

struct HistoricalLifecycleEffect {
    block: i64,
    transaction: i64,
    event: i64,
    effect: i64,
    change: AccountLifecycleChange,
}

fn next_historical_lifecycle_effect(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Option<HistoricalLifecycleEffect>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let account = pubkey_from_blob(&row.get::<_, Vec<u8>>(4)?, "lifecycle account")?;
    let before_generation_le = row.get::<_, Option<Vec<u8>>>(5)?;
    let before_generation_text = row.get::<_, Option<String>>(6)?;
    let before_state = row.get::<_, Option<String>>(7)?;
    let before_mint = row
        .get::<_, Option<Vec<u8>>>(8)?
        .as_deref()
        .map(|bytes| pubkey_from_blob(bytes, "lifecycle before-state mint"))
        .transpose()?;
    let before = match (
        before_generation_le.as_deref(),
        before_generation_text.as_deref(),
        before_state.as_deref(),
    ) {
        (None, None, None) => None,
        (Some(generation_le), Some(generation_text), Some(state)) => Some(TokenAccountLifecycle {
            generation: parse_u64_pair(
                generation_le,
                generation_text,
                "lifecycle before generation",
            )?,
            state: parse_account_state(state, before_mint)?,
        }),
        _ => {
            return Err(invalid_historical_row(
                "a lifecycle effect has an incomplete before-state",
            ));
        }
    };
    let after_generation_le = row.get::<_, Vec<u8>>(9)?;
    let after_generation_text = row.get::<_, String>(10)?;
    let after_state = row.get::<_, String>(11)?;
    let after_mint = row
        .get::<_, Option<Vec<u8>>>(12)?
        .as_deref()
        .map(|bytes| pubkey_from_blob(bytes, "lifecycle after-state mint"))
        .transpose()?;
    let after = TokenAccountLifecycle {
        generation: parse_u64_pair(
            &after_generation_le,
            &after_generation_text,
            "lifecycle after generation",
        )?,
        state: parse_account_state(&after_state, after_mint)?,
    };
    let cause = match row.get::<_, String>(13)?.as_str() {
        "initialize-account" => LifecycleCause::InitializeAccount,
        "explicit-mint-instruction" => LifecycleCause::ExplicitMintInstruction,
        "checked-transfer" => LifecycleCause::CheckedTransfer,
        "unchecked-transfer" => LifecycleCause::UncheckedTransfer,
        "close-account" => LifecycleCause::CloseAccount,
        cause => {
            return Err(invalid_historical_row(format!(
                "unknown lifecycle cause {cause:?}"
            )));
        }
    };
    Ok(Some(HistoricalLifecycleEffect {
        block: row.get(0)?,
        transaction: row.get(1)?,
        event: row.get(2)?,
        effect: row.get(3)?,
        change: AccountLifecycleChange {
            account,
            before,
            after,
            cause,
        },
    }))
}

struct HistoricalTrackerUpdate {
    block: i64,
    transaction: i64,
    index: i64,
    account: StoredAccountRow,
}

fn next_historical_tracker_update(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Option<HistoricalTrackerUpdate>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some(HistoricalTrackerUpdate {
        block: row.get(0)?,
        transaction: row.get(1)?,
        index: row.get(2)?,
        account: StoredAccountRow {
            account: row.get(3)?,
            generation_le: row.get(4)?,
            generation_text: row.get(5)?,
            state: row.get(6)?,
            state_mint: row.get(7)?,
            revision_le: row.get(8)?,
            revision_text: row.get(9)?,
        },
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedLifetimeRow {
    pubkey_id: i64,
    generation: u64,
    state: String,
    state_mint_pubkey_id: Option<i64>,
    confirmed_revision: u64,
}

impl ExpectedLifetimeRow {
    const fn key(&self) -> (i64, u64) {
        (self.pubkey_id, self.generation)
    }
}

fn read_lifetime_row(
    row: &rusqlite::Row<'_>,
    pubkey_column: usize,
    generation_column: usize,
    state_column: usize,
    mint_column: usize,
    revision_column: usize,
    field_prefix: &str,
) -> Result<ExpectedLifetimeRow> {
    let generation_le = row.get::<_, Vec<u8>>(generation_column)?;
    let generation_text = row.get::<_, String>(generation_column + 1)?;
    let revision_le = row.get::<_, Vec<u8>>(revision_column)?;
    let revision_text = row.get::<_, String>(revision_column + 1)?;
    Ok(ExpectedLifetimeRow {
        pubkey_id: row.get(pubkey_column)?,
        generation: parse_u64_pair(
            &generation_le,
            &generation_text,
            &format!("{field_prefix} generation"),
        )?,
        state: row.get(state_column)?,
        state_mint_pubkey_id: row.get(mint_column)?,
        confirmed_revision: parse_u64_pair(
            &revision_le,
            &revision_text,
            &format!("{field_prefix} confirmed revision"),
        )?,
    })
}

fn next_opening_lifetime(rows: &mut rusqlite::Rows<'_>) -> Result<Option<ExpectedLifetimeRow>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    read_lifetime_row(row, 0, 1, 3, 4, 5, "opening lifetime").map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LifetimeWriteOrder {
    block: i64,
    transaction: i64,
    phase: u8,
    first_index: i64,
    second_index: i64,
}

struct HistoricalLifetimeWrite {
    row: ExpectedLifetimeRow,
    order: LifetimeWriteOrder,
}

fn next_update_lifetime(rows: &mut rusqlite::Rows<'_>) -> Result<Option<HistoricalLifetimeWrite>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some(HistoricalLifetimeWrite {
        row: read_lifetime_row(row, 0, 1, 6, 7, 8, "tracker update lifetime")?,
        order: LifetimeWriteOrder {
            block: row.get(3)?,
            transaction: row.get(4)?,
            phase: 1,
            first_index: row.get(5)?,
            second_index: 0,
        },
    }))
}

fn next_lifecycle_lifetime(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Option<HistoricalLifetimeWrite>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    Ok(Some(HistoricalLifetimeWrite {
        row: read_lifetime_row(row, 0, 1, 7, 8, 9, "lifecycle after-state lifetime")?,
        order: LifetimeWriteOrder {
            block: row.get(3)?,
            transaction: row.get(4)?,
            phase: 0,
            first_index: row.get(5)?,
            second_index: row.get(6)?,
        },
    }))
}

fn next_materialized_lifetime(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Option<ExpectedLifetimeRow>> {
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    read_lifetime_row(row, 0, 1, 3, 4, 5, "materialized lifetime").map(Some)
}

fn validate_exact_lifetime_materialization(connection: &Connection) -> Result<()> {
    let mut opening_statement = connection.prepare(
        "SELECT pubkey_id, generation_le, generation_text, account_state,
                state_mint_pubkey_id, confirmed_revision_le,
                confirmed_revision_text
           FROM opening_tracker_accounts
          ORDER BY pubkey_id",
    )?;
    let mut update_statement = connection.prepare(
        "SELECT pubkey_id, generation_le, generation_text, block_ordinal,
                tx_index, update_index, account_state, state_mint_pubkey_id,
                confirmed_revision_le, confirmed_revision_text
           FROM tracker_account_updates
          ORDER BY pubkey_id, length(generation_text), generation_text,
                   block_ordinal, tx_index, update_index",
    )?;
    let mut lifecycle_statement = connection.prepare(
        "SELECT effect.account_pubkey_id, effect.after_generation_le,
                effect.after_generation_text, event.block_ordinal,
                event.tx_index, event.event_index, effect.effect_index,
                effect.after_state, effect.after_state_mint_pubkey_id,
                tx.tracker_revision_after_le,
                tx.tracker_revision_after_text
           FROM lifecycle_effects effect
           JOIN events event ON event.event_id = effect.event_id
           JOIN transactions tx
             ON tx.block_ordinal = event.block_ordinal
            AND tx.tx_index = event.tx_index
          ORDER BY effect.account_pubkey_id,
                   length(effect.after_generation_text),
                   effect.after_generation_text, event.block_ordinal,
                   event.tx_index, event.event_index, effect.effect_index",
    )?;
    let mut lifetime_statement = connection.prepare(
        "SELECT pubkey_id, generation_le, generation_text, account_state,
                state_mint_pubkey_id, confirmed_revision_le,
                confirmed_revision_text
           FROM account_lifetimes
          ORDER BY pubkey_id, length(generation_text), generation_text",
    )?;
    let mut opening_rows = opening_statement.query([])?;
    let mut update_rows = update_statement.query([])?;
    let mut lifecycle_rows = lifecycle_statement.query([])?;
    let mut lifetime_rows = lifetime_statement.query([])?;
    let mut opening = next_opening_lifetime(&mut opening_rows)?;
    let mut update = next_update_lifetime(&mut update_rows)?;
    let mut lifecycle = next_lifecycle_lifetime(&mut lifecycle_rows)?;
    let mut lifetime = next_materialized_lifetime(&mut lifetime_rows)?;

    while opening.is_some() || update.is_some() || lifecycle.is_some() {
        let key = [
            opening.as_ref().map(ExpectedLifetimeRow::key),
            update.as_ref().map(|write| write.row.key()),
            lifecycle.as_ref().map(|write| write.row.key()),
        ]
        .into_iter()
        .flatten()
        .min()
        .ok_or_else(|| invalid_historical_row("expected lifetime streams disappeared"))?;
        let mut expected = if opening.as_ref().is_some_and(|row| row.key() == key) {
            let row = opening
                .take()
                .ok_or_else(|| invalid_historical_row("opening lifetime lookahead disappeared"))?;
            opening = next_opening_lifetime(&mut opening_rows)?;
            Some(row)
        } else {
            None
        };
        while update.as_ref().is_some_and(|write| write.row.key() == key)
            || lifecycle
                .as_ref()
                .is_some_and(|write| write.row.key() == key)
        {
            let take_lifecycle = match (lifecycle.as_ref(), update.as_ref()) {
                (Some(lifecycle), Some(update))
                    if lifecycle.row.key() == key && update.row.key() == key =>
                {
                    lifecycle.order < update.order
                }
                (Some(lifecycle), _) if lifecycle.row.key() == key => true,
                _ => false,
            };
            if take_lifecycle {
                let write = lifecycle.take().ok_or_else(|| {
                    invalid_historical_row("lifecycle lifetime lookahead disappeared")
                })?;
                expected = Some(write.row);
                lifecycle = next_lifecycle_lifetime(&mut lifecycle_rows)?;
            } else {
                let write = update.take().ok_or_else(|| {
                    invalid_historical_row("tracker update lifetime lookahead disappeared")
                })?;
                expected = Some(write.row);
                update = next_update_lifetime(&mut update_rows)?;
            }
        }
        let expected = expected.ok_or_else(|| {
            invalid_historical_row("expected lifetime reconstruction lost its row")
        })?;
        let actual = lifetime.take().ok_or_else(|| {
            invalid_historical_row(format!(
                "materialized lifetime {}:{} is missing",
                key.0, key.1
            ))
        })?;
        if actual.key() != key {
            return Err(invalid_historical_row(format!(
                "materialized lifetime {}:{} is not expected {}:{}",
                actual.pubkey_id, actual.generation, key.0, key.1
            )));
        }
        if actual != expected {
            return Err(invalid_historical_row(format!(
                "materialized lifetime {}:{} differs from its final historical state",
                key.0, key.1
            )));
        }
        lifetime = next_materialized_lifetime(&mut lifetime_rows)?;
    }
    if let Some(extra) = lifetime {
        return Err(invalid_historical_row(format!(
            "materialized lifetime {}:{} has no opening row, lifecycle effect, or tracker update",
            extra.pubkey_id, extra.generation
        )));
    }
    Ok(())
}

fn validate_no_unreferenced_pubkeys(connection: &Connection) -> Result<()> {
    let orphan: Option<i64> = connection
        .query_row(
            "WITH referenced(pubkey_id) AS (
                SELECT target_mint_pubkey_id FROM run_identity
                UNION SELECT token_program_pubkey_id FROM run_identity
                UNION SELECT pubkey_id FROM opening_tracker_accounts
                UNION SELECT state_mint_pubkey_id FROM opening_tracker_accounts
                UNION SELECT pubkey_id FROM account_lifetimes
                UNION SELECT state_mint_pubkey_id FROM account_lifetimes
                UNION SELECT pubkey_id FROM tracker_accounts
                UNION SELECT pubkey_id FROM tracker_account_updates
                UNION SELECT state_mint_pubkey_id FROM tracker_account_updates
                UNION SELECT program_pubkey_id FROM events
                UNION SELECT embedded_pubkey_a FROM events
                UNION SELECT embedded_pubkey_b FROM events
                UNION SELECT pubkey_id FROM event_accounts
                UNION SELECT account_pubkey_id FROM lifecycle_effects
                UNION SELECT before_state_mint_pubkey_id FROM lifecycle_effects
                UNION SELECT after_state_mint_pubkey_id FROM lifecycle_effects
                UNION SELECT account_pubkey_id FROM delta_legs
                UNION SELECT first_pubkey_id FROM coverage_issues
                UNION SELECT second_pubkey_id FROM coverage_issues
                UNION SELECT known_mint_pubkey_id FROM coverage_issues
                UNION SELECT observed_mint_pubkey_id FROM coverage_issues
            )
            SELECT p.pubkey_id
              FROM pubkeys AS p
              LEFT JOIN referenced AS r ON r.pubkey_id = p.pubkey_id
             WHERE r.pubkey_id IS NULL
             ORDER BY p.pubkey_id
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(pubkey_id) = orphan {
        return Err(invalid_historical_row(format!(
            "pubkey dictionary row {pubkey_id} is unreferenced"
        )));
    }
    Ok(())
}

fn parse_stored_account_row(
    row: &StoredAccountRow,
) -> Result<(PubkeyBytes, TargetAccountSnapshot)> {
    let account = pubkey_from_blob(&row.account, "tracker account")?;
    let generation = parse_u64_pair(
        &row.generation_le,
        &row.generation_text,
        "account generation",
    )?;
    let state_mint = row
        .state_mint
        .as_deref()
        .map(|bytes| pubkey_from_blob(bytes, "account state mint"))
        .transpose()?;
    let state = parse_account_state(&row.state, state_mint)?;
    let confirmed_revision = parse_u64_pair(
        &row.revision_le,
        &row.revision_text,
        "account confirmed revision",
    )?;
    Ok((
        account,
        TargetAccountSnapshot {
            lifecycle: TokenAccountLifecycle { generation, state },
            confirmed_revision,
        },
    ))
}

fn validate_committed_universe(
    connection: &Connection,
    spec: &TokenEventRunSpec,
    next_block_ordinal: u32,
) -> Result<()> {
    let expected_count = next_block_ordinal
        .checked_sub(spec.range.first_block)
        .ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint(
                "next block ordinal is before the scan range".into(),
            )
        })?;
    let mut statement = connection.prepare(
        "SELECT b.block_ordinal, b.epoch_le, b.epoch_text, b.slot_le, b.slot_text,
                b.transaction_count, count(t.tx_index), min(t.tx_index), max(t.tx_index)
           FROM blocks b
           LEFT JOIN transactions t ON t.block_ordinal = b.block_ordinal
          GROUP BY b.block_ordinal
          ORDER BY b.block_ordinal",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<i64>>(8)?,
        ))
    })?;
    let mut observed_count = 0u32;
    let mut previous_slot = None;
    for row in rows {
        let (
            ordinal,
            epoch_le,
            epoch_text,
            slot_le,
            slot_text,
            declared_transactions,
            actual_transactions,
            minimum_tx,
            maximum_tx,
        ) = row?;
        let ordinal = u32_from_i64(ordinal, "stored block ordinal")?;
        let expected_ordinal = spec
            .range
            .first_block
            .checked_add(observed_count)
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidCheckpoint(
                    "stored block ordinal exceeds u32".into(),
                )
            })?;
        if ordinal != expected_ordinal {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored block ordinal {ordinal} is not dense at {expected_ordinal}"
            )));
        }
        if parse_u64_pair(&epoch_le, &epoch_text, "stored block epoch")? != spec.source.epoch {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored block {ordinal} has the wrong source epoch"
            )));
        }
        let slot = parse_u64_pair(&slot_le, &slot_text, "stored block slot")?;
        let last_source_slot = spec.source.first_slot + spec.source.slots_per_epoch - 1;
        if slot < spec.source.first_slot || slot > last_source_slot {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored block {ordinal} has slot {slot} outside the source epoch"
            )));
        }
        if previous_slot.is_some_and(|previous| slot <= previous) {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored block {ordinal} does not have an increasing slot"
            )));
        }
        previous_slot = Some(slot);
        let declared_transactions =
            u32_from_i64(declared_transactions, "declared transaction count")?;
        let actual_transactions = u32_from_i64(actual_transactions, "actual transaction count")?;
        if declared_transactions != actual_transactions
            || (actual_transactions == 0 && (minimum_tx.is_some() || maximum_tx.is_some()))
            || (actual_transactions != 0
                && (minimum_tx != Some(0)
                    || maximum_tx != Some(i64::from(actual_transactions - 1))))
        {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored block {ordinal} has a non-dense transaction universe"
            )));
        }
        observed_count = observed_count.checked_add(1).ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint("stored block count exceeds u32".into())
        })?;
    }
    if observed_count != expected_count {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "checkpoint covers {expected_count} blocks but database stores {observed_count}"
        )));
    }
    Ok(())
}

fn validate_previous_slot(
    connection: &Connection,
    spec: &TokenEventRunSpec,
    next_block_ordinal: u32,
    current_slot: u64,
) -> Result<()> {
    if next_block_ordinal == spec.range.first_block {
        return Ok(());
    }
    let previous_ordinal = next_block_ordinal - 1;
    let stored: Option<(Vec<u8>, String)> = {
        let mut statement = connection
            .prepare_cached("SELECT slot_le, slot_text FROM blocks WHERE block_ordinal = ?1")?;
        statement
            .query_row(params![i64::from(previous_ordinal)], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .optional()?
    };
    let Some((slot_le, slot_text)) = stored else {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "previous block {previous_ordinal} is absent"
        )));
    };
    let previous_slot = parse_u64_pair(&slot_le, &slot_text, "previous block slot")?;
    if current_slot <= previous_slot {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "block slot {current_slot} is not after previous slot {previous_slot}"
        )));
    }
    Ok(())
}

fn configure_safety(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

fn configure_writer(connection: &Connection) -> Result<()> {
    // The block path uses more statements than rusqlite's small default
    // cache. Keep parsed statements across block transactions without changing
    // any SQLite durability or transaction boundary.
    connection.set_prepared_statement_cache_capacity(64);
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    verify_writer_configuration(connection)?;
    Ok(())
}

fn verify_writer_configuration(connection: &Connection) -> Result<()> {
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "SQLite journal_mode is {journal_mode}, expected WAL"
        )));
    }
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if synchronous != 2 {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "SQLite synchronous is {synchronous}, expected FULL (2)"
        )));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct SchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

fn load_schema_objects(connection: &Connection) -> Result<Vec<SchemaObject>> {
    let mut statement = connection.prepare(
        "SELECT type, name, tbl_name, sql
           FROM sqlite_schema
          WHERE name NOT LIKE 'sqlite_%'
          ORDER BY type, name, tbl_name",
    )?;
    statement
        .query_map([], |row| {
            Ok(SchemaObject {
                object_type: row.get(0)?,
                name: row.get(1)?,
                table_name: row.get(2)?,
                sql: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn validate_schema_topology(connection: &Connection) -> Result<()> {
    let reference = Connection::open_in_memory()?;
    reference.execute_batch(SCHEMA)?;
    let expected = load_schema_objects(&reference)?;
    let actual = load_schema_objects(connection)?;
    if actual != expected {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "SQLite schema topology differs from the exact token event schema".into(),
        ));
    }
    Ok(())
}

fn validate_run_identity_bounds(connection: &Connection) -> Result<()> {
    let lengths = connection.query_row(
        "SELECT length(CAST(source_label AS BLOB)),
                coalesce(length(CAST(source_cluster_id AS BLOB)), 0),
                coalesce(length(CAST(source_binding AS BLOB)), 0)
           FROM run_identity WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    if [lengths.0, lengths.1, lengths.2]
        .into_iter()
        .any(|length| length < 0 || length > MAX_SOURCE_IDENTITY_TEXT_BYTES as i64)
    {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "a source identity string exceeds {MAX_SOURCE_IDENTITY_TEXT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_opening_cardinality(
    connection: &Connection,
    expected: &TokenEventRunSpec,
) -> Result<()> {
    let stored: i64 =
        connection.query_row("SELECT count(*) FROM opening_tracker_accounts", [], |row| {
            row.get(0)
        })?;
    let expected_count =
        i64::try_from(expected.opening_tracker.accounts().len()).map_err(|_| {
            TokenEventDatabaseError::InvalidSpecification(
                "opening tracker account count exceeds the SQLite integer limit".into(),
            )
        })?;
    if stored != expected_count {
        return Err(TokenEventDatabaseError::SpecificationMismatch(format!(
            "opening tracker account count is {stored}, expected {expected_count}"
        )));
    }
    Ok(())
}

fn validate_durable_resource_bounds(connection: &Connection) -> Result<()> {
    let events_limit = i64::try_from(MAX_EVENTS_PER_TRANSACTION).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("event limit exceeds SQLite i64".into())
    })?;
    let updates_limit = i64::try_from(MAX_ACCOUNT_UPDATES_PER_TRANSACTION).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("account-update limit exceeds SQLite i64".into())
    })?;
    let issues_limit = i64::try_from(MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("coverage-issue limit exceeds SQLite i64".into())
    })?;
    let effects_limit = i64::try_from(MAX_EFFECTS_PER_TRANSACTION).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("effect limit exceeds SQLite i64".into())
    })?;
    let resource_limit = i64::try_from(MAX_TRACKED_EVENT_RESOURCE_BYTES).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("resource-byte limit exceeds SQLite i64".into())
    })?;
    let text_limit = i64::try_from(MAX_DURABLE_TEXT_BYTES).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint("text-byte limit exceeds SQLite i64".into())
    })?;
    let over_limit: i64 = connection.query_row(
        "SELECT EXISTS (
            SELECT 1
              FROM transactions AS t
             WHERE (SELECT count(*) FROM events AS e
                     WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index) > ?1
                OR (SELECT count(*) FROM tracker_account_updates AS u
                     WHERE u.block_ordinal = t.block_ordinal AND u.tx_index = t.tx_index) > ?2
                OR (SELECT count(*) FROM coverage_issues AS c
                     WHERE c.block_ordinal = t.block_ordinal AND c.tx_index = t.tx_index) > ?3
                OR (SELECT count(*) FROM event_effects AS f
                     JOIN events AS e ON e.event_id = f.event_id
                    WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index) > ?4
                OR (SELECT coalesce(sum(coalesce(length(e.raw_data), 0)
                                        + coalesce(length(e.trailing_data), 0)), 0)
                      FROM events AS e
                     WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index)
                   + 32 * (SELECT count(*) FROM event_accounts AS a
                            JOIN events AS e ON e.event_id = a.event_id
                           WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index) > ?5
                OR EXISTS (
                    SELECT 1 FROM events AS e
                     WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index
                       AND (coalesce(length(e.raw_data), 0) > ?5
                            OR coalesce(length(e.trailing_data), 0) > ?5
                            OR coalesce(length(CAST(e.ui_amount AS BLOB)), 0) > ?6))
                OR EXISTS (
                    SELECT 1 FROM coverage_issues AS c
                     WHERE c.block_ordinal = t.block_ordinal AND c.tx_index = t.tx_index
                       AND coalesce(length(CAST(c.detail AS BLOB)), 0) > ?6)
                OR EXISTS (
                    SELECT 1 FROM events AS e
                    JOIN event_accounts AS a ON a.event_id = e.event_id
                    WHERE e.block_ordinal = t.block_ordinal AND e.tx_index = t.tx_index
                    GROUP BY a.event_id HAVING count(*) > 65535)
        )",
        params![
            events_limit,
            updates_limit,
            issues_limit,
            effects_limit,
            resource_limit,
            text_limit,
        ],
        |row| row.get(0),
    )?;
    if over_limit != 0 {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "durable transaction rows exceed the token event storage bounds".into(),
        ));
    }
    Ok(())
}

fn validate_application(connection: &Connection) -> Result<()> {
    let application_id: i64 =
        connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    if application_id != TOKEN_EVENT_APPLICATION_ID {
        return Err(TokenEventDatabaseError::WrongApplication(application_id));
    }
    let version: i64 = connection.query_row(
        "SELECT schema_version FROM run_identity WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    if version != TOKEN_EVENT_SCHEMA_VERSION {
        return Err(TokenEventDatabaseError::UnsupportedSchema(version));
    }
    Ok(())
}

fn execute_cached<P: rusqlite::Params>(
    transaction: &Transaction<'_>,
    sql: &str,
    params: P,
) -> Result<usize> {
    let mut statement = transaction.prepare_cached(sql)?;
    Ok(statement.execute(params)?)
}

fn insert_run(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    spec: &TokenEventRunSpec,
) -> Result<[u8; DIGEST_BYTES]> {
    let target_id = intern_pubkey(transaction, &spec.target_mint)?;
    let program_id = intern_pubkey(transaction, &spec.token_program)?;
    let epoch = U64Sql::new(spec.source.epoch);
    let first_slot = U64Sql::new(spec.source.first_slot);
    let slots_per_epoch = U64Sql::new(spec.source.slots_per_epoch);
    transaction.execute(
        "INSERT INTO run_identity (
            singleton, schema_version, source_format, source_label, source_cluster_id,
            source_epoch_le, source_epoch_text, source_first_slot_le, source_first_slot_text,
            source_slots_per_epoch_le, source_slots_per_epoch_text, source_block_count,
            source_verification, source_binding, target_mint_pubkey_id,
            token_program_pubkey_id, first_block_ordinal, range_block_count
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
            TOKEN_EVENT_SCHEMA_VERSION,
            archive_format_text(spec.source.format),
            spec.source.label,
            spec.source.cluster_id,
            epoch.bytes.as_slice(),
            epoch.text,
            first_slot.bytes.as_slice(),
            first_slot.text,
            slots_per_epoch.bytes.as_slice(),
            slots_per_epoch.text,
            i64::from(spec.source.block_count),
            source_verification_text(spec.source.verification),
            spec.source.binding,
            target_id,
            program_id,
            i64::from(spec.range.first_block),
            i64::from(spec.range.block_count.get()),
        ],
    )?;

    insert_opening_snapshot(transaction, &spec.opening_tracker)?;
    insert_current_snapshot(transaction, &spec.opening_tracker)?;
    let tracker_digest = opening_tracker_digest(&spec.opening_tracker)?;
    transaction.execute(
        "INSERT INTO checkpoint (
            singleton, next_block_ordinal, digest_head, tracker_digest
         ) VALUES (1, ?1, ?2, ?3)",
        params![
            i64::from(spec.range.first_block),
            EMPTY_DIGEST_HEAD.as_slice(),
            tracker_digest.as_slice(),
        ],
    )?;
    Ok(tracker_digest)
}

fn validate_run_match(stored: &TokenEventRunSpec, expected: &TokenEventRunSpec) -> Result<()> {
    if stored.source != expected.source {
        return Err(TokenEventDatabaseError::SpecificationMismatch(
            "source identity or binding differs".into(),
        ));
    }
    if stored.target_mint != expected.target_mint {
        return Err(TokenEventDatabaseError::SpecificationMismatch(
            "target mint differs".into(),
        ));
    }
    if stored.token_program != expected.token_program {
        return Err(TokenEventDatabaseError::SpecificationMismatch(
            "Token program differs".into(),
        ));
    }
    if stored.range != expected.range {
        return Err(TokenEventDatabaseError::SpecificationMismatch(
            "scan range differs".into(),
        ));
    }
    if stored.opening_tracker != expected.opening_tracker {
        return Err(TokenEventDatabaseError::SpecificationMismatch(
            "opening tracker snapshot differs".into(),
        ));
    }
    Ok(())
}

fn audit_connection(
    connection: &Connection,
    expected: Option<&TokenEventRunSpec>,
) -> Result<TokenEventAudit> {
    validate_application(connection)?;
    validate_schema_topology(connection)?;
    validate_run_identity_bounds(connection)?;
    validate_durable_resource_bounds(connection)?;
    if let Some(expected) = expected {
        validate_opening_cardinality(connection, expected)?;
    }
    let spec = load_run_spec(connection)?;
    if let Err(error) = spec.validate() {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "stored run specification is invalid: {error}"
        )));
    }
    if let Some(expected) = expected {
        validate_run_match(&spec, expected)?;
    }
    validate_database_integrity(connection)?;
    validate_historical_rows(connection, &spec)?;
    let resume = load_resume_state(connection, &spec)?;
    let digest_head = load_digest_head(connection)?;
    let tracker_digest = load_checkpoint_tracker_digest(connection)?;
    Ok(TokenEventAudit {
        spec,
        resume,
        digest_head,
        tracker_digest,
    })
}

fn load_run_spec(connection: &Connection) -> Result<TokenEventRunSpec> {
    let mut statement = connection.prepare(
        "SELECT r.source_format, r.source_label, r.source_cluster_id,
                r.source_epoch_le, r.source_epoch_text,
                r.source_first_slot_le, r.source_first_slot_text,
                r.source_slots_per_epoch_le, r.source_slots_per_epoch_text,
                r.source_block_count, r.source_verification, r.source_binding,
                target.address, program.address, r.first_block_ordinal,
                r.range_block_count
           FROM run_identity r
           JOIN pubkeys target ON target.pubkey_id = r.target_mint_pubkey_id
           JOIN pubkeys program ON program.pubkey_id = r.token_program_pubkey_id
          WHERE r.singleton = 1",
    )?;
    let row = statement.query_row([], |row| {
        Ok(StoredRunRow {
            format: row.get(0)?,
            label: row.get(1)?,
            cluster_id: row.get(2)?,
            epoch_le: row.get(3)?,
            epoch_text: row.get(4)?,
            first_slot_le: row.get(5)?,
            first_slot_text: row.get(6)?,
            slots_per_epoch_le: row.get(7)?,
            slots_per_epoch_text: row.get(8)?,
            block_count: row.get(9)?,
            verification: row.get(10)?,
            binding: row.get(11)?,
            target: row.get(12)?,
            program: row.get(13)?,
            first_block: row.get(14)?,
            range_count: row.get(15)?,
        })
    })?;
    let source = SourceIdentity {
        format: parse_archive_format(&row.format)?,
        label: row.label,
        cluster_id: row.cluster_id,
        epoch: parse_u64_pair(&row.epoch_le, &row.epoch_text, "source epoch")?,
        first_slot: parse_u64_pair(
            &row.first_slot_le,
            &row.first_slot_text,
            "source first slot",
        )?,
        slots_per_epoch: parse_u64_pair(
            &row.slots_per_epoch_le,
            &row.slots_per_epoch_text,
            "source slots per epoch",
        )?,
        block_count: u32_from_i64(row.block_count, "source block count")?,
        verification: parse_source_verification(&row.verification)?,
        binding: row.binding,
    };
    let target_mint = pubkey_from_blob(&row.target, "target mint")?;
    let token_program = pubkey_from_blob(&row.program, "token program")?;
    let first_block = u32_from_i64(row.first_block, "first block ordinal")?;
    let range_count = u32_from_i64(row.range_count, "range block count")?;
    let range_count = std::num::NonZeroU32::new(range_count).ok_or_else(|| {
        TokenEventDatabaseError::InvalidCheckpoint("range block count is zero".into())
    })?;
    let opening_tracker = load_opening_snapshot(connection, target_mint)?;
    Ok(TokenEventRunSpec {
        source,
        target_mint,
        token_program,
        range: ScanRange {
            first_block,
            block_count: range_count,
        },
        opening_tracker,
    })
}

struct StoredRunRow {
    format: String,
    label: String,
    cluster_id: Option<String>,
    epoch_le: Vec<u8>,
    epoch_text: String,
    first_slot_le: Vec<u8>,
    first_slot_text: String,
    slots_per_epoch_le: Vec<u8>,
    slots_per_epoch_text: String,
    block_count: i64,
    verification: String,
    binding: Option<String>,
    target: Vec<u8>,
    program: Vec<u8>,
    first_block: i64,
    range_count: i64,
}

#[derive(Debug)]
struct U64Sql {
    bytes: [u8; 8],
    text: String,
}

impl U64Sql {
    fn new(value: u64) -> Self {
        Self {
            bytes: value.to_le_bytes(),
            text: value.to_string(),
        }
    }
}

fn parse_u64_pair(bytes: &[u8], text: &str, field: &str) -> Result<u64> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint(format!(
            "{field} byte value does not have length 8"
        ))
    })?;
    let from_bytes = u64::from_le_bytes(bytes);
    let from_text = text.parse::<u64>().map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint(format!(
            "{field} text value is not a canonical u64"
        ))
    })?;
    if from_text.to_string() != text || from_bytes != from_text {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "{field} byte and text values differ"
        )));
    }
    Ok(from_bytes)
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint(format!("{field} value {value} is outside u32"))
    })
}

fn pubkey_from_blob(bytes: &[u8], field: &str) -> Result<PubkeyBytes> {
    bytes.try_into().map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint(format!("{field} does not have length 32"))
    })
}

fn archive_format_text(value: ArchiveFormat) -> &'static str {
    match value {
        ArchiveFormat::Car => "car",
        ArchiveFormat::CompactV2 => "compact-v2",
        ArchiveFormat::IndexerV3 => "indexer-v3",
    }
}

fn parse_archive_format(value: &str) -> Result<ArchiveFormat> {
    match value {
        "car" => Ok(ArchiveFormat::Car),
        "compact-v2" => Ok(ArchiveFormat::CompactV2),
        "indexer-v3" => Ok(ArchiveFormat::IndexerV3),
        _ => Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "unknown archive format {value:?}"
        ))),
    }
}

fn source_verification_text(value: SourceVerification) -> &'static str {
    match value {
        SourceVerification::ObjectSetBound => "object-set-bound",
        SourceVerification::OperatorTrusted => "operator-trusted",
        SourceVerification::InternalBindingOnly => "internal-binding-only",
        SourceVerification::Unverified => "unverified",
    }
}

fn parse_source_verification(value: &str) -> Result<SourceVerification> {
    match value {
        "object-set-bound" => Ok(SourceVerification::ObjectSetBound),
        "operator-trusted" => Ok(SourceVerification::OperatorTrusted),
        "internal-binding-only" => Ok(SourceVerification::InternalBindingOnly),
        "unverified" => Ok(SourceVerification::Unverified),
        _ => Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "unknown source verification {value:?}"
        ))),
    }
}

fn load_pubkey_ids(connection: &Connection) -> Result<HashMap<PubkeyBytes, i64>> {
    let row_count: i64 =
        connection.query_row("SELECT count(*) FROM pubkeys", [], |row| row.get(0))?;
    let row_count = usize::try_from(row_count).map_err(|_| {
        TokenEventDatabaseError::InvalidCheckpoint(
            "the pubkey table row count exceeds the address space".into(),
        )
    })?;
    let mut statement =
        connection.prepare("SELECT pubkey_id, address FROM pubkeys ORDER BY pubkey_id")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut pubkey_ids = HashMap::with_capacity(row_count);
    for row in rows {
        let (pubkey_id, address) = row?;
        let address: PubkeyBytes = address.try_into().map_err(|address: Vec<u8>| {
            TokenEventDatabaseError::InvalidCheckpoint(format!(
                "pubkey {pubkey_id} has {} address bytes instead of 32",
                address.len()
            ))
        })?;
        if pubkey_ids.insert(address, pubkey_id).is_some() {
            return Err(TokenEventDatabaseError::InvalidCheckpoint(
                "the pubkey table contains a duplicate address".into(),
            ));
        }
    }
    Ok(pubkey_ids)
}

fn intern_pubkey(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    pubkey: &PubkeyBytes,
) -> Result<i64> {
    if let Some(pubkey_id) = transaction.committed.get(pubkey) {
        transaction
            .cache_hits
            .set(transaction.cache_hits.get().saturating_add(1));
        return Ok(*pubkey_id);
    }
    if let Some(pubkey_id) = transaction.pending.borrow().get(pubkey) {
        transaction
            .cache_hits
            .set(transaction.cache_hits.get().saturating_add(1));
        transaction
            .pending_hits
            .set(transaction.pending_hits.get().saturating_add(1));
        return Ok(*pubkey_id);
    }
    transaction
        .sql_misses
        .set(transaction.sql_misses.get().saturating_add(1));
    {
        let mut insert = transaction.prepare_cached(
            "INSERT INTO pubkeys (address) VALUES (?1) ON CONFLICT(address) DO NOTHING",
        )?;
        insert.execute(params![pubkey.as_slice()])?;
    }
    let mut select =
        transaction.prepare_cached("SELECT pubkey_id FROM pubkeys WHERE address = ?1")?;
    let pubkey_id = select.query_row(params![pubkey.as_slice()], |row| row.get(0))?;
    transaction.pending.borrow_mut().insert(*pubkey, pubkey_id);
    Ok(pubkey_id)
}

fn insert_opening_snapshot(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    snapshot: &TargetMintTrackerSnapshot,
) -> Result<()> {
    let revision = U64Sql::new(snapshot.certainty_revision());
    execute_cached(
        transaction,
        "INSERT INTO opening_tracker_state (
            singleton, history_coverage, certainty_revision_le, certainty_revision_text
         ) VALUES (1, ?1, ?2, ?3)",
        params![
            history_text(snapshot.history_coverage()),
            revision.bytes.as_slice(),
            revision.text,
        ],
    )?;
    for (account, state) in snapshot.accounts() {
        let account_id = intern_pubkey(transaction, account)?;
        let generation = U64Sql::new(state.lifecycle.generation);
        let confirmed_revision = U64Sql::new(state.confirmed_revision);
        let (account_state, state_mint) = state_sql(transaction, state.lifecycle.state)?;
        execute_cached(
            transaction,
            "INSERT INTO opening_tracker_accounts (
                pubkey_id, generation_le, generation_text, account_state,
                state_mint_pubkey_id, confirmed_revision_le, confirmed_revision_text
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                account_id,
                generation.bytes.as_slice(),
                generation.text,
                account_state,
                state_mint,
                confirmed_revision.bytes.as_slice(),
                confirmed_revision.text,
            ],
        )?;
    }
    Ok(())
}

fn insert_current_snapshot(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    snapshot: &TargetMintTrackerSnapshot,
) -> Result<()> {
    let revision = U64Sql::new(snapshot.certainty_revision());
    execute_cached(
        transaction,
        "INSERT INTO tracker_state (
            singleton, history_coverage, certainty_revision_le, certainty_revision_text
         ) VALUES (1, ?1, ?2, ?3)",
        params![
            history_text(snapshot.history_coverage()),
            revision.bytes.as_slice(),
            revision.text,
        ],
    )?;
    for (account, state) in snapshot.accounts() {
        upsert_lifetime(transaction, *account, *state)?;
        let account_id = intern_pubkey(transaction, account)?;
        let generation = U64Sql::new(state.lifecycle.generation);
        execute_cached(
            transaction,
            "INSERT INTO tracker_accounts (pubkey_id, generation_le) VALUES (?1, ?2)",
            params![account_id, generation.bytes.as_slice()],
        )?;
    }
    Ok(())
}

fn apply_current_checkpoint(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    tracked_transactions: &[TrackedTokenTransaction],
    tracker_before: TrackerStateAfter,
    tracker_after: TrackerStateAfter,
) -> Result<()> {
    for tracked in tracked_transactions {
        for update in &tracked.account_updates {
            let account_id = intern_pubkey(transaction, &update.account)?;
            let generation = U64Sql::new(update.state.lifecycle.generation);
            execute_cached(
                transaction,
                "INSERT INTO tracker_accounts (pubkey_id, generation_le)
                 VALUES (?1, ?2)
                 ON CONFLICT(pubkey_id) DO UPDATE SET
                    generation_le = excluded.generation_le",
                params![account_id, generation.bytes.as_slice()],
            )?;
        }
    }
    if tracker_after == tracker_before {
        return Ok(());
    }
    // Keep the block-start validation fail-closed without a SELECT for every
    // transaction. A changed row must still have the exact prior state.
    let before_revision = U64Sql::new(tracker_before.certainty_revision);
    let after_revision = U64Sql::new(tracker_after.certainty_revision);
    let updated = execute_cached(
        transaction,
        "UPDATE tracker_state
            SET history_coverage = ?1,
                certainty_revision_le = ?2,
                certainty_revision_text = ?3
          WHERE singleton = 1
            AND history_coverage = ?4
            AND certainty_revision_le = ?5
            AND certainty_revision_text = ?6",
        params![
            history_text(tracker_after.history),
            after_revision.bytes.as_slice(),
            after_revision.text,
            history_text(tracker_before.history),
            before_revision.bytes.as_slice(),
            before_revision.text,
        ],
    )?;
    if updated != 1 {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "tracker state differs from the validated in-memory transition base".into(),
        ));
    }
    Ok(())
}

fn upsert_lifetime(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    account: PubkeyBytes,
    state: TargetAccountSnapshot,
) -> Result<()> {
    let account_id = intern_pubkey(transaction, &account)?;
    let generation = U64Sql::new(state.lifecycle.generation);
    let revision = U64Sql::new(state.confirmed_revision);
    let (account_state, state_mint) = state_sql(transaction, state.lifecycle.state)?;
    execute_cached(
        transaction,
        "INSERT INTO account_lifetimes (
            pubkey_id, generation_le, generation_text, account_state,
            state_mint_pubkey_id, confirmed_revision_le, confirmed_revision_text
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(pubkey_id, generation_le) DO UPDATE SET
            generation_text = excluded.generation_text,
            account_state = excluded.account_state,
            state_mint_pubkey_id = excluded.state_mint_pubkey_id,
            confirmed_revision_le = excluded.confirmed_revision_le,
            confirmed_revision_text = excluded.confirmed_revision_text",
        params![
            account_id,
            generation.bytes.as_slice(),
            generation.text,
            account_state,
            state_mint,
            revision.bytes.as_slice(),
            revision.text,
        ],
    )?;
    Ok(())
}

fn state_sql(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    state: TokenAccountState,
) -> Result<(&'static str, Option<i64>)> {
    match state {
        TokenAccountState::ActiveTarget => Ok(("active-target", None)),
        TokenAccountState::ActiveOther { mint } => {
            Ok(("active-other", Some(intern_pubkey(transaction, &mint)?)))
        }
        TokenAccountState::Closed { last_mint } => Ok((
            "closed",
            last_mint
                .map(|mint| intern_pubkey(transaction, &mint))
                .transpose()?,
        )),
    }
}

fn history_text(value: HistoryCoverage) -> &'static str {
    match value {
        HistoryCoverage::Complete => "complete",
        HistoryCoverage::Partial => "partial",
    }
}

fn parse_history(value: &str) -> Result<HistoryCoverage> {
    match value {
        "complete" => Ok(HistoryCoverage::Complete),
        "partial" => Ok(HistoryCoverage::Partial),
        _ => Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "unknown tracker history coverage {value:?}"
        ))),
    }
}

fn load_opening_snapshot(
    connection: &Connection,
    target_mint: PubkeyBytes,
) -> Result<TargetMintTrackerSnapshot> {
    load_snapshot(
        connection,
        target_mint,
        "SELECT history_coverage, certainty_revision_le, certainty_revision_text
           FROM opening_tracker_state WHERE singleton = 1",
        "SELECT account.address, state.generation_le, state.generation_text,
                state.account_state, mint.address,
                state.confirmed_revision_le, state.confirmed_revision_text
           FROM opening_tracker_accounts state
           JOIN pubkeys account ON account.pubkey_id = state.pubkey_id
           LEFT JOIN pubkeys mint ON mint.pubkey_id = state.state_mint_pubkey_id
          ORDER BY account.address",
    )
}

fn load_current_snapshot(
    connection: &Connection,
    target_mint: PubkeyBytes,
) -> Result<TargetMintTrackerSnapshot> {
    load_snapshot(
        connection,
        target_mint,
        "SELECT history_coverage, certainty_revision_le, certainty_revision_text
           FROM tracker_state WHERE singleton = 1",
        "SELECT account.address, lifetime.generation_le, lifetime.generation_text,
                lifetime.account_state, mint.address,
                lifetime.confirmed_revision_le, lifetime.confirmed_revision_text
           FROM tracker_accounts current
           JOIN pubkeys account ON account.pubkey_id = current.pubkey_id
           JOIN account_lifetimes lifetime
             ON lifetime.pubkey_id = current.pubkey_id
            AND lifetime.generation_le = current.generation_le
           LEFT JOIN pubkeys mint ON mint.pubkey_id = lifetime.state_mint_pubkey_id
          ORDER BY account.address",
    )
}

fn load_snapshot(
    connection: &Connection,
    target_mint: PubkeyBytes,
    state_sql: &str,
    accounts_sql: &str,
) -> Result<TargetMintTrackerSnapshot> {
    let (history, revision_le, revision_text): (String, Vec<u8>, String) =
        connection.query_row(state_sql, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    let history = parse_history(&history)?;
    let certainty_revision =
        parse_u64_pair(&revision_le, &revision_text, "tracker certainty revision")?;

    let mut statement = connection.prepare(accounts_sql)?;
    let rows = statement.query_map([], |row| {
        Ok(StoredAccountRow {
            account: row.get(0)?,
            generation_le: row.get(1)?,
            generation_text: row.get(2)?,
            state: row.get(3)?,
            state_mint: row.get(4)?,
            revision_le: row.get(5)?,
            revision_text: row.get(6)?,
        })
    })?;
    let mut accounts = Vec::new();
    for row in rows {
        let row = row?;
        let account = pubkey_from_blob(&row.account, "tracker account")?;
        let generation = parse_u64_pair(
            &row.generation_le,
            &row.generation_text,
            "account generation",
        )?;
        let state_mint = row
            .state_mint
            .as_deref()
            .map(|bytes| pubkey_from_blob(bytes, "account state mint"))
            .transpose()?;
        let state = parse_account_state(&row.state, state_mint)?;
        let confirmed_revision = parse_u64_pair(
            &row.revision_le,
            &row.revision_text,
            "account confirmed revision",
        )?;
        accounts.push((
            account,
            TargetAccountSnapshot {
                lifecycle: TokenAccountLifecycle { generation, state },
                confirmed_revision,
            },
        ));
    }
    TargetMintTrackerSnapshot::from_parts(target_mint, history, certainty_revision, accounts)
        .map_err(|error| {
            TokenEventDatabaseError::InvalidCheckpoint(format!(
                "stored tracker snapshot is invalid: {error:?}"
            ))
        })
}

struct StoredAccountRow {
    account: Vec<u8>,
    generation_le: Vec<u8>,
    generation_text: String,
    state: String,
    state_mint: Option<Vec<u8>>,
    revision_le: Vec<u8>,
    revision_text: String,
}

fn parse_account_state(value: &str, mint: Option<PubkeyBytes>) -> Result<TokenAccountState> {
    match (value, mint) {
        ("active-target", None) => Ok(TokenAccountState::ActiveTarget),
        ("active-other", Some(mint)) => Ok(TokenAccountState::ActiveOther { mint }),
        ("closed", last_mint) => Ok(TokenAccountState::Closed { last_mint }),
        _ => Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "invalid account state and mint pair for {value:?}"
        ))),
    }
}

fn validate_source_block(spec: &TokenEventRunSpec, block: BlockView<'_>) -> Result<()> {
    if block.header.epoch != spec.source.epoch {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "block epoch {} differs from source epoch {}",
            block.header.epoch, spec.source.epoch
        )));
    }
    let range_end = spec.end_block_exclusive()?;
    if block.header.block_ordinal < spec.range.first_block
        || block.header.block_ordinal >= range_end
    {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "block ordinal {} is outside {}..{}",
            block.header.block_ordinal, spec.range.first_block, range_end
        )));
    }
    let last_slot = spec.source.first_slot + spec.source.slots_per_epoch - 1;
    if block.header.slot < spec.source.first_slot || block.header.slot > last_slot {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "block slot {} is outside source slot range {}..={last_slot}",
            block.header.slot, spec.source.first_slot
        )));
    }
    u32::try_from(block.transactions.len()).map_err(|_| {
        TokenEventDatabaseError::InvalidBlock("block transaction count exceeds u32".into())
    })?;

    for (position, source) in block.transaction_views().enumerate() {
        let expected_tx_index = u32::try_from(position).map_err(|_| {
            TokenEventDatabaseError::InvalidBlock("transaction index exceeds u32".into())
        })?;
        if source.header.tx_index != expected_tx_index {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "source transaction index {} is not dense at {expected_tx_index}",
                source.header.tx_index
            )));
        }
    }
    Ok(())
}

fn validate_tracked_transaction(
    source: TransactionView<'_>,
    tracked: &TrackedTokenTransaction,
) -> Result<()> {
    if tracked.block != source.block
        || tracked.tx_index != source.header.tx_index
        || tracked.execution_status != source.header.status
    {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "tracked transaction {} does not match its source header",
            source.header.tx_index
        )));
    }
    if tracked.events.len() > MAX_EVENTS_PER_TRANSACTION {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "transaction {} has too many token events",
            source.header.tx_index
        )));
    }
    if tracked.coverage_issues.len() > MAX_TOKEN_COVERAGE_ISSUES_PER_TRANSACTION {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "transaction {} has too many token coverage issues",
            source.header.tx_index
        )));
    }
    if tracked.account_updates.len() > MAX_ACCOUNT_UPDATES_PER_TRANSACTION {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "transaction {} has too many token account updates",
            source.header.tx_index
        )));
    }
    validate_source_token_input(source)?;
    validate_tracked_resource_bounds(tracked)?;
    if !matches!(source.header.status, ExecutionStatus::Succeeded)
        && !tracked.account_updates.is_empty()
    {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "non-successful transaction {} has committed account updates",
            source.header.tx_index
        )));
    }

    let mut previous_order = None;
    let mut last_batch_index = None;
    let mut saw_unbatched_event = false;
    let mut decoded_batch = None::<(u32, DecodedClassicTokenBatch)>;
    for event in &tracked.events {
        let instruction = source
            .instructions
            .get(usize::try_from(event.coordinate.order).map_err(|_| {
                TokenEventDatabaseError::InvalidBlock("instruction order exceeds usize".into())
            })?)
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(format!(
                    "event instruction order {} is outside transaction {}",
                    event.coordinate.order, source.header.tx_index
                ))
            })?;
        if instruction.coordinate != event.coordinate {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "event coordinate {:?} differs from its source instruction",
                event.coordinate
            )));
        }
        if previous_order.is_some_and(|previous| event.coordinate.order < previous) {
            return Err(TokenEventDatabaseError::InvalidBlock(
                "token events are not in source instruction order".into(),
            ));
        }
        if previous_order != Some(event.coordinate.order) {
            previous_order = Some(event.coordinate.order);
            last_batch_index = None;
            saw_unbatched_event = false;
        }
        match event.batch_index {
            Some(batch_index) => {
                if saw_unbatched_event
                    || last_batch_index.is_some_and(|previous| batch_index <= previous)
                {
                    return Err(TokenEventDatabaseError::InvalidBlock(
                        "token Batch events are not in strict child order".into(),
                    ));
                }
                last_batch_index = Some(batch_index);
            }
            None => {
                if saw_unbatched_event {
                    return Err(TokenEventDatabaseError::InvalidBlock(
                        "one source instruction has duplicate unbatched events".into(),
                    ));
                }
                saw_unbatched_event = true;
            }
        }
        validate_event_status(source.header.status, event)?;
        let batch = if event.batch_index.is_some() {
            if decoded_batch
                .as_ref()
                .is_none_or(|(order, _)| *order != event.coordinate.order)
            {
                let decoded = decode_classic_token_batch(&instruction.accounts, &instruction.data)
                    .map_err(|error| {
                        TokenEventDatabaseError::InvalidBlock(format!(
                            "a Batch child event has invalid parent geometry: {error:?}"
                        ))
                    })?;
                decoded_batch = Some((event.coordinate.order, decoded));
            }
            decoded_batch.as_ref().map(|(_, batch)| batch)
        } else {
            None
        };
        validate_event_raw(instruction, event, batch)?;
        for effect in &event.effects {
            validate_effect(effect)?;
        }
    }
    for issue in &tracked.coverage_issues {
        if let Some(coordinate) = issue.coordinate {
            let source_coordinate = source
                .instructions
                .get(usize::try_from(coordinate.order).map_err(|_| {
                    TokenEventDatabaseError::InvalidBlock("issue order exceeds usize".into())
                })?)
                .map(|instruction| instruction.coordinate);
            if source_coordinate != Some(coordinate) {
                return Err(TokenEventDatabaseError::InvalidBlock(
                    "a token coverage issue has no matching source coordinate".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_source_token_input(source: TransactionView<'_>) -> Result<()> {
    let mut bytes = 0usize;
    for instruction in source
        .instructions
        .iter()
        .filter(|instruction| instruction.program_id == Some(CLASSIC_SPL_TOKEN_PROGRAM_ID))
    {
        let account_bytes = instruction.accounts.len().checked_mul(32).ok_or_else(|| {
            TokenEventDatabaseError::InvalidBlock(
                "classic Token source account bytes exceed usize".into(),
            )
        })?;
        bytes = bytes
            .checked_add(account_bytes)
            .and_then(|value| value.checked_add(instruction.data.len()))
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(
                    "classic Token source bytes exceed usize".into(),
                )
            })?;
        if bytes > MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "classic Token source bytes {bytes} exceed {MAX_TOKEN_INPUT_BYTES_PER_TRANSACTION}"
            )));
        }
    }
    Ok(())
}

fn validate_tracked_resource_bounds(tracked: &TrackedTokenTransaction) -> Result<()> {
    let mut resource_bytes = 0usize;
    let mut effect_count = 0usize;
    for event in &tracked.events {
        let (account_count, payload_bytes) = match &event.raw {
            ObservedTokenInstruction::Classic(decoded) => {
                let ui_amount_bytes = match &decoded.instruction {
                    ClassicTokenInstruction::UiAmountToAmount { ui_amount } => ui_amount.len(),
                    _ => 0,
                };
                (
                    decoded.roles.len(),
                    decoded
                        .trailing_data
                        .len()
                        .checked_add(ui_amount_bytes)
                        .ok_or_else(|| {
                            TokenEventDatabaseError::InvalidBlock(
                                "classic event payload bytes exceed usize".into(),
                            )
                        })?,
                )
            }
            ObservedTokenInstruction::Unknown(raw) => (raw.accounts.len(), raw.data.len()),
        };
        let account_bytes = account_count.checked_mul(32).ok_or_else(|| {
            TokenEventDatabaseError::InvalidBlock("event account bytes exceed usize".into())
        })?;
        resource_bytes = resource_bytes
            .checked_add(account_bytes)
            .and_then(|value| value.checked_add(payload_bytes))
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(
                    "aggregate tracked event bytes exceed usize".into(),
                )
            })?;
        if resource_bytes > MAX_TRACKED_EVENT_RESOURCE_BYTES {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "tracked event resource bytes {resource_bytes} exceed {MAX_TRACKED_EVENT_RESOURCE_BYTES}"
            )));
        }
        effect_count = effect_count
            .checked_add(event.effects.len())
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(
                    "aggregate token effect count exceeds usize".into(),
                )
            })?;
        if effect_count > MAX_EFFECTS_PER_TRANSACTION {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "tracked effect count {effect_count} exceeds {MAX_EFFECTS_PER_TRANSACTION}"
            )));
        }
    }
    Ok(())
}

fn validate_event_raw(
    source: &blockzilla_model::ResolvedInstruction,
    event: &TrackedTokenEvent,
    decoded_batch: Option<&DecodedClassicTokenBatch>,
) -> Result<()> {
    if source.program_id != Some(CLASSIC_SPL_TOKEN_PROGRAM_ID) {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "a token event has a non-Token source program".into(),
        ));
    }
    let expected = if let Some(batch_index) = event.batch_index {
        if source.data_coverage != InstructionDataCoverage::Exact
            || source.data.first() != Some(&255)
        {
            return Err(TokenEventDatabaseError::InvalidBlock(
                "a Batch child event has no exact Batch source instruction".into(),
            ));
        }
        let child = decoded_batch
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(
                    "a Batch child event has no decoded parent".into(),
                )
            })?
            .children
            .iter()
            .find(|child| child.batch_index == batch_index)
            .ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(format!(
                    "Batch child event index {batch_index} is absent from the source"
                ))
            })?;
        match decode_classic_token_instruction(&child.accounts, &child.data) {
            Ok(decoded) => ObservedTokenInstruction::Classic(decoded),
            Err(_) => ObservedTokenInstruction::Unknown(RawUnknownTokenInstruction {
                program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
                accounts: child.accounts.clone(),
                data_coverage: InstructionDataCoverage::Exact,
                data: child.data.clone(),
            }),
        }
    } else if source.data_coverage == InstructionDataCoverage::Exact
        && source.data.first() != Some(&255)
    {
        match decode_classic_token_instruction(&source.accounts, &source.data) {
            Ok(decoded) => ObservedTokenInstruction::Classic(decoded),
            Err(_) => ObservedTokenInstruction::Unknown(RawUnknownTokenInstruction {
                program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
                accounts: source.accounts.clone(),
                data_coverage: source.data_coverage,
                data: source.data.clone(),
            }),
        }
    } else {
        ObservedTokenInstruction::Unknown(RawUnknownTokenInstruction {
            program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
            accounts: source.accounts.clone(),
            data_coverage: source.data_coverage,
            data: source.data.clone(),
        })
    };
    if event.raw != expected {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "token event payload differs from the source instruction".into(),
        ));
    }
    Ok(())
}

fn validate_event_status(status: ExecutionStatus, event: &TrackedTokenEvent) -> Result<()> {
    let valid = matches!(
        (status, event.commit, event.invocation),
        (
            ExecutionStatus::Succeeded,
            TokenCommitState::Committed,
            TokenInvocationEvidence::Invoked,
        ) | (
            ExecutionStatus::Failed,
            TokenCommitState::RolledBack,
            TokenInvocationEvidence::Invoked,
        ) | (
            ExecutionStatus::Failed,
            TokenCommitState::NotCommitted,
            TokenInvocationEvidence::NotInvoked | TokenInvocationEvidence::Unknown,
        ) | (ExecutionStatus::Unknown(_), TokenCommitState::Unknown, _)
    );
    if !valid {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "invalid invocation/commit pair {:?}/{:?} for transaction status {:?}",
            event.invocation, event.commit, status
        )));
    }
    if event.commit != TokenCommitState::Committed && !event.effects.is_empty() {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "a non-committed event contains balance or lifecycle effects".into(),
        ));
    }
    Ok(())
}

fn validate_effect(effect: &TargetMintEffect) -> Result<()> {
    let valid = match effect {
        TargetMintEffect::Lifecycle(change) => {
            change.after.generation != 0
                && match change.before {
                    None => change.after.generation == 1,
                    Some(before) => {
                        before.generation != 0
                            && (change.after.generation == before.generation
                                || before.generation.checked_add(1)
                                    == Some(change.after.generation))
                    }
                }
        }
        TargetMintEffect::Transfer(transfer) => {
            let [source, destination] = transfer.legs;
            source.role == TransferLegRole::Source
                && source.account == transfer.source
                && source.generation != 0
                && source.direction == BalanceDirection::Debit
                && source.amount == transfer.amount
                && destination.role == TransferLegRole::Destination
                && destination.account == transfer.destination
                && destination.generation != 0
                && destination.direction == BalanceDirection::Credit
                && destination.amount == transfer.amount
                && transfer.checked == transfer.decimals.is_some()
        }
        TargetMintEffect::Mint { generation, .. } | TargetMintEffect::Burn { generation, .. } => {
            *generation != 0
        }
    };
    if !valid {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an event effect has invalid lifecycle or delta geometry".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TrackerStateAfter {
    history: HistoryCoverage,
    certainty_revision: u64,
}

fn validate_tracker_transition(
    connection: &Connection,
    target_mint: PubkeyBytes,
    tracker_before: TrackerStateAfter,
    tracked_transactions: &[TrackedTokenTransaction],
) -> Result<TrackerStateAfter> {
    let TrackerStateAfter {
        mut history,
        mut certainty_revision,
    } = tracker_before;
    let mut account_overlay: BTreeMap<PubkeyBytes, TargetAccountSnapshot> = BTreeMap::new();
    for tracked in tracked_transactions {
        if history == HistoryCoverage::Partial && tracked.history_after == HistoryCoverage::Complete
        {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "transaction {} makes partial tracker history complete",
                tracked.tx_index
            )));
        }
        if tracked.certainty_revision_after < certainty_revision {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "transaction {} moves the tracker revision backwards",
                tracked.tx_index
            )));
        }
        if history == HistoryCoverage::Complete
            && tracked.history_after == HistoryCoverage::Complete
            && tracked.certainty_revision_after != certainty_revision
        {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "transaction {} changes revision without a history gap",
                tracked.tx_index
            )));
        }
        let mut lifecycle_overlay = BTreeMap::new();
        for event in &tracked.events {
            for effect in &event.effects {
                let TargetMintEffect::Lifecycle(change) = effect else {
                    continue;
                };
                let current = match lifecycle_overlay.get(&change.account).copied() {
                    Some(current) => Some(current),
                    None => match account_overlay.get(&change.account).copied() {
                        Some(current) => Some(current.lifecycle),
                        None => load_current_account(connection, change.account, target_mint)?
                            .map(|current| current.lifecycle),
                    },
                };
                if change.before != current {
                    return Err(TokenEventDatabaseError::InvalidBlock(format!(
                        "transaction {} has a lifecycle effect that does not continue the account state",
                        tracked.tx_index
                    )));
                }
                validate_lifecycle_change(change, target_mint)?;
                lifecycle_overlay.insert(change.account, change.after);
            }
        }
        let mut previous_update_account = None;
        for update in &tracked.account_updates {
            if previous_update_account.is_some_and(|previous| update.account <= previous) {
                return Err(TokenEventDatabaseError::InvalidBlock(format!(
                    "transaction {} account updates are not in strict public-key order",
                    tracked.tx_index
                )));
            }
            previous_update_account = Some(update.account);
            let current = match account_overlay.get(&update.account).copied() {
                Some(current) => Some(current),
                None => load_current_account(connection, update.account, target_mint)?,
            };
            let lifecycle_after_effects = lifecycle_overlay.remove(&update.account);
            validate_account_update(
                update.account,
                current,
                update.state,
                lifecycle_after_effects,
                target_mint,
                tracked.history_after,
                tracked.certainty_revision_after,
            )?;
            account_overlay.insert(update.account, update.state);
        }
        if !lifecycle_overlay.is_empty() {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "transaction {} has a lifecycle effect without a final account update",
                tracked.tx_index
            )));
        }
        history = tracked.history_after;
        certainty_revision = tracked.certainty_revision_after;
    }
    Ok(TrackerStateAfter {
        history,
        certainty_revision,
    })
}

fn validate_lifecycle_change(
    change: &AccountLifecycleChange,
    target_mint: PubkeyBytes,
) -> Result<()> {
    let after_is_closed = matches!(change.after.state, TokenAccountState::Closed { .. });
    if matches!(change.after.state, TokenAccountState::ActiveOther { mint } if mint == target_mint)
        || change.before.is_some_and(
            |before| matches!(before.state, TokenAccountState::ActiveOther { mint } if mint == target_mint),
        )
    {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "a lifecycle effect stores the target mint as another mint".into(),
        ));
    }
    match (change.before, change.cause) {
        (None, LifecycleCause::CloseAccount) => Err(TokenEventDatabaseError::InvalidBlock(
            "a close lifecycle effect has no prior account lifetime".into(),
        )),
        (None, _)
            if change.after.generation != 1
                || change.after.state != TokenAccountState::ActiveTarget =>
        {
            Err(TokenEventDatabaseError::InvalidBlock(
                "a new lifecycle effect has invalid generation or state".into(),
            ))
        }
        (None, _) => Ok(()),
        (Some(before), LifecycleCause::CloseAccount) => {
            let expected = TokenAccountLifecycle {
                generation: before.generation,
                state: TokenAccountState::Closed {
                    last_mint: before.state.active_mint(target_mint),
                },
            };
            if matches!(before.state, TokenAccountState::Closed { .. }) || change.after != expected
            {
                return Err(TokenEventDatabaseError::InvalidBlock(
                    "a close lifecycle effect has an invalid transition".into(),
                ));
            }
            Ok(())
        }
        (Some(before), _) => {
            let next_generation = before.generation.checked_add(1).ok_or_else(|| {
                TokenEventDatabaseError::InvalidBlock(format!(
                    "account {:?} lifecycle generation is exhausted",
                    change.account
                ))
            })?;
            if change.after.generation != next_generation || after_is_closed {
                return Err(TokenEventDatabaseError::InvalidBlock(
                    "an active lifecycle effect has an invalid transition".into(),
                ));
            }
            Ok(())
        }
    }
}

fn load_tracker_state(connection: &Connection) -> Result<(HistoryCoverage, u64)> {
    let (history, revision_le, revision_text): (String, Vec<u8>, String) = {
        let mut statement = connection.prepare_cached(
            "SELECT history_coverage, certainty_revision_le, certainty_revision_text
               FROM tracker_state WHERE singleton = 1",
        )?;
        statement.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
    };
    Ok((
        parse_history(&history)?,
        parse_u64_pair(&revision_le, &revision_text, "tracker certainty revision")?,
    ))
}

fn load_current_account(
    connection: &Connection,
    account: PubkeyBytes,
    target_mint: PubkeyBytes,
) -> Result<Option<TargetAccountSnapshot>> {
    type StoredCurrentAccount = (Vec<u8>, String, String, Option<Vec<u8>>, Vec<u8>, String);
    let stored: Option<StoredCurrentAccount> = {
        let mut statement = connection.prepare_cached(
            "SELECT lifetime.generation_le, lifetime.generation_text,
                    lifetime.account_state, mint.address,
                    lifetime.confirmed_revision_le, lifetime.confirmed_revision_text
               FROM tracker_accounts current
               JOIN pubkeys account ON account.pubkey_id = current.pubkey_id
               JOIN account_lifetimes lifetime
                 ON lifetime.pubkey_id = current.pubkey_id
                AND lifetime.generation_le = current.generation_le
              LEFT JOIN pubkeys mint ON mint.pubkey_id = lifetime.state_mint_pubkey_id
              WHERE account.address = ?1",
        )?;
        statement
            .query_row(params![account.as_slice()], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .optional()?
    };
    stored
        .map(
            |(generation_le, generation_text, state, state_mint, revision_le, revision_text)| {
                let generation = parse_u64_pair(
                    &generation_le,
                    &generation_text,
                    "current account generation",
                )?;
                let state_mint = state_mint
                    .as_deref()
                    .map(|bytes| pubkey_from_blob(bytes, "current account mint"))
                    .transpose()?;
                let state = parse_account_state(&state, state_mint)?;
                if matches!(state, TokenAccountState::ActiveOther { mint } if mint == target_mint) {
                    return Err(TokenEventDatabaseError::InvalidCheckpoint(
                        "current account stores the target mint as another mint".into(),
                    ));
                }
                let confirmed_revision = parse_u64_pair(
                    &revision_le,
                    &revision_text,
                    "current account confirmed revision",
                )?;
                Ok(TargetAccountSnapshot {
                    lifecycle: TokenAccountLifecycle { generation, state },
                    confirmed_revision,
                })
            },
        )
        .transpose()
}

fn validate_account_update(
    account: PubkeyBytes,
    current: Option<TargetAccountSnapshot>,
    update: TargetAccountSnapshot,
    lifecycle_after_effects: Option<TokenAccountLifecycle>,
    target_mint: PubkeyBytes,
    history_after: HistoryCoverage,
    certainty_revision_after: u64,
) -> Result<()> {
    if update.lifecycle.generation == 0 || update.confirmed_revision == 0 {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an account update has a zero generation or revision".into(),
        ));
    }
    if update.confirmed_revision > certainty_revision_after {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an account update has a future confirmation revision".into(),
        ));
    }
    if history_after == HistoryCoverage::Complete
        && update.confirmed_revision != certainty_revision_after
    {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an account update is inexact in complete history".into(),
        ));
    }
    if matches!(update.lifecycle.state, TokenAccountState::ActiveOther { mint } if mint == target_mint)
    {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an account update stores the target mint as another mint".into(),
        ));
    }
    let expected_lifecycle = match (lifecycle_after_effects, current) {
        (Some(lifecycle), _) => lifecycle,
        (None, Some(current)) => current.lifecycle,
        (None, None) => {
            return Err(TokenEventDatabaseError::InvalidBlock(format!(
                "new account {account:?} has an update without a lifecycle effect"
            )));
        }
    };
    if update.lifecycle != expected_lifecycle {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "an account update does not match its proven lifecycle effects".into(),
        ));
    }
    Ok(())
}

fn validate_already_committed(
    connection: &Connection,
    spec: &TokenEventRunSpec,
    block: BlockView<'_>,
    expected_source_digest: &[u8; DIGEST_BYTES],
    next_block_ordinal: u32,
    checkpoint_digest_head: &[u8; DIGEST_BYTES],
    checkpoint_tracker_digest: &[u8; DIGEST_BYTES],
) -> Result<()> {
    type StoredReplayDigests = (Vec<u8>, Vec<u8>, Vec<u8>, String, Vec<u8>, String, Vec<u8>);
    let stored: Option<StoredReplayDigests> = connection
        .query_row(
            "SELECT source_digest, durable_rows_digest, chain_digest,
                    tracker_history_after, tracker_revision_after_le,
                    tracker_revision_after_text, tracker_digest_after
               FROM blocks WHERE block_ordinal = ?1",
            params![i64::from(block.header.block_ordinal)],
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
        )
        .optional()?;
    let Some((
        stored_source,
        stored_durable,
        stored_chain,
        history_after,
        revision_after_le,
        revision_after_text,
        stored_tracker_digest,
    )) = stored
    else {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "checkpoint passed block {}, but its block row is absent",
            block.header.block_ordinal
        )));
    };
    let stored_source = digest_from_blob(stored_source, "stored source block digest")?;
    if &stored_source != expected_source_digest {
        return Err(TokenEventDatabaseError::InvalidBlock(
            "replayed source block facts differ from the committed source digest".into(),
        ));
    }
    let previous_tracker_digest = if block.header.block_ordinal == spec.range.first_block {
        opening_tracker_digest(&spec.opening_tracker)?
    } else {
        let previous_ordinal = block.header.block_ordinal.checked_sub(1).ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint(
                "replayed block has no previous tracker-digest ordinal".into(),
            )
        })?;
        let value: Vec<u8> = connection.query_row(
            "SELECT tracker_digest_after FROM blocks WHERE block_ordinal = ?1",
            [i64::from(previous_ordinal)],
            |row| row.get(0),
        )?;
        digest_from_blob(value, "previous block tracker digest")?
    };
    let history_after = parse_history(&history_after)?;
    let revision_after = parse_u64_pair(
        &revision_after_le,
        &revision_after_text,
        "replayed tracker revision",
    )?;
    let tracker_digest = tracker_after_digest(
        connection,
        block.header.block_ordinal,
        &previous_tracker_digest,
        history_after,
        revision_after,
    )?;
    let stored_tracker_digest =
        digest_from_blob(stored_tracker_digest, "stored block tracker digest")?;
    if tracker_digest != stored_tracker_digest {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "tracker-after digest changed for replayed block {}",
            block.header.block_ordinal
        )));
    }
    let stored_durable = digest_from_blob(stored_durable, "stored durable row digest")?;
    let durable = durable_block_digest(connection, block.header.block_ordinal)?;
    if durable != stored_durable {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "durable rows changed for replayed block {}",
            block.header.block_ordinal
        )));
    }
    let previous = if block.header.block_ordinal == spec.range.first_block {
        EMPTY_DIGEST_HEAD
    } else {
        let previous_ordinal = block.header.block_ordinal.checked_sub(1).ok_or_else(|| {
            TokenEventDatabaseError::InvalidCheckpoint(
                "replayed block has no previous digest ordinal".into(),
            )
        })?;
        let value: Vec<u8> = connection.query_row(
            "SELECT chain_digest FROM blocks WHERE block_ordinal = ?1",
            [i64::from(previous_ordinal)],
            |row| row.get(0),
        )?;
        digest_from_blob(value, "previous block chain digest")?
    };
    let expected_chain = chained_block_digest(&previous, &stored_source, &durable);
    let stored_chain = digest_from_blob(stored_chain, "stored block chain digest")?;
    if expected_chain != stored_chain {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(format!(
            "digest chain changed for replayed block {}",
            block.header.block_ordinal
        )));
    }
    if block.header.block_ordinal.checked_add(1) == Some(next_block_ordinal)
        && &stored_chain != checkpoint_digest_head
    {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "the replayed final block differs from the checkpoint digest head".into(),
        ));
    }
    if block.header.block_ordinal.checked_add(1) == Some(next_block_ordinal)
        && &stored_tracker_digest != checkpoint_tracker_digest
    {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "the replayed final block differs from the checkpoint tracker digest".into(),
        ));
    }
    Ok(())
}

fn insert_block_header(
    transaction: &Transaction<'_>,
    block: BlockView<'_>,
    tracker_history: HistoryCoverage,
    tracker_revision: u64,
    tracker_digest: &[u8; DIGEST_BYTES],
    source_digest: &[u8; DIGEST_BYTES],
) -> Result<()> {
    let epoch = U64Sql::new(block.header.epoch);
    let slot = U64Sql::new(block.header.slot);
    let tracker_revision = U64Sql::new(tracker_revision);
    let transaction_count = u32::try_from(block.transactions.len()).map_err(|_| {
        TokenEventDatabaseError::InvalidBlock("block transaction count exceeds u32".into())
    })?;
    execute_cached(
        transaction,
        "INSERT INTO blocks (
            block_ordinal, epoch_le, epoch_text, slot_le, slot_text, transaction_count,
            tracker_history_after, tracker_revision_after_le,
            tracker_revision_after_text, tracker_digest_after, source_digest,
            durable_rows_digest, chain_digest
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            i64::from(block.header.block_ordinal),
            epoch.bytes.as_slice(),
            epoch.text,
            slot.bytes.as_slice(),
            slot.text,
            i64::from(transaction_count),
            history_text(tracker_history),
            tracker_revision.bytes.as_slice(),
            tracker_revision.text,
            tracker_digest.as_slice(),
            source_digest.as_slice(),
            EMPTY_DIGEST_HEAD.as_slice(),
            EMPTY_DIGEST_HEAD.as_slice(),
        ],
    )?;

    Ok(())
}

fn finalize_block_header(
    transaction: &Transaction<'_>,
    block_ordinal: u32,
    tracker_history: HistoryCoverage,
    tracker_revision: u64,
    tracker_digest: &[u8; DIGEST_BYTES],
) -> Result<()> {
    let revision = U64Sql::new(tracker_revision);
    let changed = execute_cached(
        transaction,
        "UPDATE blocks
            SET tracker_history_after = ?1,
                tracker_revision_after_le = ?2,
                tracker_revision_after_text = ?3,
                tracker_digest_after = ?4
          WHERE block_ordinal = ?5",
        params![
            history_text(tracker_history),
            revision.bytes.as_slice(),
            revision.text,
            tracker_digest.as_slice(),
            i64::from(block_ordinal),
        ],
    )?;
    if changed != 1 {
        return Err(TokenEventDatabaseError::InvalidCheckpoint(
            "the pending block header is absent during digest finalization".into(),
        ));
    }
    Ok(())
}

fn insert_transaction(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    source: TransactionView<'_>,
    tracked: &TrackedTokenTransaction,
) -> Result<()> {
    let (execution_status, status_reason) = execution_status_sql(source.header.status);
    let tracker_revision = U64Sql::new(tracked.certainty_revision_after);
    let signature = source.primary_signature.map(|value| value.as_slice());
    execute_cached(
        transaction,
        "INSERT INTO transactions (
            block_ordinal, tx_index, execution_status, status_reason,
            failed_outer_index, primary_signature, tracker_history_after,
            tracker_revision_after_le, tracker_revision_after_text
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            i64::from(source.block.block_ordinal),
            i64::from(source.header.tx_index),
            execution_status,
            status_reason,
            source.header.failed_outer_instruction_index.map(i64::from),
            signature,
            history_text(tracked.history_after),
            tracker_revision.bytes.as_slice(),
            tracker_revision.text,
        ],
    )?;

    stage_transaction_lifetimes(transaction, tracked)?;
    insert_account_updates(transaction, tracked)?;
    for (event_index, event) in tracked.events.iter().enumerate() {
        let event_index = u32::try_from(event_index)
            .map_err(|_| TokenEventDatabaseError::InvalidBlock("event index exceeds u32".into()))?;
        insert_event(transaction, source, event_index, event)?;
    }
    for (issue_index, issue) in tracked.coverage_issues.iter().enumerate() {
        let issue_index = u32::try_from(issue_index).map_err(|_| {
            TokenEventDatabaseError::InvalidBlock("coverage issue index exceeds u32".into())
        })?;
        insert_coverage_issue(transaction, source, issue_index, issue)?;
    }
    Ok(())
}

fn stage_transaction_lifetimes(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    tracked: &TrackedTokenTransaction,
) -> Result<()> {
    let final_updates = tracked
        .account_updates
        .iter()
        .map(|update| (update.account, update.state))
        .collect::<BTreeMap<_, _>>();
    for event in &tracked.events {
        for effect in &event.effects {
            if let TargetMintEffect::Lifecycle(change) = effect {
                if let Some(before) = change.before {
                    ensure_lifetime_exists(
                        transaction,
                        change.account,
                        before.generation,
                        "lifecycle before-state",
                    )?;
                }
                let state = final_updates
                    .get(&change.account)
                    .filter(|state| state.lifecycle.generation == change.after.generation)
                    .copied()
                    .unwrap_or(TargetAccountSnapshot {
                        lifecycle: change.after,
                        confirmed_revision: tracked.certainty_revision_after,
                    });
                upsert_lifetime(transaction, change.account, state)?;
            }
        }
    }
    for update in &tracked.account_updates {
        upsert_lifetime(transaction, update.account, update.state)?;
    }
    for event in &tracked.events {
        for effect in &event.effects {
            match effect {
                TargetMintEffect::Lifecycle(_) => {}
                TargetMintEffect::Transfer(transfer) => {
                    for leg in transfer.legs {
                        ensure_lifetime_exists(
                            transaction,
                            leg.account,
                            leg.generation,
                            "transfer delta leg",
                        )?;
                    }
                }
                TargetMintEffect::Mint {
                    account,
                    generation,
                    ..
                }
                | TargetMintEffect::Burn {
                    account,
                    generation,
                    ..
                } => ensure_lifetime_exists(
                    transaction,
                    *account,
                    *generation,
                    "mint or burn delta leg",
                )?,
            }
        }
    }
    Ok(())
}

fn ensure_lifetime_exists(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    account: PubkeyBytes,
    generation: u64,
    context: &str,
) -> Result<()> {
    let account_id = intern_pubkey(transaction, &account)?;
    let generation = U64Sql::new(generation);
    let exists: bool = {
        let mut statement = transaction.prepare_cached(
            "SELECT EXISTS(
            SELECT 1 FROM account_lifetimes
             WHERE pubkey_id = ?1 AND generation_le = ?2
         )",
        )?;
        statement.query_row(params![account_id, generation.bytes.as_slice()], |row| {
            row.get(0)
        })?
    };
    if !exists {
        return Err(TokenEventDatabaseError::InvalidBlock(format!(
            "{context} refers to an unknown account generation"
        )));
    }
    Ok(())
}

fn insert_account_updates(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    tracked: &TrackedTokenTransaction,
) -> Result<()> {
    for (update_index, update) in tracked.account_updates.iter().enumerate() {
        let update_index = u32::try_from(update_index).map_err(|_| {
            TokenEventDatabaseError::InvalidBlock("account update index exceeds u32".into())
        })?;
        let account_id = intern_pubkey(transaction, &update.account)?;
        let generation = U64Sql::new(update.state.lifecycle.generation);
        let revision = U64Sql::new(update.state.confirmed_revision);
        let (account_state, state_mint) = state_sql(transaction, update.state.lifecycle.state)?;
        execute_cached(
            transaction,
            "INSERT INTO tracker_account_updates (
                block_ordinal, tx_index, update_index, pubkey_id,
                generation_le, generation_text, account_state,
                state_mint_pubkey_id, confirmed_revision_le, confirmed_revision_text
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                i64::from(tracked.block.block_ordinal),
                i64::from(tracked.tx_index),
                i64::from(update_index),
                account_id,
                generation.bytes.as_slice(),
                generation.text,
                account_state,
                state_mint,
                revision.bytes.as_slice(),
                revision.text,
            ],
        )?;
    }
    Ok(())
}

fn insert_event(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    source: TransactionView<'_>,
    event_index: u32,
    event: &TrackedTokenEvent,
) -> Result<()> {
    let encoded = EncodedEvent::from_event(event);
    let program_id = intern_pubkey(transaction, &encoded.program_id)?;
    let embedded_a = encoded
        .embedded_a
        .map(|value| intern_pubkey(transaction, &value))
        .transpose()?;
    let embedded_b = encoded
        .embedded_b
        .map(|value| intern_pubkey(transaction, &value))
        .transpose()?;
    let amount = encoded.amount.map(U64Sql::new);
    let amount_bytes = amount.as_ref().map(|value| value.bytes.as_slice());
    let amount_text = amount.as_ref().map(|value| value.text.as_str());
    execute_cached(
        transaction,
        "INSERT INTO events (
            block_ordinal, tx_index, event_index, instruction_order,
            outer_index, inner_index, stack_height, batch_index,
            invocation_state, commit_state, program_pubkey_id, raw_kind,
            token_tag, data_coverage, data_coverage_reason, raw_data,
            trailing_data, amount_le, amount_text, decimals, required_signers,
            authority_type, embedded_pubkey_a, embedded_pubkey_b,
            optional_value_present, ui_amount
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
            ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26
         )",
        params![
            i64::from(source.block.block_ordinal),
            i64::from(source.header.tx_index),
            i64::from(event_index),
            i64::from(event.coordinate.order),
            i64::from(event.coordinate.outer_index),
            event.coordinate.inner_index.map(i64::from),
            event.coordinate.stack_height.map(i64::from),
            event.batch_index.map(i64::from),
            invocation_text(event.invocation),
            commit_text(event.commit),
            program_id,
            encoded.raw_kind,
            encoded.tag.map(i64::from),
            encoded.data_coverage,
            encoded.data_coverage_reason,
            encoded.raw_data.as_deref(),
            encoded.trailing_data.as_deref(),
            amount_bytes,
            amount_text,
            encoded.decimals.map(i64::from),
            encoded.required_signers.map(i64::from),
            encoded.authority_type,
            embedded_a,
            embedded_b,
            encoded.optional_value_present.map(i64::from),
            encoded.ui_amount,
        ],
    )?;
    let event_id = transaction.last_insert_rowid();

    insert_event_accounts(transaction, event_id, &event.raw)?;
    for (effect_index, effect) in event.effects.iter().enumerate() {
        let effect_index = u32::try_from(effect_index).map_err(|_| {
            TokenEventDatabaseError::InvalidBlock("event effect index exceeds u32".into())
        })?;
        insert_effect(transaction, event_id, effect_index, effect)?;
    }
    Ok(())
}

struct EncodedEvent {
    program_id: PubkeyBytes,
    raw_kind: &'static str,
    tag: Option<u8>,
    data_coverage: &'static str,
    data_coverage_reason: Option<&'static str>,
    raw_data: Option<Vec<u8>>,
    trailing_data: Option<Vec<u8>>,
    amount: Option<u64>,
    decimals: Option<u8>,
    required_signers: Option<u8>,
    authority_type: Option<&'static str>,
    embedded_a: Option<PubkeyBytes>,
    embedded_b: Option<PubkeyBytes>,
    optional_value_present: Option<bool>,
    ui_amount: Option<String>,
}

impl EncodedEvent {
    fn from_event(event: &TrackedTokenEvent) -> Self {
        match &event.raw {
            ObservedTokenInstruction::Unknown(raw) => {
                let (data_coverage, data_coverage_reason) =
                    instruction_data_coverage_sql(raw.data_coverage);
                Self {
                    program_id: raw.program_id,
                    raw_kind: "unknown",
                    tag: matches!(raw.data_coverage, InstructionDataCoverage::Exact)
                        .then(|| raw.data.first().copied())
                        .flatten(),
                    data_coverage,
                    data_coverage_reason,
                    raw_data: Some(raw.data.clone()),
                    trailing_data: None,
                    amount: None,
                    decimals: None,
                    required_signers: None,
                    authority_type: None,
                    embedded_a: None,
                    embedded_b: None,
                    optional_value_present: None,
                    ui_amount: None,
                }
            }
            ObservedTokenInstruction::Classic(decoded) => {
                let mut encoded = Self {
                    program_id: CLASSIC_SPL_TOKEN_PROGRAM_ID,
                    raw_kind: "classic",
                    tag: Some(decoded.instruction.tag()),
                    data_coverage: "exact",
                    data_coverage_reason: None,
                    raw_data: None,
                    trailing_data: Some(decoded.trailing_data.clone()),
                    amount: decoded.instruction.amount(),
                    decimals: decoded.instruction.decimals(),
                    required_signers: None,
                    authority_type: None,
                    embedded_a: None,
                    embedded_b: None,
                    optional_value_present: None,
                    ui_amount: None,
                };
                match &decoded.instruction {
                    ClassicTokenInstruction::InitializeMint {
                        mint_authority,
                        freeze_authority,
                        ..
                    }
                    | ClassicTokenInstruction::InitializeMint2 {
                        mint_authority,
                        freeze_authority,
                        ..
                    } => {
                        encoded.embedded_a = Some(*mint_authority);
                        encoded.embedded_b = *freeze_authority;
                        encoded.optional_value_present = Some(freeze_authority.is_some());
                    }
                    ClassicTokenInstruction::InitializeMultisig { required_signers }
                    | ClassicTokenInstruction::InitializeMultisig2 { required_signers } => {
                        encoded.required_signers = Some(*required_signers);
                    }
                    ClassicTokenInstruction::SetAuthority {
                        authority_type,
                        new_authority,
                    } => {
                        encoded.authority_type = Some(authority_type_text(*authority_type));
                        encoded.embedded_a = *new_authority;
                        encoded.optional_value_present = Some(new_authority.is_some());
                    }
                    ClassicTokenInstruction::InitializeAccount2 { owner }
                    | ClassicTokenInstruction::InitializeAccount3 { owner } => {
                        encoded.embedded_a = Some(*owner);
                    }
                    ClassicTokenInstruction::UiAmountToAmount { ui_amount } => {
                        encoded.ui_amount = Some(ui_amount.clone());
                    }
                    ClassicTokenInstruction::UnwrapLamports { amount } => {
                        encoded.amount = *amount;
                        encoded.optional_value_present = Some(amount.is_some());
                    }
                    _ => {}
                }
                encoded
            }
        }
    }
}

fn insert_event_accounts(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    event_id: i64,
    raw: &ObservedTokenInstruction,
) -> Result<()> {
    match raw {
        ObservedTokenInstruction::Classic(decoded) => {
            for (binding_index, binding) in decoded.roles.iter().enumerate() {
                let binding_index = u32::try_from(binding_index).map_err(|_| {
                    TokenEventDatabaseError::InvalidBlock(
                        "event account binding index exceeds u32".into(),
                    )
                })?;
                let pubkey_id = intern_pubkey(transaction, &binding.address)?;
                execute_cached(
                    transaction,
                    "INSERT INTO event_accounts (
                        event_id, binding_index, account_index, pubkey_id, semantic_role
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        event_id,
                        i64::from(binding_index),
                        i64::from(binding.account_index),
                        pubkey_id,
                        account_role_text(binding.role),
                    ],
                )?;
            }
        }
        ObservedTokenInstruction::Unknown(raw) => {
            for (account_index, account) in raw.accounts.iter().enumerate() {
                let account_index = u32::try_from(account_index).map_err(|_| {
                    TokenEventDatabaseError::InvalidBlock(
                        "raw event account index exceeds u32".into(),
                    )
                })?;
                let pubkey_id = intern_pubkey(transaction, account)?;
                execute_cached(
                    transaction,
                    "INSERT INTO event_accounts (
                        event_id, binding_index, account_index, pubkey_id, semantic_role
                     ) VALUES (?1, ?2, ?3, ?4, NULL)",
                    params![
                        event_id,
                        i64::from(account_index),
                        i64::from(account_index),
                        pubkey_id,
                    ],
                )?;
            }
        }
    }
    Ok(())
}

fn insert_effect(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    event_id: i64,
    effect_index: u32,
    effect: &TargetMintEffect,
) -> Result<()> {
    match effect {
        TargetMintEffect::Lifecycle(change) => {
            execute_cached(
                transaction,
                "INSERT INTO event_effects (
                    event_id, effect_index, effect_kind,
                    amount_le, amount_text, decimals, checked
                 ) VALUES (?1, ?2, 'lifecycle', NULL, NULL, NULL, NULL)",
                params![event_id, i64::from(effect_index)],
            )?;
            insert_lifecycle_effect(transaction, event_id, effect_index, change)?;
        }
        TargetMintEffect::Transfer(transfer) => {
            insert_amount_effect(
                transaction,
                event_id,
                effect_index,
                "transfer",
                transfer.amount,
                transfer.decimals,
                Some(transfer.checked),
            )?;
            for (leg_index, leg) in transfer.legs.iter().enumerate() {
                let leg_index = u32::try_from(leg_index).map_err(|_| {
                    TokenEventDatabaseError::InvalidBlock("delta leg index exceeds u32".into())
                })?;
                insert_delta_leg(
                    transaction,
                    event_id,
                    effect_index,
                    leg_index,
                    leg,
                    Some(leg.role),
                )?;
            }
        }
        TargetMintEffect::Mint {
            account,
            generation,
            amount,
            decimals,
        } => {
            insert_amount_effect(
                transaction,
                event_id,
                effect_index,
                "mint",
                *amount,
                *decimals,
                None,
            )?;
            insert_delta_leg(
                transaction,
                event_id,
                effect_index,
                0,
                &TransferLeg {
                    role: TransferLegRole::Destination,
                    account: *account,
                    generation: *generation,
                    direction: BalanceDirection::Credit,
                    amount: *amount,
                },
                None,
            )?;
        }
        TargetMintEffect::Burn {
            account,
            generation,
            amount,
            decimals,
        } => {
            insert_amount_effect(
                transaction,
                event_id,
                effect_index,
                "burn",
                *amount,
                *decimals,
                None,
            )?;
            insert_delta_leg(
                transaction,
                event_id,
                effect_index,
                0,
                &TransferLeg {
                    role: TransferLegRole::Source,
                    account: *account,
                    generation: *generation,
                    direction: BalanceDirection::Debit,
                    amount: *amount,
                },
                None,
            )?;
        }
    }
    Ok(())
}

fn insert_amount_effect(
    transaction: &Transaction<'_>,
    event_id: i64,
    effect_index: u32,
    kind: &str,
    amount: u64,
    decimals: Option<u8>,
    checked: Option<bool>,
) -> Result<()> {
    let amount = U64Sql::new(amount);
    execute_cached(
        transaction,
        "INSERT INTO event_effects (
            event_id, effect_index, effect_kind,
            amount_le, amount_text, decimals, checked
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            event_id,
            i64::from(effect_index),
            kind,
            amount.bytes.as_slice(),
            amount.text,
            decimals.map(i64::from),
            checked.map(i64::from),
        ],
    )?;
    Ok(())
}

fn insert_lifecycle_effect(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    event_id: i64,
    effect_index: u32,
    change: &AccountLifecycleChange,
) -> Result<()> {
    let account_id = intern_pubkey(transaction, &change.account)?;
    let before_generation = change.before.map(|before| U64Sql::new(before.generation));
    let (before_state, before_mint) = match change.before {
        Some(before) => {
            let (state, mint) = state_sql(transaction, before.state)?;
            (Some(state), mint)
        }
        None => (None, None),
    };
    let after_generation = U64Sql::new(change.after.generation);
    let (after_state, after_mint) = state_sql(transaction, change.after.state)?;
    execute_cached(
        transaction,
        "INSERT INTO lifecycle_effects (
            event_id, effect_index, account_pubkey_id,
            before_generation_le, before_generation_text, before_state,
            before_state_mint_pubkey_id, after_generation_le,
            after_generation_text, after_state, after_state_mint_pubkey_id, cause
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            event_id,
            i64::from(effect_index),
            account_id,
            before_generation
                .as_ref()
                .map(|value| value.bytes.as_slice()),
            before_generation.as_ref().map(|value| value.text.as_str()),
            before_state,
            before_mint,
            after_generation.bytes.as_slice(),
            after_generation.text,
            after_state,
            after_mint,
            lifecycle_cause_text(change.cause),
        ],
    )?;
    Ok(())
}

fn insert_delta_leg(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    event_id: i64,
    effect_index: u32,
    leg_index: u32,
    leg: &TransferLeg,
    transfer_role: Option<TransferLegRole>,
) -> Result<()> {
    let account_id = intern_pubkey(transaction, &leg.account)?;
    let generation = U64Sql::new(leg.generation);
    let amount = U64Sql::new(leg.amount);
    execute_cached(
        transaction,
        "INSERT INTO delta_legs (
            event_id, effect_index, leg_index, account_pubkey_id,
            generation_le, generation_text, direction, transfer_role,
            amount_le, amount_text
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            event_id,
            i64::from(effect_index),
            i64::from(leg_index),
            account_id,
            generation.bytes.as_slice(),
            generation.text,
            balance_direction_text(leg.direction),
            transfer_role.map(transfer_leg_role_text),
            amount.bytes.as_slice(),
            amount.text,
        ],
    )?;
    Ok(())
}

fn insert_coverage_issue(
    transaction: &PubkeyCachingTransaction<'_, '_>,
    source: TransactionView<'_>,
    issue_index: u32,
    issue: &TokenCoverageIssue,
) -> Result<()> {
    let fields = CoverageIssueFields::new(transaction, &issue.kind)?;
    let coordinate = issue.coordinate;
    execute_cached(
        transaction,
        "INSERT INTO coverage_issues (
            block_ordinal, tx_index, issue_index, instruction_order,
            outer_index, inner_index, stack_height, issue_kind, detail,
            data_coverage, coverage_reason, first_pubkey_id, second_pubkey_id,
            known_mint_pubkey_id, observed_mint_pubkey_id,
            expected_index, actual_index
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                   ?13, ?14, ?15, ?16, ?17)",
        params![
            i64::from(source.block.block_ordinal),
            i64::from(source.header.tx_index),
            i64::from(issue_index),
            coordinate.map(|value| i64::from(value.order)),
            coordinate.map(|value| i64::from(value.outer_index)),
            coordinate
                .and_then(|value| value.inner_index)
                .map(i64::from),
            coordinate
                .and_then(|value| value.stack_height)
                .map(i64::from),
            fields.kind,
            fields.detail,
            fields.data_coverage,
            fields.coverage_reason,
            fields.first_pubkey_id,
            fields.second_pubkey_id,
            fields.known_mint_pubkey_id,
            fields.observed_mint_pubkey_id,
            fields.expected_index.map(i64::from),
            fields.actual_index.map(i64::from),
        ],
    )?;
    Ok(())
}

struct CoverageIssueFields {
    kind: &'static str,
    detail: Option<String>,
    data_coverage: Option<&'static str>,
    coverage_reason: Option<&'static str>,
    first_pubkey_id: Option<i64>,
    second_pubkey_id: Option<i64>,
    known_mint_pubkey_id: Option<i64>,
    observed_mint_pubkey_id: Option<i64>,
    expected_index: Option<u32>,
    actual_index: Option<u32>,
}

impl CoverageIssueFields {
    fn empty(kind: &'static str) -> Self {
        Self {
            kind,
            detail: None,
            data_coverage: None,
            coverage_reason: None,
            first_pubkey_id: None,
            second_pubkey_id: None,
            known_mint_pubkey_id: None,
            observed_mint_pubkey_id: None,
            expected_index: None,
            actual_index: None,
        }
    }

    fn new(
        transaction: &PubkeyCachingTransaction<'_, '_>,
        issue: &TokenCoverageIssueKind,
    ) -> Result<Self> {
        Ok(match issue {
            TokenCoverageIssueKind::Decode(error) => {
                let mut fields = Self::empty("decode");
                fields.detail = Some(format!("{error:?}"));
                fields
            }
            TokenCoverageIssueKind::InstructionDataUnavailable(coverage) => {
                let mut fields = Self::empty("instruction-data-unavailable");
                let (coverage, reason) = instruction_data_coverage_sql(*coverage);
                fields.data_coverage = Some(coverage);
                fields.coverage_reason = reason;
                fields
            }
            TokenCoverageIssueKind::InsufficientHistory {
                first_account,
                second_account,
            } => {
                let mut fields = Self::empty("insufficient-history");
                fields.first_pubkey_id = Some(intern_pubkey(transaction, first_account)?);
                fields.second_pubkey_id = second_account
                    .map(|value| intern_pubkey(transaction, &value))
                    .transpose()?;
                fields
            }
            TokenCoverageIssueKind::ConflictingMintEvidence {
                account,
                known_mint,
                observed_mint,
            } => {
                let mut fields = Self::empty("conflicting-mint-evidence");
                fields.first_pubkey_id = Some(intern_pubkey(transaction, account)?);
                fields.known_mint_pubkey_id = Some(intern_pubkey(transaction, known_mint)?);
                fields.observed_mint_pubkey_id = Some(intern_pubkey(transaction, observed_mint)?);
                fields
            }
            TokenCoverageIssueKind::SyncNativeOnTargetAccount { account } => {
                let mut fields = Self::empty("sync-native-on-target");
                fields.first_pubkey_id = Some(intern_pubkey(transaction, account)?);
                fields
            }
            TokenCoverageIssueKind::InvalidInstructionOrder { expected, actual } => {
                let mut fields = Self::empty("invalid-instruction-order");
                fields.expected_index = Some(*expected);
                fields.actual_index = Some(*actual);
                fields
            }
            TokenCoverageIssueKind::IncompleteInstructions(reason) => {
                let mut fields = Self::empty("incomplete-instructions");
                fields.coverage_reason = Some(coverage_reason_text(*reason));
                fields
            }
            TokenCoverageIssueKind::IncompleteCpi(reason) => {
                let mut fields = Self::empty("incomplete-cpi");
                fields.coverage_reason = Some(coverage_reason_text(*reason));
                fields
            }
            TokenCoverageIssueKind::CpiNotRecorded => Self::empty("cpi-not-recorded"),
            TokenCoverageIssueKind::UnknownExecution(reason) => {
                let mut fields = Self::empty("unknown-execution");
                fields.coverage_reason = Some(coverage_reason_text(*reason));
                fields
            }
        })
    }
}

fn execution_status_sql(status: ExecutionStatus) -> (&'static str, Option<&'static str>) {
    match status {
        ExecutionStatus::Succeeded => ("succeeded", None),
        ExecutionStatus::Failed => ("failed", None),
        ExecutionStatus::Unknown(reason) => ("unknown", Some(coverage_reason_text(reason))),
    }
}

fn instruction_data_coverage_sql(
    coverage: InstructionDataCoverage,
) -> (&'static str, Option<&'static str>) {
    match coverage {
        InstructionDataCoverage::Exact => ("exact", None),
        InstructionDataCoverage::NotRequested => ("not-requested", None),
        InstructionDataCoverage::Unknown(reason) => ("unknown", Some(coverage_reason_text(reason))),
    }
}

fn coverage_reason_text(reason: CoverageReason) -> &'static str {
    match reason {
        CoverageReason::MetadataAbsent => "metadata-absent",
        CoverageReason::RawTransaction => "raw-transaction",
        CoverageReason::RawMetadata => "raw-metadata",
        CoverageReason::ProjectionNotRequested => "projection-not-requested",
        CoverageReason::InvalidReference => "invalid-reference",
        CoverageReason::AmbiguousInstructionData => "ambiguous-instruction-data",
        CoverageReason::InstructionDataUnavailable => "instruction-data-unavailable",
        CoverageReason::UnsupportedInstruction => "unsupported-instruction",
        CoverageReason::SourceUnverified => "source-unverified",
        CoverageReason::NonContiguousHistory => "non-contiguous-history",
        CoverageReason::Other => "other",
    }
}

fn invocation_text(value: TokenInvocationEvidence) -> &'static str {
    match value {
        TokenInvocationEvidence::Invoked => "invoked",
        TokenInvocationEvidence::NotInvoked => "not-invoked",
        TokenInvocationEvidence::Unknown => "unknown",
    }
}

fn commit_text(value: TokenCommitState) -> &'static str {
    match value {
        TokenCommitState::Committed => "committed",
        TokenCommitState::RolledBack => "rolled-back",
        TokenCommitState::NotCommitted => "not-committed",
        TokenCommitState::Unknown => "unknown",
    }
}

fn account_role_text(value: TokenAccountRole) -> &'static str {
    match value {
        TokenAccountRole::Mint => "mint",
        TokenAccountRole::TokenAccount => "token-account",
        TokenAccountRole::MultisigAccount => "multisig-account",
        TokenAccountRole::Source => "source",
        TokenAccountRole::Destination => "destination",
        TokenAccountRole::LamportDestination => "lamport-destination",
        TokenAccountRole::Owner => "owner",
        TokenAccountRole::Delegate => "delegate",
        TokenAccountRole::Authority => "authority",
        TokenAccountRole::AuthoritySubject => "authority-subject",
        TokenAccountRole::RentSysvar => "rent-sysvar",
        TokenAccountRole::MultisigSigner => "multisig-signer",
        TokenAccountRole::Additional => "additional",
    }
}

fn authority_type_text(value: TokenAuthorityType) -> &'static str {
    match value {
        TokenAuthorityType::MintTokens => "mint-tokens",
        TokenAuthorityType::FreezeAccount => "freeze-account",
        TokenAuthorityType::AccountOwner => "account-owner",
        TokenAuthorityType::CloseAccount => "close-account",
    }
}

fn lifecycle_cause_text(value: LifecycleCause) -> &'static str {
    match value {
        LifecycleCause::InitializeAccount => "initialize-account",
        LifecycleCause::ExplicitMintInstruction => "explicit-mint-instruction",
        LifecycleCause::CheckedTransfer => "checked-transfer",
        LifecycleCause::UncheckedTransfer => "unchecked-transfer",
        LifecycleCause::CloseAccount => "close-account",
    }
}

fn balance_direction_text(value: BalanceDirection) -> &'static str {
    match value {
        BalanceDirection::Debit => "debit",
        BalanceDirection::Credit => "credit",
    }
}

fn transfer_leg_role_text(value: TransferLegRole) -> &'static str {
    match value {
        TransferLegRole::Source => "source",
        TransferLegRole::Destination => "destination",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_state_connection(state: TrackerStateAfter) -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE tracker_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    history_coverage TEXT NOT NULL,
                    certainty_revision_le BLOB NOT NULL,
                    certainty_revision_text TEXT NOT NULL
                ) STRICT;",
            )
            .unwrap();
        let revision = U64Sql::new(state.certainty_revision);
        connection
            .execute(
                "INSERT INTO tracker_state (
                    singleton, history_coverage,
                    certainty_revision_le, certainty_revision_text
                 ) VALUES (1, ?1, ?2, ?3)",
                params![
                    history_text(state.history),
                    revision.bytes.as_slice(),
                    revision.text,
                ],
            )
            .unwrap();
        connection
    }

    #[test]
    fn tracker_state_compare_and_swap_rejects_stale_state_and_rolls_back() {
        let before = TrackerStateAfter {
            history: HistoryCoverage::Complete,
            certainty_revision: 0,
        };
        let after = TrackerStateAfter {
            history: HistoryCoverage::Partial,
            certainty_revision: 1,
        };
        let stale = TrackerStateAfter {
            history: HistoryCoverage::Partial,
            certainty_revision: 99,
        };
        let mut connection = tracker_state_connection(before);
        let committed = HashMap::new();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let transaction = PubkeyCachingTransaction::new(transaction, &committed);
        let stale_revision = U64Sql::new(stale.certainty_revision);
        transaction
            .execute(
                "UPDATE tracker_state
                    SET history_coverage = ?1,
                        certainty_revision_le = ?2,
                        certainty_revision_text = ?3
                  WHERE singleton = 1",
                params![
                    history_text(stale.history),
                    stale_revision.bytes.as_slice(),
                    stale_revision.text,
                ],
            )
            .unwrap();

        let error = apply_current_checkpoint(&transaction, &[], before, after).unwrap_err();
        assert!(matches!(
            error,
            TokenEventDatabaseError::InvalidCheckpoint(reason)
                if reason.contains("validated in-memory transition base")
        ));
        drop(transaction);

        assert_eq!(
            load_tracker_state(&connection).unwrap(),
            (before.history, 0)
        );
    }

    #[test]
    fn no_op_tracker_state_transition_does_not_execute_an_update() {
        let state = TrackerStateAfter {
            history: HistoryCoverage::Complete,
            certainty_revision: 7,
        };
        let mut connection = tracker_state_connection(state);
        connection
            .execute_batch(
                "CREATE TRIGGER reject_tracker_state_update
                 BEFORE UPDATE ON tracker_state
                 BEGIN
                     SELECT RAISE(ABORT, 'tracker_state update was not a no-op');
                 END;",
            )
            .unwrap();
        let committed = HashMap::new();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let transaction = PubkeyCachingTransaction::new(transaction, &committed);

        apply_current_checkpoint(&transaction, &[], state, state).unwrap();
        transaction.commit().unwrap();

        assert_eq!(load_tracker_state(&connection).unwrap(), (state.history, 7));
    }

    #[test]
    fn changed_tracker_state_transition_updates_the_validated_base() {
        let before = TrackerStateAfter {
            history: HistoryCoverage::Complete,
            certainty_revision: 0,
        };
        let after = TrackerStateAfter {
            history: HistoryCoverage::Partial,
            certainty_revision: 1,
        };
        let mut connection = tracker_state_connection(before);
        let committed = HashMap::new();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let transaction = PubkeyCachingTransaction::new(transaction, &committed);

        apply_current_checkpoint(&transaction, &[], before, after).unwrap();
        transaction.commit().unwrap();

        assert_eq!(load_tracker_state(&connection).unwrap(), (after.history, 1));
    }

    #[test]
    fn pubkey_cache_serves_committed_and_pending_hits_and_drops_rollback_state() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE pubkeys (
                    pubkey_id INTEGER PRIMARY KEY,
                    address BLOB NOT NULL UNIQUE CHECK (length(address) = 32)
                ) STRICT;",
            )
            .unwrap();
        let existing = [1; 32];
        connection
            .execute("INSERT INTO pubkeys (address) VALUES (?1)", [existing])
            .unwrap();
        let mut committed = load_pubkey_ids(&connection).unwrap();
        assert_eq!(committed.get(&existing), Some(&1));

        let rolled_back = [2; 32];
        {
            let transaction = connection.transaction().unwrap();
            let transaction = PubkeyCachingTransaction::new(transaction, &committed);
            assert_eq!(intern_pubkey(&transaction, &existing).unwrap(), 1);
            assert_eq!(intern_pubkey(&transaction, &rolled_back).unwrap(), 2);
            assert_eq!(intern_pubkey(&transaction, &rolled_back).unwrap(), 2);
            assert_eq!(transaction.cache_hits.get(), 2);
            assert_eq!(transaction.pending_hits.get(), 1);
            assert_eq!(transaction.sql_misses.get(), 1);
        }
        assert!(!committed.contains_key(&rolled_back));
        assert_eq!(load_pubkey_ids(&connection).unwrap(), committed);

        let transaction = connection.transaction().unwrap();
        let transaction = PubkeyCachingTransaction::new(transaction, &committed);
        assert_eq!(intern_pubkey(&transaction, &rolled_back).unwrap(), 2);
        let committed_transaction = transaction.commit().unwrap();
        assert_eq!(committed_transaction.sql_misses, 1);
        committed.extend(committed_transaction.pending);
        assert_eq!(load_pubkey_ids(&connection).unwrap(), committed);
    }
}

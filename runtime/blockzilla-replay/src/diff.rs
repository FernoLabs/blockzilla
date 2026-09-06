use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
    sync::{Arc, LazyLock},
};

use crate::AccountMap;

static EMPTY_ACCOUNT_DATA: LazyLock<Arc<Vec<u8>>> = LazyLock::new(|| Arc::new(Vec::new()));

/// Cheaply cloned account payload with copy-on-write mutation.
///
/// Replay snapshots are cloned at transaction and instruction boundaries. An
/// ordinary `Vec<u8>` clone copies the complete account even when execution
/// never writes it. `AccountData` shares the immutable payload and performs
/// that copy only when mutable access is requested.
///
/// Dereferencing targets `Vec<u8>` intentionally, rather than `[u8]`, so
/// existing runtime code can continue to use `resize`, `push`, `clear`, and
/// the normal indexing operations. Persisted formats still encode only the
/// contained bytes; the `Arc` is strictly an in-memory implementation detail.
#[derive(Debug, Clone)]
pub struct AccountData(Arc<Vec<u8>>);

impl AccountData {
    pub fn new(data: Vec<u8>) -> Self {
        if data.is_empty() {
            Self(Arc::clone(&EMPTY_ACCOUNT_DATA))
        } else {
            Self(Arc::new(data))
        }
    }

    /// Return true when two payloads share the same backing allocation.
    ///
    /// This is useful for hot-path diagnostics and does not affect equality,
    /// which remains byte-for-byte equality.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Replace the payload without first cloning bytes that will be discarded.
    ///
    /// A shared payload is replaced by a fresh allocation containing `data`;
    /// a uniquely owned payload reuses its existing capacity. This is the
    /// writeback path for a VM that returns the complete account buffer.
    pub fn set_from_slice(&mut self, data: &[u8]) {
        if self.as_slice() == data {
            return;
        }
        if let Some(owned) = Arc::get_mut(&mut self.0) {
            if owned.len() == data.len() {
                owned.copy_from_slice(data);
            } else {
                owned.clear();
                owned.extend_from_slice(data);
            }
        } else if data.is_empty() {
            self.0 = Arc::clone(&EMPTY_ACCOUNT_DATA);
        } else {
            self.0 = Arc::new(data.to_vec());
        }
    }

    /// Consume the wrapper without copying when it is uniquely owned.
    pub fn into_vec(self) -> Vec<u8> {
        Arc::try_unwrap(self.0).unwrap_or_else(|shared| (*shared).clone())
    }
}

impl Default for AccountData {
    fn default() -> Self {
        Self(Arc::clone(&EMPTY_ACCOUNT_DATA))
    }
}

impl Deref for AccountData {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AccountData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl AsRef<[u8]> for AccountData {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for AccountData {
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for AccountData {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.as_slice() == other.as_slice()
    }
}

impl Eq for AccountData {}

impl PartialEq<Vec<u8>> for AccountData {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<AccountData> for Vec<u8> {
    fn eq(&self, other: &AccountData) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl PartialEq<[u8]> for AccountData {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for AccountData {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for AccountData {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for AccountData {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == *other
    }
}

impl From<Vec<u8>> for AccountData {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl From<&[u8]> for AccountData {
    fn from(data: &[u8]) -> Self {
        Self::new(data.to_vec())
    }
}

impl From<AccountData> for Vec<u8> {
    fn from(data: AccountData) -> Self {
        data.into_vec()
    }
}

impl FromIterator<u8> for AccountData {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

/// Complete account state at one execution boundary.
///
/// "PDA" is intentionally absent: a PDA is not an account-state flag.  It can
/// only be labelled when derivation seeds/program context are known.  Replay
/// captures every writable account and lets a later enrichment layer attach
/// PDA provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSnapshot {
    pub lamports: u64,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: AccountData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffPolicy {
    /// Lamports remain part of runtime state, but may be omitted from the
    /// analytical diff stream requested by Horizon/Blockzilla.
    pub include_lamports: bool,
    /// Maximum combined before/after bytes retained across inline ranges for
    /// one account. Hashes and lengths are always retained.
    pub max_inline_data_bytes: usize,
}

impl Default for DiffPolicy {
    fn default() -> Self {
        Self {
            include_lamports: false,
            max_inline_data_bytes: 4 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueDiff<T> {
    pub before: Option<T>,
    pub after: Option<T>,
}

/// Inline storage for the common short changed byte run.
///
/// Longer runs spill to the heap without truncation; the inline capacity is
/// only an allocation policy and is not part of diff semantics.
pub type InlineDiffBytes = SmallVec<[u8; 8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRangeDiff {
    pub offset: usize,
    pub before: InlineDiffBytes,
    pub after: InlineDiffBytes,
}

/// Most account writes produce at least one range. Keeping the first range in
/// the account diff removes that list allocation while preserving unbounded
/// spill behavior for fragmented writes.
pub type InlineByteRangeDiffs = SmallVec<[ByteRangeDiff; 1]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDiff {
    pub before_len: Option<usize>,
    pub after_len: Option<usize>,
    pub before_sha256: Option<[u8; 32]>,
    pub after_sha256: Option<[u8; 32]>,
    pub ranges: InlineByteRangeDiffs,
    pub ranges_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDiff {
    pub pubkey: [u8; 32],
    pub created: bool,
    pub deleted: bool,
    pub lamports: Option<ValueDiff<u64>>,
    pub owner: Option<ValueDiff<[u8; 32]>>,
    pub executable: Option<ValueDiff<bool>>,
    pub rent_epoch: Option<ValueDiff<u64>>,
    pub data: Option<DataDiff>,
}

/// CPI paths are shallow in the common case but remain unbounded.
pub type InlineInstructionPath = SmallVec<[u16; 4]>;

/// Exact nested instruction identity. `instruction_path=[2, 0, 1]` means the
/// third top-level instruction, its first CPI, then that CPI's second child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffBoundary {
    pub slot: u64,
    pub transaction_index: u32,
    pub trace_index: u32,
    pub stack_height: u16,
    pub instruction_path: InlineInstructionPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffDisposition {
    /// Instruction returned successfully, but the containing transaction has
    /// not committed yet.
    Speculative,
    /// The containing transaction committed these mutations.
    Committed,
    /// The instruction ran, but its mutations were later rolled back.
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionDiff {
    pub boundary: DiffBoundary,
    pub program_id: [u8; 32],
    pub disposition: DiffDisposition,
    pub accounts: Vec<AccountDiff>,
}

impl InstructionDiff {
    pub fn capture(
        boundary: DiffBoundary,
        program_id: [u8; 32],
        disposition: DiffDisposition,
        before: &AccountMap,
        after: &AccountMap,
        policy: DiffPolicy,
    ) -> Self {
        Self {
            boundary,
            program_id,
            disposition,
            accounts: diff_account_sets(before, after, policy),
        }
    }
}

/// Instruction-local account preimages captured at the first writable access.
///
/// Runtime account metadata may contain the same pubkey more than once.  A
/// journal deliberately keeps only the first preimage: subsequent writes in
/// the same instruction must still diff against the instruction boundary, not
/// against an intermediate value.  Entries remain in a small inline buffer
/// while the instruction executes and are sorted only when the public diff is
/// materialized.
///
/// The journal owns only preimages.  Final account state is borrowed from the
/// transaction overlay in [`Self::finish`], avoiding a second account snapshot
/// collection.
#[derive(Debug, Default)]
pub(crate) struct AccountDiffJournal {
    entries: SmallVec<[AccountDiffJournalEntry; 8]>,
}

#[derive(Debug)]
struct AccountDiffJournalEntry {
    pubkey: [u8; 32],
    before: Option<AccountSnapshot>,
}

impl AccountDiffJournal {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record the account boundary preimage exactly once.
    ///
    /// `None` means the writable account did not exist before this
    /// instruction. That absence is retained so a later materialized account
    /// is reported as a creation.
    pub(crate) fn record_first_write(
        &mut self,
        pubkey: [u8; 32],
        before: Option<&AccountSnapshot>,
    ) {
        if self.entries.iter().any(|entry| entry.pubkey == pubkey) {
            return;
        }
        self.entries.push(AccountDiffJournalEntry {
            pubkey,
            before: before.cloned(),
        });
    }

    /// Materialize a canonical instruction diff from journaled preimages and
    /// borrowed final overlay state.
    pub(crate) fn finish<'a, F>(
        mut self,
        boundary: DiffBoundary,
        program_id: [u8; 32],
        disposition: DiffDisposition,
        policy: DiffPolicy,
        mut after: F,
    ) -> InstructionDiff
    where
        F: FnMut(&[u8; 32]) -> Option<&'a AccountSnapshot>,
    {
        self.entries
            .sort_unstable_by(|left, right| left.pubkey.cmp(&right.pubkey));
        let mut accounts = Vec::new();
        for entry in self.entries {
            if let Some(diff) = diff_account(
                entry.pubkey,
                entry.before.as_ref(),
                after(&entry.pubkey),
                policy,
            ) {
                accounts.push(diff);
            }
        }
        InstructionDiff {
            boundary,
            program_id,
            disposition,
            accounts,
        }
    }
}

pub fn diff_account_sets(
    before: &AccountMap,
    after: &AccountMap,
    policy: DiffPolicy,
) -> Vec<AccountDiff> {
    // AccountMap is unordered; materialize a sorted key union so the merge is
    // deterministic and still allocation-light relative to full snapshot clones.
    let mut keys = Vec::with_capacity(before.len().saturating_add(after.len()));
    keys.extend(before.keys().copied());
    keys.extend(after.keys().copied());
    keys.sort_unstable();
    keys.dedup();

    let mut diffs = Vec::new();
    for pubkey in keys {
        let before_account = before.get(&pubkey);
        let after_account = after.get(&pubkey);
        if let Some(diff) = diff_account(pubkey, before_account, after_account, policy) {
            diffs.push(diff);
        }
    }
    diffs
}

fn diff_account(
    pubkey: [u8; 32],
    before: Option<&AccountSnapshot>,
    after: Option<&AccountSnapshot>,
    policy: DiffPolicy,
) -> Option<AccountDiff> {
    before.or(after)?;
    let created = before.is_none();
    let deleted = after.is_none();

    let lamports = policy
        .include_lamports
        .then(|| optional_value_diff(before.map(|a| a.lamports), after.map(|a| a.lamports)))
        .flatten();
    let owner = optional_value_diff(before.map(|a| a.owner), after.map(|a| a.owner));
    let executable = optional_value_diff(before.map(|a| a.executable), after.map(|a| a.executable));
    let rent_epoch = optional_value_diff(before.map(|a| a.rent_epoch), after.map(|a| a.rent_epoch));
    let data = data_diff(before.map(|a| &a.data), after.map(|a| &a.data), policy);

    if !created
        && !deleted
        && lamports.is_none()
        && owner.is_none()
        && executable.is_none()
        && rent_epoch.is_none()
        && data.is_none()
    {
        return None;
    }
    Some(AccountDiff {
        pubkey,
        created,
        deleted,
        lamports,
        owner,
        executable,
        rent_epoch,
        data,
    })
}

fn optional_value_diff<T: Copy + Eq>(before: Option<T>, after: Option<T>) -> Option<ValueDiff<T>> {
    (before != after).then_some(ValueDiff { before, after })
}

fn data_diff(
    before: Option<&AccountData>,
    after: Option<&AccountData>,
    policy: DiffPolicy,
) -> Option<DataDiff> {
    if before == after {
        return None;
    }
    let before_bytes = before.map(|data| data.as_slice()).unwrap_or_default();
    let after_bytes = after.map(|data| data.as_slice()).unwrap_or_default();
    let (ranges, ranges_truncated) =
        changed_ranges(before_bytes, after_bytes, policy.max_inline_data_bytes);
    Some(DataDiff {
        before_len: before.map(|data| data.len()),
        after_len: after.map(|data| data.len()),
        before_sha256: before.map(|data| sha256(data)),
        after_sha256: after.map(|data| sha256(data)),
        ranges,
        ranges_truncated,
    })
}

fn changed_ranges(
    before: &[u8],
    after: &[u8],
    inline_budget: usize,
) -> (InlineByteRangeDiffs, bool) {
    let max_len = before.len().max(after.len());
    let mut cursor = 0;
    let mut retained_bytes = 0usize;
    let mut ranges = InlineByteRangeDiffs::new();
    let mut truncated = false;
    while cursor < max_len {
        if byte_at(before, cursor) == byte_at(after, cursor) {
            cursor += 1;
            continue;
        }
        let start = cursor;
        cursor += 1;
        while cursor < max_len && byte_at(before, cursor) != byte_at(after, cursor) {
            cursor += 1;
        }
        let before_range = start.min(before.len())..cursor.min(before.len());
        let after_range = start.min(after.len())..cursor.min(after.len());
        let range_bytes = before_range.len().saturating_add(after_range.len());
        if retained_bytes.saturating_add(range_bytes) > inline_budget {
            truncated = true;
            continue;
        }
        retained_bytes = retained_bytes.saturating_add(range_bytes);
        ranges.push(ByteRangeDiff {
            offset: start,
            before: InlineDiffBytes::from_slice(&before[before_range]),
            after: InlineDiffBytes::from_slice(&after[after_range]),
        });
    }
    (ranges, truncated)
}

fn byte_at(bytes: &[u8], index: usize) -> Option<u8> {
    bytes.get(index).copied()
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(pubkey: u8, data: &[u8]) -> ([u8; 32], AccountSnapshot) {
        (
            [pubkey; 32],
            AccountSnapshot {
                lamports: 10,
                owner: [9; 32],
                executable: false,
                rent_epoch: 0,
                data: data.to_vec().into(),
            },
        )
    }

    #[test]
    fn dirty_journal_matches_ordered_map_capture_and_keeps_first_preimage() {
        let boundary = DiffBoundary {
            slot: 7,
            transaction_index: 8,
            trace_index: 9,
            stack_height: 2,
            instruction_path: InlineInstructionPath::from_slice(&[3, 1]),
        };
        let policy = DiffPolicy {
            include_lamports: true,
            max_inline_data_bytes: 128,
        };
        let (deleted_key, deleted) = account(3, b"deleted");
        let (changed_key, changed_before) = account(2, b"before");
        let (_, wrong_intermediate) = account(2, b"intermediate");
        let (unchanged_key, unchanged) = account(4, b"same");
        let created_key = [1; 32];

        let mut changed_after = changed_before.clone();
        changed_after.lamports = 42;
        changed_after.data.set_from_slice(b"after!");
        let (_, created) = account(1, b"created");

        let before = AccountMap::from([
            (deleted_key, deleted.clone()),
            (changed_key, changed_before.clone()),
            (unchanged_key, unchanged.clone()),
        ]);
        let after = AccountMap::from([
            (created_key, created),
            (changed_key, changed_after),
            (unchanged_key, unchanged),
        ]);
        let expected = InstructionDiff::capture(
            boundary.clone(),
            [5; 32],
            DiffDisposition::Speculative,
            &before,
            &after,
            policy,
        );

        let mut journal = AccountDiffJournal::new();
        // Deliberately record in non-canonical order and repeat a key after an
        // intermediate write. The first boundary preimage must win.
        journal.record_first_write(changed_key, Some(&changed_before));
        journal.record_first_write(created_key, None);
        journal.record_first_write(changed_key, Some(&wrong_intermediate));
        journal.record_first_write(unchanged_key, before.get(&unchanged_key));
        journal.record_first_write(deleted_key, Some(&deleted));
        let actual = journal.finish(
            boundary,
            [5; 32],
            DiffDisposition::Speculative,
            policy,
            |pubkey| after.get(pubkey),
        );

        assert_eq!(actual, expected);
        assert_eq!(
            actual
                .accounts
                .iter()
                .map(|diff| diff.pubkey)
                .collect::<Vec<_>>(),
            vec![created_key, changed_key, deleted_key]
        );
        assert!(actual.accounts[0].created);
        assert!(actual.accounts[2].deleted);
    }

    #[test]
    fn inline_diff_collections_spill_without_changing_large_ranges_or_paths() {
        let key = [7; 32];
        let before_bytes = (0_u8..96).collect::<Vec<_>>();
        let after_bytes = before_bytes
            .iter()
            .map(|byte| byte ^ 0xff)
            .collect::<Vec<_>>();
        let before = AccountMap::from([(
            key,
            AccountSnapshot {
                lamports: 1,
                owner: [8; 32],
                executable: false,
                rent_epoch: 0,
                data: before_bytes.clone().into(),
            },
        )]);
        let after = AccountMap::from([(
            key,
            AccountSnapshot {
                data: after_bytes.clone().into(),
                ..before[&key].clone()
            },
        )]);
        let path = [0, 1, 2, 3, 4, 5];
        let diff = InstructionDiff::capture(
            DiffBoundary {
                slot: 1,
                transaction_index: 2,
                trace_index: 3,
                stack_height: path.len() as u16,
                instruction_path: InlineInstructionPath::from_slice(&path),
            },
            [9; 32],
            DiffDisposition::Speculative,
            &before,
            &after,
            DiffPolicy {
                include_lamports: false,
                max_inline_data_bytes: usize::MAX,
            },
        );

        assert!(diff.boundary.instruction_path.spilled());
        let data = diff.accounts[0].data.as_ref().expect("data changed");
        assert!(!data.ranges.spilled());
        assert_eq!(data.ranges.len(), 1);
        assert!(data.ranges[0].before.spilled());
        assert!(data.ranges[0].after.spilled());
        assert_eq!(data.ranges[0].before.as_slice(), before_bytes);
        assert_eq!(data.ranges[0].after.as_slice(), after_bytes);

        let fragmented_before = [0_u8; 8];
        let fragmented_after = [1_u8, 0, 1, 0, 1, 0, 1, 0];
        let (ranges, truncated) = changed_ranges(&fragmented_before, &fragmented_after, usize::MAX);
        assert!(!truncated);
        assert!(ranges.spilled());
        assert_eq!(
            ranges.iter().map(|range| range.offset).collect::<Vec<_>>(),
            [0, 2, 4, 6]
        );
    }

    #[test]
    fn clone_shares_payload_until_first_mutation() {
        let original = AccountData::from(vec![1, 2, 3, 4]);
        let mut clone = original.clone();

        assert!(original.shares_allocation_with(&clone));
        assert_eq!(original, clone);

        clone[1] = 9;

        assert!(!original.shares_allocation_with(&clone));
        assert_eq!(original, vec![1, 2, 3, 4]);
        assert_eq!(clone, [1, 9, 3, 4]);
        assert_ne!(original, clone);

        let equal_distinct_allocation = AccountData::from(vec![1, 2, 3, 4]);
        assert!(!original.shares_allocation_with(&equal_distinct_allocation));
        assert_eq!(original, equal_distinct_allocation);
    }

    #[test]
    fn empty_payloads_share_one_process_wide_allocation() {
        let default = AccountData::default();
        let from_vec = AccountData::from(Vec::new());
        let from_slice = AccountData::from(&[][..]);

        assert!(default.shares_allocation_with(&from_vec));
        assert!(default.shares_allocation_with(&from_slice));
    }

    #[test]
    fn full_writeback_reuses_unique_data_and_replaces_shared_data() {
        let mut unique = AccountData::from(vec![1, 2, 3]);
        unique.set_from_slice(&[4, 5, 6]);
        assert_eq!(unique, [4, 5, 6]);

        let original = AccountData::from(vec![1, 2, 3]);
        let mut shared = original.clone();
        shared.set_from_slice(&[7, 8, 9]);
        assert_eq!(original, [1, 2, 3]);
        assert_eq!(shared, [7, 8, 9]);
        assert!(!original.shares_allocation_with(&shared));
    }

    #[test]
    fn captures_general_data_diff_but_omits_balance_by_default() {
        let (pubkey, before_account) = account(1, &[0, 1, 2, 3, 4, 5]);
        let mut after_account = before_account.clone();
        after_account.data = vec![0, 8, 9, 3, 4, 7, 6].into();
        after_account.lamports = 11;
        after_account.owner = [8; 32];
        after_account.executable = true;
        let before = AccountMap::from([(pubkey, before_account)]);
        let after = AccountMap::from([(pubkey, after_account)]);
        let diffs = diff_account_sets(&before, &after, DiffPolicy::default());
        assert_eq!(diffs.len(), 1);
        let diff = &diffs[0];
        assert_eq!(diff.lamports, None);
        assert!(diff.owner.is_some());
        assert!(diff.executable.is_some());
        let data = diff.data.as_ref().unwrap();
        assert_eq!(data.before_len, Some(6));
        assert_eq!(data.after_len, Some(7));
        assert_eq!(data.ranges.len(), 2);
        assert_eq!(data.ranges[0].offset, 1);
        assert_eq!(data.ranges[0].before.as_slice(), [1, 2]);
        assert_eq!(data.ranges[0].after.as_slice(), [8, 9]);
        assert_eq!(data.ranges[1].offset, 5);
        assert_eq!(data.ranges[1].before.as_slice(), [5]);
        assert_eq!(data.ranges[1].after.as_slice(), [7, 6]);
    }

    #[test]
    fn marks_creation_and_deletion_even_without_balance_output() {
        let (created_pubkey, created) = account(1, &[1]);
        let (deleted_pubkey, deleted) = account(2, &[2]);
        let before = AccountMap::from([(deleted_pubkey, deleted)]);
        let after = AccountMap::from([(created_pubkey, created)]);
        let diffs = diff_account_sets(&before, &after, DiffPolicy::default());
        assert_eq!(diffs.len(), 2);
        assert!(diffs.iter().any(|diff| diff.created && !diff.deleted));
        assert!(diffs.iter().any(|diff| diff.deleted && !diff.created));
    }

    #[test]
    fn keeps_hashes_when_inline_ranges_exceed_budget() {
        let (pubkey, before_account) = account(1, &[0; 64]);
        let (_, after_account) = account(1, &[1; 64]);
        let before = AccountMap::from([(pubkey, before_account)]);
        let after = AccountMap::from([(pubkey, after_account)]);
        let policy = DiffPolicy {
            max_inline_data_bytes: 16,
            ..DiffPolicy::default()
        };
        let diff = &diff_account_sets(&before, &after, policy)[0];
        let data = diff.data.as_ref().unwrap();
        assert!(data.ranges.is_empty());
        assert!(data.ranges_truncated);
        assert_ne!(data.before_sha256, data.after_sha256);
    }

    #[test]
    fn instruction_boundary_preserves_nested_cpi_identity() {
        let boundary = DiffBoundary {
            slot: 7,
            transaction_index: 3,
            trace_index: 5,
            stack_height: 3,
            instruction_path: InlineInstructionPath::from_slice(&[2, 0, 1]),
        };
        let diff = InstructionDiff::capture(
            boundary.clone(),
            [4; 32],
            DiffDisposition::Speculative,
            &AccountMap::new(),
            &AccountMap::new(),
            DiffPolicy::default(),
        );
        assert_eq!(diff.boundary, boundary);
        assert!(diff.accounts.is_empty());
    }

    #[test]
    fn balance_diff_is_opt_in() {
        let (pubkey, before_account) = account(1, &[]);
        let mut after_account = before_account.clone();
        after_account.lamports = 99;
        let before = AccountMap::from([(pubkey, before_account)]);
        let after = AccountMap::from([(pubkey, after_account)]);
        let policy = DiffPolicy {
            include_lamports: true,
            ..DiffPolicy::default()
        };
        let diff = &diff_account_sets(&before, &after, policy)[0];
        assert_eq!(
            diff.lamports,
            Some(ValueDiff {
                before: Some(10),
                after: Some(99)
            })
        );
    }
}

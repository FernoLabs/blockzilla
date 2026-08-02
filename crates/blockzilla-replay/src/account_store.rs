//! Canonical account storage for replay.
//!
//! The execution hot path is deliberately in-process. Transaction-local maps
//! remain small `BTreeMap` overlays; this module owns the much larger canonical
//! state and publishes an overlay through one validated batch. Hash-table
//! iteration order is never observable: hashes and future checkpoints visit
//! accounts in lexicographic pubkey order.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    ops::Index,
};

use hashbrown::HashMap;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::AccountSnapshot;

pub type AccountPubkey = [u8; 32];

/// Backend contract used by canonical replay state.
///
/// `apply_batch` must be all-or-nothing. A durable backend may stage a batch in
/// a journal before publishing it, while the memory backend validates every
/// operation before mutating its hash table.
pub trait AccountStore {
    fn get(&self, pubkey: &AccountPubkey) -> Option<&AccountSnapshot>;
    fn len(&self) -> usize;
    fn apply_batch(
        &mut self,
        batch: AccountWriteBatch,
    ) -> Result<AccountBatchCommit, AccountStoreError>;
    fn visit_sorted(&self, visitor: &mut dyn FnMut(AccountPubkey, &AccountSnapshot));

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_key(&self, pubkey: &AccountPubkey) -> bool {
        self.get(pubkey).is_some()
    }
}

/// The first replay backend: one writer and an in-process hash index.
///
/// The map layout is an implementation detail and is never serialized. This
/// keeps lookup latency low while preserving deterministic external output via
/// [`AccountStore::visit_sorted`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryAccountStore {
    accounts: HashMap<AccountPubkey, AccountSnapshot>,
}

impl MemoryAccountStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            accounts: HashMap::with_capacity(capacity),
        }
    }

    pub fn get(&self, pubkey: &AccountPubkey) -> Option<&AccountSnapshot> {
        self.accounts.get(pubkey)
    }

    /// Mutable access for runtime-owned updates that have already completed
    /// their transaction-level validation.
    pub fn get_mut(&mut self, pubkey: &AccountPubkey) -> Option<&mut AccountSnapshot> {
        self.accounts.get_mut(pubkey)
    }

    /// One-probe access for runtime-owned hot paths that must distinguish an
    /// existing account from a vacant key before updating canonical state.
    pub(crate) fn entry(
        &mut self,
        pubkey: AccountPubkey,
    ) -> hashbrown::hash_map::Entry<'_, AccountPubkey, AccountSnapshot, hashbrown::DefaultHashBuilder>
    {
        self.accounts.entry(pubkey)
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    pub fn contains_key(&self, pubkey: &AccountPubkey) -> bool {
        self.accounts.contains_key(pubkey)
    }

    /// Direct insertion is reserved for initialization and Bank-owned state.
    /// Transaction execution should publish through [`Self::apply_batch`].
    pub fn insert(
        &mut self,
        pubkey: AccountPubkey,
        account: AccountSnapshot,
    ) -> Option<AccountSnapshot> {
        self.accounts.insert(pubkey, account)
    }

    pub fn remove(&mut self, pubkey: &AccountPubkey) -> Option<AccountSnapshot> {
        self.accounts.remove(pubkey)
    }

    pub fn apply_batch(
        &mut self,
        batch: AccountWriteBatch,
    ) -> Result<AccountBatchCommit, AccountStoreError> {
        <Self as AccountStore>::apply_batch(self, batch)
    }

    pub fn visit_sorted(&self, visitor: &mut dyn FnMut(AccountPubkey, &AccountSnapshot)) {
        <Self as AccountStore>::visit_sorted(self, visitor);
    }

    /// Stable account-state hash used to prove backend and checkpoint parity.
    pub fn canonical_hash(&self) -> [u8; 32] {
        canonical_account_state_hash(self)
    }
}

impl AccountStore for MemoryAccountStore {
    fn get(&self, pubkey: &AccountPubkey) -> Option<&AccountSnapshot> {
        self.accounts.get(pubkey)
    }

    fn len(&self) -> usize {
        self.accounts.len()
    }

    fn apply_batch(
        &mut self,
        batch: AccountWriteBatch,
    ) -> Result<AccountBatchCommit, AccountStoreError> {
        // Validate every fallible operation first. No mutation occurs unless
        // the complete batch can be applied.
        for (pubkey, write) in &batch.writes {
            if let AccountWrite::PatchData {
                expected_data_len,
                patches,
            } = write
            {
                let account = self
                    .accounts
                    .get(pubkey)
                    .ok_or(AccountStoreError::MissingPatchAccount { pubkey: *pubkey })?;
                if account.data.len() != *expected_data_len {
                    return Err(AccountStoreError::DataLengthMismatch {
                        pubkey: *pubkey,
                        expected: *expected_data_len,
                        found: account.data.len(),
                    });
                }
                validate_patches(*pubkey, patches, *expected_data_len)?;
            }
        }

        let mut commit = AccountBatchCommit::default();
        for (pubkey, write) in batch.writes {
            match write {
                AccountWrite::Put(account) => {
                    if self.accounts.insert(pubkey, account).is_some() {
                        commit.updated += 1;
                    } else {
                        commit.inserted += 1;
                    }
                }
                AccountWrite::Delete => {
                    if self.accounts.remove(&pubkey).is_some() {
                        commit.deleted += 1;
                    }
                }
                AccountWrite::PatchData { patches, .. } => {
                    let account = self
                        .accounts
                        .get_mut(&pubkey)
                        .expect("patch account was validated before batch publication");
                    for patch in patches {
                        let end = patch.offset + patch.bytes.len();
                        account.data[patch.offset..end].copy_from_slice(&patch.bytes);
                    }
                    commit.patched += 1;
                }
            }
        }
        Ok(commit)
    }

    fn visit_sorted(&self, visitor: &mut dyn FnMut(AccountPubkey, &AccountSnapshot)) {
        let mut keys = self.accounts.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();
        for pubkey in keys {
            visitor(
                pubkey,
                self.accounts
                    .get(&pubkey)
                    .expect("sorted key came from the account table"),
            );
        }
    }
}

impl Index<&AccountPubkey> for MemoryAccountStore {
    type Output = AccountSnapshot;

    fn index(&self, pubkey: &AccountPubkey) -> &Self::Output {
        &self.accounts[pubkey]
    }
}

impl FromIterator<(AccountPubkey, AccountSnapshot)> for MemoryAccountStore {
    fn from_iter<T: IntoIterator<Item = (AccountPubkey, AccountSnapshot)>>(iter: T) -> Self {
        Self {
            accounts: iter.into_iter().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountDataPatch {
    pub offset: usize,
    pub bytes: Vec<u8>,
}

impl AccountDataPatch {
    pub fn new(offset: usize, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            bytes: bytes.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountWrite {
    Put(AccountSnapshot),
    Delete,
    /// Modify only these ranges of an existing account's data.
    ///
    /// This avoids rewriting the 131,097-byte launch SlotHistory account for
    /// every eight-byte word change. A future append log can persist the same
    /// operation as a compact patch record.
    PatchData {
        expected_data_len: usize,
        patches: Vec<AccountDataPatch>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountWriteBatch {
    writes: BTreeMap<AccountPubkey, AccountWrite>,
}

impl AccountWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.writes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub fn put(
        &mut self,
        pubkey: AccountPubkey,
        account: AccountSnapshot,
    ) -> Result<(), AccountStoreError> {
        self.add(pubkey, AccountWrite::Put(account))
    }

    pub fn delete(&mut self, pubkey: AccountPubkey) -> Result<(), AccountStoreError> {
        self.add(pubkey, AccountWrite::Delete)
    }

    pub fn patch_data(
        &mut self,
        pubkey: AccountPubkey,
        expected_data_len: usize,
        mut patches: Vec<AccountDataPatch>,
    ) -> Result<(), AccountStoreError> {
        patches.sort_unstable_by_key(|patch| patch.offset);
        validate_patches(pubkey, &patches, expected_data_len)?;
        self.add(
            pubkey,
            AccountWrite::PatchData {
                expected_data_len,
                patches,
            },
        )
    }

    fn add(&mut self, pubkey: AccountPubkey, write: AccountWrite) -> Result<(), AccountStoreError> {
        match self.writes.entry(pubkey) {
            Entry::Vacant(entry) => {
                entry.insert(write);
                Ok(())
            }
            Entry::Occupied(_) => Err(AccountStoreError::DuplicateWrite { pubkey }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountBatchCommit {
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
    pub patched: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AccountStoreError {
    #[error("account batch contains more than one write for {pubkey:?}")]
    DuplicateWrite { pubkey: AccountPubkey },
    #[error("account data patch targets absent account {pubkey:?}")]
    MissingPatchAccount { pubkey: AccountPubkey },
    #[error(
        "account {pubkey:?} data length changed before patch: expected {expected}, found {found}"
    )]
    DataLengthMismatch {
        pubkey: AccountPubkey,
        expected: usize,
        found: usize,
    },
    #[error(
        "account {pubkey:?} data patch [{offset}, {end}) exceeds expected data length {data_len}"
    )]
    PatchOutOfBounds {
        pubkey: AccountPubkey,
        offset: usize,
        end: usize,
        data_len: usize,
    },
    #[error("account {pubkey:?} data patches overlap at byte {offset}")]
    OverlappingPatches {
        pubkey: AccountPubkey,
        offset: usize,
    },
}

fn validate_patches(
    pubkey: AccountPubkey,
    patches: &[AccountDataPatch],
    data_len: usize,
) -> Result<(), AccountStoreError> {
    let mut previous_end = 0;
    for (index, patch) in patches.iter().enumerate() {
        let end = patch.offset.checked_add(patch.bytes.len()).ok_or(
            AccountStoreError::PatchOutOfBounds {
                pubkey,
                offset: patch.offset,
                end: usize::MAX,
                data_len,
            },
        )?;
        if end > data_len {
            return Err(AccountStoreError::PatchOutOfBounds {
                pubkey,
                offset: patch.offset,
                end,
                data_len,
            });
        }
        if index != 0 && patch.offset < previous_end {
            return Err(AccountStoreError::OverlappingPatches {
                pubkey,
                offset: patch.offset,
            });
        }
        previous_end = end;
    }
    Ok(())
}

pub fn canonical_account_state_hash(store: &impl AccountStore) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla/replay-account-state/v1\0");
    hasher.update((store.len() as u64).to_le_bytes());
    store.visit_sorted(&mut |pubkey, account| {
        hasher.update(pubkey);
        hasher.update(account.lamports.to_le_bytes());
        hasher.update(account.owner);
        hasher.update([u8::from(account.executable)]);
        hasher.update(account.rent_epoch.to_le_bytes());
        hasher.update((account.data.len() as u64).to_le_bytes());
        hasher.update(&account.data);
    });
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(seed: u8, data_len: usize) -> AccountSnapshot {
        AccountSnapshot {
            lamports: u64::from(seed),
            owner: [seed.wrapping_add(1); 32],
            executable: seed.is_multiple_of(2),
            rent_epoch: u64::from(seed.wrapping_add(2)),
            data: vec![seed; data_len].into(),
        }
    }

    #[test]
    fn batch_put_update_delete_is_atomic() {
        let key_a = [1; 32];
        let key_b = [2; 32];
        let missing = [3; 32];
        let mut store = MemoryAccountStore::new();
        store.insert(key_a, account(1, 4));

        let mut invalid = AccountWriteBatch::new();
        invalid.put(key_b, account(2, 4)).unwrap();
        invalid
            .patch_data(missing, 4, vec![AccountDataPatch::new(0, [9])])
            .unwrap();
        assert_eq!(
            store.apply_batch(invalid),
            Err(AccountStoreError::MissingPatchAccount { pubkey: missing })
        );
        assert!(!store.contains_key(&key_b));
        assert_eq!(store.len(), 1);

        let mut valid = AccountWriteBatch::new();
        valid.put(key_b, account(2, 4)).unwrap();
        valid.delete(key_a).unwrap();
        let commit = store.apply_batch(valid).unwrap();
        assert_eq!(commit.inserted, 1);
        assert_eq!(commit.deleted, 1);
        assert!(!store.contains_key(&key_a));
        assert_eq!(store[&key_b], account(2, 4));
    }

    #[test]
    fn patch_updates_only_declared_ranges() {
        let key = [4; 32];
        let mut store = MemoryAccountStore::new();
        store.insert(key, account(0, 16));
        let mut batch = AccountWriteBatch::new();
        batch
            .patch_data(
                key,
                16,
                vec![
                    AccountDataPatch::new(12, [7, 8]),
                    AccountDataPatch::new(2, [5, 6]),
                ],
            )
            .unwrap();
        let commit = store.apply_batch(batch).unwrap();
        assert_eq!(commit.patched, 1);
        assert_eq!(
            store[&key].data,
            [0, 0, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 7, 8, 0, 0]
        );
    }

    #[test]
    fn patch_validation_rejects_bounds_overlap_and_stale_length() {
        let key = [8; 32];

        let mut out_of_bounds = AccountWriteBatch::new();
        assert_eq!(
            out_of_bounds.patch_data(key, 4, vec![AccountDataPatch::new(3, [1, 2])]),
            Err(AccountStoreError::PatchOutOfBounds {
                pubkey: key,
                offset: 3,
                end: 5,
                data_len: 4,
            })
        );

        let mut overlapping = AccountWriteBatch::new();
        assert_eq!(
            overlapping.patch_data(
                key,
                4,
                vec![
                    AccountDataPatch::new(0, [1, 2, 3]),
                    AccountDataPatch::new(2, [4]),
                ],
            ),
            Err(AccountStoreError::OverlappingPatches {
                pubkey: key,
                offset: 2,
            })
        );

        let original = account(4, 4);
        let mut store = MemoryAccountStore::new();
        store.insert(key, original.clone());
        let mut stale = AccountWriteBatch::new();
        stale
            .patch_data(key, 5, vec![AccountDataPatch::new(0, [9])])
            .unwrap();
        assert_eq!(
            store.apply_batch(stale),
            Err(AccountStoreError::DataLengthMismatch {
                pubkey: key,
                expected: 5,
                found: 4,
            })
        );
        assert_eq!(store.get(&key), Some(&original));
    }

    #[test]
    fn duplicate_batch_keys_are_rejected() {
        let key = [5; 32];
        let mut batch = AccountWriteBatch::new();
        let first = account(1, 0);
        batch.put(key, first.clone()).unwrap();
        assert_eq!(
            batch.delete(key),
            Err(AccountStoreError::DuplicateWrite { pubkey: key })
        );
        let mut store = MemoryAccountStore::new();
        store.apply_batch(batch).unwrap();
        assert_eq!(store.get(&key), Some(&first));
    }

    #[test]
    fn canonical_hash_ignores_hash_table_insertion_order() {
        let keys = [[9; 32], [1; 32], [7; 32], [3; 32]];
        let mut forward = MemoryAccountStore::new();
        let mut reverse = MemoryAccountStore::new();
        for (index, key) in keys.iter().enumerate() {
            forward.insert(*key, account(index as u8, index));
        }
        for (index, key) in keys.iter().enumerate().rev() {
            reverse.insert(*key, account(index as u8, index));
        }
        assert_eq!(forward.canonical_hash(), reverse.canonical_hash());
    }

    #[test]
    fn zero_lamport_account_can_remain_present() {
        let key = [6; 32];
        let mut zero = account(1, 1);
        zero.lamports = 0;
        let mut store = MemoryAccountStore::new();
        store.insert(key, zero.clone());
        assert_eq!(store.get(&key), Some(&zero));
    }

    #[test]
    fn get_mut_updates_the_existing_account_in_place() {
        let key = [10; 32];
        let missing = [11; 32];
        let mut store = MemoryAccountStore::new();
        store.insert(key, account(4, 8));
        let data_pointer = store.get(&key).unwrap().data.as_ptr();

        let existing = store.get_mut(&key).unwrap();
        existing.lamports = 99;
        existing.data[3] = 42;

        assert_eq!(store.len(), 1);
        assert_eq!(store.get(&key).unwrap().lamports, 99);
        assert_eq!(store.get(&key).unwrap().data[3], 42);
        assert_eq!(store.get(&key).unwrap().data.as_ptr(), data_pointer);
        assert!(store.get_mut(&missing).is_none());
    }
}

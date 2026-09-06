//! Borrowed account geometry shared by canonical readers and writers.

use super::transactions::{LoadedAddresses, Message, MessageHeader, PubkeyId, Transaction};

/// The resolved runtime account order for one transaction.
///
/// Known IDs are always static, loaded writable, then loaded readonly. When a
/// V0 source did not retain loaded pubkeys, `resolved_len` still includes the
/// exact width declared by its lookup descriptors, but `get` returns `None`
/// for those unknown positions. The transaction and its header must already
/// satisfy the canonical transaction validation rules.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedAccounts<'a> {
    static_accounts: &'a [PubkeyId],
    loaded_writable: &'a [PubkeyId],
    loaded_readonly: &'a [PubkeyId],
    resolved_len: usize,
    complete: bool,
}

impl<'a> ResolvedAccounts<'a> {
    pub fn new(transaction: &'a Transaction) -> Self {
        match &transaction.message {
            Message::Legacy {
                static_accounts, ..
            } => Self {
                static_accounts,
                loaded_writable: &[],
                loaded_readonly: &[],
                resolved_len: static_accounts.len(),
                complete: true,
            },
            Message::V0 {
                static_accounts,
                loaded_addresses,
                lookups,
                ..
            } => match loaded_addresses {
                LoadedAddresses::Source { writable, readonly }
                | LoadedAddresses::Backfilled { writable, readonly } => Self {
                    static_accounts,
                    loaded_writable: writable,
                    loaded_readonly: readonly,
                    resolved_len: static_accounts.len() + writable.len() + readonly.len(),
                    complete: true,
                },
                LoadedAddresses::Unavailable => {
                    let (writable, readonly) =
                        lookups
                            .iter()
                            .fold((0_usize, 0_usize), |(writable, readonly), lookup| {
                                (
                                    writable + lookup.writable_indexes.len(),
                                    readonly + lookup.readonly_indexes.len(),
                                )
                            });
                    Self {
                        static_accounts,
                        loaded_writable: &[],
                        loaded_readonly: &[],
                        resolved_len: static_accounts.len() + writable + readonly,
                        complete: false,
                    }
                }
            },
        }
    }

    pub fn static_len(self) -> usize {
        self.static_accounts.len()
    }

    pub fn loaded_writable_len(self) -> usize {
        self.loaded_writable.len()
    }

    pub fn resolved_len(self) -> usize {
        self.resolved_len
    }

    pub fn is_complete(self) -> bool {
        self.complete
    }

    pub fn get(self, position: usize) -> Option<u32> {
        self.iter().nth(position)
    }

    pub fn iter(self) -> impl Iterator<Item = u32> + 'a {
        self.static_accounts
            .iter()
            .chain(self.loaded_writable)
            .chain(self.loaded_readonly)
            .map(|id| id.0)
    }

    pub fn positional_roles(self, header: MessageHeader, position: usize) -> u8 {
        use crate::indexes::accounts::{ROLE_SIGNER, ROLE_WRITABLE};

        let signer_count = usize::from(header.num_required_signatures);
        let mut roles = 0;
        if position < signer_count {
            roles |= ROLE_SIGNER;
            if position < signer_count - usize::from(header.num_readonly_signed) {
                roles |= ROLE_WRITABLE;
            }
        } else if position < self.static_len() {
            if position < self.static_len() - usize::from(header.num_readonly_unsigned) {
                roles |= ROLE_WRITABLE;
            }
        } else if position < self.static_len() + self.loaded_writable_len() {
            roles |= ROLE_WRITABLE;
        }
        roles
    }
}

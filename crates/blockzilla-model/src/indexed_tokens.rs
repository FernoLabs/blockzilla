//! Optional token projection that preserves source registry references.
//!
//! Registry IDs have meaning only within the source and registry recorded by
//! the consumer. An inline key is a different namespace, and an absent key is
//! represented by `None`, never by ID zero.

use std::num::NonZeroU32;

use crate::{BlockView, Result, TokenBalanceSide};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountReference {
    Registry(NonZeroU32),
    Inline([u8; 32]),
}

/// One selected row in a flat, reusable block buffer. No per-row vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedTokenBalance {
    pub tx_index: u32,
    pub side: TokenBalanceSide,
    pub balance_index: u32,
    /// Position in this transaction's message account list, not a registry ID.
    pub account_index: u32,
    /// Actual token account from static + writable-loaded + readonly-loaded keys.
    pub token_account: AccountReference,
    pub mint: Option<AccountReference>,
    pub owner: Option<AccountReference>,
    pub token_program: Option<AccountReference>,
    pub amount: u64,
    pub decimals: u8,
}

/// Called by a consumer only when it needs a new dictionary entry.
pub trait AccountResolver {
    fn resolve(&mut self, reference: AccountReference) -> Result<[u8; 32]>;
}

/// Ordered token-only output. Views borrow reused storage for this callback.
///
/// `block` contains transaction headers and coverage; its canonical balance
/// lists are empty. `balances` contains selected rows in transaction/pre/post
/// order. A consumer must not interpret an unknown execution state as failure.
pub trait IndexedTokenSink {
    fn visit_indexed_block(
        &mut self,
        block: BlockView<'_>,
        balances: &[IndexedTokenBalance],
        resolver: &mut dyn AccountResolver,
    ) -> Result<()>;
}

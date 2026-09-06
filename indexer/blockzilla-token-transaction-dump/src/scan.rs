//! Pure, ordered token-transaction matching.
//!
//! Archive readers build [`TransactionFacts`] with resolved public keys. This
//! module does not read archive files and does not depend on epoch-local
//! registry ids.

use std::collections::HashSet;

pub type PubkeyBytes = [u8; 32];

pub const SPL_TOKEN_PROGRAM_ID: PubkeyBytes =
    solana_pubkey::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").to_bytes();
pub const SPL_TOKEN_2022_PROGRAM_ID: PubkeyBytes =
    solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").to_bytes();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionOrigin {
    Outer,
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenProgram {
    SplToken,
    SplToken2022,
}

/// The resolved facts from one outer or inner instruction.
///
/// `accounts` contains only the instruction account list. It does not include
/// `program_id` unless the program is also explicitly present in that list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFact {
    pub origin: InstructionOrigin,
    pub program_id: PubkeyBytes,
    pub accounts: Vec<PubkeyBytes>,
    pub token_instruction: Option<TokenInstructionFact>,
}

impl InstructionFact {
    pub fn new(
        origin: InstructionOrigin,
        program_id: PubkeyBytes,
        accounts: Vec<PubkeyBytes>,
        data: &[u8],
    ) -> Self {
        let token_instruction = decode_token_program_instruction(program_id, &accounts, data);
        Self {
            origin,
            program_id,
            accounts,
            token_instruction,
        }
    }

    pub fn outer(program_id: PubkeyBytes, accounts: Vec<PubkeyBytes>, data: &[u8]) -> Self {
        Self::new(InstructionOrigin::Outer, program_id, accounts, data)
    }

    pub fn inner(program_id: PubkeyBytes, accounts: Vec<PubkeyBytes>, data: &[u8]) -> Self {
        Self::new(InstructionOrigin::Inner, program_id, accounts, data)
    }

    #[inline]
    pub fn touches(&self, pubkey: &PubkeyBytes) -> bool {
        self.accounts.iter().any(|account| account == pubkey)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenBalanceFact {
    pub account: PubkeyBytes,
    pub mint: PubkeyBytes,
}

impl TokenBalanceFact {
    pub const fn new(account: PubkeyBytes, mint: PubkeyBytes) -> Self {
        Self { account, mint }
    }
}

/// Compact owned facts for one transaction in ledger order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionFacts {
    /// True when transaction metadata records an execution error.
    pub has_error: bool,
    /// Outer and inner instructions in their observed order.
    pub instructions: Vec<InstructionFact>,
    pub pre_token_balances: Vec<TokenBalanceFact>,
    pub post_token_balances: Vec<TokenBalanceFact>,
}

impl TransactionFacts {
    pub fn successful(instructions: Vec<InstructionFact>) -> Self {
        Self {
            has_error: false,
            instructions,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
        }
    }

    pub fn failed(instructions: Vec<InstructionFact>) -> Self {
        Self {
            has_error: true,
            instructions,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
        }
    }

    pub fn with_pre_token_balance(mut self, account: PubkeyBytes, mint: PubkeyBytes) -> Self {
        self.pre_token_balances
            .push(TokenBalanceFact::new(account, mint));
        self
    }

    pub fn with_post_token_balance(mut self, account: PubkeyBytes, mint: PubkeyBytes) -> Self {
        self.post_token_balances
            .push(TokenBalanceFact::new(account, mint));
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenInstructionFact {
    pub program: TokenProgram,
    pub kind: TokenInstructionKind,
}

/// State-relevant roles from the stable SPL Token instruction layouts.
///
/// A known tag with too few accounts is kept as [`Self::Unknown`]. Unknown
/// Token-2022 extension instructions are also kept as `Unknown`. Their full
/// account list remains available on [`InstructionFact`] for selection, but
/// they never change tracker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenInstructionKind {
    InitializeMint {
        mint: PubkeyBytes,
    },
    InitializeAccount {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    Transfer {
        source: PubkeyBytes,
        destination: PubkeyBytes,
    },
    Approve {
        source: PubkeyBytes,
    },
    Revoke {
        source: PubkeyBytes,
    },
    SetAuthority {
        account: PubkeyBytes,
    },
    MintTo {
        mint: PubkeyBytes,
        destination: PubkeyBytes,
    },
    Burn {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    CloseAccount {
        account: PubkeyBytes,
        destination: PubkeyBytes,
    },
    FreezeAccount {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    ThawAccount {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    TransferChecked {
        source: PubkeyBytes,
        mint: PubkeyBytes,
        destination: PubkeyBytes,
    },
    ApproveChecked {
        source: PubkeyBytes,
        mint: PubkeyBytes,
    },
    MintToChecked {
        mint: PubkeyBytes,
        destination: PubkeyBytes,
    },
    BurnChecked {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    InitializeAccount2 {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    SyncNative {
        account: PubkeyBytes,
    },
    InitializeAccount3 {
        account: PubkeyBytes,
        mint: PubkeyBytes,
    },
    InitializeMint2 {
        mint: PubkeyBytes,
    },
    Unknown {
        tag: Option<u8>,
    },
}

/// Decode the common instruction tags shared by classic SPL Token and
/// Token-2022. Returns `None` for a non-token program.
///
/// This decoder extracts account roles only. It does not parse amounts,
/// authorities, or extension payloads because the ordered matcher does not
/// need those values.
pub fn decode_token_program_instruction(
    program_id: PubkeyBytes,
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Option<TokenInstructionFact> {
    let program = if program_id == SPL_TOKEN_PROGRAM_ID {
        TokenProgram::SplToken
    } else if program_id == SPL_TOKEN_2022_PROGRAM_ID {
        TokenProgram::SplToken2022
    } else {
        return None;
    };

    let tag = data.first().copied();
    let unknown = || TokenInstructionKind::Unknown { tag };
    let kind = match tag {
        Some(0) => match accounts {
            [mint, ..] => TokenInstructionKind::InitializeMint { mint: *mint },
            _ => unknown(),
        },
        Some(1) => match accounts {
            [account, mint, _owner, ..] => TokenInstructionKind::InitializeAccount {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(3) => match accounts {
            [source, destination, _authority, ..] => TokenInstructionKind::Transfer {
                source: *source,
                destination: *destination,
            },
            _ => unknown(),
        },
        Some(4) => match accounts {
            [source, _delegate, _authority, ..] => {
                TokenInstructionKind::Approve { source: *source }
            }
            _ => unknown(),
        },
        Some(5) => match accounts {
            [source, _authority, ..] => TokenInstructionKind::Revoke { source: *source },
            _ => unknown(),
        },
        Some(6) => match accounts {
            [account, _authority, ..] => TokenInstructionKind::SetAuthority { account: *account },
            _ => unknown(),
        },
        Some(7) => match accounts {
            [mint, destination, _authority, ..] => TokenInstructionKind::MintTo {
                mint: *mint,
                destination: *destination,
            },
            _ => unknown(),
        },
        Some(8) => match accounts {
            [account, mint, _authority, ..] => TokenInstructionKind::Burn {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(9) => match accounts {
            [account, destination, _authority, ..] => TokenInstructionKind::CloseAccount {
                account: *account,
                destination: *destination,
            },
            _ => unknown(),
        },
        Some(10) => match accounts {
            [account, mint, _authority, ..] => TokenInstructionKind::FreezeAccount {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(11) => match accounts {
            [account, mint, _authority, ..] => TokenInstructionKind::ThawAccount {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(12) => match accounts {
            [source, mint, destination, _authority, ..] => TokenInstructionKind::TransferChecked {
                source: *source,
                mint: *mint,
                destination: *destination,
            },
            _ => unknown(),
        },
        Some(13) => match accounts {
            [source, mint, _delegate, _authority, ..] => TokenInstructionKind::ApproveChecked {
                source: *source,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(14) => match accounts {
            [mint, destination, _authority, ..] => TokenInstructionKind::MintToChecked {
                mint: *mint,
                destination: *destination,
            },
            _ => unknown(),
        },
        Some(15) => match accounts {
            [account, mint, _authority, ..] => TokenInstructionKind::BurnChecked {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(16) => match accounts {
            [account, mint, ..] => TokenInstructionKind::InitializeAccount2 {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(17) => match accounts {
            [account, ..] => TokenInstructionKind::SyncNative { account: *account },
            _ => unknown(),
        },
        Some(18) => match accounts {
            [account, mint, ..] => TokenInstructionKind::InitializeAccount3 {
                account: *account,
                mint: *mint,
            },
            _ => unknown(),
        },
        Some(20) => match accounts {
            [mint, ..] => TokenInstructionKind::InitializeMint2 { mint: *mint },
            _ => unknown(),
        },
        _ => unknown(),
    };

    Some(TokenInstructionFact { program, kind })
}

/// Stateful selector. Calls must follow ledger order across blocks and epochs.
#[derive(Debug, Clone)]
pub struct TokenAccountTracker {
    mint: PubkeyBytes,
    token_accounts: HashSet<PubkeyBytes>,
}

impl TokenAccountTracker {
    pub fn new(mint: PubkeyBytes) -> Self {
        Self {
            mint,
            token_accounts: HashSet::new(),
        }
    }

    /// Restore a tracker from a previously validated epoch-boundary checkpoint.
    ///
    /// The caller must authenticate and validate the checkpoint before it calls
    /// this function. Duplicate accounts are harmless and are normalized by the
    /// set, but checkpoint readers should reject them so that the serialized
    /// state has one canonical representation.
    pub fn from_tracked_accounts(
        mint: PubkeyBytes,
        accounts: impl IntoIterator<Item = PubkeyBytes>,
    ) -> Self {
        Self {
            mint,
            token_accounts: accounts.into_iter().collect(),
        }
    }

    pub const fn mint(&self) -> PubkeyBytes {
        self.mint
    }

    pub fn tracked_account_count(&self) -> usize {
        self.token_accounts.len()
    }

    pub fn is_tracked(&self, account: &PubkeyBytes) -> bool {
        self.token_accounts.contains(account)
    }

    pub fn tracked_accounts(&self) -> impl Iterator<Item = &PubkeyBytes> {
        self.token_accounts.iter()
    }

    /// Replace the active epoch-boundary set after verified local-ID tracking completes.
    pub(crate) fn replace_tracked_accounts(
        &mut self,
        accounts: impl IntoIterator<Item = PubkeyBytes>,
    ) {
        self.token_accounts = accounts.into_iter().collect();
    }

    /// Select one transaction and update state if it succeeded.
    ///
    /// Instructions are applied in their supplied execution order. A failed
    /// transaction is simulated on temporary state for selection, then all of
    /// its state changes are discarded.
    pub fn select(&mut self, transaction: &TransactionFacts) -> bool {
        if transaction.has_error {
            let mut temporary = self.token_accounts.clone();
            return simulate_transaction(&mut temporary, self.mint, transaction, true);
        }
        simulate_transaction(&mut self.token_accounts, self.mint, transaction, true)
    }

    /// Process transactions in the supplied order and return one decision for
    /// each transaction.
    pub fn select_ordered(&mut self, transactions: &[TransactionFacts]) -> Vec<bool> {
        transactions
            .iter()
            .map(|transaction| self.select(transaction))
            .collect()
    }
}

fn simulate_transaction(
    token_accounts: &mut HashSet<PubkeyBytes>,
    mint: PubkeyBytes,
    transaction: &TransactionFacts,
    include_balance_facts: bool,
) -> bool {
    let mut selected = include_balance_facts
        && apply_balance_facts(token_accounts, mint, &transaction.pre_token_balances);
    for instruction in &transaction.instructions {
        selected |= instruction_touches_target(instruction, mint, token_accounts);
        if let Some(token) = instruction.token_instruction {
            apply_token_instruction(token_accounts, mint, token.kind);
        }
        selected |= instruction_touches_target(instruction, mint, token_accounts);
    }
    if include_balance_facts {
        selected |= apply_balance_facts(token_accounts, mint, &transaction.post_token_balances);
        selected |= transaction
            .instructions
            .iter()
            .any(|instruction| instruction_touches_target(instruction, mint, token_accounts));
    }
    selected
}

fn instruction_touches_target(
    instruction: &InstructionFact,
    mint: PubkeyBytes,
    token_accounts: &HashSet<PubkeyBytes>,
) -> bool {
    instruction
        .accounts
        .iter()
        .any(|account| *account == mint || token_accounts.contains(account))
}

fn apply_balance_facts(
    token_accounts: &mut HashSet<PubkeyBytes>,
    target_mint: PubkeyBytes,
    balances: &[TokenBalanceFact],
) -> bool {
    let mut contains_target = false;
    for balance in balances {
        contains_target |= balance.mint == target_mint;
        set_account_mint(token_accounts, target_mint, balance.account, balance.mint);
    }
    contains_target
}

fn set_account_mint(
    token_accounts: &mut HashSet<PubkeyBytes>,
    target_mint: PubkeyBytes,
    account: PubkeyBytes,
    mint: PubkeyBytes,
) {
    if mint == target_mint {
        token_accounts.insert(account);
    } else {
        token_accounts.remove(&account);
    }
}

fn apply_token_instruction(
    token_accounts: &mut HashSet<PubkeyBytes>,
    target_mint: PubkeyBytes,
    instruction: TokenInstructionKind,
) {
    match instruction {
        TokenInstructionKind::InitializeAccount { account, mint }
        | TokenInstructionKind::InitializeAccount2 { account, mint }
        | TokenInstructionKind::InitializeAccount3 { account, mint } => {
            set_account_mint(token_accounts, target_mint, account, mint);
        }
        TokenInstructionKind::Burn { account, mint }
        | TokenInstructionKind::BurnChecked { account, mint }
        | TokenInstructionKind::FreezeAccount { account, mint }
        | TokenInstructionKind::ThawAccount { account, mint }
        | TokenInstructionKind::ApproveChecked {
            source: account,
            mint,
        } => {
            set_account_mint(token_accounts, target_mint, account, mint);
        }
        TokenInstructionKind::MintTo { mint, destination }
        | TokenInstructionKind::MintToChecked { mint, destination } => {
            set_account_mint(token_accounts, target_mint, destination, mint);
        }
        TokenInstructionKind::TransferChecked {
            source,
            mint,
            destination,
        } => {
            set_account_mint(token_accounts, target_mint, source, mint);
            set_account_mint(token_accounts, target_mint, destination, mint);
        }
        TokenInstructionKind::Transfer {
            source,
            destination,
        } => {
            if token_accounts.contains(&source) || token_accounts.contains(&destination) {
                token_accounts.insert(source);
                token_accounts.insert(destination);
            }
        }
        TokenInstructionKind::CloseAccount { account, .. } => {
            token_accounts.remove(&account);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OTHER_PROGRAM: PubkeyBytes = [200; 32];

    const fn key(byte: u8) -> PubkeyBytes {
        [byte; 32]
    }

    fn token_outer(accounts: Vec<PubkeyBytes>, tag: u8) -> InstructionFact {
        InstructionFact::outer(SPL_TOKEN_PROGRAM_ID, accounts, &[tag])
    }

    fn token_2022_inner(accounts: Vec<PubkeyBytes>, tag: u8) -> InstructionFact {
        InstructionFact::inner(SPL_TOKEN_2022_PROGRAM_ID, accounts, &[tag])
    }

    fn non_token_inner(accounts: Vec<PubkeyBytes>) -> InstructionFact {
        InstructionFact::inner(OTHER_PROGRAM, accounts, &[99])
    }

    #[test]
    fn inner_non_token_instruction_selects_a_tracked_account() {
        let mint = key(1);
        let account = key(2);
        let mut tracker = TokenAccountTracker::new(mint);
        let discovery =
            TransactionFacts::successful(vec![token_outer(vec![account, mint, key(3)], 1)]);
        assert!(tracker.select(&discovery));

        let inner_touch = TransactionFacts::successful(vec![non_token_inner(vec![account])]);
        assert!(tracker.select(&inner_touch));
    }

    #[test]
    fn initialize_account_selects_and_tracks_the_created_account() {
        let mint = key(10);
        let account = key(11);
        let transaction =
            TransactionFacts::successful(vec![token_2022_inner(vec![account, mint], 18)]);
        let mut tracker = TokenAccountTracker::new(mint);

        assert_eq!(tracker.select_ordered(&[transaction]), vec![true]);
        assert!(tracker.is_tracked(&account));
    }

    #[test]
    fn unchecked_transfer_propagation_respects_instruction_order() {
        let mint = key(20);
        let source = key(21);
        let middle = key(22);
        let destination = key(23);
        let authority = key(24);
        let mut tracker = TokenAccountTracker::new(mint);

        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![source, mint, authority], 1)]),
            TransactionFacts::successful(vec![
                // This transfer runs before `middle` is known to hold the
                // target mint, so it must not retroactively track destination.
                token_outer(vec![middle, destination, authority], 3),
                token_outer(vec![source, middle, authority], 3),
            ]),
            TransactionFacts::successful(vec![non_token_inner(vec![destination])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, false]
        );
        assert!(tracker.is_tracked(&source));
        assert!(tracker.is_tracked(&middle));
        assert!(!tracker.is_tracked(&destination));
    }

    #[test]
    fn forward_unchecked_transfer_chain_propagates_in_execution_order() {
        let mint = key(25);
        let source = key(26);
        let middle = key(27);
        let destination = key(28);
        let authority = key(29);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![source, mint, authority], 1)]),
            TransactionFacts::successful(vec![
                token_outer(vec![source, middle, authority], 3),
                token_outer(vec![middle, destination, authority], 3),
            ]),
            TransactionFacts::successful(vec![non_token_inner(vec![destination])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, true]
        );
        assert!(tracker.is_tracked(&destination));
    }

    #[test]
    fn failed_initialize_is_selected_but_does_not_change_state() {
        let mint = key(30);
        let account = key(31);
        let mut tracker = TokenAccountTracker::new(mint);
        let failed = TransactionFacts::failed(vec![token_outer(vec![account, mint, key(32)], 1)]);
        let later_touch = TransactionFacts::successful(vec![non_token_inner(vec![account])]);

        assert_eq!(
            tracker.select_ordered(&[failed, later_touch]),
            vec![true, false]
        );
        assert!(!tracker.is_tracked(&account));
    }

    #[test]
    fn successful_close_is_selected_then_removes_account() {
        let mint = key(40);
        let account = key(41);
        let wallet = key(42);
        let authority = key(43);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![account, mint, authority], 1)]),
            TransactionFacts::successful(vec![token_outer(vec![account, wallet, authority], 9)]),
            TransactionFacts::successful(vec![non_token_inner(vec![account])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, false]
        );
        assert!(!tracker.is_tracked(&account));
        assert!(!tracker.is_tracked(&wallet));
    }

    #[test]
    fn unrelated_transaction_is_not_selected() {
        let mut tracker = TokenAccountTracker::new(key(50));
        let transaction = TransactionFacts::successful(vec![InstructionFact::outer(
            OTHER_PROGRAM,
            vec![key(51), key(52)],
            &[7],
        )]);

        assert!(!tracker.select(&transaction));
        assert_eq!(tracker.tracked_account_count(), 0);
    }

    #[test]
    fn closed_account_can_be_reused_for_another_mint_without_false_matches() {
        let mint = key(60);
        let other_mint = key(61);
        let account = key(62);
        let wallet = key(63);
        let authority = key(64);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![account, mint, authority], 1)]),
            TransactionFacts::successful(vec![token_outer(vec![account, wallet, authority], 9)]),
            TransactionFacts::successful(vec![token_outer(
                vec![account, other_mint, authority],
                1,
            )]),
            TransactionFacts::successful(vec![non_token_inner(vec![account])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, false, false]
        );
        assert!(!tracker.is_tracked(&account));
    }

    #[test]
    fn close_then_recreate_for_other_mint_in_one_transaction_ends_untracked() {
        let mint = key(65);
        let other_mint = key(66);
        let account = key(67);
        let wallet = key(68);
        let authority = key(69);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![account, mint, authority], 1)]),
            TransactionFacts::successful(vec![
                token_outer(vec![account, wallet, authority], 9),
                token_outer(vec![account, other_mint, authority], 1),
            ]),
            TransactionFacts::successful(vec![non_token_inner(vec![account])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, false]
        );
        assert!(!tracker.is_tracked(&account));
    }

    #[test]
    fn close_then_recreate_for_target_mint_in_one_transaction_ends_tracked() {
        let mint = key(73);
        let account = key(74);
        let wallet = key(75);
        let authority = key(76);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![token_outer(vec![account, mint, authority], 1)]),
            TransactionFacts::successful(vec![
                token_outer(vec![account, wallet, authority], 9),
                token_outer(vec![account, mint, authority], 1),
            ]),
            TransactionFacts::successful(vec![non_token_inner(vec![account])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, true]
        );
        assert!(tracker.is_tracked(&account));
    }

    #[test]
    fn target_balance_facts_learn_accounts_and_failed_balances_do_not() {
        let mint = key(70);
        let account = key(71);
        let failed_account = key(72);
        let mut tracker = TokenAccountTracker::new(mint);
        let transactions = vec![
            TransactionFacts::successful(vec![non_token_inner(vec![account])])
                .with_post_token_balance(account, mint),
            TransactionFacts::failed(vec![non_token_inner(vec![failed_account])])
                .with_post_token_balance(failed_account, mint),
            TransactionFacts::successful(vec![non_token_inner(vec![failed_account])]),
        ];

        assert_eq!(
            tracker.select_ordered(&transactions),
            vec![true, true, false]
        );
        assert!(tracker.is_tracked(&account));
        assert!(!tracker.is_tracked(&failed_account));
    }

    #[test]
    fn failed_target_balance_selects_without_persisting_state() {
        let mint = key(77);
        let account = key(78);
        let mut tracker = TokenAccountTracker::new(mint);
        let failed = TransactionFacts::failed(Vec::new()).with_post_token_balance(account, mint);

        assert!(tracker.select(&failed));
        assert!(!tracker.is_tracked(&account));
        assert!(
            !tracker.select(&TransactionFacts::successful(vec![non_token_inner(vec![
                account
            ]),]))
        );
    }

    #[test]
    fn decoder_covers_classic_and_token_2022_stable_tags() {
        let accounts = vec![key(80), key(81), key(82), key(83)];
        let cases = [
            (0, "initialize_mint"),
            (1, "initialize_account"),
            (3, "transfer"),
            (4, "approve"),
            (5, "revoke"),
            (6, "set_authority"),
            (7, "mint_to"),
            (8, "burn"),
            (9, "close"),
            (10, "freeze"),
            (11, "thaw"),
            (12, "transfer_checked"),
            (13, "approve_checked"),
            (14, "mint_to_checked"),
            (15, "burn_checked"),
            (16, "initialize_account_2"),
            (17, "sync_native"),
            (18, "initialize_account_3"),
            (20, "initialize_mint_2"),
        ];

        for program_id in [SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID] {
            for (tag, label) in cases {
                let decoded = decode_token_program_instruction(program_id, &accounts, &[tag])
                    .unwrap_or_else(|| panic!("did not decode {label}"));
                assert!(
                    !matches!(decoded.kind, TokenInstructionKind::Unknown { .. }),
                    "decoded {label} as unknown"
                );
            }
        }
    }

    #[test]
    fn unknown_and_truncated_token_instructions_are_conservative() {
        let unknown =
            decode_token_program_instruction(SPL_TOKEN_2022_PROGRAM_ID, &[key(90)], &[250])
                .expect("token program");
        assert_eq!(
            unknown.kind,
            TokenInstructionKind::Unknown { tag: Some(250) }
        );

        let truncated = decode_token_program_instruction(SPL_TOKEN_PROGRAM_ID, &[key(91)], &[3])
            .expect("token program");
        assert_eq!(
            truncated.kind,
            TokenInstructionKind::Unknown { tag: Some(3) }
        );
        assert!(decode_token_program_instruction(OTHER_PROGRAM, &[key(92)], &[3]).is_none());
    }
}

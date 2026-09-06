//! `dictionary/account_flags.pages`: what each registry account was used as.
//!
//! One byte per account, indexed by the same ordinal `ledger/accounts` stores.
//! These are properties of the **account**, not of a transaction, so they
//! belong once in the dictionary rather than once per reference. Epoch 822 has
//! ~45M accounts and ~7.5B references to them — ~168x fewer entries, so 45 MiB
//! for the whole epoch against gigabytes to flag each reference.
//!
//! It also keeps columns out of common filters. "Which programs did this wallet
//! call" previously had to walk `ledger/instructions` (3.9 GiB at epoch scale)
//! purely to map program positions back to accounts; with this table the
//! account column answers it alone.
//!
//! ## The flags
//!
//! - [`FLAG_SIGNER`] — appeared in a signing position of at least one
//!   transaction in this generation.
//! - [`FLAG_PROGRAM`] — was named as a `program_id` by at least one
//!   instruction, top-level or through CPI.
//!
//! ## What "the rest" means
//!
//! An account with neither bit was only ever a passive participant here, which
//! is the shape of a program-derived address — [`is_derived`] reports that. It
//! is an **inference from observed use, not a derivation**: a wallet that only
//! received funds this generation, or a token mint, also signed nothing and was
//! never invoked, so it answers the same way.
//!
//! The fact that would settle it is on-curve-ness, since a PDA is off-curve by
//! construction. That is computable from the 32 key bytes alone and needs no
//! ledger context, so it can be added as another bit later without changing the
//! layout — which is why this is a byte with room rather than a packed bitmap.
//!
//! ## What the bits do and do not claim
//!
//! Both are observations about **this generation**: an executable account no
//! transaction called is not flagged as a program, and a wallet that signed
//! nothing this epoch is not flagged as a signer. They are derived from the
//! generation\'s own ledger, so they can never disagree with it, but they are
//! not statements about the account on chain.

use thiserror::Error;

pub const PATH: &str = "dictionary/account_flags.pages";
pub const SCHEMA: u16 = 1;

/// Signed at least one transaction in this generation.
pub const FLAG_SIGNER: u8 = 1 << 0;
/// Named as a `program_id` in this generation, top-level or through CPI.
pub const FLAG_PROGRAM: u8 = 1 << 1;
pub const KNOWN_FLAGS: u8 = FLAG_SIGNER | FLAG_PROGRAM;

/// Bytes needed for `entries` accounts.
pub const fn byte_len(entries: u32) -> usize {
    entries as usize
}

/// Flags for one registry ordinal.
///
/// Ordinals are 1-based, matching `dictionary/pubkeys`: zero is the source\'s
/// inline-key sentinel, so it is not an index.
pub fn flags_at(table: &[u8], ordinal: u32) -> Result<u8, AccountFlagsError> {
    if ordinal == 0 {
        return Err(AccountFlagsError::ReservedOrdinal);
    }
    table
        .get((ordinal - 1) as usize)
        .copied()
        .ok_or(AccountFlagsError::OrdinalOutOfRange {
            ordinal,
            entries: table.len() as u32,
        })
}

pub fn set_flags(table: &mut [u8], ordinal: u32, flags: u8) -> Result<(), AccountFlagsError> {
    if ordinal == 0 {
        return Err(AccountFlagsError::ReservedOrdinal);
    }
    if flags & !KNOWN_FLAGS != 0 {
        return Err(AccountFlagsError::UnknownFlags(flags));
    }
    let entries = table.len() as u32;
    let slot = table
        .get_mut((ordinal - 1) as usize)
        .ok_or(AccountFlagsError::OrdinalOutOfRange { ordinal, entries })?;
    *slot |= flags;
    Ok(())
}

pub fn is_signer(table: &[u8], ordinal: u32) -> Result<bool, AccountFlagsError> {
    Ok(flags_at(table, ordinal)? & FLAG_SIGNER != 0)
}

pub fn is_program(table: &[u8], ordinal: u32) -> Result<bool, AccountFlagsError> {
    Ok(flags_at(table, ordinal)? & FLAG_PROGRAM != 0)
}

/// Neither signed nor invoked here, which is the shape of a derived address.
///
/// See the module docs: this is an inference from observed use. A wallet that
/// only received funds this generation answers `true` as well.
pub fn is_derived(table: &[u8], ordinal: u32) -> Result<bool, AccountFlagsError> {
    Ok(flags_at(table, ordinal)? & KNOWN_FLAGS == 0)
}

pub fn count_with(table: &[u8], flag: u8) -> u32 {
    table.iter().filter(|byte| *byte & flag != 0).count() as u32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AccountFlagsError {
    #[error("account ordinal 0 is the inline-key sentinel, not an index")]
    ReservedOrdinal,
    #[error("account ordinal {ordinal} is outside {entries} entries")]
    OrdinalOutOfRange { ordinal: u32, entries: u32 },
    #[error("unknown account flag bits: {0:#010b}")]
    UnknownFlags(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_accumulate_per_account() {
        let mut table = vec![0u8; byte_len(3)];
        set_flags(&mut table, 1, FLAG_SIGNER).unwrap();
        // A fee payer that also gets invoked keeps both; the writes are ORs,
        // not assignments, because each reference only knows its own role.
        set_flags(&mut table, 1, FLAG_PROGRAM).unwrap();
        set_flags(&mut table, 2, FLAG_SIGNER).unwrap();
        assert_eq!(flags_at(&table, 1).unwrap(), FLAG_SIGNER | FLAG_PROGRAM);
        assert_eq!(count_with(&table, FLAG_SIGNER), 2);
        assert_eq!(count_with(&table, FLAG_PROGRAM), 1);
    }

    #[test]
    fn neither_flag_reads_as_derived() {
        let mut table = vec![0u8; byte_len(3)];
        set_flags(&mut table, 1, FLAG_SIGNER).unwrap();
        set_flags(&mut table, 2, FLAG_PROGRAM).unwrap();
        assert!(!is_derived(&table, 1).unwrap());
        assert!(!is_derived(&table, 2).unwrap());
        assert!(is_derived(&table, 3).unwrap());
    }

    #[test]
    fn ordinal_zero_is_the_sentinel_and_past_the_end_errors() {
        let mut table = vec![0u8; byte_len(2)];
        assert_eq!(flags_at(&table, 0), Err(AccountFlagsError::ReservedOrdinal));
        assert_eq!(
            set_flags(&mut table, 0, FLAG_SIGNER),
            Err(AccountFlagsError::ReservedOrdinal)
        );
        // Erroring rather than reading zero: zero would report a program as a
        // derived address.
        assert_eq!(
            flags_at(&table, 3),
            Err(AccountFlagsError::OrdinalOutOfRange {
                ordinal: 3,
                entries: 2
            })
        );
    }

    #[test]
    fn unknown_flag_bits_are_rejected() {
        // The spare bits are reserved for facts like on-curve; writing one now
        // would make an old reader\'s is_derived silently wrong.
        let mut table = vec![0u8; byte_len(1)];
        assert_eq!(
            set_flags(&mut table, 1, 1 << 2),
            Err(AccountFlagsError::UnknownFlags(1 << 2))
        );
    }
}

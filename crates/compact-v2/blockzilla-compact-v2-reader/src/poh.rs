//! Exact signature Merkle roots and PoH entry hashes for archive verification.
//!
//! These helpers came from the unused Replay Projection codec. They retain
//! the same limits, hashing rules, and diagnostic field labels. They do not
//! encode a replay payload or activate reserved Hivezilla payload format 8.

use std::{error::Error, fmt};

use sha2::{Digest, Sha256};

pub const MAX_POH_NUM_HASHES_PER_ENTRY: u64 = 16_777_216;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PohError {
    InvalidValue {
        field: &'static str,
        reason: &'static str,
    },
    AggregateOutOfBounds {
        field: &'static str,
        max: u64,
        actual: u64,
    },
    ArithmeticOverflow {
        field: &'static str,
    },
}

impl fmt::Display for PohError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidValue { field, reason } => write!(formatter, "invalid {field}: {reason}"),
            Self::AggregateOutOfBounds { field, max, actual } => {
                write!(formatter, "{field} aggregate {actual} exceeds {max}")
            }
            Self::ArithmeticOverflow { field } => {
                write!(formatter, "arithmetic overflow while validating {field}")
            }
        }
    }
}

impl Error for PohError {}

pub type PohResult<T> = Result<T, PohError>;

/// Streaming construction of the exact signature Merkle root mixed into one
/// transaction-bearing PoH entry.
///
/// Signatures are consumed in transaction order and signer order. They are not
/// retained by the builder.
#[derive(Debug, Clone)]
pub struct SignatureMixinBuilder {
    frontier: [Option<[u8; 32]>; 64],
    signature_count: u64,
}

impl Default for SignatureMixinBuilder {
    fn default() -> Self {
        Self {
            frontier: [None; 64],
            signature_count: 0,
        }
    }
}

impl SignatureMixinBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_signature(&mut self, signature: &[u8; 64]) -> PohResult<()> {
        self.signature_count =
            self.signature_count
                .checked_add(1)
                .ok_or(PohError::ArithmeticOverflow {
                    field: "ReplaySignatureMixinBuilder.signature_count",
                })?;

        let mut node = signature_leaf_hash(signature);
        let mut level = 0usize;
        loop {
            let Some(frontier) = self.frontier.get_mut(level) else {
                return Err(PohError::ArithmeticOverflow {
                    field: "ReplaySignatureMixinBuilder.frontier",
                });
            };
            if let Some(left) = frontier.take() {
                node = signature_node_hash(left, node);
                level += 1;
            } else {
                *frontier = Some(node);
                return Ok(());
            }
        }
    }

    #[must_use]
    pub const fn signature_count(&self) -> u64 {
        self.signature_count
    }

    /// Finish using the frozen odd-node rule: duplicate the last node at every
    /// incomplete Merkle level.
    #[must_use]
    pub fn finish(self) -> [u8; 32] {
        if self.signature_count == 0 {
            return [0; 32];
        }

        let mut right: Option<([u8; 32], usize)> = None;
        for (level, left) in self.frontier.into_iter().enumerate() {
            let Some(left) = left else {
                continue;
            };
            right = Some(match right {
                None => (left, level),
                Some((mut right, mut right_level)) => {
                    while right_level < level {
                        right = signature_node_hash(right, right);
                        right_level += 1;
                    }
                    (signature_node_hash(left, right), level + 1)
                }
            });
        }
        right.expect("a non-empty frontier has a root").0
    }
}

/// Compute the exact Agave signature mixin without retaining signature
/// bytes after the call.
pub fn signature_mixin<'a>(
    signatures: impl IntoIterator<Item = &'a [u8; 64]>,
) -> PohResult<[u8; 32]> {
    let mut builder = SignatureMixinBuilder::new();
    for signature in signatures {
        builder.push_signature(signature)?;
    }
    Ok(builder.finish())
}

/// Derive one entry hash using the frozen Agave-compatible PoH formula.
pub fn derive_entry_hash(
    previous_hash: [u8; 32],
    num_hashes: u64,
    transaction_count: u32,
    signature_mixin: Option<[u8; 32]>,
) -> PohResult<[u8; 32]> {
    if num_hashes > MAX_POH_NUM_HASHES_PER_ENTRY {
        return Err(PohError::AggregateOutOfBounds {
            field: "ReplayEntryV1.num_hashes",
            max: MAX_POH_NUM_HASHES_PER_ENTRY,
            actual: num_hashes,
        });
    }
    match (transaction_count == 0, signature_mixin) {
        (true, None) | (false, Some(_)) => {}
        (true, Some(_)) => {
            return Err(PohError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                reason: "must be absent when transaction_count is zero",
            });
        }
        (false, None) => {
            return Err(PohError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                reason: "must be present when transaction_count is non-zero",
            });
        }
    }

    if num_hashes == 0 && transaction_count == 0 {
        return Ok(previous_hash);
    }

    let mut hash = previous_hash;
    for _ in 0..num_hashes.saturating_sub(1) {
        hash = Sha256::digest(hash).into();
    }
    if transaction_count == 0 {
        Ok(Sha256::digest(hash).into())
    } else {
        let mut hasher = Sha256::new();
        hasher.update(hash);
        hasher.update(signature_mixin.expect("non-empty transaction count checked"));
        Ok(hasher.finalize().into())
    }
}

fn signature_leaf_hash(signature: &[u8; 64]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0]);
    hasher.update(signature);
    hasher.finalize().into()
}

fn signature_node_hash(left: [u8; 32], right: [u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([1]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naive_signature_mixin(signatures: &[[u8; 64]]) -> [u8; 32] {
        if signatures.is_empty() {
            return [0; 32];
        }
        let mut level: Vec<[u8; 32]> = signatures.iter().map(signature_leaf_hash).collect();
        while level.len() > 1 {
            if !level.len().is_multiple_of(2) {
                level.push(*level.last().unwrap());
            }
            level = level
                .chunks_exact(2)
                .map(|pair| signature_node_hash(pair[0], pair[1]))
                .collect();
        }
        level[0]
    }

    #[test]
    fn streaming_signature_mixin_matches_frozen_merkle_rule() {
        let signatures: Vec<[u8; 64]> = (0u8..20).map(|value| [value; 64]).collect();
        assert_eq!(signature_mixin([].iter()).unwrap(), [0; 32]);
        for count in 1..=signatures.len() {
            assert_eq!(
                signature_mixin(signatures[..count].iter()).unwrap(),
                naive_signature_mixin(&signatures[..count]),
                "signature count {count}"
            );
        }
    }

    #[test]
    fn entry_hash_formula_covers_tick_record_and_zero_hash_edges() {
        let previous = [7; 32];
        assert_eq!(derive_entry_hash(previous, 0, 0, None).unwrap(), previous);
        assert_eq!(
            derive_entry_hash(previous, 1, 0, None).unwrap(),
            Sha256::digest(previous).as_slice()
        );

        let mixin = [9; 32];
        let mut expected = Sha256::new();
        expected.update(previous);
        expected.update(mixin);
        assert_eq!(
            derive_entry_hash(previous, 0, 1, Some(mixin)).unwrap(),
            expected.finalize().as_slice()
        );
        assert!(matches!(
            derive_entry_hash(previous, 1, 0, Some(mixin)),
            Err(PohError::InvalidValue {
                field: "ReplayEntryV1.signature_mixin",
                ..
            })
        ));
    }
}

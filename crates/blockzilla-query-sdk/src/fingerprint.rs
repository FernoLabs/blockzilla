use sha2::{Digest, Sha256};

use crate::{BlockView, Error, Result};

/// Incremental SHA-256 fingerprint of the canonical block universe.
///
/// Each observed block adds one 16-byte record in callback order:
/// `block_ordinal` as little-endian `u32`, `slot` as little-endian `u64`, and
/// transaction count as little-endian `u32`. No domain prefix or separator is
/// added. This exact encoding lets independent readers prove that they scanned
/// the same ordered block and transaction-count universe.
#[derive(Clone, Debug, Default)]
pub struct BlockUniverseFingerprint {
    hasher: Sha256,
    records: u64,
}

impl BlockUniverseFingerprint {
    /// Start an empty fingerprint.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one canonical block in callback order.
    pub fn update(&mut self, block: BlockView<'_>) -> Result<()> {
        let transaction_count = u32::try_from(block.transactions.len()).map_err(|_| {
            Error::InvalidStream("block transaction count does not fit in u32".into())
        })?;
        let records = self
            .records
            .checked_add(1)
            .ok_or_else(|| Error::InvalidStream("block-universe record count overflow".into()))?;

        let mut record = [0u8; 16];
        record[..4].copy_from_slice(&block.header.block_ordinal.to_le_bytes());
        record[4..12].copy_from_slice(&block.header.slot.to_le_bytes());
        record[12..].copy_from_slice(&transaction_count.to_le_bytes());
        self.hasher.update(record);
        self.records = records;
        Ok(())
    }

    /// Number of 16-byte records included in the fingerprint.
    pub const fn records(&self) -> u64 {
        self.records
    }

    /// Return the current SHA-256 value as 64 lowercase hexadecimal digits.
    pub fn sha256_hex(&self) -> String {
        const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

        let digest = self.hasher.clone().finalize();
        let mut output = String::with_capacity(64);
        for byte in digest {
            output.push(char::from(LOWER_HEX[usize::from(byte >> 4)]));
            output.push(char::from(LOWER_HEX[usize::from(byte & 0x0f)]));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, CpiCoverage, ExecutionStatus,
        InstructionCoverage, TokenBalanceCoverage, TransactionHeader,
    };

    use super::*;

    fn block(block_ordinal: u32, slot: u64, transaction_count: u32) -> CanonicalBlock {
        let transactions = (0..transaction_count)
            .map(|tx_index| CanonicalTransaction {
                header: TransactionHeader {
                    tx_index,
                    status: ExecutionStatus::Succeeded,
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Complete,
                    cpi_coverage: CpiCoverage::Complete,
                },
                primary_signature: None,
                required_signers: Vec::new(),
                instructions: Vec::new(),
                token_balance_coverage: TokenBalanceCoverage::NotRequested,
                token_balances: Vec::new(),
            })
            .collect();
        CanonicalBlock {
            header: BlockHeader {
                epoch: 9,
                block_ordinal,
                slot,
            },
            transactions,
        }
    }

    #[test]
    fn empty_fingerprint_is_standard_sha256() {
        let fingerprint = BlockUniverseFingerprint::new();
        assert_eq!(fingerprint.records(), 0);
        assert_eq!(
            fingerprint.sha256_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uses_the_retained_16_byte_little_endian_record_encoding() {
        let first = block(0, 1, 2);
        let second = block(3, 0x0102_0304_0506_0708, 4);
        let mut fingerprint = BlockUniverseFingerprint::new();
        fingerprint.update(first.as_view()).unwrap();
        fingerprint.update(second.as_view()).unwrap();

        assert_eq!(fingerprint.records(), 2);
        assert_eq!(
            fingerprint.sha256_hex(),
            "11fb2e47c7743f1e63f405e67b9dd103a6bbc228aa794661a170d87eccfa1d9f"
        );

        let mut reversed = BlockUniverseFingerprint::new();
        reversed.update(second.as_view()).unwrap();
        reversed.update(first.as_view()).unwrap();
        assert_ne!(reversed.sha256_hex(), fingerprint.sha256_hex());
    }
}

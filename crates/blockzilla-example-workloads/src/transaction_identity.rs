use std::io::Write;

use blockzilla_query_sdk::{BlockSink, BlockView, TransactionView};

use crate::{FinishedOutput, Result, output::CanonicalOutput};

/// Header bytes before the fixed transaction identity records.
pub const HEADER_BYTES: usize = 48;
/// Fixed bytes in one transaction identity record.
pub const RECORD_BYTES: usize = 80;
/// Version of the transaction identity dump wire schema.
pub const SCHEMA_VERSION: u32 = 1;

const TRANSACTION_IDENTITY_DUMP_MAGIC: [u8; 8] = *b"BZTXID01";

/// Final facts for one canonical transaction identity dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionIdentityDumpReport {
    /// Number of 80-byte transaction records written.
    pub records: u64,
    /// Header and record bytes written to the output.
    pub output_bytes: u64,
    /// SHA-256 of all output bytes, including the 48-byte header.
    pub output_sha256: [u8; 32],
    /// First slot that has a written transaction record.
    pub first_slot: Option<u64>,
    /// Last slot that has a written transaction record.
    pub last_slot: Option<u64>,
}

impl TransactionIdentityDumpReport {
    /// Lower-case hexadecimal SHA-256 for logs and parity reports.
    pub fn output_sha256_hex(&self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.output_sha256 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

/// Write every transaction coordinate and required primary signature.
///
/// Input records must have the configured epoch and be in the header slot
/// range. Slots must increase and transaction indexes must start at zero for
/// each slot and increase by one. A missing primary signature is an error.
pub struct TransactionIdentityDumpSink<W> {
    epoch: u64,
    start_slot: u64,
    end_slot_exclusive: u64,
    output: CanonicalOutput<W>,
    record_buffer: [u8; RECORD_BYTES],
    last_slot: Option<u64>,
    last_tx_index: u32,
    first_written_slot: Option<u64>,
    last_written_slot: Option<u64>,
}

impl<W: Write> TransactionIdentityDumpSink<W> {
    pub const HEADER_BYTES: usize = HEADER_BYTES;
    pub const RECORD_BYTES: usize = RECORD_BYTES;

    /// Create a dump with its explicit epoch and covered slot range.
    pub fn new(writer: W, epoch: u64, start_slot: u64, end_slot_exclusive: u64) -> Result<Self> {
        if start_slot >= end_slot_exclusive {
            return Err(crate::Error::TransactionIdentityInvalidRange {
                start_slot,
                end_slot_exclusive,
            });
        }
        let header = header(epoch, start_slot, end_slot_exclusive);
        Ok(Self {
            epoch,
            start_slot,
            end_slot_exclusive,
            output: CanonicalOutput::new(writer, &header)?,
            record_buffer: [0; RECORD_BYTES],
            last_slot: None,
            last_tx_index: 0,
            first_written_slot: None,
            last_written_slot: None,
        })
    }

    /// Process all transaction views in one block.
    pub fn process_block(&mut self, block: BlockView<'_>) -> Result<()> {
        self.validate_block_identity(block.header.epoch, block.header.slot)?;
        for transaction in block.transaction_views() {
            self.process_transaction(transaction)?;
        }
        Ok(())
    }

    /// Process one transaction view in canonical ledger order.
    pub fn process_transaction(&mut self, transaction: TransactionView<'_>) -> Result<()> {
        self.validate_block_identity(transaction.block.epoch, transaction.block.slot)?;
        self.validate_order(transaction.block.slot, transaction.header.tx_index)?;
        let signature = transaction.primary_signature.ok_or(
            crate::Error::TransactionIdentityPrimarySignatureMissing {
                epoch: transaction.block.epoch,
                slot: transaction.block.slot,
                tx_index: transaction.header.tx_index,
            },
        )?;

        self.record_buffer[0..8].copy_from_slice(&transaction.block.slot.to_le_bytes());
        self.record_buffer[8..12].copy_from_slice(&transaction.header.tx_index.to_le_bytes());
        self.record_buffer[12..16].fill(0);
        self.record_buffer[16..80].copy_from_slice(signature);
        self.output.write_row(&self.record_buffer)?;
        self.last_slot = Some(transaction.block.slot);
        self.last_tx_index = transaction.header.tx_index;
        self.first_written_slot
            .get_or_insert(transaction.block.slot);
        self.last_written_slot = Some(transaction.block.slot);
        Ok(())
    }

    /// Flush output and return the writer with the final deterministic report.
    pub fn finish(self) -> Result<FinishedOutput<W, TransactionIdentityDumpReport>> {
        let finished = self.output.finish("blockzilla-transaction-identity/v1")?;
        Ok(FinishedOutput {
            writer: finished.writer,
            report: TransactionIdentityDumpReport {
                records: finished.report.row_count,
                output_bytes: finished.report.output_bytes,
                output_sha256: finished.report.sha256,
                first_slot: self.first_written_slot,
                last_slot: self.last_written_slot,
            },
        })
    }

    fn validate_block_identity(&self, epoch: u64, slot: u64) -> Result<()> {
        if epoch != self.epoch {
            return Err(crate::Error::TransactionIdentityEpoch {
                expected_epoch: self.epoch,
                actual_epoch: epoch,
            });
        }
        if slot < self.start_slot || slot >= self.end_slot_exclusive {
            return Err(crate::Error::TransactionIdentitySlotRange {
                slot,
                start_slot: self.start_slot,
                end_slot_exclusive: self.end_slot_exclusive,
            });
        }
        Ok(())
    }

    fn validate_order(&self, slot: u64, tx_index: u32) -> Result<()> {
        let expected_tx_index = match self.last_slot {
            None => 0,
            Some(last_slot) if slot == last_slot => {
                self.last_tx_index
                    .checked_add(1)
                    .ok_or(crate::Error::CounterOverflow(
                        "transaction identity tx index",
                    ))?
            }
            Some(last_slot) if slot > last_slot => 0,
            Some(_) => 0,
        };
        if tx_index != expected_tx_index || self.last_slot.is_some_and(|last_slot| slot < last_slot)
        {
            return Err(crate::Error::TransactionIdentityOrder {
                slot,
                tx_index,
                expected_tx_index,
            });
        }
        Ok(())
    }
}

impl<W: Write> BlockSink for TransactionIdentityDumpSink<W> {
    fn visit_block(&mut self, block: BlockView<'_>) -> blockzilla_query_sdk::Result<()> {
        self.process_block(block)
            .map_err(blockzilla_query_sdk::Error::sink)
    }
}

fn header(epoch: u64, start_slot: u64, end_slot_exclusive: u64) -> [u8; HEADER_BYTES] {
    let mut bytes = [0_u8; HEADER_BYTES];
    bytes[0..8].copy_from_slice(&TRANSACTION_IDENTITY_DUMP_MAGIC);
    bytes[8..12].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
    bytes[12..16].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
    bytes[16..20].copy_from_slice(&(RECORD_BYTES as u32).to_le_bytes());
    bytes[24..32].copy_from_slice(&epoch.to_le_bytes());
    bytes[32..40].copy_from_slice(&start_slot.to_le_bytes());
    bytes[40..48].copy_from_slice(&end_slot_exclusive.to_le_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use blockzilla_query_sdk::{
        BlockHeader, CanonicalBlock, CanonicalTransaction, CpiCoverage, ExecutionStatus,
        InstructionCoverage, TokenBalanceCoverage, TransactionHeader,
    };
    use sha2::{Digest, Sha256};

    use super::*;

    fn transaction(tx_index: u32, signature: Option<[u8; 64]>) -> CanonicalTransaction {
        CanonicalTransaction {
            header: TransactionHeader {
                tx_index,
                status: ExecutionStatus::Succeeded,
                failed_outer_instruction_index: None,
                instruction_coverage: InstructionCoverage::Complete,
                cpi_coverage: CpiCoverage::Complete,
            },
            primary_signature: signature,
            required_signers: vec![],
            instructions: vec![],
            token_balance_coverage: TokenBalanceCoverage::NotRequested,
            token_balances: vec![],
        }
    }

    fn block(epoch: u64, slot: u64, transactions: Vec<CanonicalTransaction>) -> CanonicalBlock {
        CanonicalBlock {
            header: BlockHeader {
                epoch,
                block_ordinal: 0,
                slot,
            },
            transactions,
        }
    }

    #[test]
    fn writes_exact_little_endian_header_records_and_report() {
        let first = block(
            7,
            70,
            vec![
                transaction(0, Some([0x11; 64])),
                transaction(1, Some([0x12; 64])),
            ],
        );
        let second = block(7, 72, vec![transaction(0, Some([0x13; 64]))]);
        let mut sink = TransactionIdentityDumpSink::new(Vec::new(), 7, 70, 80).unwrap();
        sink.process_block(first.as_view()).unwrap();
        sink.process_block(second.as_view()).unwrap();
        let finished = sink.finish().unwrap();

        assert_eq!(finished.report.records, 3);
        assert_eq!(
            finished.report.output_bytes,
            (HEADER_BYTES + 3 * RECORD_BYTES) as u64
        );
        assert_eq!(finished.report.first_slot, Some(70));
        assert_eq!(finished.report.last_slot, Some(72));
        let expected_sha256: [u8; 32] = Sha256::digest(&finished.writer).into();
        assert_eq!(finished.report.output_sha256, expected_sha256);

        let header = &finished.writer[..HEADER_BYTES];
        assert_eq!(&header[0..8], b"BZTXID01");
        assert_eq!(u32::from_le_bytes(header[8..12].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(header[12..16].try_into().unwrap()), 48);
        assert_eq!(u32::from_le_bytes(header[16..20].try_into().unwrap()), 80);
        assert_eq!(u32::from_le_bytes(header[20..24].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(header[24..32].try_into().unwrap()), 7);
        assert_eq!(u64::from_le_bytes(header[32..40].try_into().unwrap()), 70);
        assert_eq!(u64::from_le_bytes(header[40..48].try_into().unwrap()), 80);

        let first_record = &finished.writer[48..128];
        assert_eq!(
            u64::from_le_bytes(first_record[0..8].try_into().unwrap()),
            70
        );
        assert_eq!(
            u32::from_le_bytes(first_record[8..12].try_into().unwrap()),
            0
        );
        assert_eq!(
            u32::from_le_bytes(first_record[12..16].try_into().unwrap()),
            0
        );
        assert_eq!(&first_record[16..80], &[0x11; 64]);
    }

    #[test]
    fn rejects_noncanonical_transaction_indexes_and_slot_order() {
        let mut sink = TransactionIdentityDumpSink::new(Vec::new(), 7, 70, 80).unwrap();
        let out_of_order = block(7, 70, vec![transaction(1, Some([0x11; 64]))]);
        assert!(matches!(
            sink.process_block(out_of_order.as_view()),
            Err(crate::Error::TransactionIdentityOrder {
                expected_tx_index: 0,
                ..
            })
        ));

        sink.process_block(block(7, 72, vec![transaction(0, Some([0x11; 64]))]).as_view())
            .unwrap();
        assert!(matches!(
            sink.process_block(block(7, 71, vec![transaction(0, Some([0x12; 64]))]).as_view()),
            Err(crate::Error::TransactionIdentityOrder {
                expected_tx_index: 0,
                ..
            })
        ));
    }

    #[test]
    fn requires_primary_signature_and_header_identity() {
        let mut sink = TransactionIdentityDumpSink::new(Vec::new(), 7, 70, 80).unwrap();
        assert!(matches!(
            sink.process_block(block(7, 70, vec![transaction(0, None)]).as_view()),
            Err(crate::Error::TransactionIdentityPrimarySignatureMissing { .. })
        ));
        assert!(matches!(
            sink.process_block(block(8, 70, vec![]).as_view()),
            Err(crate::Error::TransactionIdentityEpoch { .. })
        ));
        assert!(matches!(
            sink.process_block(block(7, 80, vec![]).as_view()),
            Err(crate::Error::TransactionIdentitySlotRange { .. })
        ));
    }

    #[test]
    fn rejects_empty_or_inverted_slot_range() {
        assert!(matches!(
            TransactionIdentityDumpSink::new(Vec::new(), 7, 80, 80),
            Err(crate::Error::TransactionIdentityInvalidRange { .. })
        ));
        assert!(matches!(
            TransactionIdentityDumpSink::new(Vec::new(), 7, 81, 80),
            Err(crate::Error::TransactionIdentityInvalidRange { .. })
        ));
    }

    #[test]
    fn no_transaction_dump_has_only_header() {
        let finished = TransactionIdentityDumpSink::new(Vec::new(), 7, 70, 80)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(finished.report.records, 0);
        assert_eq!(finished.report.output_bytes, 48);
        assert_eq!(finished.report.first_slot, None);
        assert_eq!(finished.report.last_slot, None);
    }
}

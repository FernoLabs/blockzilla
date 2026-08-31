use std::io::Write;

use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// Deterministic output counts used for cross-format parity checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputReport {
    /// Stable schema name for the fixed-record stream.
    pub schema: &'static str,
    /// Number of application rows after the stream header.
    pub row_count: u64,
    /// Header and record bytes written to the output.
    pub output_bytes: u64,
}

/// Deterministic identity of transactions whose application result is partial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageReport {
    /// Transactions with one or more stable workload-specific reason bits.
    pub indeterminate_transactions: u64,
    /// SHA-256 of ordered `(epoch, slot, tx_index, reason_bits)` records.
    pub sha256: [u8; 32],
}

impl CoverageReport {
    pub const fn output_complete(&self) -> bool {
        self.indeterminate_transactions == 0
    }

    pub fn sha256_hex(&self) -> String {
        hex_lower(self.sha256)
    }
}

fn hex_lower(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

/// The returned writer and the final workload report.
#[derive(Debug)]
pub struct FinishedOutput<W, R> {
    pub writer: W,
    pub report: R,
}

impl<W, R> FinishedOutput<W, R> {
    pub fn into_parts(self) -> (W, R) {
        (self.writer, self.report)
    }
}

pub(crate) struct CanonicalOutput<W> {
    writer: W,
    output_bytes: u64,
    row_count: u64,
}

impl<W: Write> CanonicalOutput<W> {
    pub(crate) fn new(mut writer: W, header: &[u8]) -> Result<Self> {
        writer.write_all(header)?;
        Ok(Self {
            writer,
            output_bytes: u64::try_from(header.len())
                .map_err(|_| Error::CounterOverflow("output byte"))?,
            row_count: 0,
        })
    }

    pub(crate) fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.writer.write_all(row)?;
        self.output_bytes = self
            .output_bytes
            .checked_add(
                u64::try_from(row.len()).map_err(|_| Error::CounterOverflow("output byte"))?,
            )
            .ok_or(Error::CounterOverflow("output byte"))?;
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or(Error::CounterOverflow("output row"))?;
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        schema: &'static str,
    ) -> Result<FinishedOutput<W, OutputReport>> {
        self.writer.flush()?;
        Ok(FinishedOutput {
            writer: self.writer,
            report: OutputReport {
                schema,
                row_count: self.row_count,
                output_bytes: self.output_bytes,
            },
        })
    }
}

pub(crate) fn target_header(magic: [u8; 8], record_bytes: u32, target: [u8; 32]) -> [u8; 44] {
    let mut header = [0_u8; 44];
    header[..8].copy_from_slice(&magic);
    header[8..12].copy_from_slice(&record_bytes.to_be_bytes());
    header[12..].copy_from_slice(&target);
    header
}

#[derive(Debug, Default)]
pub(crate) struct TransactionOrder {
    last: Option<(u64, u64, u32)>,
}

impl TransactionOrder {
    pub(crate) fn observe(
        &mut self,
        workload: &'static str,
        transaction: blockzilla_query_sdk::TransactionView<'_>,
    ) -> Result<()> {
        let position = (
            transaction.block.epoch,
            transaction.block.slot,
            transaction.header.tx_index,
        );
        if self.last.is_some_and(|last| position <= last) {
            return Err(Error::TransactionOrder {
                workload,
                epoch: transaction.block.epoch,
                slot: transaction.block.slot,
                tx_index: transaction.header.tx_index,
            });
        }
        self.last = Some(position);
        Ok(())
    }
}

pub(crate) struct CoverageTracker {
    hasher: Sha256,
    indeterminate_transactions: u64,
}

impl Default for CoverageTracker {
    fn default() -> Self {
        Self {
            hasher: Sha256::new(),
            indeterminate_transactions: 0,
        }
    }
}

impl CoverageTracker {
    pub(crate) fn observe(
        &mut self,
        transaction: blockzilla_query_sdk::TransactionView<'_>,
        reason_bits: u8,
    ) -> Result<()> {
        debug_assert_ne!(reason_bits, 0);
        let mut row = [0_u8; 21];
        row[0..8].copy_from_slice(&transaction.block.epoch.to_be_bytes());
        row[8..16].copy_from_slice(&transaction.block.slot.to_be_bytes());
        row[16..20].copy_from_slice(&transaction.header.tx_index.to_be_bytes());
        row[20] = reason_bits;
        self.hasher.update(row);
        increment(
            &mut self.indeterminate_transactions,
            "indeterminate transaction",
        )
    }

    pub(crate) fn finish(self) -> CoverageReport {
        CoverageReport {
            indeterminate_transactions: self.indeterminate_transactions,
            sha256: self.hasher.finalize().into(),
        }
    }
}

pub(crate) fn increment(value: &mut u64, label: &'static str) -> Result<()> {
    *value = value.checked_add(1).ok_or(Error::CounterOverflow(label))?;
    Ok(())
}

pub(crate) fn add(value: &mut u64, amount: u64, label: &'static str) -> Result<()> {
    *value = value
        .checked_add(amount)
        .ok_or(Error::CounterOverflow(label))?;
    Ok(())
}

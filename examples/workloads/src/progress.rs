//! Low-cost progress for the small reader programs. No transaction data is copied.
use std::time::{Duration, Instant};

use blockzilla_model::{BlockSink, BlockView, Result};

pub struct ReadProgress {
    started: Instant,
    last_log: Instant,
    expected_blocks: Option<u64>,
    blocks: u64,
    transactions: u64,
}

impl ReadProgress {
    /// Use `None` for a reverse-index query: visited blocks do not measure full-epoch progress.
    pub fn new(expected_blocks: Option<u64>) -> Self {
        let now = Instant::now();
        eprintln!("progress=scan state=started eta_s=unknown");
        Self {
            started: now,
            last_log: now,
            expected_blocks,
            blocks: 0,
            transactions: 0,
        }
    }

    pub fn observe(&mut self, block: BlockView<'_>) {
        self.blocks += 1;
        self.transactions += block
            .counts
            .map_or(block.transactions.len() as u64, |counts| {
                counts.transactions
            });
        if self.last_log.elapsed() >= Duration::from_secs(10) {
            self.log();
            self.last_log = Instant::now();
        }
    }

    fn log(&self) {
        let seconds = self.started.elapsed().as_secs_f64().max(0.000001);
        let eta = self
            .expected_blocks
            .filter(|_| self.blocks > 0)
            .map(|total| {
                format!(
                    "{:.1}",
                    total.saturating_sub(self.blocks) as f64 * seconds / self.blocks as f64
                )
            })
            .unwrap_or_else(|| "unknown".into());
        eprintln!(
            "progress=scan blocks={} transactions={} elapsed_s={seconds:.1} blocks_s={:.1} tps={:.1} eta_s={eta}",
            self.blocks,
            self.transactions,
            self.blocks as f64 / seconds,
            self.transactions as f64 / seconds
        );
    }
}

impl Drop for ReadProgress {
    fn drop(&mut self) {
        self.log();
    }
}

/// A borrowed wrapper leaves the application's sink and output unchanged.
pub struct ProgressSink<'a, S> {
    sink: &'a mut S,
    progress: ReadProgress,
}

impl<'a, S> ProgressSink<'a, S> {
    pub fn new(sink: &'a mut S, expected_blocks: u64) -> Self {
        Self {
            sink,
            progress: ReadProgress::new(Some(expected_blocks)),
        }
    }
}

impl<S: BlockSink> BlockSink for ProgressSink<'_, S> {
    fn visit_block(&mut self, block: BlockView<'_>) -> Result<()> {
        self.sink.visit_block(block)?;
        self.progress.observe(block);
        Ok(())
    }
}

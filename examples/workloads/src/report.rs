//! Shared summary counters and text output for reader workload examples.

use crate::{CoverageReport, FirewatchReport, OutputReport, PumpReport, UsdcReport};

/// Counters common to the three reader workload reports.
pub trait ExampleReport {
    fn workload(&self) -> &'static str;
    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool);
    fn print_details(&self);
}

impl ExampleReport for UsdcReport {
    fn workload(&self) -> &'static str {
        "usdc-recorded-balances"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} matching_transactions={} pre_rows={} post_rows={} token_balances_unavailable_transactions={} token_mint_unavailable_transactions={} skipped_failed_transactions={}",
            self.workload(),
            self.matching_transactions,
            self.pre_rows,
            self.post_rows,
            self.token_balances_unavailable_transactions,
            self.token_mint_unavailable_transactions,
            self.skipped_failed_transactions,
        );
    }
}

impl ExampleReport for PumpReport {
    fn workload(&self) -> &'static str {
        "pumpfun-transactions"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} matching_transactions={} written_transactions={} direct_invocations={} cpi_invocations={} incomplete_instruction_transactions={} incomplete_cpi_transactions={} matches_without_primary_signature={} skipped_failed_transactions={}",
            self.workload(),
            self.matching_transactions,
            self.written_transactions,
            self.direct_invocations,
            self.cpi_invocations,
            self.incomplete_instruction_transactions,
            self.incomplete_cpi_transactions,
            self.matches_without_primary_signature,
            self.skipped_failed_transactions,
        );
    }
}

impl ExampleReport for FirewatchReport {
    fn workload(&self) -> &'static str {
        "firewatch-wallet-programs"
    }

    fn common(&self) -> (u64, u64, OutputReport, CoverageReport, bool) {
        (
            self.blocks_seen,
            self.transactions_seen,
            self.output,
            self.coverage,
            self.output_complete,
        )
    }

    fn print_details(&self) {
        println!(
            "workload={} signer_transactions={} successful_signer_transactions={} failed_signer_transactions={} unknown_execution_signer_transactions={} reached_instructions={} distinct_programs={} incomplete_instruction_transactions={} incomplete_cpi_transactions={}",
            self.workload(),
            self.signer_transactions,
            self.successful_signer_transactions,
            self.failed_signer_transactions,
            self.unknown_execution_signer_transactions,
            self.reached_instructions,
            self.distinct_programs,
            self.incomplete_instruction_transactions,
            self.incomplete_cpi_transactions,
        );
    }
}

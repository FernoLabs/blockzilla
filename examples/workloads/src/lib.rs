//! Small, format-neutral application workloads for archive reader examples.
//!
//! Reader examples keep source setup in their own binary. This crate contains
//! only the application rules and canonical output. This split keeps each
//! example small while all formats prove parity with the same record bytes.

mod error;
mod firewatch;
mod output;
mod progress;
mod pump;
mod report;
pub mod transaction_identity;
mod usdc;

pub use error::{Error, Result};
pub use firewatch::{
    FIREWATCH_COVERAGE_INCOMPLETE_CPI, FIREWATCH_COVERAGE_INCOMPLETE_INSTRUCTIONS,
    FIREWATCH_COVERAGE_UNKNOWN_EXECUTION, FIREWATCH_HEADER_BYTES, FIREWATCH_RECORD_BYTES,
    FirewatchReport, FirewatchSink, firewatch_scan_request,
};
pub use output::{CoverageReport, FinishedOutput, OutputReport};
pub use progress::{ProgressSink, ReadProgress};
pub use pump::{
    MAINNET_PUMP_FUN_PROGRAM, MAINNET_PUMP_FUN_PROGRAM_BASE58, PUMP_COVERAGE_INCOMPLETE_CPI,
    PUMP_COVERAGE_INCOMPLETE_INSTRUCTIONS, PUMP_COVERAGE_PRIMARY_SIGNATURE_UNAVAILABLE,
    PUMP_HEADER_BYTES, PUMP_RECORD_BYTES, PumpReport, PumpSink, pump_scan_request,
};
pub use report::ExampleReport;
pub use transaction_identity::{
    HEADER_BYTES, HEADER_BYTES as TRANSACTION_IDENTITY_DUMP_HEADER_BYTES, RECORD_BYTES,
    RECORD_BYTES as TRANSACTION_IDENTITY_DUMP_RECORD_BYTES, SCHEMA_VERSION,
    SCHEMA_VERSION as TRANSACTION_IDENTITY_DUMP_SCHEMA_VERSION, TransactionIdentityDumpReport,
    TransactionIdentityDumpSink,
};
pub use usdc::{
    MAINNET_USDC_MINT, MAINNET_USDC_MINT_BASE58, USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE,
    USDC_COVERAGE_TOKEN_MINT_UNAVAILABLE, USDC_HEADER_BYTES, USDC_RECORD_BYTES, UsdcBalanceSink,
    UsdcReport, usdc_scan_request,
};

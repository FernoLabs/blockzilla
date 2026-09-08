//! Small, format-neutral application workloads for archive reader examples.
//!
//! Reader examples keep source setup in their own binary. This crate contains
//! only the application rules and canonical output. This split keeps each
//! example small while all formats prove parity with the same record bytes.

mod error;
mod output;
mod progress;
mod pump;
mod report;
pub mod transaction_identity;
mod transport;
mod usdc;
mod usdc_indexed;
mod user_program_index;

pub use error::{Error, Result};
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
pub use transport::transport_metrics;
pub use usdc::{
    MAINNET_USDC_MINT, MAINNET_USDC_MINT_BASE58, USDC_COVERAGE_TOKEN_BALANCES_UNAVAILABLE,
    USDC_COVERAGE_TOKEN_MINT_UNAVAILABLE, USDC_HEADER_BYTES, USDC_RECORD_BYTES, UsdcBalanceSink,
    UsdcReport, usdc_scan_request,
};
pub use usdc_indexed::{
    INDEXED_USDC_DICTIONARY_RECORD_BYTES, INDEXED_USDC_HEADER_BYTES, INDEXED_USDC_INLINE_ID_START,
    INDEXED_USDC_RECORD_BYTES, IndexedUsdcBalanceSink, expand_indexed_usdc,
};
pub use user_program_index::{
    USER_PROGRAM_INDEX_COVERAGE_INCOMPLETE_CPI,
    USER_PROGRAM_INDEX_COVERAGE_INCOMPLETE_INSTRUCTIONS,
    USER_PROGRAM_INDEX_COVERAGE_UNKNOWN_EXECUTION, USER_PROGRAM_INDEX_HEADER_BYTES,
    USER_PROGRAM_INDEX_RECORD_BYTES, UserProgramIndexReport, UserProgramIndexSink,
    user_program_index_scan_request,
};

//! Exact, allocation-stable DEX parser coverage over a consolidated dump.

use std::{
    fs,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::ARCHIVE_V2_TX_FLAG_HAS_ERROR;
use blockzilla_dex_parser::{
    DecodeOutcome, DispatchTable, Evidence, InstructionClass, MalformedReason,
    PARSER_IMPLEMENTATION_FINGERPRINT, PARSER_SEMANTIC_VERSION, PROGRAM_SPECS, Program,
    ProgramRole,
};
use blockzilla_read_sdk::{
    ArchiveV2MetadataProjectionLimits, BorrowedArchiveV2InnerTokenInstruction,
    LogPayloadValidation, MAX_MESSAGE_ACCOUNTS,
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::*;
use crate::{
    consolidated_posting_projection::{
        ConsolidatedPostingProjectionScratch, project_consolidated_transaction_postings,
    },
    consolidated_reader::ExactMetadataSchemaSelection,
};

const COVERAGE_SCHEMA_VERSION: u32 = 1;
const PROGRAM_COUNT: usize = PROGRAM_SPECS.len();

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct MalformedCounters {
    data_too_short: u64,
    accounts_too_short: u64,
    invalid_data: u64,
}

impl MalformedCounters {
    fn record(&mut self, reason: MalformedReason) {
        match reason {
            MalformedReason::InstructionDataTooShort { .. } => self.data_too_short += 1,
            MalformedReason::InstructionAccountsTooShort { .. } => self.accounts_too_short += 1,
            MalformedReason::InvalidInstructionData { .. } => self.invalid_data += 1,
        }
    }

    fn checked_add_assign(&mut self, other: Self) -> Result<()> {
        checked_add_assign(
            &mut self.data_too_short,
            other.data_too_short,
            "malformed data",
        )?;
        checked_add_assign(
            &mut self.accounts_too_short,
            other.accounts_too_short,
            "malformed accounts",
        )?;
        checked_add_assign(&mut self.invalid_data, other.invalid_data, "invalid data")
    }

    fn total(self) -> Result<u64> {
        self.data_too_short
            .checked_add(self.accounts_too_short)
            .and_then(|value| value.checked_add(self.invalid_data))
            .context("malformed reason count overflow")
    }
}

/// Coverage for supported program-address hits. Every hit has exactly one
/// parser outcome. Class fields apply only to decoded outcomes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct InstructionCoverageCounters {
    supported_address_hits: u64,
    decoded: u64,
    unsupported_discriminator: u64,
    malformed: u64,
    missing_instruction_data: u64,
    malformed_reasons: MalformedCounters,
    semantic_decoded: u64,
    structural_only_decoded: u64,
    swaps: u64,
    routes: u64,
    orders: u64,
    semantic_swaps: u64,
}

impl InstructionCoverageCounters {
    fn record(&mut self, outcome: CoverageOutcome) {
        self.supported_address_hits += 1;
        match outcome {
            CoverageOutcome::Parser(DecodeOutcome::Decoded(decoded)) => {
                self.decoded += 1;
                let structural = decoded.evidence.contains(Evidence::STRUCTURAL_ONLY);
                if structural {
                    self.structural_only_decoded += 1;
                } else {
                    self.semantic_decoded += 1;
                }
                match decoded.class {
                    InstructionClass::Swap(_) => {
                        self.swaps += 1;
                        if !structural {
                            self.semantic_swaps += 1;
                        }
                    }
                    InstructionClass::Route => self.routes += 1,
                    InstructionClass::Order(_) => self.orders += 1,
                }
            }
            CoverageOutcome::Parser(DecodeOutcome::Unsupported { .. }) => {
                self.unsupported_discriminator += 1;
            }
            CoverageOutcome::Parser(DecodeOutcome::Malformed(reason)) => {
                self.malformed += 1;
                self.malformed_reasons.record(reason);
            }
            CoverageOutcome::MissingInstructionData => self.missing_instruction_data += 1,
            CoverageOutcome::Parser(DecodeOutcome::UnknownProgram) => {
                unreachable!("a resolved parser program cannot decode as unknown")
            }
        }
    }

    fn checked_add_assign(&mut self, other: Self) -> Result<()> {
        checked_add_assign(
            &mut self.supported_address_hits,
            other.supported_address_hits,
            "supported address hit",
        )?;
        checked_add_assign(&mut self.decoded, other.decoded, "decoded instruction")?;
        checked_add_assign(
            &mut self.unsupported_discriminator,
            other.unsupported_discriminator,
            "unsupported discriminator",
        )?;
        checked_add_assign(
            &mut self.malformed,
            other.malformed,
            "malformed instruction",
        )?;
        checked_add_assign(
            &mut self.missing_instruction_data,
            other.missing_instruction_data,
            "missing instruction data",
        )?;
        self.malformed_reasons
            .checked_add_assign(other.malformed_reasons)?;
        checked_add_assign(
            &mut self.semantic_decoded,
            other.semantic_decoded,
            "semantic decoded instruction",
        )?;
        checked_add_assign(
            &mut self.structural_only_decoded,
            other.structural_only_decoded,
            "structural-only instruction",
        )?;
        checked_add_assign(&mut self.swaps, other.swaps, "swap instruction")?;
        checked_add_assign(&mut self.routes, other.routes, "route instruction")?;
        checked_add_assign(&mut self.orders, other.orders, "order instruction")?;
        checked_add_assign(
            &mut self.semantic_swaps,
            other.semantic_swaps,
            "semantic swap instruction",
        )?;
        Ok(())
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.decoded
                .checked_add(self.unsupported_discriminator)
                .and_then(|value| value.checked_add(self.malformed))
                .and_then(|value| value.checked_add(self.missing_instruction_data))
                == Some(self.supported_address_hits),
            "parser outcomes do not partition supported address hits"
        );
        ensure!(
            self.semantic_decoded
                .checked_add(self.structural_only_decoded)
                == Some(self.decoded),
            "semantic and structural classifications do not partition decoded instructions"
        );
        ensure!(
            self.swaps
                .checked_add(self.routes)
                .and_then(|value| value.checked_add(self.orders))
                == Some(self.decoded),
            "instruction classes do not partition decoded instructions"
        );
        ensure!(
            self.semantic_swaps <= self.swaps && self.malformed_reasons.total()? == self.malformed,
            "instruction coverage detail counters are inconsistent"
        );
        Ok(())
    }
}

/// Role and class counts from each successful `DecodedInstruction`. The role
/// comes from the decoded result, not from the nominal program specification.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct DecodedInstructionCounters {
    decoded: u64,
    semantic_decoded: u64,
    structural_only_decoded: u64,
    swaps: u64,
    routes: u64,
    orders: u64,
    semantic_swaps: u64,
}

impl DecodedInstructionCounters {
    fn record(&mut self, decoded: blockzilla_dex_parser::DecodedInstruction) {
        self.decoded += 1;
        let structural = decoded.evidence.contains(Evidence::STRUCTURAL_ONLY);
        if structural {
            self.structural_only_decoded += 1;
        } else {
            self.semantic_decoded += 1;
        }
        match decoded.class {
            InstructionClass::Swap(_) => {
                self.swaps += 1;
                if !structural {
                    self.semantic_swaps += 1;
                }
            }
            InstructionClass::Route => self.routes += 1,
            InstructionClass::Order(_) => self.orders += 1,
        }
    }

    fn checked_add_assign(&mut self, other: Self) -> Result<()> {
        checked_add_assign(&mut self.decoded, other.decoded, "decoded role instruction")?;
        checked_add_assign(
            &mut self.semantic_decoded,
            other.semantic_decoded,
            "semantic decoded role instruction",
        )?;
        checked_add_assign(
            &mut self.structural_only_decoded,
            other.structural_only_decoded,
            "structural-only decoded role instruction",
        )?;
        checked_add_assign(&mut self.swaps, other.swaps, "decoded role swap")?;
        checked_add_assign(&mut self.routes, other.routes, "decoded role route")?;
        checked_add_assign(&mut self.orders, other.orders, "decoded role order")?;
        checked_add_assign(
            &mut self.semantic_swaps,
            other.semantic_swaps,
            "decoded role semantic swap",
        )
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.semantic_decoded
                .checked_add(self.structural_only_decoded)
                == Some(self.decoded)
                && self
                    .swaps
                    .checked_add(self.routes)
                    .and_then(|value| value.checked_add(self.orders))
                    == Some(self.decoded)
                && self.semantic_swaps <= self.swaps,
            "decoded role counters are inconsistent"
        );
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct OriginInstructionCounters {
    outer: InstructionCoverageCounters,
    inner: InstructionCoverageCounters,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct StatusInstructionCounters {
    successful_transactions: InstructionCoverageCounters,
    failed_transactions: InstructionCoverageCounters,
    unknown_status_transactions: InstructionCoverageCounters,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct NominalRoleInstructionCounters {
    router_programs: InstructionCoverageCounters,
    venue_programs: InstructionCoverageCounters,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct DecodedRoleCounters {
    router: DecodedInstructionCounters,
    venue: DecodedInstructionCounters,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct InstructionOccurrenceCounters {
    all: u64,
    outer: u64,
    inner: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct TransactionCoverageCounters {
    scanned: u64,
    successful: u64,
    failed: u64,
    unknown_status: u64,
    candidate: u64,
    candidate_successful: u64,
    candidate_failed: u64,
    candidate_unknown_status: u64,
    decoded: u64,
    decoded_successful: u64,
    decoded_failed: u64,
    decoded_unknown_status: u64,
    semantic_venue_swap: u64,
    semantic_venue_swap_successful: u64,
    semantic_venue_swap_failed: u64,
    semantic_venue_swap_unknown_status: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct ProgramTransactionCounters {
    candidate: u64,
    candidate_successful: u64,
    candidate_failed: u64,
    candidate_unknown_status: u64,
    decoded: u64,
    decoded_successful: u64,
    decoded_failed: u64,
    decoded_unknown_status: u64,
    semantic_venue_swap: u64,
    semantic_venue_swap_successful: u64,
    semantic_venue_swap_failed: u64,
    semantic_venue_swap_unknown_status: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Successful,
    Failed,
    Unknown,
}

fn transaction_status(record: &BorrowedTransactionRecord<'_>) -> TransactionStatus {
    if record.metadata_bytes.is_empty() {
        TransactionStatus::Unknown
    } else if record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
        TransactionStatus::Failed
    } else {
        TransactionStatus::Successful
    }
}

impl ProgramTransactionCounters {
    fn record(
        &mut self,
        status: TransactionStatus,
        instruction: InstructionCoverageCounters,
        has_semantic_venue_swap: bool,
    ) -> Result<()> {
        checked_increment(&mut self.candidate, "per-program candidate transaction")?;
        match status {
            TransactionStatus::Successful => checked_increment(
                &mut self.candidate_successful,
                "per-program successful candidate transaction",
            )?,
            TransactionStatus::Failed => checked_increment(
                &mut self.candidate_failed,
                "per-program failed candidate transaction",
            )?,
            TransactionStatus::Unknown => checked_increment(
                &mut self.candidate_unknown_status,
                "per-program unknown-status candidate transaction",
            )?,
        }
        if instruction.decoded != 0 {
            checked_increment(&mut self.decoded, "per-program decoded transaction")?;
            match status {
                TransactionStatus::Successful => checked_increment(
                    &mut self.decoded_successful,
                    "per-program successful decoded transaction",
                )?,
                TransactionStatus::Failed => checked_increment(
                    &mut self.decoded_failed,
                    "per-program failed decoded transaction",
                )?,
                TransactionStatus::Unknown => checked_increment(
                    &mut self.decoded_unknown_status,
                    "per-program unknown-status decoded transaction",
                )?,
            }
        }
        if has_semantic_venue_swap {
            checked_increment(
                &mut self.semantic_venue_swap,
                "per-program semantic venue-swap transaction",
            )?;
            match status {
                TransactionStatus::Successful => checked_increment(
                    &mut self.semantic_venue_swap_successful,
                    "per-program successful semantic venue-swap transaction",
                )?,
                TransactionStatus::Failed => checked_increment(
                    &mut self.semantic_venue_swap_failed,
                    "per-program failed semantic venue-swap transaction",
                )?,
                TransactionStatus::Unknown => checked_increment(
                    &mut self.semantic_venue_swap_unknown_status,
                    "per-program unknown-status semantic venue-swap transaction",
                )?,
            }
        }
        Ok(())
    }

    fn validate(self) -> Result<()> {
        ensure!(
            self.candidate_successful
                .checked_add(self.candidate_failed)
                .and_then(|value| value.checked_add(self.candidate_unknown_status))
                == Some(self.candidate)
                && self
                    .decoded_successful
                    .checked_add(self.decoded_failed)
                    .and_then(|value| value.checked_add(self.decoded_unknown_status))
                    == Some(self.decoded)
                && self
                    .semantic_venue_swap_successful
                    .checked_add(self.semantic_venue_swap_failed)
                    .and_then(|value| {
                        value.checked_add(self.semantic_venue_swap_unknown_status)
                    })
                    == Some(self.semantic_venue_swap)
                && self.semantic_venue_swap <= self.decoded
                && self.decoded <= self.candidate,
            "per-program transaction counters are inconsistent"
        );
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct StagedProgramCounters {
    all: InstructionCoverageCounters,
    outer: InstructionCoverageCounters,
    inner: InstructionCoverageCounters,
    has_semantic_venue_swap: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct ProgramCoverageAccumulator {
    all: InstructionCoverageCounters,
    outer: InstructionCoverageCounters,
    inner: InstructionCoverageCounters,
    successful: InstructionCoverageCounters,
    failed: InstructionCoverageCounters,
    unknown_status: InstructionCoverageCounters,
    transactions: ProgramTransactionCounters,
}

#[derive(Debug)]
struct TransactionStage {
    occurrences: InstructionOccurrenceCounters,
    all: InstructionCoverageCounters,
    origin: OriginInstructionCounters,
    nominal_role: NominalRoleInstructionCounters,
    decoded_role: DecodedRoleCounters,
    programs: [StagedProgramCounters; PROGRAM_COUNT],
    touched_programs: [u8; PROGRAM_COUNT],
    touched_program_count: usize,
    callback_invalid: bool,
}

impl TransactionStage {
    fn new() -> Self {
        Self {
            occurrences: InstructionOccurrenceCounters::default(),
            all: InstructionCoverageCounters::default(),
            origin: OriginInstructionCounters::default(),
            nominal_role: NominalRoleInstructionCounters::default(),
            decoded_role: DecodedRoleCounters::default(),
            programs: [StagedProgramCounters::default(); PROGRAM_COUNT],
            touched_programs: [0; PROGRAM_COUNT],
            touched_program_count: 0,
            callback_invalid: false,
        }
    }

    fn begin(&mut self) {
        for &index in &self.touched_programs[..self.touched_program_count] {
            self.programs[usize::from(index)] = StagedProgramCounters::default();
        }
        self.touched_program_count = 0;
        self.occurrences = InstructionOccurrenceCounters::default();
        self.all = InstructionCoverageCounters::default();
        self.origin = OriginInstructionCounters::default();
        self.nominal_role = NominalRoleInstructionCounters::default();
        self.decoded_role = DecodedRoleCounters::default();
        self.callback_invalid = false;
    }

    fn record_occurrence(&mut self, origin: InstructionOrigin) {
        self.occurrences.all += 1;
        match origin {
            InstructionOrigin::Outer => self.occurrences.outer += 1,
            InstructionOrigin::Inner => self.occurrences.inner += 1,
        }
    }

    fn record_supported(
        &mut self,
        program: Program,
        origin: InstructionOrigin,
        outcome: CoverageOutcome,
    ) {
        let program_index = program as usize;
        if program_index >= PROGRAM_COUNT || PROGRAM_SPECS[program_index].program != program {
            self.callback_invalid = true;
            return;
        }
        if self.programs[program_index].all.supported_address_hits == 0 {
            self.touched_programs[self.touched_program_count] = program_index as u8;
            self.touched_program_count += 1;
        }

        self.all.record(outcome);
        match origin {
            InstructionOrigin::Outer => self.origin.outer.record(outcome),
            InstructionOrigin::Inner => self.origin.inner.record(outcome),
        }
        match program.role() {
            ProgramRole::Router => self.nominal_role.router_programs.record(outcome),
            ProgramRole::Venue => self.nominal_role.venue_programs.record(outcome),
        }

        let program_counters = &mut self.programs[program_index];
        program_counters.all.record(outcome);
        match origin {
            InstructionOrigin::Outer => program_counters.outer.record(outcome),
            InstructionOrigin::Inner => program_counters.inner.record(outcome),
        }

        if let CoverageOutcome::Parser(DecodeOutcome::Decoded(decoded)) = outcome {
            match decoded.role {
                ProgramRole::Router => self.decoded_role.router.record(decoded),
                ProgramRole::Venue => self.decoded_role.venue.record(decoded),
            }
            if decoded.role == ProgramRole::Venue
                && matches!(decoded.class, InstructionClass::Swap(_))
                && !decoded.evidence.contains(Evidence::STRUCTURAL_ONLY)
            {
                program_counters.has_semantic_venue_swap = true;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum InstructionOrigin {
    Outer,
    Inner,
}

#[derive(Debug, Clone, Copy)]
enum CoverageOutcome {
    Parser(DecodeOutcome),
    /// A structured Archive V2 message payload has no borrowed raw DEX bytes.
    /// It is not equivalent to a real empty raw instruction.
    MissingInstructionData,
}

#[derive(Debug, Default)]
struct CoverageAccumulator {
    transactions: TransactionCoverageCounters,
    occurrences: InstructionOccurrenceCounters,
    all: InstructionCoverageCounters,
    origin: OriginInstructionCounters,
    status: StatusInstructionCounters,
    nominal_role: NominalRoleInstructionCounters,
    decoded_role: DecodedRoleCounters,
    programs: Vec<ProgramCoverageAccumulator>,
}

impl CoverageAccumulator {
    fn new() -> Self {
        Self {
            programs: vec![ProgramCoverageAccumulator::default(); PROGRAM_COUNT],
            ..Self::default()
        }
    }

    fn commit_transaction(
        &mut self,
        status: TransactionStatus,
        stage: &TransactionStage,
    ) -> Result<()> {
        checked_increment(&mut self.transactions.scanned, "scanned transaction")?;
        match status {
            TransactionStatus::Successful => {
                checked_increment(&mut self.transactions.successful, "successful transaction")?;
            }
            TransactionStatus::Failed => {
                checked_increment(&mut self.transactions.failed, "failed transaction")?;
            }
            TransactionStatus::Unknown => checked_increment(
                &mut self.transactions.unknown_status,
                "unknown-status transaction",
            )?,
        }

        let candidate = stage.all.supported_address_hits != 0;
        let decoded = stage.all.decoded != 0;
        let semantic_venue_swap = stage.decoded_role.venue.semantic_swaps != 0;
        record_transaction_union(
            &mut self.transactions,
            status,
            candidate,
            decoded,
            semantic_venue_swap,
        )?;

        checked_add_assign(
            &mut self.occurrences.all,
            stage.occurrences.all,
            "all instruction occurrence",
        )?;
        checked_add_assign(
            &mut self.occurrences.outer,
            stage.occurrences.outer,
            "outer instruction occurrence",
        )?;
        checked_add_assign(
            &mut self.occurrences.inner,
            stage.occurrences.inner,
            "inner instruction occurrence",
        )?;
        self.all.checked_add_assign(stage.all)?;
        self.origin.outer.checked_add_assign(stage.origin.outer)?;
        self.origin.inner.checked_add_assign(stage.origin.inner)?;
        match status {
            TransactionStatus::Successful => self
                .status
                .successful_transactions
                .checked_add_assign(stage.all)?,
            TransactionStatus::Failed => self
                .status
                .failed_transactions
                .checked_add_assign(stage.all)?,
            TransactionStatus::Unknown => self
                .status
                .unknown_status_transactions
                .checked_add_assign(stage.all)?,
        }
        self.nominal_role
            .router_programs
            .checked_add_assign(stage.nominal_role.router_programs)?;
        self.nominal_role
            .venue_programs
            .checked_add_assign(stage.nominal_role.venue_programs)?;
        self.decoded_role
            .router
            .checked_add_assign(stage.decoded_role.router)?;
        self.decoded_role
            .venue
            .checked_add_assign(stage.decoded_role.venue)?;

        for &index in &stage.touched_programs[..stage.touched_program_count] {
            let index = usize::from(index);
            let staged = stage.programs[index];
            let program = &mut self.programs[index];
            program.all.checked_add_assign(staged.all)?;
            program.outer.checked_add_assign(staged.outer)?;
            program.inner.checked_add_assign(staged.inner)?;
            match status {
                TransactionStatus::Successful => {
                    program.successful.checked_add_assign(staged.all)?;
                }
                TransactionStatus::Failed => program.failed.checked_add_assign(staged.all)?,
                TransactionStatus::Unknown => {
                    program.unknown_status.checked_add_assign(staged.all)?;
                }
            }
            program
                .transactions
                .record(status, staged.all, staged.has_semantic_venue_swap)?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.transactions
                .successful
                .checked_add(self.transactions.failed)
                .and_then(|value| value.checked_add(self.transactions.unknown_status))
                == Some(self.transactions.scanned),
            "transaction status counts do not partition scanned transactions"
        );
        ensure!(
            self.transactions
                .candidate_successful
                .checked_add(self.transactions.candidate_failed)
                .and_then(|value| value.checked_add(self.transactions.candidate_unknown_status))
                == Some(self.transactions.candidate)
                && self
                    .transactions
                    .decoded_successful
                    .checked_add(self.transactions.decoded_failed)
                    .and_then(|value| value.checked_add(self.transactions.decoded_unknown_status))
                    == Some(self.transactions.decoded)
                && self
                    .transactions
                    .semantic_venue_swap_successful
                    .checked_add(self.transactions.semantic_venue_swap_failed)
                    .and_then(|value| {
                        value.checked_add(self.transactions.semantic_venue_swap_unknown_status)
                    })
                    == Some(self.transactions.semantic_venue_swap)
                && self.transactions.semantic_venue_swap <= self.transactions.decoded
                && self.transactions.decoded <= self.transactions.candidate,
            "transaction union counters are inconsistent"
        );
        ensure!(
            self.occurrences.outer + self.occurrences.inner == self.occurrences.all,
            "outer and inner occurrences do not cover all instructions"
        );
        for counters in [
            self.all,
            self.origin.outer,
            self.origin.inner,
            self.status.successful_transactions,
            self.status.failed_transactions,
            self.status.unknown_status_transactions,
            self.nominal_role.router_programs,
            self.nominal_role.venue_programs,
        ] {
            counters.validate()?;
        }
        ensure!(
            sum_instruction_counters(self.origin.outer, self.origin.inner)? == self.all
                && sum_three_instruction_counters(
                    self.status.successful_transactions,
                    self.status.failed_transactions,
                    self.status.unknown_status_transactions,
                )? == self.all
                && sum_instruction_counters(
                    self.nominal_role.router_programs,
                    self.nominal_role.venue_programs,
                )? == self.all,
            "instruction coverage dimensions do not reproduce aggregate coverage"
        );
        self.decoded_role.router.validate()?;
        self.decoded_role.venue.validate()?;
        let mut decoded_role_sum = self.decoded_role.router;
        decoded_role_sum.checked_add_assign(self.decoded_role.venue)?;
        let expected_decoded_roles = DecodedInstructionCounters {
            decoded: self.all.decoded,
            semantic_decoded: self.all.semantic_decoded,
            structural_only_decoded: self.all.structural_only_decoded,
            swaps: self.all.swaps,
            routes: self.all.routes,
            orders: self.all.orders,
            semantic_swaps: self.all.semantic_swaps,
        };
        ensure!(
            decoded_role_sum == expected_decoded_roles,
            "decoded roles do not partition all decoded instruction classifications"
        );
        for program in &self.programs {
            program.all.validate()?;
            program.outer.validate()?;
            program.inner.validate()?;
            program.successful.validate()?;
            program.failed.validate()?;
            program.unknown_status.validate()?;
            program.transactions.validate()?;
            ensure!(
                sum_instruction_counters(program.outer, program.inner)? == program.all
                    && sum_three_instruction_counters(
                        program.successful,
                        program.failed,
                        program.unknown_status,
                    )? == program.all,
                "per-program dimensions do not reproduce aggregate coverage"
            );
        }
        let mut program_sum = InstructionCoverageCounters::default();
        for program in &self.programs {
            program_sum.checked_add_assign(program.all)?;
        }
        ensure!(
            program_sum == self.all,
            "per-program rows do not reproduce aggregate coverage"
        );
        Ok(())
    }
}

fn record_transaction_union(
    counters: &mut TransactionCoverageCounters,
    status: TransactionStatus,
    candidate: bool,
    decoded: bool,
    semantic_venue_swap: bool,
) -> Result<()> {
    if candidate {
        checked_increment(&mut counters.candidate, "candidate transaction")?;
        match status {
            TransactionStatus::Successful => checked_increment(
                &mut counters.candidate_successful,
                "successful candidate transaction",
            )?,
            TransactionStatus::Failed => checked_increment(
                &mut counters.candidate_failed,
                "failed candidate transaction",
            )?,
            TransactionStatus::Unknown => checked_increment(
                &mut counters.candidate_unknown_status,
                "unknown-status candidate transaction",
            )?,
        }
    }
    if decoded {
        checked_increment(&mut counters.decoded, "decoded transaction")?;
        match status {
            TransactionStatus::Successful => checked_increment(
                &mut counters.decoded_successful,
                "successful decoded transaction",
            )?,
            TransactionStatus::Failed => {
                checked_increment(&mut counters.decoded_failed, "failed decoded transaction")?;
            }
            TransactionStatus::Unknown => checked_increment(
                &mut counters.decoded_unknown_status,
                "unknown-status decoded transaction",
            )?,
        }
    }
    if semantic_venue_swap {
        checked_increment(
            &mut counters.semantic_venue_swap,
            "semantic venue-swap transaction",
        )?;
        match status {
            TransactionStatus::Successful => checked_increment(
                &mut counters.semantic_venue_swap_successful,
                "successful semantic venue-swap transaction",
            )?,
            TransactionStatus::Failed => checked_increment(
                &mut counters.semantic_venue_swap_failed,
                "failed semantic venue-swap transaction",
            )?,
            TransactionStatus::Unknown => checked_increment(
                &mut counters.semantic_venue_swap_unknown_status,
                "unknown-status semantic venue-swap transaction",
            )?,
        }
    }
    Ok(())
}

fn checked_add_assign(target: &mut u64, value: u64, label: &'static str) -> Result<()> {
    *target = target
        .checked_add(value)
        .with_context(|| format!("{label} count overflow"))?;
    Ok(())
}

fn sum_instruction_counters(
    left: InstructionCoverageCounters,
    right: InstructionCoverageCounters,
) -> Result<InstructionCoverageCounters> {
    let mut sum = left;
    sum.checked_add_assign(right)?;
    Ok(sum)
}

fn sum_three_instruction_counters(
    first: InstructionCoverageCounters,
    second: InstructionCoverageCounters,
    third: InstructionCoverageCounters,
) -> Result<InstructionCoverageCounters> {
    let mut sum = first;
    sum.checked_add_assign(second)?;
    sum.checked_add_assign(third)?;
    Ok(sum)
}

struct InstructionClassifier<'a> {
    dispatch: &'a DispatchTable,
    resolved_accounts: &'a [u32],
    account_scratch: &'a mut [u32; MAX_MESSAGE_ACCOUNTS],
}

impl InstructionClassifier<'_> {
    fn classify(
        &mut self,
        stage: &mut TransactionStage,
        program_id_index: usize,
        account_indices: &[u8],
        data: Option<&[u8]>,
        origin: InstructionOrigin,
    ) {
        stage.record_occurrence(origin);
        let Some(&program_registry_id) = self.resolved_accounts.get(program_id_index) else {
            stage.callback_invalid = true;
            return;
        };
        let Some(program) = self.dispatch.program(program_registry_id) else {
            return;
        };
        if account_indices.len() > self.account_scratch.len() {
            stage.callback_invalid = true;
            return;
        }
        for (destination, &index) in self.account_scratch.iter_mut().zip(account_indices) {
            let Some(&registry_id) = self.resolved_accounts.get(usize::from(index)) else {
                stage.callback_invalid = true;
                return;
            };
            *destination = registry_id;
        }
        let outcome = match data {
            Some(data) => {
                let outcome = self.dispatch.decode(
                    program_registry_id,
                    data,
                    &self.account_scratch[..account_indices.len()],
                );
                if matches!(outcome, DecodeOutcome::UnknownProgram) {
                    stage.callback_invalid = true;
                    return;
                }
                if matches!(outcome, DecodeOutcome::Decoded(decoded) if decoded.program != program)
                {
                    stage.callback_invalid = true;
                    return;
                }
                CoverageOutcome::Parser(outcome)
            }
            None => CoverageOutcome::MissingInstructionData,
        };
        stage.record_supported(program, origin, outcome);
    }
}

#[derive(Debug, Serialize)]
struct CoverageSource {
    mint: String,
    manifest_sha256: String,
    transaction_stream_sha256: String,
    pubkey_registry_sha256: String,
    transactions: u64,
    signatures: u64,
    registry_entries: u32,
    first_epoch: u64,
    last_epoch: u64,
}

#[derive(Debug, Serialize)]
struct ParserSetSummary {
    semantic_version: &'static str,
    implementation_fingerprint: &'static str,
    programs: usize,
    programs_present_in_registry: u64,
    programs_absent_from_registry: u64,
    programs_observed_with_supported_address_hits: u64,
}

#[derive(Debug, Serialize)]
struct GeneratorProvenance {
    crate_name: &'static str,
    crate_version: &'static str,
    executable_sha256: String,
}

#[derive(Debug, Serialize)]
struct CoverageFraction {
    numerator: u64,
    denominator: u64,
    percent: f64,
}

impl CoverageFraction {
    fn new(numerator: u64, denominator: u64) -> Self {
        Self {
            numerator,
            denominator,
            percent: if denominator == 0 {
                0.0
            } else {
                numerator as f64 * 100.0 / denominator as f64
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct CoverageFractions {
    supported_address_hits_of_all_instructions: CoverageFraction,
    decoded_instructions_of_all_instructions: CoverageFraction,
    decoded_instructions_of_supported_address_hits: CoverageFraction,
    candidate_transactions_of_all_transactions: CoverageFraction,
    decoded_transactions_of_all_transactions: CoverageFraction,
    semantic_venue_swap_transactions_of_all_transactions: CoverageFraction,
    candidate_successful_transactions_of_successful_transactions: CoverageFraction,
    decoded_successful_transactions_of_successful_transactions: CoverageFraction,
    semantic_venue_swap_successful_transactions_of_successful_transactions: CoverageFraction,
}

#[derive(Debug, Serialize)]
struct InstructionCoverageReport {
    occurrences: InstructionOccurrenceCounters,
    supported_address_hits: InstructionCoverageCounters,
    by_origin: OriginInstructionCounters,
    by_transaction_status: StatusInstructionCounters,
    by_nominal_program_role: NominalRoleInstructionCounters,
    decoded_by_instruction_role: DecodedRoleCounters,
}

#[derive(Debug, Serialize)]
struct ProgramCoverageReport {
    parser_index: usize,
    program_id: &'static str,
    label: &'static str,
    nominal_program_role: &'static str,
    registry_id: Option<u32>,
    instructions: InstructionCoverageCounters,
    by_origin: OriginInstructionCounters,
    by_transaction_status: StatusInstructionCounters,
    transactions: ProgramTransactionCounters,
}

#[derive(Debug, Serialize)]
struct CoverageDefinitions {
    all_instruction_denominator: &'static str,
    scan_method: &'static str,
    candidate_transaction: &'static str,
    decoded_transaction: &'static str,
    semantic_decoded_instruction: &'static str,
    structural_only_instruction: &'static str,
    semantic_venue_swap_transaction: &'static str,
    successful_transaction: &'static str,
    unknown_status_transaction: &'static str,
    nominal_program_role: &'static str,
    decoded_instruction_role: &'static str,
    target_relevance: &'static str,
    missing_instruction_data: &'static str,
    volume: &'static str,
}

#[derive(Debug, Serialize)]
struct DexParserCoverageReport {
    schema_version: u32,
    artifact_kind: &'static str,
    complete: bool,
    generator: GeneratorProvenance,
    source: CoverageSource,
    parser_set: ParserSetSummary,
    transactions: TransactionCoverageCounters,
    instructions: InstructionCoverageReport,
    coverage: CoverageFractions,
    programs: Vec<ProgramCoverageReport>,
    definitions: CoverageDefinitions,
}

fn role_name(role: ProgramRole) -> &'static str {
    match role {
        ProgramRole::Router => "router",
        ProgramRole::Venue => "venue",
    }
}

fn hash_current_executable() -> Result<[u8; 32]> {
    let path = std::env::current_exe().context("resolve current coverage executable")?;
    let file = File::open(&path)
        .with_context(|| format!("open current coverage executable {}", path.display()))?;
    let stamp = FileStamp::read(&file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let count = reader
            .read(&mut buffer)
            .with_context(|| format!("hash current coverage executable {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let file = reader.into_inner();
    stamp.verify(&file, "current coverage executable")?;
    verify_path_binding(&path, &stamp, "current coverage executable")?;
    Ok(hasher.finalize().into())
}

fn build_dispatch(
    registry: &[u8],
    registry_entries: u32,
) -> Result<(DispatchTable, Vec<Option<u32>>, u64)> {
    ensure!(
        PROGRAM_COUNT <= usize::from(u8::MAX) + 1,
        "DEX parser program set exceeds its dense transaction scratch"
    );
    for (index, spec) in PROGRAM_SPECS.iter().enumerate() {
        ensure!(
            spec.program as usize == index && spec.role == spec.program.role(),
            "DEX parser program specifications are not in enum order"
        );
    }

    let mut registry_ids = Vec::with_capacity(PROGRAM_COUNT);
    let mut present = 0u64;
    for spec in PROGRAM_SPECS {
        let key = parse_pubkey(spec.address, "DEX parser program ID")?;
        let registry_id = registry_id_for_key(registry, &key);
        if registry_id.is_some() {
            checked_increment(&mut present, "DEX parser programs present in registry")?;
        }
        registry_ids.push(registry_id);
    }
    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("DEX dispatch table length overflow")?;
    let dispatch = DispatchTable::from_resolver(dense_len, |address| {
        PROGRAM_SPECS
            .iter()
            .position(|spec| spec.address == address)
            .and_then(|index| registry_ids[index])
    });
    ensure!(
        dispatch.len() == dense_len,
        "DEX dispatch table length differs from one-based registry domain"
    );
    Ok((dispatch, registry_ids, present))
}

fn serialize_programs(
    accumulators: &[ProgramCoverageAccumulator],
    registry_ids: &[Option<u32>],
) -> Result<Vec<ProgramCoverageReport>> {
    ensure!(
        accumulators.len() == PROGRAM_COUNT && registry_ids.len() == PROGRAM_COUNT,
        "DEX program report inputs differ from parser set"
    );
    PROGRAM_SPECS
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let counters = accumulators[index];
            Ok(ProgramCoverageReport {
                parser_index: index,
                program_id: spec.address,
                label: spec.label,
                nominal_program_role: role_name(spec.role),
                registry_id: registry_ids[index],
                instructions: counters.all,
                by_origin: OriginInstructionCounters {
                    outer: counters.outer,
                    inner: counters.inner,
                },
                by_transaction_status: StatusInstructionCounters {
                    successful_transactions: counters.successful,
                    failed_transactions: counters.failed,
                    unknown_status_transactions: counters.unknown_status,
                },
                transactions: counters.transactions,
            })
        })
        .collect()
}

/// Scan each consolidated frame once. Message and metadata bytes can be walked
/// more than once in memory so exact account resolution and schema selection
/// finish before parser counts are committed.
pub(super) fn measure_dex_parser_coverage_v3(dump: &Path, report: &Path) -> Result<()> {
    let started = Instant::now();
    let dump = fs::canonicalize(dump)
        .with_context(|| format!("resolve consolidated dump {}", dump.display()))?;
    ensure!(dump.is_dir(), "consolidated dump is not a directory");
    validate_exact_final_files(&dump)?;

    let report_parent = report.parent().unwrap_or_else(|| Path::new("."));
    let report_parent = if report_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        report_parent
    };
    let canonical_report_parent = fs::canonicalize(report_parent)
        .with_context(|| format!("resolve report directory {}", report_parent.display()))?;
    ensure!(
        canonical_report_parent != dump,
        "DEX parser coverage report must not modify the immutable dump directory"
    );
    let report_name = report
        .file_name()
        .context("DEX parser coverage report path has no file name")?;
    ensure!(
        !canonical_report_parent.join(report_name).exists(),
        "refusing to replace an existing DEX parser coverage report"
    );
    let executable_sha256 = hash_current_executable()?;

    let manifest_bytes =
        read_bounded_regular(&dump.join(DUMP_MANIFEST_FILE), MAX_ROOT_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(DUMP_SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings differ"
    );
    validate_source_binding(&manifest.source_binding)?;
    let expected_transaction_sha256 = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("missing transaction digest")?,
        "transaction digest",
    )?;
    let expected_registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let expected_signatures = manifest.signatures.context("missing signature count")?;
    let expected_registry_rows = manifest.pubkeys.context("missing public-key count")?;
    ensure!(
        expected_registry_rows != 0 && expected_registry_rows < u64::from(u32::MAX),
        "invalid registry row count"
    );
    let registry_entries =
        u32::try_from(expected_registry_rows).context("registry row count exceeds u32")?;
    let expected_registry_bytes = expected_registry_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let registry = read_bounded_regular(&dump.join(PUBKEY_REGISTRY_FILE), expected_registry_bytes)?;
    ensure!(
        u64::try_from(registry.len())? == expected_registry_bytes,
        "registry size differs from its manifest"
    );
    let actual_registry_sha256 = sha256_bytes(&registry);
    ensure!(
        actual_registry_sha256 == expected_registry_sha256,
        "registry digest differs from its manifest"
    );
    ensure!(
        registry
            .chunks_exact(KEY_BYTES)
            .zip(registry.chunks_exact(KEY_BYTES).skip(1))
            .all(|(left, right)| left < right),
        "registry is not strictly sorted and unique"
    );
    let (dispatch, parser_registry_ids, programs_present) =
        build_dispatch(&registry, registry_entries)?;

    let signature_bytes = expected_signatures
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature byte length overflow")?;
    let signature_metadata = fs::symlink_metadata(dump.join(DUMP_SIGNATURES_FILE))?;
    ensure!(
        signature_metadata.file_type().is_file() && signature_metadata.len() == signature_bytes,
        "signature sidecar size differs from its manifest"
    );

    let target = TargetBinding {
        mint: parse_pubkey(&manifest.mint, "mint")?,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    let transaction_path = dump.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut transaction_hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut transaction_hasher,
        &mut payload,
    )?
    .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = decode_borrowed_frame(&payload)? else {
        bail!("consolidated transaction stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );

    let mut posting_scratch = ConsolidatedPostingProjectionScratch::new(registry_entries)?;
    let mut account_scratch = [0u32; MAX_MESSAGE_ACCOUNTS];
    let mut stage = TransactionStage::new();
    let mut counters = CoverageAccumulator::new();
    let mut signatures = 0u64;
    let mut previous_coordinate = None;
    let mut previous_slot = None::<(u64, u64, u32, BlockIdentity)>;
    let footer = loop {
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .context("consolidated transaction stream has no footer")?;
        match decode_borrowed_frame(&payload)? {
            BorrowedDumpRecord::Header(_) => {
                bail!("consolidated transaction stream repeats its header")
            }
            BorrowedDumpRecord::Footer(footer) => break footer,
            BorrowedDumpRecord::Transaction(record) => {
                let coordinate = ProgramInventoryCoordinate::from_record(&record);
                ensure!(
                    previous_coordinate
                        .is_none_or(|previous| previous < coordinate.canonical_key()),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate.canonical_key());
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.slot >= manifest.mint_slot
                        && record.block.parent_slot < record.block.slot
                        && record.block.transaction_count != 0
                        && record.tx_index < record.block.transaction_count
                        && record.signature_count != 0
                        && !record.message_bytes.is_empty()
                        && record.flags & !ARCHIVE_V2_TX_KNOWN_FLAGS == 0
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                let DumpSourceBinding::TrustedLocalSizesOnly {
                    slots_per_epoch,
                    wire_profile,
                    ..
                } = &manifest.source_binding;
                let first_slot = record
                    .source_epoch
                    .checked_mul(*slots_per_epoch)
                    .context("source epoch first slot overflow")?;
                ensure!(
                    record.source_wire_profile == *wire_profile
                        && record.block.slot >= first_slot
                        && record.block.slot - first_slot < *slots_per_epoch
                        && u64::from(record.source_block_id) < *slots_per_epoch,
                    "consolidated transaction differs from its trusted source binding"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );
                ensure!(
                    record.dump_signature_ordinal == Some(signatures),
                    "consolidated signature ordinals are not contiguous"
                );
                record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                let identity = BlockIdentity::from(&record.block);
                if let Some((epoch, slot, block_id, previous_identity)) = previous_slot
                    && epoch == record.source_epoch
                    && slot == record.block.slot
                {
                    ensure!(
                        block_id == record.source_block_id && previous_identity == identity,
                        "one source slot has conflicting block context"
                    );
                }
                previous_slot = Some((
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    identity,
                ));

                stage.begin();
                let projection = project_consolidated_transaction_postings(
                    &record,
                    registry_entries,
                    &mut posting_scratch,
                )
                .with_context(|| {
                    format!(
                        "resolve message accounts at epoch {} slot {} transaction {}",
                        record.source_epoch, record.block.slot, record.tx_index
                    )
                })?;
                let resolved_accounts = projection.resolved_account_registry_ids;
                let mut classifier = InstructionClassifier {
                    dispatch: &dispatch,
                    resolved_accounts,
                    account_scratch: &mut account_scratch,
                };
                let mut outer_callbacks = 0usize;
                let message = projector(record.source_wire_profile)
                    .visit_static_accounts_and_instructions_exact(
                        record.message_bytes,
                        registry_entries,
                        |_, _| {},
                        |instruction| {
                            outer_callbacks += 1;
                            classifier.classify(
                                &mut stage,
                                usize::from(instruction.program_id_index),
                                instruction.accounts,
                                instruction.raw_data,
                                InstructionOrigin::Outer,
                            );
                        },
                    )
                    .with_context(|| {
                        format!(
                            "decode DEX coverage message at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                ensure!(
                    outer_callbacks == message.instruction_count && !stage.callback_invalid,
                    "DEX message callbacks differ from exact resolved message"
                );

                if let Some(schema) = projection.metadata_schema.selected_schema() {
                    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
                        record.metadata_bytes,
                        schema,
                        ArchiveV2MetadataProjectionLimits {
                            total_message_accounts: resolved_accounts.len(),
                            top_level_instruction_count: message.instruction_count,
                        },
                        registry_entries,
                        LogPayloadValidation::StructureOnly,
                        |_, instruction: BorrowedArchiveV2InnerTokenInstruction<'_>| {
                            classifier.classify(
                                &mut stage,
                                usize::try_from(instruction.program_id_index)
                                    .unwrap_or(MAX_MESSAGE_ACCOUNTS),
                                instruction.accounts,
                                Some(instruction.data),
                                InstructionOrigin::Inner,
                            );
                        },
                        |_, _| {},
                        |_, _, _| {},
                    )
                    .with_context(|| {
                        format!(
                            "decode DEX coverage metadata at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                } else {
                    ensure!(
                        projection.metadata_schema == ExactMetadataSchemaSelection::NoMetadata,
                        "metadata schema has no selected wire schema"
                    );
                }
                ensure!(
                    !stage.callback_invalid,
                    "DEX callbacks contain an unresolved message account"
                );
                counters.commit_transaction(transaction_status(&record), &stage)?;

                signatures = signatures
                    .checked_add(u64::from(record.signature_count))
                    .context("signature count overflow")?;
                if counters
                    .transactions
                    .scanned
                    .is_multiple_of(PROGRAM_INVENTORY_PROGRESS_TRANSACTIONS)
                {
                    inventory_progress(
                        "DEX parser coverage",
                        started,
                        counters.transactions.scanned,
                        manifest.transactions,
                        logical_offset,
                    );
                }
            }
        }
    };
    ensure!(
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .is_none(),
        "consolidated transaction stream has records after its footer"
    );
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    verify_path_binding(
        &transaction_path,
        &transaction_stamp,
        "consolidated transaction stream",
    )?;
    ensure!(
        logical_offset == transaction_stamp.bytes,
        "transaction stream size changed while it was read"
    );
    let actual_transaction_sha256: [u8; 32] = transaction_hasher.finalize().into();
    ensure!(
        actual_transaction_sha256 == expected_transaction_sha256,
        "transaction digest differs from its manifest"
    );
    let epoch_count = manifest
        .last_epoch
        .checked_sub(manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        counters.transactions.scanned == manifest.transactions
            && signatures == expected_signatures
            && footer.epochs == epoch_count
            && footer.transactions_written == counters.transactions.scanned
            && footer.transactions_scanned >= counters.transactions.scanned
            && footer.pubkeys == expected_registry_rows
            && footer.signatures == signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream counters differ from its manifest"
    );
    counters.validate()?;

    let source = CoverageSource {
        mint: manifest.mint,
        manifest_sha256: hex_digest(manifest_sha256),
        transaction_stream_sha256: hex_digest(actual_transaction_sha256),
        pubkey_registry_sha256: hex_digest(actual_registry_sha256),
        transactions: counters.transactions.scanned,
        signatures,
        registry_entries,
        first_epoch: manifest.first_epoch,
        last_epoch: manifest.last_epoch,
    };
    let programs_observed_with_hits = u64::try_from(
        counters
            .programs
            .iter()
            .filter(|program| program.all.supported_address_hits != 0)
            .count(),
    )?;
    ensure!(
        programs_observed_with_hits <= programs_present,
        "observed parser program count exceeds parser keys present in registry"
    );
    let coverage = CoverageFractions {
        supported_address_hits_of_all_instructions: CoverageFraction::new(
            counters.all.supported_address_hits,
            counters.occurrences.all,
        ),
        decoded_instructions_of_all_instructions: CoverageFraction::new(
            counters.all.decoded,
            counters.occurrences.all,
        ),
        decoded_instructions_of_supported_address_hits: CoverageFraction::new(
            counters.all.decoded,
            counters.all.supported_address_hits,
        ),
        candidate_transactions_of_all_transactions: CoverageFraction::new(
            counters.transactions.candidate,
            counters.transactions.scanned,
        ),
        decoded_transactions_of_all_transactions: CoverageFraction::new(
            counters.transactions.decoded,
            counters.transactions.scanned,
        ),
        semantic_venue_swap_transactions_of_all_transactions: CoverageFraction::new(
            counters.transactions.semantic_venue_swap,
            counters.transactions.scanned,
        ),
        candidate_successful_transactions_of_successful_transactions: CoverageFraction::new(
            counters.transactions.candidate_successful,
            counters.transactions.successful,
        ),
        decoded_successful_transactions_of_successful_transactions: CoverageFraction::new(
            counters.transactions.decoded_successful,
            counters.transactions.successful,
        ),
        semantic_venue_swap_successful_transactions_of_successful_transactions:
            CoverageFraction::new(
                counters.transactions.semantic_venue_swap_successful,
                counters.transactions.successful,
            ),
    };
    let report = DexParserCoverageReport {
        schema_version: COVERAGE_SCHEMA_VERSION,
        artifact_kind: "dex_parser_coverage",
        complete: true,
        generator: GeneratorProvenance {
            crate_name: env!("CARGO_PKG_NAME"),
            crate_version: env!("CARGO_PKG_VERSION"),
            executable_sha256: hex_digest(executable_sha256),
        },
        source,
        parser_set: ParserSetSummary {
            semantic_version: PARSER_SEMANTIC_VERSION,
            implementation_fingerprint: PARSER_IMPLEMENTATION_FINGERPRINT,
            programs: PROGRAM_COUNT,
            programs_present_in_registry: programs_present,
            programs_absent_from_registry: u64::try_from(PROGRAM_COUNT)?
                .checked_sub(programs_present)
                .context("program presence count exceeds parser set")?,
            programs_observed_with_supported_address_hits: programs_observed_with_hits,
        },
        transactions: counters.transactions,
        instructions: InstructionCoverageReport {
            occurrences: counters.occurrences,
            supported_address_hits: counters.all,
            by_origin: counters.origin,
            by_transaction_status: counters.status,
            by_nominal_program_role: counters.nominal_role,
            decoded_by_instruction_role: counters.decoded_role,
        },
        coverage,
        programs: serialize_programs(&counters.programs, &parser_registry_ids)?,
        definitions: CoverageDefinitions {
            all_instruction_denominator: "stored top-level instructions plus metadata-recorded inner instructions; it does not claim every runtime-executed instruction when metadata is absent or incomplete",
            scan_method: "single-threaded sequential file read with one reused frame buffer; message and metadata bytes are re-walked in memory for exact account/schema projection and parser classification; the transaction stream is SHA-256 verified during that read",
            candidate_transaction: "contains at least one instruction whose program address is in the parser set",
            decoded_transaction: "contains at least one instruction accepted by a parser",
            semantic_decoded_instruction: "decoded instruction without STRUCTURAL_ONLY evidence",
            structural_only_instruction: "decoded selector whose instruction body is not fully validated",
            semantic_venue_swap_transaction: "contains at least one decoded venue-role swap without STRUCTURAL_ONLY evidence",
            successful_transaction: "metadata is present and records no transaction error",
            unknown_status_transaction: "metadata is absent, so the report does not infer success from a clear HAS_ERROR flag",
            nominal_program_role: "role of the parser program specification; this partitions all supported address hits",
            decoded_instruction_role: "role returned by DecodedInstruction; venue programs can decode router-container selectors",
            target_relevance: "a decoded venue swap can be an unrelated leg in a transaction selected for SPYx; token-flow reduction must prove SPYx participation",
            missing_instruction_data: "supported outer program address with a structured Archive V2 payload and no borrowed raw instruction bytes; never treated as empty raw data",
            volume: "not measured by this report; executed token-flow reduction is required",
        },
    };
    let mut bytes = serde_json::to_vec_pretty(&report)?;
    bytes.push(b'\n');
    let final_path =
        publish_dex_coverage_report(report_path(report_name, &canonical_report_parent), &bytes)?;
    eprintln!(
        "DEX parser coverage complete: {} transactions, {} supported hits, {} decoded, {:.1}s, report {}",
        report.source.transactions,
        report
            .instructions
            .supported_address_hits
            .supported_address_hits,
        report.instructions.supported_address_hits.decoded,
        started.elapsed().as_secs_f64(),
        final_path.display()
    );
    Ok(())
}

fn report_path(file_name: &std::ffi::OsStr, parent: &Path) -> PathBuf {
    parent.join(file_name)
}

fn publish_dex_coverage_report(report: PathBuf, bytes: &[u8]) -> Result<PathBuf> {
    ensure!(
        !report.exists(),
        "refusing to replace existing DEX parser coverage report {}",
        report.display()
    );
    let parent = report.parent().context("coverage report has no parent")?;
    let file_name = report
        .file_name()
        .context("coverage report has no file name")?;
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system time is before the Unix epoch")?
        .as_nanos();
    let mut temporary = None;
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(
            ".{}.dex-coverage-{}-{nonce}-{attempt}.partial",
            file_name.to_string_lossy(),
            std::process::id(),
        ));
        match create_new_file(&candidate) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if candidate.exists() => drop(error),
            Err(error) => return Err(error),
        }
    }
    let (temporary_path, file) =
        temporary.context("cannot create a unique DEX coverage temporary file")?;
    let mut published = false;
    let result = (|| -> Result<()> {
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        writer.write_all(bytes)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        fs::hard_link(&temporary_path, &report).with_context(|| {
            format!(
                "publish DEX parser coverage {} as {}",
                temporary_path.display(),
                report.display()
            )
        })?;
        published = true;
        sync_directory(parent)?;
        fs::remove_file(&temporary_path)
            .with_context(|| format!("remove {}", temporary_path.display()))?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        if published {
            let _ = fs::remove_file(&report);
            let _ = sync_directory(parent);
        }
        if temporary_path.exists() {
            let _ = fs::remove_file(&temporary_path);
        }
    }
    result?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use blockzilla_dex_parser::{
        AccountRoles, Amounts, DecodedInstruction, Discriminator, SwapKind,
    };

    use super::*;

    fn decoded(
        program: Program,
        role: ProgramRole,
        class: InstructionClass,
        evidence: Evidence,
    ) -> DecodeOutcome {
        DecodeOutcome::Decoded(DecodedInstruction {
            program,
            role,
            name: "test",
            class,
            discriminator: Discriminator::one(1),
            accounts: AccountRoles::default(),
            amounts: Amounts::Unknown,
            evidence,
        })
    }

    #[test]
    fn outcome_counters_partition_semantic_structural_and_classes() {
        let mut counters = InstructionCoverageCounters::default();
        counters.record(CoverageOutcome::Parser(decoded(
            Program::RaydiumClmm,
            ProgramRole::Venue,
            InstructionClass::Swap(SwapKind::ExactIn),
            Evidence::ACCOUNT_LAYOUT,
        )));
        counters.record(CoverageOutcome::Parser(decoded(
            Program::RaydiumClmm,
            ProgramRole::Router,
            InstructionClass::Route,
            Evidence::STRUCTURAL_ONLY,
        )));
        counters.record(CoverageOutcome::Parser(DecodeOutcome::Unsupported {
            discriminator: Discriminator::one(9),
        }));
        counters.record(CoverageOutcome::Parser(DecodeOutcome::Malformed(
            MalformedReason::InstructionAccountsTooShort {
                needed: 4,
                actual: 3,
            },
        )));
        counters.record(CoverageOutcome::MissingInstructionData);

        counters.validate().unwrap();
        assert_eq!(counters.supported_address_hits, 5);
        assert_eq!(counters.decoded, 2);
        assert_eq!(counters.semantic_decoded, 1);
        assert_eq!(counters.structural_only_decoded, 1);
        assert_eq!(counters.swaps, 1);
        assert_eq!(counters.semantic_swaps, 1);
        assert_eq!(counters.routes, 1);
        assert_eq!(counters.unsupported_discriminator, 1);
        assert_eq!(counters.malformed, 1);
        assert_eq!(counters.missing_instruction_data, 1);
    }

    #[test]
    fn unique_transaction_unions_are_counted_once() {
        let mut counters = TransactionCoverageCounters {
            scanned: 3,
            successful: 1,
            failed: 1,
            unknown_status: 1,
            ..TransactionCoverageCounters::default()
        };
        record_transaction_union(
            &mut counters,
            TransactionStatus::Successful,
            true,
            true,
            true,
        )
        .unwrap();
        record_transaction_union(&mut counters, TransactionStatus::Failed, true, false, false)
            .unwrap();
        record_transaction_union(&mut counters, TransactionStatus::Unknown, true, true, false)
            .unwrap();

        assert_eq!(counters.candidate, 3);
        assert_eq!(counters.candidate_successful, 1);
        assert_eq!(counters.candidate_failed, 1);
        assert_eq!(counters.candidate_unknown_status, 1);
        assert_eq!(counters.decoded, 2);
        assert_eq!(counters.decoded_unknown_status, 1);
        assert_eq!(counters.semantic_venue_swap, 1);
    }

    #[test]
    fn an_empty_metadata_record_has_unknown_transaction_status() {
        let record = BorrowedTransactionRecord {
            source_epoch: 1,
            source_generation_digest: [0; 32],
            source_wire_profile: DumpWireProfile::PostUnknownInstructionFallbacksV1,
            source_block_id: 0,
            block: TokenTransactionBlockContext {
                slot: 1,
                parent_slot: 0,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                transaction_count: 1,
            },
            tx_index: 0,
            flags: 0,
            source_first_signature_ordinal: 0,
            signature_count: 1,
            dump_signature_ordinal: Some(0),
            message_bytes: &[1],
            metadata_bytes: &[],
        };

        assert_eq!(transaction_status(&record), TransactionStatus::Unknown);
    }

    #[test]
    fn unknown_status_is_a_complete_independent_partition() {
        let mut accumulator = CoverageAccumulator::new();
        let mut stage = TransactionStage::new();
        stage.begin();

        accumulator
            .commit_transaction(TransactionStatus::Unknown, &stage)
            .unwrap();
        accumulator.validate().unwrap();

        assert_eq!(accumulator.transactions.scanned, 1);
        assert_eq!(accumulator.transactions.successful, 0);
        assert_eq!(accumulator.transactions.failed, 0);
        assert_eq!(accumulator.transactions.unknown_status, 1);
    }

    #[test]
    fn dispatch_uses_the_complete_one_based_registry_domain() {
        let mut keys = PROGRAM_SPECS
            .iter()
            .map(|spec| parse_pubkey(spec.address, "test program").unwrap())
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        let registry = keys.concat();
        let registry_entries = u32::try_from(keys.len()).unwrap();
        let (dispatch, registry_ids, present) =
            build_dispatch(&registry, registry_entries).unwrap();

        assert_eq!(dispatch.len(), keys.len() + 1);
        assert_eq!(present, PROGRAM_COUNT as u64);
        assert!(registry_ids.iter().all(Option::is_some));
        let maximum_id = registry_ids.iter().flatten().copied().max().unwrap();
        assert!(dispatch.program(maximum_id).is_some());
        assert!(!PARSER_SEMANTIC_VERSION.is_empty());
        assert_eq!(PARSER_IMPLEMENTATION_FINGERPRINT.len(), 64);
        assert!(
            PARSER_IMPLEMENTATION_FINGERPRINT
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
    }

    #[test]
    fn decoded_instruction_role_overrides_the_nominal_program_role() {
        let program_id = 20u32;
        let dispatch = DispatchTable::from_resolver(32, |address| {
            (address == PROGRAM_SPECS[0].address).then_some(program_id)
        });
        let mut resolved_accounts: [u32; 20] = core::array::from_fn(|index| index as u32 + 100);
        resolved_accounts[0] = program_id;
        let account_indices: [u8; 14] = core::array::from_fn(|index| index as u8);
        let mut data = [0u8; 24];
        data[..8].copy_from_slice(&[69, 125, 115, 218, 245, 186, 242, 196]);
        data[8..16].copy_from_slice(&80u64.to_le_bytes());
        data[16..24].copy_from_slice(&72u64.to_le_bytes());
        let mut account_scratch = [0u32; MAX_MESSAGE_ACCOUNTS];
        let mut classifier = InstructionClassifier {
            dispatch: &dispatch,
            resolved_accounts: &resolved_accounts,
            account_scratch: &mut account_scratch,
        };
        let mut stage = TransactionStage::new();
        stage.begin();

        classifier.classify(
            &mut stage,
            0,
            &account_indices,
            Some(&data),
            InstructionOrigin::Outer,
        );

        assert!(!stage.callback_invalid);
        assert_eq!(stage.nominal_role.venue_programs.decoded, 1);
        assert_eq!(stage.nominal_role.router_programs.decoded, 0);
        assert_eq!(stage.decoded_role.router.routes, 1);
        assert_eq!(stage.decoded_role.venue.decoded, 0);
    }
}

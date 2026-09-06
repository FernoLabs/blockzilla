//! Fail-closed classification of committed instruction invocations.
//!
//! Solana's recorded inner-instruction list contains attempted CPI calls. A
//! successful transaction can still contain a failed CPI that its caller
//! caught. A successful child call is also rolled back when one of its parent
//! calls fails, even if a higher caller catches that failure. The transaction
//! status alone is therefore not enough to decide whether an inner
//! instruction changed state.
//!
//! This module combines the ordered outer/inner instruction trace with the
//! `Invoke`, `Success`, and `Failure` boundaries from a compact log stream. It
//! has no archive or log-format dependency. A scanner resolves compact program
//! ids to 32-byte keys and converts all compact failure variants to
//! [`InvocationLogEvent::Failure`] before it calls
//! [`classify_committed_invocations`].
//!
//! Input instructions must be in execution order: an outer instruction first,
//! followed by that outer instruction's recorded inner instructions, followed
//! by the next outer instruction. Inner instructions retain their recorded
//! `stack_height`. All outer instructions can be supplied, including native
//! instructions that did not emit an `Invoke` log.
//!
//! The caller must first prove that inner-instruction recording is present and
//! complete. No classifier can recover a CPI call that is absent from both the
//! supplied instruction trace and the supplied log boundaries.

use std::collections::HashSet;

/// A registry-resolved program id.
pub type ProgramId = [u8; 32];

/// Stable position of one outer or inner instruction in a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InstructionCoordinate {
    pub outer_index: u32,
    pub inner_index: Option<u32>,
}

impl InstructionCoordinate {
    pub const fn outer(outer_index: u32) -> Self {
        Self {
            outer_index,
            inner_index: None,
        }
    }

    pub const fn inner(outer_index: u32, inner_index: u32) -> Self {
        Self {
            outer_index,
            inner_index: Some(inner_index),
        }
    }

    pub const fn is_outer(self) -> bool {
        self.inner_index.is_none()
    }
}

/// One instruction in outer/inner execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedInvocation {
    pub coordinate: InstructionCoordinate,
    pub program_id: ProgramId,
    /// `Some(1)` for an outer instruction. Inner instructions use the value
    /// recorded in transaction metadata and can therefore be `None`.
    pub stack_height: Option<u32>,
}

impl OrderedInvocation {
    pub const fn outer(outer_index: u32, program_id: ProgramId) -> Self {
        Self {
            coordinate: InstructionCoordinate::outer(outer_index),
            program_id,
            stack_height: Some(1),
        }
    }

    pub const fn inner(
        outer_index: u32,
        inner_index: u32,
        program_id: ProgramId,
        stack_height: Option<u32>,
    ) -> Self {
        Self {
            coordinate: InstructionCoordinate::inner(outer_index, inner_index),
            program_id,
            stack_height,
        }
    }
}

/// Invocation boundary extracted from a compact log stream.
///
/// The scanner must preserve log order and map every compact failure variant
/// (`Failure`, `FailureCustomProgramError`, historical BPF failure variants,
/// and similar terminal failures) to `Failure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationLogEvent {
    Invoke { program_id: ProgramId, depth: u32 },
    Success { program_id: ProgramId },
    Failure { program_id: ProgramId },
    LogTruncated,
}

/// Why a known invocation did not commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackReason {
    /// A failed transaction commits no program state. This value also covers
    /// message instructions after the failing outer instruction that were not
    /// executed; neither case can contribute a replay event.
    TransactionFailed,
    /// This invocation returned a failure.
    InvocationFailed,
    /// The invocation returned success, but a parent invocation failed and
    /// rolled its changes back before a higher caller continued.
    AncestorFailed,
}

/// Why the classifier cannot safely decide whether an inner invocation
/// committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownReason {
    MissingStackHeight,
    MissingOuterInvoke,
    MissingInvocationLog,
    InstructionLogMismatch,
    LogTruncated,
    UnterminatedInvocation,
    MalformedLogTrace,
    InvalidInstructionOrder,
}

/// Commit decision for one supplied instruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStatus {
    Committed,
    RolledBack(RollbackReason),
    Unknown(UnknownReason),
}

impl CommitStatus {
    /// True only when applying this instruction's modeled state change is safe.
    pub const fn is_committed(self) -> bool {
        matches!(self, Self::Committed)
    }

    /// True when this result is sufficient to decide whether to apply a state
    /// change. Both committed and known rolled-back calls are conclusive.
    pub const fn is_known(self) -> bool {
        !matches!(self, Self::Unknown(_))
    }
}

/// One output row. Results have the same order and length as the input trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassifiedInvocation {
    pub invocation: OrderedInvocation,
    pub status: CommitStatus,
}

/// Non-fatal trace problem. The per-instruction status remains authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceDiagnostic {
    LogTruncated {
        event_index: usize,
    },
    InvokeDepthMismatch {
        event_index: usize,
        declared_depth: u32,
        expected_depth: u32,
    },
    TerminalWithoutInvoke {
        event_index: usize,
        program_id: ProgramId,
    },
    TerminalProgramMismatch {
        event_index: usize,
        expected_program_id: ProgramId,
        logged_program_id: ProgramId,
    },
    UnterminatedInvocation {
        frame_index: usize,
        program_id: ProgramId,
        depth: u32,
    },
    InvalidInstructionOrder {
        instruction_index: usize,
    },
    MissingOuterInvoke {
        outer_index: u32,
    },
    InstructionProgramMismatch {
        coordinate: InstructionCoordinate,
        instruction_program_id: ProgramId,
        logged_program_id: ProgramId,
    },
    InstructionDepthMismatch {
        coordinate: InstructionCoordinate,
        instruction_depth: u32,
        logged_depth: u32,
    },
    ExtraLoggedInvocation {
        frame_index: usize,
        program_id: ProgramId,
        depth: u32,
    },
}

/// Complete result for one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitClassification {
    pub invocations: Vec<ClassifiedInvocation>,
    pub diagnostics: Vec<TraceDiagnostic>,
}

impl CommitClassification {
    pub fn all_known(&self) -> bool {
        self.invocations.iter().all(|row| row.status.is_known())
    }

    pub fn committed(&self) -> impl Iterator<Item = &ClassifiedInvocation> {
        self.invocations
            .iter()
            .filter(|row| row.status.is_committed())
    }
}

/// Classify which instruction invocations committed state.
///
/// `transaction_succeeded` must come from transaction status metadata. A
/// failed transaction returns a known rollback decision for every supplied
/// instruction, regardless of incomplete logs. For a successful transaction,
/// each outer instruction is known to have committed. Inner instructions are
/// fail-closed: they require a matching invocation frame, a stack height, a
/// successful terminal event, and successful terminal events for every inner
/// ancestor.
pub fn classify_committed_invocations(
    transaction_succeeded: bool,
    instructions: &[OrderedInvocation],
    log_events: &[InvocationLogEvent],
) -> CommitClassification {
    let (frames, mut diagnostics) = parse_log_frames(log_events);

    if !transaction_succeeded {
        return CommitClassification {
            invocations: instructions
                .iter()
                .copied()
                .map(|invocation| ClassifiedInvocation {
                    invocation,
                    status: CommitStatus::RolledBack(RollbackReason::TransactionFailed),
                })
                .collect(),
            diagnostics,
        };
    }

    let mut statuses =
        vec![CommitStatus::Unknown(UnknownReason::InvalidInstructionOrder); instructions.len()];
    let groups = instruction_groups(instructions, &mut diagnostics);
    let roots = root_ranges(&frames);
    let mut root_cursor = 0usize;
    let mut used_frames = HashSet::new();

    for group_index in 0..groups.len() {
        let group = &groups[group_index];
        let outer = instructions[group.outer_position];
        statuses[group.outer_position] = CommitStatus::Committed;

        // A root frame that cannot match any remaining outer instruction is
        // extra log data. Consume it so one bad frame does not hide all later
        // exact groups.
        while let Some(root) = roots.get(root_cursor) {
            let root_program = frames[root.start].program_id;
            if root_program == outer.program_id {
                break;
            }
            let belongs_to_later_outer = groups[group_index + 1..]
                .iter()
                .any(|later| instructions[later.outer_position].program_id == root_program);
            if belongs_to_later_outer {
                break;
            }
            mark_extra_range(root, &frames, &mut diagnostics, &mut used_frames);
            root_cursor += 1;
        }

        let Some(root) = roots.get(root_cursor) else {
            mark_missing_outer(group, instructions, &mut statuses, &mut diagnostics);
            continue;
        };
        let root_program = frames[root.start].program_id;
        if root_program != outer.program_id {
            mark_missing_outer(group, instructions, &mut statuses, &mut diagnostics);
            continue;
        }

        let descendant_count = root.end.saturating_sub(root.start + 1);
        if group.inner_positions.is_empty() && descendant_count > 0 {
            // A later outer call to the same program can own this logged
            // subtree. Do not consume it for an outer instruction with no
            // recorded inner calls.
            let same_program_with_inners_later = groups[group_index + 1..].iter().any(|later| {
                instructions[later.outer_position].program_id == outer.program_id
                    && !later.inner_positions.is_empty()
            });
            if same_program_with_inners_later {
                continue;
            }
        }

        used_frames.extend(root.start..root.end);
        align_group(
            group,
            root,
            instructions,
            &frames,
            &mut statuses,
            &mut diagnostics,
        );
        root_cursor += 1;
    }

    for (frame_index, frame) in frames.iter().enumerate() {
        if !used_frames.contains(&frame_index) {
            diagnostics.push(TraceDiagnostic::ExtraLoggedInvocation {
                frame_index,
                program_id: frame.program_id,
                depth: frame.depth,
            });
        }
    }

    CommitClassification {
        invocations: instructions
            .iter()
            .copied()
            .zip(statuses)
            .map(|(invocation, status)| ClassifiedInvocation { invocation, status })
            .collect(),
        diagnostics,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Success,
    Failure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameIssue {
    Truncated,
    Unterminated,
    Malformed,
}

#[derive(Debug, Clone)]
struct LogFrame {
    program_id: ProgramId,
    depth: u32,
    parent: Option<usize>,
    terminal: Option<Terminal>,
    issue: Option<FrameIssue>,
}

impl LogFrame {
    fn set_issue(&mut self, issue: FrameIssue) {
        let priority = |value| match value {
            FrameIssue::Unterminated => 1,
            FrameIssue::Truncated => 2,
            FrameIssue::Malformed => 3,
        };
        if self
            .issue
            .is_none_or(|current| priority(issue) > priority(current))
        {
            self.issue = Some(issue);
        }
    }
}

fn parse_log_frames(events: &[InvocationLogEvent]) -> (Vec<LogFrame>, Vec<TraceDiagnostic>) {
    let mut frames = Vec::<LogFrame>::new();
    let mut stack = Vec::<usize>::new();
    let mut diagnostics = Vec::new();
    let mut after_truncation = false;

    for (event_index, event) in events.iter().copied().enumerate() {
        match event {
            InvocationLogEvent::Invoke { program_id, depth } => {
                let expected_depth = stack.len() as u32 + 1;
                let malformed = depth != expected_depth;
                if malformed {
                    diagnostics.push(TraceDiagnostic::InvokeDepthMismatch {
                        event_index,
                        declared_depth: depth,
                        expected_depth,
                    });
                    for frame_index in stack.iter().copied() {
                        frames[frame_index].set_issue(FrameIssue::Malformed);
                    }
                }
                let frame_index = frames.len();
                frames.push(LogFrame {
                    program_id,
                    depth,
                    parent: stack.last().copied(),
                    terminal: None,
                    issue: if malformed {
                        Some(FrameIssue::Malformed)
                    } else if after_truncation {
                        Some(FrameIssue::Truncated)
                    } else {
                        None
                    },
                });
                stack.push(frame_index);
            }
            InvocationLogEvent::Success { program_id } => close_frame(
                event_index,
                program_id,
                Terminal::Success,
                &mut frames,
                &mut stack,
                &mut diagnostics,
            ),
            InvocationLogEvent::Failure { program_id } => close_frame(
                event_index,
                program_id,
                Terminal::Failure,
                &mut frames,
                &mut stack,
                &mut diagnostics,
            ),
            InvocationLogEvent::LogTruncated => {
                diagnostics.push(TraceDiagnostic::LogTruncated { event_index });
                after_truncation = true;
                for frame_index in stack.iter().copied() {
                    frames[frame_index].set_issue(FrameIssue::Truncated);
                }
            }
        }
    }

    for frame_index in stack {
        let frame = &mut frames[frame_index];
        frame.set_issue(FrameIssue::Unterminated);
        diagnostics.push(TraceDiagnostic::UnterminatedInvocation {
            frame_index,
            program_id: frame.program_id,
            depth: frame.depth,
        });
    }

    (frames, diagnostics)
}

fn close_frame(
    event_index: usize,
    program_id: ProgramId,
    terminal: Terminal,
    frames: &mut [LogFrame],
    stack: &mut Vec<usize>,
    diagnostics: &mut Vec<TraceDiagnostic>,
) {
    let Some(top_index) = stack.last().copied() else {
        diagnostics.push(TraceDiagnostic::TerminalWithoutInvoke {
            event_index,
            program_id,
        });
        return;
    };
    if frames[top_index].program_id == program_id {
        frames[top_index].terminal = Some(terminal);
        stack.pop();
        return;
    }

    diagnostics.push(TraceDiagnostic::TerminalProgramMismatch {
        event_index,
        expected_program_id: frames[top_index].program_id,
        logged_program_id: program_id,
    });
    for frame_index in stack.iter().copied() {
        frames[frame_index].set_issue(FrameIssue::Malformed);
    }

    // Recover at a matching ancestor, if one exists, so later independent
    // outer calls can still be classified.
    let Some(position) = stack
        .iter()
        .rposition(|frame_index| frames[*frame_index].program_id == program_id)
    else {
        return;
    };
    while stack.len() > position + 1 {
        stack.pop();
    }
    if let Some(frame_index) = stack.pop() {
        frames[frame_index].terminal = Some(terminal);
    }
}

#[derive(Debug)]
struct InstructionGroup {
    outer_position: usize,
    inner_positions: Vec<usize>,
}

fn instruction_groups(
    instructions: &[OrderedInvocation],
    diagnostics: &mut Vec<TraceDiagnostic>,
) -> Vec<InstructionGroup> {
    let mut groups = Vec::<InstructionGroup>::new();
    for (instruction_index, instruction) in instructions.iter().enumerate() {
        if instruction.coordinate.is_outer() {
            groups.push(InstructionGroup {
                outer_position: instruction_index,
                inner_positions: Vec::new(),
            });
            continue;
        }
        let Some(group) = groups.last_mut() else {
            diagnostics.push(TraceDiagnostic::InvalidInstructionOrder { instruction_index });
            continue;
        };
        let outer = instructions[group.outer_position].coordinate.outer_index;
        if instruction.coordinate.outer_index != outer {
            diagnostics.push(TraceDiagnostic::InvalidInstructionOrder { instruction_index });
            continue;
        }
        group.inner_positions.push(instruction_index);
    }
    groups
}

#[derive(Debug)]
struct RootRange {
    start: usize,
    end: usize,
}

fn root_ranges(frames: &[LogFrame]) -> Vec<RootRange> {
    let roots: Vec<usize> = frames
        .iter()
        .enumerate()
        .filter_map(|(index, frame)| (frame.depth == 1).then_some(index))
        .collect();
    roots
        .iter()
        .enumerate()
        .map(|(position, start)| RootRange {
            start: *start,
            end: roots.get(position + 1).copied().unwrap_or(frames.len()),
        })
        .collect()
}

fn mark_extra_range(
    range: &RootRange,
    frames: &[LogFrame],
    diagnostics: &mut Vec<TraceDiagnostic>,
    used_frames: &mut HashSet<usize>,
) {
    for (frame_index, frame) in frames.iter().enumerate().take(range.end).skip(range.start) {
        diagnostics.push(TraceDiagnostic::ExtraLoggedInvocation {
            frame_index,
            program_id: frame.program_id,
            depth: frame.depth,
        });
        used_frames.insert(frame_index);
    }
}

fn mark_missing_outer(
    group: &InstructionGroup,
    instructions: &[OrderedInvocation],
    statuses: &mut [CommitStatus],
    diagnostics: &mut Vec<TraceDiagnostic>,
) {
    if group.inner_positions.is_empty() {
        return;
    }
    let outer_index = instructions[group.outer_position].coordinate.outer_index;
    diagnostics.push(TraceDiagnostic::MissingOuterInvoke { outer_index });
    for position in group.inner_positions.iter().copied() {
        statuses[position] = CommitStatus::Unknown(UnknownReason::MissingOuterInvoke);
    }
}

fn align_group(
    group: &InstructionGroup,
    root: &RootRange,
    instructions: &[OrderedInvocation],
    frames: &[LogFrame],
    statuses: &mut [CommitStatus],
    diagnostics: &mut Vec<TraceDiagnostic>,
) {
    let descendant_frames = root.start + 1..root.end;
    let mut mismatch_seen = false;

    for (ordinal, instruction_position) in group.inner_positions.iter().copied().enumerate() {
        if mismatch_seen {
            statuses[instruction_position] =
                CommitStatus::Unknown(UnknownReason::InstructionLogMismatch);
            continue;
        }
        let Some(frame_index) = descendant_frames.clone().nth(ordinal) else {
            statuses[instruction_position] =
                CommitStatus::Unknown(UnknownReason::MissingInvocationLog);
            continue;
        };
        let invocation = instructions[instruction_position];
        let frame = &frames[frame_index];
        if invocation.program_id != frame.program_id {
            diagnostics.push(TraceDiagnostic::InstructionProgramMismatch {
                coordinate: invocation.coordinate,
                instruction_program_id: invocation.program_id,
                logged_program_id: frame.program_id,
            });
            statuses[instruction_position] =
                CommitStatus::Unknown(UnknownReason::InstructionLogMismatch);
            mismatch_seen = true;
            continue;
        }
        let Some(instruction_depth) = invocation.stack_height else {
            statuses[instruction_position] =
                CommitStatus::Unknown(UnknownReason::MissingStackHeight);
            continue;
        };
        if instruction_depth != frame.depth {
            diagnostics.push(TraceDiagnostic::InstructionDepthMismatch {
                coordinate: invocation.coordinate,
                instruction_depth,
                logged_depth: frame.depth,
            });
            statuses[instruction_position] =
                CommitStatus::Unknown(UnknownReason::InstructionLogMismatch);
            mismatch_seen = true;
            continue;
        }
        statuses[instruction_position] = classify_inner_frame(frame_index, frames);
    }

    let used_descendants = group.inner_positions.len().min(descendant_frames.len());
    for frame_index in descendant_frames.skip(used_descendants) {
        let frame = &frames[frame_index];
        diagnostics.push(TraceDiagnostic::ExtraLoggedInvocation {
            frame_index,
            program_id: frame.program_id,
            depth: frame.depth,
        });
    }
}

fn classify_inner_frame(frame_index: usize, frames: &[LogFrame]) -> CommitStatus {
    let frame = &frames[frame_index];
    if let Some(issue) = frame.issue {
        return CommitStatus::Unknown(issue_reason(issue));
    }
    match frame.terminal {
        Some(Terminal::Failure) => {
            return CommitStatus::RolledBack(RollbackReason::InvocationFailed);
        }
        Some(Terminal::Success) => {}
        None => return CommitStatus::Unknown(UnknownReason::UnterminatedInvocation),
    }

    let mut parent = frame.parent;
    while let Some(parent_index) = parent {
        let ancestor = &frames[parent_index];
        if ancestor.depth == 1 {
            // A successful transaction proves that its top-level instruction
            // committed, even if the outer boundary log was truncated.
            if ancestor.issue == Some(FrameIssue::Malformed)
                || ancestor.terminal == Some(Terminal::Failure)
            {
                return CommitStatus::Unknown(UnknownReason::MalformedLogTrace);
            }
            parent = ancestor.parent;
            continue;
        }
        if let Some(issue) = ancestor.issue {
            return CommitStatus::Unknown(issue_reason(issue));
        }
        match ancestor.terminal {
            Some(Terminal::Success) => parent = ancestor.parent,
            Some(Terminal::Failure) => {
                return CommitStatus::RolledBack(RollbackReason::AncestorFailed);
            }
            None => return CommitStatus::Unknown(UnknownReason::UnterminatedInvocation),
        }
    }
    CommitStatus::Committed
}

const fn issue_reason(issue: FrameIssue) -> UnknownReason {
    match issue {
        FrameIssue::Truncated => UnknownReason::LogTruncated,
        FrameIssue::Unterminated => UnknownReason::UnterminatedInvocation,
        FrameIssue::Malformed => UnknownReason::MalformedLogTrace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn key(byte: u8) -> ProgramId {
        [byte; 32]
    }

    fn invoke(program_id: ProgramId, depth: u32) -> InvocationLogEvent {
        InvocationLogEvent::Invoke { program_id, depth }
    }

    fn success(program_id: ProgramId) -> InvocationLogEvent {
        InvocationLogEvent::Success { program_id }
    }

    fn failure(program_id: ProgramId) -> InvocationLogEvent {
        InvocationLogEvent::Failure { program_id }
    }

    fn statuses(result: &CommitClassification) -> Vec<CommitStatus> {
        result.invocations.iter().map(|row| row.status).collect()
    }

    #[test]
    fn successful_outer_and_inner_commit() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            success(token),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![CommitStatus::Committed, CommitStatus::Committed]
        );
        assert!(result.all_known());
    }

    #[test]
    fn failed_transaction_rolls_everything_back_even_with_bad_logs() {
        let instructions = [
            OrderedInvocation::outer(0, key(1)),
            OrderedInvocation::inner(0, 0, key(2), Some(2)),
            OrderedInvocation::outer(1, key(3)),
        ];
        let logs = [invoke(key(1), 1), InvocationLogEvent::LogTruncated];

        let result = classify_committed_invocations(false, &instructions, &logs);
        assert!(result.invocations.iter().all(|row| {
            row.status == CommitStatus::RolledBack(RollbackReason::TransactionFailed)
        }));
        assert!(result.all_known());
    }

    #[test]
    fn caught_inner_failure_does_not_commit() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            failure(token),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::RolledBack(RollbackReason::InvocationFailed),
            ]
        );
    }

    #[test]
    fn successful_child_is_rolled_back_by_failed_caught_parent() {
        let outer = key(1);
        let parent = key(2);
        let token = key(3);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, parent, Some(2)),
            OrderedInvocation::inner(0, 1, token, Some(3)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(parent, 2),
            invoke(token, 3),
            success(token),
            failure(parent),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::RolledBack(RollbackReason::InvocationFailed),
                CommitStatus::RolledBack(RollbackReason::AncestorFailed),
            ]
        );
    }

    #[test]
    fn successful_sibling_after_caught_failure_commits() {
        let outer = key(1);
        let failed = key(2);
        let committed = key(3);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, failed, Some(2)),
            OrderedInvocation::inner(0, 1, committed, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(failed, 2),
            failure(failed),
            invoke(committed, 2),
            success(committed),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::RolledBack(RollbackReason::InvocationFailed),
                CommitStatus::Committed,
            ]
        );
    }

    #[test]
    fn truncation_makes_an_active_inner_frame_unknown() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            InvocationLogEvent::LogTruncated,
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::Unknown(UnknownReason::LogTruncated),
            ]
        );
        assert!(!result.all_known());
    }

    #[test]
    fn completed_child_is_unknown_when_an_inner_parent_is_truncated() {
        let outer = key(1);
        let parent = key(2);
        let token = key(3);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, parent, Some(2)),
            OrderedInvocation::inner(0, 1, token, Some(3)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(parent, 2),
            invoke(token, 3),
            success(token),
            InvocationLogEvent::LogTruncated,
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::Unknown(UnknownReason::LogTruncated),
                CommitStatus::Unknown(UnknownReason::LogTruncated),
            ]
        );
    }

    #[test]
    fn completed_direct_child_before_truncation_is_still_committed() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            success(token),
            InvocationLogEvent::LogTruncated,
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![CommitStatus::Committed, CommitStatus::Committed]
        );
    }

    #[test]
    fn missing_inner_stack_height_is_unknown() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, None),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            success(token),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            result.invocations[1].status,
            CommitStatus::Unknown(UnknownReason::MissingStackHeight)
        );
    }

    #[test]
    fn program_mismatch_fails_closed() {
        let outer = key(1);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, key(2), Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(key(9), 2),
            success(key(9)),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            result.invocations[1].status,
            CommitStatus::Unknown(UnknownReason::InstructionLogMismatch)
        );
        assert!(result.diagnostics.iter().any(|diagnostic| matches!(
            diagnostic,
            TraceDiagnostic::InstructionProgramMismatch { .. }
        )));
    }

    #[test]
    fn unlogged_outer_does_not_shift_a_later_logged_outer() {
        let compute_budget = key(1);
        let outer = key(2);
        let token = key(3);
        let instructions = [
            OrderedInvocation::outer(0, compute_budget),
            OrderedInvocation::outer(1, outer),
            OrderedInvocation::inner(1, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            success(token),
            success(outer),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert_eq!(
            statuses(&result),
            vec![
                CommitStatus::Committed,
                CommitStatus::Committed,
                CommitStatus::Committed,
            ]
        );
    }

    #[test]
    fn inner_instruction_without_any_logs_is_unknown() {
        let instructions = [
            OrderedInvocation::outer(0, key(1)),
            OrderedInvocation::inner(0, 0, key(2), Some(2)),
        ];

        let result = classify_committed_invocations(true, &instructions, &[]);
        assert_eq!(result.invocations[0].status, CommitStatus::Committed);
        assert_eq!(
            result.invocations[1].status,
            CommitStatus::Unknown(UnknownReason::MissingOuterInvoke)
        );
    }

    #[test]
    fn malformed_terminal_order_is_unknown() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let logs = [
            invoke(outer, 1),
            invoke(token, 2),
            success(outer),
            success(token),
        ];

        let result = classify_committed_invocations(true, &instructions, &logs);
        assert!(matches!(
            result.invocations[1].status,
            CommitStatus::Unknown(UnknownReason::MalformedLogTrace)
        ));
        assert!(result.diagnostics.iter().any(|diagnostic| matches!(
            diagnostic,
            TraceDiagnostic::TerminalProgramMismatch { .. }
        )));
    }
}

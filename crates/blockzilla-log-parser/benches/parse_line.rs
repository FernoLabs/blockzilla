use std::hint::black_box;

use blockzilla_log_parser::{ParsedLogLine, parse_line};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const RAYDIUM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

const INVOKE_LINES: &[&str] = &[
    "Program ComputeBudget111111111111111111111111111111 invoke [1]",
    "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]",
    "Program CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C invoke [3]",
];

const SUCCESS_LINES: &[&str] = &[
    "Program ComputeBudget111111111111111111111111111111 success",
    "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success",
    "Program CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C success",
];

const CONSUMED_LINES: &[&str] = &[
    "Program ComputeBudget111111111111111111111111111111 consumed 150 of 200000 compute units",
    "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4,382 of 193,720 compute units",
    "Program CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C consumed 58,281 of 140,000 compute units",
];

const FAILURE_LINES: &[&str] = &[
    "Program ComputeBudget111111111111111111111111111111 failed: custom program error: 0x1",
    "Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA failed: invalid account data for instruction",
    "Program CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C failed: insufficient funds",
];

const PROGRAM_LOG_LINES: &[&str] = &[
    "Program log: Instruction: Transfer",
    "Program log: No profitable arbitrage found",
    "Program log: Left 123456789",
    "Program log: Right 987654321",
];

const PROGRAM_DATA_LINES: &[&str] = &[
    "Program data: 3Bxs4NN8M2Yn4TLb",
    "Program data: qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
];

const PROGRAM_RETURN_LINES: &[&str] = &[
    "Program return: ComputeBudget111111111111111111111111111111 AQIDBA==",
    "Program return: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA AAAAAAAAAAA=",
];

const PROGRAM_CONSUMPTION_LINES: &[&str] = &["Program consumption: 42,000 units remaining"];

const MISC_LINES: &[&str] = &[
    "VerifyEd25519",
    "VerifySecp256k1",
    "Log truncated",
    "custom program error: 0x1771",
    "Transfer: insufficient lamports 6,990,000, need 8,526,566,072",
    "Program log:",
    "Program ComputeBudget111111111111111111111111111111 log: Instruction: RequestUnits",
    "Program ComputeBudget111111111111111111111111111111 request units: 42",
    "BPF program consumed 1,234 of 2,000 units",
];

fn extend_round_robin<'a>(out: &mut Vec<&'a str>, samples: &'a [&'a str], count: usize) {
    for index in 0..count {
        out.push(samples[index % samples.len()]);
    }
}

fn epoch_822_shape_weighted_corpus() -> Vec<&'static str> {
    let mut lines = Vec::with_capacity(26_767);
    extend_round_robin(&mut lines, INVOKE_LINES, 9_168);
    extend_round_robin(&mut lines, SUCCESS_LINES, 7_781);
    extend_round_robin(&mut lines, PROGRAM_LOG_LINES, 4_154);
    extend_round_robin(&mut lines, CONSUMED_LINES, 3_815);
    extend_round_robin(&mut lines, FAILURE_LINES, 1_387);
    extend_round_robin(&mut lines, PROGRAM_DATA_LINES, 300);
    extend_round_robin(&mut lines, PROGRAM_RETURN_LINES, 138);
    extend_round_robin(&mut lines, PROGRAM_CONSUMPTION_LINES, 24);
    lines
}

fn dominant_program_tail_corpus() -> Vec<&'static str> {
    let mut lines = Vec::with_capacity(16_000);
    extend_round_robin(&mut lines, INVOKE_LINES, 4_000);
    extend_round_robin(&mut lines, SUCCESS_LINES, 4_000);
    extend_round_robin(&mut lines, CONSUMED_LINES, 4_000);
    extend_round_robin(&mut lines, FAILURE_LINES, 4_000);
    lines
}

fn payload_corpus() -> Vec<&'static str> {
    let mut lines = Vec::with_capacity(10_000);
    extend_round_robin(&mut lines, PROGRAM_LOG_LINES, 4_000);
    extend_round_robin(&mut lines, PROGRAM_DATA_LINES, 2_000);
    extend_round_robin(&mut lines, PROGRAM_RETURN_LINES, 2_000);
    extend_round_robin(&mut lines, MISC_LINES, 2_000);
    lines
}

fn score(parsed: ParsedLogLine<'_>) -> usize {
    match parsed {
        ParsedLogLine::CustomProgramError { code } => code as usize,
        ParsedLogLine::FailedToComplete { reason } => reason.len(),
        ParsedLogLine::UnknownProgram { program }
        | ParsedLogLine::LoaderUpgradedProgram { program }
        | ParsedLogLine::BpfInvoke { program }
        | ParsedLogLine::BpfSuccess { program }
        | ParsedLogLine::Invoke { program, .. }
        | ParsedLogLine::Success { program }
        | ParsedLogLine::ProgramNotCached {
            program: Some(program),
        }
        | ParsedLogLine::ProgramNotDeployed {
            program: Some(program),
        } => program.len(),
        ParsedLogLine::UnknownAccount { account }
        | ParsedLogLine::LoaderFinalizedAccount { account }
        | ParsedLogLine::RuntimeWritablePrivilegeEscalated { account }
        | ParsedLogLine::RuntimeSignerPrivilegeEscalated { account }
        | ParsedLogLine::RuntimeAccountOwnerBalanceVerificationFailed { account } => account.len(),
        ParsedLogLine::BpfFailure { program, reason }
        | ParsedLogLine::Failure { program, reason } => program.len() ^ reason.len(),
        ParsedLogLine::BpfConsumed { used, limit }
        | ParsedLogLine::Consumed { used, limit, .. } => used as usize ^ limit as usize,
        ParsedLogLine::SystemTransferInsufficient { have, need } => have as usize ^ need as usize,
        ParsedLogLine::SystemAllocateAccountAlreadyInUse { account }
        | ParsedLogLine::SystemCreateAccountAlreadyInUse { account } => {
            account.address.len() ^ account.base.map(str::len).unwrap_or_default()
        }
        ParsedLogLine::SystemCreateAccountDataSizeLimited { limit } => limit as usize,
        ParsedLogLine::ProgramLog { text }
        | ParsedLogLine::ProgramData { data: text }
        | ParsedLogLine::Plain { text } => text.len(),
        ParsedLogLine::ProgramIdLog { program, text }
        | ParsedLogLine::ProgramReturn {
            program,
            data: text,
        } => program.len() ^ text.len(),
        ParsedLogLine::ProgramConsumption { units }
        | ParsedLogLine::CbRequestUnits { units, .. } => units as usize,
        ParsedLogLine::ProgramNotCached { program: None }
        | ParsedLogLine::ProgramNotDeployed { program: None } => 1,
        ParsedLogLine::LogTruncated
        | ParsedLogLine::VerifyEd25519
        | ParsedLogLine::VerifySecp256k1
        | ParsedLogLine::CloseContextState
        | ParsedLogLine::ProgramAccountNotWritable
        | ParsedLogLine::ProgramIdMismatch
        | ParsedLogLine::ProgramNotUpgradeable
        | ParsedLogLine::ProgramAndProgramDataAccountMismatch
        | ParsedLogLine::ProgramWasExtendedInThisBlockAlready
        | ParsedLogLine::StakeMergingAccounts
        | ParsedLogLine::SystemTransferFromMustNotCarryData
        | ParsedLogLine::UnparsedProgram => 1,
    }
}

fn bench_corpus(c: &mut Criterion, name: &str, corpus: Vec<&'static str>) {
    let bytes = corpus.iter().map(|line| line.len()).sum::<usize>() as u64;
    let mut group = c.benchmark_group("parse_line");
    group.throughput(Throughput::ElementsAndBytes {
        elements: corpus.len() as u64,
        bytes,
    });
    group.bench_function(name, |b| {
        b.iter(|| {
            let mut checksum = 0usize;
            for line in &corpus {
                checksum ^= score(parse_line(black_box(*line)));
            }
            black_box(checksum)
        });
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    black_box([COMPUTE_BUDGET, TOKEN, RAYDIUM]);
    bench_corpus(
        c,
        "epoch_822_shape_weighted",
        epoch_822_shape_weighted_corpus(),
    );
    bench_corpus(c, "dominant_program_tail", dominant_program_tail_corpus());
    bench_corpus(c, "payload_and_misc", payload_corpus());
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

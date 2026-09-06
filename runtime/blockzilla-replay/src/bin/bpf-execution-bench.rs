//! Phase-isolated microbenchmark for Blockzilla's legacy-BPF pipeline.
//!
//! The benchmark keeps production semantics out of this binary: it calls the
//! public compiler/executor for authoritative cold compilation and VM timing,
//! and mirrors the launch-era account ABI only to isolate serialization and
//! fixed-size output copy-back.  The mirror is covered by local round-trip
//! tests and the production end-to-end path remains separately benchmarked for
//! a bundled fixture that changes the first byte of a writable program-owned
//! account.
//!
//! On x86-64, `production_compile_cold` includes the upstream solana-sbpf JIT
//! and execution phases can require that native artifact explicitly.  On
//! AArch64, it includes Blockzilla's strict-subset Cranelift compiler; programs
//! outside that subset report `InterpreterOnly` instead of silently claiming
//! native execution.  Native artifacts are process-local executable memory,
//! not persisted shared objects.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    convert::Infallible,
    fmt::Write as _,
    hint::black_box,
    path::PathBuf,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use blockzilla_replay::{
    AccountData, AccountMap, AccountSnapshot, CompilationBackend, CompiledProgram, ExecutionEngine,
    ExecutionOutcome, ExecutionRequest, LaunchAccountMeta, LaunchBpfLoaderRent, LoaderAccountKind,
    ReplayCompiler, apply_launch_bpf_program_instruction, extract_program,
};
use clap::{Parser, ValueEnum};
use hashbrown::HashMap;
use sha2::{Digest, Sha256};
use solana_sbpf::{
    aligned_memory::AlignedMemory,
    ebpf,
    elf::Executable,
    error::EbpfError,
    memory_region::{MemoryMapping, MemoryRegion},
    program::{BuiltinProgram, SBPFVersion},
    verifier::RequisiteVerifier,
    vm::{Config, ContextObject},
};

const FULL_ABI_PROGRAM_ID: [u8; 32] = [9; 32];
const PROGRAM_CACHE_KEY: [u8; 32] = [0x42; 32];
// Legacy launch ABI: 8-byte account count, then the first unique account's
// duplicate marker (1), signer/writable flags (2), pubkey (32), lamports (8),
// and data length (8).
const FIRST_ACCOUNT_DATA_OFFSET: i16 = 59;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied this exact allocation layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplied this exact allocation layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: pointer and layout came from this allocator.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: pointer and layout came from this allocator; size is passed
        // through unchanged.
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_allocation(new_size);
        }
        new_pointer
    }
}

fn record_allocation(bytes: usize) {
    if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Debug, Parser)]
#[command(name = "bpf-execution-bench")]
struct Cli {
    /// Execute the raw one-byte fixture ABI or a launch-era account buffer.
    #[arg(long, value_enum, default_value_t = BenchPath::FullAbi)]
    path: BenchPath,

    /// Read a bare SBPF ELF from disk instead of the bundled minor fixture.
    #[arg(long)]
    elf: Option<PathBuf>,

    /// Compile/load/verify operations per round. Keep this much lower than
    /// execution iterations because every operation is intentionally cold.
    #[arg(long, default_value_t = 20)]
    compile_iterations: u64,

    #[arg(long, default_value_t = 4)]
    accounts: usize,

    #[arg(long, default_value_t = 200)]
    account_data_bytes: usize,

    #[arg(long, default_value_t = 10_000)]
    iterations: u64,

    #[arg(long, default_value_t = 100)]
    warmups: u64,

    #[arg(long, default_value_t = 5)]
    rounds: usize,

    /// Benchmark extraction/loading/verification/compilation without trying
    /// to invoke an arbitrary external program.
    #[arg(long)]
    compile_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchPath {
    Raw,
    FullAbi,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    allocation_calls: u64,
    allocated_bytes: u64,
    checksum: u64,
}

struct FullAbiFixture {
    metas: Vec<LaunchAccountMeta>,
    accounts: AccountMap,
    instruction_data: Vec<u8>,
}

/// Owns the backing allocations referenced by `mapping`. Box/AlignedMemory
/// payload addresses remain stable when this value is moved.
struct DirectMappingFixture {
    mapping: MemoryMapping,
    _ro: Box<[u8]>,
    _stack: AlignedMemory<{ ebpf::HOST_ALIGN }>,
    _heap: AlignedMemory<{ ebpf::HOST_ALIGN }>,
    _input: Box<[u8]>,
}

#[derive(Clone, Copy)]
enum MappingPattern {
    HotInput,
    RoundRobin,
}

impl MappingPattern {
    fn label(self) -> &'static str {
        match self {
            Self::HotInput => "hot_input",
            Self::RoundRobin => "round_robin",
        }
    }
}

#[derive(Debug)]
struct PreparedAccount {
    pubkey: [u8; 32],
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<AccountData>,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
}

/// Context used only to decompose ELF load, requisite verification, and the
/// upstream x86 JIT. Production compilation is always measured separately
/// through `ReplayCompiler` and therefore uses the real syscall context.
struct BenchContext {
    remaining: u64,
}

impl ContextObject for BenchContext {
    fn consume(&mut self, amount: u64) {
        self.remaining = self.remaining.saturating_sub(amount);
    }

    fn get_remaining(&self) -> u64 {
        self.remaining
    }

    fn active_mapping_ptr(&mut self) -> NonNull<MemoryMapping> {
        // This context is never executed. Loading, verification, and codegen
        // do not dereference an active guest mapping.
        NonNull::dangling()
    }
}

solana_sbpf::declare_builtin_function!(
    BenchNoopSyscall,
    fn rust(
        _context: &mut BenchContext,
        _arg1: u64,
        _arg2: u64,
        _arg3: u64,
        _arg4: u64,
        _arg5: u64,
    ) -> Result<u64, Infallible> {
        Ok(0)
    }
);

fn main() -> Result<()> {
    let cli = Cli::parse();
    ensure!(cli.iterations > 0, "--iterations must be positive");
    ensure!(
        cli.compile_iterations > 0,
        "--compile-iterations must be positive"
    );
    ensure!(cli.rounds > 0, "--rounds must be positive");
    ensure!(
        (1..=255).contains(&cli.accounts),
        "--accounts must be in 1..=255"
    );
    if !cli.compile_only && cli.elf.is_none() && cli.path == BenchPath::FullAbi {
        ensure!(
            cli.account_data_bytes >= 8,
            "embedded full-ABI mutation requires --account-data-bytes >= 8"
        );
    }

    let external_elf = cli.elf.is_some();
    let mut elf = match &cli.elf {
        Some(path) => std::fs::read(path)
            .with_context(|| format!("read external SBPF ELF {}", path.display()))?,
        None => STANDARD
            .decode(include_str!("../../fixtures/relative_call_sbpfv0.so.b64").trim())
            .context("decode embedded SBPFv0 fixture")?,
    };
    if !external_elf && cli.path == BenchPath::FullAbi {
        patch_fixture_to_mutate_first_account_data(&mut elf)?;
    }

    let extracted = extract_program(LoaderAccountKind::BareElf, &elf)
        .context("extract canonical ELF before benchmark")?;
    let compiler = ReplayCompiler::new();
    let program = compiler
        .compile_extracted(&extracted)
        .context("production-compile selected SBPF ELF")?;
    let fixture = full_abi_fixture(cli.accounts, cli.account_data_bytes);
    let abi_input = serialize_parameters_mirror(
        FULL_ABI_PROGRAM_ID,
        &fixture.metas,
        &fixture.accounts,
        &fixture.instruction_data,
    )?;

    print_program_manifest(
        cli.elf.as_ref(),
        !external_elf && cli.path == BenchPath::FullAbi,
        &program,
        &extracted.elf_sha256,
        abi_input.len(),
    );
    benchmark_memory_mapping(&cli)?;
    println!(
        "abi_wire_sha256={} account_state_sha256={}",
        hex_digest(Sha256::digest(&abi_input).into()),
        hex_digest(account_state_fingerprint(&fixture.accounts)),
    );

    benchmark_compilation_pipeline(&cli, &elf, &extracted, &compiler)?;
    benchmark_abi_pipeline(&cli, &fixture, &abi_input)?;

    if cli.compile_only {
        println!("execution=SKIP reason=compile-only");
        return Ok(());
    }

    let execution_input = match cli.path {
        BenchPath::Raw => vec![1_u8],
        BenchPath::FullAbi => abi_input.clone(),
    };
    benchmark_execution_engines(&cli, &compiler, &program, &execution_input)?;

    if !external_elf && cli.path == BenchPath::FullAbi {
        benchmark_production_full_abi(&cli, &compiler, &program, &fixture)?;
    } else {
        println!(
            "phase=production_full_abi status=SKIP reason={}",
            if external_elf {
                "external-program-may-mutate-fixture"
            } else {
                "raw-fixture-returns-nonzero"
            }
        );
    }
    Ok(())
}

fn benchmark_memory_mapping(cli: &Cli) -> Result<()> {
    const MEMORY_OPERATIONS_PER_ITERATION: usize = 8;
    println!(
        "memory_mapping_control memory_operations_per_iteration={MEMORY_OPERATIONS_PER_ITERATION} address_translation=checked stack_gaps=enabled"
    );

    for pattern in [MappingPattern::HotInput, MappingPattern::RoundRobin] {
        benchmark_mapping_pair(cli, pattern)?;
    }
    Ok(())
}

fn benchmark_mapping_pair(cli: &Cli, pattern: MappingPattern) -> Result<()> {
    let mut unaligned = DirectMappingFixture::new(false)?;
    let mut aligned = DirectMappingFixture::new(true)?;
    if cli.warmups != 0 {
        run_operations(&mut || unaligned.run(pattern), cli.warmups)?;
        run_operations(&mut || aligned.run(pattern), cli.warmups)?;
    }

    let mut unaligned_timing = Vec::with_capacity(cli.rounds);
    let mut aligned_timing = Vec::with_capacity(cli.rounds);
    for round in 0..cli.rounds {
        if round % 2 == 0 {
            unaligned_timing.push(measure_mapping(
                &mut unaligned,
                pattern,
                cli.iterations,
                false,
            )?);
            aligned_timing.push(measure_mapping(
                &mut aligned,
                pattern,
                cli.iterations,
                false,
            )?);
        } else {
            aligned_timing.push(measure_mapping(
                &mut aligned,
                pattern,
                cli.iterations,
                false,
            )?);
            unaligned_timing.push(measure_mapping(
                &mut unaligned,
                pattern,
                cli.iterations,
                false,
            )?);
        }
    }

    let unaligned_allocations = measure_mapping(&mut unaligned, pattern, cli.iterations, true)?;
    let aligned_allocations = measure_mapping(&mut aligned, pattern, cli.iterations, true)?;
    ensure!(
        unaligned_timing
            .iter()
            .chain(&aligned_timing)
            .all(|sample| sample.checksum == unaligned_timing[0].checksum),
        "aligned and unaligned mapping controls produced different values"
    );
    let unaligned_allocation_samples = vec![unaligned_allocations; cli.rounds];
    let aligned_allocation_samples = vec![aligned_allocations; cli.rounds];
    print_summary(
        &format!("memory_mapping_{}_unaligned", pattern.label()),
        &unaligned_timing,
        &unaligned_allocation_samples,
        cli.iterations,
        unaligned_timing[0].checksum,
    );
    print_summary(
        &format!("memory_mapping_{}_aligned", pattern.label()),
        &aligned_timing,
        &aligned_allocation_samples,
        cli.iterations,
        aligned_timing[0].checksum,
    );
    Ok(())
}

fn measure_mapping(
    fixture: &mut DirectMappingFixture,
    pattern: MappingPattern,
    iterations: u64,
    count_allocations: bool,
) -> Result<Sample> {
    measure(&mut || fixture.run(pattern), iterations, count_allocations)
}

impl DirectMappingFixture {
    fn new(aligned_memory_mapping: bool) -> Result<Self> {
        const REGION_BYTES: usize = 64;
        const WORD: u8 = 0x11;

        let mut config = benchmark_config();
        config.aligned_memory_mapping = aligned_memory_mapping;
        let ro = vec![WORD; REGION_BYTES].into_boxed_slice();
        let mut stack = AlignedMemory::zero_filled(config.stack_size());
        stack.as_slice_mut().fill(WORD);
        let mut heap = AlignedMemory::zero_filled(REGION_BYTES);
        heap.as_slice_mut().fill(WORD);
        let mut input = vec![WORD; REGION_BYTES].into_boxed_slice();
        let ro_ptr: *const [u8] = ro.as_ref();
        let stack_ptr: *mut [u8] = stack.as_slice_mut();
        let heap_ptr: *mut [u8] = heap.as_slice_mut();
        let input_ptr: *mut [u8] = input.as_mut();
        let mut regions = Vec::with_capacity(5);
        regions.extend([
            MemoryRegion::new(ro_ptr, ebpf::MM_BYTECODE_START),
            MemoryRegion::new_gapped(
                stack_ptr,
                ebpf::MM_STACK_START,
                config.stack_frame_size as u64,
            ),
            MemoryRegion::new(heap_ptr, ebpf::MM_HEAP_START),
            MemoryRegion::new(input_ptr, ebpf::MM_INPUT_START),
        ]);
        // SAFETY: the four backing allocations are retained by the returned
        // fixture and their payload addresses remain stable when it is moved.
        let mapping = unsafe { MemoryMapping::new(regions, &config, SBPFVersion::V0) }
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(Self {
            mapping,
            _ro: ro,
            _stack: stack,
            _heap: heap,
            _input: input,
        })
    }

    fn run(&mut self, pattern: MappingPattern) -> Result<u64> {
        match pattern {
            MappingPattern::HotInput => self.hot_input_batch(),
            MappingPattern::RoundRobin => self.round_robin_batch(),
        }
    }

    fn hot_input_batch(&mut self) -> Result<u64> {
        let mut checksum = 0_u64;
        for offset in [0_u64, 8, 16, 24] {
            checksum ^= self.load(ebpf::MM_INPUT_START + offset)?;
            self.store(ebpf::MM_INPUT_START + offset)?;
        }
        Ok(checksum)
    }

    fn round_robin_batch(&mut self) -> Result<u64> {
        let mut checksum = self.load(ebpf::MM_BYTECODE_START)?;
        checksum ^= self.load(ebpf::MM_BYTECODE_START + 8)?;
        for address in [
            ebpf::MM_STACK_START,
            ebpf::MM_HEAP_START,
            ebpf::MM_INPUT_START,
        ] {
            checksum ^= self.load(address)?;
            self.store(address)?;
        }
        Ok(checksum)
    }

    fn load(&mut self, address: u64) -> Result<u64> {
        let value: std::result::Result<u64, EbpfError> =
            self.mapping.load::<u64>(black_box(address)).into();
        sbpf(value)
    }

    fn store(&mut self, address: u64) -> Result<()> {
        const WORD: u64 = 0x1111_1111_1111_1111;
        let result: std::result::Result<u64, EbpfError> = self
            .mapping
            .store::<u64>(black_box(WORD), black_box(address))
            .into();
        black_box(sbpf(result)?);
        Ok(())
    }
}

fn print_program_manifest(
    source: Option<&PathBuf>,
    mutates_full_abi: bool,
    program: &CompiledProgram,
    elf_sha256: &[u8; 32],
    abi_input_bytes: usize,
) {
    let source = source
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| {
            if mutates_full_abi {
                "embedded-account-data-write-sbpfv0".to_owned()
            } else {
                "embedded-relative-call-sbpfv0".to_owned()
            }
        });
    println!(
        "program source={} host_arch={} target={} elf_bytes={} text_bytes={} elf_sha256={} backend={:?} native_machine_code_bytes={} native_lowered_instructions={} compiler={} verifier={} abi_input_bytes={}",
        source,
        std::env::consts::ARCH,
        program.manifest.target,
        program.manifest.elf_len,
        program.manifest.text_len,
        hex_digest(*elf_sha256),
        program.manifest.backend,
        optional_usize(program.manifest.native_machine_code_len),
        optional_u64(program.manifest.native_lowered_instruction_count),
        program.manifest.compiler_id,
        program.manifest.verifier,
        abi_input_bytes,
    );
    println!(
        "native_artifact persistence=process-local artifact_key={} entry_abi={} backend_id={}",
        hex_digest(program.manifest.artifact_key),
        program.manifest.native_entry_abi_id,
        program.manifest.native_backend_id,
    );
}

fn benchmark_compilation_pipeline(
    cli: &Cli,
    account_data: &[u8],
    extracted: &blockzilla_replay::ExtractedProgram,
    compiler: &ReplayCompiler,
) -> Result<()> {
    let compile_warmups = cli.warmups.min(3);
    benchmark_phase(
        "elf_extract",
        cli.compile_iterations,
        compile_warmups,
        cli.rounds,
        || {
            let value = extract_program(LoaderAccountKind::BareElf, black_box(account_data))?;
            Ok(fingerprint_bytes(&value.elf_sha256)
                ^ value.elf.len() as u64
                ^ value.elf_offset as u64)
        },
    )?;

    let loader = benchmark_loader()?;
    benchmark_phase(
        "sbpf_elf_load",
        cli.compile_iterations,
        compile_warmups,
        cli.rounds,
        || {
            let executable = sbpf(Executable::<BenchContext>::from_elf(
                black_box(&extracted.elf),
                Arc::clone(&loader),
            ))?;
            let (_, text) = executable.get_text_bytes();
            Ok(text.len() as u64
                ^ executable.get_entrypoint_instruction_offset() as u64
                ^ executable.get_ro_section().len() as u64)
        },
    )?;

    let verifier_executable = sbpf(Executable::<BenchContext>::from_elf(
        &extracted.elf,
        Arc::clone(&loader),
    ))?;
    benchmark_phase(
        "requisite_verifier",
        cli.compile_iterations,
        compile_warmups,
        cli.rounds,
        || {
            sbpf(verifier_executable.verify::<RequisiteVerifier>())?;
            Ok(verifier_executable.get_text_bytes().1.len() as u64)
        },
    )?;

    #[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
    {
        let jit_executable = sbpf(Executable::<BenchContext>::from_elf(
            &extracted.elf,
            Arc::clone(&loader),
        ))?;
        sbpf(jit_executable.verify::<RequisiteVerifier>())?;
        benchmark_phase(
            "sbpf_jit_cold_recompile",
            cli.compile_iterations,
            compile_warmups,
            cli.rounds,
            || {
                sbpf(jit_executable.jit_compile())?;
                let machine_code_len = jit_executable
                    .get_compiled_program()
                    .map(|compiled| compiled.machine_code_length() as u64)
                    .unwrap_or_default();
                // JIT layout can legitimately vary across recompilations
                // (for example through process-local address blinding). Keep
                // the value live without treating it as semantic output.
                black_box(machine_code_len);
                Ok(jit_executable.get_text_bytes().1.len() as u64)
            },
        )?;
        ensure!(
            jit_executable.get_compiled_program().is_some(),
            "x86 JIT phase did not retain an artifact"
        );
        benchmark_phase(
            "sbpf_jit_cached_get",
            cli.iterations,
            cli.warmups,
            cli.rounds,
            || {
                let compiled = jit_executable
                    .get_compiled_program()
                    .context("cached x86 JIT artifact disappeared")?;
                Ok(compiled.machine_code_length() as u64)
            },
        )?;
    }

    #[cfg(not(all(not(target_os = "windows"), target_arch = "x86_64")))]
    println!("phase=sbpf_jit_cold_recompile status=SKIP reason=upstream-jit-is-x86_64-only");

    benchmark_phase(
        "production_compile_cold",
        cli.compile_iterations,
        compile_warmups,
        cli.rounds,
        || {
            let compiled = compiler.compile_extracted(black_box(extracted))?;
            Ok(compilation_fingerprint(&compiled))
        },
    )?;

    let mut replay_cache = HashMap::new();
    replay_cache.insert(PROGRAM_CACHE_KEY, compiler.compile_extracted(extracted)?);
    benchmark_phase(
        "replay_program_cache_hit",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            let compiled = replay_cache
                .get(black_box(&PROGRAM_CACHE_KEY))
                .context("benchmark program cache miss")?;
            Ok(compilation_fingerprint(compiled))
        },
    )?;
    Ok(())
}

fn benchmark_abi_pipeline(cli: &Cli, fixture: &FullAbiFixture, abi_input: &[u8]) -> Result<()> {
    benchmark_phase(
        "abi_pre_account_prepare",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            let prepared = prepare_pre_accounts_mirror(
                FULL_ABI_PROGRAM_ID,
                &fixture.metas,
                &fixture.accounts,
            )?;
            Ok(prepared_accounts_checksum(&prepared))
        },
    )?;

    benchmark_phase(
        "abi_parameter_serialize",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            let buffer = serialize_parameters_mirror(
                FULL_ABI_PROGRAM_ID,
                &fixture.metas,
                &fixture.accounts,
                &fixture.instruction_data,
            )?;
            Ok(buffer_checksum(&buffer))
        },
    )?;

    let mut copyback_accounts = fixture.accounts.clone();
    benchmark_phase(
        "abi_output_copyback",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            deserialize_parameters_mirror(
                &fixture.metas,
                &mut copyback_accounts,
                black_box(abi_input),
            )?;
            Ok(account_sample_checksum(&copyback_accounts))
        },
    )?;
    ensure!(
        copyback_accounts == fixture.accounts,
        "ABI output copy-back did not preserve the no-op fixture"
    );
    ensure!(
        account_data_allocations_are_shared(&copyback_accounts, &fixture.accounts),
        "no-op ABI output copy-back detached account data"
    );
    println!(
        "abi_equivalence=PASS accounts={} account_data_bytes={} buffer_bytes={} external_data_clone_accounts={}",
        cli.accounts,
        cli.account_data_bytes,
        abi_input.len(),
        cli.accounts / 2,
    );
    Ok(())
}

fn benchmark_execution_engines(
    cli: &Cli,
    compiler: &ReplayCompiler,
    program: &CompiledProgram,
    input: &[u8],
) -> Result<()> {
    let interpreted = compiler
        .execute_with_request(program, input.to_vec(), ExecutionRequest::Interpreter)
        .context("probe explicit interpreter")?;
    print_engine_probe("interpreter", &interpreted);

    let native_probe = match compiler.execute_with_request(
        program,
        input.to_vec(),
        ExecutionRequest::NativeRequired,
    ) {
        Ok(native) => {
            ensure_semantic_equivalence(&interpreted, &native, "native")?;
            print_engine_probe("native-required", &native);
            Some(native)
        }
        Err(blockzilla_replay::CompilerError::NativeUnavailable { reason }) => {
            println!("engine_probe request=native-required status=UNAVAILABLE reason={reason}");
            None
        }
        Err(error) => return Err(error).context("probe explicit native execution"),
    };

    let auto = compiler
        .execute_with_request(program, input.to_vec(), ExecutionRequest::Auto)
        .context("probe automatic execution")?;
    ensure_semantic_equivalence(&interpreted, &auto, "auto")?;
    print_engine_probe("auto", &auto);
    println!(
        "execution_equivalence={} semantic_sha256={}",
        if native_probe.is_some() {
            "PASS"
        } else {
            "PASS_INTERPRETER_ONLY"
        },
        hex_digest(execution_semantic_fingerprint(&interpreted)),
    );

    benchmark_input_reset(cli, input)?;
    benchmark_execution_request(
        "execute_interpreter",
        ExecutionRequest::Interpreter,
        cli,
        compiler,
        program,
        input,
    )?;
    if native_probe.is_some() {
        benchmark_execution_request(
            "execute_native_required",
            ExecutionRequest::NativeRequired,
            cli,
            compiler,
            program,
            input,
        )?;
    } else {
        println!("phase=execute_native_required status=SKIP reason=native-unavailable");
    }
    benchmark_execution_request(
        "execute_auto",
        ExecutionRequest::Auto,
        cli,
        compiler,
        program,
        input,
    )?;
    Ok(())
}

fn benchmark_input_reset(cli: &Cli, input: &[u8]) -> Result<()> {
    let mut working = vec![0_u8; input.len()];
    benchmark_phase(
        "execution_input_reset_control",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            working.copy_from_slice(black_box(input));
            Ok(buffer_checksum(&working))
        },
    )?;
    Ok(())
}

fn benchmark_execution_request(
    label: &str,
    request: ExecutionRequest,
    cli: &Cli,
    compiler: &ReplayCompiler,
    program: &CompiledProgram,
    input: &[u8],
) -> Result<()> {
    let mut working = input.to_vec();
    benchmark_phase(label, cli.iterations, cli.warmups, cli.rounds, || {
        working.copy_from_slice(black_box(input));
        let owned = std::mem::take(&mut working);
        let outcome = compiler.execute_with_request(program, owned, request)?;
        let checksum = execution_sample_checksum(&outcome);
        working = outcome.input_after;
        Ok(checksum)
    })?;
    Ok(())
}

fn benchmark_production_full_abi(
    cli: &Cli,
    compiler: &ReplayCompiler,
    program: &CompiledProgram,
    fixture: &FullAbiFixture,
) -> Result<()> {
    let mut accounts = fixture.accounts.clone();
    benchmark_phase(
        "production_full_abi_auto_changed_data",
        cli.iterations,
        cli.warmups,
        cli.rounds,
        || {
            // Transaction overlays start from cheap AccountData clones. Reset
            // before every sample so VM copyback always exercises the first
            // write to a shared payload instead of reusing a prior detached
            // allocation.
            accounts.clone_from(&fixture.accounts);
            debug_assert!(account_data_allocations_are_shared(
                &accounts,
                &fixture.accounts
            ));
            let mutation = apply_launch_bpf_program_instruction(
                FULL_ABI_PROGRAM_ID,
                &fixture.instruction_data,
                &fixture.metas,
                &mut accounts,
                compiler,
                program,
                LaunchBpfLoaderRent {
                    lamports_per_byte_year: 0,
                    exemption_threshold: 0.0,
                },
            )?;
            Ok(engine_code(mutation.engine)
                ^ mutation.watchdog_instructions
                ^ account_sample_checksum(&accounts))
        },
    )?;
    ensure!(
        embedded_fixture_mutated_exactly_first_account(&fixture.accounts, &accounts),
        "production full-ABI fixture did not make its expected account-data mutation"
    );
    println!(
        "production_full_abi_equivalence=PASS mutation=first-program-owned-account-byte first_write_cow=exercised"
    );
    Ok(())
}

fn benchmark_phase<F>(
    label: &str,
    iterations: u64,
    warmups: u64,
    rounds: usize,
    mut operation: F,
) -> Result<u64>
where
    F: FnMut() -> Result<u64>,
{
    if warmups != 0 {
        run_operations(&mut operation, warmups)?;
    }
    let mut timing: Vec<Sample> = Vec::with_capacity(rounds);
    let mut allocations: Vec<Sample> = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let (timing_sample, allocation_sample) = if round % 2 == 0 {
            (
                measure(&mut operation, iterations, false)?,
                measure(&mut operation, iterations, true)?,
            )
        } else {
            let allocation_sample = measure(&mut operation, iterations, true)?;
            let timing_sample = measure(&mut operation, iterations, false)?;
            (timing_sample, allocation_sample)
        };
        ensure!(
            timing_sample.checksum == allocation_sample.checksum,
            "phase {label} output changed while allocation counting"
        );
        if let Some(first) = timing.first() {
            ensure!(
                first.checksum == timing_sample.checksum,
                "phase {label} output changed between rounds"
            );
        }
        timing.push(timing_sample);
        allocations.push(allocation_sample);
    }
    let checksum = timing[0].checksum;
    print_summary(label, &timing, &allocations, iterations, checksum);
    Ok(checksum)
}

fn measure<F>(operation: &mut F, iterations: u64, count_allocations: bool) -> Result<Sample>
where
    F: FnMut() -> Result<u64>,
{
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(count_allocations, Ordering::Relaxed);
    let started = Instant::now();
    let result = run_operations(operation, iterations);
    let elapsed = started.elapsed();
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    let checksum = result?;
    Ok(Sample {
        elapsed,
        allocation_calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        checksum,
    })
}

fn run_operations<F>(operation: &mut F, iterations: u64) -> Result<u64>
where
    F: FnMut() -> Result<u64>,
{
    let mut checksum = 0xcbf2_9ce4_8422_2325_u64;
    for _ in 0..iterations {
        checksum = checksum.rotate_left(7).wrapping_mul(0x9e37_79b1_85eb_ca87) ^ operation()?;
    }
    Ok(black_box(checksum))
}

fn print_summary(
    label: &str,
    timing: &[Sample],
    allocations: &[Sample],
    iterations: u64,
    checksum: u64,
) {
    let elapsed = median_duration(timing.iter().map(|sample| sample.elapsed).collect());
    let calls = median_u64(
        allocations
            .iter()
            .map(|sample| sample.allocation_calls)
            .collect(),
    );
    let bytes = median_u64(
        allocations
            .iter()
            .map(|sample| sample.allocated_bytes)
            .collect(),
    );
    println!(
        "phase={label} iterations={iterations} median_ms={:.3} ns_per_operation={:.1} operations_per_s={:.1} allocation_calls={} calls_per_operation={:.3} allocated_bytes={} bytes_per_operation={:.1} checksum={checksum:016x}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() as f64 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64(),
        calls,
        calls as f64 / iterations as f64,
        bytes,
        bytes as f64 / iterations as f64,
    );
}

fn sbpf<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> Result<T> {
    result.map_err(|error| anyhow!(error.to_string()))
}

fn benchmark_loader() -> Result<Arc<BuiltinProgram<BenchContext>>> {
    let mut loader = BuiltinProgram::new_loader(benchmark_config());
    for name in [
        "abort",
        "sol_create_program_address",
        "sol_invoke_signed_rust",
        "sol_panic_",
        "sol_log_",
        "sol_log_64_",
        "sol_alloc_free_",
    ] {
        loader
            .register_definition::<BenchNoopSyscall>(name)
            .with_context(|| format!("register benchmark syscall {name}"))?;
    }
    Ok(Arc::new(loader))
}

fn benchmark_config() -> Config {
    // Keep this decomposition config byte-for-byte aligned with
    // ReplayCompiler::new. The authoritative total remains
    // `production_compile_cold` if that production profile changes.
    Config {
        max_call_depth: 20,
        enable_instruction_meter: true,
        instruction_meter_checkpoint_distance: 1_000,
        reject_broken_elfs: false,
        noop_instruction_rate: 0,
        aligned_memory_mapping: true,
        enabled_sbpf_versions: SBPFVersion::V0..=SBPFVersion::V0,
        ..Config::default()
    }
}

fn full_abi_fixture(account_count: usize, account_data_bytes: usize) -> FullAbiFixture {
    let mut metas = Vec::with_capacity(account_count);
    let mut accounts = AccountMap::new();
    for index in 0..account_count {
        let mut pubkey = [0_u8; 32];
        pubkey[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
        metas.push(LaunchAccountMeta {
            pubkey,
            is_signer: index == 0,
            is_writable: true,
        });
        accounts.insert(
            pubkey,
            AccountSnapshot {
                lamports: index as u64 + 1,
                // Alternate ownership so pre-account preparation exercises
                // both the program-owned case and the external-data baseline
                // reference retained by the production verifier.
                owner: if index % 2 == 0 {
                    FULL_ABI_PROGRAM_ID
                } else {
                    [8; 32]
                },
                executable: false,
                rent_epoch: 0,
                data: vec![index as u8; account_data_bytes].into(),
            },
        );
    }
    FullAbiFixture {
        metas,
        accounts,
        instruction_data: vec![0xaa, 0xbb],
    }
}

fn patch_fixture_to_mutate_first_account_data(elf: &mut [u8]) -> Result<()> {
    let entry = 0x120 + 4 * 8;
    let entry_end = entry + 4 * 8;
    ensure!(
        elf.get(entry..entry_end).is_some(),
        "embedded fixture entrypoint is truncated"
    );
    let offset = FIRST_ACCOUNT_DATA_OFFSET.to_le_bytes();
    // The launch ABI input address arrives in r1 and r0 is zero at VM entry.
    // Increment the first byte of the first account, then write it back as a
    // little-endian u64. The fixture initializes that account with zero bytes,
    // so only byte zero changes. These four opcodes are deliberately inside
    // the strict AArch64 subset as well as the upstream interpreter/JIT:
    // LD_B_REG, ADD64_IMM, ST_DW_REG, EXIT.
    elf[entry..entry_end].copy_from_slice(&[
        0x71, 0x12, offset[0], offset[1], 0, 0, 0, 0, // r2 = *(u8 *)(r1 + 59)
        0x07, 0x02, 0, 0, 1, 0, 0, 0, // r2 += 1
        0x7b, 0x21, offset[0], offset[1], 0, 0, 0, 0, // *(u64 *)(r1 + 59) = r2
        0x95, 0, 0, 0, 0, 0, 0, 0, // exit with the entry r0 value (zero)
    ]);
    Ok(())
}

fn prepare_pre_accounts_mirror(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
) -> Result<Vec<PreparedAccount>> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| duplicate_position(&account_metas[..*index], meta.pubkey).is_none())
        .map(|(_, meta)| {
            let account = required_account(accounts, meta.pubkey)?;
            Ok(PreparedAccount {
                pubkey: meta.pubkey,
                is_writable: meta.is_writable,
                lamports: account.lamports,
                data_len: account.data.len(),
                data: should_verify_data(
                    account.owner,
                    program_id,
                    meta.is_writable,
                    account.executable,
                )
                .then(|| account.data.clone()),
                owner: account.owner,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
            })
        })
        .collect()
}

fn serialize_parameters_mirror(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    instruction_data: &[u8],
) -> Result<Vec<u8>> {
    let mut capacity = 8_usize
        .checked_add(8)
        .and_then(|value| value.checked_add(instruction_data.len()))
        .and_then(|value| value.checked_add(32))
        .context("ABI parameter length overflow")?;
    for (index, meta) in account_metas.iter().enumerate() {
        if duplicate_position(&account_metas[..index], meta.pubkey).is_some() {
            capacity = capacity
                .checked_add(1)
                .context("ABI parameter length overflow")?;
        } else {
            let account = required_account(accounts, meta.pubkey)?;
            capacity = capacity
                .checked_add(92)
                .and_then(|value| value.checked_add(account.data.len()))
                .context("ABI parameter length overflow")?;
        }
    }

    let mut bytes = Vec::with_capacity(capacity);
    push_u64(&mut bytes, account_metas.len() as u64);
    for (index, meta) in account_metas.iter().enumerate() {
        if let Some(position) = duplicate_position(&account_metas[..index], meta.pubkey) {
            bytes.push(position as u8);
            continue;
        }
        let account = required_account(accounts, meta.pubkey)?;
        bytes.push(u8::MAX);
        bytes.push(u8::from(meta.is_signer));
        bytes.push(u8::from(meta.is_writable));
        bytes.extend_from_slice(&meta.pubkey);
        push_u64(&mut bytes, account.lamports);
        push_u64(&mut bytes, account.data.len() as u64);
        bytes.extend_from_slice(&account.data);
        bytes.extend_from_slice(&account.owner);
        bytes.push(u8::from(account.executable));
        push_u64(&mut bytes, account.rent_epoch);
    }
    push_u64(&mut bytes, instruction_data.len() as u64);
    bytes.extend_from_slice(instruction_data);
    bytes.extend_from_slice(&program_id);
    ensure!(bytes.len() == capacity, "ABI capacity calculation drifted");
    Ok(bytes)
}

fn deserialize_parameters_mirror(
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    buffer: &[u8],
) -> Result<()> {
    let mut start = 8_usize;
    for (index, meta) in account_metas.iter().enumerate() {
        start = start.checked_add(1).context("malformed ABI buffer")?;
        if duplicate_position(&account_metas[..index], meta.pubkey).is_some() {
            continue;
        }
        start = start.checked_add(34).context("malformed ABI buffer")?;
        let lamports = read_u64(buffer, start)?;
        start = start.checked_add(16).context("malformed ABI buffer")?;
        let data_len = required_account(accounts, meta.pubkey)?.data.len();
        let data_end = start
            .checked_add(data_len)
            .context("malformed ABI buffer")?;
        let data = buffer
            .get(start..data_end)
            .context("malformed ABI buffer")?;
        let account = accounts
            .get_mut(&meta.pubkey)
            .with_context(|| format!("missing account {:?}", meta.pubkey))?;
        if account.lamports != lamports {
            account.lamports = lamports;
        }
        if account.data.as_slice() != data {
            account.data.set_from_slice(data);
        }
        start = data_end.checked_add(41).context("malformed ABI buffer")?;
    }
    Ok(())
}

fn should_verify_data(
    owner: [u8; 32],
    program_id: [u8; 32],
    is_writable: bool,
    is_executable: bool,
) -> bool {
    owner != program_id || !is_writable || is_executable
}

fn duplicate_position(metas: &[LaunchAccountMeta], pubkey: [u8; 32]) -> Option<usize> {
    metas.iter().position(|meta| meta.pubkey == pubkey)
}

fn required_account(accounts: &AccountMap, pubkey: [u8; 32]) -> Result<&AccountSnapshot> {
    accounts
        .get(&pubkey)
        .with_context(|| format!("missing account {pubkey:?}"))
}

fn account_data_allocations_are_shared(left: &AccountMap, right: &AccountMap) -> bool {
    left.len() == right.len()
        && left.iter().all(|(pubkey, left_account)| {
            right.get(pubkey).is_some_and(|right_account| {
                left_account
                    .data
                    .shares_allocation_with(&right_account.data)
            })
        })
}

fn embedded_fixture_mutated_exactly_first_account(before: &AccountMap, after: &AccountMap) -> bool {
    if before.len() != after.len() {
        return false;
    }
    // `full_abi_fixture` assigns pubkeys from 1..N in little-endian order and
    // the patched ELF mutates only the first message account (pubkey index 0).
    // Do not use HashMap iteration order.
    let mut first_pubkey = [0_u8; 32];
    first_pubkey[..8].copy_from_slice(&1u64.to_le_bytes());
    let Some(first_before) = before.get(&first_pubkey) else {
        return false;
    };
    let Some(first_after) = after.get(&first_pubkey) else {
        return false;
    };
    if first_before.data.is_empty()
        || first_after.lamports != first_before.lamports
        || first_after.owner != first_before.owner
        || first_after.executable != first_before.executable
        || first_after.rent_epoch != first_before.rent_epoch
        || first_after.data.len() != first_before.data.len()
        || first_after.data[0] != first_before.data[0].wrapping_add(1)
        || first_after.data[1..] != first_before.data[1..]
    {
        return false;
    }
    before.iter().all(|(pubkey, account)| {
        if pubkey == &first_pubkey {
            return true;
        }
        after
            .get(pubkey)
            .is_some_and(|after_account| after_account == account)
    })
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(start..start.saturating_add(8))
            .context("truncated ABI u64")?
            .try_into()
            .expect("checked u64 parameter length"),
    ))
}

fn prepared_accounts_checksum(accounts: &[PreparedAccount]) -> u64 {
    accounts.iter().fold(0_u64, |checksum, account| {
        checksum
            .rotate_left(5)
            .wrapping_add(fingerprint_bytes(&account.pubkey))
            .wrapping_add(account.lamports)
            .wrapping_add(account.data_len as u64)
            .wrapping_add(account.data.as_ref().map_or(0, |data| data.len()) as u64)
            .wrapping_add(fingerprint_bytes(&account.owner))
            .wrapping_add(u64::from(account.is_writable))
            .wrapping_add(u64::from(account.executable))
            .wrapping_add(account.rent_epoch)
    })
}

fn compilation_fingerprint(program: &CompiledProgram) -> u64 {
    // Native machine-code size is useful telemetry but not semantic identity:
    // an in-process JIT may vary layout between equally correct compiles.
    black_box(program.manifest.native_machine_code_len);
    fingerprint_bytes(&program.manifest.artifact_key) ^ backend_code(&program.manifest.backend)
}

fn backend_code(backend: &CompilationBackend) -> u64 {
    match backend {
        CompilationBackend::NativeJitX86_64 => 1,
        CompilationBackend::NativeCraneliftAarch64Subset => 2,
        CompilationBackend::InterpreterOnly { reason } => 3 ^ reason.len() as u64,
    }
}

fn engine_code(engine: ExecutionEngine) -> u64 {
    match engine {
        ExecutionEngine::Interpreter => 1,
        ExecutionEngine::NativeJitX86_64 => 2,
        ExecutionEngine::NativeCraneliftAarch64Subset => 3,
    }
}

fn execution_sample_checksum(outcome: &ExecutionOutcome) -> u64 {
    outcome.return_value
        ^ outcome.watchdog_instructions.rotate_left(11)
        ^ buffer_checksum(&outcome.input_after)
}

fn buffer_checksum(buffer: &[u8]) -> u64 {
    let mut checksum = buffer.len() as u64;
    if let Some(first) = buffer.first() {
        checksum ^= u64::from(*first) << 8;
    }
    if let Some(middle) = buffer.get(buffer.len() / 2) {
        checksum ^= u64::from(*middle) << 24;
    }
    if let Some(last) = buffer.last() {
        checksum ^= u64::from(*last) << 40;
    }
    black_box(checksum)
}

fn account_sample_checksum(accounts: &AccountMap) -> u64 {
    accounts.iter().fold(0_u64, |checksum, (pubkey, account)| {
        checksum
            .rotate_left(3)
            .wrapping_add(fingerprint_bytes(pubkey))
            .wrapping_add(account.lamports)
            .wrapping_add(buffer_checksum(&account.data))
    })
}

fn execution_semantic_fingerprint(outcome: &ExecutionOutcome) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"blockzilla-bpf-bench-execution-v1\0");
    digest.update(outcome.return_value.to_le_bytes());
    digest.update(outcome.watchdog_instructions.to_le_bytes());
    digest.update((outcome.input_after.len() as u64).to_le_bytes());
    digest.update(&outcome.input_after);
    digest.finalize().into()
}

fn account_state_fingerprint(accounts: &AccountMap) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"blockzilla-bpf-bench-account-state-v1\0");
    let mut pubkeys = accounts.keys().copied().collect::<Vec<_>>();
    pubkeys.sort_unstable();
    for pubkey in pubkeys {
        let account = &accounts[&pubkey];
        digest.update(pubkey);
        digest.update(account.lamports.to_le_bytes());
        digest.update(account.owner);
        digest.update([u8::from(account.executable)]);
        digest.update(account.rent_epoch.to_le_bytes());
        digest.update((account.data.len() as u64).to_le_bytes());
        digest.update(&account.data);
    }
    digest.finalize().into()
}

fn ensure_semantic_equivalence(
    interpreted: &ExecutionOutcome,
    other: &ExecutionOutcome,
    label: &str,
) -> Result<()> {
    ensure!(
        interpreted.return_value == other.return_value,
        "{label} return value differs from interpreter"
    );
    ensure!(
        interpreted.watchdog_instructions == other.watchdog_instructions,
        "{label} watchdog count differs from interpreter"
    );
    ensure!(
        interpreted.input_after == other.input_after,
        "{label} output bytes differ from interpreter"
    );
    Ok(())
}

fn print_engine_probe(request: &str, outcome: &ExecutionOutcome) {
    println!(
        "engine_probe request={} selected={:?} return_value={} watchdog_instructions={} output_bytes={} semantic_sha256={}",
        request,
        outcome.engine,
        outcome.return_value,
        outcome.watchdog_instructions,
        outcome.input_after.len(),
        hex_digest(execution_semantic_fingerprint(outcome)),
    );
}

fn fingerprint_bytes(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(value)
}

fn hex_digest(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("write to String cannot fail");
    }
    output
}

fn optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

fn median_duration(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_mirror_round_trips_noop_buffer() {
        let fixture = full_abi_fixture(5, 257);
        let buffer = serialize_parameters_mirror(
            FULL_ABI_PROGRAM_ID,
            &fixture.metas,
            &fixture.accounts,
            &fixture.instruction_data,
        )
        .unwrap();
        let mut accounts = fixture.accounts.clone();
        assert!(account_data_allocations_are_shared(
            &accounts,
            &fixture.accounts
        ));
        deserialize_parameters_mirror(&fixture.metas, &mut accounts, &buffer).unwrap();
        assert_eq!(accounts, fixture.accounts);
        assert!(account_data_allocations_are_shared(
            &accounts,
            &fixture.accounts
        ));
    }

    #[test]
    fn pre_account_mirror_clones_only_external_data() {
        let fixture = full_abi_fixture(6, 31);
        let prepared =
            prepare_pre_accounts_mirror(FULL_ABI_PROGRAM_ID, &fixture.metas, &fixture.accounts)
                .unwrap();
        assert_eq!(prepared.len(), 6);
        assert_eq!(
            prepared
                .iter()
                .filter(|account| account.data.is_some())
                .count(),
            3
        );
        for account in prepared {
            let source = fixture.accounts.get(&account.pubkey).unwrap();
            if let Some(data) = account.data {
                assert!(data.shares_allocation_with(&source.data));
            }
        }
    }

    #[test]
    fn account_state_fingerprint_ignores_hashmap_insertion_order() {
        let fixture = full_abi_fixture(8, 31);
        let mut entries = fixture.accounts.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(pubkey, _)| **pubkey);

        let mut ascending = AccountMap::with_capacity(entries.len());
        for (pubkey, account) in &entries {
            ascending.insert(**pubkey, (**account).clone());
        }
        let mut descending = AccountMap::with_capacity(entries.len());
        for (pubkey, account) in entries.iter().rev() {
            descending.insert(**pubkey, (**account).clone());
        }

        assert_eq!(
            account_state_fingerprint(&ascending),
            account_state_fingerprint(&descending)
        );
    }

    #[test]
    fn patched_mutation_fixture_interpreter_and_native_are_equivalent_when_available() {
        let mut elf = STANDARD
            .decode(include_str!("../../fixtures/relative_call_sbpfv0.so.b64").trim())
            .unwrap();
        patch_fixture_to_mutate_first_account_data(&mut elf).unwrap();
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();
        #[cfg(target_arch = "aarch64")]
        assert_eq!(
            program.manifest.backend,
            CompilationBackend::NativeCraneliftAarch64Subset
        );
        let fixture = full_abi_fixture(4, 32);
        let input = serialize_parameters_mirror(
            FULL_ABI_PROGRAM_ID,
            &fixture.metas,
            &fixture.accounts,
            &fixture.instruction_data,
        )
        .unwrap();
        let interpreted = compiler
            .execute_with_request(&program, input.clone(), ExecutionRequest::Interpreter)
            .unwrap();
        assert_eq!(interpreted.return_value, 0);
        assert_eq!(
            interpreted.input_after[FIRST_ACCOUNT_DATA_OFFSET as usize],
            1
        );
        assert_eq!(
            &interpreted.input_after
                [FIRST_ACCOUNT_DATA_OFFSET as usize + 1..FIRST_ACCOUNT_DATA_OFFSET as usize + 8],
            &[0; 7]
        );
        let auto = compiler
            .execute_with_request(&program, input.clone(), ExecutionRequest::Auto)
            .unwrap();
        ensure_semantic_equivalence(&interpreted, &auto, "auto").unwrap();
        if let Ok(native) =
            compiler.execute_with_request(&program, input, ExecutionRequest::NativeRequired)
        {
            ensure_semantic_equivalence(&interpreted, &native, "native").unwrap();
        }
    }

    #[test]
    fn production_mutation_detaches_only_first_account_on_every_reset() {
        let mut elf = STANDARD
            .decode(include_str!("../../fixtures/relative_call_sbpfv0.so.b64").trim())
            .unwrap();
        patch_fixture_to_mutate_first_account_data(&mut elf).unwrap();
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();
        let fixture = full_abi_fixture(4, 32);
        let mut accounts = fixture.accounts.clone();

        for _ in 0..2 {
            accounts.clone_from(&fixture.accounts);
            assert!(account_data_allocations_are_shared(
                &accounts,
                &fixture.accounts
            ));
            apply_launch_bpf_program_instruction(
                FULL_ABI_PROGRAM_ID,
                &fixture.instruction_data,
                &fixture.metas,
                &mut accounts,
                &compiler,
                &program,
                LaunchBpfLoaderRent {
                    lamports_per_byte_year: 0,
                    exemption_threshold: 0.0,
                },
            )
            .unwrap();
            assert!(embedded_fixture_mutated_exactly_first_account(
                &fixture.accounts,
                &accounts
            ));

            let first_pubkey = fixture.metas[0].pubkey;
            assert!(
                !accounts[&first_pubkey]
                    .data
                    .shares_allocation_with(&fixture.accounts[&first_pubkey].data)
            );
            for pubkey in fixture
                .accounts
                .keys()
                .filter(|pubkey| **pubkey != first_pubkey)
            {
                assert!(
                    accounts[pubkey]
                        .data
                        .shares_allocation_with(&fixture.accounts[pubkey].data)
                );
            }
        }
    }
}

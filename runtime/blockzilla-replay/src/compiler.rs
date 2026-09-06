use crate::{
    AccountMap, AccountSnapshot, BPF_LOADER_PROGRAM_ID, CowAccountMap, LaunchAccountMeta,
    LaunchBpfExecutionError, LaunchBpfLoaderRent,
    launch_bpf_execute::{
        LaunchPreAccounts, apply_launch_bpf_program_instruction_with_stack,
        verify_launch_bpf_instruction,
    },
    program::{ExtractedProgram, LoaderAccountKind, ProgramExtractError, extract_program},
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use hashbrown::HashMap;
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use solana_sbpf::{
    aligned_memory::AlignedMemory,
    ebpf,
    elf::Executable,
    error::EbpfError,
    memory_region::{AccessType, HostMemoryObject, MemoryMapping, MemoryRegion},
    program::{BuiltinProgram, SBPFVersion},
    verifier::RequisiteVerifier,
    vm::{CallFrame, Config, ContextObject, EbpfVm, ExecutionMode},
};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    cell::RefCell,
    fmt,
    ptr::NonNull,
    slice, str,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use thiserror::Error;

const COMPILER_ID: &str = "solana-sbpf-0.21.0";
const PROFILE_ID: &str = "blockzilla-poc-sbpfv0-launch-pda-cpi-syscalls-no-cu-static-watchdog-native-dispatch-immutable-cpi-metadata-aligned-map-no-region-zero-v7";
const NATIVE_ENTRY_ABI_ID: &str = "blockzilla-native-entry-v1-checked-memory-helpers";
const WATCHDOG_INSTRUCTION_LIMIT: u64 = 100_000;
const LEGACY_BPF_HEAP_SIZE: usize = 32 * 1024;
const MAX_LEGACY_INSTRUCTION_ACCOUNTS: usize = 256;
const MAX_LEGACY_INSTRUCTION_STACK_DEPTH: usize = 5;

/// Compilation result available on this host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompilationBackend {
    /// `solana-sbpf` emitted executable x86-64 machine code in memory.
    NativeJitX86_64,
    /// Cranelift emitted AArch64 machine code for Blockzilla's deliberately
    /// small, syscall-free and acyclic SBPFv0 subset.
    NativeCraneliftAarch64Subset,
    /// The ELF is loaded and verified but the host or program is outside the
    /// available native backend; the interpreter remains available.
    InterpreterOnly { reason: String },
}

/// Stable metadata for an in-memory POC compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompilationManifest {
    pub elf_sha256: [u8; 32],
    pub artifact_key: [u8; 32],
    pub elf_len: usize,
    pub text_len: usize,
    pub text_virtual_address: u64,
    pub entrypoint_instruction: usize,
    pub sbpf_version: String,
    pub compiler_id: &'static str,
    pub profile_id: &'static str,
    pub target: String,
    pub verifier: &'static str,
    pub native_backend_id: &'static str,
    pub native_entry_abi_id: &'static str,
    pub native_isa_fingerprint: Option<String>,
    /// Protocol CU accounting/reporting is not part of this profile. The
    /// upstream instruction meter is used only as a host-safety watchdog.
    pub protocol_compute_accounting_enabled: bool,
    pub watchdog_instruction_limit: u64,
    pub backend: CompilationBackend,
    pub native_machine_code_len: Option<usize>,
    /// Number of dynamically expanded guest instructions lowered into the
    /// strict-subset artifact. `None` for the upstream x86 JIT/interpreter.
    pub native_lowered_instruction_count: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEngine {
    Interpreter,
    NativeJitX86_64,
    NativeCraneliftAarch64Subset,
}

/// Selects execution explicitly so tests and callers cannot mistake a silent
/// interpreter fallback for successful native compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRequest {
    /// Use a native artifact when one is available, otherwise interpret.
    Auto,
    /// Always use the verified SBPF interpreter.
    Interpreter,
    /// Require native execution and return an error if no artifact exists.
    NativeRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub engine: ExecutionEngine,
    pub return_value: u64,
    /// Guest instructions observed by the non-consensus safety watchdog. This
    /// is not Solana CU output and is not applied to replay fees or metadata.
    pub watchdog_instructions: u64,
    pub input_after: Vec<u8>,
    /// Final verifier baseline after successful nested invocations. This is
    /// crate-private because it is an implementation detail of legacy Bank
    /// execution, not part of the public minor-program execution API.
    pub(crate) verifier_baselines: LaunchPreAccounts,
}

#[derive(Debug, Error)]
pub enum CompilerError {
    #[error(transparent)]
    Extract(#[from] ProgramExtractError),
    #[error("load SBPF ELF: {0}")]
    Load(String),
    #[error("verify SBPF bytecode: {0}")]
    Verify(String),
    #[error("compile SBPF to native machine code: {0}")]
    NativeCompile(String),
    #[error("native execution required but unavailable: {reason}")]
    NativeUnavailable { reason: String },
    #[error("create SBPF memory mapping: {0}")]
    MemoryMap(String),
    #[error("execute SBPF program: {0}")]
    Execute(String),
    #[error("SBPF execution exceeded the {limit}-instruction safety watchdog")]
    WatchdogExceeded { limit: u64 },
}

/// Loaded bytecode plus an optional in-memory native artifact.
pub struct CompiledProgram {
    executable: Executable<ReplayContext>,
    #[cfg(target_arch = "aarch64")]
    native_aarch64: Option<crate::native_aarch64::NativeProgram>,
    pub manifest: CompilationManifest,
}

impl std::fmt::Debug for CompiledProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledProgram")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

/// Launch-era compiler profile used by deployment and replay execution.
///
/// It accepts only SBPFv0 and runs the requisite bytecode verifier even though
/// block/signature verification is outside replay. No protocol CU accounting
/// is produced, but the upstream instruction counter remains enabled with a
/// fixed non-consensus watchdog bound so a loop cannot hang the process. The
/// five syscalls exposed by Solana v1.1.14's legacy loader are registered with
/// its 32-KiB bump heap. Logging is validated and discarded because it is not
/// Bank state.
pub struct ReplayCompiler {
    config: Config,
    runtime_environment: Arc<BuiltinProgram<ReplayContext>>,
    shared: Arc<ReplayCompilerShared>,
}

struct ReplayCompilerShared {
    nested_program_cache: Mutex<HashMap<[u8; 32], Arc<NestedProgramSlot>>>,
    cross_program_supported: AtomicBool,
    #[cfg(test)]
    nested_compile_count: AtomicUsize,
}

#[derive(Default)]
struct NestedProgramSlot {
    /// Published programs are immutable, so successful cache hits need only
    /// the `OnceLock` load after resolving the program-id slot.
    compiled: OnceLock<Arc<CompiledProgram>>,
    /// Only a cold miss for this program id takes this lock. Different
    /// programs can still compile concurrently, while same-key callers wait
    /// for one compiler instead of compiling an artifact that will be thrown
    /// away by the cache race.
    compile_lock: Mutex<()>,
}

struct ReplayExecutionScratch {
    stack: AlignedMemory<{ ebpf::HOST_ALIGN }>,
    heap: AlignedMemory<{ ebpf::HOST_ALIGN }>,
    call_frames: Vec<CallFrame>,
    /// Bytes of stack written by the previous invocation that still need zeroing.
    stack_dirty_len: usize,
    /// Bytes of heap that still need zeroing. The complete mapped heap is guest
    /// writable, so every completed invocation marks the full allocation dirty;
    /// a fresh zero-filled lease retains the zero-length fast path.
    heap_dirty_len: usize,
    /// When true, call frames carry residual state from the previous invoke.
    call_frames_dirty: bool,
}

std::thread_local! {
    /// Each replay worker keeps its own reusable VM workspaces. A lease owns
    /// the workspace after removing it from this pool, so a nested CPI can
    /// safely acquire another workspace without holding a `RefCell` borrow or
    /// aliasing the outer VM's stack and heap.
    static REPLAY_EXECUTION_SCRATCH: RefCell<Vec<ReplayExecutionScratch>> =
        const { RefCell::new(Vec::new()) };
}

impl ReplayExecutionScratch {
    fn new(config: &Config) -> Self {
        Self {
            stack: AlignedMemory::zero_filled(config.stack_size()),
            heap: AlignedMemory::zero_filled(LEGACY_BPF_HEAP_SIZE),
            call_frames: vec![CallFrame::default(); config.max_call_depth],
            // Fresh zero-filled memory needs no reset on the first acquire.
            stack_dirty_len: 0,
            heap_dirty_len: 0,
            call_frames_dirty: false,
        }
    }

    fn reset(&mut self, config: &Config) {
        if self.stack.len() != config.stack_size()
            || self.heap.len() != LEGACY_BPF_HEAP_SIZE
            || self.call_frames.len() != config.max_call_depth
        {
            *self = Self::new(config);
            return;
        }
        let stack_end = self.stack_dirty_len.min(self.stack.len());
        if stack_end > 0 {
            self.stack.as_slice_mut()[..stack_end].fill(0);
            self.stack_dirty_len = 0;
        }
        let heap_end = self.heap_dirty_len.min(self.heap.len());
        if heap_end > 0 {
            self.heap.as_slice_mut()[..heap_end].fill(0);
            self.heap_dirty_len = 0;
        }
        if self.call_frames_dirty {
            self.call_frames.fill(CallFrame::default());
            self.call_frames_dirty = false;
        }
    }

    /// Record residual state after a VM invoke so the next [`Self::reset`]
    /// only clears bytes that may be non-zero.
    fn mark_used_after_execute(&mut self, _heap_position: u64, stack_fully_used: bool) {
        // `heap_position` tracks only the bump allocator. A guest can also
        // write directly through the mapped heap or through a syscall-provided
        // destination, so no smaller watermark safely bounds residual bytes.
        self.heap_dirty_len = self.heap.len();
        if stack_fully_used {
            self.stack_dirty_len = self.stack.len();
        }
        self.call_frames_dirty = true;
    }
}

struct ReplayExecutionScratchLease {
    scratch: Option<ReplayExecutionScratch>,
}

impl ReplayExecutionScratchLease {
    fn acquire(config: &Config) -> Self {
        let mut scratch = REPLAY_EXECUTION_SCRATCH
            .with(|pool| pool.borrow_mut().pop())
            .unwrap_or_else(|| ReplayExecutionScratch::new(config));
        scratch.reset(config);
        Self {
            scratch: Some(scratch),
        }
    }

    fn get_mut(&mut self) -> &mut ReplayExecutionScratch {
        self.scratch
            .as_mut()
            .expect("execution scratch lease always owns one workspace")
    }
}

impl Drop for ReplayExecutionScratchLease {
    fn drop(&mut self) {
        let Some(scratch) = self.scratch.take() else {
            return;
        };
        REPLAY_EXECUTION_SCRATCH.with(|pool| pool.borrow_mut().push(scratch));
    }
}

impl Clone for ReplayCompiler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            runtime_environment: Arc::clone(&self.runtime_environment),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl fmt::Debug for ReplayCompiler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplayCompiler")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Default for ReplayCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayCompiler {
    pub fn new() -> Self {
        let config = Config {
            max_call_depth: 20,
            enable_instruction_meter: true,
            // State-only replay can checkpoint less often than rbpf 0.1.28:
            // every branch/call/exit still checks the 100k watchdog, and a
            // failing VM never deserializes its parameter buffer into Bank
            // state. This avoids a meter branch on every guest instruction.
            instruction_meter_checkpoint_distance: 1_000,
            reject_broken_elfs: false,
            noop_instruction_rate: 0,
            // Launch replay exposes one region in each canonical 4-GiB VM
            // slot (program, stack, heap, input). Direct indexing by the high
            // address bits avoids the generic MappingCache probe on every
            // guest load/store while retaining translation, bounds,
            // writability, and stack-gap checks.
            aligned_memory_mapping: true,
            // The former unaligned SBPFv0 mapper rejected address zero even
            // for a zero-length translation. Do not make the aligned mapper's
            // empty sentinel guest-visible.
            allow_memory_region_zero: false,
            enabled_sbpf_versions: SBPFVersion::V0..=SBPFVersion::V0,
            ..Config::default()
        };
        Self::with_config(config)
    }

    fn with_config(config: Config) -> Self {
        let runtime_environment = launch_builtin_environment(config.clone())
            .expect("the static launch-era syscall registry must be valid");
        Self {
            config,
            runtime_environment,
            shared: Arc::new(ReplayCompilerShared {
                nested_program_cache: Mutex::new(HashMap::new()),
                cross_program_supported: AtomicBool::new(true),
                #[cfg(test)]
                nested_compile_count: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn set_cross_program_supported(&self, supported: bool) {
        self.shared
            .cross_program_supported
            .store(supported, Ordering::Release);
    }

    fn compile_nested_program(
        &self,
        program_id: [u8; 32],
        account_data: &[u8],
    ) -> Result<Arc<CompiledProgram>, CompilerError> {
        let slot = {
            let mut cache = self
                .shared
                .nested_program_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(
                cache
                    .entry(program_id)
                    .or_insert_with(|| Arc::new(NestedProgramSlot::default())),
            )
        };
        if let Some(program) = slot.compiled.get() {
            return Ok(Arc::clone(program));
        }

        let _compile_guard = slot
            .compile_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(program) = slot.compiled.get() {
            return Ok(Arc::clone(program));
        }

        #[cfg(test)]
        self.shared
            .nested_compile_count
            .fetch_add(1, Ordering::Relaxed);
        let compiled = Arc::new(self.compile_account(LoaderAccountKind::Legacy, account_data)?);
        if slot.compiled.set(Arc::clone(&compiled)).is_err() {
            // The per-key compile lock makes this unreachable, but prefer the
            // already-published canonical entry if that invariant changes.
            return Ok(Arc::clone(
                slot.compiled
                    .get()
                    .expect("a failed OnceLock set leaves the prior value"),
            ));
        }
        Ok(compiled)
    }

    pub fn compile_account(
        &self,
        loader: LoaderAccountKind,
        account_data: &[u8],
    ) -> Result<CompiledProgram, CompilerError> {
        let extracted = extract_program(loader, account_data)?;
        self.compile_extracted(&extracted)
    }

    pub fn compile_extracted(
        &self,
        program: &ExtractedProgram,
    ) -> Result<CompiledProgram, CompilerError> {
        let executable = Executable::<ReplayContext>::from_elf(
            &program.elf,
            Arc::clone(&self.runtime_environment),
        )
        .map_err(|error| CompilerError::Load(error.to_string()))?;
        executable
            .verify::<RequisiteVerifier>()
            .map_err(|error| CompilerError::Verify(error.to_string()))?;

        let native = compile_native_if_supported(&executable)?;
        let (text_virtual_address, text) = executable.get_text_bytes();
        let sbpf_version = format!("{:?}", executable.get_sbpf_version());
        let target = target_lexicon::Triple::host().to_string();
        // `ExtractedProgram` is a public tooling type and its metadata can be
        // constructed or changed by a caller. Artifact identity must derive
        // from the bytes that were actually loaded and verified.
        let elf_sha256: [u8; 32] = Sha256::digest(&program.elf).into();
        let artifact_key = artifact_key(
            elf_sha256,
            &sbpf_version,
            &target,
            &native.artifact_identity,
        );
        let manifest = CompilationManifest {
            elf_sha256,
            artifact_key,
            elf_len: program.elf.len(),
            text_len: text.len(),
            text_virtual_address,
            entrypoint_instruction: executable.get_entrypoint_instruction_offset(),
            sbpf_version,
            compiler_id: COMPILER_ID,
            profile_id: PROFILE_ID,
            target,
            verifier: "solana_sbpf::verifier::RequisiteVerifier",
            native_backend_id: native.backend_id,
            native_entry_abi_id: NATIVE_ENTRY_ABI_ID,
            native_isa_fingerprint: native.isa_fingerprint,
            protocol_compute_accounting_enabled: false,
            watchdog_instruction_limit: WATCHDOG_INSTRUCTION_LIMIT,
            backend: native.backend,
            native_machine_code_len: native.machine_code_len,
            native_lowered_instruction_count: native.lowered_instruction_count,
        };
        Ok(CompiledProgram {
            executable,
            #[cfg(target_arch = "aarch64")]
            native_aarch64: native.aarch64,
            manifest,
        })
    }

    /// Executes the minor-program ABI: r1 points to `input` and r0 is returned.
    pub fn execute(
        &self,
        program: &CompiledProgram,
        input: Vec<u8>,
    ) -> Result<ExecutionOutcome, CompilerError> {
        self.execute_with_request(program, input, ExecutionRequest::Auto)
    }

    /// Executes with an explicit engine policy. `NativeRequired` never falls
    /// back, which makes native-backend acceptance tests unambiguous.
    pub fn execute_with_request(
        &self,
        program: &CompiledProgram,
        input: Vec<u8>,
        request: ExecutionRequest,
    ) -> Result<ExecutionOutcome, CompilerError> {
        self.execute_with_environment(
            program,
            input,
            request,
            SmallVec::from_slice(&[[0; 32]]),
            LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            LaunchPreAccounts::new(),
        )
    }

    pub(crate) fn execute_replay_program_with_stack(
        &self,
        program: &CompiledProgram,
        input: Vec<u8>,
        bank_rent: LaunchBpfLoaderRent,
        program_stack: SmallVec<[[u8; 32]; 5]>,
        verifier_baselines: LaunchPreAccounts,
    ) -> Result<ExecutionOutcome, CompilerError> {
        self.execute_with_environment(
            program,
            input,
            ExecutionRequest::Auto,
            program_stack,
            bank_rent,
            verifier_baselines,
        )
    }

    fn execute_with_environment(
        &self,
        program: &CompiledProgram,
        input: Vec<u8>,
        request: ExecutionRequest,
        program_stack: SmallVec<[[u8; 32]; 5]>,
        bank_rent: LaunchBpfLoaderRent,
        verifier_baselines: LaunchPreAccounts,
    ) -> Result<ExecutionOutcome, CompilerError> {
        match request {
            ExecutionRequest::Interpreter => self.execute_upstream(
                program,
                input,
                ExecutionMode::Interpreted,
                program_stack,
                bank_rent,
                verifier_baselines,
            ),
            ExecutionRequest::Auto => {
                #[cfg(target_arch = "aarch64")]
                if program.native_aarch64.is_some() {
                    return self.execute_aarch64_native(
                        program,
                        input,
                        program_stack,
                        bank_rent,
                        verifier_baselines,
                    );
                }

                #[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
                if matches!(
                    program.manifest.backend,
                    CompilationBackend::NativeJitX86_64
                ) {
                    return self.execute_upstream(
                        program,
                        input,
                        ExecutionMode::Jit,
                        program_stack,
                        bank_rent,
                        verifier_baselines,
                    );
                }

                self.execute_upstream(
                    program,
                    input,
                    ExecutionMode::Interpreted,
                    program_stack,
                    bank_rent,
                    verifier_baselines,
                )
            }
            ExecutionRequest::NativeRequired => {
                #[cfg(target_arch = "aarch64")]
                if program.native_aarch64.is_some() {
                    return self.execute_aarch64_native(
                        program,
                        input,
                        program_stack,
                        bank_rent,
                        verifier_baselines,
                    );
                }

                #[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
                if matches!(
                    program.manifest.backend,
                    CompilationBackend::NativeJitX86_64
                ) {
                    return self.execute_upstream(
                        program,
                        input,
                        ExecutionMode::Jit,
                        program_stack,
                        bank_rent,
                        verifier_baselines,
                    );
                }

                Err(CompilerError::NativeUnavailable {
                    reason: interpreter_only_reason(&program.manifest.backend),
                })
            }
        }
    }

    fn execute_upstream(
        &self,
        program: &CompiledProgram,
        mut input: Vec<u8>,
        mut mode: ExecutionMode,
        program_stack: SmallVec<[[u8; 32]; 5]>,
        bank_rent: LaunchBpfLoaderRent,
        verifier_baselines: LaunchPreAccounts,
    ) -> Result<ExecutionOutcome, CompilerError> {
        #[cfg(test)]
        if matches!(mode, ExecutionMode::Interpreted) {
            INTERPRETER_ENTRY_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        }

        let executable = &program.executable;
        let config = executable.get_config();
        let sbpf_version = executable.get_sbpf_version();
        let mut scratch = ReplayExecutionScratchLease::acquire(config);
        let ReplayExecutionScratch {
            stack,
            heap,
            call_frames,
            ..
        } = scratch.get_mut();
        let stack_len = stack.len();
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // AlignedMemoryMapping inserts an empty region-zero sentinel. Reserve
        // its slot so constructing a VM does not grow this vector.
        let mut regions = Vec::with_capacity(5);
        regions.extend([
            executable.get_ro_region(),
            stack_memory_region(stack, config, sbpf_version),
            MemoryRegion::new(heap, ebpf::MM_HEAP_START),
            MemoryRegion::new(input_ptr, ebpf::MM_INPUT_START),
        ]);
        validate_canonical_execution_regions(&regions)?;
        // SAFETY: stack, heap, and input remain allocated and are not moved
        // until both the mapping and VM have been dropped. Their byte contents
        // may be mutated by the guest without violating Rust invariants.
        let memory_mapping = unsafe { MemoryMapping::new(regions, config, sbpf_version) }
            .map_err(|error| CompilerError::MemoryMap(error.to_string()))?;
        let mut context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler: self,
            input_len: input.len(),
            bank_rent,
            program_stack,
            cross_program_supported: self.shared.cross_program_supported.load(Ordering::Acquire),
            verifier_baselines,
        };
        let mut vm = EbpfVm::new(
            Arc::clone(executable.get_loader()),
            sbpf_version,
            &mut context,
            stack_len,
        );
        vm.registers[1] = ebpf::MM_INPUT_START;
        let (watchdog_instructions, result) =
            vm.execute_program(executable, &mut mode, call_frames);
        let result: Result<u64, EbpfError> = result.into();
        let return_value = match result {
            Ok(value) => value,
            Err(EbpfError::ExceededMaxInstructions) => {
                scratch
                    .get_mut()
                    .mark_used_after_execute(context.heap_position, true);
                return Err(CompilerError::WatchdogExceeded {
                    limit: WATCHDOG_INSTRUCTION_LIMIT,
                });
            }
            Err(error) => {
                let pc = vm.registers[11];
                scratch
                    .get_mut()
                    .mark_used_after_execute(context.heap_position, true);
                return Err(CompilerError::Execute(format!("{error} at guest pc {pc}")));
            }
        };
        scratch
            .get_mut()
            .mark_used_after_execute(context.heap_position, true);
        let engine = match mode {
            ExecutionMode::Jit => ExecutionEngine::NativeJitX86_64,
            ExecutionMode::Interpreted | ExecutionMode::PreferJit => ExecutionEngine::Interpreter,
        };
        let verifier_baselines = std::mem::take(&mut context.verifier_baselines);
        Ok(ExecutionOutcome {
            engine,
            return_value,
            watchdog_instructions,
            input_after: input,
            verifier_baselines,
        })
    }

    #[cfg(target_arch = "aarch64")]
    fn execute_aarch64_native(
        &self,
        program: &CompiledProgram,
        mut input: Vec<u8>,
        _program_stack: SmallVec<[[u8; 32]; 5]>,
        _bank_rent: LaunchBpfLoaderRent,
        verifier_baselines: LaunchPreAccounts,
    ) -> Result<ExecutionOutcome, CompilerError> {
        let native =
            program
                .native_aarch64
                .as_ref()
                .ok_or_else(|| CompilerError::NativeUnavailable {
                    reason: interpreter_only_reason(&program.manifest.backend),
                })?;
        let executable = &program.executable;
        let config = executable.get_config();
        let sbpf_version = executable.get_sbpf_version();
        let mut scratch = ReplayExecutionScratchLease::acquire(config);
        let ReplayExecutionScratch { stack, heap, .. } = scratch.get_mut();
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // Keep the same spare sentinel slot as the upstream VM path.
        let mut regions = Vec::with_capacity(5);
        regions.extend([
            executable.get_ro_region(),
            stack_memory_region(stack, config, sbpf_version),
            MemoryRegion::new(heap, ebpf::MM_HEAP_START),
            MemoryRegion::new(input_ptr, ebpf::MM_INPUT_START),
        ]);
        validate_canonical_execution_regions(&regions)?;
        // SAFETY: the backing allocations outlive both the mapping and native
        // invocation. Native code can access guest memory only through helpers
        // which call this mapping's checked load/store methods.
        let mut memory_mapping = unsafe { MemoryMapping::new(regions, config, sbpf_version) }
            .map_err(|error| CompilerError::MemoryMap(error.to_string()))?;
        let frame_pointer = ebpf::MM_STACK_START.saturating_add(config.stack_frame_size as u64);
        let outcome = native
            .execute(&mut memory_mapping, ebpf::MM_INPUT_START, frame_pointer)
            .map_err(CompilerError::Execute);
        // Native AArch64 does not expose the bump-allocator watermark, so treat
        // the full stack and heap as dirty after every invoke.
        scratch
            .get_mut()
            .mark_used_after_execute(LEGACY_BPF_HEAP_SIZE as u64, true);
        let outcome = outcome?;
        Ok(ExecutionOutcome {
            engine: ExecutionEngine::NativeCraneliftAarch64Subset,
            return_value: outcome.return_value,
            watchdog_instructions: outcome.watchdog_instructions,
            input_after: input,
            verifier_baselines,
        })
    }
}

fn stack_memory_region<Stack: HostMemoryObject>(
    stack: Stack,
    config: &Config,
    sbpf_version: SBPFVersion,
) -> MemoryRegion {
    MemoryRegion::new_gapped(
        stack,
        ebpf::MM_STACK_START,
        if sbpf_version.stack_frame_gaps() && config.enable_stack_frame_gaps {
            config.stack_frame_size as u64
        } else {
            0
        },
    )
}

fn validate_canonical_execution_regions(regions: &[MemoryRegion]) -> Result<(), CompilerError> {
    let expected_starts = [
        ebpf::MM_BYTECODE_START,
        ebpf::MM_STACK_START,
        ebpf::MM_HEAP_START,
        ebpf::MM_INPUT_START,
    ];
    if regions.len() != expected_starts.len() {
        return Err(CompilerError::MemoryMap(format!(
            "aligned replay mapping requires {} canonical regions, got {}",
            expected_starts.len(),
            regions.len()
        )));
    }
    for (region, expected_start) in regions.iter().zip(expected_starts) {
        let expected_end = expected_start.saturating_add(ebpf::MM_REGION_SIZE);
        let actual_end = region.vm_addr_range().end;
        if region.vm_addr < expected_start || actual_end > expected_end {
            return Err(CompilerError::MemoryMap(format!(
                "region {:#x}..{actual_end:#x} escapes canonical slot {expected_start:#x}..{expected_end:#x}",
                region.vm_addr
            )));
        }
    }
    Ok(())
}

fn artifact_key(
    elf_sha256: [u8; 32],
    sbpf_version: &str,
    target: &str,
    native_backend_id: &str,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"blockzilla-replay-native-artifact-v2\0");
    digest.update(elf_sha256);
    digest.update(COMPILER_ID.as_bytes());
    digest.update([0]);
    digest.update(PROFILE_ID.as_bytes());
    digest.update([0]);
    digest.update(sbpf_version.as_bytes());
    digest.update([0]);
    digest.update(target.as_bytes());
    digest.update([0]);
    digest.update(native_backend_id.as_bytes());
    digest.update([0]);
    digest.update(NATIVE_ENTRY_ABI_ID.as_bytes());
    digest.update([0]);
    digest.update(WATCHDOG_INSTRUCTION_LIMIT.to_le_bytes());
    digest.finalize().into()
}

fn interpreter_only_reason(backend: &CompilationBackend) -> String {
    match backend {
        CompilationBackend::InterpreterOnly { reason } => reason.clone(),
        other => format!("compiled backend {other:?} has no native artifact on this process"),
    }
}

struct NativeCompilation {
    backend: CompilationBackend,
    backend_id: &'static str,
    artifact_identity: String,
    isa_fingerprint: Option<String>,
    machine_code_len: Option<usize>,
    lowered_instruction_count: Option<u64>,
    #[cfg(target_arch = "aarch64")]
    aarch64: Option<crate::native_aarch64::NativeProgram>,
}

#[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
fn compile_native_if_supported(
    executable: &Executable<ReplayContext>,
) -> Result<NativeCompilation, CompilerError> {
    if let Err(error) = executable.jit_compile() {
        return Ok(NativeCompilation {
            backend: CompilationBackend::InterpreterOnly {
                reason: format!("x86-64 native JIT unavailable: {error}"),
            },
            backend_id: "interpreter-only",
            artifact_identity: "interpreter-only".to_owned(),
            isa_fingerprint: None,
            machine_code_len: None,
            lowered_instruction_count: None,
        });
    }
    let machine_code_len = executable
        .get_compiled_program()
        .map(|compiled| compiled.machine_code_length());
    Ok(NativeCompilation {
        backend: CompilationBackend::NativeJitX86_64,
        backend_id: "solana-sbpf-0.21.0-x86_64-jit",
        artifact_identity: "solana-sbpf-0.21.0-x86_64-jit".to_owned(),
        isa_fingerprint: None,
        machine_code_len,
        lowered_instruction_count: None,
    })
}

#[cfg(target_arch = "aarch64")]
fn compile_native_if_supported(
    executable: &Executable<ReplayContext>,
) -> Result<NativeCompilation, CompilerError> {
    use crate::native_aarch64::NativeProgram;

    let (_, text) = executable.get_text_bytes();
    let functions = executable
        .get_function_registry()
        .iter()
        .map(|(key, (_, pc))| (key, pc))
        .collect();
    let config = executable.get_config();
    match NativeProgram::compile(
        text,
        executable.get_entrypoint_instruction_offset(),
        &functions,
        config.stack_frame_size,
        executable.get_sbpf_version().stack_frame_gaps() && config.enable_stack_frame_gaps,
        config.max_call_depth,
        WATCHDOG_INSTRUCTION_LIMIT,
    ) {
        Ok(program) => {
            let machine_code_len = program.machine_code_len();
            let lowered_instruction_count = program.instruction_count() as u64;
            let isa_fingerprint = program.manifest.isa_fingerprint.clone();
            let artifact_identity = format!(
                "cranelift-{}:{}:{}:{}:{}",
                program.manifest.cranelift_version,
                program.manifest.subset_profile_id,
                program.manifest.import_abi_id,
                program.manifest.watchdog_strategy,
                isa_fingerprint
            );
            Ok(NativeCompilation {
                backend: CompilationBackend::NativeCraneliftAarch64Subset,
                backend_id: "cranelift-0.134.2-aarch64-sbpfv0-subset-v1",
                artifact_identity,
                isa_fingerprint: Some(isa_fingerprint),
                machine_code_len: Some(machine_code_len),
                lowered_instruction_count: Some(lowered_instruction_count),
                aarch64: Some(program),
            })
        }
        Err(error) if error.is_unsupported() => Ok(NativeCompilation {
            backend: CompilationBackend::InterpreterOnly {
                reason: error.to_string(),
            },
            backend_id: "interpreter-only",
            artifact_identity: "interpreter-only".to_owned(),
            isa_fingerprint: None,
            machine_code_len: None,
            lowered_instruction_count: None,
            aarch64: None,
        }),
        Err(error) => Err(CompilerError::NativeCompile(error.to_string())),
    }
}

#[cfg(not(any(
    target_arch = "aarch64",
    all(not(target_os = "windows"), target_arch = "x86_64")
)))]
fn compile_native_if_supported(
    _executable: &Executable<ReplayContext>,
) -> Result<NativeCompilation, CompilerError> {
    Ok(NativeCompilation {
        backend: CompilationBackend::InterpreterOnly {
            reason: "no native Blockzilla replay backend exists for this target".to_owned(),
        },
        backend_id: "interpreter-only",
        artifact_identity: "interpreter-only".to_owned(),
        isa_fingerprint: None,
        machine_code_len: None,
        lowered_instruction_count: None,
    })
}

#[derive(Debug, Error)]
enum ReplaySyscallError {
    #[error("BPF program called abort()")]
    Abort,
    #[error("BPF program panicked at {file}, {line}:{column}")]
    Panic {
        file: String,
        line: u64,
        column: u64,
    },
    #[error("invalid UTF-8 passed to a launch-era log syscall")]
    InvalidString,
    #[error("launch-era syscall memory access failed: {0}")]
    MemoryAccess(String),
    #[error("launch-era syscall argument length overflow")]
    LengthOverflow,
    #[error("could not create program address with signer seeds: {0}")]
    BadSeeds(String),
    #[error("cross-program invocation is not active in this historical Bank")]
    CrossProgramUnavailable,
    #[error("malformed legacy Rust CPI arguments: {0}")]
    MalformedCpi(String),
    #[error("CPI account {0:?} is missing from the caller instruction")]
    MissingCpiAccount([u8; 32]),
    #[error("CPI account {0:?} is missing from the passed AccountInfo slice")]
    MissingCpiAccountInfo([u8; 32]),
    #[error("CPI privilege escalation for account {0:?}")]
    CpiPrivilegeEscalation([u8; 32]),
    #[error("CPI target program account {0:?} is not executable")]
    CpiProgramNotExecutable([u8; 32]),
    #[error("CPI target program account {program_id:?} is owned by {owner:?}, expected BPFLoader")]
    CpiProgramWrongOwner {
        program_id: [u8; 32],
        owner: [u8; 32],
    },
    #[error("CPI instruction stack exceeded launch-era depth {MAX_LEGACY_INSTRUCTION_STACK_DEPTH}")]
    CpiCallDepth,
    #[error("CPI reentrancy into non-caller program {0:?}")]
    CpiReentrancy([u8; 32]),
    #[error("caller state failed the pre-CPI verifier: {0}")]
    PreCpiVerifier(String),
    #[error("cross-program execution failed: {0}")]
    CrossProgramExecution(String),
}

#[derive(Debug, Clone)]
struct GuestParameterAccount {
    pubkey: [u8; 32],
    is_signer: bool,
    is_writable: bool,
    key_vm_address: u64,
    lamports_vm_address: u64,
    data_vm_address: u64,
    data_len: usize,
    snapshot: AccountSnapshot,
}

#[derive(Debug)]
struct DecodedRustInstruction {
    program_id: [u8; 32],
    accounts: Vec<LaunchAccountMeta>,
    data: Vec<u8>,
}

#[derive(Debug)]
struct GuestAccountWriteback {
    pubkey: [u8; 32],
    lamports_vm_address: u64,
    data_vm_address: u64,
    data_len: usize,
    post: AccountSnapshot,
}

fn malformed_cpi(message: impl Into<String>) -> ReplaySyscallError {
    ReplaySyscallError::MalformedCpi(message.into())
}

fn checked_guest_range(
    start: usize,
    len: usize,
) -> Result<std::ops::Range<usize>, ReplaySyscallError> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| malformed_cpi("guest range overflow"))?;
    Ok(start..end)
}

fn read_cpi_u64(bytes: &[u8], start: usize) -> Result<u64, ReplaySyscallError> {
    let range = checked_guest_range(start, 8)?;
    Ok(u64::from_le_bytes(
        bytes
            .get(range)
            .ok_or_else(|| malformed_cpi("truncated u64"))?
            .try_into()
            .expect("checked u64 width"),
    ))
}

fn copy_guest_bytes(
    context: &ReplayContext,
    vm_address: u64,
    len: u64,
) -> Result<Vec<u8>, ReplaySyscallError> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let host_address = launch_syscall_map(context, AccessType::Load, vm_address, len)?;
    let len = usize::try_from(len).map_err(|_| ReplaySyscallError::LengthOverflow)?;
    // SAFETY: the memory mapping validated the complete load range. Copying
    // now ensures no host reference survives a recursive VM invocation.
    Ok(unsafe { slice::from_raw_parts(host_address as *const u8, len) }.to_vec())
}

fn input_vm_address(offset: usize) -> Result<u64, ReplaySyscallError> {
    ebpf::MM_INPUT_START
        .checked_add(u64::try_from(offset).map_err(|_| ReplaySyscallError::LengthOverflow)?)
        .ok_or(ReplaySyscallError::LengthOverflow)
}

/// Decode the launch BPFLoader's compact account parameter buffer and retain
/// only owned snapshots plus guest addresses. The addresses are re-mapped
/// after child execution; host references never cross a recursive VM call.
fn scan_guest_parameter_accounts(
    context: &ReplayContext,
) -> Result<Vec<GuestParameterAccount>, ReplaySyscallError> {
    let input_len =
        u64::try_from(context.input_len).map_err(|_| ReplaySyscallError::LengthOverflow)?;
    let input = copy_guest_bytes(context, ebpf::MM_INPUT_START, input_len)?;
    let account_count = usize::try_from(read_cpi_u64(&input, 0)?)
        .map_err(|_| ReplaySyscallError::LengthOverflow)?;
    if account_count > MAX_LEGACY_INSTRUCTION_ACCOUNTS {
        return Err(malformed_cpi("account count exceeds legacy message limit"));
    }

    let mut cursor = 8_usize;
    let mut position_to_unique = Vec::with_capacity(account_count);
    let mut accounts = Vec::with_capacity(context.verifier_baselines.len());
    for position in 0..account_count {
        let duplicate = *input
            .get(cursor)
            .ok_or_else(|| malformed_cpi("truncated account duplicate marker"))?;
        cursor = cursor
            .checked_add(1)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        if duplicate != u8::MAX {
            let duplicate = usize::from(duplicate);
            if duplicate >= position {
                return Err(malformed_cpi("forward or self duplicate account index"));
            }
            let unique = *position_to_unique
                .get(duplicate)
                .ok_or_else(|| malformed_cpi("duplicate account index is absent"))?;
            position_to_unique.push(unique);
            continue;
        }

        let baseline = context
            .verifier_baselines
            .get(accounts.len())
            .ok_or_else(|| malformed_cpi("guest account layout exceeds verifier baseline"))?;
        let is_signer = match input.get(cursor).copied() {
            Some(0) => false,
            Some(1) => true,
            _ => return Err(malformed_cpi("invalid account signer boolean")),
        };
        let is_writable = match input.get(cursor.saturating_add(1)).copied() {
            Some(0) => false,
            Some(1) => true,
            _ => return Err(malformed_cpi("invalid account writable boolean")),
        };
        let key_offset = cursor
            .checked_add(2)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        let key_range = checked_guest_range(key_offset, 32)?;
        let mut pubkey = [0_u8; 32];
        pubkey.copy_from_slice(
            input
                .get(key_range)
                .ok_or_else(|| malformed_cpi("truncated account pubkey"))?,
        );
        cursor = cursor
            .checked_add(34)
            .ok_or(ReplaySyscallError::LengthOverflow)?;

        let lamports_offset = cursor;
        let lamports = read_cpi_u64(&input, cursor)?;
        cursor = cursor
            .checked_add(8)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        let data_len = usize::try_from(read_cpi_u64(&input, cursor)?)
            .map_err(|_| ReplaySyscallError::LengthOverflow)?;
        if data_len != baseline.data_len() {
            return Err(malformed_cpi("guest mutated immutable account data length"));
        }
        cursor = cursor
            .checked_add(8)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        let data_offset = cursor;
        let data_range = checked_guest_range(cursor, data_len)?;
        let data = input
            .get(data_range.clone())
            .ok_or_else(|| malformed_cpi("truncated account data"))?
            .to_vec();
        cursor = data_range.end;

        let owner_range = checked_guest_range(cursor, 32)?;
        let mut owner = [0_u8; 32];
        owner.copy_from_slice(
            input
                .get(owner_range.clone())
                .ok_or_else(|| malformed_cpi("truncated account owner"))?,
        );
        cursor = owner_range.end;
        let executable = match input.get(cursor).copied() {
            Some(0) => false,
            Some(1) => true,
            _ => return Err(malformed_cpi("invalid account executable boolean")),
        };
        cursor = cursor
            .checked_add(1)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        let rent_epoch = read_cpi_u64(&input, cursor)?;
        cursor = cursor
            .checked_add(8)
            .ok_or(ReplaySyscallError::LengthOverflow)?;

        if pubkey != baseline.pubkey() {
            return Err(malformed_cpi("guest mutated immutable account pubkey"));
        }
        if is_signer != baseline.is_signer() {
            return Err(malformed_cpi(
                "guest mutated immutable account signer privilege",
            ));
        }
        if is_writable != baseline.is_writable() {
            return Err(malformed_cpi(
                "guest mutated immutable account writable privilege",
            ));
        }
        if owner != baseline.owner() {
            return Err(malformed_cpi("guest mutated immutable account owner"));
        }
        if executable != baseline.executable() {
            return Err(malformed_cpi(
                "guest mutated immutable account executable flag",
            ));
        }
        if rent_epoch != baseline.rent_epoch() {
            return Err(malformed_cpi("guest mutated immutable account rent epoch"));
        }

        let unique = accounts.len();
        accounts.push(GuestParameterAccount {
            pubkey: baseline.pubkey(),
            is_signer: baseline.is_signer(),
            is_writable: baseline.is_writable(),
            key_vm_address: input_vm_address(key_offset)?,
            lamports_vm_address: input_vm_address(lamports_offset)?,
            data_vm_address: input_vm_address(data_offset)?,
            data_len,
            snapshot: AccountSnapshot {
                lamports,
                owner: baseline.owner(),
                executable: baseline.executable(),
                rent_epoch: baseline.rent_epoch(),
                data: data.into(),
            },
        });
        position_to_unique.push(unique);
    }

    // Validate the trailing instruction-data vector and caller program id so
    // malformed arbitrary input cannot be mistaken for the loader ABI.
    let instruction_data_len = usize::try_from(read_cpi_u64(&input, cursor)?)
        .map_err(|_| ReplaySyscallError::LengthOverflow)?;
    cursor = cursor
        .checked_add(8)
        .ok_or(ReplaySyscallError::LengthOverflow)?;
    cursor = checked_guest_range(cursor, instruction_data_len)?.end;
    let program_id_end = checked_guest_range(cursor, 32)?.end;
    if program_id_end != input.len() {
        return Err(malformed_cpi(
            "unexpected bytes after loader parameter buffer",
        ));
    }
    if accounts.len() != context.verifier_baselines.len() {
        return Err(malformed_cpi(
            "guest account layout omits verifier baseline entries",
        ));
    }
    Ok(accounts)
}

fn decode_rust_instruction(
    context: &ReplayContext,
    instruction_address: u64,
) -> Result<DecodedRustInstruction, ReplaySyscallError> {
    const INSTRUCTION_LEN: u64 = 80;
    const ACCOUNT_META_LEN: usize = 34;
    let instruction = copy_guest_bytes(context, instruction_address, INSTRUCTION_LEN)?;

    let accounts_address = read_cpi_u64(&instruction, 0)?;
    let accounts_capacity = read_cpi_u64(&instruction, 8)?;
    let accounts_len = usize::try_from(read_cpi_u64(&instruction, 16)?)
        .map_err(|_| ReplaySyscallError::LengthOverflow)?;
    if u64::try_from(accounts_len).unwrap_or(u64::MAX) > accounts_capacity
        || accounts_len > MAX_LEGACY_INSTRUCTION_ACCOUNTS
    {
        return Err(malformed_cpi("invalid Instruction.accounts Vec"));
    }
    let accounts_bytes_len = accounts_len
        .checked_mul(ACCOUNT_META_LEN)
        .ok_or(ReplaySyscallError::LengthOverflow)?;
    let accounts_bytes = copy_guest_bytes(
        context,
        accounts_address,
        u64::try_from(accounts_bytes_len).map_err(|_| ReplaySyscallError::LengthOverflow)?,
    )?;
    let mut accounts = Vec::with_capacity(accounts_len);
    for account in accounts_bytes.chunks_exact(ACCOUNT_META_LEN) {
        let mut pubkey = [0_u8; 32];
        pubkey.copy_from_slice(&account[..32]);
        let is_signer = match account[32] {
            0 => false,
            1 => true,
            _ => return Err(malformed_cpi("invalid AccountMeta signer boolean")),
        };
        let is_writable = match account[33] {
            0 => false,
            1 => true,
            _ => return Err(malformed_cpi("invalid AccountMeta writable boolean")),
        };
        accounts.push(LaunchAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        });
    }

    let data_address = read_cpi_u64(&instruction, 24)?;
    let data_capacity = read_cpi_u64(&instruction, 32)?;
    let data_len = read_cpi_u64(&instruction, 40)?;
    if data_len > data_capacity {
        return Err(malformed_cpi("invalid Instruction.data Vec"));
    }
    let data = copy_guest_bytes(context, data_address, data_len)?;
    let mut program_id = [0_u8; 32];
    program_id.copy_from_slice(&instruction[48..80]);
    Ok(DecodedRustInstruction {
        program_id,
        accounts,
        data,
    })
}

fn decode_passed_account_infos(
    context: &ReplayContext,
    account_infos_address: u64,
    account_infos_len: u64,
    bindings: &[GuestParameterAccount],
) -> Result<Vec<[u8; 32]>, ReplaySyscallError> {
    const ACCOUNT_INFO_LEN: u64 = 48;
    let count =
        usize::try_from(account_infos_len).map_err(|_| ReplaySyscallError::LengthOverflow)?;
    if count > MAX_LEGACY_INSTRUCTION_ACCOUNTS {
        return Err(malformed_cpi(
            "AccountInfo count exceeds legacy message limit",
        ));
    }
    let byte_len = account_infos_len
        .checked_mul(ACCOUNT_INFO_LEN)
        .ok_or(ReplaySyscallError::LengthOverflow)?;
    let infos = copy_guest_bytes(context, account_infos_address, byte_len)?;
    let mut passed = Vec::with_capacity(count);
    for info in infos.chunks_exact(ACCOUNT_INFO_LEN as usize) {
        let key_address = read_cpi_u64(info, 0)?;
        let binding = bindings
            .iter()
            .find(|binding| binding.key_vm_address == key_address)
            .ok_or_else(|| malformed_cpi("AccountInfo key does not reference a caller binding"))?;
        let key = copy_guest_bytes(context, key_address, 32)?;
        if key.as_slice() != binding.pubkey {
            return Err(malformed_cpi("AccountInfo key binding was modified"));
        }
        passed.push(binding.pubkey);
    }
    Ok(passed)
}

fn derive_program_address(
    context: &ReplayContext,
    seed_descriptors_address: u64,
    seed_count: u64,
    program_id: [u8; 32],
) -> Result<[u8; 32], ReplaySyscallError> {
    let descriptor_len = seed_count
        .checked_mul(16)
        .ok_or(ReplaySyscallError::LengthOverflow)?;
    let descriptors = copy_guest_bytes(context, seed_descriptors_address, descriptor_len)?;
    let mut hasher = Sha256::new();
    for descriptor in descriptors.chunks_exact(16) {
        let seed_address = read_cpi_u64(descriptor, 0)?;
        let seed_len = read_cpi_u64(descriptor, 8)?;
        if seed_len > 32 {
            return Err(ReplaySyscallError::BadSeeds(
                "max seed length exceeded".to_owned(),
            ));
        }
        hasher.update(copy_guest_bytes(context, seed_address, seed_len)?);
    }
    hasher.update(program_id);
    hasher.update(b"ProgramDerivedAddress");
    let address: [u8; 32] = hasher.finalize().into();
    if CompressedEdwardsY(address).decompress().is_some() {
        return Err(ReplaySyscallError::BadSeeds(
            "provided seeds do not result in a valid address".to_owned(),
        ));
    }
    Ok(address)
}

fn decode_cpi_signers(
    context: &ReplayContext,
    signers_seeds_address: u64,
    signers_seeds_len: u64,
    caller_program_id: [u8; 32],
) -> Result<Vec<[u8; 32]>, ReplaySyscallError> {
    let descriptor_len = signers_seeds_len
        .checked_mul(16)
        .ok_or(ReplaySyscallError::LengthOverflow)?;
    let signer_descriptors = copy_guest_bytes(context, signers_seeds_address, descriptor_len)?;
    let mut signers = Vec::with_capacity(
        usize::try_from(signers_seeds_len).map_err(|_| ReplaySyscallError::LengthOverflow)?,
    );
    for descriptor in signer_descriptors.chunks_exact(16) {
        signers.push(derive_program_address(
            context,
            read_cpi_u64(descriptor, 0)?,
            read_cpi_u64(descriptor, 8)?,
            caller_program_id,
        )?);
    }
    Ok(signers)
}

fn normalize_cpi_account_privileges(accounts: &mut [LaunchAccountMeta]) {
    for index in 0..accounts.len() {
        let pubkey = accounts[index].pubkey;
        let mut is_signer = false;
        let mut is_writable = false;
        for account in accounts.iter() {
            if account.pubkey == pubkey {
                is_signer |= account.is_signer;
                is_writable |= account.is_writable;
            }
        }
        accounts[index].is_signer = is_signer;
        accounts[index].is_writable = is_writable;
    }
}

solana_sbpf::declare_builtin_function!(
    SyscallAbort,
    fn rust(
        _context: &mut ReplayContext,
        _arg1: u64,
        _arg2: u64,
        _arg3: u64,
        _arg4: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        Err(ReplaySyscallError::Abort)
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallSolPanic,
    fn rust(
        context: &mut ReplayContext,
        file: u64,
        len: u64,
        line: u64,
        column: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        let file = launch_syscall_string(context, file, len)?.to_owned();
        Err(ReplaySyscallError::Panic { file, line, column })
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallSolLog,
    fn rust(
        context: &mut ReplayContext,
        address: u64,
        len: u64,
        _arg3: u64,
        _arg4: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        // v1.1.14 validates the string before logging it. Replay intentionally
        // drops the textual side effect after performing the same validation.
        let _ = launch_syscall_string(context, address, len)?;
        Ok(0)
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallSolLogU64,
    fn rust(
        _context: &mut ReplayContext,
        _arg1: u64,
        _arg2: u64,
        _arg3: u64,
        _arg4: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        Ok(0)
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallSolAllocFree,
    fn rust(
        context: &mut ReplayContext,
        size: u64,
        free_address: u64,
        _arg3: u64,
        _arg4: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        if free_address != 0 {
            // The historical allocator is a bump allocator; free is a no-op.
            return Ok(0);
        }
        let end = context.heap_position.saturating_add(size);
        if end > LEGACY_BPF_HEAP_SIZE as u64 {
            return Ok(0);
        }
        let address = ebpf::MM_HEAP_START.saturating_add(context.heap_position);
        context.heap_position = end;
        Ok(address)
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallCreateProgramAddress,
    fn rust(
        context: &mut ReplayContext,
        seeds_address: u64,
        seeds_len: u64,
        program_id_address: u64,
        result_address: u64,
        _arg5: u64,
    ) -> Result<u64, ReplaySyscallError> {
        // v1.3.3's Rust ABI passes `&[&str]`: each guest slice is a
        // little-endian (address, length) pair, but the syscall treats the
        // contents as arbitrary bytes. The epoch-63 runtime first translated
        // every descriptor and seed, raised a syscall error for a seed longer
        // than 32 bytes or an on-curve result, and returned 0 after writing 32
        // bytes. Unlike the modern SDK, it imposed no seed-count limit.
        let descriptor_len = seeds_len
            .checked_mul(16)
            .ok_or(ReplaySyscallError::LengthOverflow)?;
        let descriptor_host =
            launch_syscall_map(context, AccessType::Load, seeds_address, descriptor_len)?;
        let descriptor_len =
            usize::try_from(descriptor_len).map_err(|_| ReplaySyscallError::LengthOverflow)?;
        // SAFETY: the memory mapping validated the complete descriptor range.
        let descriptors =
            unsafe { slice::from_raw_parts(descriptor_host as *const u8, descriptor_len) };

        let mut hasher = Sha256::new();
        let mut valid_seed_shape = true;
        for seed_index in 0..seeds_len {
            let descriptor_start = usize::try_from(seed_index)
                .ok()
                .and_then(|index| index.checked_mul(16))
                .ok_or(ReplaySyscallError::LengthOverflow)?;
            let seed_address = u64::from_le_bytes(
                descriptors[descriptor_start..descriptor_start + 8]
                    .try_into()
                    .expect("validated descriptor width"),
            );
            let seed_len = u64::from_le_bytes(
                descriptors[descriptor_start + 8..descriptor_start + 16]
                    .try_into()
                    .expect("validated descriptor width"),
            );
            let seed_host = launch_syscall_map(context, AccessType::Load, seed_address, seed_len)?;
            if seed_len > 32 {
                valid_seed_shape = false;
                continue;
            }
            let seed_len =
                usize::try_from(seed_len).map_err(|_| ReplaySyscallError::LengthOverflow)?;
            // SAFETY: the memory mapping validated this complete seed range.
            let seed = unsafe { slice::from_raw_parts(seed_host as *const u8, seed_len) };
            hasher.update(seed);
        }

        let program_id_host =
            launch_syscall_map(context, AccessType::Load, program_id_address, 32)?;
        // Copy before mapping the writable result so aliased guest arguments
        // cannot create host references with overlapping Rust lifetimes.
        let mut program_id = [0_u8; 32];
        // SAFETY: the memory mapping validated the 32-byte program-id range.
        program_id
            .copy_from_slice(unsafe { slice::from_raw_parts(program_id_host as *const u8, 32) });

        if !valid_seed_shape {
            return Err(ReplaySyscallError::BadSeeds(
                "max seed length exceeded".to_owned(),
            ));
        }
        hasher.update(program_id);
        hasher.update(b"ProgramDerivedAddress");
        let address: [u8; 32] = hasher.finalize().into();
        if CompressedEdwardsY(address).decompress().is_some() {
            return Err(ReplaySyscallError::BadSeeds(
                "provided seeds do not result in a valid address".to_owned(),
            ));
        }
        let result_host = launch_syscall_map(context, AccessType::Store, result_address, 32)?;
        // SAFETY: the memory mapping validated the complete writable range.
        unsafe { slice::from_raw_parts_mut(result_host as *mut u8, 32) }.copy_from_slice(&address);
        Ok(0)
    }
);

solana_sbpf::declare_builtin_function!(
    SyscallInvokeSignedRust,
    fn rust(
        context: &mut ReplayContext,
        instruction_address: u64,
        account_infos_address: u64,
        account_infos_len: u64,
        signers_seeds_address: u64,
        signers_seeds_len: u64,
    ) -> Result<u64, ReplaySyscallError> {
        if !context.cross_program_supported {
            return Err(ReplaySyscallError::CrossProgramUnavailable);
        }
        let caller_program_id = context
            .program_stack
            .last()
            .copied()
            .ok_or_else(|| malformed_cpi("missing caller program stack frame"))?;

        let bindings = scan_guest_parameter_accounts(context)?;
        let current_accounts = bindings
            .iter()
            .map(|binding| (binding.pubkey, binding.snapshot.clone()))
            .collect::<AccountMap>();
        if current_accounts.len() != bindings.len() {
            return Err(malformed_cpi("duplicate unique account pubkey"));
        }
        if context.verifier_baselines.is_empty() {
            return Err(malformed_cpi("missing caller verifier baseline"));
        }
        let current_accounts = CowAccountMap::detached(current_accounts);
        verify_launch_bpf_instruction(
            caller_program_id,
            &context.verifier_baselines,
            &current_accounts,
            context.bank_rent,
        )
        .map_err(|error| ReplaySyscallError::PreCpiVerifier(error.to_string()))?;

        let mut instruction = decode_rust_instruction(context, instruction_address)?;
        normalize_cpi_account_privileges(&mut instruction.accounts);
        let passed_account_infos = decode_passed_account_infos(
            context,
            account_infos_address,
            account_infos_len,
            &bindings,
        )?;
        let derived_signers = decode_cpi_signers(
            context,
            signers_seeds_address,
            signers_seeds_len,
            caller_program_id,
        )?;

        for account in &instruction.accounts {
            let binding = bindings
                .iter()
                .find(|binding| binding.pubkey == account.pubkey)
                .ok_or(ReplaySyscallError::MissingCpiAccount(account.pubkey))?;
            if !passed_account_infos.contains(&account.pubkey) {
                return Err(ReplaySyscallError::MissingCpiAccountInfo(account.pubkey));
            }
            if account.is_writable && !binding.is_writable {
                return Err(ReplaySyscallError::CpiPrivilegeEscalation(account.pubkey));
            }
            if account.is_signer && !binding.is_signer && !derived_signers.contains(&account.pubkey)
            {
                return Err(ReplaySyscallError::CpiPrivilegeEscalation(account.pubkey));
            }
        }
        let program_binding = bindings
            .iter()
            .find(|binding| binding.pubkey == instruction.program_id)
            .ok_or(ReplaySyscallError::MissingCpiAccount(
                instruction.program_id,
            ))?;
        if !passed_account_infos.contains(&instruction.program_id) {
            return Err(ReplaySyscallError::MissingCpiAccountInfo(
                instruction.program_id,
            ));
        }
        if !program_binding.snapshot.executable {
            return Err(ReplaySyscallError::CpiProgramNotExecutable(
                instruction.program_id,
            ));
        }
        if program_binding.snapshot.owner != BPF_LOADER_PROGRAM_ID {
            return Err(ReplaySyscallError::CpiProgramWrongOwner {
                program_id: instruction.program_id,
                owner: program_binding.snapshot.owner,
            });
        }
        if context.program_stack.len() >= MAX_LEGACY_INSTRUCTION_STACK_DEPTH {
            return Err(ReplaySyscallError::CpiCallDepth);
        }
        if instruction.program_id != caller_program_id
            && context.program_stack.contains(&instruction.program_id)
        {
            return Err(ReplaySyscallError::CpiReentrancy(instruction.program_id));
        }

        // SAFETY: ReplayContext is created synchronously by a ReplayCompiler
        // execution method and cannot outlive that compiler. Recursive calls
        // use an independent scratch lease and return before this syscall.
        let compiler = unsafe { &*context.compiler };
        let compiled_program = compiler
            .compile_nested_program(instruction.program_id, &program_binding.snapshot.data)
            .map_err(|error| ReplaySyscallError::CrossProgramExecution(error.to_string()))?;
        let mut cpi_accounts = AccountMap::with_capacity(instruction.accounts.len() + 1);
        for account in &instruction.accounts {
            if cpi_accounts.contains_key(&account.pubkey) {
                continue;
            }
            let snapshot = current_accounts
                .get(&account.pubkey)
                .ok_or(ReplaySyscallError::MissingCpiAccount(account.pubkey))?;
            cpi_accounts.insert(account.pubkey, snapshot.clone());
        }
        cpi_accounts.insert(instruction.program_id, program_binding.snapshot.clone());
        let mut cpi_accounts = CowAccountMap::detached(cpi_accounts);

        let mut child_stack = context.program_stack.clone();
        child_stack.push(instruction.program_id);
        match apply_launch_bpf_program_instruction_with_stack(
            instruction.program_id,
            &instruction.data,
            &instruction.accounts,
            &mut cpi_accounts,
            compiler,
            &compiled_program,
            context.bank_rent,
            child_stack,
        ) {
            Ok(_) => {}
            Err(LaunchBpfExecutionError::ProgramReturnedError { status }) => return Ok(status),
            Err(error) => {
                return Err(ReplaySyscallError::CrossProgramExecution(error.to_string()));
            }
        }

        let mut writebacks = Vec::new();
        for (index, account) in instruction.accounts.iter().enumerate() {
            if instruction.accounts[..index]
                .iter()
                .any(|prior| prior.pubkey == account.pubkey)
                || !account.is_writable
            {
                continue;
            }
            let post = cpi_accounts
                .get(&account.pubkey)
                .ok_or(ReplaySyscallError::MissingCpiAccount(account.pubkey))?;
            if post.executable {
                continue;
            }
            let binding = bindings
                .iter()
                .find(|binding| binding.pubkey == account.pubkey)
                .ok_or(ReplaySyscallError::MissingCpiAccount(account.pubkey))?;
            if post.data.len() != binding.data_len {
                return Err(ReplaySyscallError::CrossProgramExecution(
                    "child changed fixed-size account data length".to_owned(),
                ));
            }
            writebacks.push(GuestAccountWriteback {
                pubkey: account.pubkey,
                lamports_vm_address: binding.lamports_vm_address,
                data_vm_address: binding.data_vm_address,
                data_len: binding.data_len,
                post: post.clone(),
            });
        }

        for writeback in &writebacks {
            let lamports_host =
                launch_syscall_map(context, AccessType::Store, writeback.lamports_vm_address, 8)?;
            // SAFETY: the memory mapping validated this writable u64 range.
            unsafe { slice::from_raw_parts_mut(lamports_host as *mut u8, 8) }
                .copy_from_slice(&writeback.post.lamports.to_le_bytes());
            if writeback.data_len != 0 {
                let data_host = launch_syscall_map(
                    context,
                    AccessType::Store,
                    writeback.data_vm_address,
                    u64::try_from(writeback.data_len)
                        .map_err(|_| ReplaySyscallError::LengthOverflow)?,
                )?;
                // SAFETY: the mapping validated the entire fixed-size data
                // range and the child result length was checked above.
                unsafe { slice::from_raw_parts_mut(data_host as *mut u8, writeback.data_len) }
                    .copy_from_slice(&writeback.post.data);
            }
        }
        for writeback in &writebacks {
            let baseline = context
                .verifier_baselines
                .iter_mut()
                .find(|baseline| baseline.pubkey() == writeback.pubkey)
                .ok_or(ReplaySyscallError::MissingCpiAccount(writeback.pubkey))?;
            baseline.adopt_cpi_post(&writeback.post);
        }
        Ok(0)
    }
);

fn launch_builtin_environment(
    config: Config,
) -> Result<Arc<BuiltinProgram<ReplayContext>>, CompilerError> {
    let mut loader = BuiltinProgram::new_loader(config);
    loader
        .register_definition::<SyscallAbort>("abort")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallCreateProgramAddress>("sol_create_program_address")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallInvokeSignedRust>("sol_invoke_signed_rust")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallSolPanic>("sol_panic_")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallSolLog>("sol_log_")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallSolLogU64>("sol_log_64_")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    loader
        .register_definition::<SyscallSolAllocFree>("sol_alloc_free_")
        .map_err(|error| CompilerError::Load(error.to_string()))?;
    Ok(Arc::new(loader))
}

fn launch_syscall_map(
    context: &ReplayContext,
    access: AccessType,
    vm_address: u64,
    len: u64,
) -> Result<u64, ReplaySyscallError> {
    let host_address: Result<u64, EbpfError> =
        context.memory_mapping.map(access, vm_address, len).into();
    host_address.map_err(|error| ReplaySyscallError::MemoryAccess(error.to_string()))
}

fn launch_syscall_string(
    context: &mut ReplayContext,
    vm_address: u64,
    len: u64,
) -> Result<&str, ReplaySyscallError> {
    let host_address = launch_syscall_map(context, AccessType::Load, vm_address, len)?;
    let len = usize::try_from(len)
        .map_err(|_| ReplaySyscallError::MemoryAccess("string length exceeds usize".to_owned()))?;
    // SAFETY: `MemoryMapping::map` validated the complete load range, and the
    // mapping's backing allocations outlive the syscall invocation.
    let bytes = unsafe { slice::from_raw_parts(host_address as *const u8, len) };
    let terminated = bytes
        .iter()
        .position(|byte| *byte == 0)
        .map_or(bytes, |end| &bytes[..end]);
    str::from_utf8(terminated).map_err(|_| ReplaySyscallError::InvalidString)
}

struct ReplayContext {
    remaining: u64,
    memory_mapping: MemoryMapping,
    heap_position: u64,
    compiler: *const ReplayCompiler,
    input_len: usize,
    bank_rent: LaunchBpfLoaderRent,
    program_stack: SmallVec<[[u8; 32]; 5]>,
    cross_program_supported: bool,
    verifier_baselines: LaunchPreAccounts,
}

impl ContextObject for ReplayContext {
    fn consume(&mut self, amount: u64) {
        self.remaining = self.remaining.saturating_sub(amount);
    }

    fn get_remaining(&self) -> u64 {
        self.remaining
    }

    fn active_mapping_ptr(&mut self) -> NonNull<MemoryMapping> {
        NonNull::from(&mut self.memory_mapping)
    }
}

#[cfg(test)]
std::thread_local! {
    static INTERPRETER_ENTRY_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use solana_sbpf::program::BuiltinFunctionDefinition;

    fn fixture() -> Vec<u8> {
        STANDARD
            .decode(include_str!("../fixtures/relative_call_sbpfv0.so.b64").trim())
            .unwrap()
    }

    fn input_only_test_mapping(input: *mut [u8], config: &Config) -> MemoryMapping {
        // Production execution always supplies slots 1 through 4. These
        // syscall unit tests need only input, but aligned V0 mapping with a
        // hidden region-zero sentinel still requires the preceding canonical
        // slots to be present.
        let empty: *const [u8] = &[];
        let regions = vec![
            MemoryRegion::new(empty, ebpf::MM_BYTECODE_START),
            MemoryRegion::new(empty, ebpf::MM_STACK_START),
            MemoryRegion::new(empty, ebpf::MM_HEAP_START),
            MemoryRegion::new(input, ebpf::MM_INPUT_START),
        ];
        // SAFETY: the empty slices have static storage; each caller keeps its
        // input allocation alive for the mapping's complete lifetime.
        unsafe { MemoryMapping::new(regions, config, SBPFVersion::V0).unwrap() }
    }

    #[test]
    fn aligned_mapping_preserves_fixture_execution_and_faults() {
        let aligned = ReplayCompiler::new();
        assert!(aligned.config.enable_address_translation);
        assert!(aligned.config.enable_stack_frame_gaps);
        assert!(aligned.config.aligned_memory_mapping);
        assert!(!aligned.config.allow_memory_region_zero);
        let mut region_zero_probe = [0_u8];
        let region_zero_probe_ptr: *mut [u8] = &mut region_zero_probe;
        let region_zero_mapping = input_only_test_mapping(region_zero_probe_ptr, &aligned.config);
        let region_zero_result: Result<u64, EbpfError> =
            region_zero_mapping.map(AccessType::Load, 0, 0).into();
        assert!(region_zero_result.is_err());

        let mut unaligned_config = aligned.config.clone();
        unaligned_config.aligned_memory_mapping = false;
        let unaligned = ReplayCompiler::with_config(unaligned_config);
        let elf = fixture();
        let aligned_program = aligned
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();
        let unaligned_program = unaligned
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();

        for input in [vec![1], vec![1; 256]] {
            let aligned_outcome = aligned
                .execute_with_request(
                    &aligned_program,
                    input.clone(),
                    ExecutionRequest::Interpreter,
                )
                .unwrap();
            let unaligned_outcome = unaligned
                .execute_with_request(
                    &unaligned_program,
                    input.clone(),
                    ExecutionRequest::Interpreter,
                )
                .unwrap();
            assert_eq!(aligned_outcome, unaligned_outcome);

            #[cfg(any(
                target_arch = "aarch64",
                all(not(target_os = "windows"), target_arch = "x86_64")
            ))]
            {
                let aligned_native = aligned
                    .execute_with_request(
                        &aligned_program,
                        input.clone(),
                        ExecutionRequest::NativeRequired,
                    )
                    .unwrap();
                let unaligned_native = unaligned
                    .execute_with_request(
                        &unaligned_program,
                        input,
                        ExecutionRequest::NativeRequired,
                    )
                    .unwrap();
                assert_eq!(aligned_native, unaligned_native);
            }
        }

        let aligned_error = aligned
            .execute_with_request(&aligned_program, Vec::new(), ExecutionRequest::Interpreter)
            .unwrap_err();
        let unaligned_error = unaligned
            .execute_with_request(
                &unaligned_program,
                Vec::new(),
                ExecutionRequest::Interpreter,
            )
            .unwrap_err();
        assert_eq!(aligned_error.to_string(), unaligned_error.to_string());

        #[cfg(any(
            target_arch = "aarch64",
            all(not(target_os = "windows"), target_arch = "x86_64")
        ))]
        {
            let aligned_native_error = aligned
                .execute_with_request(
                    &aligned_program,
                    Vec::new(),
                    ExecutionRequest::NativeRequired,
                )
                .unwrap_err();
            let unaligned_native_error = unaligned
                .execute_with_request(
                    &unaligned_program,
                    Vec::new(),
                    ExecutionRequest::NativeRequired,
                )
                .unwrap_err();
            assert_eq!(
                aligned_native_error.to_string(),
                unaligned_native_error.to_string()
            );
        }
    }

    #[test]
    fn aligned_mapping_rejects_a_region_crossing_canonical_slots() {
        let bytes = [0_u8; 2];
        let bytes_ptr: *const [u8] = &bytes;
        let empty: *const [u8] = &[];
        let regions = vec![
            MemoryRegion::new(bytes_ptr, ebpf::MM_STACK_START - 1),
            MemoryRegion::new(empty, ebpf::MM_STACK_START),
            MemoryRegion::new(empty, ebpf::MM_HEAP_START),
            MemoryRegion::new(empty, ebpf::MM_INPUT_START),
        ];
        let error = validate_canonical_execution_regions(&regions).unwrap_err();
        assert!(error.to_string().contains("escapes canonical slot"));
    }

    #[test]
    fn loads_verifies_compiles_and_executes_minor_elf() {
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &fixture())
            .unwrap();
        assert_eq!(program.manifest.sbpf_version, "V0");
        assert!(!program.manifest.protocol_compute_accounting_enabled);
        assert_eq!(
            program.manifest.watchdog_instruction_limit,
            WATCHDOG_INSTRUCTION_LIMIT
        );
        let outcome = compiler.execute(&program, vec![1]).unwrap();
        assert_eq!(outcome.return_value, 3);
        assert!(outcome.watchdog_instructions > 0);
        assert!(outcome.watchdog_instructions < WATCHDOG_INSTRUCTION_LIMIT);

        #[cfg(all(not(target_os = "windows"), target_arch = "x86_64"))]
        {
            assert_eq!(outcome.engine, ExecutionEngine::NativeJitX86_64);
            assert!(matches!(
                program.manifest.backend,
                CompilationBackend::NativeJitX86_64
            ));
            assert!(program.manifest.native_machine_code_len.is_some());
        }
        #[cfg(target_arch = "aarch64")]
        {
            assert_eq!(
                outcome.engine,
                ExecutionEngine::NativeCraneliftAarch64Subset
            );
            assert!(matches!(
                program.manifest.backend,
                CompilationBackend::NativeCraneliftAarch64Subset
            ));
            assert!(
                program
                    .manifest
                    .native_machine_code_len
                    .is_some_and(|len| len > 0)
            );
            assert_eq!(program.manifest.native_lowered_instruction_count, Some(16));
        }
        #[cfg(not(any(
            target_arch = "aarch64",
            all(not(target_os = "windows"), target_arch = "x86_64")
        )))]
        {
            assert_eq!(outcome.engine, ExecutionEngine::Interpreter);
            assert!(matches!(
                program.manifest.backend,
                CompilationBackend::InterpreterOnly { .. }
            ));
            assert_eq!(program.manifest.native_machine_code_len, None);
        }
    }

    #[test]
    fn compiler_clones_share_the_registered_runtime_environment() {
        let compiler = ReplayCompiler::new();
        let cloned = compiler.clone();
        assert!(Arc::ptr_eq(
            &compiler.runtime_environment,
            &cloned.runtime_environment
        ));

        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &fixture())
            .unwrap();
        assert!(Arc::ptr_eq(
            &compiler.runtime_environment,
            program.executable.get_loader()
        ));
    }

    #[test]
    fn compiler_clones_share_nested_cache_and_activation_state() {
        let compiler = ReplayCompiler::new();
        let cloned = compiler.clone();
        assert!(Arc::ptr_eq(&compiler.shared, &cloned.shared));

        compiler.set_cross_program_supported(false);
        assert!(
            !cloned
                .shared
                .cross_program_supported
                .load(Ordering::Acquire)
        );
        cloned.set_cross_program_supported(true);
        assert!(
            compiler
                .shared
                .cross_program_supported
                .load(Ordering::Acquire)
        );

        let program_id = [0x42; 32];
        let account_data = fixture();
        let first = compiler
            .compile_nested_program(program_id, &account_data)
            .unwrap();
        let second = cloned
            .compile_nested_program(program_id, &account_data)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            compiler.shared.nested_compile_count.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            compiler.shared.nested_program_cache.lock().unwrap().len(),
            1
        );
    }

    #[test]
    fn concurrent_same_key_nested_compilation_is_single_flight() {
        const WORKERS: usize = 8;

        let compiler = ReplayCompiler::new();
        let account_data = fixture();
        let program_id = [0x24; 32];
        let barrier = Arc::new(std::sync::Barrier::new(WORKERS));
        let programs = std::thread::scope(|scope| {
            let mut workers = Vec::with_capacity(WORKERS);
            for _ in 0..WORKERS {
                let cloned = compiler.clone();
                let barrier = Arc::clone(&barrier);
                let account_data = &account_data;
                workers.push(scope.spawn(move || {
                    barrier.wait();
                    cloned
                        .compile_nested_program(program_id, account_data)
                        .unwrap()
                }));
            }
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert!(
            programs
                .iter()
                .skip(1)
                .all(|program| Arc::ptr_eq(&programs[0], program))
        );
        assert_eq!(
            compiler.shared.nested_compile_count.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn thread_local_scratch_leases_are_recursion_safe() {
        let config = ReplayCompiler::new().config;
        let mut outer = ReplayExecutionScratchLease::acquire(&config);
        outer.get_mut().stack.as_slice_mut()[0] = 0x5a;
        let outer_stack = outer.get_mut().stack.as_slice_mut().as_mut_ptr();

        let mut nested = ReplayExecutionScratchLease::acquire(&config);
        let nested_stack = nested.get_mut().stack.as_slice_mut().as_mut_ptr();
        assert_ne!(outer_stack, nested_stack);
        nested.get_mut().stack.as_slice_mut()[0] = 0xa5;
        drop(nested);

        assert_eq!(outer.get_mut().stack.as_slice_mut()[0], 0x5a);
    }

    #[test]
    fn scratch_reset_clears_heap_writes_beyond_allocator_watermark() {
        let config = ReplayCompiler::new().config;
        let mut scratch = ReplayExecutionScratch::new(&config);
        let last_heap_byte = scratch.heap.len() - 1;
        scratch.heap.as_slice_mut()[last_heap_byte] = 0xa5;

        // A direct guest store does not advance `ReplayContext::heap_position`.
        scratch.mark_used_after_execute(0, false);
        scratch.reset(&config);

        assert_eq!(scratch.heap.as_slice_mut()[last_heap_byte], 0);
    }

    #[test]
    fn thread_local_scratch_supports_shared_parallel_execution() {
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &fixture())
            .unwrap();

        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..4 {
                workers.push(scope.spawn(|| {
                    for _ in 0..64 {
                        let outcome = compiler
                            .execute_with_request(&program, vec![1], ExecutionRequest::Interpreter)
                            .unwrap();
                        assert_eq!(outcome.return_value, 3);
                    }
                }));
            }
            for worker in workers {
                worker.join().unwrap();
            }
        });
    }

    #[test]
    fn artifact_identity_is_recomputed_from_loaded_elf_bytes() {
        let compiler = ReplayCompiler::new();
        let mut extracted = extract_program(LoaderAccountKind::BareElf, &fixture()).unwrap();
        extracted.elf_sha256 = [0; 32];
        let program = compiler.compile_extracted(&extracted).unwrap();
        assert_ne!(program.manifest.elf_sha256, extracted.elf_sha256);
        assert_eq!(
            program.manifest.elf_sha256,
            <[u8; 32]>::from(Sha256::digest(&extracted.elf))
        );
    }

    #[test]
    fn epoch_67_serum_pda_syscall_matches_historical_address() {
        let market = bs58::decode("ugq3ix2zG9EMDWMWSc14mcxhZuSmrzWuAEWhmLZipxF")
            .into_vec()
            .unwrap();
        let program_id = bs58::decode("DXgi6RmREQNFnWRV4gP28CQFdp4k8f6YSqGjH6fgJLDq")
            .into_vec()
            .unwrap();
        let expected = bs58::decode("B2pp1q3AaBt722ofSmPBQKBnEGpmHQuHkhkUSK87Rqz3")
            .into_vec()
            .unwrap();
        assert_eq!(market.len(), 32);
        assert_eq!(program_id.len(), 32);
        assert_eq!(expected.len(), 32);

        const DESCRIPTORS: usize = 0;
        const MARKET: usize = 32;
        const NONCE: usize = 64;
        const PROGRAM_ID: usize = 72;
        const RESULT: usize = 104;
        let mut input = vec![0xa5; 136];
        let vm = ebpf::MM_INPUT_START;
        input[DESCRIPTORS..DESCRIPTORS + 8].copy_from_slice(&(vm + MARKET as u64).to_le_bytes());
        input[DESCRIPTORS + 8..DESCRIPTORS + 16].copy_from_slice(&32_u64.to_le_bytes());
        input[DESCRIPTORS + 16..DESCRIPTORS + 24]
            .copy_from_slice(&(vm + NONCE as u64).to_le_bytes());
        input[DESCRIPTORS + 24..DESCRIPTORS + 32].copy_from_slice(&8_u64.to_le_bytes());
        input[MARKET..MARKET + 32].copy_from_slice(&market);
        input[NONCE..NONCE + 8].copy_from_slice(&0_u64.to_le_bytes());
        input[PROGRAM_ID..PROGRAM_ID + 32].copy_from_slice(&program_id);

        let compiler = ReplayCompiler::new();
        let config = &compiler.config;
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // SAFETY: `input` outlives the mapping and syscall invocation.
        let memory_mapping = input_only_test_mapping(input_ptr, config);
        let mut context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler: &compiler,
            input_len: input.len(),
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            program_stack: SmallVec::from_slice(&[[0; 32]]),
            cross_program_supported: true,
            verifier_baselines: LaunchPreAccounts::new(),
        };
        let status =
            <SyscallCreateProgramAddress as BuiltinFunctionDefinition<ReplayContext>>::rust(
                &mut context,
                vm + DESCRIPTORS as u64,
                2,
                vm + PROGRAM_ID as u64,
                vm + RESULT as u64,
                0,
            )
            .unwrap();
        assert_eq!(status, 0);
        assert_eq!(&input[RESULT..RESULT + 32], expected.as_slice());

        let loader = &compiler.runtime_environment;
        assert!(
            loader
                .get_function_registry()
                .lookup_by_name(b"sol_create_program_address")
                .is_some()
        );
        assert!(
            loader
                .get_function_registry()
                .lookup_by_name(b"sol_invoke_signed_rust")
                .is_some()
        );
    }

    #[test]
    fn epoch_63_pda_syscall_has_no_modern_seed_count_limit() {
        const SEED_COUNT: usize = 17;
        const SEED: usize = SEED_COUNT * 16;
        const PROGRAM_ID: usize = SEED + 1;
        const RESULT: usize = PROGRAM_ID + 32;
        let vm = ebpf::MM_INPUT_START;
        let mut input = vec![0xa5; RESULT + 32];
        for descriptor in 0..SEED_COUNT {
            let start = descriptor * 16;
            input[start..start + 8].copy_from_slice(&(vm + SEED as u64).to_le_bytes());
            input[start + 8..start + 16].copy_from_slice(&1_u64.to_le_bytes());
        }
        input[SEED] = 7;

        let mut program_id = [0_u8; 32];
        let expected = loop {
            let mut hasher = Sha256::new();
            for _ in 0..SEED_COUNT {
                hasher.update([7]);
            }
            hasher.update(program_id);
            hasher.update(b"ProgramDerivedAddress");
            let digest: [u8; 32] = hasher.finalize().into();
            if CompressedEdwardsY(digest).decompress().is_none() {
                break digest;
            }
            program_id[0] = program_id[0].wrapping_add(1);
        };
        input[PROGRAM_ID..PROGRAM_ID + 32].copy_from_slice(&program_id);

        let compiler = ReplayCompiler::new();
        let config = &compiler.config;
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // SAFETY: `input` outlives the mapping and syscall invocation.
        let memory_mapping = input_only_test_mapping(input_ptr, config);
        let mut context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler: &compiler,
            input_len: input.len(),
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            program_stack: SmallVec::from_slice(&[[0; 32]]),
            cross_program_supported: true,
            verifier_baselines: LaunchPreAccounts::new(),
        };
        let status =
            <SyscallCreateProgramAddress as BuiltinFunctionDefinition<ReplayContext>>::rust(
                &mut context,
                vm,
                SEED_COUNT as u64,
                vm + PROGRAM_ID as u64,
                vm + RESULT as u64,
                0,
            )
            .unwrap();
        assert_eq!(status, 0);
        assert_eq!(&input[RESULT..], &expected);
    }

    #[test]
    fn cpi_scanner_reuses_duplicate_parameter_binding() {
        let first_key = [1_u8; 32];
        let second_key = [2_u8; 32];
        let program_id = [9_u8; 32];
        let metas = [
            LaunchAccountMeta {
                pubkey: first_key,
                is_signer: true,
                is_writable: true,
            },
            LaunchAccountMeta {
                pubkey: first_key,
                is_signer: true,
                is_writable: true,
            },
            LaunchAccountMeta {
                pubkey: second_key,
                is_signer: false,
                is_writable: false,
            },
        ];
        let mut bank = AccountMap::new();
        bank.insert(
            first_key,
            AccountSnapshot {
                lamports: 41,
                owner: [3; 32],
                executable: false,
                rent_epoch: 7,
                data: vec![4, 5, 6].into(),
            },
        );
        bank.insert(
            second_key,
            AccountSnapshot {
                lamports: 42,
                owner: [8; 32],
                executable: true,
                rent_epoch: 9,
                data: vec![10, 11].into(),
            },
        );
        let mut input = crate::launch_bpf_execute::serialize_parameters(
            program_id,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
            &[12, 13],
        )
        .unwrap();
        let verifier_baselines = crate::launch_bpf_execute::launch_pre_accounts(
            program_id,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
        )
        .unwrap();
        let compiler = ReplayCompiler::new();
        let input_len = input.len();
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // SAFETY: input outlives the mapping and scanner.
        let memory_mapping = input_only_test_mapping(input_ptr, &compiler.config);
        let context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler: &compiler,
            input_len,
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            program_stack: SmallVec::from_slice(&[program_id]),
            cross_program_supported: true,
            verifier_baselines,
        };

        let bindings = scan_guest_parameter_accounts(&context).unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].pubkey, first_key);
        assert_eq!(bindings[0].key_vm_address, ebpf::MM_INPUT_START + 11);
        assert_eq!(bindings[0].data_vm_address, ebpf::MM_INPUT_START + 59);
        assert_eq!(bindings[0].snapshot, bank[&first_key]);
        assert_eq!(bindings[1].pubkey, second_key);
        assert_eq!(bindings[1].snapshot, bank[&second_key]);
    }

    fn scan_test_parameters(
        compiler: &ReplayCompiler,
        input: &mut Vec<u8>,
        program_id: [u8; 32],
        verifier_baselines: LaunchPreAccounts,
    ) -> Result<Vec<GuestParameterAccount>, ReplaySyscallError> {
        let input_len = input.len();
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // SAFETY: input outlives the mapping and scanner invocation.
        let memory_mapping = input_only_test_mapping(input_ptr, &compiler.config);
        let context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler,
            input_len,
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            program_stack: SmallVec::from_slice(&[program_id]),
            cross_program_supported: true,
            verifier_baselines,
        };
        scan_guest_parameter_accounts(&context)
    }

    #[test]
    fn cpi_scanner_rejects_guest_mutation_of_every_immutable_account_field() {
        const PUBKEY: [u8; 32] = [31; 32];
        const PROGRAM_ID: [u8; 32] = [32; 32];
        const SIGNER_OFFSET: usize = 9;
        const WRITABLE_OFFSET: usize = 10;
        const PUBKEY_OFFSET: usize = 11;
        const DATA_LEN_OFFSET: usize = 51;
        const OWNER_OFFSET: usize = 62;
        const EXECUTABLE_OFFSET: usize = 94;
        const RENT_EPOCH_OFFSET: usize = 95;

        let metas = [LaunchAccountMeta {
            pubkey: PUBKEY,
            is_signer: false,
            is_writable: false,
        }];
        let bank = AccountMap::from([(
            PUBKEY,
            AccountSnapshot {
                lamports: 41,
                owner: [33; 32],
                executable: false,
                rent_epoch: 7,
                data: vec![4, 5, 6].into(),
            },
        )]);
        let serialized = crate::launch_bpf_execute::serialize_parameters(
            PROGRAM_ID,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
            &[],
        )
        .unwrap();
        let verifier_baselines = crate::launch_bpf_execute::launch_pre_accounts(
            PROGRAM_ID,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
        )
        .unwrap();
        let mutations = [
            ("pubkey", PUBKEY_OFFSET..PUBKEY_OFFSET + 1, vec![0xff]),
            ("signer", SIGNER_OFFSET..SIGNER_OFFSET + 1, vec![1]),
            ("writable", WRITABLE_OFFSET..WRITABLE_OFFSET + 1, vec![1]),
            (
                "data length",
                DATA_LEN_OFFSET..DATA_LEN_OFFSET + 8,
                4_u64.to_le_bytes().to_vec(),
            ),
            ("owner", OWNER_OFFSET..OWNER_OFFSET + 1, vec![0xfe]),
            (
                "executable",
                EXECUTABLE_OFFSET..EXECUTABLE_OFFSET + 1,
                vec![1],
            ),
            (
                "rent epoch",
                RENT_EPOCH_OFFSET..RENT_EPOCH_OFFSET + 8,
                8_u64.to_le_bytes().to_vec(),
            ),
        ];
        let compiler = ReplayCompiler::new();

        for (field, range, replacement) in mutations {
            let mut input = serialized.clone();
            input[range].copy_from_slice(&replacement);
            let error = scan_test_parameters(
                &compiler,
                &mut input,
                PROGRAM_ID,
                verifier_baselines.clone(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(field),
                "{field} mutation produced unexpected error: {error}"
            );
        }
    }

    #[test]
    fn cpi_scanner_preserves_valid_layout_and_uses_guest_lamports_and_data() {
        const PUBKEY: [u8; 32] = [34; 32];
        const PROGRAM_ID: [u8; 32] = [35; 32];
        const LAMPORTS_OFFSET: usize = 43;
        const DATA_OFFSET: usize = 59;

        let metas = [LaunchAccountMeta {
            pubkey: PUBKEY,
            is_signer: true,
            is_writable: true,
        }];
        let bank = AccountMap::from([(
            PUBKEY,
            AccountSnapshot {
                lamports: 41,
                owner: [36; 32],
                executable: false,
                rent_epoch: 7,
                data: vec![4, 5, 6].into(),
            },
        )]);
        let mut input = crate::launch_bpf_execute::serialize_parameters(
            PROGRAM_ID,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
            &[],
        )
        .unwrap();
        input[LAMPORTS_OFFSET..LAMPORTS_OFFSET + 8].copy_from_slice(&99_u64.to_le_bytes());
        input[DATA_OFFSET..DATA_OFFSET + 3].copy_from_slice(&[7, 8, 9]);
        let verifier_baselines = crate::launch_bpf_execute::launch_pre_accounts(
            PROGRAM_ID,
            &metas,
            &crate::CowAccountMap::detached(bank.clone()),
        )
        .unwrap();
        let compiler = ReplayCompiler::new();

        let bindings =
            scan_test_parameters(&compiler, &mut input, PROGRAM_ID, verifier_baselines).unwrap();

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].pubkey, PUBKEY);
        assert!(bindings[0].is_signer);
        assert!(bindings[0].is_writable);
        assert_eq!(bindings[0].snapshot.lamports, 99);
        assert_eq!(bindings[0].snapshot.data, [7, 8, 9]);
        assert_eq!(bindings[0].snapshot.owner, [36; 32]);
        assert!(!bindings[0].snapshot.executable);
        assert_eq!(bindings[0].snapshot.rent_epoch, 7);
    }

    #[test]
    fn rust_cpi_decoder_matches_epoch_67_guest_layout() {
        const INSTRUCTION: usize = 0;
        const METAS: usize = 80;
        const DATA: usize = METAS + 2 * 34;
        const INFOS: usize = 176;
        const FIRST_KEY: usize = 272;
        const SECOND_KEY: usize = 304;
        let vm = ebpf::MM_INPUT_START;
        let first_key = [21_u8; 32];
        let second_key = [22_u8; 32];
        let program_id = [23_u8; 32];
        let transfer = [3, 0xde, 0x05, 0, 0, 0, 0, 0, 0];
        let mut input = vec![0_u8; 336];

        input[INSTRUCTION..INSTRUCTION + 8].copy_from_slice(&(vm + METAS as u64).to_le_bytes());
        input[INSTRUCTION + 8..INSTRUCTION + 16].copy_from_slice(&2_u64.to_le_bytes());
        input[INSTRUCTION + 16..INSTRUCTION + 24].copy_from_slice(&2_u64.to_le_bytes());
        input[INSTRUCTION + 24..INSTRUCTION + 32]
            .copy_from_slice(&(vm + DATA as u64).to_le_bytes());
        input[INSTRUCTION + 32..INSTRUCTION + 40].copy_from_slice(&9_u64.to_le_bytes());
        input[INSTRUCTION + 40..INSTRUCTION + 48].copy_from_slice(&9_u64.to_le_bytes());
        input[INSTRUCTION + 48..INSTRUCTION + 80].copy_from_slice(&program_id);
        input[METAS..METAS + 32].copy_from_slice(&first_key);
        input[METAS + 32] = 0;
        input[METAS + 33] = 1;
        input[METAS + 34..METAS + 66].copy_from_slice(&second_key);
        input[METAS + 66] = 1;
        input[METAS + 67] = 0;
        input[DATA..DATA + transfer.len()].copy_from_slice(&transfer);

        input[INFOS..INFOS + 8].copy_from_slice(&(vm + FIRST_KEY as u64).to_le_bytes());
        input[INFOS + 48..INFOS + 56].copy_from_slice(&(vm + SECOND_KEY as u64).to_le_bytes());
        input[FIRST_KEY..FIRST_KEY + 32].copy_from_slice(&first_key);
        input[SECOND_KEY..SECOND_KEY + 32].copy_from_slice(&second_key);

        let compiler = ReplayCompiler::new();
        let input_len = input.len();
        let input_ptr: *mut [u8] = input.as_mut_slice();
        // SAFETY: input outlives the mapping and decoder.
        let memory_mapping = input_only_test_mapping(input_ptr, &compiler.config);
        let context = ReplayContext {
            remaining: WATCHDOG_INSTRUCTION_LIMIT,
            memory_mapping,
            heap_position: 0,
            compiler: &compiler,
            input_len,
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 0,
                exemption_threshold: 0.0,
            },
            program_stack: SmallVec::from_slice(&[[24; 32]]),
            cross_program_supported: true,
            verifier_baselines: LaunchPreAccounts::new(),
        };

        let instruction = decode_rust_instruction(&context, vm + INSTRUCTION as u64).unwrap();
        assert_eq!(instruction.program_id, program_id);
        assert_eq!(instruction.data, transfer);
        assert_eq!(
            instruction.accounts,
            vec![
                LaunchAccountMeta {
                    pubkey: first_key,
                    is_signer: false,
                    is_writable: true,
                },
                LaunchAccountMeta {
                    pubkey: second_key,
                    is_signer: true,
                    is_writable: false,
                },
            ]
        );
        let bindings = vec![
            GuestParameterAccount {
                pubkey: first_key,
                is_signer: false,
                is_writable: true,
                key_vm_address: vm + FIRST_KEY as u64,
                lamports_vm_address: 0,
                data_vm_address: 0,
                data_len: 0,
                snapshot: AccountSnapshot {
                    lamports: 0,
                    owner: [0; 32],
                    executable: false,
                    rent_epoch: 0,
                    data: Vec::new().into(),
                },
            },
            GuestParameterAccount {
                pubkey: second_key,
                is_signer: true,
                is_writable: false,
                key_vm_address: vm + SECOND_KEY as u64,
                lamports_vm_address: 0,
                data_vm_address: 0,
                data_len: 0,
                snapshot: AccountSnapshot {
                    lamports: 0,
                    owner: [0; 32],
                    executable: false,
                    rent_epoch: 0,
                    data: Vec::new().into(),
                },
            },
        ];
        assert_eq!(
            decode_passed_account_infos(&context, vm + INFOS as u64, 2, &bindings).unwrap(),
            vec![first_key, second_key]
        );
    }

    #[test]
    fn sbpfv0_stack_mapping_preserves_guard_gaps_between_frames() {
        use solana_sbpf::memory_region::AccessType;

        let config = ReplayCompiler::new().config;
        let frame_size = config.stack_frame_size as u64;
        let mut stack = AlignedMemory::<{ ebpf::HOST_ALIGN }>::zero_filled(config.stack_size());
        let region = stack_memory_region(&mut stack, &config, SBPFVersion::V0);

        assert!(
            region
                .vm_to_host(AccessType::Store, ebpf::MM_STACK_START + frame_size - 1, 1,)
                .is_some()
        );
        assert!(
            region
                .vm_to_host(AccessType::Store, ebpf::MM_STACK_START + frame_size, 1)
                .is_none()
        );
        assert!(
            region
                .vm_to_host(AccessType::Store, ebpf::MM_STACK_START + frame_size * 2, 1,)
                .is_some()
        );
    }

    #[test]
    fn watchdog_stops_a_verified_backward_loop() {
        let mut elf = fixture();
        // The fixture's .text starts at file offset 0x120 and its entrypoint is
        // instruction four. Replace that instruction with `ja -1`, a loop back
        // to itself that remains valid SBPFv0 bytecode.
        let entry = 0x120 + 4 * ebpf::INSN_SIZE;
        elf[entry..entry + ebpf::INSN_SIZE].copy_from_slice(&[ebpf::JA, 0, 0xff, 0xff, 0, 0, 0, 0]);
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();

        #[cfg(target_arch = "aarch64")]
        {
            assert!(matches!(
                program.manifest.backend,
                CompilationBackend::InterpreterOnly { .. }
            ));
            assert!(matches!(
                compiler.execute_with_request(&program, vec![1], ExecutionRequest::NativeRequired),
                Err(CompilerError::NativeUnavailable { .. })
            ));
        }

        let error = compiler.execute(&program, vec![1]).unwrap_err();
        assert!(matches!(
            error,
            CompilerError::WatchdogExceeded {
                limit: WATCHDOG_INSTRUCTION_LIMIT
            }
        ));
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn forced_native_matches_forced_interpreter_for_every_input_byte() {
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &fixture())
            .unwrap();
        assert!(matches!(
            program.manifest.backend,
            CompilationBackend::NativeCraneliftAarch64Subset
        ));

        for input in 0..=u8::MAX {
            let interpreted = compiler
                .execute_with_request(&program, vec![input], ExecutionRequest::Interpreter)
                .unwrap();
            let interpreter_entries_before = INTERPRETER_ENTRY_COUNT.with(std::cell::Cell::get);
            let native = compiler
                .execute_with_request(&program, vec![input], ExecutionRequest::NativeRequired)
                .unwrap();
            let interpreter_entries_after = INTERPRETER_ENTRY_COUNT.with(std::cell::Cell::get);

            assert_eq!(native.engine, ExecutionEngine::NativeCraneliftAarch64Subset);
            assert_eq!(
                native.return_value, interpreted.return_value,
                "input={input}"
            );
            assert_eq!(native.input_after, interpreted.input_after, "input={input}");
            assert_eq!(
                native.watchdog_instructions, interpreted.watchdog_instructions,
                "input={input}"
            );
            assert_eq!(
                interpreter_entries_after, interpreter_entries_before,
                "native execution entered the interpreter for input={input}"
            );
        }
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn native_empty_input_fails_through_checked_memory_mapping() {
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &fixture())
            .unwrap();
        let interpreted = compiler
            .execute_with_request(&program, vec![], ExecutionRequest::Interpreter)
            .unwrap_err();
        let native = compiler
            .execute_with_request(&program, vec![], ExecutionRequest::NativeRequired)
            .unwrap_err();

        assert!(matches!(interpreted, CompilerError::Execute(_)));
        assert!(matches!(native, CompilerError::Execute(_)));
        assert!(
            interpreted.to_string().starts_with(&native.to_string()),
            "interpreter and native fault differ: interpreted={interpreted}, native={native}"
        );
        assert!(interpreted.to_string().ends_with("at guest pc 4"));
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn native_store_cannot_cross_an_sbpfv0_stack_guard_gap() {
        let mut elf = fixture();
        // pc 5 is `stxdw [r10-256], r6`. Point it at `[r10]`, which is the
        // first byte of the guard gap immediately above the root frame.
        let store = 0x120 + 5 * ebpf::INSN_SIZE;
        elf[store + 2..store + 4].copy_from_slice(&0i16.to_le_bytes());
        let compiler = ReplayCompiler::new();
        let program = compiler
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap();
        assert!(matches!(
            program.manifest.backend,
            CompilationBackend::NativeCraneliftAarch64Subset
        ));

        let interpreted = compiler
            .execute_with_request(&program, vec![1], ExecutionRequest::Interpreter)
            .unwrap_err();
        let native = compiler
            .execute_with_request(&program, vec![1], ExecutionRequest::NativeRequired)
            .unwrap_err();
        assert!(interpreted.to_string().contains("stack frame"));
        assert!(
            interpreted.to_string().starts_with(&native.to_string()),
            "interpreter and native fault differ: interpreted={interpreted}, native={native}"
        );
        assert!(interpreted.to_string().ends_with("at guest pc 5"));
    }

    #[test]
    fn verifier_rejects_invalid_register_before_native_lowering() {
        let mut elf = fixture();
        // Make the entrypoint load read from invalid r11. ELF parsing still
        // succeeds; the requisite bytecode verifier must reject the register.
        let entry = 0x120 + 4 * ebpf::INSN_SIZE;
        elf[entry + 1] = (11 << 4) | 6;
        let error = ReplayCompiler::new()
            .compile_account(LoaderAccountKind::BareElf, &elf)
            .unwrap_err();
        assert!(matches!(error, CompilerError::Verify(_)));
    }
}

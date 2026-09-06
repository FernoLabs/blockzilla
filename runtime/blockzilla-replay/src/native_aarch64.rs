//! Native Cranelift proof of concept for a deliberately tiny SBPFv0 subset.
//!
//! The input to [`NativeProgram::compile`] must be the relocated `.text` bytes
//! from an ELF which has already passed `RequisiteVerifier`.  This module does
//! not parse an ELF, resolve syscalls, or silently accept an instruction it
//! does not understand.  Guest-derived addresses are never dereferenced by
//! generated code: loads and stores go through checked [`MemoryMapping`]
//! helpers.

use {
    cranelift_codegen::{
        ir::{AbiParam, Block, FuncRef, InstBuilder, UserFuncName, Value, types},
        settings::{self, Configurable},
    },
    cranelift_frontend::{FunctionBuilder, FunctionBuilderContext},
    cranelift_jit::{JITBuilder, JITModule},
    cranelift_module::{Linkage, Module, ModuleError, default_libcall_names},
    solana_sbpf::{ebpf, error::StableResult, memory_region::MemoryMapping},
    std::{
        collections::HashMap,
        mem,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Mutex,
    },
    thiserror::Error,
};

/// Versioned independently from Cranelift because changing lowering semantics
/// must invalidate every cached artifact.
pub const SUBSET_PROFILE_ID: &str = "blockzilla-sbpfv0-cranelift-straight-line-v1";

/// Host imports are part of the generated code's ABI and therefore part of
/// artifact identity.
pub const IMPORT_ABI_ID: &str = "blockzilla-native-imports-v1";

/// The first backend intentionally inlines calls.  This limit prevents a
/// small call DAG from expanding into an unexpectedly huge native function.
const MAX_EXPANDED_INSTRUCTIONS: usize = 4_096;

const LOAD_SYMBOL: &str = "blockzilla_native_load_v1";
const STORE_SYMBOL: &str = "blockzilla_native_store_v1";
const FAULTED_SYMBOL: &str = "blockzilla_native_faulted_v1";
const WATCHDOG_SYMBOL: &str = "blockzilla_native_watchdog_v1";

/// Explicit backend name reported in manifests and differential-test output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeBackend {
    CraneliftAarch64Subset,
}

/// Reproducibility metadata for the process-local native artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeManifest {
    pub backend: NativeBackend,
    pub target_triple: String,
    pub isa_fingerprint: String,
    pub cranelift_version: &'static str,
    pub subset_profile_id: &'static str,
    pub import_abi_id: &'static str,
    pub watchdog_strategy: &'static str,
    pub watchdog_limit: u64,
    pub source_instruction_count: usize,
    pub expanded_instruction_count: usize,
}

/// Successful execution of a compiled subset program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeOutcome {
    pub return_value: u64,
    /// This is a non-consensus host-safety counter, never Solana CU output.
    pub watchdog_instructions: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NativeCompileError {
    #[error("the Cranelift subset backend is available only on native aarch64 hosts")]
    UnsupportedHost,
    #[error("SBPF text length {0} is not a multiple of eight bytes")]
    MisalignedText(usize),
    #[error("SBPF text is empty")]
    EmptyText,
    #[error("entrypoint pc {entry_pc} is outside {instruction_count} instructions")]
    InvalidEntrypoint {
        entry_pc: usize,
        instruction_count: usize,
    },
    #[error("unsupported SBPF opcode 0x{opcode:02x} at pc {pc}")]
    UnsupportedOpcode { pc: usize, opcode: u8 },
    #[error("invalid register r{register} used by pc {pc}")]
    InvalidRegister { pc: usize, register: u8 },
    #[error("unresolved or external CALL_IMM key 0x{key:08x} at pc {pc}")]
    UnresolvedCall { pc: usize, key: u32 },
    #[error("CALL_IMM at pc {pc} resolves outside the text to pc {target}")]
    CallOutsideText { pc: usize, target: usize },
    #[error("recursive/cyclic internal call to pc {target} is outside the v1 subset")]
    RecursiveCall { target: usize },
    #[error("internal call would reach depth {depth}, but max_call_depth is {maximum}")]
    CallDepthExceeded { depth: usize, maximum: usize },
    #[error("control flow reached the end of text without EXIT from pc {start_pc}")]
    ExecutionOverrun { start_pc: usize },
    #[error("inlined program exceeds the {limit}-instruction POC expansion bound")]
    ExpansionLimit { limit: usize },
    #[error("stack-frame bump does not fit the native lowering")]
    InvalidStackFrameSize,
    #[error("native AArch64 JIT is unavailable in this environment: {0}")]
    NativeUnavailable(String),
    #[error("configure Cranelift: {0}")]
    CraneliftConfiguration(String),
    #[error("generate native code: {0}")]
    CraneliftCodegen(String),
}

impl NativeCompileError {
    /// Whether the verified program is valid but outside this deliberately
    /// narrow native profile and should use the interpreter fallback.
    pub const fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedHost
                | Self::UnsupportedOpcode { .. }
                | Self::UnresolvedCall { .. }
                | Self::RecursiveCall { .. }
                | Self::CallDepthExceeded { .. }
                | Self::ExecutionOverrun { .. }
                | Self::ExpansionLimit { .. }
                | Self::NativeUnavailable(_)
        )
    }
}

/// Process-local JIT artifact.
///
/// The module is retained solely to keep executable memory alive.  It is
/// placed behind a mutex because `JITModule` is `Send` but not `Sync`; native
/// execution itself does not lock or mutate the finalized module.
pub struct NativeProgram {
    module: Mutex<Option<JITModule>>,
    entry_address: usize,
    machine_code_len: usize,
    watchdog_limit: u64,
    pub manifest: NativeManifest,
}

impl std::fmt::Debug for NativeProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeProgram")
            .field("entry_address", &format_args!("0x{:x}", self.entry_address))
            .field("machine_code_len", &self.machine_code_len)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl Drop for NativeProgram {
    fn drop(&mut self) {
        if let Ok(module) = self.module.get_mut()
            && let Some(module) = module.take()
        {
            // SAFETY: `entry_address` is private and can only be invoked while
            // borrowing `self`.  Drop has exclusive access, so no native call
            // from this artifact can still be in progress.
            unsafe { module.free_memory() };
        }
    }
}

impl NativeProgram {
    /// Compile verified, relocated SBPFv0 text into native AArch64 code.
    ///
    /// `functions` maps the legacy SBPFv0 CALL_IMM hash/key found in relocated
    /// instructions to its instruction-index target.  A key absent from this
    /// map is treated as a syscall/external call and rejected.
    #[allow(clippy::too_many_arguments)]
    pub fn compile(
        text: &[u8],
        entry_pc: usize,
        functions: &HashMap<u32, usize>,
        stack_frame_size: usize,
        stack_frame_gaps: bool,
        max_call_depth: usize,
        watchdog_limit: u64,
    ) -> Result<Self, NativeCompileError> {
        if std::env::consts::ARCH != "aarch64" {
            return Err(NativeCompileError::UnsupportedHost);
        }
        let decoded = DecodedProgram::new(text, entry_pc, functions)?;
        let frame_multiplier = if stack_frame_gaps { 2 } else { 1 };
        let frame_bump = stack_frame_size
            .checked_mul(frame_multiplier)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(NativeCompileError::InvalidStackFrameSize)?;

        let mut flag_builder = settings::builder();
        flag_builder
            .set("opt_level", "speed")
            .map_err(|error| NativeCompileError::CraneliftConfiguration(error.to_string()))?;
        flag_builder
            .set("use_colocated_libcalls", "false")
            .map_err(|error| NativeCompileError::CraneliftConfiguration(error.to_string()))?;
        flag_builder
            .set("is_pic", "false")
            .map_err(|error| NativeCompileError::CraneliftConfiguration(error.to_string()))?;
        flag_builder
            .set("enable_verifier", "true")
            .map_err(|error| NativeCompileError::CraneliftConfiguration(error.to_string()))?;
        let isa_builder = cranelift_native::builder()
            .map_err(|error| NativeCompileError::NativeUnavailable(error.to_string()))?;
        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .map_err(|error| NativeCompileError::NativeUnavailable(error.to_string()))?;
        let target_triple = isa.triple().to_string();
        let isa_flags = isa
            .isa_flags()
            .into_iter()
            .map(|flag| flag.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let isa_fingerprint = format!("{isa};isa_flags=[{isa_flags}]");

        let mut jit_builder = JITBuilder::with_isa(isa, default_libcall_names());
        jit_builder
            .symbol(LOAD_SYMBOL, native_load as *const u8)
            .symbol(STORE_SYMBOL, native_store as *const u8)
            .symbol(FAULTED_SYMBOL, native_faulted as *const u8)
            .symbol(WATCHDOG_SYMBOL, native_watchdog as *const u8);
        let mut module = JITModule::new(jit_builder);
        let pointer_type = module.target_config().pointer_type();

        let mut entry_signature = module.make_signature();
        entry_signature.params.push(AbiParam::new(pointer_type));
        for _ in 0..=ebpf::FRAME_PTR_REG {
            entry_signature.params.push(AbiParam::new(types::I64));
        }
        entry_signature.returns.push(AbiParam::new(types::I64));
        let entry_id = module
            .declare_function("blockzilla_sbpf_entry_v1", Linkage::Local, &entry_signature)
            .map_err(codegen_error)?;

        let helpers = declare_helpers(&mut module, pointer_type)?;
        let mut context = module.make_context();
        context.func.signature = entry_signature;
        context.func.name = UserFuncName::user(0, entry_id.as_u32());
        let helper_refs = HelperRefs {
            load: module.declare_func_in_func(helpers.load, &mut context.func),
            store: module.declare_func_in_func(helpers.store, &mut context.func),
            faulted: module.declare_func_in_func(helpers.faulted, &mut context.func),
            watchdog: module.declare_func_in_func(helpers.watchdog, &mut context.func),
        };

        let mut function_context = FunctionBuilderContext::new();
        let expanded_instruction_count;
        {
            let mut builder = FunctionBuilder::new(&mut context.func, &mut function_context);
            let entry_block = builder.create_block();
            let error_block = builder.create_block();
            builder.switch_to_block(entry_block);
            builder.append_block_params_for_function_params(entry_block);
            let parameters = builder.block_params(entry_block);
            let invocation = parameters[0];
            let mut registers: [Value; 11] = parameters[1..12]
                .try_into()
                .expect("entry signature has eleven SBPF registers");
            let mut active_calls = vec![decoded.entry_pc];
            let mut emitted = 0usize;

            emit_sequence(
                &mut builder,
                &decoded,
                &helper_refs,
                invocation,
                &mut registers,
                decoded.entry_pc,
                0,
                max_call_depth,
                frame_bump,
                error_block,
                &mut active_calls,
                &mut emitted,
            )?;
            builder.ins().return_(&[registers[0]]);

            builder.switch_to_block(error_block);
            let zero = builder.ins().iconst(types::I64, 0);
            builder.ins().return_(&[zero]);
            builder.seal_all_blocks();
            builder.finalize(module.target_config());
            expanded_instruction_count = emitted;
        }

        if let Err(error) = module.define_function(entry_id, &mut context) {
            let error = classify_define_error(error);
            // SAFETY: no function pointer has been published or invoked.
            unsafe { module.free_memory() };
            return Err(error);
        }
        let machine_code_len = context
            .compiled_code()
            .map(|code| code.code_buffer().len())
            .unwrap_or_default();
        if let Err(error) = module.finalize_definitions() {
            let error = NativeCompileError::NativeUnavailable(error.to_string());
            // SAFETY: finalization failed before an entry pointer was exposed.
            unsafe { module.free_memory() };
            return Err(error);
        }
        let entry_address = module.get_finalized_function(entry_id).expose_provenance();
        let manifest = NativeManifest {
            backend: NativeBackend::CraneliftAarch64Subset,
            target_triple,
            isa_fingerprint,
            cranelift_version: "0.134.2",
            subset_profile_id: SUBSET_PROFILE_ID,
            import_abi_id: IMPORT_ABI_ID,
            watchdog_strategy: "checked host tick before every expanded SBPF instruction",
            watchdog_limit,
            source_instruction_count: decoded.instructions.len(),
            expanded_instruction_count,
        };
        Ok(Self {
            module: Mutex::new(Some(module)),
            entry_address,
            machine_code_len,
            watchdog_limit,
            manifest,
        })
    }

    /// Execute with all SBPF registers zeroed except r1 and r10.
    pub fn execute(
        &self,
        mapping: &mut MemoryMapping,
        r1: u64,
        r10: u64,
    ) -> Result<NativeOutcome, String> {
        let mut invocation = NativeInvocation {
            memory_mapping: mapping,
            watchdog_remaining: self.watchdog_limit,
            watchdog_consumed: 0,
            fault: None,
        };
        // SAFETY: the entrypoint is produced by Cranelift with exactly this C
        // ABI signature.  The owning module remains alive for this borrow and
        // `invocation` plus its mapping remain valid until the call returns.
        let entry_pointer = std::ptr::with_exposed_provenance::<u8>(self.entry_address);
        let entry: NativeEntry = unsafe { mem::transmute(entry_pointer) };
        let return_value = unsafe { entry(&mut invocation, 0, r1, 0, 0, 0, 0, 0, 0, 0, 0, r10) };
        if let Some(error) = invocation.fault {
            return Err(error);
        }
        Ok(NativeOutcome {
            return_value,
            watchdog_instructions: invocation.watchdog_consumed,
        })
    }

    pub const fn machine_code_len(&self) -> usize {
        self.machine_code_len
    }

    /// Dynamic instruction count of the expanded straight-line artifact.
    pub const fn instruction_count(&self) -> usize {
        self.manifest.expanded_instruction_count
    }
}

type NativeEntry = unsafe extern "C" fn(
    *mut NativeInvocation,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
) -> u64;

struct DecodedProgram {
    instructions: Vec<ebpf::Insn>,
    calls: HashMap<usize, usize>,
    entry_pc: usize,
}

impl DecodedProgram {
    fn new(
        text: &[u8],
        entry_pc: usize,
        functions: &HashMap<u32, usize>,
    ) -> Result<Self, NativeCompileError> {
        if text.is_empty() {
            return Err(NativeCompileError::EmptyText);
        }
        if !text.len().is_multiple_of(ebpf::INSN_SIZE) {
            return Err(NativeCompileError::MisalignedText(text.len()));
        }
        let instruction_count = text.len() / ebpf::INSN_SIZE;
        if entry_pc >= instruction_count {
            return Err(NativeCompileError::InvalidEntrypoint {
                entry_pc,
                instruction_count,
            });
        }
        let mut instructions = Vec::with_capacity(instruction_count);
        let mut calls = HashMap::new();
        for pc in 0..instruction_count {
            let instruction = ebpf::get_insn(text, pc);
            validate_instruction(pc, &instruction)?;
            if instruction.opc == ebpf::CALL_IMM {
                let key = instruction.imm as u32;
                let target = functions
                    .get(&key)
                    .copied()
                    .ok_or(NativeCompileError::UnresolvedCall { pc, key })?;
                if target >= instruction_count {
                    return Err(NativeCompileError::CallOutsideText { pc, target });
                }
                calls.insert(pc, target);
            }
            instructions.push(instruction);
        }
        Ok(Self {
            instructions,
            calls,
            entry_pc,
        })
    }
}

fn validate_instruction(pc: usize, instruction: &ebpf::Insn) -> Result<(), NativeCompileError> {
    match instruction.opc {
        ebpf::LD_B_REG | ebpf::LD_DW_REG => {
            validate_register(pc, instruction.dst)?;
            validate_register(pc, instruction.src)?;
        }
        ebpf::ST_DW_REG | ebpf::MOV64_REG | ebpf::ADD64_REG => {
            validate_register(pc, instruction.dst)?;
            validate_register(pc, instruction.src)?;
        }
        ebpf::ADD64_IMM => validate_register(pc, instruction.dst)?,
        ebpf::CALL_IMM | ebpf::EXIT => {}
        opcode => return Err(NativeCompileError::UnsupportedOpcode { pc, opcode }),
    }
    Ok(())
}

fn validate_register(pc: usize, register: u8) -> Result<(), NativeCompileError> {
    if register as usize <= ebpf::FRAME_PTR_REG {
        Ok(())
    } else {
        Err(NativeCompileError::InvalidRegister { pc, register })
    }
}

struct HelperIds {
    load: cranelift_module::FuncId,
    store: cranelift_module::FuncId,
    faulted: cranelift_module::FuncId,
    watchdog: cranelift_module::FuncId,
}

struct HelperRefs {
    load: FuncRef,
    store: FuncRef,
    faulted: FuncRef,
    watchdog: FuncRef,
}

fn declare_helpers(
    module: &mut JITModule,
    pointer_type: cranelift_codegen::ir::Type,
) -> Result<HelperIds, NativeCompileError> {
    let mut load_signature = module.make_signature();
    load_signature.params.push(AbiParam::new(pointer_type));
    load_signature.params.push(AbiParam::new(types::I64));
    load_signature.params.push(AbiParam::new(types::I32));
    load_signature.returns.push(AbiParam::new(types::I64));

    let mut store_signature = module.make_signature();
    store_signature.params.push(AbiParam::new(pointer_type));
    store_signature.params.push(AbiParam::new(types::I64));
    store_signature.params.push(AbiParam::new(types::I32));
    store_signature.params.push(AbiParam::new(types::I64));
    store_signature.returns.push(AbiParam::new(types::I32));

    let mut faulted_signature = module.make_signature();
    faulted_signature.params.push(AbiParam::new(pointer_type));
    faulted_signature.returns.push(AbiParam::new(types::I32));

    let mut watchdog_signature = module.make_signature();
    watchdog_signature.params.push(AbiParam::new(pointer_type));
    watchdog_signature.params.push(AbiParam::new(types::I64));
    watchdog_signature.returns.push(AbiParam::new(types::I32));

    Ok(HelperIds {
        load: module
            .declare_function(LOAD_SYMBOL, Linkage::Import, &load_signature)
            .map_err(codegen_error)?,
        store: module
            .declare_function(STORE_SYMBOL, Linkage::Import, &store_signature)
            .map_err(codegen_error)?,
        faulted: module
            .declare_function(FAULTED_SYMBOL, Linkage::Import, &faulted_signature)
            .map_err(codegen_error)?,
        watchdog: module
            .declare_function(WATCHDOG_SYMBOL, Linkage::Import, &watchdog_signature)
            .map_err(codegen_error)?,
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_sequence(
    builder: &mut FunctionBuilder<'_>,
    program: &DecodedProgram,
    helpers: &HelperRefs,
    invocation: Value,
    registers: &mut [Value; 11],
    start_pc: usize,
    depth: usize,
    max_call_depth: usize,
    frame_bump: i64,
    error_block: Block,
    active_calls: &mut Vec<usize>,
    emitted: &mut usize,
) -> Result<(), NativeCompileError> {
    let mut pc = start_pc;
    while let Some(instruction) = program.instructions.get(pc) {
        *emitted = emitted
            .checked_add(1)
            .ok_or(NativeCompileError::ExpansionLimit {
                limit: MAX_EXPANDED_INSTRUCTIONS,
            })?;
        if *emitted > MAX_EXPANDED_INSTRUCTIONS {
            return Err(NativeCompileError::ExpansionLimit {
                limit: MAX_EXPANDED_INSTRUCTIONS,
            });
        }
        emit_watchdog_tick(builder, helpers, invocation, error_block);

        let dst = instruction.dst as usize;
        let src = instruction.src as usize;
        match instruction.opc {
            ebpf::LD_B_REG | ebpf::LD_DW_REG => {
                let address = builder
                    .ins()
                    .iadd_imm_s(registers[src], i64::from(instruction.off));
                let width = if instruction.opc == ebpf::LD_B_REG {
                    1
                } else {
                    8
                };
                registers[dst] =
                    emit_checked_load(builder, helpers, invocation, address, width, error_block);
            }
            ebpf::ST_DW_REG => {
                let address = builder
                    .ins()
                    .iadd_imm_s(registers[dst], i64::from(instruction.off));
                emit_checked_store(
                    builder,
                    helpers,
                    invocation,
                    address,
                    8,
                    registers[src],
                    error_block,
                );
            }
            ebpf::MOV64_REG => registers[dst] = registers[src],
            ebpf::ADD64_IMM => {
                registers[dst] = builder.ins().iadd_imm_s(registers[dst], instruction.imm);
            }
            ebpf::ADD64_REG => {
                registers[dst] = builder.ins().iadd(registers[dst], registers[src]);
            }
            ebpf::CALL_IMM => {
                let target = program.calls[&pc];
                if active_calls.contains(&target) {
                    return Err(NativeCompileError::RecursiveCall { target });
                }
                let next_depth = depth.saturating_add(1);
                if next_depth >= max_call_depth {
                    return Err(NativeCompileError::CallDepthExceeded {
                        depth: next_depth,
                        maximum: max_call_depth,
                    });
                }
                let saved_nonvolatile = [
                    registers[6],
                    registers[7],
                    registers[8],
                    registers[9],
                    registers[10],
                ];
                registers[10] = builder.ins().iadd_imm_s(registers[10], frame_bump);
                active_calls.push(target);
                emit_sequence(
                    builder,
                    program,
                    helpers,
                    invocation,
                    registers,
                    target,
                    next_depth,
                    max_call_depth,
                    frame_bump,
                    error_block,
                    active_calls,
                    emitted,
                )?;
                active_calls.pop();
                registers[6..=10].copy_from_slice(&saved_nonvolatile);
            }
            ebpf::EXIT => return Ok(()),
            _ => unreachable!("all opcodes were fail-closed during decoding"),
        }
        pc = pc.saturating_add(1);
    }
    Err(NativeCompileError::ExecutionOverrun { start_pc })
}

fn emit_watchdog_tick(
    builder: &mut FunctionBuilder<'_>,
    helpers: &HelperRefs,
    invocation: Value,
    error_block: Block,
) {
    let one = builder.ins().iconst(types::I64, 1);
    let call = builder.ins().call(helpers.watchdog, &[invocation, one]);
    let status = builder.inst_results(call)[0];
    branch_on_error(builder, status, error_block);
}

fn emit_checked_load(
    builder: &mut FunctionBuilder<'_>,
    helpers: &HelperRefs,
    invocation: Value,
    address: Value,
    width: i64,
    error_block: Block,
) -> Value {
    let width = builder.ins().iconst(types::I32, width);
    let call = builder
        .ins()
        .call(helpers.load, &[invocation, address, width]);
    let value = builder.inst_results(call)[0];
    let faulted = builder.ins().call(helpers.faulted, &[invocation]);
    let status = builder.inst_results(faulted)[0];
    branch_on_error(builder, status, error_block);
    value
}

#[allow(clippy::too_many_arguments)]
fn emit_checked_store(
    builder: &mut FunctionBuilder<'_>,
    helpers: &HelperRefs,
    invocation: Value,
    address: Value,
    width: i64,
    value: Value,
    error_block: Block,
) {
    let width = builder.ins().iconst(types::I32, width);
    let call = builder
        .ins()
        .call(helpers.store, &[invocation, address, width, value]);
    let status = builder.inst_results(call)[0];
    branch_on_error(builder, status, error_block);
}

fn branch_on_error(builder: &mut FunctionBuilder<'_>, status: Value, error_block: Block) {
    let continuation = builder.create_block();
    let failed =
        builder
            .ins()
            .icmp_imm_u(cranelift_codegen::ir::condcodes::IntCC::NotEqual, status, 0);
    builder
        .ins()
        .brif(failed, error_block, &[], continuation, &[]);
    builder.switch_to_block(continuation);
    builder.seal_block(continuation);
}

fn codegen_error(error: impl std::fmt::Display) -> NativeCompileError {
    NativeCompileError::CraneliftCodegen(error.to_string())
}

fn classify_define_error(error: ModuleError) -> NativeCompileError {
    match error {
        error @ (ModuleError::Allocation { .. } | ModuleError::Backend(_)) => {
            NativeCompileError::NativeUnavailable(error.to_string())
        }
        error => NativeCompileError::CraneliftCodegen(error.to_string()),
    }
}

struct NativeInvocation {
    memory_mapping: *mut MemoryMapping,
    watchdog_remaining: u64,
    watchdog_consumed: u64,
    fault: Option<String>,
}

impl NativeInvocation {
    fn record_fault(&mut self, error: impl Into<String>) {
        if self.fault.is_none() {
            self.fault = Some(error.into());
        }
    }
}

extern "C" fn native_load(invocation: *mut NativeInvocation, address: u64, width: u32) -> u64 {
    let Some(invocation) = (unsafe { invocation.as_mut() }) else {
        return 0;
    };
    if !matches!(width, 1 | 8) {
        invocation.record_fault(format!("unsupported native load width {width}"));
        return 0;
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: `execute` holds the unique mutable mapping borrow for the
        // complete native call and generated code invokes helpers serially.
        let mapping = unsafe { &mut *invocation.memory_mapping };
        match width {
            1 => mapping.load::<u8>(address),
            8 => mapping.load::<u64>(address),
            _ => unreachable!("width validated before entering helper"),
        }
    }));
    match result {
        Ok(StableResult::Ok(value)) => value,
        Ok(StableResult::Err(error)) => {
            invocation.record_fault(error.to_string());
            0
        }
        Err(_) => {
            invocation.record_fault("native memory load helper panicked");
            0
        }
    }
}

extern "C" fn native_store(
    invocation: *mut NativeInvocation,
    address: u64,
    width: u32,
    value: u64,
) -> u32 {
    let Some(invocation) = (unsafe { invocation.as_mut() }) else {
        return 1;
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: see `native_load`.
        let mapping = unsafe { &mut *invocation.memory_mapping };
        match width {
            1 => mapping.store::<u8>(value as u8, address),
            8 => mapping.store::<u64>(value, address),
            _ => {
                invocation.record_fault(format!("unsupported native store width {width}"));
                StableResult::Ok(0)
            }
        }
    }));
    match result {
        Ok(StableResult::Ok(_)) if invocation.fault.is_none() => 0,
        Ok(StableResult::Ok(_)) => 1,
        Ok(StableResult::Err(error)) => {
            invocation.record_fault(error.to_string());
            1
        }
        Err(_) => {
            invocation.record_fault("native memory store helper panicked");
            1
        }
    }
}

extern "C" fn native_faulted(invocation: *mut NativeInvocation) -> u32 {
    let Some(invocation) = (unsafe { invocation.as_ref() }) else {
        return 1;
    };
    u32::from(invocation.fault.is_some())
}

extern "C" fn native_watchdog(invocation: *mut NativeInvocation, amount: u64) -> u32 {
    let Some(invocation) = (unsafe { invocation.as_mut() }) else {
        return 1;
    };
    if amount > invocation.watchdog_remaining {
        invocation.record_fault(format!(
            "native execution exceeded the {}-instruction safety watchdog",
            invocation
                .watchdog_consumed
                .saturating_add(invocation.watchdog_remaining)
        ));
        return 1;
    }
    invocation.watchdog_remaining -= amount;
    invocation.watchdog_consumed = invocation.watchdog_consumed.saturating_add(amount);
    0
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sbpf::{program::SBPFVersion, vm::Config},
    };

    fn instruction(opcode: u8, dst: u8, src: u8, off: i16, imm: i64) -> ebpf::Insn {
        ebpf::Insn {
            ptr: 0,
            opc: opcode,
            dst,
            src,
            off,
            imm,
        }
    }

    fn encode(instructions: impl IntoIterator<Item = ebpf::Insn>) -> Vec<u8> {
        instructions
            .into_iter()
            .flat_map(|instruction| instruction.to_array())
            .collect()
    }

    fn decode_error(
        text: &[u8],
        entry_pc: usize,
        functions: &HashMap<u32, usize>,
    ) -> NativeCompileError {
        match DecodedProgram::new(text, entry_pc, functions) {
            Ok(_) => panic!("expected strict decoder to reject the program"),
            Err(error) => error,
        }
    }

    #[test]
    fn strict_decoder_rejects_malformed_or_unresolved_programs() {
        let no_functions = HashMap::new();
        assert_eq!(
            decode_error(&[], 0, &no_functions),
            NativeCompileError::EmptyText
        );
        assert_eq!(
            decode_error(&[0; ebpf::INSN_SIZE - 1], 0, &no_functions),
            NativeCompileError::MisalignedText(ebpf::INSN_SIZE - 1)
        );

        let exit = encode([instruction(ebpf::EXIT, 0, 0, 0, 0)]);
        assert_eq!(
            decode_error(&exit, 1, &no_functions),
            NativeCompileError::InvalidEntrypoint {
                entry_pc: 1,
                instruction_count: 1,
            }
        );

        let branch = encode([
            instruction(ebpf::JA, 0, 0, -1, 0),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        assert_eq!(
            decode_error(&branch, 0, &no_functions),
            NativeCompileError::UnsupportedOpcode {
                pc: 0,
                opcode: ebpf::JA,
            }
        );

        let invalid_register = encode([
            instruction(ebpf::MOV64_REG, 11, 0, 0, 0),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        assert_eq!(
            decode_error(&invalid_register, 0, &no_functions),
            NativeCompileError::InvalidRegister {
                pc: 0,
                register: 11,
            }
        );

        let call = encode([
            instruction(ebpf::CALL_IMM, 0, 1, 0, 0x1234),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        assert_eq!(
            decode_error(&call, 0, &no_functions),
            NativeCompileError::UnresolvedCall { pc: 0, key: 0x1234 }
        );

        let outside_text = HashMap::from([(0x1234, 2)]);
        assert_eq!(
            decode_error(&call, 0, &outside_text),
            NativeCompileError::CallOutsideText { pc: 0, target: 2 }
        );
    }

    #[test]
    fn compile_rejects_recursive_and_overdeep_call_graphs() {
        let recursive = encode([
            instruction(ebpf::CALL_IMM, 0, 1, 0, 1),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        let recursive_functions = HashMap::from([(1, 0)]);
        let error =
            NativeProgram::compile(&recursive, 0, &recursive_functions, 4_096, true, 64, 100)
                .unwrap_err();
        assert_eq!(error, NativeCompileError::RecursiveCall { target: 0 });
        assert!(error.is_unsupported());

        // max_call_depth=2 permits one active callee, matching the upstream
        // VM, but the second nested call must be rejected before codegen.
        let overdeep = encode([
            instruction(ebpf::CALL_IMM, 0, 1, 0, 1),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
            instruction(ebpf::CALL_IMM, 0, 1, 0, 2),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        let overdeep_functions = HashMap::from([(1, 2), (2, 4)]);
        let error = NativeProgram::compile(&overdeep, 0, &overdeep_functions, 4_096, true, 2, 100)
            .unwrap_err();
        assert_eq!(
            error,
            NativeCompileError::CallDepthExceeded {
                depth: 2,
                maximum: 2,
            }
        );
        assert!(error.is_unsupported());
    }

    #[test]
    fn supported_straight_line_program_without_exit_falls_back() {
        // The upstream verifier permits this shape; execution, rather than
        // verification, reports the text overrun. The native subset must not
        // turn that canonical runtime failure into a deployment-time error.
        let no_exit = encode([instruction(ebpf::ADD64_IMM, 0, 0, 0, 1)]);
        let error =
            NativeProgram::compile(&no_exit, 0, &HashMap::new(), 4_096, true, 64, 100).unwrap_err();
        assert_eq!(error, NativeCompileError::ExecutionOverrun { start_pc: 0 });
        assert!(error.is_unsupported());
    }

    #[test]
    fn compile_bounds_exponential_call_dag_expansion() {
        // Each function calls the next function twice.  Only 37 source
        // instructions are needed to exceed the 4,096-instruction inlining
        // limit, which is the adversarial shape this bound is meant to stop.
        const CALL_LAYERS: usize = 12;
        let mut instructions = Vec::with_capacity(CALL_LAYERS * 3 + 1);
        let mut functions = HashMap::new();
        for layer in 0..CALL_LAYERS {
            let key = (layer + 1) as u32;
            let target = (layer + 1) * 3;
            functions.insert(key, target);
            instructions.push(instruction(ebpf::CALL_IMM, 0, 1, 0, i64::from(key)));
            instructions.push(instruction(ebpf::CALL_IMM, 0, 1, 0, i64::from(key)));
            instructions.push(instruction(ebpf::EXIT, 0, 0, 0, 0));
        }
        instructions.push(instruction(ebpf::EXIT, 0, 0, 0, 0));

        let error = NativeProgram::compile(
            &encode(instructions),
            0,
            &functions,
            4_096,
            true,
            64,
            100_000,
        )
        .unwrap_err();
        assert_eq!(
            error,
            NativeCompileError::ExpansionLimit {
                limit: MAX_EXPANDED_INSTRUCTIONS,
            }
        );
        assert!(error.is_unsupported());
    }

    #[test]
    fn emitted_watchdog_stops_a_bounded_native_program_at_its_limit() {
        let text = encode([
            instruction(ebpf::ADD64_IMM, 0, 0, 0, 1),
            instruction(ebpf::EXIT, 0, 0, 0, 0),
        ]);
        let program =
            NativeProgram::compile(&text, 0, &HashMap::new(), 4_096, true, 64, 1).unwrap();
        let config = Config::default();
        // SAFETY: there are no regions and this program performs no memory
        // operations, so the mapping contains no borrowed backing objects.
        let mut mapping = unsafe { MemoryMapping::new(vec![], &config, SBPFVersion::V0) }.unwrap();
        let error = program.execute(&mut mapping, 0, 0).unwrap_err();
        assert_eq!(
            error,
            "native execution exceeded the 1-instruction safety watchdog"
        );
    }
}

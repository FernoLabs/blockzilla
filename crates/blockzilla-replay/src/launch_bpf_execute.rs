//! Launch-era legacy BPF program invocation.
//!
//! This mirrors the v1.1.14 `BPFLoader` ABI: instruction accounts are packed
//! into one mutable input buffer, the deployed SBPFv0 program runs with r1 at
//! that buffer, and only fixed-size account data plus lamports are copied back.
//! The Bank's generic post-instruction verifier remains authoritative for the
//! resulting account mutation.

use std::cell::RefCell;

use hashbrown::HashMap;
use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    AccountData, AccountMap, AccountSnapshot, BPF_LOADER_PROGRAM_ID, CompiledProgram,
    ExecutionEngine, LaunchAccountMeta, LaunchBpfLoaderRent, ReplayCompiler,
};

const MAX_LAUNCH_BPF_INSTRUCTION_ACCOUNTS: usize = 256;
/// Do not let one pathological account set permanently pin an oversized
/// parameter allocation on every replay worker or nested invocation depth.
const MAX_RETAINED_PARAMETER_BUFFER_CAPACITY: usize = 16 * 1024 * 1024;

std::thread_local! {
    /// A lease removes its Vec from the thread-local pool while it is in use.
    /// Consequently a re-entrant CPI can acquire another buffer without
    /// holding a `RefCell` borrow or aliasing the caller's guest input.
    static LEGACY_BPF_PARAMETER_BUFFERS: RefCell<Vec<Vec<u8>>> =
        const { RefCell::new(Vec::new()) };
}

struct LegacyBpfParameterBufferLease {
    buffer: Option<Vec<u8>>,
}

impl LegacyBpfParameterBufferLease {
    fn acquire() -> Self {
        let mut buffer = LEGACY_BPF_PARAMETER_BUFFERS
            .with(|pool| pool.borrow_mut().pop())
            .unwrap_or_default();
        buffer.clear();
        Self {
            buffer: Some(buffer),
        }
    }

    fn get_mut(&mut self) -> &mut Vec<u8> {
        self.buffer
            .as_mut()
            .expect("parameter buffer lease owns its Vec before execution")
    }

    fn take(&mut self) -> Vec<u8> {
        self.buffer
            .take()
            .expect("parameter buffer lease transfers its Vec only once")
    }

    fn recycle(&mut self, buffer: Vec<u8>) {
        debug_assert!(self.buffer.is_none());
        self.buffer = Some(buffer);
    }
}

impl Drop for LegacyBpfParameterBufferLease {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            // Execution owns the buffer on this error path and will drop it.
            return;
        };
        return_parameter_buffer_to_pool(buffer, MAX_RETAINED_PARAMETER_BUFFER_CAPACITY);
    }
}

fn return_parameter_buffer_to_pool(mut buffer: Vec<u8>, maximum_capacity: usize) {
    if buffer.capacity() > maximum_capacity {
        return;
    }
    buffer.clear();
    LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow_mut().push(buffer));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchBpfExecutionMutation {
    pub program_account: [u8; 32],
    pub engine: ExecutionEngine,
    pub watchdog_instructions: u64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LaunchBpfExecutionError {
    #[error("legacy BPF program account {program_id:?} is absent from replay state")]
    MissingProgramAccount { program_id: [u8; 32] },
    #[error("legacy BPF program account {program_id:?} is not executable")]
    ProgramNotExecutable { program_id: [u8; 32] },
    #[error("legacy BPF program account {program_id:?} is owned by {owner:?}, expected BPFLoader")]
    WrongProgramOwner {
        program_id: [u8; 32],
        owner: [u8; 32],
    },
    #[error("legacy BPF instruction account {pubkey:?} is absent from replay state")]
    MissingAccountState { pubkey: [u8; 32] },
    #[error("legacy BPF parameter buffer length overflow")]
    ParameterLengthOverflow,
    #[error("legacy BPF instruction has {count} accounts; maximum is {max}")]
    TooManyInstructionAccounts { count: usize, max: usize },
    #[error("legacy BPF parameter buffer is malformed after execution")]
    MalformedParameterBuffer,
    #[error("legacy BPF execution failed: {message}")]
    Execute { message: String },
    #[error("legacy BPF program returned instruction status {status}")]
    ProgramReturnedError { status: u64 },
    #[error("legacy BPF program {program_id:?} cannot change owner of account {pubkey:?}")]
    ModifiedProgramId {
        program_id: [u8; 32],
        pubkey: [u8; 32],
    },
    #[error("legacy BPF program {program_id:?} cannot spend lamports from account {pubkey:?}")]
    ExternalAccountLamportSpend {
        program_id: [u8; 32],
        pubkey: [u8; 32],
    },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: [u8; 32] },
    #[error("executable account {pubkey:?} changed lamports")]
    ExecutableLamportChange { pubkey: [u8; 32] },
    #[error("legacy BPF program resized account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: [u8; 32] },
    #[error("legacy BPF program changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: [u8; 32] },
    #[error("executable account {pubkey:?} changed data")]
    ExecutableDataModified { pubkey: [u8; 32] },
    #[error(
        "legacy BPF program made account {pubkey:?} executable without rent exemption: balance={balance}, minimum={minimum}"
    )]
    ExecutableAccountNotRentExempt {
        pubkey: [u8; 32],
        balance: u64,
        minimum: u64,
    },
    #[error("legacy BPF program made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: [u8; 32] },
    #[error("legacy BPF program changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: [u8; 32] },
    #[error("legacy BPF instruction is unbalanced: pre={pre_lamports}, post={post_lamports}")]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
}

impl LaunchBpfExecutionError {
    /// Errors produced by guest execution or the Bank verifier are ordinary
    /// instruction failures. Environment/account-shape errors remain replay
    /// gaps and must stop the diagnostic chain.
    pub(crate) fn is_historical_instruction_failure(&self) -> bool {
        !matches!(
            self,
            Self::MissingProgramAccount { .. }
                | Self::ProgramNotExecutable { .. }
                | Self::WrongProgramOwner { .. }
                | Self::MissingAccountState { .. }
                | Self::ParameterLengthOverflow
                | Self::TooManyInstructionAccounts { .. }
                | Self::MalformedParameterBuffer
        )
    }
}

pub fn validate_launch_bpf_program_account(
    program_id: [u8; 32],
    account: Option<&AccountSnapshot>,
) -> Result<&AccountSnapshot, LaunchBpfExecutionError> {
    let account = account.ok_or(LaunchBpfExecutionError::MissingProgramAccount { program_id })?;
    if !account.executable {
        return Err(LaunchBpfExecutionError::ProgramNotExecutable { program_id });
    }
    if account.owner != BPF_LOADER_PROGRAM_ID {
        return Err(LaunchBpfExecutionError::WrongProgramOwner {
            program_id,
            owner: account.owner,
        });
    }
    Ok(account)
}

pub fn apply_launch_bpf_program_instruction(
    program_id: [u8; 32],
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    compiler: &ReplayCompiler,
    compiled_program: &CompiledProgram,
    bank_rent: LaunchBpfLoaderRent,
) -> Result<LaunchBpfExecutionMutation, LaunchBpfExecutionError> {
    apply_launch_bpf_program_instruction_with_stack(
        program_id,
        instruction_data,
        account_metas,
        accounts,
        compiler,
        compiled_program,
        bank_rent,
        SmallVec::from_slice(&[program_id]),
    )
}

pub(crate) fn apply_launch_bpf_program_instruction_with_stack(
    program_id: [u8; 32],
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    compiler: &ReplayCompiler,
    compiled_program: &CompiledProgram,
    bank_rent: LaunchBpfLoaderRent,
    program_stack: SmallVec<[[u8; 32]; 5]>,
) -> Result<LaunchBpfExecutionMutation, LaunchBpfExecutionError> {
    let plan = LaunchBpfSerializationPlan::collect(account_metas, accounts)?;
    let mut parameter_buffer = LegacyBpfParameterBufferLease::acquire();
    let pre_accounts = serialize_parameters_and_baselines_with_plan(
        program_id,
        account_metas,
        accounts,
        instruction_data,
        &plan,
        parameter_buffer.get_mut(),
    )?;
    let mut execution = compiler
        .execute_replay_program_with_stack(
            compiled_program,
            parameter_buffer.take(),
            bank_rent,
            program_stack,
            pre_accounts,
        )
        .map_err(|error| LaunchBpfExecutionError::Execute {
            message: error.to_string(),
        })?;
    parameter_buffer.recycle(std::mem::take(&mut execution.input_after));
    if execution.return_value != 0 {
        return Err(LaunchBpfExecutionError::ProgramReturnedError {
            status: execution.return_value,
        });
    }
    deserialize_parameters_with_plan(account_metas, accounts, parameter_buffer.get_mut(), &plan)?;
    verify_launch_bpf_instruction(
        program_id,
        &execution.verifier_baselines,
        accounts,
        bank_rent,
    )?;
    let mutation = LaunchBpfExecutionMutation {
        program_account: program_id,
        engine: execution.engine,
        watchdog_instructions: execution.watchdog_instructions,
    };
    Ok(mutation)
}

const INLINE_SERIALIZATION_PLAN_ACCOUNTS: usize = 16;

/// Collected launch-era account layout for one invocation.
///
/// A transaction may mention the same account multiple times, but the packed
/// ABI serializes its state only at the first position.  Keeping that decision
/// and the resulting byte offsets here avoids rediscovering duplicates during
/// sizing, serialization, verifier-baseline preparation, and copyback.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchBpfSerializationPlan {
    entries: SmallVec<[LaunchBpfSerializationEntry; INLINE_SERIALIZATION_PLAN_ACCOUNTS]>,
    account_region_end: usize,
    unique_account_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchBpfSerializationEntry {
    Duplicate {
        first_position: u8,
    },
    Unique {
        meta_index: usize,
        lamports_offset: usize,
        data_offset: usize,
        data_len: usize,
    },
}

impl LaunchBpfSerializationPlan {
    fn collect(
        account_metas: &[LaunchAccountMeta],
        accounts: &AccountMap,
    ) -> Result<Self, LaunchBpfExecutionError> {
        if account_metas.len() > MAX_LAUNCH_BPF_INSTRUCTION_ACCOUNTS {
            return Err(LaunchBpfExecutionError::TooManyInstructionAccounts {
                count: account_metas.len(),
                max: MAX_LAUNCH_BPF_INSTRUCTION_ACCOUNTS,
            });
        }
        let mut entries = SmallVec::with_capacity(account_metas.len());
        // A hash table costs more than the bounded scan for normal small
        // instructions.  Keep those positions inline, and switch to O(1)
        // lookup when the account list no longer fits in the plan's inline
        // storage.  Either path discovers duplicates only once per invocation.
        let mut inline_first_positions =
            SmallVec::<[([u8; 32], usize); INLINE_SERIALIZATION_PLAN_ACCOUNTS]>::new();
        let mut hashed_first_positions = (account_metas.len() > INLINE_SERIALIZATION_PLAN_ACCOUNTS)
            .then(|| HashMap::with_capacity(account_metas.len()));
        let mut account_region_end = 8_usize;
        let mut unique_account_count = 0_usize;

        for (meta_index, meta) in account_metas.iter().enumerate() {
            let first_position = if let Some(first_positions) = &hashed_first_positions {
                first_positions.get(&meta.pubkey).copied()
            } else {
                inline_first_positions
                    .iter()
                    .find_map(|(pubkey, position)| (*pubkey == meta.pubkey).then_some(*position))
            };
            if let Some(first_position) = first_position {
                entries.push(LaunchBpfSerializationEntry::Duplicate {
                    // The legacy ABI stores duplicate positions in one byte.
                    first_position: first_position as u8,
                });
                account_region_end = account_region_end
                    .checked_add(1)
                    .ok_or(LaunchBpfExecutionError::ParameterLengthOverflow)?;
                continue;
            }

            let account = required_account(accounts, meta.pubkey)?;
            let lamports_offset = account_region_end
                .checked_add(35)
                .ok_or(LaunchBpfExecutionError::ParameterLengthOverflow)?;
            let data_offset = lamports_offset
                .checked_add(16)
                .ok_or(LaunchBpfExecutionError::ParameterLengthOverflow)?;
            account_region_end = account_region_end
                .checked_add(92)
                .and_then(|value| value.checked_add(account.data.len()))
                .ok_or(LaunchBpfExecutionError::ParameterLengthOverflow)?;
            if let Some(first_positions) = &mut hashed_first_positions {
                first_positions.insert(meta.pubkey, meta_index);
            } else {
                inline_first_positions.push((meta.pubkey, meta_index));
            }
            entries.push(LaunchBpfSerializationEntry::Unique {
                meta_index,
                lamports_offset,
                data_offset,
                data_len: account.data.len(),
            });
            unique_account_count += 1;
        }

        Ok(Self {
            entries,
            account_region_end,
            unique_account_count,
        })
    }

    fn serialized_len(
        &self,
        instruction_data_len: usize,
    ) -> Result<usize, LaunchBpfExecutionError> {
        self.account_region_end
            .checked_add(8)
            .and_then(|value| value.checked_add(instruction_data_len))
            .and_then(|value| value.checked_add(32))
            .ok_or(LaunchBpfExecutionError::ParameterLengthOverflow)
    }
}

#[cfg(test)]
pub(crate) fn serialize_parameters(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    instruction_data: &[u8],
) -> Result<Vec<u8>, LaunchBpfExecutionError> {
    let plan = LaunchBpfSerializationPlan::collect(account_metas, accounts)?;
    let mut bytes = Vec::new();
    serialize_parameters_with_plan(
        program_id,
        account_metas,
        accounts,
        instruction_data,
        &plan,
        &mut bytes,
    )?;
    Ok(bytes)
}

#[cfg(test)]
fn serialize_parameters_with_plan(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    instruction_data: &[u8],
    plan: &LaunchBpfSerializationPlan,
    bytes: &mut Vec<u8>,
) -> Result<(), LaunchBpfExecutionError> {
    serialize_parameters_with_plan_inner(
        program_id,
        account_metas,
        accounts,
        instruction_data,
        plan,
        bytes,
        None,
    )
}

fn serialize_parameters_and_baselines_with_plan(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    instruction_data: &[u8],
    plan: &LaunchBpfSerializationPlan,
    bytes: &mut Vec<u8>,
) -> Result<LaunchPreAccounts, LaunchBpfExecutionError> {
    let mut pre_accounts = LaunchPreAccounts::with_capacity(plan.unique_account_count);
    serialize_parameters_with_plan_inner(
        program_id,
        account_metas,
        accounts,
        instruction_data,
        plan,
        bytes,
        Some(&mut pre_accounts),
    )?;
    Ok(pre_accounts)
}

fn serialize_parameters_with_plan_inner(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    instruction_data: &[u8],
    plan: &LaunchBpfSerializationPlan,
    bytes: &mut Vec<u8>,
    mut pre_accounts: Option<&mut LaunchPreAccounts>,
) -> Result<(), LaunchBpfExecutionError> {
    let capacity = plan.serialized_len(instruction_data.len())?;
    bytes.clear();
    bytes.reserve(capacity);
    push_u64(bytes, account_metas.len() as u64);
    for entry in &plan.entries {
        match *entry {
            LaunchBpfSerializationEntry::Duplicate { first_position } => {
                bytes.push(first_position);
            }
            LaunchBpfSerializationEntry::Unique {
                meta_index,
                data_len,
                ..
            } => {
                let meta = &account_metas[meta_index];
                let account = required_account(accounts, meta.pubkey)?;
                debug_assert_eq!(account.data.len(), data_len);
                if let Some(pre_accounts) = &mut pre_accounts {
                    pre_accounts.push(LaunchPreAccount::new(program_id, meta, account));
                }
                bytes.push(u8::MAX);
                bytes.push(u8::from(meta.is_signer));
                bytes.push(u8::from(meta.is_writable));
                bytes.extend_from_slice(&meta.pubkey);
                push_u64(bytes, account.lamports);
                push_u64(bytes, account.data.len() as u64);
                bytes.extend_from_slice(&account.data);
                bytes.extend_from_slice(&account.owner);
                bytes.push(u8::from(account.executable));
                push_u64(bytes, account.rent_epoch);
            }
        }
    }
    push_u64(bytes, instruction_data.len() as u64);
    bytes.extend_from_slice(instruction_data);
    bytes.extend_from_slice(&program_id);
    debug_assert_eq!(bytes.len(), capacity);
    Ok(())
}

#[cfg(test)]
fn deserialize_parameters(
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    buffer: &[u8],
) -> Result<(), LaunchBpfExecutionError> {
    let plan = LaunchBpfSerializationPlan::collect(account_metas, accounts)?;
    deserialize_parameters_with_plan(account_metas, accounts, buffer, &plan)
}

fn deserialize_parameters_with_plan(
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    buffer: &[u8],
    plan: &LaunchBpfSerializationPlan,
) -> Result<(), LaunchBpfExecutionError> {
    for entry in &plan.entries {
        let LaunchBpfSerializationEntry::Unique {
            meta_index,
            lamports_offset,
            data_offset,
            data_len,
        } = *entry
        else {
            continue;
        };
        let meta = &account_metas[meta_index];
        let lamports = read_u64(buffer, lamports_offset)?;
        let data_end = data_offset
            .checked_add(data_len)
            .ok_or(LaunchBpfExecutionError::MalformedParameterBuffer)?;
        let data = buffer
            .get(data_offset..data_end)
            .ok_or(LaunchBpfExecutionError::MalformedParameterBuffer)?;
        let account =
            accounts
                .get_mut(&meta.pubkey)
                .ok_or(LaunchBpfExecutionError::MissingAccountState {
                    pubkey: meta.pubkey,
                })?;
        if account.lamports != lamports {
            account.lamports = lamports;
        }
        // AccountData is copy-on-write. Avoid mutable access for the common
        // no-op case so a shared payload stays shared through VM copyback.
        if account.data.as_slice() != data {
            account.data.set_from_slice(data);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaunchPreAccount {
    pubkey: [u8; 32],
    is_signer: bool,
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<AccountData>,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
}

/// Verifier state follows the same small-account fast path as the serialized
/// ABI plan.  Normal instructions keep every baseline inline; unusually wide
/// instructions spill transparently without changing verification semantics.
pub(crate) type LaunchPreAccounts =
    SmallVec<[LaunchPreAccount; INLINE_SERIALIZATION_PLAN_ACCOUNTS]>;

impl LaunchPreAccount {
    pub(crate) fn pubkey(&self) -> [u8; 32] {
        self.pubkey
    }

    pub(crate) fn is_signer(&self) -> bool {
        self.is_signer
    }

    pub(crate) fn is_writable(&self) -> bool {
        self.is_writable
    }

    pub(crate) fn data_len(&self) -> usize {
        self.data_len
    }

    pub(crate) fn owner(&self) -> [u8; 32] {
        self.owner
    }

    pub(crate) fn executable(&self) -> bool {
        self.executable
    }

    pub(crate) fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    fn new(program_id: [u8; 32], meta: &LaunchAccountMeta, account: &AccountSnapshot) -> Self {
        Self {
            pubkey: meta.pubkey,
            is_signer: meta.is_signer,
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
        }
    }

    fn verify(
        &self,
        program_id: [u8; 32],
        post: &AccountSnapshot,
        bank_rent: LaunchBpfLoaderRent,
    ) -> Result<(), LaunchBpfExecutionError> {
        if self.owner != post.owner
            && (!self.is_writable
                || program_id != self.owner
                || !post.data.iter().all(|byte| *byte == 0))
        {
            return Err(LaunchBpfExecutionError::ModifiedProgramId {
                program_id,
                pubkey: self.pubkey,
            });
        }
        if program_id != self.owner && self.lamports > post.lamports {
            return Err(LaunchBpfExecutionError::ExternalAccountLamportSpend {
                program_id,
                pubkey: self.pubkey,
            });
        }
        if self.lamports != post.lamports {
            if !self.is_writable {
                return Err(LaunchBpfExecutionError::ReadonlyLamportChange {
                    pubkey: self.pubkey,
                });
            }
            if self.executable {
                return Err(LaunchBpfExecutionError::ExecutableLamportChange {
                    pubkey: self.pubkey,
                });
            }
        }
        if self.data_len != post.data.len() {
            return Err(LaunchBpfExecutionError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }
        if should_verify_data(self.owner, program_id, self.is_writable, self.executable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.executable {
                LaunchBpfExecutionError::ExecutableDataModified {
                    pubkey: self.pubkey,
                }
            } else if self.is_writable {
                LaunchBpfExecutionError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchBpfExecutionError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }
        if self.executable != post.executable {
            let minimum = bank_rent.minimum_balance(post.data.len());
            if post.lamports < minimum {
                return Err(LaunchBpfExecutionError::ExecutableAccountNotRentExempt {
                    pubkey: self.pubkey,
                    balance: post.lamports,
                    minimum,
                });
            }
            if !self.is_writable || self.executable || program_id != self.owner {
                return Err(LaunchBpfExecutionError::ExecutableModified {
                    pubkey: self.pubkey,
                });
            }
        }
        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchBpfExecutionError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }
        Ok(())
    }

    /// Attribute a successful writable child-program change to that child.
    ///
    /// The caller's final verifier must compare externally owned state with
    /// the state returned by CPI, not with the state that existed before the
    /// caller started. Legacy BPF writeback can change only lamports and
    /// fixed-size data, so owner/executable/rent metadata remains anchored to
    /// the original instruction baseline.
    pub(crate) fn adopt_cpi_post(&mut self, post: &AccountSnapshot) {
        self.lamports = post.lamports;
        debug_assert_eq!(self.data_len, post.data.len());
        if let Some(data) = &mut self.data {
            *data = post.data.clone();
        }
    }
}

#[cfg(test)]
pub(crate) fn launch_pre_accounts(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
) -> Result<LaunchPreAccounts, LaunchBpfExecutionError> {
    let plan = LaunchBpfSerializationPlan::collect(account_metas, accounts)?;
    launch_pre_accounts_with_plan(program_id, account_metas, accounts, &plan)
}

#[cfg(test)]
fn launch_pre_accounts_with_plan(
    program_id: [u8; 32],
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
    plan: &LaunchBpfSerializationPlan,
) -> Result<LaunchPreAccounts, LaunchBpfExecutionError> {
    let mut pre_accounts = LaunchPreAccounts::with_capacity(plan.unique_account_count);
    for entry in &plan.entries {
        let LaunchBpfSerializationEntry::Unique { meta_index, .. } = *entry else {
            continue;
        };
        let meta = &account_metas[meta_index];
        pre_accounts.push(LaunchPreAccount::new(
            program_id,
            meta,
            required_account(accounts, meta.pubkey)?,
        ));
    }
    Ok(pre_accounts)
}

pub(crate) fn verify_launch_bpf_instruction(
    program_id: [u8; 32],
    pre_accounts: &[LaunchPreAccount],
    accounts: &AccountMap,
    bank_rent: LaunchBpfLoaderRent,
) -> Result<(), LaunchBpfExecutionError> {
    let mut pre_lamports = 0_u128;
    let mut post_lamports = 0_u128;
    for pre in pre_accounts {
        let post = required_account(accounts, pre.pubkey)?;
        pre.verify(program_id, post, bank_rent)?;
        pre_lamports += u128::from(pre.lamports);
        post_lamports += u128::from(post.lamports);
    }
    if pre_lamports != post_lamports {
        return Err(LaunchBpfExecutionError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
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

fn required_account(
    accounts: &AccountMap,
    pubkey: [u8; 32],
) -> Result<&AccountSnapshot, LaunchBpfExecutionError> {
    accounts
        .get(&pubkey)
        .ok_or(LaunchBpfExecutionError::MissingAccountState { pubkey })
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64, LaunchBpfExecutionError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(start..start.saturating_add(8))
            .ok_or(LaunchBpfExecutionError::MalformedParameterBuffer)?
            .try_into()
            .expect("checked u64 parameter length"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROGRAM: [u8; 32] = [9; 32];
    const OWNED: [u8; 32] = [7; 32];
    const EXTERNAL: [u8; 32] = [8; 32];

    fn meta(pubkey: [u8; 32], writable: bool) -> LaunchAccountMeta {
        LaunchAccountMeta {
            pubkey,
            is_signer: false,
            is_writable: writable,
        }
    }

    fn account(owner: [u8; 32], lamports: u64, data: &[u8]) -> AccountSnapshot {
        AccountSnapshot {
            lamports,
            owner,
            executable: false,
            rent_epoch: 3,
            data: data.to_vec().into(),
        }
    }

    fn clear_parameter_buffer_pool() {
        LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow_mut().clear());
    }

    #[test]
    fn pooled_parameter_serialization_matches_fresh_abi_and_reuses_output() {
        clear_parameter_buffer_pool();
        let metas = [meta(OWNED, true), meta(OWNED, true), meta(EXTERNAL, false)];
        let accounts = AccountMap::from([
            (OWNED, account(PROGRAM, 11, &[1, 2, 3])),
            (EXTERNAL, account([6; 32], 12, &[4, 5])),
        ]);
        let instruction_data = [0xaa, 0xbb];
        let expected = serialize_parameters(PROGRAM, &metas, &accounts, &instruction_data).unwrap();
        let plan = LaunchBpfSerializationPlan::collect(&metas, &accounts).unwrap();

        let mut lease = LegacyBpfParameterBufferLease::acquire();
        lease.get_mut().extend_from_slice(&[0xff; 17]);
        serialize_parameters_with_plan(
            PROGRAM,
            &metas,
            &accounts,
            &instruction_data,
            &plan,
            lease.get_mut(),
        )
        .unwrap();
        assert_eq!(lease.get_mut().as_slice(), expected);
        let allocation = lease.get_mut().as_ptr();

        // Model the ownership round-trip through ReplayCompiler.
        let execution_output = lease.take();
        assert!(LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow().is_empty()));
        lease.recycle(execution_output);
        drop(lease);

        let mut reused = LegacyBpfParameterBufferLease::acquire();
        assert!(reused.get_mut().is_empty());
        assert_eq!(reused.get_mut().as_ptr(), allocation);
        drop(reused);
        clear_parameter_buffer_pool();
    }

    #[test]
    fn fused_serialization_builds_identical_verifier_baselines() {
        let metas = [meta(OWNED, true), meta(OWNED, true), meta(EXTERNAL, false)];
        let accounts = AccountMap::from([
            (OWNED, account(PROGRAM, 11, &[1, 2, 3])),
            (EXTERNAL, account([6; 32], 12, &[4, 5])),
        ]);
        let instruction_data = [0xaa, 0xbb];
        let plan = LaunchBpfSerializationPlan::collect(&metas, &accounts).unwrap();
        let expected_bytes =
            serialize_parameters(PROGRAM, &metas, &accounts, &instruction_data).unwrap();
        let expected_baselines = launch_pre_accounts(PROGRAM, &metas, &accounts).unwrap();
        let mut bytes = Vec::new();

        let baselines = serialize_parameters_and_baselines_with_plan(
            PROGRAM,
            &metas,
            &accounts,
            &instruction_data,
            &plan,
            &mut bytes,
        )
        .unwrap();

        assert_eq!(bytes, expected_bytes);
        assert_eq!(baselines, expected_baselines);
        assert_eq!(baselines.len(), 2);
        assert!(!baselines.spilled());
    }

    #[test]
    fn nested_parameter_leases_never_alias_the_outer_execution_buffer() {
        clear_parameter_buffer_pool();
        let mut outer_lease = LegacyBpfParameterBufferLease::acquire();
        outer_lease.get_mut().extend_from_slice(&[1; 64]);
        let mut outer_execution_buffer = outer_lease.take();
        let outer_allocation = outer_execution_buffer.as_mut_ptr();

        let mut nested_lease = LegacyBpfParameterBufferLease::acquire();
        nested_lease.get_mut().extend_from_slice(&[2; 64]);
        assert_ne!(nested_lease.get_mut().as_mut_ptr(), outer_allocation);
        assert_eq!(outer_execution_buffer, [1; 64]);

        let nested_execution_buffer = nested_lease.take();
        nested_lease.recycle(nested_execution_buffer);
        drop(nested_lease);
        outer_lease.recycle(outer_execution_buffer);
        drop(outer_lease);

        assert_eq!(
            LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow().len()),
            2
        );
        clear_parameter_buffer_pool();
    }

    #[test]
    fn parameter_pool_drops_buffers_above_the_retention_cap() {
        clear_parameter_buffer_pool();
        let retained = Vec::with_capacity(64);
        let test_cap = retained.capacity();
        return_parameter_buffer_to_pool(retained, test_cap);
        assert_eq!(
            LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow().len()),
            1
        );

        clear_parameter_buffer_pool();
        let oversized = Vec::with_capacity(test_cap + 1);
        assert!(oversized.capacity() > test_cap);
        return_parameter_buffer_to_pool(oversized, test_cap);
        assert!(LEGACY_BPF_PARAMETER_BUFFERS.with(|pool| pool.borrow().is_empty()));
    }

    #[test]
    fn launch_parameter_abi_is_packed_and_duplicate_uses_one_byte() {
        let metas = [meta(OWNED, true), meta(OWNED, true), meta(EXTERNAL, false)];
        let accounts = AccountMap::from([
            (OWNED, account(PROGRAM, 11, &[1, 2, 3])),
            (EXTERNAL, account([6; 32], 12, &[4, 5])),
        ]);
        let bytes = serialize_parameters(PROGRAM, &metas, &accounts, &[0xaa, 0xbb]).unwrap();

        assert_eq!(u64::from_le_bytes(bytes[..8].try_into().unwrap()), 3);
        assert_eq!(bytes[8], u8::MAX);
        let first_len = 92 + 3;
        assert_eq!(bytes[8 + first_len], 0);
        assert_eq!(bytes.len(), 8 + first_len + 1 + (92 + 2) + 8 + 2 + 32);
        assert_eq!(&bytes[bytes.len() - 32..], &PROGRAM);
    }

    #[test]
    fn launch_parameter_plan_collects_duplicate_and_packed_offsets_once() {
        let metas = [meta(OWNED, true), meta(OWNED, true), meta(EXTERNAL, false)];
        let accounts = AccountMap::from([
            (OWNED, account(PROGRAM, 11, &[1, 2, 3])),
            (EXTERNAL, account([6; 32], 12, &[4, 5])),
        ]);

        let plan = LaunchBpfSerializationPlan::collect(&metas, &accounts).unwrap();

        assert_eq!(plan.unique_account_count, 2);
        assert_eq!(plan.account_region_end, 198);
        assert_eq!(plan.serialized_len(2).unwrap(), 240);
        assert_eq!(
            plan.entries.as_slice(),
            &[
                LaunchBpfSerializationEntry::Unique {
                    meta_index: 0,
                    lamports_offset: 43,
                    data_offset: 59,
                    data_len: 3,
                },
                LaunchBpfSerializationEntry::Duplicate { first_position: 0 },
                LaunchBpfSerializationEntry::Unique {
                    meta_index: 2,
                    lamports_offset: 139,
                    data_offset: 155,
                    data_len: 2,
                },
            ]
        );
    }

    #[test]
    fn launch_parameter_plan_enforces_legacy_u8_account_boundary() {
        let accounts = (0_u16..=u8::MAX.into())
            .map(|index| {
                let pubkey = [index as u8; 32];
                (pubkey, account(PROGRAM, 1, &[]))
            })
            .collect::<AccountMap>();
        let metas = (0_u16..=u8::MAX.into())
            .map(|index| meta([index as u8; 32], true))
            .collect::<Vec<_>>();

        let plan = LaunchBpfSerializationPlan::collect(&metas, &accounts).unwrap();
        assert_eq!(plan.entries.len(), 256);
        assert!(matches!(
            plan.entries.last(),
            Some(LaunchBpfSerializationEntry::Unique {
                meta_index: 255,
                ..
            })
        ));

        let mut duplicate_at_last = metas.clone();
        duplicate_at_last[255] = duplicate_at_last[254].clone();
        let plan = LaunchBpfSerializationPlan::collect(&duplicate_at_last, &accounts).unwrap();
        assert_eq!(
            plan.entries.last(),
            Some(&LaunchBpfSerializationEntry::Duplicate {
                first_position: 254
            })
        );

        let mut too_many = metas;
        too_many.push(meta(OWNED, true));
        assert_eq!(
            LaunchBpfSerializationPlan::collect(&too_many, &accounts),
            Err(LaunchBpfExecutionError::TooManyInstructionAccounts {
                count: 257,
                max: 256,
            })
        );
    }

    #[test]
    fn verifier_baselines_are_inline_for_common_instructions_and_spill_when_wide() {
        fn fixture(count: usize) -> (Vec<LaunchAccountMeta>, AccountMap) {
            let metas = (0..count)
                .map(|index| meta([index as u8; 32], true))
                .collect::<Vec<_>>();
            let accounts = (0..count)
                .map(|index| {
                    (
                        [index as u8; 32],
                        account(PROGRAM, index as u64 + 1, &[index as u8]),
                    )
                })
                .collect::<AccountMap>();
            (metas, accounts)
        }

        let (inline_metas, inline_accounts) = fixture(INLINE_SERIALIZATION_PLAN_ACCOUNTS);
        let inline = launch_pre_accounts(PROGRAM, &inline_metas, &inline_accounts).unwrap();
        assert_eq!(inline.len(), INLINE_SERIALIZATION_PLAN_ACCOUNTS);
        assert!(!inline.spilled());

        let (wide_metas, wide_accounts) = fixture(INLINE_SERIALIZATION_PLAN_ACCOUNTS + 1);
        let wide = launch_pre_accounts(PROGRAM, &wide_metas, &wide_accounts).unwrap();
        assert_eq!(wide.len(), INLINE_SERIALIZATION_PLAN_ACCOUNTS + 1);
        assert!(wide.spilled());
    }

    #[test]
    fn no_op_copyback_keeps_account_data_shared() {
        let metas = [meta(OWNED, true)];
        let mut accounts = AccountMap::from([(OWNED, account(PROGRAM, 11, &[1, 2, 3]))]);
        let shared_before = accounts[&OWNED].data.clone();
        let bytes = serialize_parameters(PROGRAM, &metas, &accounts, &[]).unwrap();

        deserialize_parameters(&metas, &mut accounts, &bytes).unwrap();

        assert!(accounts[&OWNED].data.shares_allocation_with(&shared_before));
    }

    #[test]
    fn deserializer_copies_back_only_lamports_and_fixed_data() {
        let metas = [meta(OWNED, true)];
        let mut accounts = AccountMap::from([(OWNED, account(PROGRAM, 11, &[1, 2, 3]))]);
        let shared_before = accounts[&OWNED].data.clone();
        let mut bytes = serialize_parameters(PROGRAM, &metas, &accounts, &[]).unwrap();
        let lamports_start = 8 + 1 + 1 + 1 + 32;
        bytes[lamports_start..lamports_start + 8].copy_from_slice(&19_u64.to_le_bytes());
        let data_start = lamports_start + 16;
        bytes[data_start..data_start + 3].copy_from_slice(&[7, 8, 9]);
        let owner_start = data_start + 3;
        bytes[owner_start..owner_start + 32].fill(0xff);

        deserialize_parameters(&metas, &mut accounts, &bytes).unwrap();

        assert_eq!(accounts[&OWNED].lamports, 19);
        assert_eq!(accounts[&OWNED].data, [7, 8, 9]);
        assert!(!accounts[&OWNED].data.shares_allocation_with(&shared_before));
        assert_eq!(accounts[&OWNED].owner, PROGRAM);
        assert_eq!(accounts[&OWNED].rent_epoch, 3);
    }

    #[test]
    fn post_verifier_allows_owned_writable_data_but_balances_lamports() {
        let metas = [meta(OWNED, true), meta(EXTERNAL, true)];
        let mut accounts = AccountMap::from([
            (OWNED, account(PROGRAM, 11, &[1, 2, 3])),
            (EXTERNAL, account([6; 32], 12, &[4, 5])),
        ]);
        let pre = launch_pre_accounts(PROGRAM, &metas, &accounts).unwrap();
        accounts.get_mut(&OWNED).unwrap().data[0] = 9;
        accounts.get_mut(&OWNED).unwrap().lamports = 10;
        accounts.get_mut(&EXTERNAL).unwrap().lamports = 13;

        verify_launch_bpf_instruction(
            PROGRAM,
            &pre,
            &accounts,
            LaunchBpfLoaderRent {
                lamports_per_byte_year: 1,
                exemption_threshold: 2.0,
            },
        )
        .unwrap();
    }

    #[test]
    fn post_verifier_rejects_external_data_change_before_balance_check() {
        let metas = [meta(EXTERNAL, true)];
        let mut accounts = AccountMap::from([(EXTERNAL, account([6; 32], 12, &[4, 5]))]);
        let pre = launch_pre_accounts(PROGRAM, &metas, &accounts).unwrap();
        accounts.get_mut(&EXTERNAL).unwrap().data[0] = 9;

        assert_eq!(
            verify_launch_bpf_instruction(
                PROGRAM,
                &pre,
                &accounts,
                LaunchBpfLoaderRent {
                    lamports_per_byte_year: 1,
                    exemption_threshold: 2.0,
                },
            ),
            Err(LaunchBpfExecutionError::ExternalAccountDataModified { pubkey: EXTERNAL })
        );
    }
}

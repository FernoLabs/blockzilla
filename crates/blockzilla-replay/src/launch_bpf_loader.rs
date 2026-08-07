//! Launch-era native legacy BPF-loader deployment primitives.
//!
//! `Write` follows the behavior shared by Solana v1.0.7 and the v1.1.14 Stable
//! epoch-34 activation runtime. `Finalize` selects the historical profile:
//! v1.0.7 consumes an explicit Rent account, while the epoch-34 profile uses
//! Bank Rent. Program invocation is deliberately kept separate from deployment
//! so the replay dispatcher can cache a derived native artifact only after the
//! containing transaction commits.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AccountMap, AccountSnapshot, CompilationManifest, CompiledProgram, LaunchAccountMeta,
    LoaderAccountKind, RENT_SYSVAR_ID, ReplayCompiler, extract_program,
};

/// `BPFLoader1111111111111111111111111111111111`.
pub const BPF_LOADER_PROGRAM_ID: [u8; 32] = [
    2, 168, 246, 145, 78, 136, 161, 107, 189, 35, 149, 133, 95, 100, 4, 217, 180, 244, 86, 183,
    130, 27, 176, 20, 87, 73, 66, 140, 0, 0, 0, 0,
];

const PACKET_DATA_SIZE: u64 = 1_232;
const ACCOUNT_STORAGE_OVERHEAD: u64 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchBpfLoaderProfile {
    /// Genesis runtime behavior: Finalize consumes an explicit Rent account.
    V1_0_7,
    /// Stable epoch-34 activation behavior: Finalize consumes only the program
    /// account and the generic verifier uses the Bank's Rent collector.
    V1_1_14,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LaunchBpfLoaderRent {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: f64,
}

impl LaunchBpfLoaderRent {
    pub(crate) fn minimum_balance(self, data_len: usize) -> u64 {
        ((ACCOUNT_STORAGE_OVERHEAD
            .wrapping_add(data_len as u64)
            .wrapping_mul(self.lamports_per_byte_year)) as f64
            * self.exemption_threshold) as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LaunchBpfLoaderContext {
    pub profile: LaunchBpfLoaderProfile,
    pub bank_rent: LaunchBpfLoaderRent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchBpfLoaderMutation {
    Write {
        program_account: [u8; 32],
        offset: u32,
        bytes_written: usize,
    },
    Finalize {
        program_account: [u8; 32],
        elf_sha256: [u8; 32],
        compilation: Option<Box<CompilationManifest>>,
        compiler_error: Option<String>,
    },
}

#[derive(Debug)]
pub struct LaunchBpfLoaderApply {
    pub mutation: LaunchBpfLoaderMutation,
    /// Present only for `Finalize`. The caller must publish this artifact to
    /// its runtime cache only after the containing transaction commits.
    pub compiled_program: Option<([u8; 32], CompiledProgram)>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LaunchBpfLoaderError {
    #[error("legacy BPF-loader instruction is missing account position {position}")]
    MissingAccount { position: usize },
    #[error("legacy BPF-loader account {pubkey:?} is absent from replay state")]
    MissingAccountState { pubkey: [u8; 32] },
    #[error("legacy BPF-loader instruction data is invalid")]
    InvalidInstructionData,
    #[error("executable account {pubkey:?} reached the legacy loader deployment path")]
    ExecutableInvocation { pubkey: [u8; 32] },
    #[error("account {pubkey:?} must sign the legacy BPF-loader instruction")]
    MissingRequiredSignature { pubkey: [u8; 32] },
    #[error(
        "legacy BPF-loader write for {pubkey:?} ends at {needed}, account data length is {available}"
    )]
    AccountDataTooSmall {
        pubkey: [u8; 32],
        needed: usize,
        available: usize,
    },
    #[error("legacy BPF program {pubkey:?} failed ELF verification/compilation: {message}")]
    InvalidAccountData { pubkey: [u8; 32], message: String },
    #[error("legacy BPF-loader account {position} is {found:?}, expected Rent sysvar {expected:?}")]
    InvalidSysvar {
        position: usize,
        expected: [u8; 32],
        found: [u8; 32],
    },
    #[error("legacy BPF-loader Rent account at position {position} contains invalid data")]
    InvalidSysvarData { position: usize },
    #[error(
        "legacy BPF program {pubkey:?} is not rent exempt: balance={balance}, minimum={minimum}"
    )]
    InsufficientFunds {
        pubkey: [u8; 32],
        balance: u64,
        minimum: u64,
    },
    #[error(
        "executable legacy BPF account {pubkey:?} is not Bank-rent exempt: balance={balance}, minimum={minimum}"
    )]
    ExecutableAccountNotRentExempt {
        pubkey: [u8; 32],
        balance: u64,
        minimum: u64,
    },
    #[error("legacy BPF loader cannot change the owner of account {pubkey:?}")]
    ModifiedProgramId { pubkey: [u8; 32] },
    #[error("legacy BPF loader cannot spend lamports from account {pubkey:?}")]
    ExternalAccountLamportSpend { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed lamports")]
    ReadonlyLamportChange { pubkey: [u8; 32] },
    #[error("legacy BPF loader cannot resize account {pubkey:?}")]
    AccountDataSizeChanged { pubkey: [u8; 32] },
    #[error("read-only account {pubkey:?} changed data")]
    ReadonlyDataModified { pubkey: [u8; 32] },
    #[error("legacy BPF loader changed data in externally owned account {pubkey:?}")]
    ExternalAccountDataModified { pubkey: [u8; 32] },
    #[error("legacy BPF loader made an invalid executable change to account {pubkey:?}")]
    ExecutableModified { pubkey: [u8; 32] },
    #[error("legacy BPF loader changed rent_epoch on account {pubkey:?}")]
    RentEpochModified { pubkey: [u8; 32] },
    #[error(
        "legacy BPF-loader instruction is unbalanced: pre={pre_lamports}, post={post_lamports}"
    )]
    UnbalancedInstruction {
        pre_lamports: u128,
        post_lamports: u128,
    },
}

/// Apply one v1.0.7 legacy BPF-loader deployment instruction atomically.
///
/// The returned compiled artifact remains tentative. The transaction owner
/// must insert it into its cache only after every instruction succeeds and the
/// account overlay commits.
///
/// Instruction-atomic for external callers. Replay uses
/// [`apply_launch_bpf_loader_instruction_in_place`] on a disposable overlay.
pub fn apply_launch_bpf_loader_instruction(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    compiler: &ReplayCompiler,
    context: LaunchBpfLoaderContext,
) -> Result<LaunchBpfLoaderApply, LaunchBpfLoaderError> {
    let mut working = accounts.clone();
    let applied = apply_launch_bpf_loader_instruction_in_place(
        instruction_data,
        account_metas,
        &mut working,
        compiler,
        context,
    )?;
    *accounts = working;
    Ok(applied)
}

/// Replay-only fast path. On error `accounts` may be partially mutated and
/// must be discarded with the transaction overlay.
pub fn apply_launch_bpf_loader_instruction_in_place(
    instruction_data: &[u8],
    account_metas: &[LaunchAccountMeta],
    accounts: &mut AccountMap,
    compiler: &ReplayCompiler,
    context: LaunchBpfLoaderContext,
) -> Result<LaunchBpfLoaderApply, LaunchBpfLoaderError> {
    let program_meta = required_meta(account_metas, 0)?;
    let program = required_account(accounts, program_meta.pubkey)?;
    if program.executable {
        return Err(LaunchBpfLoaderError::ExecutableInvocation {
            pubkey: program_meta.pubkey,
        });
    }
    let instruction = decode_instruction(instruction_data)?;

    let pre_accounts = launch_pre_accounts(account_metas, accounts)?;
    let mut compiled_program = None;
    let mutation = match instruction {
        LoaderInstructionRef::Write { offset, bytes } => {
            if !program_meta.is_signer {
                return Err(LaunchBpfLoaderError::MissingRequiredSignature {
                    pubkey: program_meta.pubkey,
                });
            }
            let offset_usize = offset as usize;
            let needed = offset_usize.saturating_add(bytes.len());
            let account = required_account_mut(accounts, program_meta.pubkey)?;
            if account.data.len() < needed {
                return Err(LaunchBpfLoaderError::AccountDataTooSmall {
                    pubkey: program_meta.pubkey,
                    needed,
                    available: account.data.len(),
                });
            }
            account.data[offset_usize..needed].copy_from_slice(bytes);
            LaunchBpfLoaderMutation::Write {
                program_account: program_meta.pubkey,
                offset,
                bytes_written: bytes.len(),
            }
        }
        LoaderInstructionRef::Finalize => {
            // v1.0.7 advances to Rent before checking the signature. The
            // v1.1.14 Stable epoch-34 processor no longer consumes Rent here.
            let explicit_rent_meta = if context.profile == LaunchBpfLoaderProfile::V1_0_7 {
                let meta = required_meta(account_metas, 1)?;
                required_account(accounts, meta.pubkey)?;
                Some(meta)
            } else {
                None
            };
            if !program_meta.is_signer {
                return Err(LaunchBpfLoaderError::MissingRequiredSignature {
                    pubkey: program_meta.pubkey,
                });
            }

            // The canonical extractor is the current fail-closed structural
            // gate. Exact solana_rbpf-0.1.28 verifier parity remains separate
            // work; the derivative modern compiler must not decide whether a
            // historically accepted Finalize mutates Bank state.
            let extracted = extract_program(
                LoaderAccountKind::Legacy,
                &required_account(accounts, program_meta.pubkey)?.data,
            )
            .map_err(|error| LaunchBpfLoaderError::InvalidAccountData {
                pubkey: program_meta.pubkey,
                message: error.to_string(),
            })?;
            let (compilation, compiler_error) = match compiler.compile_extracted(&extracted) {
                Ok(compiled) => {
                    let manifest = Box::new(compiled.manifest.clone());
                    compiled_program = Some((program_meta.pubkey, compiled));
                    (Some(manifest), None)
                }
                Err(error) => (None, Some(error.to_string())),
            };
            if let Some(rent_meta) = explicit_rent_meta {
                let rent = read_rent(accounts, rent_meta, 1)?;
                let account = required_account(accounts, program_meta.pubkey)?;
                let minimum = rent.minimum_balance(account.data.len());
                if account.lamports < minimum {
                    return Err(LaunchBpfLoaderError::InsufficientFunds {
                        pubkey: program_meta.pubkey,
                        balance: account.lamports,
                        minimum,
                    });
                }
            }
            required_account_mut(accounts, program_meta.pubkey)?.executable = true;
            LaunchBpfLoaderMutation::Finalize {
                program_account: program_meta.pubkey,
                elf_sha256: extracted.elf_sha256,
                compilation,
                compiler_error,
            }
        }
    };

    verify_launch_bpf_loader_instruction(&pre_accounts, accounts, context)?;
    Ok(LaunchBpfLoaderApply {
        mutation,
        compiled_program,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoaderInstructionRef<'a> {
    Write { offset: u32, bytes: &'a [u8] },
    Finalize,
}

fn decode_instruction(bytes: &[u8]) -> Result<LoaderInstructionRef<'_>, LaunchBpfLoaderError> {
    let tag = u32::from_le_bytes(
        bytes
            .get(..4)
            .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?
            .try_into()
            .expect("checked loader tag length"),
    );
    match tag {
        0 => {
            let offset = u32::from_le_bytes(
                bytes
                    .get(4..8)
                    .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?
                    .try_into()
                    .expect("checked loader offset length"),
            );
            let wire_len = u64::from_le_bytes(
                bytes
                    .get(8..16)
                    .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?
                    .try_into()
                    .expect("checked loader byte length"),
            );
            let decoded_len = 16_u64
                .checked_add(wire_len)
                .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?;
            if decoded_len > PACKET_DATA_SIZE {
                return Err(LaunchBpfLoaderError::InvalidInstructionData);
            }
            let len = usize::try_from(wire_len)
                .map_err(|_| LaunchBpfLoaderError::InvalidInstructionData)?;
            let end = 16_usize
                .checked_add(len)
                .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?;
            let payload = bytes
                .get(16..end)
                .ok_or(LaunchBpfLoaderError::InvalidInstructionData)?;
            // v1.0.7 bincode uses `allow_trailing_bytes()` here.
            Ok(LoaderInstructionRef::Write {
                offset,
                bytes: payload,
            })
        }
        1 => Ok(LoaderInstructionRef::Finalize),
        _ => Err(LaunchBpfLoaderError::InvalidInstructionData),
    }
}

fn required_meta(
    account_metas: &[LaunchAccountMeta],
    position: usize,
) -> Result<&LaunchAccountMeta, LaunchBpfLoaderError> {
    account_metas
        .get(position)
        .ok_or(LaunchBpfLoaderError::MissingAccount { position })
}

fn required_account(
    accounts: &AccountMap,
    pubkey: [u8; 32],
) -> Result<&AccountSnapshot, LaunchBpfLoaderError> {
    accounts
        .get(&pubkey)
        .ok_or(LaunchBpfLoaderError::MissingAccountState { pubkey })
}

fn required_account_mut(
    accounts: &mut AccountMap,
    pubkey: [u8; 32],
) -> Result<&mut AccountSnapshot, LaunchBpfLoaderError> {
    accounts
        .get_mut(&pubkey)
        .ok_or(LaunchBpfLoaderError::MissingAccountState { pubkey })
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct RentV100 {
    lamports_per_byte_year: u64,
    exemption_threshold: f64,
    #[allow(dead_code)]
    burn_percent: u8,
}

impl RentV100 {
    fn minimum_balance(self, data_len: usize) -> u64 {
        LaunchBpfLoaderRent {
            lamports_per_byte_year: self.lamports_per_byte_year,
            exemption_threshold: self.exemption_threshold,
        }
        .minimum_balance(data_len)
    }
}

fn read_rent(
    accounts: &AccountMap,
    meta: &LaunchAccountMeta,
    position: usize,
) -> Result<RentV100, LaunchBpfLoaderError> {
    if meta.pubkey != RENT_SYSVAR_ID {
        return Err(LaunchBpfLoaderError::InvalidSysvar {
            position,
            expected: RENT_SYSVAR_ID,
            found: meta.pubkey,
        });
    }
    wincode::deserialize(&required_account(accounts, meta.pubkey)?.data)
        .map_err(|_| LaunchBpfLoaderError::InvalidSysvarData { position })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaunchPreAccount {
    pubkey: [u8; 32],
    is_writable: bool,
    lamports: u64,
    data_len: usize,
    data: Option<Vec<u8>>,
    owner: [u8; 32],
    executable: bool,
    rent_epoch: u64,
}

impl LaunchPreAccount {
    fn new(pubkey: [u8; 32], is_writable: bool, account: &AccountSnapshot) -> Self {
        Self {
            pubkey,
            is_writable,
            lamports: account.lamports,
            data_len: account.data.len(),
            data: should_verify_data(&account.owner, is_writable).then(|| account.data.to_vec()),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        }
    }

    fn verify(
        &self,
        post: &AccountSnapshot,
        context: LaunchBpfLoaderContext,
    ) -> Result<(), LaunchBpfLoaderError> {
        if self.owner != post.owner
            && (!self.is_writable
                || self.owner != BPF_LOADER_PROGRAM_ID
                || !post.data.iter().all(|byte| *byte == 0))
        {
            return Err(LaunchBpfLoaderError::ModifiedProgramId {
                pubkey: self.pubkey,
            });
        }
        if self.owner != BPF_LOADER_PROGRAM_ID && self.lamports > post.lamports {
            return Err(LaunchBpfLoaderError::ExternalAccountLamportSpend {
                pubkey: self.pubkey,
            });
        }
        if !self.is_writable && self.lamports != post.lamports {
            return Err(LaunchBpfLoaderError::ReadonlyLamportChange {
                pubkey: self.pubkey,
            });
        }
        if self.data_len != post.data.len() {
            return Err(LaunchBpfLoaderError::AccountDataSizeChanged {
                pubkey: self.pubkey,
            });
        }
        if should_verify_data(&self.owner, self.is_writable)
            && self.data.as_ref() != Some(&post.data)
        {
            return Err(if self.is_writable {
                LaunchBpfLoaderError::ExternalAccountDataModified {
                    pubkey: self.pubkey,
                }
            } else {
                LaunchBpfLoaderError::ReadonlyDataModified {
                    pubkey: self.pubkey,
                }
            });
        }
        if self.executable != post.executable {
            if context.profile == LaunchBpfLoaderProfile::V1_1_14 {
                let minimum = context.bank_rent.minimum_balance(post.data.len());
                if post.lamports < minimum {
                    return Err(LaunchBpfLoaderError::ExecutableAccountNotRentExempt {
                        pubkey: self.pubkey,
                        balance: post.lamports,
                        minimum,
                    });
                }
            }
            if !self.is_writable || self.executable || self.owner != BPF_LOADER_PROGRAM_ID {
                return Err(LaunchBpfLoaderError::ExecutableModified {
                    pubkey: self.pubkey,
                });
            }
        }
        if self.rent_epoch != post.rent_epoch {
            return Err(LaunchBpfLoaderError::RentEpochModified {
                pubkey: self.pubkey,
            });
        }
        Ok(())
    }
}

fn should_verify_data(owner: &[u8; 32], is_writable: bool) -> bool {
    *owner != BPF_LOADER_PROGRAM_ID || !is_writable
}

fn launch_pre_accounts(
    account_metas: &[LaunchAccountMeta],
    accounts: &AccountMap,
) -> Result<Vec<LaunchPreAccount>, LaunchBpfLoaderError> {
    account_metas
        .iter()
        .enumerate()
        .filter(|(index, meta)| {
            !account_metas[index + 1..]
                .iter()
                .any(|later| later.pubkey == meta.pubkey)
        })
        .map(|(_, meta)| {
            Ok(LaunchPreAccount::new(
                meta.pubkey,
                meta.is_writable,
                required_account(accounts, meta.pubkey)?,
            ))
        })
        .collect()
}

fn verify_launch_bpf_loader_instruction(
    pre_accounts: &[LaunchPreAccount],
    accounts: &AccountMap,
    context: LaunchBpfLoaderContext,
) -> Result<(), LaunchBpfLoaderError> {
    let mut pre_lamports = 0_u128;
    let mut post_lamports = 0_u128;
    for pre in pre_accounts {
        let post = required_account(accounts, pre.pubkey)?;
        pre.verify(post, context)?;
        pre_lamports += u128::from(pre.lamports);
        post_lamports += u128::from(post.lamports);
    }
    if pre_lamports != post_lamports {
        return Err(LaunchBpfLoaderError::UnbalancedInstruction {
            pre_lamports,
            post_lamports,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    const PROGRAM: [u8; 32] = [7; 32];

    fn meta(pubkey: [u8; 32], is_signer: bool, is_writable: bool) -> LaunchAccountMeta {
        LaunchAccountMeta {
            pubkey,
            is_signer,
            is_writable,
        }
    }

    fn program_account(data: Vec<u8>) -> AccountSnapshot {
        AccountSnapshot {
            lamports: 10_000_000_000,
            owner: BPF_LOADER_PROGRAM_ID,
            executable: false,
            rent_epoch: 0,
            data: data.into(),
        }
    }

    fn rent_account(rent: RentV100) -> AccountSnapshot {
        AccountSnapshot {
            lamports: 1,
            owner: [9; 32],
            executable: false,
            rent_epoch: 0,
            data: wincode::serialize(&rent).unwrap().into(),
        }
    }

    fn write(offset: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + payload.len());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn fixture() -> Vec<u8> {
        STANDARD
            .decode(include_str!("../fixtures/relative_call_sbpfv0.so.b64").trim())
            .unwrap()
    }

    fn context(profile: LaunchBpfLoaderProfile) -> LaunchBpfLoaderContext {
        LaunchBpfLoaderContext {
            profile,
            bank_rent: LaunchBpfLoaderRent {
                lamports_per_byte_year: 3_480,
                exemption_threshold: 2.0,
            },
        }
    }

    #[test]
    fn decodes_observed_932_byte_write_without_allocating_payload() {
        let payload = vec![0x5a; 932];
        let instruction = write(932, &payload);
        assert_eq!(instruction.len(), 948);
        assert_eq!(
            decode_instruction(&instruction).unwrap(),
            LoaderInstructionRef::Write {
                offset: 932,
                bytes: &payload,
            }
        );
    }

    #[test]
    fn decodes_all_five_observed_epoch_34_write_offsets() {
        for offset in [932_u32, 0, 4_660, 1_864, 2_796] {
            let mut payload = vec![0x5a; 932];
            if offset == 0 {
                payload[..4].copy_from_slice(b"\x7fELF");
            }
            let instruction = write(offset, &payload);
            assert_eq!(instruction.len(), 948);
            assert_eq!(
                decode_instruction(&instruction).unwrap(),
                LoaderInstructionRef::Write {
                    offset,
                    bytes: &payload,
                }
            );
        }
    }

    #[test]
    fn write_mutates_exact_range_and_allows_historical_trailing_bytes() {
        let mut accounts = AccountMap::from([(PROGRAM, program_account(vec![0; 8]))]);
        let mut instruction = write(2, &[1, 2, 3]);
        instruction.extend_from_slice(&[0xaa, 0xbb]);
        let applied = apply_launch_bpf_loader_instruction(
            &instruction,
            &[meta(PROGRAM, true, true)],
            &mut accounts,
            &ReplayCompiler::new(),
            context(LaunchBpfLoaderProfile::V1_0_7),
        )
        .unwrap();
        assert_eq!(accounts[&PROGRAM].data, [0, 0, 1, 2, 3, 0, 0, 0]);
        assert!(applied.compiled_program.is_none());
        assert_eq!(
            applied.mutation,
            LaunchBpfLoaderMutation::Write {
                program_account: PROGRAM,
                offset: 2,
                bytes_written: 3,
            }
        );
    }

    #[test]
    fn write_error_order_is_signature_then_size_then_post_verifier() {
        let original = program_account(vec![0; 2]);
        let instruction = write(1, &[1, 2]);
        let mut accounts = AccountMap::from([(PROGRAM, original.clone())]);
        assert_eq!(
            apply_launch_bpf_loader_instruction(
                &instruction,
                &[meta(PROGRAM, false, false)],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_0_7),
            )
            .unwrap_err(),
            LaunchBpfLoaderError::MissingRequiredSignature { pubkey: PROGRAM }
        );
        assert_eq!(accounts[&PROGRAM], original);

        assert!(matches!(
            apply_launch_bpf_loader_instruction(
                &instruction,
                &[meta(PROGRAM, true, false)],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_0_7),
            ),
            Err(LaunchBpfLoaderError::AccountDataTooSmall { .. })
        ));
        assert_eq!(accounts[&PROGRAM], original);

        let mut external = program_account(vec![0; 4]);
        external.owner = [8; 32];
        accounts.insert(PROGRAM, external.clone());
        assert_eq!(
            apply_launch_bpf_loader_instruction(
                &write(1, &[9]),
                &[meta(PROGRAM, true, true)],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_0_7),
            )
            .unwrap_err(),
            LaunchBpfLoaderError::ExternalAccountDataModified { pubkey: PROGRAM }
        );
        assert_eq!(accounts[&PROGRAM], external);
    }

    #[test]
    fn finalize_compiles_then_checks_rent_and_sets_executable() {
        let elf = fixture();
        let rent = RentV100 {
            lamports_per_byte_year: 3_480,
            exemption_threshold: 2.0,
            burn_percent: 100,
        };
        let mut accounts = AccountMap::from([
            (PROGRAM, program_account(elf.clone())),
            (RENT_SYSVAR_ID, rent_account(rent)),
        ]);
        let applied = apply_launch_bpf_loader_instruction(
            &1_u32.to_le_bytes(),
            &[
                meta(PROGRAM, true, true),
                meta(RENT_SYSVAR_ID, false, false),
            ],
            &mut accounts,
            &ReplayCompiler::new(),
            context(LaunchBpfLoaderProfile::V1_0_7),
        )
        .unwrap();
        assert!(accounts[&PROGRAM].executable);
        let (_, compiled) = applied.compiled_program.unwrap();
        assert_eq!(compiled.manifest.elf_len, elf.len());
        assert!(matches!(
            applied.mutation,
            LaunchBpfLoaderMutation::Finalize {
                program_account: PROGRAM,
                ..
            }
        ));
    }

    #[test]
    fn epoch_34_finalize_uses_bank_rent_without_instruction_rent_meta() {
        let elf = fixture();
        let mut account = program_account(elf.clone());
        let bank_context = context(LaunchBpfLoaderProfile::V1_1_14);
        account.lamports = bank_context.bank_rent.minimum_balance(elf.len());
        let mut accounts = AccountMap::from([(PROGRAM, account)]);
        let applied = apply_launch_bpf_loader_instruction(
            &1_u32.to_le_bytes(),
            &[meta(PROGRAM, true, true)],
            &mut accounts,
            &ReplayCompiler::new(),
            bank_context,
        )
        .unwrap();
        assert!(accounts[&PROGRAM].executable);
        assert!(matches!(
            applied.mutation,
            LaunchBpfLoaderMutation::Finalize {
                program_account: PROGRAM,
                ..
            }
        ));
    }

    #[test]
    fn epoch_34_bank_rent_error_precedes_executable_owner_and_writable_errors() {
        let elf = fixture();
        let mut account = program_account(elf);
        account.lamports = 0;
        account.owner = [8; 32];
        let original = account.clone();
        let mut accounts = AccountMap::from([(PROGRAM, account)]);
        assert!(matches!(
            apply_launch_bpf_loader_instruction(
                &1_u32.to_le_bytes(),
                &[meta(PROGRAM, true, false)],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_1_14),
            ),
            Err(LaunchBpfLoaderError::ExecutableAccountNotRentExempt { .. })
        ));
        assert_eq!(accounts[&PROGRAM], original);
    }

    #[test]
    fn finalize_requests_rent_meta_before_signature_and_checks_elf_before_rent_data() {
        let invalid_program = program_account(vec![0; 64]);
        let mut accounts = AccountMap::from([(PROGRAM, invalid_program.clone())]);
        assert_eq!(
            apply_launch_bpf_loader_instruction(
                &1_u32.to_le_bytes(),
                &[meta(PROGRAM, false, true)],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_0_7),
            )
            .unwrap_err(),
            LaunchBpfLoaderError::MissingAccount { position: 1 }
        );

        accounts.insert(RENT_SYSVAR_ID, program_account(Vec::new()));
        assert!(matches!(
            apply_launch_bpf_loader_instruction(
                &1_u32.to_le_bytes(),
                &[
                    meta(PROGRAM, true, true),
                    meta(RENT_SYSVAR_ID, false, false),
                ],
                &mut accounts,
                &ReplayCompiler::new(),
                context(LaunchBpfLoaderProfile::V1_0_7),
            ),
            Err(LaunchBpfLoaderError::InvalidAccountData { .. })
        ));
        assert_eq!(accounts[&PROGRAM], invalid_program);
    }

    #[test]
    fn decoder_rejects_truncation_unknown_tag_and_over_limit_write() {
        assert_eq!(
            decode_instruction(&[]),
            Err(LaunchBpfLoaderError::InvalidInstructionData)
        );
        assert_eq!(
            decode_instruction(&2_u32.to_le_bytes()),
            Err(LaunchBpfLoaderError::InvalidInstructionData)
        );
        assert_eq!(
            decode_instruction(&write(0, &vec![0; 1_217])),
            Err(LaunchBpfLoaderError::InvalidInstructionData)
        );
    }
}

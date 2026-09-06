use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, Program, SwapKind, account, anchor_discriminator,
    one_byte_discriminator, read_u8, read_u64_le,
};

const SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
const SWAP_V3_DYN: [u8; 8] = [229, 46, 213, 132, 105, 40, 40, 228];
const SWAP2: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
const SWAP_WITH_PARTNER: [u8; 8] = [133, 215, 191, 214, 102, 243, 55, 25];
const SWAP_EXACT_AMOUNT_IN: [u8; 8] = [8, 151, 245, 76, 172, 203, 144, 39];

const SEMANTIC_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const LAYOUT_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT.union(Evidence::TOKEN_FLOW_REQUIRED);
const STRUCTURAL_EVIDENCE: Evidence =
    Evidence::TOKEN_FLOW_REQUIRED.union(Evidence::STRUCTURAL_ONLY);

#[inline]
pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::LifinityV2 => decode_lifinity(data, accounts),
        Program::BonkSwap => decode_bonkswap(data, accounts),
        Program::MeteoraPools => decode_meteora_pools(data, accounts),
        Program::Byreal => decode_byreal(data, accounts),
        Program::MeteoraDbc => decode_meteora_dbc(data, accounts),
        Program::StabbleWeighted | Program::StabbleStable => {
            decode_stabble(program, data, accounts)
        }
        Program::LegacyCtma => decode_legacy_ctma(data),
        Program::Legacy9tke
        | Program::Aldrin
        | Program::CremaClmm
        | Program::AldrinV2
        | Program::LegacyD3bb
        | Program::OneDex
        | Program::Cropper
        | Program::Invariant
        | Program::ObricV2 => decode_structural_anchor(program, data),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_lifinity(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes != SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 24)?;
        require_accounts(accounts, 13)?;
        Ok(DecodedInstruction {
            program: Program::LifinityV2,
            role: Program::LifinityV2.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 1)?),
                authority: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 2)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 4)?),
                input_vault: Some(account(accounts, 5)?),
                output_vault: Some(account(accounts, 6)?),
                fee_account: Some(account(accounts, 8)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 8, 16)?,
                minimum_amount_out: amount(data, 16, 24)?,
            },
            evidence: SEMANTIC_EVIDENCE,
        })
    })())
}

fn decode_bonkswap(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes != SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        // delta_in is a u64, price_limit is a u128, and x_to_y is a Borsh bool.
        require_data(data, 33)?;
        require_accounts(accounts, 17)?;
        let x_to_y = bool_at(data, 32)?;
        let (user_source, user_destination, input_vault, output_vault, input_mint, output_mint) =
            if x_to_y {
                (
                    account(accounts, 6)?,
                    account(accounts, 7)?,
                    account(accounts, 4)?,
                    account(accounts, 5)?,
                    account(accounts, 2)?,
                    account(accounts, 3)?,
                )
            } else {
                (
                    account(accounts, 7)?,
                    account(accounts, 6)?,
                    account(accounts, 5)?,
                    account(accounts, 4)?,
                    account(accounts, 3)?,
                    account(accounts, 2)?,
                )
            };
        Ok(DecodedInstruction {
            program: Program::BonkSwap,
            role: Program::BonkSwap.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 1)?),
                authority: Some(account(accounts, 12)?),
                user_authority: Some(account(accounts, 8)?),
                user_source: Some(user_source),
                user_destination: Some(user_destination),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                input_vault: Some(input_vault),
                output_vault: Some(output_vault),
                input_mint: Some(input_mint),
                output_mint: Some(output_mint),
                ..AccountRoles::default()
            },
            // The second scalar is a fixed-point price limit, not a minimum
            // output amount, so the common amount pair cannot represent it.
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn decode_meteora_pools(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes != SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 24)?;
        require_accounts(accounts, 15)?;
        Ok(DecodedInstruction {
            program: Program::MeteoraPools,
            role: Program::MeteoraPools.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 12)?),
                user_source: Some(account(accounts, 1)?),
                user_destination: Some(account(accounts, 2)?),
                vault_a: Some(account(accounts, 5)?),
                vault_b: Some(account(accounts, 6)?),
                fee_account: Some(account(accounts, 11)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 8, 16)?,
                minimum_amount_out: amount(data, 16, 24)?,
            },
            evidence: SEMANTIC_EVIDENCE,
        })
    })())
}

fn decode_byreal(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let (name, token_2022_layout) = match discriminator.bytes {
        SWAP => ("swap", false),
        SWAP_V2 => ("swap_v2", true),
        SWAP_V3_DYN => ("swap_v3_dyn", true),
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    outcome((|| {
        require_data(data, 41)?;
        require_accounts(accounts, if token_2022_layout { 13 } else { 10 })?;
        let specified_amount = amount(data, 8, 16)?;
        let threshold = amount(data, 16, 24)?;
        let is_base_input = bool_at(data, 40)?;
        Ok(DecodedInstruction {
            program: Program::Byreal,
            role: Program::Byreal.role(),
            name,
            class: InstructionClass::Swap(if is_base_input {
                SwapKind::ExactIn
            } else {
                SwapKind::ExactOut
            }),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 2)?),
                user_authority: Some(account(accounts, 0)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 4)?),
                input_vault: Some(account(accounts, 5)?),
                output_vault: Some(account(accounts, 6)?),
                input_mint: if token_2022_layout {
                    Some(account(accounts, 11)?)
                } else {
                    None
                },
                output_mint: if token_2022_layout {
                    Some(account(accounts, 12)?)
                } else {
                    None
                },
                ..AccountRoles::default()
            },
            amounts: if is_base_input {
                Amounts::ExactIn {
                    amount_in: specified_amount,
                    minimum_amount_out: threshold,
                }
            } else {
                Amounts::ExactOut {
                    maximum_amount_in: threshold,
                    amount_out: specified_amount,
                }
            },
            evidence: SEMANTIC_EVIDENCE,
        })
    })())
}

fn decode_meteora_dbc(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes != SWAP && discriminator.bytes != SWAP2 {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_accounts(accounts, 15)?;
        let (name, kind, amounts) = if discriminator.bytes == SWAP {
            require_data(data, 24)?;
            (
                "swap",
                SwapKind::ExactIn,
                Amounts::ExactIn {
                    amount_in: amount(data, 8, 16)?,
                    minimum_amount_out: amount(data, 16, 24)?,
                },
            )
        } else {
            require_data(data, 25)?;
            let amount_0 = amount(data, 8, 16)?;
            let amount_1 = amount(data, 16, 24)?;
            match read_u8(data, 24) {
                Some(0) => (
                    "swap2",
                    SwapKind::ExactIn,
                    Amounts::ExactIn {
                        amount_in: amount_0,
                        minimum_amount_out: amount_1,
                    },
                ),
                Some(1) => (
                    "swap2",
                    SwapKind::PartialFill,
                    Amounts::PartialFill {
                        amount_in: amount_0,
                        minimum_amount_out: amount_1,
                    },
                ),
                Some(2) => (
                    "swap2",
                    SwapKind::ExactOut,
                    Amounts::ExactOut {
                        maximum_amount_in: amount_1,
                        amount_out: amount_0,
                    },
                ),
                Some(_) => {
                    return Err(MalformedReason::InvalidInstructionData { offset: 24 });
                }
                None => return Err(short_data(data, 25)),
            }
        };
        Ok(DecodedInstruction {
            program: Program::MeteoraDbc,
            role: Program::MeteoraDbc.role(),
            name,
            class: InstructionClass::Swap(kind),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 2)?),
                authority: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 9)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 4)?),
                vault_a: Some(account(accounts, 5)?),
                vault_b: Some(account(accounts, 6)?),
                // referral_token_account is optional and can be represented by
                // a program-id placeholder, so it is not exposed as a fee role.
                ..AccountRoles::default()
            },
            amounts,
            evidence: SEMANTIC_EVIDENCE,
        })
    })())
}

fn decode_stabble(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let is_v2 = match discriminator.bytes {
        SWAP => false,
        SWAP_V2 => true,
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    outcome((|| {
        require_data(data, 9)?;
        require_accounts(accounts, if is_v2 { 15 } else { 12 })?;
        let (amounts, amount_evidence) = match read_u8(data, 8) {
            Some(0) => {
                require_data(data, 17)?;
                // Read the fixed minimum to validate the full known layout.
                let _minimum_amount_out = amount(data, 9, 17)?;
                (Amounts::Unknown, LAYOUT_EVIDENCE)
            }
            Some(1) => {
                require_data(data, 25)?;
                (
                    Amounts::ExactIn {
                        amount_in: amount(data, 9, 17)?,
                        minimum_amount_out: amount(data, 17, 25)?,
                    },
                    SEMANTIC_EVIDENCE,
                )
            }
            Some(_) => return Err(MalformedReason::InvalidInstructionData { offset: 8 }),
            None => return Err(short_data(data, 9)),
        };
        let roles = if is_v2 {
            AccountRoles {
                pool: Some(account(accounts, 8)?),
                authority: Some(account(accounts, 9)?),
                user_authority: Some(account(accounts, 0)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 4)?),
                input_vault: Some(account(accounts, 5)?),
                output_vault: Some(account(accounts, 6)?),
                input_mint: Some(account(accounts, 1)?),
                output_mint: Some(account(accounts, 2)?),
                fee_account: Some(account(accounts, 7)?),
                ..AccountRoles::default()
            }
        } else {
            AccountRoles {
                pool: Some(account(accounts, 6)?),
                authority: Some(account(accounts, 7)?),
                user_authority: Some(account(accounts, 0)?),
                user_source: Some(account(accounts, 1)?),
                user_destination: Some(account(accounts, 2)?),
                input_vault: Some(account(accounts, 3)?),
                output_vault: Some(account(accounts, 4)?),
                fee_account: Some(account(accounts, 5)?),
                ..AccountRoles::default()
            }
        };
        Ok(DecodedInstruction {
            program,
            role: program.role(),
            name: if is_v2 { "swap_v2" } else { "swap" },
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: roles,
            amounts,
            evidence: amount_evidence,
        })
    })())
}

fn decode_structural_anchor(program: Program, data: &[u8]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let name = match program {
        Program::Legacy9tke
        | Program::Aldrin
        | Program::AldrinV2
        | Program::LegacyD3bb
        | Program::Cropper
        | Program::Invariant => {
            if discriminator.bytes != SWAP {
                return DecodeOutcome::Unsupported { discriminator };
            }
            "swap"
        }
        Program::CremaClmm => {
            if discriminator.bytes != SWAP_WITH_PARTNER {
                return DecodeOutcome::Unsupported { discriminator };
            }
            "swap_with_partner"
        }
        Program::OneDex => {
            if discriminator.bytes != SWAP_EXACT_AMOUNT_IN {
                return DecodeOutcome::Unsupported { discriminator };
            }
            "swap_exact_amount_in"
        }
        Program::ObricV2 => match discriminator.bytes {
            SWAP => "swap",
            SWAP2 => "swap2",
            _ => return DecodeOutcome::Unsupported { discriminator },
        },
        _ => return DecodeOutcome::UnknownProgram,
    };

    structural_instruction(program, name, discriminator)
}

fn decode_legacy_ctma(data: &[u8]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::one(1) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    structural_instruction(Program::LegacyCtma, "swap", discriminator)
}

fn structural_instruction(
    program: Program,
    name: &'static str,
    discriminator: Discriminator,
) -> DecodeOutcome {
    DecodeOutcome::Decoded(DecodedInstruction {
        program,
        role: program.role(),
        name,
        class: InstructionClass::Swap(SwapKind::Unspecified),
        discriminator,
        accounts: AccountRoles::default(),
        amounts: Amounts::Unknown,
        evidence: STRUCTURAL_EVIDENCE,
    })
}

#[inline]
fn amount(data: &[u8], offset: usize, needed: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(short_data(data, needed))
}

#[inline]
fn bool_at(data: &[u8], offset: usize) -> Result<bool, MalformedReason> {
    match read_u8(data, offset) {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(short_data(data, offset.saturating_add(1))),
    }
}

#[inline]
fn require_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    if data.len() < needed {
        Err(short_data(data, needed))
    } else {
        Ok(())
    }
}

#[inline]
fn short_data(data: &[u8], needed: usize) -> MalformedReason {
    MalformedReason::InstructionDataTooShort {
        needed,
        actual: data.len(),
    }
}

#[inline]
fn require_accounts(accounts: &[CompactId], needed: usize) -> Result<(), MalformedReason> {
    account(accounts, needed.saturating_sub(1)).map(|_| ())
}

#[inline]
fn outcome(decoded: Result<DecodedInstruction, MalformedReason>) -> DecodeOutcome {
    match decoded {
        Ok(decoded) => DecodeOutcome::Decoded(decoded),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids<const N: usize>() -> [CompactId; N] {
        core::array::from_fn(|index| index as CompactId)
    }

    fn anchor_data<const N: usize>(discriminator: [u8; 8]) -> [u8; N] {
        let mut data = [0_u8; N];
        if N >= 8 {
            data[..8].copy_from_slice(&discriminator);
        }
        data
    }

    fn decoded(outcome: DecodeOutcome) -> Option<DecodedInstruction> {
        match outcome {
            DecodeOutcome::Decoded(decoded) => Some(decoded),
            _ => None,
        }
    }

    #[test]
    fn semantic_exact_in_programs_decode_proven_roles_and_amounts() {
        let mut lifinity = anchor_data::<24>(SWAP);
        lifinity[8..16].copy_from_slice(&100_u64.to_le_bytes());
        lifinity[16..24].copy_from_slice(&90_u64.to_le_bytes());
        let Some(lifinity) = decoded(decode(Program::LifinityV2, &lifinity, &ids::<13>())) else {
            panic!("valid Lifinity swap did not decode");
        };
        assert_eq!(lifinity.accounts.pool, Some(1));
        assert_eq!(lifinity.accounts.authority, Some(0));
        assert_eq!(lifinity.accounts.user_authority, Some(2));
        assert_eq!(lifinity.accounts.fee_account, Some(8));
        assert_eq!(
            lifinity.amounts,
            Amounts::ExactIn {
                amount_in: 100,
                minimum_amount_out: 90
            }
        );

        let mut meteora = anchor_data::<24>(SWAP);
        meteora[8..16].copy_from_slice(&200_u64.to_le_bytes());
        meteora[16..24].copy_from_slice(&180_u64.to_le_bytes());
        let Some(meteora) = decoded(decode(Program::MeteoraPools, &meteora, &ids::<15>())) else {
            panic!("valid Meteora pools swap did not decode");
        };
        assert_eq!(meteora.accounts.pool, Some(0));
        assert_eq!(meteora.accounts.vault_a, Some(5));
        assert_eq!(meteora.accounts.vault_b, Some(6));
        assert_eq!(meteora.accounts.user_authority, Some(12));
        assert_eq!(meteora.accounts.fee_account, Some(11));
    }

    #[test]
    fn bonkswap_uses_direction_to_assign_roles_but_not_a_false_amount_pair() {
        let mut data = anchor_data::<33>(SWAP);
        data[8..16].copy_from_slice(&300_u64.to_le_bytes());
        data[32] = 1;
        let Some(decoded) = decoded(decode(Program::BonkSwap, &data, &ids::<17>())) else {
            panic!("valid BonkSwap did not decode");
        };
        assert_eq!(decoded.accounts.pool, Some(1));
        assert_eq!(decoded.accounts.authority, Some(12));
        assert_eq!(decoded.accounts.user_authority, Some(8));
        assert_eq!(decoded.accounts.user_source, Some(6));
        assert_eq!(decoded.accounts.user_destination, Some(7));
        assert_eq!(decoded.accounts.input_mint, Some(2));
        assert_eq!(decoded.accounts.output_mint, Some(3));
        assert_eq!(decoded.amounts, Amounts::Unknown);
        assert!(!decoded.evidence.contains(Evidence::AMOUNTS));
    }

    #[test]
    fn byreal_decodes_all_layouts_and_strict_borsh_direction() {
        let variants = [
            (SWAP, 10_usize, "swap"),
            (SWAP_V2, 13_usize, "swap_v2"),
            (SWAP_V3_DYN, 13_usize, "swap_v3_dyn"),
        ];
        for (discriminator, account_count, name) in variants {
            let mut data = anchor_data::<41>(discriminator);
            data[8..16].copy_from_slice(&500_u64.to_le_bytes());
            data[16..24].copy_from_slice(&650_u64.to_le_bytes());
            let account_ids = ids::<13>();
            let Some(decoded) = decoded(decode(
                Program::Byreal,
                &data,
                &account_ids[..account_count],
            )) else {
                panic!("valid Byreal swap did not decode");
            };
            assert_eq!(decoded.name, name);
            assert_eq!(decoded.accounts.user_authority, Some(0));
            assert_eq!(
                decoded.amounts,
                Amounts::ExactOut {
                    maximum_amount_in: 650,
                    amount_out: 500
                }
            );
        }

        let mut invalid = anchor_data::<41>(SWAP);
        invalid[40] = 2;
        assert_eq!(
            decode(Program::Byreal, &invalid, &ids::<10>()),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 40 })
        );
    }

    #[test]
    fn meteora_dbc_swap2_decodes_each_documented_mode() {
        let cases = [
            (
                0,
                SwapKind::ExactIn,
                Amounts::ExactIn {
                    amount_in: 700,
                    minimum_amount_out: 600,
                },
            ),
            (
                1,
                SwapKind::PartialFill,
                Amounts::PartialFill {
                    amount_in: 700,
                    minimum_amount_out: 600,
                },
            ),
            (
                2,
                SwapKind::ExactOut,
                Amounts::ExactOut {
                    maximum_amount_in: 600,
                    amount_out: 700,
                },
            ),
        ];
        for (mode, kind, expected_amounts) in cases {
            let mut data = anchor_data::<25>(SWAP2);
            data[8..16].copy_from_slice(&700_u64.to_le_bytes());
            data[16..24].copy_from_slice(&600_u64.to_le_bytes());
            data[24] = mode;
            let Some(decoded) = decoded(decode(Program::MeteoraDbc, &data, &ids::<15>())) else {
                panic!("valid Meteora DBC swap2 did not decode");
            };
            assert_eq!(decoded.class, InstructionClass::Swap(kind));
            assert_eq!(decoded.amounts, expected_amounts);
            assert_eq!(decoded.accounts.authority, Some(0));
            assert_eq!(decoded.accounts.user_authority, Some(9));
        }
    }

    #[test]
    fn stabble_decodes_borsh_option_and_both_account_layouts() {
        let mut v1 = anchor_data::<25>(SWAP);
        v1[8] = 1;
        v1[9..17].copy_from_slice(&800_u64.to_le_bytes());
        v1[17..25].copy_from_slice(&750_u64.to_le_bytes());
        let Some(v1) = decoded(decode(Program::StabbleWeighted, &v1, &ids::<12>())) else {
            panic!("valid Stabble v1 swap did not decode");
        };
        assert_eq!(v1.accounts.pool, Some(6));
        assert_eq!(v1.accounts.user_authority, Some(0));
        assert_eq!(v1.accounts.fee_account, Some(5));
        assert_eq!(
            v1.amounts,
            Amounts::ExactIn {
                amount_in: 800,
                minimum_amount_out: 750
            }
        );

        let mut v2 = anchor_data::<17>(SWAP_V2);
        v2[8] = 0;
        v2[9..17].copy_from_slice(&700_u64.to_le_bytes());
        let Some(v2) = decoded(decode(Program::StabbleStable, &v2, &ids::<15>())) else {
            panic!("valid Stabble v2 no-input swap did not decode");
        };
        assert_eq!(v2.accounts.pool, Some(8));
        assert_eq!(v2.accounts.input_mint, Some(1));
        assert_eq!(v2.accounts.output_mint, Some(2));
        assert_eq!(v2.amounts, Amounts::Unknown);
        assert!(!v2.evidence.contains(Evidence::AMOUNTS));
    }

    #[test]
    fn structural_only_variants_recognize_only_proven_names() {
        let cases = [
            (Program::Legacy9tke, SWAP, "swap"),
            (Program::Aldrin, SWAP, "swap"),
            (Program::CremaClmm, SWAP_WITH_PARTNER, "swap_with_partner"),
            (Program::AldrinV2, SWAP, "swap"),
            (Program::LegacyD3bb, SWAP, "swap"),
            (
                Program::OneDex,
                SWAP_EXACT_AMOUNT_IN,
                "swap_exact_amount_in",
            ),
            (Program::Cropper, SWAP, "swap"),
            (Program::Invariant, SWAP, "swap"),
            (Program::ObricV2, SWAP, "swap"),
            (Program::ObricV2, SWAP2, "swap2"),
        ];
        for (program, discriminator, name) in cases {
            let Some(decoded) = decoded(decode(program, &discriminator, &[])) else {
                panic!("proven structural swap did not decode");
            };
            assert_eq!(decoded.name, name);
            assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::Unspecified));
            assert_eq!(decoded.accounts, AccountRoles::default());
            assert_eq!(decoded.amounts, Amounts::Unknown);
            assert_eq!(decoded.evidence, STRUCTURAL_EVIDENCE);
        }

        let Some(ctma) = decoded(decode(Program::LegacyCtma, &[1], &[])) else {
            panic!("proven CTMA structural swap did not decode");
        };
        assert_eq!(ctma.name, "swap");
        assert_eq!(ctma.discriminator, Discriminator::one(1));
    }

    #[test]
    fn known_semantic_variants_reject_truncated_data_and_accounts() {
        let data_cases = [
            (Program::LifinityV2, SWAP, 24_usize),
            (Program::BonkSwap, SWAP, 33_usize),
            (Program::MeteoraPools, SWAP, 24_usize),
            (Program::Byreal, SWAP, 41_usize),
            (Program::MeteoraDbc, SWAP, 24_usize),
        ];
        for (program, discriminator, needed) in data_cases {
            assert_eq!(
                decode(program, &discriminator, &ids::<17>()),
                DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                    needed,
                    actual: 8
                })
            );
        }

        let mut lifinity = anchor_data::<24>(SWAP);
        lifinity[8..16].copy_from_slice(&1_u64.to_le_bytes());
        assert_eq!(
            decode(Program::LifinityV2, &lifinity, &ids::<12>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 13,
                actual: 12
            })
        );
        assert_eq!(
            decode(Program::LegacyCtma, &[], &[]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 1,
                actual: 0
            })
        );
    }

    #[test]
    fn every_routed_variant_rejects_an_unsupported_discriminator() {
        let anchor_programs = [
            Program::LifinityV2,
            Program::Legacy9tke,
            Program::Aldrin,
            Program::BonkSwap,
            Program::CremaClmm,
            Program::AldrinV2,
            Program::LegacyD3bb,
            Program::OneDex,
            Program::MeteoraPools,
            Program::Cropper,
            Program::Invariant,
            Program::Byreal,
            Program::MeteoraDbc,
            Program::ObricV2,
            Program::StabbleWeighted,
            Program::StabbleStable,
        ];
        for program in anchor_programs {
            assert_eq!(
                decode(program, &[255; 8], &[]),
                DecodeOutcome::Unsupported {
                    discriminator: Discriminator::eight([255; 8])
                }
            );
        }
        assert_eq!(
            decode(Program::LegacyCtma, &[255], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::one(255)
            }
        );
    }
}

use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, SwapKind, account,
    anchor_discriminator, one_byte_discriminator, read_u8, read_u64_le,
};

const CLMM_SWAP: Discriminator = Discriminator::eight([248, 198, 158, 145, 225, 117, 135, 200]);
const CLMM_SWAP_V2: Discriminator = Discriminator::eight([43, 4, 237, 11, 26, 201, 30, 98]);
const CLMM_SWAP_ROUTER_BASE_IN: Discriminator =
    Discriminator::eight([69, 125, 115, 218, 245, 186, 242, 196]);

const CPMM_SWAP_BASE_INPUT: Discriminator =
    Discriminator::eight([143, 190, 90, 218, 196, 30, 51, 222]);
const CPMM_SWAP_BASE_OUTPUT: Discriminator =
    Discriminator::eight([55, 217, 98, 86, 163, 74, 180, 173]);

const AMM_V4_SWAP_BASE_IN: Discriminator = Discriminator::one(9);
const AMM_V4_SWAP_BASE_OUT: Discriminator = Discriminator::one(11);
const AMM_V4_SWAP_BASE_IN_V2: Discriminator = Discriminator::one(16);
const AMM_V4_SWAP_BASE_OUT_V2: Discriminator = Discriminator::one(17);

const SWAP_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const ROUTE_EVIDENCE: Evidence = SWAP_EVIDENCE.union(Evidence::ROUTE_CONTAINER);

pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::RaydiumClmm => decode_clmm(data, accounts),
        Program::RaydiumCpmm => decode_cpmm(data, accounts),
        Program::RaydiumAmmV4 => decode_amm_v4(data, accounts),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_clmm(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let decoded = match discriminator {
        CLMM_SWAP | CLMM_SWAP_V2 => {
            decode_clmm_swap(data, accounts, discriminator, discriminator == CLMM_SWAP_V2)
        }
        CLMM_SWAP_ROUTER_BASE_IN => decode_clmm_router(data, accounts, discriminator),
        _ => return DecodeOutcome::Unsupported { discriminator },
    };
    outcome(decoded)
}

fn decode_clmm_swap(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
    token_2022_layout: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    // amount: u64, other_amount_threshold: u64, sqrt_price_limit_x64: u128,
    // is_base_input: bool.
    require_data(data, 41)?;
    require_accounts(accounts, if token_2022_layout { 13 } else { 10 })?;

    let specified_amount = amount(data, 8, 16)?;
    let other_amount_threshold = amount(data, 16, 24)?;
    let is_base_input = match read_u8(data, 40) {
        Some(0) => false,
        Some(1) => true,
        Some(_) => return Err(MalformedReason::InvalidInstructionData { offset: 40 }),
        None => {
            return Err(MalformedReason::InstructionDataTooShort {
                needed: 41,
                actual: data.len(),
            });
        }
    };
    let (class, amounts) = if is_base_input {
        (
            InstructionClass::Swap(SwapKind::ExactIn),
            Amounts::ExactIn {
                amount_in: specified_amount,
                minimum_amount_out: other_amount_threshold,
            },
        )
    } else {
        (
            InstructionClass::Swap(SwapKind::ExactOut),
            Amounts::ExactOut {
                maximum_amount_in: other_amount_threshold,
                amount_out: specified_amount,
            },
        )
    };

    Ok(DecodedInstruction {
        program: Program::RaydiumClmm,
        role: ProgramRole::Venue,
        name: if token_2022_layout { "swap_v2" } else { "swap" },
        class,
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
        amounts,
        evidence: SWAP_EVIDENCE,
    })
}

fn decode_clmm_router(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, 24)?;
    // Six fixed accounts are followed by at least one hop (seven accounts)
    // and one tick-array account.
    require_accounts(accounts, 14)?;

    Ok(DecodedInstruction {
        program: Program::RaydiumClmm,
        role: ProgramRole::Router,
        name: "swap_router_base_in",
        class: InstructionClass::Route,
        discriminator,
        accounts: AccountRoles {
            user_authority: Some(account(accounts, 0)?),
            user_source: Some(account(accounts, 1)?),
            input_mint: Some(account(accounts, 2)?),
            ..AccountRoles::default()
        },
        amounts: Amounts::ExactIn {
            amount_in: amount(data, 8, 16)?,
            minimum_amount_out: amount(data, 16, 24)?,
        },
        evidence: ROUTE_EVIDENCE,
    })
}

fn decode_cpmm(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let (name, class, amounts) = match discriminator {
        CPMM_SWAP_BASE_INPUT => {
            if let Err(reason) = require_data(data, 24) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                "swap_base_input",
                InstructionClass::Swap(SwapKind::ExactIn),
                Amounts::ExactIn {
                    amount_in: match amount(data, 8, 16) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    minimum_amount_out: match amount(data, 16, 24) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
            )
        }
        CPMM_SWAP_BASE_OUTPUT => {
            if let Err(reason) = require_data(data, 24) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                "swap_base_output",
                InstructionClass::Swap(SwapKind::ExactOut),
                Amounts::ExactOut {
                    maximum_amount_in: match amount(data, 8, 16) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    amount_out: match amount(data, 16, 24) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
            )
        }
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    if let Err(reason) = require_accounts(accounts, 13) {
        return DecodeOutcome::Malformed(reason);
    }
    match cpmm_roles(accounts) {
        Ok(accounts) => DecodeOutcome::Decoded(DecodedInstruction {
            program: Program::RaydiumCpmm,
            role: ProgramRole::Venue,
            name,
            class,
            discriminator,
            accounts,
            amounts,
            evidence: SWAP_EVIDENCE,
        }),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

fn cpmm_roles(accounts: &[CompactId]) -> Result<AccountRoles, MalformedReason> {
    Ok(AccountRoles {
        pool: Some(account(accounts, 3)?),
        authority: Some(account(accounts, 1)?),
        user_authority: Some(account(accounts, 0)?),
        user_source: Some(account(accounts, 4)?),
        user_destination: Some(account(accounts, 5)?),
        input_vault: Some(account(accounts, 6)?),
        output_vault: Some(account(accounts, 7)?),
        input_mint: Some(account(accounts, 10)?),
        output_mint: Some(account(accounts, 11)?),
        ..AccountRoles::default()
    })
}

fn decode_amm_v4(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let (name, class, amounts, compact_layout) = match discriminator {
        AMM_V4_SWAP_BASE_IN | AMM_V4_SWAP_BASE_IN_V2 => {
            if let Err(reason) = require_data(data, 17) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                if discriminator == AMM_V4_SWAP_BASE_IN {
                    "swap_base_in"
                } else {
                    "swap_base_in_v2"
                },
                InstructionClass::Swap(SwapKind::ExactIn),
                Amounts::ExactIn {
                    amount_in: match amount(data, 1, 9) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    minimum_amount_out: match amount(data, 9, 17) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
                discriminator == AMM_V4_SWAP_BASE_IN_V2,
            )
        }
        AMM_V4_SWAP_BASE_OUT | AMM_V4_SWAP_BASE_OUT_V2 => {
            if let Err(reason) = require_data(data, 17) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                if discriminator == AMM_V4_SWAP_BASE_OUT {
                    "swap_base_out"
                } else {
                    "swap_base_out_v2"
                },
                InstructionClass::Swap(SwapKind::ExactOut),
                Amounts::ExactOut {
                    maximum_amount_in: match amount(data, 1, 9) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    amount_out: match amount(data, 9, 17) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
                discriminator == AMM_V4_SWAP_BASE_OUT_V2,
            )
        }
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    let roles = if compact_layout {
        amm_v4_compact_roles(accounts)
    } else {
        if accounts.len() > 18 {
            return DecodeOutcome::Unsupported { discriminator };
        }
        amm_v4_orderbook_roles(accounts)
    };
    match roles {
        Ok(accounts) => DecodeOutcome::Decoded(DecodedInstruction {
            program: Program::RaydiumAmmV4,
            role: ProgramRole::Venue,
            name,
            class,
            discriminator,
            accounts,
            amounts,
            evidence: SWAP_EVIDENCE,
        }),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

fn amm_v4_compact_roles(accounts: &[CompactId]) -> Result<AccountRoles, MalformedReason> {
    require_accounts(accounts, 8)?;
    Ok(AccountRoles {
        pool: Some(account(accounts, 1)?),
        authority: Some(account(accounts, 2)?),
        user_authority: Some(account(accounts, 7)?),
        vault_a: Some(account(accounts, 3)?),
        vault_b: Some(account(accounts, 4)?),
        user_source: Some(account(accounts, 5)?),
        user_destination: Some(account(accounts, 6)?),
        ..AccountRoles::default()
    })
}

fn amm_v4_orderbook_roles(accounts: &[CompactId]) -> Result<AccountRoles, MalformedReason> {
    require_accounts(accounts, 17)?;
    // The old target-orders account is optional. The on-chain processor accepts
    // exactly 17 accounts without it or 18 accounts with it.
    let shift = usize::from(accounts.len() == 18);
    Ok(AccountRoles {
        pool: Some(account(accounts, 1)?),
        authority: Some(account(accounts, 2)?),
        user_authority: Some(account(accounts, 16 + shift)?),
        vault_a: Some(account(accounts, 4 + shift)?),
        vault_b: Some(account(accounts, 5 + shift)?),
        user_source: Some(account(accounts, 14 + shift)?),
        user_destination: Some(account(accounts, 15 + shift)?),
        ..AccountRoles::default()
    })
}

fn amount(data: &[u8], offset: usize, needed: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(MalformedReason::InstructionDataTooShort {
        needed,
        actual: data.len(),
    })
}

fn require_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    if data.len() < needed {
        return Err(MalformedReason::InstructionDataTooShort {
            needed,
            actual: data.len(),
        });
    }
    Ok(())
}

fn require_accounts(accounts: &[CompactId], needed: usize) -> Result<(), MalformedReason> {
    if accounts.len() < needed {
        return Err(MalformedReason::InstructionAccountsTooShort {
            needed,
            actual: accounts.len(),
        });
    }
    Ok(())
}

fn outcome(decoded: Result<DecodedInstruction, MalformedReason>) -> DecodeOutcome {
    match decoded {
        Ok(decoded) => DecodeOutcome::Decoded(decoded),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anchor_data(discriminator: Discriminator, first: u64, second: u64) -> [u8; 41] {
        let mut data = [0_u8; 41];
        data[..8].copy_from_slice(&discriminator.bytes);
        data[8..16].copy_from_slice(&first.to_le_bytes());
        data[16..24].copy_from_slice(&second.to_le_bytes());
        data
    }

    fn amm_data(tag: u8, first: u64, second: u64) -> [u8; 17] {
        let mut data = [0_u8; 17];
        data[0] = tag;
        data[1..9].copy_from_slice(&first.to_le_bytes());
        data[9..17].copy_from_slice(&second.to_le_bytes());
        data
    }

    #[test]
    fn clmm_swap_v2_decodes_exact_input_and_roles() {
        let mut data = anchor_data(CLMM_SWAP_V2, 42, 39);
        data[40] = 1;
        let accounts: [CompactId; 13] = core::array::from_fn(|index| index as CompactId + 100);

        let DecodeOutcome::Decoded(decoded) = decode(Program::RaydiumClmm, &data, &accounts) else {
            panic!("valid CLMM swap_v2 must decode");
        };
        assert_eq!(decoded.name, "swap_v2");
        assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactIn));
        assert_eq!(
            decoded.amounts,
            Amounts::ExactIn {
                amount_in: 42,
                minimum_amount_out: 39
            }
        );
        assert_eq!(decoded.accounts.pool, Some(102));
        assert_eq!(decoded.accounts.authority, None);
        assert_eq!(decoded.accounts.user_authority, Some(100));
        assert_eq!(decoded.accounts.user_source, Some(103));
        assert_eq!(decoded.accounts.user_destination, Some(104));
        assert_eq!(decoded.accounts.input_vault, Some(105));
        assert_eq!(decoded.accounts.output_vault, Some(106));
        assert_eq!(decoded.accounts.input_mint, Some(111));
        assert_eq!(decoded.accounts.output_mint, Some(112));
    }

    #[test]
    fn clmm_swap_decodes_exact_output() {
        let data = anchor_data(CLMM_SWAP, 50, 61);
        let accounts = [0; 10];
        let DecodeOutcome::Decoded(decoded) = decode(Program::RaydiumClmm, &data, &accounts) else {
            panic!("valid CLMM swap must decode");
        };
        assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactOut));
        assert_eq!(
            decoded.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 61,
                amount_out: 50
            }
        );
    }

    #[test]
    fn clmm_router_is_not_a_venue_swap() {
        let data = anchor_data(CLMM_SWAP_ROUTER_BASE_IN, 80, 72);
        let accounts: [CompactId; 14] = core::array::from_fn(|index| index as CompactId + 10);
        let DecodeOutcome::Decoded(decoded) = decode(Program::RaydiumClmm, &data[..24], &accounts)
        else {
            panic!("valid CLMM router instruction must decode");
        };
        assert_eq!(decoded.role, ProgramRole::Router);
        assert_eq!(decoded.class, InstructionClass::Route);
        assert!(decoded.evidence.contains(Evidence::ROUTE_CONTAINER));
    }

    #[test]
    fn cpmm_decodes_both_amount_modes() {
        let accounts: [CompactId; 13] = core::array::from_fn(|index| index as CompactId);
        let input = anchor_data(CPMM_SWAP_BASE_INPUT, 100, 90);
        let output = anchor_data(CPMM_SWAP_BASE_OUTPUT, 110, 95);

        let DecodeOutcome::Decoded(input) = decode(Program::RaydiumCpmm, &input[..24], &accounts)
        else {
            panic!("valid CPMM exact-input swap must decode");
        };
        let DecodeOutcome::Decoded(output) = decode(Program::RaydiumCpmm, &output[..24], &accounts)
        else {
            panic!("valid CPMM exact-output swap must decode");
        };
        assert_eq!(
            input.amounts,
            Amounts::ExactIn {
                amount_in: 100,
                minimum_amount_out: 90
            }
        );
        assert_eq!(
            output.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 110,
                amount_out: 95
            }
        );
        assert_eq!(input.accounts.pool, Some(3));
        assert_eq!(input.accounts.authority, Some(1));
        assert_eq!(input.accounts.user_authority, Some(0));
        assert_eq!(input.accounts.input_mint, Some(10));
        assert_eq!(input.accounts.output_mint, Some(11));
    }

    #[test]
    fn amm_v4_handles_legacy_optional_target_orders_and_v2() {
        let legacy_accounts: [CompactId; 17] = core::array::from_fn(|index| index as CompactId);
        let target_accounts: [CompactId; 18] = core::array::from_fn(|index| index as CompactId);
        let compact_accounts: [CompactId; 8] = core::array::from_fn(|index| index as CompactId);

        let DecodeOutcome::Decoded(legacy) = decode(
            Program::RaydiumAmmV4,
            &amm_data(9, 25, 20),
            &legacy_accounts,
        ) else {
            panic!("valid legacy AMM v4 swap must decode");
        };
        let DecodeOutcome::Decoded(with_target) = decode(
            Program::RaydiumAmmV4,
            &amm_data(11, 30, 21),
            &target_accounts,
        ) else {
            panic!("valid target-orders AMM v4 swap must decode");
        };
        let DecodeOutcome::Decoded(compact) = decode(
            Program::RaydiumAmmV4,
            &amm_data(16, 35, 22),
            &compact_accounts,
        ) else {
            panic!("valid compact AMM v4 swap must decode");
        };
        let DecodeOutcome::Decoded(compact_out) = decode(
            Program::RaydiumAmmV4,
            &amm_data(17, 40, 23),
            &compact_accounts,
        ) else {
            panic!("valid compact exact-output AMM v4 swap must decode");
        };

        assert_eq!(legacy.accounts.vault_a, Some(4));
        assert_eq!(legacy.accounts.user_source, Some(14));
        assert_eq!(legacy.accounts.user_authority, Some(16));
        assert_eq!(with_target.accounts.vault_a, Some(5));
        assert_eq!(with_target.accounts.user_source, Some(15));
        assert_eq!(with_target.accounts.user_authority, Some(17));
        assert_eq!(compact.name, "swap_base_in_v2");
        assert_eq!(compact.accounts.vault_a, Some(3));
        assert_eq!(compact.accounts.user_source, Some(5));
        assert_eq!(compact.accounts.user_authority, Some(7));
        assert_eq!(compact_out.name, "swap_base_out_v2");
        assert_eq!(
            compact_out.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 40,
                amount_out: 23
            }
        );
    }

    #[test]
    fn supported_variants_reject_truncated_data_and_accounts() {
        assert_eq!(
            decode(Program::RaydiumClmm, &CLMM_SWAP.bytes[..7], &[0; 10]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 8,
                actual: 7
            })
        );
        assert_eq!(
            decode(Program::RaydiumClmm, &CLMM_SWAP.bytes, &[0; 10]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 41,
                actual: 8
            })
        );
        assert_eq!(
            decode(Program::RaydiumCpmm, &CPMM_SWAP_BASE_INPUT.bytes, &[0; 13]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 24,
                actual: 8
            })
        );
        let data = anchor_data(CPMM_SWAP_BASE_INPUT, 1, 1);
        assert_eq!(
            decode(Program::RaydiumCpmm, &data[..24], &[0; 12]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 13,
                actual: 12
            })
        );
        assert_eq!(
            decode(Program::RaydiumAmmV4, &amm_data(9, 1, 1), &[0; 16]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 17,
                actual: 16
            })
        );
        assert_eq!(
            decode(Program::RaydiumAmmV4, &[9], &[0; 17]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 17,
                actual: 1
            })
        );
    }

    #[test]
    fn unknown_discriminators_are_unsupported() {
        let anchor_unknown = [255_u8; 8];
        assert_eq!(
            decode(Program::RaydiumClmm, &anchor_unknown, &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight(anchor_unknown)
            }
        );
        assert_eq!(
            decode(Program::RaydiumAmmV4, &[255], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::one(255)
            }
        );

        let mut invalid_bool = anchor_data(CLMM_SWAP, 1, 1);
        invalid_bool[40] = 2;
        assert_eq!(
            decode(Program::RaydiumClmm, &invalid_bool, &[0; 10]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 40 })
        );
    }
}

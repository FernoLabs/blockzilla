use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, SwapKind, account,
    anchor_discriminator, read_u8, read_u64_le,
};

const SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
const SWAP_ROUTER_BASE_IN: [u8; 8] = [69, 125, 115, 218, 245, 186, 242, 196];

pub(crate) fn decode(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let decoded = match discriminator.bytes {
        SWAP => decode_swap(data, accounts, discriminator, false),
        SWAP_V2 => decode_swap(data, accounts, discriminator, true),
        SWAP_ROUTER_BASE_IN => decode_router_base_in(data, accounts, discriminator),
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    match decoded {
        Ok(instruction) => DecodeOutcome::Decoded(instruction),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

fn decode_swap(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
    is_v2: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, 41)?;
    require_accounts(accounts, if is_v2 { 13 } else { 10 })?;

    let amount = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let threshold = read_u64_le(data, 16).ok_or(short_data(data, 24))?;
    let is_base_input = read_bool(data, 40)?;
    let amounts = if is_base_input {
        Amounts::ExactIn {
            amount_in: amount,
            minimum_amount_out: threshold,
        }
    } else {
        Amounts::ExactOut {
            maximum_amount_in: threshold,
            amount_out: amount,
        }
    };

    Ok(DecodedInstruction {
        program: Program::PancakeSwap,
        role: Program::PancakeSwap.role(),
        name: if is_v2 { "swap_v2" } else { "swap" },
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
            input_mint: if is_v2 {
                Some(account(accounts, 11)?)
            } else {
                None
            },
            output_mint: if is_v2 {
                Some(account(accounts, 12)?)
            } else {
                None
            },
            ..AccountRoles::default()
        },
        amounts,
        evidence: Evidence::ACCOUNT_LAYOUT
            .union(Evidence::AMOUNTS)
            .union(Evidence::TOKEN_FLOW_REQUIRED),
    })
}

fn decode_router_base_in(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, 24)?;
    // Six fixed accounts are followed by at least one seven-account hop and a
    // tick-array account.
    require_accounts(accounts, 14)?;
    let amount_in = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let minimum_amount_out = read_u64_le(data, 16).ok_or(short_data(data, 24))?;

    Ok(DecodedInstruction {
        program: Program::PancakeSwap,
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
            amount_in,
            minimum_amount_out,
        },
        evidence: Evidence::ACCOUNT_LAYOUT
            .union(Evidence::AMOUNTS)
            .union(Evidence::ROUTE_CONTAINER)
            .union(Evidence::TOKEN_FLOW_REQUIRED),
    })
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
fn read_bool(data: &[u8], offset: usize) -> Result<bool, MalformedReason> {
    match read_u8(data, offset) {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(short_data(data, offset.saturating_add(1))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids<const N: usize>() -> [CompactId; N] {
        core::array::from_fn(|index| index as CompactId)
    }

    #[test]
    fn decodes_swap_v2_exact_out_and_directional_accounts() {
        let mut data = [0_u8; 41];
        data[..8].copy_from_slice(&SWAP_V2);
        data[8..16].copy_from_slice(&600_u64.to_le_bytes());
        data[16..24].copy_from_slice(&700_u64.to_le_bytes());

        let outcome = decode(&data, &ids::<13>());
        let DecodeOutcome::Decoded(decoded) = outcome else {
            assert!(matches!(outcome, DecodeOutcome::Decoded(_)));
            return;
        };
        assert_eq!(decoded.role, Program::PancakeSwap.role());
        assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactOut));
        assert_eq!(decoded.accounts.pool, Some(2));
        assert_eq!(decoded.accounts.input_vault, Some(5));
        assert_eq!(decoded.accounts.output_vault, Some(6));
        assert_eq!(decoded.accounts.input_mint, Some(11));
        assert_eq!(decoded.accounts.output_mint, Some(12));
        assert_eq!(
            decoded.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 700,
                amount_out: 600,
            }
        );
    }

    #[test]
    fn matching_instruction_rejects_short_data_and_accounts() {
        assert_eq!(
            decode(&SWAP, &ids::<10>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 41,
                actual: 8,
            })
        );

        let mut data = [0_u8; 41];
        data[..8].copy_from_slice(&SWAP);
        assert_eq!(
            decode(&data, &ids::<9>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 10,
                actual: 9,
            })
        );
    }

    #[test]
    fn router_base_in_is_not_a_venue_swap() {
        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&SWAP_ROUTER_BASE_IN);
        data[8..16].copy_from_slice(&100_u64.to_le_bytes());
        data[16..24].copy_from_slice(&90_u64.to_le_bytes());
        let outcome = decode(&data, &ids::<14>());
        assert!(matches!(
            outcome,
            DecodeOutcome::Decoded(DecodedInstruction {
                role: ProgramRole::Router,
                class: InstructionClass::Route,
                amounts: Amounts::ExactIn {
                    amount_in: 100,
                    minimum_amount_out: 90,
                },
                ..
            })
        ));
    }

    #[test]
    fn rejects_unsupported_discriminator() {
        let data = [7_u8; 8];
        assert_eq!(
            decode(&data, &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight(data),
            }
        );
    }
}

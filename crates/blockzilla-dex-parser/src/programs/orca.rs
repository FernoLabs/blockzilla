use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, Program, SwapKind, account, anchor_discriminator, read_u8,
    read_u64_le,
};

const SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];
const TWO_HOP_SWAP: [u8; 8] = [195, 96, 237, 108, 68, 162, 219, 230];
const TWO_HOP_SWAP_V2: [u8; 8] = [186, 143, 209, 29, 254, 2, 194, 117];

pub(crate) fn decode(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let decoded = match discriminator.bytes {
        SWAP => decode_swap(data, accounts, discriminator, false),
        SWAP_V2 => decode_swap(data, accounts, discriminator, true),
        TWO_HOP_SWAP => decode_two_hop_swap(data, accounts, discriminator, false),
        TWO_HOP_SWAP_V2 => decode_two_hop_swap(data, accounts, discriminator, true),
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
    // The V1 and V2 prefixes have two u64 values, one u128 value, and two booleans.
    require_data(data, if is_v2 { 43 } else { 42 })?;
    let remaining_accounts = if is_v2 {
        optional_remaining_accounts(data, 42)?
    } else {
        0
    };

    let amount = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let threshold = read_u64_le(data, 16).ok_or(short_data(data, 24))?;
    let amount_is_input = read_bool(data, 40)?;
    let a_to_b = read_bool(data, 41)?;

    let (name, pool, user_authority, owner_a, vault_a, owner_b, vault_b, mint_a, mint_b) = if is_v2
    {
        require_accounts(accounts, 15_usize.saturating_add(remaining_accounts))?;
        (
            "swap_v2",
            account(accounts, 4)?,
            account(accounts, 3)?,
            account(accounts, 7)?,
            account(accounts, 8)?,
            account(accounts, 9)?,
            account(accounts, 10)?,
            Some(account(accounts, 5)?),
            Some(account(accounts, 6)?),
        )
    } else {
        require_accounts(accounts, 11)?;
        (
            "swap",
            account(accounts, 2)?,
            account(accounts, 1)?,
            account(accounts, 3)?,
            account(accounts, 4)?,
            account(accounts, 5)?,
            account(accounts, 6)?,
            None,
            None,
        )
    };

    let (user_source, user_destination, input_vault, output_vault, input_mint, output_mint) =
        if a_to_b {
            (owner_a, owner_b, vault_a, vault_b, mint_a, mint_b)
        } else {
            (owner_b, owner_a, vault_b, vault_a, mint_b, mint_a)
        };

    Ok(DecodedInstruction {
        program: Program::OrcaWhirlpool,
        role: Program::OrcaWhirlpool.role(),
        name,
        class: InstructionClass::Swap(if amount_is_input {
            SwapKind::ExactIn
        } else {
            SwapKind::ExactOut
        }),
        discriminator,
        accounts: AccountRoles {
            pool: Some(pool),
            user_authority: Some(user_authority),
            user_source: Some(user_source),
            user_destination: Some(user_destination),
            vault_a: Some(vault_a),
            vault_b: Some(vault_b),
            input_vault: Some(input_vault),
            output_vault: Some(output_vault),
            input_mint,
            output_mint,
            ..AccountRoles::default()
        },
        amounts: swap_amounts(amount, threshold, amount_is_input),
        evidence: venue_evidence(),
    })
}

fn decode_two_hop_swap(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
    is_v2: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, if is_v2 { 60 } else { 59 })?;
    let remaining_accounts = if is_v2 {
        optional_remaining_accounts(data, 59)?
    } else {
        0
    };

    let amount = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let threshold = read_u64_le(data, 16).ok_or(short_data(data, 24))?;
    let amount_is_input = read_bool(data, 24)?;
    let first_a_to_b = read_bool(data, 25)?;
    let second_a_to_b = read_bool(data, 26)?;

    let (
        name,
        pool,
        user_authority,
        user_source,
        user_destination,
        input_vault,
        output_vault,
        roles,
    ) = if is_v2 {
        require_accounts(accounts, 24_usize.saturating_add(remaining_accounts))?;
        let input_vault = account(accounts, 9)?;
        let output_vault = account(accounts, 12)?;
        (
            "two_hop_swap_v2",
            account(accounts, 0)?,
            account(accounts, 14)?,
            account(accounts, 8)?,
            account(accounts, 13)?,
            input_vault,
            output_vault,
            AccountRoles {
                second_pool: Some(account(accounts, 1)?),
                vault_a: Some(account(accounts, 9)?),
                vault_b: Some(account(accounts, 10)?),
                second_vault_a: Some(account(accounts, 11)?),
                second_vault_b: Some(account(accounts, 12)?),
                input_mint: Some(account(accounts, 2)?),
                intermediate_mint: Some(account(accounts, 3)?),
                output_mint: Some(account(accounts, 4)?),
                ..AccountRoles::default()
            },
        )
    } else {
        require_accounts(accounts, 20)?;
        let (user_source, input_vault) = if first_a_to_b {
            (account(accounts, 4)?, account(accounts, 5)?)
        } else {
            (account(accounts, 6)?, account(accounts, 7)?)
        };
        let (user_destination, output_vault) = if second_a_to_b {
            (account(accounts, 10)?, account(accounts, 11)?)
        } else {
            (account(accounts, 8)?, account(accounts, 9)?)
        };
        (
            "two_hop_swap",
            account(accounts, 2)?,
            account(accounts, 1)?,
            user_source,
            user_destination,
            input_vault,
            output_vault,
            AccountRoles {
                second_pool: Some(account(accounts, 3)?),
                vault_a: Some(account(accounts, 5)?),
                vault_b: Some(account(accounts, 7)?),
                second_vault_a: Some(account(accounts, 9)?),
                second_vault_b: Some(account(accounts, 11)?),
                ..AccountRoles::default()
            },
        )
    };

    Ok(DecodedInstruction {
        program: Program::OrcaWhirlpool,
        role: Program::OrcaWhirlpool.role(),
        name,
        class: InstructionClass::Swap(if amount_is_input {
            SwapKind::TwoHopExactIn
        } else {
            SwapKind::TwoHopExactOut
        }),
        discriminator,
        accounts: AccountRoles {
            pool: Some(pool),
            user_authority: Some(user_authority),
            user_source: Some(user_source),
            user_destination: Some(user_destination),
            input_vault: Some(input_vault),
            output_vault: Some(output_vault),
            ..roles
        },
        amounts: swap_amounts(amount, threshold, amount_is_input),
        evidence: venue_evidence(),
    })
}

#[inline]
fn swap_amounts(amount: u64, threshold: u64, amount_is_input: bool) -> Amounts {
    if amount_is_input {
        Amounts::ExactIn {
            amount_in: amount,
            minimum_amount_out: threshold,
        }
    } else {
        Amounts::ExactOut {
            maximum_amount_in: threshold,
            amount_out: amount,
        }
    }
}

#[inline]
fn venue_evidence() -> Evidence {
    Evidence::ACCOUNT_LAYOUT
        .union(Evidence::AMOUNTS)
        .union(Evidence::TOKEN_FLOW_REQUIRED)
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

fn optional_remaining_accounts(
    data: &[u8],
    option_offset: usize,
) -> Result<usize, MalformedReason> {
    let tag_end = option_offset.saturating_add(1);
    require_data(data, tag_end)?;
    match read_u8(data, option_offset).ok_or(short_data(data, tag_end))? {
        0 => return Ok(0),
        1 => {}
        _ => {
            return Err(MalformedReason::InvalidInstructionData {
                offset: option_offset,
            });
        }
    }

    let entries_offset = tag_end.saturating_add(4);
    require_data(data, entries_offset)?;
    let count = read_u32_le(data, tag_end).ok_or(short_data(data, entries_offset))? as usize;
    let minimum_end = entries_offset.saturating_add(count.saturating_mul(2));
    require_data(data, minimum_end)?;

    let mut cursor = entries_offset;
    let mut account_count = 0_usize;
    let mut index = 0_usize;
    while index < count {
        // Whirlpool AccountsType variants are unit variants. Each slice is
        // one variant byte followed by one account-count byte.
        let entry_end = cursor.saturating_add(2);
        require_data(data, entry_end)?;
        let accounts_type = read_u8(data, cursor).ok_or(short_data(data, entry_end))?;
        if accounts_type > 12 {
            return Err(MalformedReason::InvalidInstructionData { offset: cursor });
        }
        let length =
            read_u8(data, cursor.saturating_add(1)).ok_or(short_data(data, entry_end))? as usize;
        account_count = account_count.saturating_add(length);
        cursor = entry_end;
        index += 1;
    }
    Ok(account_count)
}

#[inline]
fn read_u32_le(data: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    Some(u32::from_le_bytes(data.get(offset..end)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids<const N: usize>() -> [CompactId; N] {
        core::array::from_fn(|index| index as CompactId)
    }

    #[test]
    fn decodes_swap_v2_exact_in_with_directional_roles() {
        let mut data = [0_u8; 43];
        data[..8].copy_from_slice(&SWAP_V2);
        data[8..16].copy_from_slice(&500_u64.to_le_bytes());
        data[16..24].copy_from_slice(&450_u64.to_le_bytes());
        data[40] = 1;
        data[41] = 0;

        let outcome = decode(&data, &ids::<15>());
        let DecodeOutcome::Decoded(decoded) = outcome else {
            assert!(matches!(outcome, DecodeOutcome::Decoded(_)));
            return;
        };
        assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactIn));
        assert_eq!(decoded.accounts.user_source, Some(9));
        assert_eq!(decoded.accounts.user_destination, Some(7));
        assert_eq!(decoded.accounts.input_vault, Some(10));
        assert_eq!(decoded.accounts.output_vault, Some(8));
        assert_eq!(decoded.accounts.input_mint, Some(6));
        assert_eq!(decoded.accounts.output_mint, Some(5));
        assert_eq!(
            decoded.amounts,
            Amounts::ExactIn {
                amount_in: 500,
                minimum_amount_out: 450,
            }
        );
    }

    #[test]
    fn decodes_two_hop_v2_exact_out() {
        let mut data = [0_u8; 60];
        data[..8].copy_from_slice(&TWO_HOP_SWAP_V2);
        data[8..16].copy_from_slice(&90_u64.to_le_bytes());
        data[16..24].copy_from_slice(&100_u64.to_le_bytes());

        let outcome = decode(&data, &ids::<24>());
        assert!(matches!(
            outcome,
            DecodeOutcome::Decoded(DecodedInstruction {
                class: InstructionClass::Swap(SwapKind::TwoHopExactOut),
                amounts: Amounts::ExactOut {
                    maximum_amount_in: 100,
                    amount_out: 90,
                },
                ..
            })
        ));
    }

    #[test]
    fn matching_instruction_rejects_short_data_and_accounts() {
        assert_eq!(
            decode(&SWAP, &ids::<11>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 42,
                actual: 8,
            })
        );

        let mut data = [0_u8; 42];
        data[..8].copy_from_slice(&SWAP);
        assert_eq!(
            decode(&data, &ids::<10>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 11,
                actual: 10,
            })
        );
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

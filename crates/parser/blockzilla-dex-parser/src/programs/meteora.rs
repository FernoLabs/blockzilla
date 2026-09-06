use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, Program, SwapKind, account, anchor_discriminator, read_u8,
    read_u64_le,
};

const SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SWAP_V2: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];

const DLMM_SWAP_EXACT_OUT: [u8; 8] = [250, 73, 101, 33, 38, 207, 75, 184];
const DLMM_SWAP_EXACT_OUT_V2: [u8; 8] = [43, 215, 247, 132, 137, 60, 243, 81];
const DLMM_SWAP_WITH_PRICE_IMPACT: [u8; 8] = [56, 173, 230, 208, 173, 228, 156, 205];
const DLMM_SWAP_WITH_PRICE_IMPACT_V2: [u8; 8] = [74, 98, 192, 214, 177, 51, 75, 51];

pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    match program {
        Program::MeteoraDlmm => decode_dlmm(data, accounts, discriminator),
        Program::MeteoraDammV2 => decode_damm_v2(data, accounts, discriminator),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_dlmm(data: &[u8], accounts: &[CompactId], discriminator: Discriminator) -> DecodeOutcome {
    let decoded = match discriminator.bytes {
        SWAP => decode_dlmm_amounts(
            data,
            accounts,
            discriminator,
            "swap",
            SwapKind::ExactIn,
            false,
        ),
        SWAP_V2 => decode_dlmm_amounts(
            data,
            accounts,
            discriminator,
            "swap2",
            SwapKind::ExactIn,
            true,
        ),
        DLMM_SWAP_EXACT_OUT => decode_dlmm_amounts(
            data,
            accounts,
            discriminator,
            "swap_exact_out",
            SwapKind::ExactOut,
            false,
        ),
        DLMM_SWAP_EXACT_OUT_V2 => decode_dlmm_amounts(
            data,
            accounts,
            discriminator,
            "swap_exact_out2",
            SwapKind::ExactOut,
            true,
        ),
        DLMM_SWAP_WITH_PRICE_IMPACT => decode_dlmm_price_impact(
            data,
            accounts,
            discriminator,
            "swap_with_price_impact",
            false,
        ),
        DLMM_SWAP_WITH_PRICE_IMPACT_V2 => decode_dlmm_price_impact(
            data,
            accounts,
            discriminator,
            "swap_with_price_impact2",
            true,
        ),
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    result(decoded)
}

fn decode_dlmm_amounts(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
    name: &'static str,
    kind: SwapKind,
    is_v2: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, 24)?;
    let remaining_accounts = if is_v2 {
        dlmm_remaining_accounts(data, 24)?
    } else {
        0
    };
    let fixed_accounts = if is_v2 { 16_usize } else { 15_usize };
    require_accounts(accounts, fixed_accounts.saturating_add(remaining_accounts))?;

    let first = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let second = read_u64_le(data, 16).ok_or(short_data(data, 24))?;
    let amounts = match kind {
        SwapKind::ExactIn => Amounts::ExactIn {
            amount_in: first,
            minimum_amount_out: second,
        },
        SwapKind::ExactOut => Amounts::ExactOut {
            maximum_amount_in: first,
            amount_out: second,
        },
        _ => Amounts::Unknown,
    };

    dlmm_instruction(accounts, discriminator, name, kind, amounts, true)
}

fn decode_dlmm_price_impact(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
    name: &'static str,
    is_v2: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    // active_id is Option<i32>. It uses one tag byte and four bytes when present.
    require_data(data, 17)?;
    let core_len = match read_u8(data, 16).ok_or(short_data(data, 17))? {
        0 => 19,
        1 => 23,
        _ => return Err(MalformedReason::InvalidInstructionData { offset: 16 }),
    };
    require_data(data, core_len)?;
    let remaining_accounts = if is_v2 {
        dlmm_remaining_accounts(data, core_len)?
    } else {
        0
    };
    let fixed_accounts = if is_v2 { 16_usize } else { 15_usize };
    require_accounts(accounts, fixed_accounts.saturating_add(remaining_accounts))?;

    // The instruction has amount_in but no minimum output value. The common
    // amount type cannot state this without inventing a threshold.
    dlmm_instruction(
        accounts,
        discriminator,
        name,
        SwapKind::ExactIn,
        Amounts::Unknown,
        false,
    )
}

fn dlmm_instruction(
    accounts: &[CompactId],
    discriminator: Discriminator,
    name: &'static str,
    kind: SwapKind,
    amounts: Amounts,
    has_amounts: bool,
) -> Result<DecodedInstruction, MalformedReason> {
    Ok(DecodedInstruction {
        program: Program::MeteoraDlmm,
        role: Program::MeteoraDlmm.role(),
        name,
        class: InstructionClass::Swap(kind),
        discriminator,
        accounts: AccountRoles {
            pool: Some(account(accounts, 0)?),
            user_authority: Some(account(accounts, 10)?),
            user_source: Some(account(accounts, 4)?),
            user_destination: Some(account(accounts, 5)?),
            vault_a: Some(account(accounts, 2)?),
            vault_b: Some(account(accounts, 3)?),
            // host_fee_in is an Anchor optional account. Its slot can contain
            // the program-ID sentinel, so token flow must resolve it.
            fee_account: None,
            ..AccountRoles::default()
        },
        amounts,
        evidence: venue_evidence(has_amounts),
    })
}

fn decode_damm_v2(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
) -> DecodeOutcome {
    match discriminator.bytes {
        SWAP => result(decode_damm_swap(data, accounts, discriminator)),
        SWAP_V2 => decode_damm_swap_v2(data, accounts, discriminator),
        _ => DecodeOutcome::Unsupported { discriminator },
    }
}

fn decode_damm_swap(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
) -> Result<DecodedInstruction, MalformedReason> {
    require_data(data, 24)?;
    require_accounts(accounts, 14)?;
    let amount_in = read_u64_le(data, 8).ok_or(short_data(data, 16))?;
    let minimum_amount_out = read_u64_le(data, 16).ok_or(short_data(data, 24))?;
    damm_instruction(
        accounts,
        discriminator,
        "swap",
        SwapKind::ExactIn,
        Amounts::ExactIn {
            amount_in,
            minimum_amount_out,
        },
    )
}

fn decode_damm_swap_v2(
    data: &[u8],
    accounts: &[CompactId],
    discriminator: Discriminator,
) -> DecodeOutcome {
    if let Err(reason) = require_data(data, 25) {
        return DecodeOutcome::Malformed(reason);
    }
    if let Err(reason) = require_accounts(accounts, 14) {
        return DecodeOutcome::Malformed(reason);
    }

    let Some(amount_0) = read_u64_le(data, 8) else {
        return DecodeOutcome::Malformed(short_data(data, 16));
    };
    let Some(amount_1) = read_u64_le(data, 16) else {
        return DecodeOutcome::Malformed(short_data(data, 24));
    };
    let Some(mode) = read_u8(data, 24) else {
        return DecodeOutcome::Malformed(short_data(data, 25));
    };

    let (name, kind, amounts) = match mode {
        0 => (
            "swap2",
            SwapKind::ExactIn,
            Amounts::ExactIn {
                amount_in: amount_0,
                minimum_amount_out: amount_1,
            },
        ),
        2 => (
            "swap2",
            SwapKind::ExactOut,
            Amounts::ExactOut {
                maximum_amount_in: amount_1,
                amount_out: amount_0,
            },
        ),
        1 => (
            "swap2",
            SwapKind::PartialFill,
            Amounts::PartialFill {
                amount_in: amount_0,
                minimum_amount_out: amount_1,
            },
        ),
        _ => {
            return DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData {
                offset: 24,
            });
        }
    };

    result(damm_instruction(
        accounts,
        discriminator,
        name,
        kind,
        amounts,
    ))
}

fn damm_instruction(
    accounts: &[CompactId],
    discriminator: Discriminator,
    name: &'static str,
    kind: SwapKind,
    amounts: Amounts,
) -> Result<DecodedInstruction, MalformedReason> {
    Ok(DecodedInstruction {
        program: Program::MeteoraDammV2,
        role: Program::MeteoraDammV2.role(),
        name,
        class: InstructionClass::Swap(kind),
        discriminator,
        accounts: AccountRoles {
            pool: Some(account(accounts, 1)?),
            authority: Some(account(accounts, 0)?),
            user_authority: Some(account(accounts, 8)?),
            user_source: Some(account(accounts, 2)?),
            user_destination: Some(account(accounts, 3)?),
            vault_a: Some(account(accounts, 4)?),
            vault_b: Some(account(accounts, 5)?),
            // referral_token_account is optional and can be a sentinel.
            fee_account: None,
            ..AccountRoles::default()
        },
        amounts,
        evidence: venue_evidence(true),
    })
}

#[inline]
fn result(decoded: Result<DecodedInstruction, MalformedReason>) -> DecodeOutcome {
    match decoded {
        Ok(instruction) => DecodeOutcome::Decoded(instruction),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

#[inline]
fn venue_evidence(has_amounts: bool) -> Evidence {
    let evidence = Evidence::ACCOUNT_LAYOUT.union(Evidence::TOKEN_FLOW_REQUIRED);
    if has_amounts {
        evidence.union(Evidence::AMOUNTS)
    } else {
        evidence
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

fn dlmm_remaining_accounts(data: &[u8], vector_offset: usize) -> Result<usize, MalformedReason> {
    let entries_offset = vector_offset.saturating_add(4);
    require_data(data, entries_offset)?;
    let count = read_u32_le(data, vector_offset).ok_or(short_data(data, entries_offset))? as usize;
    let minimum_end = entries_offset.saturating_add(count.saturating_mul(2));
    require_data(data, minimum_end)?;

    let mut cursor = entries_offset;
    let mut account_count = 0_usize;
    let mut index = 0_usize;
    while index < count {
        let variant_end = cursor.saturating_add(1);
        require_data(data, variant_end)?;
        let accounts_type = read_u8(data, cursor).ok_or(short_data(data, variant_end))?;
        let payload_len = match accounts_type {
            0..=2 | 4 => 0_usize,
            3 => 1_usize,
            _ => return Err(MalformedReason::InvalidInstructionData { offset: cursor }),
        };
        let length_offset = variant_end.saturating_add(payload_len);
        let entry_end = length_offset.saturating_add(1);
        require_data(data, entry_end)?;
        let length = read_u8(data, length_offset).ok_or(short_data(data, entry_end))? as usize;
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
    fn decodes_dlmm_exact_in_without_guessing_vault_direction() {
        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&SWAP);
        data[8..16].copy_from_slice(&1_000_u64.to_le_bytes());
        data[16..24].copy_from_slice(&900_u64.to_le_bytes());

        let outcome = decode(Program::MeteoraDlmm, &data, &ids::<15>());
        let DecodeOutcome::Decoded(decoded) = outcome else {
            assert!(matches!(outcome, DecodeOutcome::Decoded(_)));
            return;
        };
        assert_eq!(decoded.accounts.pool, Some(0));
        assert_eq!(decoded.accounts.vault_a, Some(2));
        assert_eq!(decoded.accounts.vault_b, Some(3));
        assert_eq!(decoded.accounts.input_vault, None);
        assert_eq!(
            decoded.amounts,
            Amounts::ExactIn {
                amount_in: 1_000,
                minimum_amount_out: 900,
            }
        );
    }

    #[test]
    fn decodes_damm_swap_2_exact_out() {
        let mut data = [0_u8; 25];
        data[..8].copy_from_slice(&SWAP_V2);
        data[8..16].copy_from_slice(&700_u64.to_le_bytes());
        data[16..24].copy_from_slice(&800_u64.to_le_bytes());
        data[24] = 2;

        let outcome = decode(Program::MeteoraDammV2, &data, &ids::<14>());
        assert!(matches!(
            outcome,
            DecodeOutcome::Decoded(DecodedInstruction {
                class: InstructionClass::Swap(SwapKind::ExactOut),
                amounts: Amounts::ExactOut {
                    maximum_amount_in: 800,
                    amount_out: 700,
                },
                ..
            })
        ));
    }

    #[test]
    fn decodes_damm_partial_fill_without_mislabeling_it() {
        let mut data = [0_u8; 25];
        data[..8].copy_from_slice(&SWAP_V2);
        data[8..16].copy_from_slice(&500_u64.to_le_bytes());
        data[16..24].copy_from_slice(&450_u64.to_le_bytes());
        data[24] = 1;
        let outcome = decode(Program::MeteoraDammV2, &data, &ids::<14>());
        assert!(matches!(
            outcome,
            DecodeOutcome::Decoded(DecodedInstruction {
                class: InstructionClass::Swap(SwapKind::PartialFill),
                amounts: Amounts::PartialFill {
                    amount_in: 500,
                    minimum_amount_out: 450,
                },
                ..
            })
        ));
    }

    #[test]
    fn matching_instruction_rejects_short_data_and_accounts() {
        assert_eq!(
            decode(Program::MeteoraDlmm, &SWAP, &ids::<15>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 24,
                actual: 8,
            })
        );

        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&SWAP);
        assert_eq!(
            decode(Program::MeteoraDlmm, &data, &ids::<14>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 15,
                actual: 14,
            })
        );
    }

    #[test]
    fn rejects_unsupported_discriminator() {
        let data = [7_u8; 8];
        assert_eq!(
            decode(Program::MeteoraDlmm, &data, &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight(data),
            }
        );
    }
}

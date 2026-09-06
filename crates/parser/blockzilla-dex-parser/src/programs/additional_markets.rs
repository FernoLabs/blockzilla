use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, OrderKind, Program, ProgramRole, SwapKind, account,
    anchor_discriminator, read_u8, read_u64_le,
};

const PUMP_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const PUMP_BUY_EXACT_QUOTE_IN_V2: [u8; 8] = [194, 171, 28, 70, 104, 77, 91, 47];
const PUMP_BUY_EXACT_SOL_IN: [u8; 8] = [56, 252, 116, 8, 158, 223, 205, 95];
const PUMP_BUY_V2: [u8; 8] = [184, 23, 238, 97, 103, 197, 211, 61];
const PUMP_SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
const PUMP_SELL_V2: [u8; 8] = [93, 246, 130, 60, 231, 233, 64, 178];

const LAUNCH_BUY_EXACT_IN: [u8; 8] = [250, 234, 13, 123, 213, 156, 19, 236];
const LAUNCH_BUY_EXACT_OUT: [u8; 8] = [24, 211, 116, 40, 105, 3, 153, 56];
const LAUNCH_SELL_EXACT_IN: [u8; 8] = [149, 39, 222, 155, 211, 124, 152, 26];
const LAUNCH_SELL_EXACT_OUT: [u8; 8] = [95, 200, 71, 34, 8, 9, 11, 166];

const OPENBOOK_PLACE_TAKE_ORDER: [u8; 8] = [3, 44, 71, 3, 26, 199, 203, 85];

const SWAP_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const ORDER_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT.union(Evidence::TOKEN_FLOW_REQUIRED);

pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let decoded = match program {
        Program::PumpFun => decode_pump(discriminator, data, accounts),
        Program::RaydiumLaunchlab => decode_launch(discriminator, data, accounts),
        Program::OpenBookV2 => decode_openbook(discriminator, data, accounts),
        _ => return DecodeOutcome::UnknownProgram,
    };
    match decoded {
        Ok(Some(value)) => DecodeOutcome::Decoded(value),
        Ok(None) => DecodeOutcome::Unsupported { discriminator },
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

fn decode_pump(
    discriminator: Discriminator,
    data: &[u8],
    accounts: &[CompactId],
) -> Result<Option<DecodedInstruction>, MalformedReason> {
    let (name, kind, amounts, roles) = match discriminator.bytes {
        PUMP_BUY => (
            "Buy",
            SwapKind::ExactOut,
            {
                require_pump_legacy(data, accounts, true)?;
                exact_out(data)?
            },
            pump_legacy_roles(accounts, true)?,
        ),
        PUMP_BUY_EXACT_SOL_IN => (
            "BuyExactSolIn",
            SwapKind::ExactIn,
            {
                require_current_pump(data, accounts, 16, true)?;
                exact_in(data)?
            },
            pump_legacy_roles(accounts, true)?,
        ),
        PUMP_SELL => (
            "Sell",
            SwapKind::ExactIn,
            {
                require_pump_legacy(data, accounts, false)?;
                exact_in(data)?
            },
            pump_legacy_roles(accounts, false)?,
        ),
        PUMP_BUY_EXACT_QUOTE_IN_V2 => (
            "BuyExactQuoteInV2",
            SwapKind::ExactIn,
            {
                require_current_pump(data, accounts, 27, false)?;
                exact_in(data)?
            },
            pump_v2_roles(accounts, true)?,
        ),
        PUMP_BUY_V2 => (
            "BuyV2",
            SwapKind::ExactOut,
            {
                require_current_pump(data, accounts, 27, false)?;
                exact_out(data)?
            },
            pump_v2_roles(accounts, true)?,
        ),
        PUMP_SELL_V2 => (
            "SellV2",
            SwapKind::ExactIn,
            {
                require_current_pump(data, accounts, 26, false)?;
                exact_in(data)?
            },
            pump_v2_roles(accounts, false)?,
        ),
        _ => return Ok(None),
    };
    Ok(Some(DecodedInstruction {
        program: Program::PumpFun,
        role: ProgramRole::Venue,
        name,
        class: InstructionClass::Swap(kind),
        discriminator,
        accounts: roles,
        amounts,
        evidence: SWAP_EVIDENCE,
    }))
}

fn require_pump_legacy(
    data: &[u8],
    accounts: &[CompactId],
    has_current_bool: bool,
) -> Result<(), MalformedReason> {
    if data.len() < 24 {
        return Err(MalformedReason::InstructionDataTooShort {
            needed: 24,
            actual: data.len(),
        });
    }
    let required_accounts = if has_current_bool && data.len() >= 25 {
        read_borsh_bool(data, 24)?;
        16
    } else {
        // Verified historical buy/sell layouts both contain 12 accounts.
        12
    };
    require_accounts(accounts, required_accounts)
}

fn require_current_pump(
    data: &[u8],
    accounts: &[CompactId],
    required_accounts: usize,
    has_bool: bool,
) -> Result<(), MalformedReason> {
    let needed = if has_bool { 25 } else { 24 };
    if data.len() < needed {
        return Err(MalformedReason::InstructionDataTooShort {
            needed,
            actual: data.len(),
        });
    }
    if has_bool {
        read_borsh_bool(data, 24)?;
    }
    require_accounts(accounts, required_accounts)
}

fn read_borsh_bool(data: &[u8], offset: usize) -> Result<bool, MalformedReason> {
    match read_u8(data, offset) {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(MalformedReason::InstructionDataTooShort {
            needed: offset.saturating_add(1),
            actual: data.len(),
        }),
    }
}

fn require_accounts(accounts: &[CompactId], needed: usize) -> Result<(), MalformedReason> {
    if accounts.len() < needed {
        Err(MalformedReason::InstructionAccountsTooShort {
            needed,
            actual: accounts.len(),
        })
    } else {
        Ok(())
    }
}

fn pump_legacy_roles(
    accounts: &[CompactId],
    is_buy: bool,
) -> Result<AccountRoles, MalformedReason> {
    let fee = account(accounts, 1)?;
    let mint = account(accounts, 2)?;
    let pool = account(accounts, 3)?;
    let token_vault = account(accounts, 4)?;
    let user_token = account(accounts, 5)?;
    let user = account(accounts, 6)?;
    Ok(if is_buy {
        AccountRoles {
            pool: Some(pool),
            user_authority: Some(user),
            user_source: Some(user),
            user_destination: Some(user_token),
            vault_a: Some(token_vault),
            vault_b: Some(pool),
            input_vault: Some(pool),
            output_vault: Some(token_vault),
            output_mint: Some(mint),
            fee_account: Some(fee),
            ..AccountRoles::default()
        }
    } else {
        AccountRoles {
            pool: Some(pool),
            user_authority: Some(user),
            user_source: Some(user_token),
            user_destination: Some(user),
            vault_a: Some(token_vault),
            vault_b: Some(pool),
            input_vault: Some(token_vault),
            output_vault: Some(pool),
            input_mint: Some(mint),
            fee_account: Some(fee),
            ..AccountRoles::default()
        }
    })
}

fn pump_v2_roles(accounts: &[CompactId], is_buy: bool) -> Result<AccountRoles, MalformedReason> {
    let base_mint = account(accounts, 1)?;
    let quote_mint = account(accounts, 2)?;
    let fee = account(accounts, 7)?;
    let pool = account(accounts, 10)?;
    let base_vault = account(accounts, 11)?;
    let quote_vault = account(accounts, 12)?;
    let user = account(accounts, 13)?;
    let user_base = account(accounts, 14)?;
    let user_quote = account(accounts, 15)?;
    Ok(if is_buy {
        AccountRoles {
            pool: Some(pool),
            user_authority: Some(user),
            user_source: Some(user_quote),
            user_destination: Some(user_base),
            vault_a: Some(base_vault),
            vault_b: Some(quote_vault),
            input_vault: Some(quote_vault),
            output_vault: Some(base_vault),
            input_mint: Some(quote_mint),
            output_mint: Some(base_mint),
            fee_account: Some(fee),
            ..AccountRoles::default()
        }
    } else {
        AccountRoles {
            pool: Some(pool),
            user_authority: Some(user),
            user_source: Some(user_base),
            user_destination: Some(user_quote),
            vault_a: Some(base_vault),
            vault_b: Some(quote_vault),
            input_vault: Some(base_vault),
            output_vault: Some(quote_vault),
            input_mint: Some(base_mint),
            output_mint: Some(quote_mint),
            fee_account: Some(fee),
            ..AccountRoles::default()
        }
    })
}

fn decode_launch(
    discriminator: Discriminator,
    data: &[u8],
    accounts: &[CompactId],
) -> Result<Option<DecodedInstruction>, MalformedReason> {
    if matches!(
        discriminator.bytes,
        LAUNCH_BUY_EXACT_IN | LAUNCH_BUY_EXACT_OUT | LAUNCH_SELL_EXACT_IN | LAUNCH_SELL_EXACT_OUT
    ) {
        if data.len() < 32 {
            return Err(MalformedReason::InstructionDataTooShort {
                needed: 32,
                actual: data.len(),
            });
        }
        require_accounts(accounts, 15)?;
    }
    let (name, kind, amounts, is_buy) = match discriminator.bytes {
        LAUNCH_BUY_EXACT_IN => ("BuyExactIn", SwapKind::ExactIn, exact_in(data)?, true),
        LAUNCH_BUY_EXACT_OUT => ("BuyExactOut", SwapKind::ExactOut, exact_out(data)?, true),
        LAUNCH_SELL_EXACT_IN => ("SellExactIn", SwapKind::ExactIn, exact_in(data)?, false),
        LAUNCH_SELL_EXACT_OUT => ("SellExactOut", SwapKind::ExactOut, exact_out(data)?, false),
        _ => return Ok(None),
    };
    let authority = account(accounts, 1)?;
    let payer = account(accounts, 0)?;
    let pool = account(accounts, 4)?;
    let user_base = account(accounts, 5)?;
    let user_quote = account(accounts, 6)?;
    let base_vault = account(accounts, 7)?;
    let quote_vault = account(accounts, 8)?;
    let base_mint = account(accounts, 9)?;
    let quote_mint = account(accounts, 10)?;
    let roles = if is_buy {
        AccountRoles {
            pool: Some(pool),
            authority: Some(authority),
            user_authority: Some(payer),
            user_source: Some(user_quote),
            user_destination: Some(user_base),
            vault_a: Some(base_vault),
            vault_b: Some(quote_vault),
            input_vault: Some(quote_vault),
            output_vault: Some(base_vault),
            input_mint: Some(quote_mint),
            output_mint: Some(base_mint),
            fee_account: None,
            ..AccountRoles::default()
        }
    } else {
        AccountRoles {
            pool: Some(pool),
            authority: Some(authority),
            user_authority: Some(payer),
            user_source: Some(user_base),
            user_destination: Some(user_quote),
            vault_a: Some(base_vault),
            vault_b: Some(quote_vault),
            input_vault: Some(base_vault),
            output_vault: Some(quote_vault),
            input_mint: Some(base_mint),
            output_mint: Some(quote_mint),
            fee_account: None,
            ..AccountRoles::default()
        }
    };
    Ok(Some(DecodedInstruction {
        program: Program::RaydiumLaunchlab,
        role: ProgramRole::Venue,
        name,
        class: InstructionClass::Swap(kind),
        discriminator,
        accounts: roles,
        amounts,
        evidence: SWAP_EVIDENCE,
    }))
}

fn decode_openbook(
    discriminator: Discriminator,
    data: &[u8],
    accounts: &[CompactId],
) -> Result<Option<DecodedInstruction>, MalformedReason> {
    if discriminator.bytes != OPENBOOK_PLACE_TAKE_ORDER {
        return Ok(None);
    }
    if data.len() < 35 {
        return Err(MalformedReason::InstructionDataTooShort {
            needed: 35,
            actual: data.len(),
        });
    }
    require_accounts(accounts, 16)?;
    let side = read_u8(data, 8).ok_or(MalformedReason::InstructionDataTooShort {
        needed: 35,
        actual: data.len(),
    })?;
    let authority = account(accounts, 3)?;
    let signer = account(accounts, 0)?;
    let market = account(accounts, 2)?;
    let base_vault = account(accounts, 6)?;
    let quote_vault = account(accounts, 7)?;
    let user_base = account(accounts, 9)?;
    let user_quote = account(accounts, 10)?;
    let directional = match side {
        0 => (user_quote, user_base, quote_vault, base_vault),
        1 => (user_base, user_quote, base_vault, quote_vault),
        _ => return Err(MalformedReason::InvalidInstructionData { offset: 8 }),
    };
    Ok(Some(DecodedInstruction {
        program: Program::OpenBookV2,
        role: ProgramRole::Venue,
        name: "PlaceTakeOrder",
        class: InstructionClass::Order(OrderKind::PlaceTake),
        discriminator,
        accounts: AccountRoles {
            pool: Some(market),
            authority: Some(authority),
            user_authority: Some(signer),
            user_source: Some(directional.0),
            user_destination: Some(directional.1),
            vault_a: Some(base_vault),
            vault_b: Some(quote_vault),
            input_vault: Some(directional.2),
            output_vault: Some(directional.3),
            ..AccountRoles::default()
        },
        amounts: Amounts::Unknown,
        evidence: ORDER_EVIDENCE,
    }))
}

fn exact_in(data: &[u8]) -> Result<Amounts, MalformedReason> {
    let amount_in = read_u64_le(data, 8).ok_or(MalformedReason::InstructionDataTooShort {
        needed: 24,
        actual: data.len(),
    })?;
    let minimum_amount_out =
        read_u64_le(data, 16).ok_or(MalformedReason::InstructionDataTooShort {
            needed: 24,
            actual: data.len(),
        })?;
    Ok(Amounts::ExactIn {
        amount_in,
        minimum_amount_out,
    })
}

fn exact_out(data: &[u8]) -> Result<Amounts, MalformedReason> {
    let amount_out = read_u64_le(data, 8).ok_or(MalformedReason::InstructionDataTooShort {
        needed: 24,
        actual: data.len(),
    })?;
    let maximum_amount_in =
        read_u64_le(data, 16).ok_or(MalformedReason::InstructionDataTooShort {
            needed: 24,
            actual: data.len(),
        })?;
    Ok(Amounts::ExactOut {
        maximum_amount_in,
        amount_out,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instruction(discriminator: [u8; 8], first: u64, second: u64) -> [u8; 24] {
        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&discriminator);
        data[8..16].copy_from_slice(&first.to_le_bytes());
        data[16..].copy_from_slice(&second.to_le_bytes());
        data
    }

    #[test]
    fn pump_buy_is_exact_out_and_directional() {
        let data = instruction(PUMP_BUY, 11, 22);
        let accounts: Vec<_> = (100..112).collect();
        let DecodeOutcome::Decoded(decoded) = decode(Program::PumpFun, &data, &accounts) else {
            panic!("expected Pump.fun buy")
        };
        assert_eq!(
            decoded.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 22,
                amount_out: 11
            }
        );
        assert_eq!(decoded.accounts.pool, Some(103));
        assert_eq!(decoded.accounts.user_source, Some(106));
        assert_eq!(decoded.accounts.user_destination, Some(105));
    }

    #[test]
    fn launch_sell_is_exact_in_and_maps_base_to_quote() {
        let mut data = [0_u8; 32];
        data[..8].copy_from_slice(&LAUNCH_SELL_EXACT_IN);
        data[8..16].copy_from_slice(&31_u64.to_le_bytes());
        data[16..24].copy_from_slice(&29_u64.to_le_bytes());
        let accounts: Vec<_> = (0..15).collect();
        let DecodeOutcome::Decoded(decoded) = decode(Program::RaydiumLaunchlab, &data, &accounts)
        else {
            panic!("expected LaunchLab sell")
        };
        assert_eq!(decoded.accounts.input_vault, Some(7));
        assert_eq!(decoded.accounts.output_vault, Some(8));
        assert_eq!(decoded.accounts.input_mint, Some(9));
        assert_eq!(decoded.accounts.output_mint, Some(10));
    }

    #[test]
    fn openbook_bid_maps_quote_to_base() {
        let mut data = [0_u8; 35];
        data[..8].copy_from_slice(&OPENBOOK_PLACE_TAKE_ORDER);
        data[8] = 0;
        let accounts: Vec<_> = (0..16).collect();
        let DecodeOutcome::Decoded(decoded) = decode(Program::OpenBookV2, &data, &accounts) else {
            panic!("expected OpenBook place-and-take order")
        };
        assert_eq!(decoded.class, InstructionClass::Order(OrderKind::PlaceTake));
        assert_eq!(decoded.accounts.user_source, Some(10));
        assert_eq!(decoded.accounts.user_destination, Some(9));
    }

    #[test]
    fn matching_inputs_fail_closed_when_truncated() {
        assert!(matches!(
            decode(Program::PumpFun, &PUMP_BUY, &[0; 7]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 24,
                actual: 8
            })
        ));
        let mut data = [0_u8; 32];
        data[..8].copy_from_slice(&LAUNCH_BUY_EXACT_IN);
        assert!(matches!(
            decode(Program::RaydiumLaunchlab, &data, &[0; 14]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 15,
                actual: 14
            })
        ));
    }

    #[test]
    fn current_pump_buy_rejects_an_invalid_borsh_bool() {
        let mut data = [0_u8; 25];
        data[..8].copy_from_slice(&PUMP_BUY);
        data[24] = 2;
        assert_eq!(
            decode(Program::PumpFun, &data, &[0; 16]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 24 })
        );
    }

    #[test]
    fn unknown_discriminator_is_unsupported() {
        assert!(matches!(
            decode(Program::PumpFun, &[0; 8], &[]),
            DecodeOutcome::Unsupported { .. }
        ));
    }
}

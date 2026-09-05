use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, SwapKind, account,
    anchor_discriminator, read_u64_le,
};

const BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const BUY_EXACT_QUOTE_IN: [u8; 8] = [198, 46, 21, 82, 180, 217, 232, 112];
const SELL: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

const VENUE_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);

#[derive(Clone, Copy)]
enum Direction {
    BuyExactBaseOut,
    BuyExactQuoteIn,
    SellExactBaseIn,
}

#[derive(Clone, Copy)]
struct SwapSpec {
    discriminator: [u8; 8],
    name: &'static str,
    direction: Direction,
}

const SWAPS: &[SwapSpec] = &[
    SwapSpec {
        discriminator: BUY,
        name: "buy",
        direction: Direction::BuyExactBaseOut,
    },
    SwapSpec {
        discriminator: BUY_EXACT_QUOTE_IN,
        name: "buy_exact_quote_in",
        direction: Direction::BuyExactQuoteIn,
    },
    SwapSpec {
        discriminator: SELL,
        name: "sell",
        direction: Direction::SellExactBaseIn,
    },
];

pub(crate) fn decode(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let Some(spec) = SWAPS
        .iter()
        .find(|spec| spec.discriminator == discriminator.bytes)
    else {
        return DecodeOutcome::Unsupported { discriminator };
    };

    if data.len() < 24 {
        return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
            needed: 24,
            actual: data.len(),
        });
    }
    let required_accounts = match spec.direction {
        // The verified historical buy layout is 24 bytes and 19 accounts. The
        // current layout adds a Borsh bool and uses 23 accounts.
        Direction::BuyExactBaseOut if data.len() == 24 => 19,
        Direction::BuyExactBaseOut | Direction::BuyExactQuoteIn => {
            let Some(track_volume) = data.get(24).copied() else {
                return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                    needed: 25,
                    actual: data.len(),
                });
            };
            if track_volume > 1 {
                return DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData {
                    offset: 24,
                });
            }
            23
        }
        // Both the historical and current sell encodings are 24 bytes. The
        // historical 19-account layout is the smallest verified full layout.
        Direction::SellExactBaseIn => 19,
    };
    if accounts.len() < required_accounts {
        return DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
            needed: required_accounts,
            actual: accounts.len(),
        });
    }

    let first_amount = match read_amount(data, 8) {
        Ok(amount) => amount,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let second_amount = match read_amount(data, 16) {
        Ok(amount) => amount,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let decoded_accounts = match decode_accounts(accounts, spec.direction) {
        Ok(decoded_accounts) => decoded_accounts,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let (kind, amounts) = match spec.direction {
        Direction::BuyExactBaseOut => (
            SwapKind::ExactOut,
            Amounts::ExactOut {
                maximum_amount_in: second_amount,
                amount_out: first_amount,
            },
        ),
        Direction::BuyExactQuoteIn | Direction::SellExactBaseIn => (
            SwapKind::ExactIn,
            Amounts::ExactIn {
                amount_in: first_amount,
                minimum_amount_out: second_amount,
            },
        ),
    };

    DecodeOutcome::Decoded(DecodedInstruction {
        program: Program::PumpSwap,
        role: ProgramRole::Venue,
        name: spec.name,
        class: InstructionClass::Swap(kind),
        discriminator,
        accounts: decoded_accounts,
        amounts,
        evidence: VENUE_EVIDENCE,
    })
}

fn read_amount(data: &[u8], offset: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(MalformedReason::InstructionDataTooShort {
        needed: offset.saturating_add(8),
        actual: data.len(),
    })
}

fn decode_accounts(
    accounts: &[CompactId],
    direction: Direction,
) -> Result<AccountRoles, MalformedReason> {
    let base_mint = account(accounts, 3)?;
    let quote_mint = account(accounts, 4)?;
    let user_base = account(accounts, 5)?;
    let user_quote = account(accounts, 6)?;
    let pool_base = account(accounts, 7)?;
    let pool_quote = account(accounts, 8)?;

    let (user_source, user_destination, input_vault, output_vault, input_mint, output_mint) =
        match direction {
            Direction::BuyExactBaseOut | Direction::BuyExactQuoteIn => (
                user_quote, user_base, pool_quote, pool_base, quote_mint, base_mint,
            ),
            Direction::SellExactBaseIn => (
                user_base, user_quote, pool_base, pool_quote, base_mint, quote_mint,
            ),
        };

    Ok(AccountRoles {
        pool: Some(account(accounts, 0)?),
        user_authority: Some(account(accounts, 1)?),
        user_source: Some(user_source),
        user_destination: Some(user_destination),
        vault_a: Some(pool_base),
        vault_b: Some(pool_quote),
        input_vault: Some(input_vault),
        output_vault: Some(output_vault),
        input_mint: Some(input_mint),
        output_mint: Some(output_mint),
        fee_account: Some(account(accounts, 10)?),
        ..AccountRoles::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Discriminator;

    #[test]
    fn decodes_buy_as_a_pumpswap_venue_swap() {
        let mut data = [0_u8; 25];
        data[..8].copy_from_slice(&BUY);
        data[8..16].copy_from_slice(&7_u64.to_le_bytes());
        data[16..24].copy_from_slice(&11_u64.to_le_bytes());
        let accounts = [
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
            117, 118, 119, 120, 121, 122,
        ];

        assert_eq!(
            decode(&data, &accounts),
            DecodeOutcome::Decoded(DecodedInstruction {
                program: Program::PumpSwap,
                role: ProgramRole::Venue,
                name: "buy",
                class: InstructionClass::Swap(SwapKind::ExactOut),
                discriminator: Discriminator::eight(BUY),
                accounts: AccountRoles {
                    pool: Some(100),
                    user_authority: Some(101),
                    user_source: Some(106),
                    user_destination: Some(105),
                    vault_a: Some(107),
                    vault_b: Some(108),
                    input_vault: Some(108),
                    output_vault: Some(107),
                    input_mint: Some(104),
                    output_mint: Some(103),
                    fee_account: Some(110),
                    ..AccountRoles::default()
                },
                amounts: Amounts::ExactOut {
                    maximum_amount_in: 11,
                    amount_out: 7,
                },
                evidence: VENUE_EVIDENCE,
            })
        );
    }

    #[test]
    fn decodes_sell_direction_and_limits() {
        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&SELL);
        data[8..16].copy_from_slice(&13_u64.to_le_bytes());
        data[16..24].copy_from_slice(&17_u64.to_le_bytes());
        let accounts = [
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
            117, 118, 119, 120,
        ];

        let outcome = decode(&data, &accounts);
        assert!(matches!(outcome, DecodeOutcome::Decoded(_)), "{outcome:?}");
        let DecodeOutcome::Decoded(decoded) = outcome else {
            return;
        };
        assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactIn));
        assert_eq!(decoded.accounts.user_source, Some(105));
        assert_eq!(decoded.accounts.user_destination, Some(106));
        assert_eq!(
            decoded.amounts,
            Amounts::ExactIn {
                amount_in: 13,
                minimum_amount_out: 17,
            }
        );
    }

    #[test]
    fn accepts_a_historical_24_byte_buy_fixture() {
        let data = [
            102, 6, 61, 18, 1, 218, 235, 234, 65, 23, 192, 0, 0, 0, 0, 0, 81, 105, 15, 0, 0, 0, 0,
            0,
        ];
        let accounts = [
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
            117, 118,
        ];

        let outcome = decode(&data, &accounts);
        assert!(matches!(outcome, DecodeOutcome::Decoded(_)), "{outcome:?}");
        let DecodeOutcome::Decoded(decoded) = outcome else {
            return;
        };
        assert_eq!(decoded.name, "buy");
        assert_eq!(decoded.role, ProgramRole::Venue);
        assert_eq!(
            decoded.amounts,
            Amounts::ExactOut {
                maximum_amount_in: 1_010_001,
                amount_out: 12_588_865,
            }
        );
    }

    #[test]
    fn rejects_truncated_pumpswap_data_and_accounts() {
        assert_eq!(
            decode(&BUY, &[0; 19]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 24,
                actual: 8,
            })
        );

        let mut data = [0_u8; 24];
        data[..8].copy_from_slice(&BUY);
        assert_eq!(
            decode(&data, &[0; 18]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 19,
                actual: 18,
            })
        );
    }

    #[test]
    fn current_buy_rejects_an_invalid_borsh_bool() {
        let mut data = [0_u8; 25];
        data[..8].copy_from_slice(&BUY);
        data[24] = 2;
        assert_eq!(
            decode(&data, &[0; 23]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 24 })
        );
    }

    #[test]
    fn rejects_an_unknown_pumpswap_discriminator() {
        assert_eq!(
            decode(&[0xff; 8], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight([0xff; 8]),
            }
        );
    }
}

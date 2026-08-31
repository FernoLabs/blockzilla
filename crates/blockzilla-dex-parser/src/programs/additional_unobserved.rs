use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Discriminator, Evidence,
    InstructionClass, MalformedReason, OrderKind, Program, SwapKind, account, anchor_discriminator,
    one_byte_discriminator, read_u8, read_u32_le, read_u64_le,
};

const ANCHOR_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const SYMMETRY_SWAP: [u8; 8] = [112, 246, 21, 136, 172, 62, 27, 20];
const CYKURA_EXACT_INPUT_SINGLE: [u8; 8] = [23, 113, 90, 161, 237, 143, 153, 13];
const DRADEX_CREATE_ORDER: [u8; 8] = [141, 54, 37, 207, 237, 210, 250, 215];
const DRADEX_SETTLE_FUNDS: [u8; 8] = [238, 64, 163, 96, 75, 171, 16, 33];
const SERUM_SETTLE_FUNDS: [u8; 5] = [0, 5, 0, 0, 0];
const SERUM_NEW_ORDER_V3: [u8; 5] = [0, 10, 0, 0, 0];

const SWAP_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const LAYOUT_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT.union(Evidence::TOKEN_FLOW_REQUIRED);
const STRUCTURAL_EVIDENCE: Evidence =
    Evidence::TOKEN_FLOW_REQUIRED.union(Evidence::STRUCTURAL_ONLY);

#[inline]
pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::RaydiumLegacyV2 => decode_raydium(data, accounts),
        Program::SymmetryV2 => decode_symmetry(data, accounts),
        Program::Legacy2Nz => decode_legacy_2nz(data, accounts),
        Program::CremaFinance => decode_crema(data, accounts),
        Program::GooseFxSsl => decode_goosefx_ssl(data, accounts),
        Program::SerumDexV3 => decode_serum(data, accounts),
        Program::LifinityV1 => decode_lifinity(data, accounts),
        Program::GooseFxV2 => decode_goosefx_v2(data, accounts),
        Program::PenguinFinance => decode_penguin(data, accounts),
        Program::Sencha => decode_sencha(data, accounts),
        Program::Cykura => decode_cykura(data, accounts),
        Program::Dradex => decode_dradex(data, accounts),
        Program::OpenBookV1 => decode_openbook(data, accounts),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_raydium(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::one(9) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 7)?;
        Ok(DecodedInstruction {
            program: Program::RaydiumLegacyV2,
            role: Program::RaydiumLegacyV2.role(),
            name: "SwapBaseIn",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            // Historical transactions have two layouts. Vaults are 4/5 or
            // 5/6. Their compact IDs do not identify which layout is present.
            accounts: AccountRoles {
                pool: Some(account(accounts, 1)?),
                ..AccountRoles::default()
            },
            amounts: exact_in(data, 1, 9, 17)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_symmetry(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    structural_anchor(
        Program::SymmetryV2,
        data,
        accounts,
        SYMMETRY_SWAP,
        "SwapFundTokens",
        6,
    )
}

fn decode_legacy_2nz(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    structural_anchor(Program::Legacy2Nz, data, accounts, ANCHOR_SWAP, "Swap", 6)
}

fn decode_crema(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::one(1) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    outcome((|| {
        require_accounts(accounts, 7)?;
        Ok(DecodedInstruction {
            program: Program::CremaFinance,
            role: Program::CremaFinance.role(),
            name: "Swap",
            class: InstructionClass::Swap(SwapKind::Unspecified),
            discriminator,
            // The old parser proves the program and selector, but it does not
            // supply a named account or argument schema.
            accounts: AccountRoles::default(),
            amounts: Amounts::Unknown,
            evidence: STRUCTURAL_EVIDENCE,
        })
    })())
}

fn decode_goosefx_ssl(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match supported_anchor(data, ANCHOR_SWAP) {
        Ok(value) => value,
        Err(error) => return error.into_outcome(),
    };
    outcome((|| {
        require_data(data, 24)?;
        require_accounts(accounts, 14)?;
        Ok(DecodedInstruction {
            program: Program::GooseFxSsl,
            role: Program::GooseFxSsl.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 1)?),
                user_authority: Some(account(accounts, 11)?),
                user_source: Some(account(accounts, 8)?),
                user_destination: Some(account(accounts, 9)?),
                input_vault: Some(account(accounts, 4)?),
                output_vault: Some(account(accounts, 6)?),
                fee_account: Some(account(accounts, 10)?),
                ..AccountRoles::default()
            },
            amounts: exact_in(data, 8, 16, 24)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_serum(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match serum_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::five(SERUM_SETTLE_FUNDS) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    outcome((|| {
        require_exact_data(data, 5)?;
        require_accounts(accounts, 9)?;
        Ok(DecodedInstruction {
            program: Program::SerumDexV3,
            role: Program::SerumDexV3.role(),
            name: "SettleFunds",
            class: InstructionClass::Order(OrderKind::Settle),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                authority: Some(account(accounts, 7)?),
                user_authority: Some(account(accounts, 2)?),
                vault_a: Some(account(accounts, 3)?),
                vault_b: Some(account(accounts, 4)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn decode_lifinity(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match supported_anchor(data, ANCHOR_SWAP) {
        Ok(value) => value,
        Err(error) => return error.into_outcome(),
    };
    outcome((|| {
        require_data(data, 24)?;
        require_accounts(accounts, 13)?;
        Ok(DecodedInstruction {
            program: Program::LifinityV1,
            role: Program::LifinityV1.role(),
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
            amounts: exact_in(data, 8, 16, 24)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_goosefx_v2(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match supported_anchor(data, ANCHOR_SWAP) {
        Ok(value) => value,
        Err(error) => return error.into_outcome(),
    };
    outcome((|| {
        require_data(data, 24)?;
        require_accounts(accounts, 20)?;
        Ok(DecodedInstruction {
            program: Program::GooseFxV2,
            role: Program::GooseFxV2.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 3)?),
                user_source: Some(account(accounts, 6)?),
                user_destination: Some(account(accounts, 7)?),
                input_vault: Some(account(accounts, 10)?),
                output_vault: Some(account(accounts, 8)?),
                fee_account: Some(account(accounts, 12)?),
                ..AccountRoles::default()
            },
            amounts: exact_in(data, 8, 16, 24)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_penguin(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::one(1) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 10)?;
        Ok(DecodedInstruction {
            program: Program::PenguinFinance,
            role: Program::PenguinFinance.role(),
            name: "Swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                authority: Some(account(accounts, 1)?),
                user_authority: Some(account(accounts, 2)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 6)?),
                input_vault: Some(account(accounts, 4)?),
                output_vault: Some(account(accounts, 5)?),
                fee_account: Some(account(accounts, 8)?),
                ..AccountRoles::default()
            },
            amounts: exact_in(data, 1, 9, 17)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_sencha(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    structural_anchor(Program::Sencha, data, accounts, ANCHOR_SWAP, "Swap", 8)
}

fn decode_cykura(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match supported_anchor(data, CYKURA_EXACT_INPUT_SINGLE) {
        Ok(value) => value,
        Err(error) => return error.into_outcome(),
    };
    outcome((|| {
        require_data(data, 40)?;
        require_accounts(accounts, 10)?;
        Ok(DecodedInstruction {
            program: Program::Cykura,
            role: Program::Cykura.role(),
            name: "ExactInputSingle",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 2)?),
                user_authority: Some(account(accounts, 0)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 4)?),
                input_vault: Some(account(accounts, 5)?),
                output_vault: Some(account(accounts, 6)?),
                ..AccountRoles::default()
            },
            amounts: exact_in(data, 16, 24, 32)?,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_dradex(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    match discriminator.bytes {
        DRADEX_CREATE_ORDER => decode_dradex_create(discriminator, data, accounts),
        DRADEX_SETTLE_FUNDS => decode_dradex_settle(discriminator, data, accounts),
        _ => DecodeOutcome::Unsupported { discriminator },
    }
}

fn decode_dradex_create(
    discriminator: Discriminator,
    data: &[u8],
    accounts: &[CompactId],
) -> DecodeOutcome {
    outcome((|| {
        validate_dradex_order(data)?;
        require_accounts(accounts, 16)?;
        Ok(DecodedInstruction {
            program: Program::Dradex,
            role: Program::Dradex.role(),
            name: "CreateOrder",
            class: InstructionClass::Order(OrderKind::Place),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 12)?),
                vault_a: Some(account(accounts, 7)?),
                vault_b: Some(account(accounts, 8)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn decode_dradex_settle(
    discriminator: Discriminator,
    data: &[u8],
    accounts: &[CompactId],
) -> DecodeOutcome {
    outcome((|| {
        require_exact_data(data, 8)?;
        require_accounts(accounts, 18)?;
        Ok(DecodedInstruction {
            program: Program::Dradex,
            role: Program::Dradex.role(),
            name: "SettleFunds",
            class: InstructionClass::Order(OrderKind::Settle),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 14)?),
                vault_a: Some(account(accounts, 9)?),
                vault_b: Some(account(accounts, 10)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn decode_openbook(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match serum_discriminator(data) {
        Ok(value) => value,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator != Discriminator::five(SERUM_NEW_ORDER_V3) {
        return DecodeOutcome::Unsupported { discriminator };
    }
    outcome((|| {
        // Version byte + u32 selector + the 54-byte NewOrderInstructionV3.
        require_exact_data(data, 59)?;
        require_accounts(accounts, 12)?;
        let side = enum_u32(data, 5, 1)?;
        nonzero_u64(data, 9)?;
        nonzero_u64(data, 17)?;
        nonzero_u64(data, 25)?;
        enum_u32(data, 33, 2)?;
        enum_u32(data, 37, 2)?;
        let input_vault = if side == 0 {
            account(accounts, 9)?
        } else {
            account(accounts, 8)?
        };
        Ok(DecodedInstruction {
            program: Program::OpenBookV1,
            role: Program::OpenBookV1.role(),
            name: "NewOrderV3",
            class: InstructionClass::Order(OrderKind::Place),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                user_authority: Some(account(accounts, 7)?),
                user_source: Some(account(accounts, 6)?),
                vault_a: Some(account(accounts, 8)?),
                vault_b: Some(account(accounts, 9)?),
                input_vault: Some(input_vault),
                // NewOrderV3 deposits into one market vault. It does not
                // withdraw from the opposing vault; settlement happens later.
                output_vault: None,
                ..AccountRoles::default()
            },
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn structural_anchor(
    program: Program,
    data: &[u8],
    accounts: &[CompactId],
    expected: [u8; 8],
    name: &'static str,
    account_count: usize,
) -> DecodeOutcome {
    let discriminator = match supported_anchor(data, expected) {
        Ok(value) => value,
        Err(error) => return error.into_outcome(),
    };
    outcome((|| {
        require_accounts(accounts, account_count)?;
        Ok(DecodedInstruction {
            program,
            role: program.role(),
            name,
            class: InstructionClass::Swap(SwapKind::Unspecified),
            discriminator,
            accounts: AccountRoles::default(),
            amounts: Amounts::Unknown,
            evidence: STRUCTURAL_EVIDENCE,
        })
    })())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnchorSupportError {
    Malformed(MalformedReason),
    Unsupported(Discriminator),
}

impl AnchorSupportError {
    fn into_outcome(self) -> DecodeOutcome {
        match self {
            Self::Malformed(reason) => DecodeOutcome::Malformed(reason),
            Self::Unsupported(discriminator) => DecodeOutcome::Unsupported { discriminator },
        }
    }
}

fn supported_anchor(data: &[u8], expected: [u8; 8]) -> Result<Discriminator, AnchorSupportError> {
    let discriminator = anchor_discriminator(data).map_err(AnchorSupportError::Malformed)?;
    if discriminator.bytes == expected {
        Ok(discriminator)
    } else {
        Err(AnchorSupportError::Unsupported(discriminator))
    }
}

fn serum_discriminator(data: &[u8]) -> Result<Discriminator, MalformedReason> {
    let Some(prefix) = data.get(..5) else {
        return Err(short_data(data, 5));
    };
    Ok(Discriminator::five([
        prefix[0], prefix[1], prefix[2], prefix[3], prefix[4],
    ]))
}

fn validate_dradex_order(data: &[u8]) -> Result<(), MalformedReason> {
    // OrderInput: side, limit_price, amount, client_order_id, order_type,
    // Option<u64> limit_total, Option<u64> minimum_amount_out.
    require_data(data, 36)?;
    enum_u8(data, 8, 1)?;
    enum_u8(data, 33, 2)?;
    let after_first = option_u64_end(data, 34)?;
    let end = option_u64_end(data, after_first)?;
    if data.len() != end {
        return Err(MalformedReason::InvalidInstructionData { offset: end });
    }
    Ok(())
}

fn enum_u8(data: &[u8], offset: usize, maximum: u8) -> Result<u8, MalformedReason> {
    match read_u8(data, offset) {
        Some(value) if value <= maximum => Ok(value),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(short_data(data, offset.saturating_add(1))),
    }
}

fn option_u64_end(data: &[u8], offset: usize) -> Result<usize, MalformedReason> {
    match read_u8(data, offset) {
        Some(0) => Ok(offset.saturating_add(1)),
        Some(1) => {
            let end = offset.saturating_add(9);
            require_data(data, end)?;
            Ok(end)
        }
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(short_data(data, offset.saturating_add(1))),
    }
}

fn enum_u32(data: &[u8], offset: usize, maximum: u32) -> Result<u32, MalformedReason> {
    match read_u32_le(data, offset) {
        Some(value) if value <= maximum => Ok(value),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(short_data(data, offset.saturating_add(4))),
    }
}

fn nonzero_u64(data: &[u8], offset: usize) -> Result<u64, MalformedReason> {
    match read_u64_le(data, offset) {
        Some(0) => Err(MalformedReason::InvalidInstructionData { offset }),
        Some(value) => Ok(value),
        None => Err(short_data(data, offset.saturating_add(8))),
    }
}

fn exact_in(
    data: &[u8],
    first_offset: usize,
    second_offset: usize,
    needed: usize,
) -> Result<Amounts, MalformedReason> {
    Ok(Amounts::ExactIn {
        amount_in: amount(data, first_offset, needed)?,
        minimum_amount_out: amount(data, second_offset, needed)?,
    })
}

fn amount(data: &[u8], offset: usize, needed: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(short_data(data, needed))
}

fn require_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    if data.len() < needed {
        Err(short_data(data, needed))
    } else {
        Ok(())
    }
}

fn require_exact_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    require_data(data, needed)?;
    if data.len() == needed {
        Ok(())
    } else {
        Err(MalformedReason::InvalidInstructionData { offset: needed })
    }
}

fn short_data(data: &[u8], needed: usize) -> MalformedReason {
    MalformedReason::InstructionDataTooShort {
        needed,
        actual: data.len(),
    }
}

fn require_accounts(accounts: &[CompactId], needed: usize) -> Result<(), MalformedReason> {
    account(accounts, needed.saturating_sub(1)).map(|_| ())
}

fn outcome(decoded: Result<DecodedInstruction, MalformedReason>) -> DecodeOutcome {
    match decoded {
        Ok(value) => DecodeOutcome::Decoded(value),
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

    #[test]
    fn exact_in_venues_decode_proven_amounts_and_roles() {
        let mut ssl_data = anchor_data::<24>(ANCHOR_SWAP);
        ssl_data[8..16].copy_from_slice(&41_u64.to_le_bytes());
        ssl_data[16..24].copy_from_slice(&17_u64.to_le_bytes());
        let ssl = decode(Program::GooseFxSsl, &ssl_data, &ids::<14>());
        assert!(matches!(ssl, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = ssl {
            assert_eq!(value.class, InstructionClass::Swap(SwapKind::ExactIn));
            assert_eq!(value.accounts.user_authority, Some(11));
            assert_eq!(value.accounts.input_vault, Some(4));
            assert_eq!(value.accounts.vault_a, None);
            assert_eq!(
                value.amounts,
                Amounts::ExactIn {
                    amount_in: 41,
                    minimum_amount_out: 17
                }
            );
        }

        let mut lifinity_data = anchor_data::<24>(ANCHOR_SWAP);
        lifinity_data[8..16].copy_from_slice(&5_u64.to_le_bytes());
        lifinity_data[16..24].copy_from_slice(&3_u64.to_le_bytes());
        let lifinity = decode(Program::LifinityV1, &lifinity_data, &ids::<13>());
        assert!(matches!(lifinity, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = lifinity {
            assert_eq!(value.accounts.user_source, Some(3));
            assert_eq!(value.accounts.fee_account, Some(8));
        }

        let mut goose_data = anchor_data::<24>(ANCHOR_SWAP);
        goose_data[8..16].copy_from_slice(&9_u64.to_le_bytes());
        goose_data[16..24].copy_from_slice(&8_u64.to_le_bytes());
        let goose = decode(Program::GooseFxV2, &goose_data, &ids::<20>());
        assert!(matches!(goose, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = goose {
            assert_eq!(value.accounts.input_vault, Some(10));
            assert_eq!(value.accounts.output_vault, Some(8));
            assert_eq!(value.accounts.vault_a, None);
        }

        let mut penguin_data = [0_u8; 17];
        penguin_data[0] = 1;
        penguin_data[1..9].copy_from_slice(&12_u64.to_le_bytes());
        penguin_data[9..17].copy_from_slice(&10_u64.to_le_bytes());
        let penguin = decode(Program::PenguinFinance, &penguin_data, &ids::<10>());
        assert!(matches!(penguin, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = penguin {
            assert_eq!(value.accounts.vault_a, None);
            assert_eq!(value.accounts.input_vault, Some(4));
        }

        let mut cykura_data = anchor_data::<40>(CYKURA_EXACT_INPUT_SINGLE);
        cykura_data[16..24].copy_from_slice(&22_u64.to_le_bytes());
        cykura_data[24..32].copy_from_slice(&11_u64.to_le_bytes());
        let cykura = decode(Program::Cykura, &cykura_data, &ids::<10>());
        assert!(matches!(cykura, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = cykura {
            assert_eq!(value.accounts.user_authority, Some(0));
            assert_eq!(value.accounts.vault_a, None);
            assert_eq!(
                value.amounts,
                Amounts::ExactIn {
                    amount_in: 22,
                    minimum_amount_out: 11
                }
            );
        }
    }

    #[test]
    fn conservative_historical_swaps_decode_without_guessed_amounts() {
        let symmetry_data = anchor_data::<8>(SYMMETRY_SWAP);
        let symmetry = decode(Program::SymmetryV2, &symmetry_data, &ids::<6>());
        assert!(matches!(symmetry, DecodeOutcome::Decoded(_)));

        let anchor_swap = anchor_data::<8>(ANCHOR_SWAP);
        for (program, account_count) in [(Program::Legacy2Nz, 6), (Program::Sencha, 8)] {
            let accounts = ids::<8>();
            let result = decode(program, &anchor_swap, &accounts[..account_count]);
            assert!(matches!(result, DecodeOutcome::Decoded(_)));
            if let DecodeOutcome::Decoded(value) = result {
                assert_eq!(value.class, InstructionClass::Swap(SwapKind::Unspecified));
                assert_eq!(value.accounts, AccountRoles::default());
                assert_eq!(value.amounts, Amounts::Unknown);
                assert!(value.evidence.contains(Evidence::STRUCTURAL_ONLY));
            }
        }

        let crema = decode(Program::CremaFinance, &[1], &ids::<7>());
        let DecodeOutcome::Decoded(crema) = crema else {
            panic!("structural Crema selector must decode");
        };
        assert_eq!(crema.accounts, AccountRoles::default());
        assert!(crema.evidence.contains(Evidence::STRUCTURAL_ONLY));

        let mut raydium_data = [0_u8; 17];
        raydium_data[0] = 9;
        raydium_data[1..9].copy_from_slice(&7_u64.to_le_bytes());
        raydium_data[9..17].copy_from_slice(&6_u64.to_le_bytes());
        let raydium = decode(Program::RaydiumLegacyV2, &raydium_data, &ids::<7>());
        assert!(matches!(raydium, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = raydium {
            assert_eq!(value.accounts.pool, Some(1));
            assert_eq!(value.accounts.vault_a, None);
        }
    }

    #[test]
    fn serum_openbook_and_dradex_are_orders() {
        let serum = decode(Program::SerumDexV3, &SERUM_SETTLE_FUNDS, &ids::<9>());
        assert!(matches!(serum, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = serum {
            assert_eq!(value.class, InstructionClass::Order(OrderKind::Settle));
            assert_eq!(value.discriminator.len, 5);
            assert_eq!(value.accounts.authority, Some(7));
        }

        let mut openbook_data = [0_u8; 59];
        openbook_data[..5].copy_from_slice(&SERUM_NEW_ORDER_V3);
        openbook_data[9..17].copy_from_slice(&1_u64.to_le_bytes());
        openbook_data[17..25].copy_from_slice(&2_u64.to_le_bytes());
        openbook_data[25..33].copy_from_slice(&3_u64.to_le_bytes());
        let openbook = decode(Program::OpenBookV1, &openbook_data, &ids::<12>());
        assert!(matches!(openbook, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = openbook {
            assert_eq!(value.class, InstructionClass::Order(OrderKind::Place));
            assert_eq!(value.accounts.input_vault, Some(9));
            assert_eq!(value.accounts.output_vault, None);
        }

        let mut create_data = anchor_data::<36>(DRADEX_CREATE_ORDER);
        create_data[34] = 0;
        create_data[35] = 0;
        let create = decode(Program::Dradex, &create_data, &ids::<16>());
        assert!(matches!(create, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = create {
            assert_eq!(value.class, InstructionClass::Order(OrderKind::Place));
            assert_eq!(value.accounts.user_authority, Some(12));
        }

        let settle_data = anchor_data::<8>(DRADEX_SETTLE_FUNDS);
        let settle = decode(Program::Dradex, &settle_data, &ids::<18>());
        assert!(matches!(settle, DecodeOutcome::Decoded(_)));
        if let DecodeOutcome::Decoded(value) = settle {
            assert_eq!(value.class, InstructionClass::Order(OrderKind::Settle));
            assert_eq!(value.accounts.vault_a, Some(9));
        }

        let mut invalid_side = anchor_data::<36>(DRADEX_CREATE_ORDER);
        invalid_side[8] = 2;
        assert_eq!(
            decode(Program::Dradex, &invalid_side, &ids::<16>()),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 8 })
        );
        let mut invalid_order_type = anchor_data::<36>(DRADEX_CREATE_ORDER);
        invalid_order_type[33] = 3;
        assert_eq!(
            decode(Program::Dradex, &invalid_order_type, &ids::<16>()),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 33 })
        );
    }

    #[test]
    fn matching_but_truncated_input_or_accounts_is_malformed() {
        let short_ssl = anchor_data::<23>(ANCHOR_SWAP);
        assert_eq!(
            decode(Program::GooseFxSsl, &short_ssl, &ids::<14>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 24,
                actual: 23
            })
        );
        let ssl = anchor_data::<24>(ANCHOR_SWAP);
        assert_eq!(
            decode(Program::GooseFxSsl, &ssl, &ids::<13>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 14,
                actual: 13
            })
        );
        assert_eq!(
            decode(Program::SerumDexV3, &[0, 5], &ids::<9>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 5,
                actual: 2
            })
        );
        let mut short_order = [0_u8; 58];
        short_order[..5].copy_from_slice(&SERUM_NEW_ORDER_V3);
        assert_eq!(
            decode(Program::OpenBookV1, &short_order, &ids::<12>()),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 59,
                actual: 58
            })
        );
    }

    #[test]
    fn unknown_discriminators_are_unsupported() {
        for program in [
            Program::SymmetryV2,
            Program::Legacy2Nz,
            Program::GooseFxSsl,
            Program::LifinityV1,
            Program::GooseFxV2,
            Program::Sencha,
            Program::Cykura,
            Program::Dradex,
        ] {
            assert_eq!(
                decode(program, &[255; 8], &[]),
                DecodeOutcome::Unsupported {
                    discriminator: Discriminator::eight([255; 8])
                }
            );
        }
        for program in [
            Program::RaydiumLegacyV2,
            Program::CremaFinance,
            Program::PenguinFinance,
        ] {
            assert_eq!(
                decode(program, &[255], &[]),
                DecodeOutcome::Unsupported {
                    discriminator: Discriminator::one(255)
                }
            );
        }
        for program in [Program::SerumDexV3, Program::OpenBookV1] {
            assert_eq!(
                decode(program, &[0, 255, 0, 0, 0], &[]),
                DecodeOutcome::Unsupported {
                    discriminator: Discriminator::five([0, 255, 0, 0, 0])
                }
            );
        }
    }
}

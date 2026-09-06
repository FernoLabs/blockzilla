use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, account, anchor_discriminator,
    read_u32_le, read_u64_le,
};

// OKX router v2: 6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma.
const V2_PROXY_SWAP: [u8; 8] = [19, 44, 130, 148, 72, 56, 44, 238];
const V2_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const V2_SWAP_TOB_V3: [u8; 8] = [14, 191, 44, 246, 142, 225, 224, 157];
const V2_SWAP_TOB_V3_ENHANCED: [u8; 8] = [236, 71, 155, 68, 198, 98, 14, 118];
const V2_SWAP_TOB_V3_WITH_RECEIVER: [u8; 8] = [63, 114, 246, 131, 51, 2, 247, 29];
const V2_SWAP_V3: [u8; 8] = [240, 224, 38, 33, 176, 31, 241, 175];
const V2_SWAP_V3_WITH_CPI_EVENT: [u8; 8] = [184, 104, 79, 156, 107, 182, 120, 138];

// OKX router v3: proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u.
const V3_PROXY_SWAP: [u8; 8] = [19, 44, 130, 148, 72, 56, 44, 238];
const V3_SWAP: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];
const V3_SWAP_TOB: [u8; 8] = [170, 41, 85, 177, 132, 80, 31, 53];
const V3_SWAP_TOB_ENHANCED: [u8; 8] = [190, 156, 169, 176, 149, 154, 161, 108];
const V3_SWAP_TOB_V2: [u8; 8] = [72, 1, 215, 242, 8, 75, 54, 216];
const V3_SWAP_TOB_V3: [u8; 8] = [14, 191, 44, 246, 142, 225, 224, 157];
const V3_SWAP_TOB_WITH_RECEIVER: [u8; 8] = [223, 170, 216, 234, 204, 6, 241, 25];
const V3_SWAP_TOB_WITH_RECEIVER_TOKEN_LEDGER: [u8; 8] = [239, 93, 10, 202, 161, 134, 127, 130];
const V3_SWAP_TOB_WITH_RECEIVER_TOKEN_LEDGER_V3: [u8; 8] = [119, 172, 209, 16, 91, 44, 63, 224];
const V3_SWAP_TOB_WITH_RECEIVER_V3: [u8; 8] = [26, 190, 234, 223, 241, 5, 177, 189];
const V3_SWAP_TOB_WITH_TOKEN_LEDGER: [u8; 8] = [36, 92, 147, 219, 26, 176, 159, 90];
const V3_SWAP_TOB_WITH_TOKEN_LEDGER_V3: [u8; 8] = [132, 77, 6, 86, 35, 66, 224, 171];
const V3_SWAP_TOC: [u8; 8] = [187, 201, 212, 51, 16, 155, 236, 60];
const V3_SWAP_TOC_V2: [u8; 8] = [127, 214, 107, 189, 23, 90, 47, 104];
const V3_SWAP_TOC_V3: [u8; 8] = [86, 222, 68, 49, 225, 9, 201, 235];

const ROUTER_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::ROUTE_CONTAINER)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const ROUTER_AMOUNT_EVIDENCE: Evidence = ROUTER_EVIDENCE.union(Evidence::AMOUNTS);
#[derive(Clone, Copy)]
enum AmountLayout {
    V2ExactIn,
    Unknown,
}

#[derive(Clone, Copy)]
enum BodyLayout {
    V2SwapArgs { trailing_data_len: usize },
    V3SwapArgs { trailing_data_len: usize },
    V3TokenLedger { trailing_data_len: usize },
}

#[derive(Clone, Copy)]
struct RouteSpec {
    discriminator: [u8; 8],
    name: &'static str,
    minimum_data_len: usize,
    required_accounts: usize,
    amounts: AmountLayout,
    body: BodyLayout,
}

// Minimum lengths include empty Borsh vectors and the fixed fields that follow them.
const V2_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        discriminator: V2_PROXY_SWAP,
        name: "proxy_swap",
        minimum_data_len: 48,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 8,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP,
        name: "swap",
        minimum_data_len: 48,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 8,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP_TOB_V3,
        name: "swap_tob_v3",
        minimum_data_len: 55,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 15,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP_TOB_V3_ENHANCED,
        name: "swap_tob_v3_enhanced",
        minimum_data_len: 57,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 17,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP_TOB_V3_WITH_RECEIVER,
        name: "swap_tob_v3_with_receiver",
        minimum_data_len: 55,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 15,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP_V3,
        name: "swap_v3",
        minimum_data_len: 54,
        required_accounts: 5,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 14,
        },
    },
    RouteSpec {
        discriminator: V2_SWAP_V3_WITH_CPI_EVENT,
        name: "swap_v3_with_cpi_event",
        minimum_data_len: 54,
        required_accounts: 16,
        amounts: AmountLayout::V2ExactIn,
        body: BodyLayout::V2SwapArgs {
            trailing_data_len: 14,
        },
    },
];

const V3_ROUTES: &[RouteSpec] = &[
    RouteSpec {
        discriminator: V3_PROXY_SWAP,
        name: "proxy_swap",
        minimum_data_len: 38,
        required_accounts: 14,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 0,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP,
        name: "swap",
        minimum_data_len: 38,
        required_accounts: 7,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 0,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB,
        name: "swap_tob",
        minimum_data_len: 45,
        required_accounts: 16,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 7,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_ENHANCED,
        name: "swap_tob_enhanced",
        minimum_data_len: 47,
        required_accounts: 16,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 9,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_V2,
        name: "swap_tob_v2",
        minimum_data_len: 49,
        required_accounts: 17,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 11,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_V3,
        name: "swap_tob_v3",
        minimum_data_len: 44,
        required_accounts: 14,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 6,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_RECEIVER,
        name: "swap_tob_with_receiver",
        minimum_data_len: 45,
        required_accounts: 17,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 7,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_RECEIVER_TOKEN_LEDGER,
        name: "swap_tob_with_receiver_token_ledger",
        minimum_data_len: 37,
        required_accounts: 18,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3TokenLedger {
            trailing_data_len: 7,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_RECEIVER_TOKEN_LEDGER_V3,
        name: "swap_tob_with_receiver_token_ledger_v3",
        minimum_data_len: 36,
        required_accounts: 16,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3TokenLedger {
            trailing_data_len: 6,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_RECEIVER_V3,
        name: "swap_tob_with_receiver_v3",
        minimum_data_len: 44,
        required_accounts: 15,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 6,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_TOKEN_LEDGER,
        name: "swap_tob_with_token_ledger",
        minimum_data_len: 37,
        required_accounts: 17,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3TokenLedger {
            trailing_data_len: 7,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOB_WITH_TOKEN_LEDGER_V3,
        name: "swap_tob_with_token_ledger_v3",
        minimum_data_len: 36,
        required_accounts: 15,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3TokenLedger {
            trailing_data_len: 6,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOC,
        name: "swap_toc",
        minimum_data_len: 44,
        required_accounts: 16,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 6,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOC_V2,
        name: "swap_toc_v2",
        minimum_data_len: 48,
        required_accounts: 17,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 10,
        },
    },
    RouteSpec {
        discriminator: V3_SWAP_TOC_V3,
        name: "swap_toc_v3",
        minimum_data_len: 43,
        required_accounts: 14,
        amounts: AmountLayout::Unknown,
        body: BodyLayout::V3SwapArgs {
            trailing_data_len: 5,
        },
    },
];

pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::OkxRouterV2 => decode_anchor_route(program, data, accounts, V2_ROUTES),
        Program::OkxRouterV3 => decode_anchor_route(program, data, accounts, V3_ROUTES),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_anchor_route(
    program: Program,
    data: &[u8],
    accounts: &[CompactId],
    routes: &[RouteSpec],
) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let Some(spec) = routes
        .iter()
        .find(|spec| spec.discriminator == discriminator.bytes)
    else {
        return DecodeOutcome::Unsupported { discriminator };
    };

    if data.len() < spec.minimum_data_len {
        return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
            needed: spec.minimum_data_len,
            actual: data.len(),
        });
    }
    if let Err(reason) = validate_body_lower_bound(data, spec.body) {
        return DecodeOutcome::Malformed(reason);
    }
    if accounts.len() < spec.required_accounts {
        return DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
            needed: spec.required_accounts,
            actual: accounts.len(),
        });
    }

    let decoded_accounts = match decode_anchor_accounts(accounts) {
        Ok(decoded_accounts) => decoded_accounts,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let (amounts, evidence) = match spec.amounts {
        AmountLayout::V2ExactIn => {
            let amount_in = match read_amount(data, 8) {
                Ok(amount) => amount,
                Err(reason) => return DecodeOutcome::Malformed(reason),
            };
            let minimum_amount_out = match read_amount(data, 24) {
                Ok(amount) => amount,
                Err(reason) => return DecodeOutcome::Malformed(reason),
            };
            (
                Amounts::ExactIn {
                    amount_in,
                    minimum_amount_out,
                },
                ROUTER_AMOUNT_EVIDENCE,
            )
        }
        AmountLayout::Unknown => (Amounts::Unknown, ROUTER_EVIDENCE),
    };

    DecodeOutcome::Decoded(DecodedInstruction {
        program,
        role: ProgramRole::Router,
        name: spec.name,
        class: InstructionClass::Route,
        discriminator,
        accounts: decoded_accounts,
        amounts,
        evidence,
    })
}

fn validate_body_lower_bound(data: &[u8], body: BodyLayout) -> Result<(), MalformedReason> {
    match body {
        BodyLayout::V2SwapArgs { trailing_data_len } => {
            validate_v2_swap_args_lower_bound(data, trailing_data_len)
        }
        BodyLayout::V3SwapArgs { trailing_data_len } => {
            validate_flat_route_vec_lower_bound(data, 34, trailing_data_len)
        }
        BodyLayout::V3TokenLedger { trailing_data_len } => {
            validate_flat_route_vec_lower_bound(data, 26, trailing_data_len)
        }
    }
}

/// Checks the fixed-width `amounts` vector and conservative lower bounds for
/// the nested v2 route vectors. It follows consecutive empty route groups and
/// validates the first non-empty group. A Route is at least two Borsh vector
/// prefixes (dexes and weights), so its absolute minimum is eight bytes.
fn validate_v2_swap_args_lower_bound(
    data: &[u8],
    trailing_data_len: usize,
) -> Result<(), MalformedReason> {
    const AMOUNTS_LEN_OFFSET: usize = 32;
    const MINIMUM_ROUTE_LEN: usize = 8;

    let amounts_count_end = checked_add(AMOUNTS_LEN_OFFSET, 4, AMOUNTS_LEN_OFFSET)?;
    let amounts_count = read_count(data, AMOUNTS_LEN_OFFSET, amounts_count_end)?;
    let amounts_bytes = checked_mul(amounts_count, 8, AMOUNTS_LEN_OFFSET)?;
    let routes_len_offset = checked_add(amounts_count_end, amounts_bytes, AMOUNTS_LEN_OFFSET)?;
    let routes_count_end = checked_add(routes_len_offset, 4, routes_len_offset)?;
    let route_group_count = read_count(data, routes_len_offset, routes_count_end)?;

    let route_group_headers = checked_mul(route_group_count, 4, routes_len_offset)?;
    let minimum_end = checked_add(routes_count_end, route_group_headers, routes_len_offset)
        .and_then(|end| checked_add(end, trailing_data_len, routes_len_offset))?;
    require_data_len(data, minimum_end)?;

    let mut cursor = routes_count_end;
    let mut remaining_groups = route_group_count;
    while remaining_groups != 0 {
        let group_count_end = checked_add(cursor, 4, cursor)?;
        let route_count = read_count(data, cursor, group_count_end)?;
        remaining_groups -= 1;

        if route_count == 0 {
            cursor = group_count_end;
            continue;
        }

        let route_bytes = checked_mul(route_count, MINIMUM_ROUTE_LEN, cursor)?;
        let remaining_headers = checked_mul(remaining_groups, 4, cursor)?;
        let needed = checked_add(group_count_end, route_bytes, cursor)
            .and_then(|end| checked_add(end, remaining_headers, cursor))
            .and_then(|end| checked_add(end, trailing_data_len, cursor))?;
        return require_data_len(data, needed);
    }

    let needed = checked_add(cursor, trailing_data_len, routes_len_offset)?;
    require_data_len(data, needed)
}

/// Checks a v3 `Vec<Route>` using the smallest possible route encoding.
/// A route always has a one-byte Dex tag, a u16 weight, and a u8 index.
/// Dex variants with payloads can only make the encoded route larger.
fn validate_flat_route_vec_lower_bound(
    data: &[u8],
    route_count_offset: usize,
    trailing_data_len: usize,
) -> Result<(), MalformedReason> {
    const MINIMUM_ROUTE_LEN: usize = 4;

    let count_end = checked_add(route_count_offset, 4, route_count_offset)?;
    let route_count = read_count(data, route_count_offset, count_end)?;
    let route_bytes = checked_mul(route_count, MINIMUM_ROUTE_LEN, route_count_offset)?;
    let needed = checked_add(count_end, route_bytes, route_count_offset)
        .and_then(|end| checked_add(end, trailing_data_len, route_count_offset))?;
    require_data_len(data, needed)
}

fn read_count(data: &[u8], offset: usize, needed: usize) -> Result<usize, MalformedReason> {
    let count = read_u32_le(data, offset).ok_or(MalformedReason::InstructionDataTooShort {
        needed,
        actual: data.len(),
    })?;
    usize::try_from(count).map_err(|_| MalformedReason::InvalidInstructionData { offset })
}

fn checked_add(left: usize, right: usize, offset: usize) -> Result<usize, MalformedReason> {
    left.checked_add(right)
        .ok_or(MalformedReason::InvalidInstructionData { offset })
}

fn checked_mul(left: usize, right: usize, offset: usize) -> Result<usize, MalformedReason> {
    left.checked_mul(right)
        .ok_or(MalformedReason::InvalidInstructionData { offset })
}

fn require_data_len(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    if data.len() < needed {
        return Err(MalformedReason::InstructionDataTooShort {
            needed,
            actual: data.len(),
        });
    }
    Ok(())
}

fn decode_anchor_accounts(accounts: &[CompactId]) -> Result<AccountRoles, MalformedReason> {
    Ok(AccountRoles {
        user_authority: Some(account(accounts, 0)?),
        user_source: Some(account(accounts, 1)?),
        user_destination: Some(account(accounts, 2)?),
        input_mint: Some(account(accounts, 3)?),
        output_mint: Some(account(accounts, 4)?),
        // Fee slots are optional Anchor accounts and can contain the OKX
        // program-ID sentinel. Compact IDs alone cannot resolve that sentinel,
        // so the classifier does not claim a fee recipient. Token flow does.
        fee_account: None,
        ..AccountRoles::default()
    })
}

fn read_amount(data: &[u8], offset: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(MalformedReason::InstructionDataTooShort {
        needed: offset.saturating_add(8),
        actual: data.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Discriminator;

    #[test]
    fn decodes_v2_as_an_exact_in_router_container() {
        let mut data = [0_u8; 54];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        data[8..16].copy_from_slice(&100_u64.to_le_bytes());
        data[24..32].copy_from_slice(&90_u64.to_le_bytes());
        let accounts = [10, 11, 12, 13, 14];

        assert_eq!(
            decode(Program::OkxRouterV2, &data, &accounts),
            DecodeOutcome::Decoded(DecodedInstruction {
                program: Program::OkxRouterV2,
                role: ProgramRole::Router,
                name: "swap_v3",
                class: InstructionClass::Route,
                discriminator: Discriminator::eight(V2_SWAP_V3),
                accounts: AccountRoles {
                    user_authority: Some(10),
                    user_source: Some(11),
                    user_destination: Some(12),
                    input_mint: Some(13),
                    output_mint: Some(14),
                    ..AccountRoles::default()
                },
                amounts: Amounts::ExactIn {
                    amount_in: 100,
                    minimum_amount_out: 90,
                },
                evidence: ROUTER_AMOUNT_EVIDENCE,
            })
        );
    }

    #[test]
    fn decodes_v3_without_inventing_a_minimum_output() {
        let mut data = [0_u8; 43];
        data[..8].copy_from_slice(&V3_SWAP_TOC_V3);
        let accounts = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23];

        let outcome = decode(Program::OkxRouterV3, &data, &accounts);
        assert!(matches!(outcome, DecodeOutcome::Decoded(_)), "{outcome:?}");
        let DecodeOutcome::Decoded(decoded) = outcome else {
            return;
        };
        assert_eq!(decoded.role, ProgramRole::Router);
        assert_eq!(decoded.class, InstructionClass::Route);
        assert_eq!(decoded.name, "swap_toc_v3");
        assert_eq!(decoded.accounts.user_authority, Some(10));
        assert_eq!(decoded.amounts, Amounts::Unknown);
        assert!(decoded.evidence.contains(Evidence::ROUTE_CONTAINER));
        assert!(!decoded.evidence.contains(Evidence::AMOUNTS));
    }

    #[test]
    fn rejects_truncated_okx_data_and_accounts() {
        assert_eq!(
            decode(Program::OkxRouterV3, &V3_SWAP_TOC_V3, &[0; 14]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 43,
                actual: 8,
            })
        );

        let mut data = [0_u8; 43];
        data[..8].copy_from_slice(&V3_SWAP_TOC_V3);
        assert_eq!(
            decode(Program::OkxRouterV3, &data, &[0; 13]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 14,
                actual: 13,
            })
        );
    }

    #[test]
    fn rejects_a_truncated_v2_amounts_vector() {
        let mut data = [0_u8; 54];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        data[32..36].copy_from_slice(&1_u32.to_le_bytes());

        assert_eq!(
            decode(Program::OkxRouterV2, &data, &[0; 5]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 62,
                actual: 54,
            })
        );
    }

    #[test]
    fn accepts_a_v2_fixed_amount_entry_when_the_shifted_route_header_fits() {
        let mut data = [0_u8; 62];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        data[32..36].copy_from_slice(&1_u32.to_le_bytes());

        assert!(matches!(
            decode(Program::OkxRouterV2, &data, &[0; 5]),
            DecodeOutcome::Decoded(_)
        ));
    }

    #[test]
    fn rejects_a_truncated_v2_nested_route_vector() {
        let mut data = [0_u8; 58];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        data[36..40].copy_from_slice(&1_u32.to_le_bytes());
        data[40..44].copy_from_slice(&1_u32.to_le_bytes());

        assert_eq!(
            decode(Program::OkxRouterV2, &data, &[0; 5]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 66,
                actual: 58,
            })
        );
    }

    #[test]
    fn accepts_one_minimum_size_v2_nested_route() {
        let mut data = [0_u8; 66];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        data[36..40].copy_from_slice(&1_u32.to_le_bytes());
        data[40..44].copy_from_slice(&1_u32.to_le_bytes());

        assert!(matches!(
            decode(Program::OkxRouterV2, &data, &[0; 5]),
            DecodeOutcome::Decoded(_)
        ));
    }

    #[test]
    fn rejects_a_v3_route_count_that_cannot_fit_its_routes() {
        let mut data = [0_u8; 43];
        data[..8].copy_from_slice(&V3_SWAP_TOC_V3);
        data[34..38].copy_from_slice(&1_u32.to_le_bytes());

        assert_eq!(
            decode(Program::OkxRouterV3, &data, &[0; 14]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 47,
                actual: 43,
            })
        );
    }

    #[test]
    fn accepts_one_minimum_size_v3_route() {
        let mut data = [0_u8; 47];
        data[..8].copy_from_slice(&V3_SWAP_TOC_V3);
        data[34..38].copy_from_slice(&1_u32.to_le_bytes());

        assert!(matches!(
            decode(Program::OkxRouterV3, &data, &[0; 14]),
            DecodeOutcome::Decoded(_)
        ));
    }

    #[test]
    fn does_not_claim_an_optional_fee_sentinel_as_a_fee_account() {
        let mut data = [0_u8; 54];
        data[..8].copy_from_slice(&V2_SWAP_V3);
        let outcome = decode(Program::OkxRouterV2, &data, &[10, 11, 12, 13, 14, 15, 16]);
        let DecodeOutcome::Decoded(decoded) = outcome else {
            panic!("expected a decoded route, got {outcome:?}");
        };

        assert_eq!(decoded.accounts.fee_account, None);
    }

    #[test]
    fn rejects_unknown_okx_discriminators() {
        assert_eq!(
            decode(Program::OkxRouterV2, &[0xff; 8], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight([0xff; 8]),
            }
        );
        assert_eq!(
            decode(Program::OkxRouterV3, &[0xff; 8], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight([0xff; 8]),
            }
        );
    }
}

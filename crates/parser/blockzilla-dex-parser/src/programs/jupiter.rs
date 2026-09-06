use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, account, anchor_discriminator,
    read_u32_le,
};

const EXACT_OUT_ROUTE: [u8; 8] = [208, 51, 239, 151, 123, 43, 237, 92];
const ROUTE: [u8; 8] = [229, 23, 203, 151, 122, 227, 173, 42];
const ROUTE_WITH_TOKEN_LEDGER: [u8; 8] = [150, 86, 71, 116, 167, 93, 14, 104];
const SHARED_ACCOUNTS_EXACT_OUT_ROUTE: [u8; 8] = [176, 209, 105, 168, 154, 125, 69, 62];
const SHARED_ACCOUNTS_ROUTE: [u8; 8] = [193, 32, 155, 51, 65, 214, 156, 129];
const SHARED_ACCOUNTS_ROUTE_WITH_TOKEN_LEDGER: [u8; 8] = [230, 121, 143, 80, 119, 159, 106, 170];
const EXACT_OUT_ROUTE_V2: [u8; 8] = [157, 138, 184, 82, 21, 244, 243, 36];
const ROUTE_V2: [u8; 8] = [187, 100, 250, 204, 49, 196, 175, 20];
const SHARED_ACCOUNTS_EXACT_OUT_ROUTE_V2: [u8; 8] = [53, 96, 229, 202, 216, 187, 250, 24];
const SHARED_ACCOUNTS_ROUTE_V2: [u8; 8] = [209, 152, 83, 147, 124, 254, 216, 233];

const ROUTER_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::ROUTE_CONTAINER)
    .union(Evidence::TOKEN_FLOW_REQUIRED);

#[derive(Clone, Copy)]
struct AccountLayout {
    authority: Option<usize>,
    user_authority: usize,
    user_source: usize,
    user_destination: usize,
    input_mint: Option<usize>,
    output_mint: Option<usize>,
}

#[derive(Clone, Copy)]
struct RouteSpec {
    discriminator: [u8; 8],
    name: &'static str,
    minimum_data_len: usize,
    route_plan_len_offset: usize,
    minimum_route_step_len: usize,
    trailing_data_len: usize,
    required_accounts: usize,
    layout: AccountLayout,
}

const DIRECT_EXACT_OUT: AccountLayout = AccountLayout {
    authority: None,
    user_authority: 1,
    user_source: 2,
    user_destination: 3,
    input_mint: Some(5),
    output_mint: Some(6),
};

const DIRECT_ROUTE: AccountLayout = AccountLayout {
    authority: None,
    user_authority: 1,
    user_source: 2,
    user_destination: 3,
    input_mint: None,
    output_mint: Some(5),
};

const SHARED_ROUTE: AccountLayout = AccountLayout {
    authority: Some(1),
    user_authority: 2,
    user_source: 3,
    user_destination: 6,
    input_mint: Some(7),
    output_mint: Some(8),
};

const DIRECT_V2: AccountLayout = AccountLayout {
    authority: None,
    user_authority: 0,
    user_source: 1,
    user_destination: 2,
    input_mint: Some(3),
    output_mint: Some(4),
};

const SHARED_V2: AccountLayout = AccountLayout {
    authority: Some(0),
    user_authority: 1,
    user_source: 2,
    user_destination: 5,
    input_mint: Some(6),
    output_mint: Some(7),
};

// Lengths are the shortest complete Borsh payloads, including an empty route plan.
const ROUTES: &[RouteSpec] = &[
    RouteSpec {
        discriminator: EXACT_OUT_ROUTE,
        name: "exact_out_route",
        minimum_data_len: 31,
        route_plan_len_offset: 8,
        minimum_route_step_len: 4,
        trailing_data_len: 19,
        required_accounts: 11,
        layout: DIRECT_EXACT_OUT,
    },
    RouteSpec {
        discriminator: ROUTE,
        name: "route",
        minimum_data_len: 31,
        route_plan_len_offset: 8,
        minimum_route_step_len: 4,
        trailing_data_len: 19,
        required_accounts: 9,
        layout: DIRECT_ROUTE,
    },
    RouteSpec {
        discriminator: ROUTE_WITH_TOKEN_LEDGER,
        name: "route_with_token_ledger",
        minimum_data_len: 23,
        route_plan_len_offset: 8,
        minimum_route_step_len: 4,
        trailing_data_len: 11,
        required_accounts: 10,
        layout: DIRECT_ROUTE,
    },
    RouteSpec {
        discriminator: SHARED_ACCOUNTS_EXACT_OUT_ROUTE,
        name: "shared_accounts_exact_out_route",
        minimum_data_len: 32,
        route_plan_len_offset: 9,
        minimum_route_step_len: 4,
        trailing_data_len: 19,
        required_accounts: 13,
        layout: SHARED_ROUTE,
    },
    RouteSpec {
        discriminator: SHARED_ACCOUNTS_ROUTE,
        name: "shared_accounts_route",
        minimum_data_len: 32,
        route_plan_len_offset: 9,
        minimum_route_step_len: 4,
        trailing_data_len: 19,
        required_accounts: 13,
        layout: SHARED_ROUTE,
    },
    RouteSpec {
        discriminator: SHARED_ACCOUNTS_ROUTE_WITH_TOKEN_LEDGER,
        name: "shared_accounts_route_with_token_ledger",
        minimum_data_len: 24,
        route_plan_len_offset: 9,
        minimum_route_step_len: 4,
        trailing_data_len: 11,
        required_accounts: 14,
        layout: SHARED_ROUTE,
    },
    RouteSpec {
        discriminator: EXACT_OUT_ROUTE_V2,
        name: "exact_out_route_v2",
        minimum_data_len: 34,
        route_plan_len_offset: 30,
        minimum_route_step_len: 5,
        trailing_data_len: 0,
        required_accounts: 10,
        layout: DIRECT_V2,
    },
    RouteSpec {
        discriminator: ROUTE_V2,
        name: "route_v2",
        minimum_data_len: 34,
        route_plan_len_offset: 30,
        minimum_route_step_len: 5,
        trailing_data_len: 0,
        required_accounts: 10,
        layout: DIRECT_V2,
    },
    RouteSpec {
        discriminator: SHARED_ACCOUNTS_EXACT_OUT_ROUTE_V2,
        name: "shared_accounts_exact_out_route_v2",
        minimum_data_len: 35,
        route_plan_len_offset: 31,
        minimum_route_step_len: 5,
        trailing_data_len: 0,
        required_accounts: 12,
        layout: SHARED_V2,
    },
    RouteSpec {
        discriminator: SHARED_ACCOUNTS_ROUTE_V2,
        name: "shared_accounts_route_v2",
        minimum_data_len: 35,
        route_plan_len_offset: 31,
        minimum_route_step_len: 5,
        trailing_data_len: 0,
        required_accounts: 12,
        layout: SHARED_V2,
    },
];

pub(crate) fn decode(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match anchor_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    let Some(spec) = ROUTES
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
    if let Err(reason) = validate_route_plan_lower_bound(data, *spec) {
        return DecodeOutcome::Malformed(reason);
    }
    if accounts.len() < spec.required_accounts {
        return DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
            needed: spec.required_accounts,
            actual: accounts.len(),
        });
    }

    let decoded_accounts = match decode_accounts(accounts, spec.layout) {
        Ok(decoded_accounts) => decoded_accounts,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };

    DecodeOutcome::Decoded(DecodedInstruction {
        program: Program::JupiterV6,
        role: ProgramRole::Router,
        name: spec.name,
        class: InstructionClass::Route,
        discriminator,
        accounts: decoded_accounts,
        // A route plan is variable-length. Its quoted amount is not the executed
        // amount or the slippage-adjusted limit, so token flow must settle it.
        amounts: Amounts::Unknown,
        evidence: ROUTER_EVIDENCE,
    })
}

/// Checks the Borsh vector prefix and the smallest possible encoded body.
///
/// A Jupiter swap enum can contain version-specific variable fields. Parsing
/// those fields here would couple this classifier to every Jupiter venue
/// revision. This lower-bound check never rejects a valid route, but it does
/// reject a declared step count that cannot fit even tag-only swap variants.
fn validate_route_plan_lower_bound(data: &[u8], spec: RouteSpec) -> Result<(), MalformedReason> {
    let count_end = checked_add(spec.route_plan_len_offset, 4, spec.route_plan_len_offset)?;
    let count = read_u32_le(data, spec.route_plan_len_offset).ok_or(
        MalformedReason::InstructionDataTooShort {
            needed: count_end,
            actual: data.len(),
        },
    )?;
    let count = usize::try_from(count).map_err(|_| MalformedReason::InvalidInstructionData {
        offset: spec.route_plan_len_offset,
    })?;
    let route_bytes = count.checked_mul(spec.minimum_route_step_len).ok_or(
        MalformedReason::InvalidInstructionData {
            offset: spec.route_plan_len_offset,
        },
    )?;
    let needed = checked_add(count_end, route_bytes, spec.route_plan_len_offset)
        .and_then(|end| checked_add(end, spec.trailing_data_len, spec.route_plan_len_offset))?;

    if data.len() < needed {
        return Err(MalformedReason::InstructionDataTooShort {
            needed,
            actual: data.len(),
        });
    }
    Ok(())
}

fn checked_add(left: usize, right: usize, offset: usize) -> Result<usize, MalformedReason> {
    left.checked_add(right)
        .ok_or(MalformedReason::InvalidInstructionData { offset })
}

fn decode_accounts(
    accounts: &[CompactId],
    layout: AccountLayout,
) -> Result<AccountRoles, MalformedReason> {
    Ok(AccountRoles {
        authority: match layout.authority {
            Some(index) => Some(account(accounts, index)?),
            None => None,
        },
        user_authority: Some(account(accounts, layout.user_authority)?),
        user_source: Some(account(accounts, layout.user_source)?),
        user_destination: Some(account(accounts, layout.user_destination)?),
        input_mint: match layout.input_mint {
            Some(index) => Some(account(accounts, index)?),
            None => None,
        },
        output_mint: match layout.output_mint {
            Some(index) => Some(account(accounts, index)?),
            None => None,
        },
        // Legacy Jupiter fee accounts are Anchor optional accounts. Their slot
        // can contain the Jupiter program-ID sentinel, which cannot be filtered
        // from compact IDs here. Token flow resolves a real fee recipient.
        fee_account: None,
        ..AccountRoles::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Discriminator;

    #[test]
    fn decodes_route_v2_as_a_router_container() {
        let mut data = [0_u8; 34];
        data[..8].copy_from_slice(&ROUTE_V2);
        let accounts = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19];

        assert_eq!(
            decode(&data, &accounts),
            DecodeOutcome::Decoded(DecodedInstruction {
                program: Program::JupiterV6,
                role: ProgramRole::Router,
                name: "route_v2",
                class: InstructionClass::Route,
                discriminator: Discriminator::eight(ROUTE_V2),
                accounts: AccountRoles {
                    user_authority: Some(10),
                    user_source: Some(11),
                    user_destination: Some(12),
                    input_mint: Some(13),
                    output_mint: Some(14),
                    ..AccountRoles::default()
                },
                amounts: Amounts::Unknown,
                evidence: ROUTER_EVIDENCE,
            })
        );
    }

    #[test]
    fn rejects_truncated_route_data_and_accounts() {
        assert_eq!(
            decode(&ROUTE_V2, &[0; 10]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 34,
                actual: 8,
            })
        );

        let mut data = [0_u8; 34];
        data[..8].copy_from_slice(&ROUTE_V2);
        assert_eq!(
            decode(&data, &[0; 9]),
            DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort {
                needed: 10,
                actual: 9,
            })
        );
    }

    #[test]
    fn rejects_a_route_plan_count_that_cannot_fit_its_steps() {
        let mut data = [0_u8; 34];
        data[..8].copy_from_slice(&ROUTE_V2);
        data[30..34].copy_from_slice(&1_u32.to_le_bytes());

        assert_eq!(
            decode(&data, &[0; 10]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 39,
                actual: 34,
            })
        );
    }

    #[test]
    fn accepts_a_route_plan_with_one_minimum_size_step() {
        let mut data = [0_u8; 39];
        data[..8].copy_from_slice(&ROUTE_V2);
        data[30..34].copy_from_slice(&1_u32.to_le_bytes());

        assert!(matches!(decode(&data, &[0; 10]), DecodeOutcome::Decoded(_)));
    }

    #[test]
    fn preserves_trailing_fields_in_the_old_route_lower_bound() {
        let mut data = [0_u8; 31];
        data[..8].copy_from_slice(&ROUTE);
        data[8..12].copy_from_slice(&1_u32.to_le_bytes());

        assert_eq!(
            decode(&data, &[0; 9]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 35,
                actual: 31,
            })
        );
    }

    #[test]
    fn rejects_an_unknown_jupiter_discriminator() {
        assert_eq!(
            decode(&[0xff; 8], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::eight([0xff; 8]),
            }
        );
    }
}

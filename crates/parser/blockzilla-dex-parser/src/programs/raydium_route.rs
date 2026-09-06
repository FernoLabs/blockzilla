use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Evidence,
    InstructionClass, MalformedReason, Program, ProgramRole, one_byte_discriminator, read_u64_le,
};

const SWAP_BASE_IN_WITH_USER_ACCOUNT: u8 = 0;

const AMOUNT_EVIDENCE: Evidence = Evidence::AMOUNTS
    .union(Evidence::ROUTE_CONTAINER)
    .union(Evidence::TOKEN_FLOW_REQUIRED);

/// Decodes the Raydium AMM routing program.
///
/// The one-byte selector and the two amount fields are verified by historical
/// Raydium route transactions. The fixed account layout is not exposed because
/// no verified source in this repository defines it.
pub(crate) fn decode(data: &[u8], _accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != SWAP_BASE_IN_WITH_USER_ACCOUNT {
        return DecodeOutcome::Unsupported { discriminator };
    }
    if data.len() < 17 {
        return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
            needed: 17,
            actual: data.len(),
        });
    }

    let Some(amount_in) = read_u64_le(data, 1) else {
        return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
            needed: 9,
            actual: data.len(),
        });
    };
    let Some(minimum_amount_out) = read_u64_le(data, 9) else {
        return DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
            needed: 17,
            actual: data.len(),
        });
    };

    DecodeOutcome::Decoded(DecodedInstruction {
        program: Program::RaydiumRoute,
        role: ProgramRole::Router,
        name: "swap_base_in_with_user_account",
        class: InstructionClass::Route,
        discriminator,
        accounts: AccountRoles::default(),
        amounts: Amounts::ExactIn {
            amount_in,
            minimum_amount_out,
        },
        evidence: AMOUNT_EVIDENCE,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Discriminator;

    #[test]
    fn decodes_verified_rayrouter_amounts_without_invented_roles() {
        let mut data = [0_u8; 17];
        data[0] = SWAP_BASE_IN_WITH_USER_ACCOUNT;
        data[1..9].copy_from_slice(&1_000_000_u64.to_le_bytes());
        data[9..17].copy_from_slice(&42_329_459_u64.to_le_bytes());

        let DecodeOutcome::Decoded(decoded) = decode(&data, &[]) else {
            panic!("verified Raydium route instruction must decode");
        };
        assert_eq!(decoded.program, Program::RaydiumRoute);
        assert_eq!(decoded.role, ProgramRole::Router);
        assert_eq!(decoded.class, InstructionClass::Route);
        assert_eq!(decoded.accounts, AccountRoles::default());
        assert_eq!(
            decoded.amounts,
            Amounts::ExactIn {
                amount_in: 1_000_000,
                minimum_amount_out: 42_329_459,
            }
        );
    }

    #[test]
    fn rejects_short_or_unknown_route_data() {
        assert_eq!(
            decode(&[SWAP_BASE_IN_WITH_USER_ACCOUNT], &[]),
            DecodeOutcome::Malformed(MalformedReason::InstructionDataTooShort {
                needed: 17,
                actual: 1,
            })
        );
        assert_eq!(
            decode(&[0xff], &[]),
            DecodeOutcome::Unsupported {
                discriminator: Discriminator::one(0xff),
            }
        );
    }
}

use crate::{
    AccountRoles, Amounts, CompactId, DecodeOutcome, DecodedInstruction, Evidence,
    InstructionClass, MalformedReason, OrderKind, Program, SwapKind, account,
    one_byte_discriminator, read_u8, read_u64_le,
};

const CLASSIC_SWAP: u8 = 1;
const RAYDIUM_SWAP_BASE_IN: u8 = 9;
const RAYDIUM_SWAP_BASE_OUT: u8 = 11;
const SOLFI_SWAP_V1: u8 = 6;
const SOLFI_SWAP_V2: u8 = 7;
const ZEROFI_SWAP: u8 = 6;
const GAVEL_SWAP: u8 = 0;

const SWAP_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT
    .union(Evidence::AMOUNTS)
    .union(Evidence::TOKEN_FLOW_REQUIRED);
const LAYOUT_EVIDENCE: Evidence = Evidence::ACCOUNT_LAYOUT.union(Evidence::TOKEN_FLOW_REQUIRED);

#[inline]
pub(crate) fn decode(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    match program {
        Program::RaydiumStable => decode_raydium_stable(data, accounts),
        Program::OrcaV2 | Program::OrcaV1 | Program::StepnDex | Program::Saros => {
            decode_spl_token_swap(program, data, accounts)
        }
        Program::Fluxbeam => decode_fluxbeam(data, accounts),
        Program::Phoenix => decode_phoenix(data, accounts),
        Program::StepFinanceSwap => decode_step_finance(data, accounts),
        Program::Saber => decode_saber(data, accounts),
        Program::SolFi => decode_solfi(data, accounts),
        Program::ZeroFi => decode_zerofi(data, accounts),
        Program::Gavel => decode_gavel(data, accounts),
        _ => DecodeOutcome::UnknownProgram,
    }
}

fn decode_raydium_stable(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let (name, class, amounts) = match discriminator.bytes[0] {
        RAYDIUM_SWAP_BASE_IN => {
            if let Err(reason) = require_data(data, 17) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                "swap_base_in",
                InstructionClass::Swap(SwapKind::ExactIn),
                Amounts::ExactIn {
                    amount_in: match amount(data, 1) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    minimum_amount_out: match amount(data, 9) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
            )
        }
        RAYDIUM_SWAP_BASE_OUT => {
            if let Err(reason) = require_data(data, 17) {
                return DecodeOutcome::Malformed(reason);
            }
            (
                "swap_base_out",
                InstructionClass::Swap(SwapKind::ExactOut),
                Amounts::ExactOut {
                    maximum_amount_in: match amount(data, 1) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                    amount_out: match amount(data, 9) {
                        Ok(value) => value,
                        Err(reason) => return DecodeOutcome::Malformed(reason),
                    },
                },
            )
        }
        _ => return DecodeOutcome::Unsupported { discriminator },
    };

    outcome((|| {
        require_accounts(accounts, 18)?;
        Ok(DecodedInstruction {
            program: Program::RaydiumStable,
            role: Program::RaydiumStable.role(),
            name,
            class,
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 1)?),
                authority: Some(account(accounts, 2)?),
                user_authority: Some(account(accounts, 17)?),
                user_source: Some(account(accounts, 15)?),
                user_destination: Some(account(accounts, 16)?),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                ..AccountRoles::default()
            },
            amounts,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_spl_token_swap(program: Program, data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != CLASSIC_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 10)?;
        Ok(DecodedInstruction {
            program,
            role: program.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                authority: Some(account(accounts, 1)?),
                user_authority: Some(account(accounts, 2)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 6)?),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                input_vault: Some(account(accounts, 4)?),
                output_vault: Some(account(accounts, 5)?),
                fee_account: Some(account(accounts, 8)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 1)?,
                minimum_amount_out: amount(data, 9)?,
            },
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_fluxbeam(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != CLASSIC_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 14)?;
        Ok(DecodedInstruction {
            program: Program::Fluxbeam,
            role: Program::Fluxbeam.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                authority: Some(account(accounts, 1)?),
                user_authority: Some(account(accounts, 2)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 6)?),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                input_vault: Some(account(accounts, 4)?),
                output_vault: Some(account(accounts, 5)?),
                input_mint: Some(account(accounts, 9)?),
                output_mint: Some(account(accounts, 10)?),
                fee_account: Some(account(accounts, 8)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 1)?,
                minimum_amount_out: amount(data, 9)?,
            },
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_step_finance(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != CLASSIC_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        // The legacy parser proves the selector and these three account roles.
        // No protocol source remains, so the two scalar meanings stay unknown.
        require_data(data, 17)?;
        require_accounts(accounts, 6)?;
        Ok(DecodedInstruction {
            program: Program::StepFinanceSwap,
            role: Program::StepFinanceSwap.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::Unspecified),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn decode_saber(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != CLASSIC_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 9)?;
        Ok(DecodedInstruction {
            program: Program::Saber,
            role: Program::Saber.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 0)?),
                authority: Some(account(accounts, 1)?),
                user_authority: Some(account(accounts, 2)?),
                user_source: Some(account(accounts, 3)?),
                user_destination: Some(account(accounts, 6)?),
                vault_a: Some(account(accounts, 4)?),
                vault_b: Some(account(accounts, 5)?),
                input_vault: Some(account(accounts, 4)?),
                output_vault: Some(account(accounts, 5)?),
                fee_account: Some(account(accounts, 7)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 1)?,
                minimum_amount_out: amount(data, 9)?,
            },
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_solfi(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    match discriminator.bytes[0] {
        SOLFI_SWAP_V1 => outcome((|| {
            // SolFi v1 has a private, variable payload. Its maintained decoder
            // only proves that a non-empty payload after selector 6 is a swap.
            require_data(data, 2)?;
            require_accounts(accounts, 4)?;
            Ok(DecodedInstruction {
                program: Program::SolFi,
                role: Program::SolFi.role(),
                name: "swap_v1",
                class: InstructionClass::Swap(SwapKind::Unspecified),
                discriminator,
                accounts: AccountRoles {
                    pool: Some(account(accounts, 1)?),
                    vault_a: Some(account(accounts, 2)?),
                    vault_b: Some(account(accounts, 3)?),
                    ..AccountRoles::default()
                },
                amounts: Amounts::Unknown,
                evidence: LAYOUT_EVIDENCE,
            })
        })()),
        SOLFI_SWAP_V2 => outcome((|| {
            require_exact_data(data, 18)?;
            require_accounts(accounts, 8)?;
            let direction = enum_at(data, 17, 1)?;
            let (input_vault, output_vault) = if direction == 0 {
                (account(accounts, 2)?, account(accounts, 3)?)
            } else {
                (account(accounts, 3)?, account(accounts, 2)?)
            };
            Ok(DecodedInstruction {
                program: Program::SolFi,
                role: Program::SolFi.role(),
                name: "swap_v2",
                class: InstructionClass::Swap(SwapKind::ExactIn),
                discriminator,
                accounts: AccountRoles {
                    pool: Some(account(accounts, 1)?),
                    authority: Some(account(accounts, 1)?),
                    user_authority: Some(account(accounts, 0)?),
                    user_source: Some(account(accounts, 4)?),
                    user_destination: Some(account(accounts, 5)?),
                    vault_a: Some(account(accounts, 2)?),
                    vault_b: Some(account(accounts, 3)?),
                    input_vault: Some(input_vault),
                    output_vault: Some(output_vault),
                    ..AccountRoles::default()
                },
                amounts: Amounts::ExactIn {
                    amount_in: amount(data, 1)?,
                    minimum_amount_out: amount(data, 9)?,
                },
                evidence: SWAP_EVIDENCE,
            })
        })()),
        _ => DecodeOutcome::Unsupported { discriminator },
    }
}

fn decode_zerofi(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != ZEROFI_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_data(data, 17)?;
        require_accounts(accounts, 10)?;
        Ok(DecodedInstruction {
            program: Program::ZeroFi,
            role: Program::ZeroFi.role(),
            name: "swap",
            class: InstructionClass::Swap(SwapKind::ExactIn),
            discriminator,
            accounts: AccountRoles {
                user_authority: Some(account(accounts, 7)?),
                user_source: Some(account(accounts, 5)?),
                user_destination: Some(account(accounts, 6)?),
                vault_a: Some(account(accounts, 2)?),
                vault_b: Some(account(accounts, 4)?),
                input_vault: Some(account(accounts, 2)?),
                output_vault: Some(account(accounts, 4)?),
                ..AccountRoles::default()
            },
            amounts: Amounts::ExactIn {
                amount_in: amount(data, 1)?,
                minimum_amount_out: amount(data, 9)?,
            },
            evidence: SWAP_EVIDENCE,
        })
    })())
}

fn decode_gavel(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    if discriminator.bytes[0] != GAVEL_SWAP {
        return DecodeOutcome::Unsupported { discriminator };
    }

    outcome((|| {
        require_exact_data(data, 19)?;
        require_accounts(accounts, 9)?;
        let side = enum_at(data, 1, 1)?;
        let swap_type = enum_at(data, 2, 1)?;
        let first_amount = amount(data, 3)?;
        let second_amount = amount(data, 11)?;
        let (user_source, user_destination, input_vault, output_vault) = if side == 0 {
            (
                account(accounts, 5)?,
                account(accounts, 4)?,
                account(accounts, 7)?,
                account(accounts, 6)?,
            )
        } else {
            (
                account(accounts, 4)?,
                account(accounts, 5)?,
                account(accounts, 6)?,
                account(accounts, 7)?,
            )
        };
        let (class, amounts) = if swap_type == 0 {
            (
                InstructionClass::Swap(SwapKind::ExactIn),
                Amounts::ExactIn {
                    amount_in: first_amount,
                    minimum_amount_out: second_amount,
                },
            )
        } else {
            (
                InstructionClass::Swap(SwapKind::ExactOut),
                Amounts::ExactOut {
                    maximum_amount_in: second_amount,
                    amount_out: first_amount,
                },
            )
        };
        Ok(DecodedInstruction {
            program: Program::Gavel,
            role: Program::Gavel.role(),
            name: "swap",
            class,
            discriminator,
            accounts: AccountRoles {
                pool: Some(account(accounts, 2)?),
                user_authority: Some(account(accounts, 3)?),
                user_source: Some(user_source),
                user_destination: Some(user_destination),
                vault_a: Some(account(accounts, 6)?),
                vault_b: Some(account(accounts, 7)?),
                input_vault: Some(input_vault),
                output_vault: Some(output_vault),
                ..AccountRoles::default()
            },
            amounts,
            evidence: SWAP_EVIDENCE,
        })
    })())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PhoenixPacketKind {
    PostOnly,
    Limit,
    ImmediateOrCancel,
}

#[derive(Clone, Copy)]
struct PhoenixPacket {
    kind: PhoenixPacketKind,
    side: u8,
    use_only_deposited_funds: bool,
    use_only_deposited_funds_offset: usize,
}

fn decode_phoenix(data: &[u8], accounts: &[CompactId]) -> DecodeOutcome {
    let discriminator = match one_byte_discriminator(data) {
        Ok(discriminator) => discriminator,
        Err(reason) => return DecodeOutcome::Malformed(reason),
    };
    let (name, class, required_accounts, must_be_ioc, uses_free_funds) =
        match discriminator.bytes[0] {
            0 => (
                "swap_order",
                InstructionClass::Order(OrderKind::PlaceTake),
                9,
                true,
                false,
            ),
            1 => (
                "swap_order_with_free_funds",
                InstructionClass::Order(OrderKind::PlaceTake),
                5,
                true,
                true,
            ),
            2 => (
                "place_limit_order",
                InstructionClass::Order(OrderKind::Place),
                10,
                false,
                false,
            ),
            3 => (
                "place_limit_order_with_free_funds",
                InstructionClass::Order(OrderKind::Place),
                5,
                false,
                true,
            ),
            _ => return DecodeOutcome::Unsupported { discriminator },
        };

    outcome((|| {
        require_accounts(accounts, required_accounts)?;
        let packet = phoenix_packet(data)?;
        let packet_is_ioc = packet.kind == PhoenixPacketKind::ImmediateOrCancel;
        if packet_is_ioc != must_be_ioc {
            return Err(MalformedReason::InvalidInstructionData { offset: 1 });
        }
        if packet.use_only_deposited_funds != uses_free_funds {
            return Err(MalformedReason::InvalidInstructionData {
                offset: packet.use_only_deposited_funds_offset,
            });
        }

        let mut roles = AccountRoles {
            pool: Some(account(accounts, 2)?),
            user_authority: Some(account(accounts, 3)?),
            ..AccountRoles::default()
        };
        if !uses_free_funds {
            let offset = if discriminator.bytes[0] == 2 { 1 } else { 0 };
            let user_base = account(accounts, 4 + offset)?;
            let user_quote = account(accounts, 5 + offset)?;
            let base_vault = account(accounts, 6 + offset)?;
            let quote_vault = account(accounts, 7 + offset)?;
            roles.vault_a = Some(base_vault);
            roles.vault_b = Some(quote_vault);
            if packet.side == 0 {
                roles.user_source = Some(user_quote);
                roles.user_destination = Some(user_base);
                roles.input_vault = Some(quote_vault);
                roles.output_vault = Some(base_vault);
            } else {
                roles.user_source = Some(user_base);
                roles.user_destination = Some(user_quote);
                roles.input_vault = Some(base_vault);
                roles.output_vault = Some(quote_vault);
            }
        }

        Ok(DecodedInstruction {
            program: Program::Phoenix,
            role: Program::Phoenix.role(),
            name,
            class,
            discriminator,
            accounts: roles,
            amounts: Amounts::Unknown,
            evidence: LAYOUT_EVIDENCE,
        })
    })())
}

fn phoenix_packet(data: &[u8]) -> Result<PhoenixPacket, MalformedReason> {
    let first_reason = match parse_phoenix_packet(data, 0) {
        Ok(packet) => return Ok(packet),
        Err(reason) => reason,
    };
    // Phoenix accepts old packets that omit up to three trailing fields. The
    // program appends these zero bytes before it retries Borsh decoding.
    for padding in [3, 2, 1] {
        if let Ok(packet) = parse_phoenix_packet(data, padding) {
            return Ok(packet);
        }
    }
    Err(first_reason)
}

fn parse_phoenix_packet(
    data: &[u8],
    virtual_zero_padding: usize,
) -> Result<PhoenixPacket, MalformedReason> {
    let mut cursor = PhoenixCursor::new(data, virtual_zero_padding)?;
    cursor.position = 1;
    let kind = match cursor.byte()? {
        0 => PhoenixPacketKind::PostOnly,
        1 => PhoenixPacketKind::Limit,
        2 => PhoenixPacketKind::ImmediateOrCancel,
        _ => return Err(cursor.invalid_previous()),
    };
    let side = cursor.enum_byte(1)?;
    let (use_only_deposited_funds, use_only_deposited_funds_offset) = match kind {
        PhoenixPacketKind::PostOnly => {
            cursor.skip(32)?;
            cursor.boolean()?;
            let offset = cursor.position;
            let value = cursor.boolean()?;
            cursor.option_u64()?;
            cursor.option_u64()?;
            cursor.boolean()?;
            (value, offset)
        }
        PhoenixPacketKind::Limit => {
            cursor.skip(16)?;
            cursor.enum_byte(2)?;
            cursor.option_u64()?;
            cursor.skip(16)?;
            let offset = cursor.position;
            let value = cursor.boolean()?;
            cursor.option_u64()?;
            cursor.option_u64()?;
            cursor.boolean()?;
            (value, offset)
        }
        PhoenixPacketKind::ImmediateOrCancel => {
            cursor.option_u64()?;
            cursor.skip(32)?;
            cursor.enum_byte(2)?;
            cursor.option_u64()?;
            cursor.skip(16)?;
            let offset = cursor.position;
            let value = cursor.boolean()?;
            cursor.option_u64()?;
            cursor.option_u64()?;
            (value, offset)
        }
    };
    cursor.finish()?;
    Ok(PhoenixPacket {
        kind,
        side,
        use_only_deposited_funds,
        use_only_deposited_funds_offset,
    })
}

struct PhoenixCursor<'a> {
    data: &'a [u8],
    position: usize,
    virtual_len: usize,
}

impl<'a> PhoenixCursor<'a> {
    fn new(data: &'a [u8], padding: usize) -> Result<Self, MalformedReason> {
        let Some(virtual_len) = data.len().checked_add(padding) else {
            return Err(MalformedReason::InvalidInstructionData { offset: data.len() });
        };
        Ok(Self {
            data,
            position: 0,
            virtual_len,
        })
    }

    fn byte(&mut self) -> Result<u8, MalformedReason> {
        if self.position >= self.virtual_len {
            return Err(MalformedReason::InstructionDataTooShort {
                needed: self.position.saturating_add(1),
                actual: self.data.len(),
            });
        }
        let value = match self.data.get(self.position).copied() {
            Some(value) => value,
            None => 0,
        };
        self.position += 1;
        Ok(value)
    }

    fn boolean(&mut self) -> Result<bool, MalformedReason> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(self.invalid_previous()),
        }
    }

    fn enum_byte(&mut self, maximum: u8) -> Result<u8, MalformedReason> {
        let value = self.byte()?;
        if value <= maximum {
            Ok(value)
        } else {
            Err(self.invalid_previous())
        }
    }

    fn option_u64(&mut self) -> Result<(), MalformedReason> {
        match self.byte()? {
            0 => Ok(()),
            1 => self.skip(8),
            _ => Err(self.invalid_previous()),
        }
    }

    fn skip(&mut self, count: usize) -> Result<(), MalformedReason> {
        let Some(end) = self.position.checked_add(count) else {
            return Err(MalformedReason::InvalidInstructionData {
                offset: self.position,
            });
        };
        if end > self.virtual_len {
            return Err(MalformedReason::InstructionDataTooShort {
                needed: end,
                actual: self.data.len(),
            });
        }
        self.position = end;
        Ok(())
    }

    fn finish(&self) -> Result<(), MalformedReason> {
        if self.position == self.virtual_len {
            Ok(())
        } else {
            Err(MalformedReason::InvalidInstructionData {
                offset: self.position,
            })
        }
    }

    fn invalid_previous(&self) -> MalformedReason {
        MalformedReason::InvalidInstructionData {
            offset: self.position.saturating_sub(1),
        }
    }
}

#[inline]
fn require_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    if data.len() < needed {
        Err(MalformedReason::InstructionDataTooShort {
            needed,
            actual: data.len(),
        })
    } else {
        Ok(())
    }
}

#[inline]
fn require_exact_data(data: &[u8], needed: usize) -> Result<(), MalformedReason> {
    require_data(data, needed)?;
    if data.len() == needed {
        Ok(())
    } else {
        Err(MalformedReason::InvalidInstructionData { offset: needed })
    }
}

#[inline]
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

#[inline]
fn amount(data: &[u8], offset: usize) -> Result<u64, MalformedReason> {
    read_u64_le(data, offset).ok_or(MalformedReason::InstructionDataTooShort {
        needed: offset.saturating_add(8),
        actual: data.len(),
    })
}

#[inline]
fn enum_at(data: &[u8], offset: usize, maximum: u8) -> Result<u8, MalformedReason> {
    match read_u8(data, offset) {
        Some(value) if value <= maximum => Ok(value),
        Some(_) => Err(MalformedReason::InvalidInstructionData { offset }),
        None => Err(MalformedReason::InstructionDataTooShort {
            needed: offset.saturating_add(1),
            actual: data.len(),
        }),
    }
}

#[inline]
fn outcome(result: Result<DecodedInstruction, MalformedReason>) -> DecodeOutcome {
    match result {
        Ok(decoded) => DecodeOutcome::Decoded(decoded),
        Err(reason) => DecodeOutcome::Malformed(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Case<'a> {
        program: Program,
        data: &'a [u8],
        account_count: usize,
    }

    fn two_amounts(tag: u8, first: u64, second: u64) -> [u8; 17] {
        let mut data = [0_u8; 17];
        data[0] = tag;
        data[1..9].copy_from_slice(&first.to_le_bytes());
        data[9..17].copy_from_slice(&second.to_le_bytes());
        data
    }

    fn solfi_v2(first: u64, second: u64, direction: u8) -> [u8; 18] {
        let mut data = [0_u8; 18];
        data[..17].copy_from_slice(&two_amounts(SOLFI_SWAP_V2, first, second));
        data[17] = direction;
        data
    }

    fn gavel(side: u8, swap_type: u8, first: u64, second: u64) -> [u8; 19] {
        let mut data = [0_u8; 19];
        data[0] = GAVEL_SWAP;
        data[1] = side;
        data[2] = swap_type;
        data[3..11].copy_from_slice(&first.to_le_bytes());
        data[11..19].copy_from_slice(&second.to_le_bytes());
        data
    }

    fn phoenix_ioc(tag: u8, side: u8, free_funds: bool) -> [u8; 57] {
        let mut data = [0_u8; 57];
        data[0] = tag;
        data[1] = 2;
        data[2] = side;
        data[4] = 1;
        data[54] = u8::from(free_funds);
        data
    }

    fn phoenix_limit(tag: u8, side: u8, free_funds: bool) -> [u8; 41] {
        let mut data = [0_u8; 41];
        data[0] = tag;
        data[1] = 1;
        data[2] = side;
        data[3] = 1;
        data[11] = 1;
        data[37] = u8::from(free_funds);
        data
    }

    fn account_ids() -> [CompactId; 18] {
        let mut accounts = [0_u32; 18];
        let mut index = 0;
        while index < accounts.len() {
            accounts[index] = index as u32 + 100;
            index += 1;
        }
        accounts
    }

    #[test]
    fn table_accepts_valid_and_rejects_truncated_accounts_or_selector() {
        let raydium = two_amounts(RAYDIUM_SWAP_BASE_IN, 11, 7);
        let orca_v2 = two_amounts(CLASSIC_SWAP, 12, 8);
        let orca_v1 = two_amounts(CLASSIC_SWAP, 13, 9);
        let stepn = two_amounts(CLASSIC_SWAP, 14, 10);
        let fluxbeam = two_amounts(CLASSIC_SWAP, 15, 11);
        let phoenix = phoenix_ioc(0, 0, false);
        let saros = two_amounts(CLASSIC_SWAP, 16, 12);
        let step = two_amounts(CLASSIC_SWAP, 17, 13);
        let saber = two_amounts(CLASSIC_SWAP, 18, 14);
        let solfi = solfi_v2(19, 15, 0);
        let zerofi = two_amounts(ZEROFI_SWAP, 20, 16);
        let gavel = gavel(0, 0, 21, 17);
        let cases = [
            Case {
                program: Program::RaydiumStable,
                data: &raydium,
                account_count: 18,
            },
            Case {
                program: Program::OrcaV2,
                data: &orca_v2,
                account_count: 10,
            },
            Case {
                program: Program::OrcaV1,
                data: &orca_v1,
                account_count: 10,
            },
            Case {
                program: Program::StepnDex,
                data: &stepn,
                account_count: 10,
            },
            Case {
                program: Program::Fluxbeam,
                data: &fluxbeam,
                account_count: 14,
            },
            Case {
                program: Program::Phoenix,
                data: &phoenix,
                account_count: 9,
            },
            Case {
                program: Program::Saros,
                data: &saros,
                account_count: 10,
            },
            Case {
                program: Program::StepFinanceSwap,
                data: &step,
                account_count: 6,
            },
            Case {
                program: Program::Saber,
                data: &saber,
                account_count: 9,
            },
            Case {
                program: Program::SolFi,
                data: &solfi,
                account_count: 8,
            },
            Case {
                program: Program::ZeroFi,
                data: &zerofi,
                account_count: 10,
            },
            Case {
                program: Program::Gavel,
                data: &gavel,
                account_count: 9,
            },
        ];
        let accounts = account_ids();

        for case in &cases {
            assert!(matches!(
                decode(case.program, case.data, &accounts[..case.account_count]),
                DecodeOutcome::Decoded(DecodedInstruction { program, .. }) if program == case.program
            ));
            assert!(matches!(
                decode(
                    case.program,
                    &case.data[..case.data.len() - 4],
                    &accounts[..case.account_count]
                ),
                DecodeOutcome::Malformed(_)
            ));
            assert!(matches!(
                decode(case.program, case.data, &accounts[..case.account_count - 1]),
                DecodeOutcome::Malformed(MalformedReason::InstructionAccountsTooShort { .. })
            ));
            assert!(matches!(
                decode(case.program, &[255], &accounts),
                DecodeOutcome::Unsupported { .. }
            ));
        }
    }

    #[test]
    fn verified_amounts_directions_and_authorities_are_exact() {
        let accounts = account_ids();
        let solfi = solfi_v2(91, 82, 1);
        match decode(Program::SolFi, &solfi, &accounts[..8]) {
            DecodeOutcome::Decoded(decoded) => {
                assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactIn));
                assert_eq!(decoded.accounts.authority, Some(101));
                assert_eq!(decoded.accounts.user_authority, Some(100));
                assert_eq!(decoded.accounts.input_vault, Some(103));
                assert_eq!(decoded.accounts.output_vault, Some(102));
                assert_eq!(
                    decoded.amounts,
                    Amounts::ExactIn {
                        amount_in: 91,
                        minimum_amount_out: 82,
                    }
                );
            }
            other => assert!(matches!(other, DecodeOutcome::Decoded(_))),
        }

        let zerofi = two_amounts(ZEROFI_SWAP, 73, 64);
        match decode(Program::ZeroFi, &zerofi, &accounts[..10]) {
            DecodeOutcome::Decoded(decoded) => {
                assert_eq!(decoded.accounts.pool, None);
                assert_eq!(decoded.accounts.user_authority, Some(107));
                assert_eq!(decoded.accounts.user_source, Some(105));
                assert_eq!(decoded.accounts.user_destination, Some(106));
            }
            other => assert!(matches!(other, DecodeOutcome::Decoded(_))),
        }

        let exact_out = gavel(1, 1, 55, 66);
        match decode(Program::Gavel, &exact_out, &accounts[..9]) {
            DecodeOutcome::Decoded(decoded) => {
                assert_eq!(decoded.class, InstructionClass::Swap(SwapKind::ExactOut));
                assert_eq!(decoded.accounts.user_source, Some(104));
                assert_eq!(decoded.accounts.user_destination, Some(105));
                assert_eq!(
                    decoded.amounts,
                    Amounts::ExactOut {
                        maximum_amount_in: 66,
                        amount_out: 55,
                    }
                );
            }
            other => assert!(matches!(other, DecodeOutcome::Decoded(_))),
        }
    }

    #[test]
    fn phoenix_orders_accept_current_and_legacy_packets() {
        let accounts = account_ids();
        let swap = phoenix_ioc(0, 0, false);
        let swap_free = phoenix_ioc(1, 1, true);
        let place = phoenix_limit(2, 1, false);
        let place_free = phoenix_limit(3, 0, true);
        let cases = [
            (&swap[..], 9, OrderKind::PlaceTake),
            (&swap_free[..], 5, OrderKind::PlaceTake),
            (&place[..], 10, OrderKind::Place),
            (&place_free[..], 5, OrderKind::Place),
        ];
        for (data, account_count, expected_kind) in cases {
            assert!(matches!(
                decode(Program::Phoenix, data, &accounts[..account_count]),
                DecodeOutcome::Decoded(DecodedInstruction {
                    class: InstructionClass::Order(kind), ..
                }) if kind == expected_kind
            ));
        }

        // The official program pads these three omitted trailing zero fields.
        assert!(matches!(
            decode(Program::Phoenix, &place[..38], &accounts[..10]),
            DecodeOutcome::Decoded(_)
        ));
    }

    #[test]
    fn invalid_enums_booleans_and_packet_kinds_fail_closed() {
        let accounts = account_ids();
        let solfi = solfi_v2(1, 0, 2);
        assert!(matches!(
            decode(Program::SolFi, &solfi, &accounts[..8]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 17 })
        ));

        let invalid_side = gavel(2, 0, 1, 0);
        let invalid_type = gavel(0, 2, 1, 0);
        assert!(matches!(
            decode(Program::Gavel, &invalid_side, &accounts[..9]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 1 })
        ));
        assert!(matches!(
            decode(Program::Gavel, &invalid_type, &accounts[..9]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 2 })
        ));

        let wrong_packet = phoenix_limit(0, 0, false);
        assert!(matches!(
            decode(Program::Phoenix, &wrong_packet, &accounts[..9]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 1 })
        ));
        let mut invalid_bool = phoenix_ioc(0, 0, false);
        invalid_bool[54] = 2;
        assert!(matches!(
            decode(Program::Phoenix, &invalid_bool, &accounts[..9]),
            DecodeOutcome::Malformed(MalformedReason::InvalidInstructionData { offset: 54 })
        ));
    }

    #[test]
    fn private_or_closed_formats_remain_structural_only() {
        let accounts = account_ids();
        let v1 = [SOLFI_SWAP_V1, 1, 2, 3, 4];
        assert!(matches!(
            decode(Program::SolFi, &v1, &accounts[..4]),
            DecodeOutcome::Decoded(DecodedInstruction {
                class: InstructionClass::Swap(SwapKind::Unspecified),
                amounts: Amounts::Unknown,
                ..
            })
        ));
        let step = two_amounts(CLASSIC_SWAP, 1, 0);
        assert!(matches!(
            decode(Program::StepFinanceSwap, &step, &accounts[..6]),
            DecodeOutcome::Decoded(DecodedInstruction {
                class: InstructionClass::Swap(SwapKind::Unspecified),
                amounts: Amounts::Unknown,
                ..
            })
        ));
    }
}

use super::{
    ClassicTokenDecodeError, ClassicTokenInstruction, DecodedClassicTokenInstruction,
    MAX_TOKEN_INSTRUCTION_ACCOUNTS, MAX_TOKEN_INSTRUCTION_DATA_BYTES, PubkeyBytes,
    TokenAccountRole, TokenAccountRoleBinding, TokenAuthorityType,
};

/// Decode one classic SPL Token instruction.
///
/// This function follows the stable classic SPL Token 3.0 data grammar. It
/// keeps unused trailing data because the classic program accepts it for most
/// tags. Use [`super::decode_classic_token_batch`] to validate Batch children.
pub fn decode_classic_token_instruction(
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Result<DecodedClassicTokenInstruction, ClassicTokenDecodeError> {
    check_classic_source_geometry(accounts, data)?;
    let Some(&tag) = data.first() else {
        return Err(ClassicTokenDecodeError::EmptyData);
    };
    if !matches!(tag, 0..=24 | 38 | 45 | 255) {
        return Err(ClassicTokenDecodeError::UnknownTag { tag });
    }

    let mut cursor = DataCursor::new(tag, data);
    let instruction = match tag {
        0 => ClassicTokenInstruction::InitializeMint {
            decimals: cursor.read_u8()?,
            mint_authority: cursor.read_pubkey()?,
            freeze_authority: cursor.read_optional_pubkey()?,
        },
        1 => ClassicTokenInstruction::InitializeAccount,
        2 => ClassicTokenInstruction::InitializeMultisig {
            required_signers: cursor.read_u8()?,
        },
        3 => ClassicTokenInstruction::Transfer {
            amount: cursor.read_u64()?,
        },
        4 => ClassicTokenInstruction::Approve {
            amount: cursor.read_u64()?,
        },
        5 => ClassicTokenInstruction::Revoke,
        6 => ClassicTokenInstruction::SetAuthority {
            authority_type: cursor.read_authority_type()?,
            new_authority: cursor.read_optional_pubkey()?,
        },
        7 => ClassicTokenInstruction::MintTo {
            amount: cursor.read_u64()?,
        },
        8 => ClassicTokenInstruction::Burn {
            amount: cursor.read_u64()?,
        },
        9 => ClassicTokenInstruction::CloseAccount,
        10 => ClassicTokenInstruction::FreezeAccount,
        11 => ClassicTokenInstruction::ThawAccount,
        12 => ClassicTokenInstruction::TransferChecked {
            amount: cursor.read_u64()?,
            decimals: cursor.read_u8()?,
        },
        13 => ClassicTokenInstruction::ApproveChecked {
            amount: cursor.read_u64()?,
            decimals: cursor.read_u8()?,
        },
        14 => ClassicTokenInstruction::MintToChecked {
            amount: cursor.read_u64()?,
            decimals: cursor.read_u8()?,
        },
        15 => ClassicTokenInstruction::BurnChecked {
            amount: cursor.read_u64()?,
            decimals: cursor.read_u8()?,
        },
        16 => ClassicTokenInstruction::InitializeAccount2 {
            owner: cursor.read_pubkey()?,
        },
        17 => ClassicTokenInstruction::SyncNative,
        18 => ClassicTokenInstruction::InitializeAccount3 {
            owner: cursor.read_pubkey()?,
        },
        19 => ClassicTokenInstruction::InitializeMultisig2 {
            required_signers: cursor.read_u8()?,
        },
        20 => ClassicTokenInstruction::InitializeMint2 {
            decimals: cursor.read_u8()?,
            mint_authority: cursor.read_pubkey()?,
            freeze_authority: cursor.read_optional_pubkey()?,
        },
        21 => ClassicTokenInstruction::GetAccountDataSize,
        22 => ClassicTokenInstruction::InitializeImmutableOwner,
        23 => ClassicTokenInstruction::AmountToUiAmount {
            amount: cursor.read_u64()?,
        },
        24 => {
            let source = std::str::from_utf8(cursor.remaining())
                .map_err(|_| ClassicTokenDecodeError::InvalidUiAmountUtf8)?;
            let mut ui_amount = String::new();
            ui_amount.try_reserve_exact(source.len()).map_err(|_| {
                ClassicTokenDecodeError::AllocationFailed {
                    requested: source.len(),
                }
            })?;
            ui_amount.push_str(source);
            cursor.consume_remaining();
            ClassicTokenInstruction::UiAmountToAmount { ui_amount }
        }
        38 => ClassicTokenInstruction::WithdrawExcessLamports,
        45 => ClassicTokenInstruction::UnwrapLamports {
            amount: cursor.read_optional_u64()?,
        },
        255 => ClassicTokenInstruction::Batch,
        _ => unreachable!("the accepted tags were checked"),
    };

    let roles = bind_account_roles(tag, accounts)?;
    let trailing_data = copy_bytes(cursor.remaining())?;
    Ok(DecodedClassicTokenInstruction {
        instruction,
        roles,
        trailing_data,
    })
}

/// Validate one known classic Token instruction without allocation.
pub(super) fn validate_classic_token_instruction_structure(
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Result<(), ClassicTokenDecodeError> {
    check_classic_source_geometry(accounts, data)?;
    let Some(&tag) = data.first() else {
        return Err(ClassicTokenDecodeError::EmptyData);
    };
    if !matches!(tag, 0..=24 | 38 | 45 | 255) {
        return Err(ClassicTokenDecodeError::UnknownTag { tag });
    }

    let mut cursor = DataCursor::new(tag, data);
    match tag {
        0 | 20 => {
            cursor.read_u8()?;
            cursor.read_pubkey()?;
            cursor.read_optional_pubkey()?;
        }
        2 | 19 => {
            cursor.read_u8()?;
        }
        3 | 4 | 7 | 8 | 23 => {
            cursor.read_u64()?;
        }
        6 => {
            cursor.read_authority_type()?;
            cursor.read_optional_pubkey()?;
        }
        12..=15 => {
            cursor.read_u64()?;
            cursor.read_u8()?;
        }
        16 | 18 => {
            cursor.read_pubkey()?;
        }
        24 => {
            std::str::from_utf8(cursor.remaining())
                .map_err(|_| ClassicTokenDecodeError::InvalidUiAmountUtf8)?;
        }
        45 => {
            cursor.read_optional_u64()?;
        }
        1 | 5 | 9..=11 | 17 | 21 | 22 | 38 | 255 => {}
        _ => unreachable!("the accepted tags were checked"),
    }
    validate_account_count(tag, accounts.len())
}

struct DataCursor<'a> {
    tag: u8,
    data: &'a [u8],
    position: usize,
}

impl<'a> DataCursor<'a> {
    fn new(tag: u8, data: &'a [u8]) -> Self {
        Self {
            tag,
            data,
            position: 1,
        }
    }

    fn read_u8(&mut self) -> Result<u8, ClassicTokenDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn read_u64(&mut self) -> Result<u64, ClassicTokenDecodeError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .expect("the cursor returned eight bytes");
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_pubkey(&mut self) -> Result<PubkeyBytes, ClassicTokenDecodeError> {
        Ok(self
            .take(32)?
            .try_into()
            .expect("the cursor returned 32 bytes"))
    }

    fn read_optional_pubkey(&mut self) -> Result<Option<PubkeyBytes>, ClassicTokenDecodeError> {
        let option_tag = self.read_u8()?;
        match option_tag {
            0 => Ok(None),
            1 => self.read_pubkey().map(Some),
            value => Err(ClassicTokenDecodeError::InvalidOptionalPubkeyTag {
                tag: self.tag,
                value,
            }),
        }
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, ClassicTokenDecodeError> {
        let option_tag = self.read_u8()?;
        match option_tag {
            0 => Ok(None),
            1 => self.read_u64().map(Some),
            value => Err(ClassicTokenDecodeError::InvalidOptionalU64Tag {
                tag: self.tag,
                value,
            }),
        }
    }

    fn read_authority_type(&mut self) -> Result<TokenAuthorityType, ClassicTokenDecodeError> {
        match self.read_u8()? {
            0 => Ok(TokenAuthorityType::MintTokens),
            1 => Ok(TokenAuthorityType::FreezeAccount),
            2 => Ok(TokenAuthorityType::AccountOwner),
            3 => Ok(TokenAuthorityType::CloseAccount),
            value => Err(ClassicTokenDecodeError::InvalidAuthorityType { value }),
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ClassicTokenDecodeError> {
        let end =
            self.position
                .checked_add(length)
                .ok_or(ClassicTokenDecodeError::TruncatedData {
                    tag: self.tag,
                    needed: usize::MAX,
                    actual: self.data.len(),
                })?;
        let Some(bytes) = self.data.get(self.position..end) else {
            return Err(ClassicTokenDecodeError::TruncatedData {
                tag: self.tag,
                needed: end,
                actual: self.data.len(),
            });
        };
        self.position = end;
        Ok(bytes)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.data[self.position..]
    }

    fn consume_remaining(&mut self) {
        self.position = self.data.len();
    }
}

fn bind_account_roles(
    tag: u8,
    accounts: &[PubkeyBytes],
) -> Result<Vec<TokenAccountRoleBinding>, ClassicTokenDecodeError> {
    let (fixed, trailing, minimum_account_count) = account_role_layout(tag);
    validate_account_count(tag, accounts.len())?;

    let mut roles = Vec::new();
    roles.try_reserve_exact(accounts.len()).map_err(|_| {
        ClassicTokenDecodeError::AllocationFailed {
            requested: accounts.len(),
        }
    })?;
    for (index, address) in accounts.iter().enumerate() {
        let account_index =
            u32::try_from(index).map_err(|_| ClassicTokenDecodeError::TooManyAccounts)?;
        roles.push(TokenAccountRoleBinding {
            account_index,
            address: *address,
            role: fixed.get(index).copied().unwrap_or(trailing),
        });
    }
    debug_assert!(accounts.len() >= minimum_account_count);
    Ok(roles)
}

fn account_role_layout(tag: u8) -> (&'static [TokenAccountRole], TokenAccountRole, usize) {
    use TokenAccountRole::{
        Additional, Authority, AuthoritySubject, Delegate, Destination, LamportDestination, Mint,
        MultisigAccount, MultisigSigner, Owner, RentSysvar, Source, TokenAccount,
    };

    let (fixed, trailing) = match tag {
        0 => (&[Mint, RentSysvar][..], Additional),
        1 => (&[TokenAccount, Mint, Owner, RentSysvar][..], Additional),
        2 => (&[MultisigAccount, RentSysvar][..], MultisigSigner),
        3 => (&[Source, Destination, Authority][..], MultisigSigner),
        4 => (&[Source, Delegate, Authority][..], MultisigSigner),
        5 => (&[Source, Authority][..], MultisigSigner),
        6 => (&[AuthoritySubject, Authority][..], MultisigSigner),
        7 => (&[Mint, Destination, Authority][..], MultisigSigner),
        8 => (&[Source, Mint, Authority][..], MultisigSigner),
        9 => (
            &[TokenAccount, LamportDestination, Authority][..],
            MultisigSigner,
        ),
        10 | 11 => (&[TokenAccount, Mint, Authority][..], MultisigSigner),
        12 => (&[Source, Mint, Destination, Authority][..], MultisigSigner),
        13 => (&[Source, Mint, Delegate, Authority][..], MultisigSigner),
        14 => (&[Mint, Destination, Authority][..], MultisigSigner),
        15 => (&[Source, Mint, Authority][..], MultisigSigner),
        16 => (&[TokenAccount, Mint, RentSysvar][..], Additional),
        17 => (&[TokenAccount, RentSysvar][..], Additional),
        18 => (&[TokenAccount, Mint][..], Additional),
        19 => (&[MultisigAccount][..], MultisigSigner),
        20 | 21 | 23 | 24 => (&[Mint][..], Additional),
        22 => (&[TokenAccount][..], Additional),
        38 | 45 => (&[Source, LamportDestination, Authority][..], MultisigSigner),
        255 => (&[][..], Additional),
        _ => unreachable!("the accepted tags were checked"),
    };

    // SyncNative accepts the token account alone. If account 1 is present, the
    // current interface defines it as the optional Rent sysvar account.
    let minimum_account_count = if tag == 17 { 1 } else { fixed.len() };
    (fixed, trailing, minimum_account_count)
}

fn validate_account_count(tag: u8, actual: usize) -> Result<(), ClassicTokenDecodeError> {
    let (_, _, minimum_account_count) = account_role_layout(tag);
    if actual < minimum_account_count {
        return Err(ClassicTokenDecodeError::InsufficientAccounts {
            tag,
            needed: minimum_account_count,
            actual,
        });
    }
    Ok(())
}

fn check_classic_source_geometry(
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Result<(), ClassicTokenDecodeError> {
    if data.len() > MAX_TOKEN_INSTRUCTION_DATA_BYTES {
        return Err(ClassicTokenDecodeError::InstructionDataLimit {
            limit: MAX_TOKEN_INSTRUCTION_DATA_BYTES,
            actual: data.len(),
        });
    }
    if accounts.len() > MAX_TOKEN_INSTRUCTION_ACCOUNTS {
        return Err(ClassicTokenDecodeError::InstructionAccountLimit {
            limit: MAX_TOKEN_INSTRUCTION_ACCOUNTS,
            actual: accounts.len(),
        });
    }
    Ok(())
}

fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>, ClassicTokenDecodeError> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len())
        .map_err(|_| ClassicTokenDecodeError::AllocationFailed {
            requested: bytes.len(),
        })?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

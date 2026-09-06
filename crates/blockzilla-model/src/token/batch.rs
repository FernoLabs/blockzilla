use super::{
    ClassicTokenBatchChild, ClassicTokenDecodeError, DecodedClassicTokenBatch,
    MAX_TOKEN_INSTRUCTION_ACCOUNTS, MAX_TOKEN_INSTRUCTION_DATA_BYTES, PubkeyBytes,
};

/// Allocation-free facts about one Batch prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClassicTokenBatchInspection {
    pub child_count: usize,
    pub terminal_error: Option<ClassicTokenDecodeError>,
    pub consumed_account_count: usize,
}

/// One borrowed child from a validated Batch prefix.
pub(super) struct BorrowedClassicTokenBatchChild<'a> {
    pub batch_index: u32,
    pub accounts: &'a [PubkeyBytes],
    pub data: &'a [u8],
}

/// Borrow each child from a validated Batch prefix without allocation.
pub(super) fn borrowed_classic_token_batch_prefix<'a>(
    accounts: &'a [PubkeyBytes],
    data: &'a [u8],
    inspection: &ClassicTokenBatchInspection,
) -> impl Iterator<Item = BorrowedClassicTokenBatchChild<'a>> {
    let mut data_position = 1usize;
    let mut account_position = 0usize;
    let mut batch_index = 0u32;
    let child_count = inspection.child_count;

    std::iter::from_fn(move || {
        if usize::try_from(batch_index).ok()? >= child_count {
            return None;
        }
        let header = data.get(data_position..data_position.checked_add(2)?)?;
        data_position += 2;
        let account_count = usize::from(header[0]);
        let data_length = usize::from(header[1]);
        let child_data_end = data_position.checked_add(data_length)?;
        let child_accounts_end = account_position.checked_add(account_count)?;
        let child = BorrowedClassicTokenBatchChild {
            batch_index,
            accounts: accounts.get(account_position..child_accounts_end)?,
            data: data.get(data_position..child_data_end)?,
        };
        data_position = child_data_end;
        account_position = child_accounts_end;
        batch_index += 1;
        Some(child)
    })
}

/// Inspect Batch geometry without expanding or copying child data.
pub(super) fn inspect_classic_token_batch(
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Result<ClassicTokenBatchInspection, ClassicTokenDecodeError> {
    check_source_geometry(accounts, data)?;
    if data.first() != Some(&255) {
        return Err(ClassicTokenDecodeError::NotBatch);
    }
    if data.len() == 1 {
        return Ok(ClassicTokenBatchInspection {
            child_count: 0,
            terminal_error: Some(ClassicTokenDecodeError::EmptyBatch),
            consumed_account_count: 0,
        });
    }

    let mut data_position = 1usize;
    let mut account_position = 0usize;
    let mut child_count = 0usize;

    while data_position < data.len() {
        // Each child uses at least three parent data bytes. The checked parent
        // data bound keeps this conversion below u32::MAX.
        let batch_index = child_count as u32;
        let Some(header) = data.get(data_position..data_position.saturating_add(2)) else {
            return Ok(inspection_with_error(
                child_count,
                account_position,
                ClassicTokenDecodeError::TruncatedBatchHeader { batch_index },
            ));
        };
        data_position += 2;

        let account_count = usize::from(header[0]);
        let data_length = usize::from(header[1]);
        if data_length == 0 {
            return Ok(inspection_with_error(
                child_count,
                account_position,
                ClassicTokenDecodeError::EmptyBatchChildData { batch_index },
            ));
        }

        let data_available = data.len().saturating_sub(data_position);
        if data_length > data_available {
            return Ok(inspection_with_error(
                child_count,
                account_position,
                ClassicTokenDecodeError::BatchDataOverrun {
                    batch_index,
                    declared: data_length,
                    available: data_available,
                },
            ));
        }
        let child_data_end = data_position + data_length;
        let child_data = &data[data_position..child_data_end];

        // The processor checks account availability before it dispatches the
        // child discriminator. Keep the same error order here.
        let accounts_available = accounts.len().saturating_sub(account_position);
        if account_count > accounts_available {
            return Ok(inspection_with_error(
                child_count,
                account_position,
                ClassicTokenDecodeError::BatchAccountOverrun {
                    batch_index,
                    declared: account_count,
                    available: accounts_available,
                },
            ));
        }
        if child_data.first() == Some(&255) {
            return Ok(inspection_with_error(
                child_count,
                account_position,
                ClassicTokenDecodeError::NestedBatch { batch_index },
            ));
        }

        data_position = child_data_end;
        account_position += account_count;
        child_count += 1;
    }

    Ok(ClassicTokenBatchInspection {
        child_count,
        terminal_error: None,
        consumed_account_count: account_position,
    })
}

/// Decode one classic SPL Token Batch instruction.
///
/// The parent account list contains each child account list in child order.
/// The returned children keep this order. A terminal error does not remove a
/// valid child prefix.
pub fn decode_classic_token_batch(
    accounts: &[PubkeyBytes],
    data: &[u8],
) -> Result<DecodedClassicTokenBatch, ClassicTokenDecodeError> {
    let inspection = inspect_classic_token_batch(accounts, data)?;
    let mut children = Vec::new();
    children
        .try_reserve_exact(inspection.child_count)
        .map_err(|_| ClassicTokenDecodeError::AllocationFailed {
            requested: inspection.child_count,
        })?;

    for child in borrowed_classic_token_batch_prefix(accounts, data, &inspection) {
        let mut child_accounts = Vec::new();
        child_accounts
            .try_reserve_exact(child.accounts.len())
            .map_err(|_| ClassicTokenDecodeError::AllocationFailed {
                requested: child.accounts.len(),
            })?;
        child_accounts.extend_from_slice(child.accounts);

        let mut child_data = Vec::new();
        child_data
            .try_reserve_exact(child.data.len())
            .map_err(|_| ClassicTokenDecodeError::AllocationFailed {
                requested: child.data.len(),
            })?;
        child_data.extend_from_slice(child.data);

        children.push(ClassicTokenBatchChild {
            batch_index: child.batch_index,
            accounts: child_accounts,
            data: child_data,
        });
    }

    Ok(DecodedClassicTokenBatch {
        children,
        terminal_error: inspection.terminal_error,
        consumed_account_count: inspection.consumed_account_count,
    })
}

fn check_source_geometry(
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

fn inspection_with_error(
    child_count: usize,
    consumed_account_count: usize,
    error: ClassicTokenDecodeError,
) -> ClassicTokenBatchInspection {
    ClassicTokenBatchInspection {
        child_count,
        terminal_error: Some(error),
        consumed_account_count,
    }
}

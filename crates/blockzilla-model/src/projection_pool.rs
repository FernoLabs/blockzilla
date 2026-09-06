//! Worker-owned output storage. Recycle once per block, not through a lock per row.
use crate::{CanonicalBlock, RecordedTokenBalance, ResolvedInstruction};

const MAX_RETAINED_BYTES: usize = 8 << 20;
const MAX_BUFFERS: usize = 16_384;

#[derive(Debug, Default)]
pub struct ProjectionPool {
    instructions: Vec<Vec<ResolvedInstruction>>,
    balances: Vec<Vec<RecordedTokenBalance>>,
    data: Vec<Vec<u8>>,
    accounts: Vec<Vec<[u8; 32]>>,
    bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BlockHeader, CanonicalTransaction, CoverageReason, CpiCoverage, ExecutionStatus,
        InstructionCoordinate, InstructionCoverage, InstructionDataCoverage, TokenBalanceCoverage,
        TransactionHeader,
    };

    #[test]
    fn retains_nested_buffers_without_retaining_old_values() {
        let mut pool = ProjectionPool::default();
        let data = vec![7; 128];
        let data_ptr = data.as_ptr();
        let accounts = vec![[8; 32]; 4];
        let account_ptr = accounts.as_ptr();
        let mut block = CanonicalBlock {
            header: BlockHeader {
                epoch: 0,
                block_ordinal: 0,
                slot: 0,
            },
            counts: None,
            transactions: vec![CanonicalTransaction {
                header: TransactionHeader {
                    tx_index: 0,
                    status: ExecutionStatus::Unknown(CoverageReason::MetadataAbsent),
                    failed_outer_instruction_index: None,
                    instruction_coverage: InstructionCoverage::Complete,
                    cpi_coverage: CpiCoverage::Complete,
                },
                primary_signature: None,
                required_signers: Vec::new(),
                instructions: vec![ResolvedInstruction {
                    coordinate: InstructionCoordinate {
                        order: 0,
                        outer_index: 0,
                        inner_index: None,
                        stack_height: None,
                    },
                    program_id: None,
                    accounts,
                    data,
                    data_coverage: InstructionDataCoverage::Exact,
                }],
                token_balance_coverage: TokenBalanceCoverage::NotRequested,
                token_balances: Vec::new(),
            }],
        };
        pool.recycle_block(&mut block);
        let reused_data = pool.copy_data(&[9; 64]).unwrap();
        assert_eq!(reused_data.as_ptr(), data_ptr);
        assert_eq!(reused_data, [9; 64]);
        let reused_accounts = pool.accounts();
        assert_eq!(reused_accounts.as_ptr(), account_ptr);
        assert!(reused_accounts.is_empty());
        assert!(pool.bytes <= MAX_RETAINED_BYTES);
    }
}

impl ProjectionPool {
    /// Copy once into retained output storage. Output can outlive the decode buffer.
    pub fn copy_data(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<u8>, std::collections::TryReserveError> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let mut value = self.data.pop().unwrap_or_default();
        self.bytes -= value.capacity();
        value.try_reserve(bytes.len())?;
        value.extend_from_slice(bytes);
        Ok(value)
    }

    pub fn accounts(&mut self) -> Vec<[u8; 32]> {
        let value = self.accounts.pop().unwrap_or_default();
        self.bytes -= value.capacity() * size_of::<[u8; 32]>();
        value
    }

    pub fn instructions(&mut self) -> Vec<ResolvedInstruction> {
        let value = self.instructions.pop().unwrap_or_default();
        self.bytes -= value.capacity() * size_of::<ResolvedInstruction>();
        value
    }

    pub fn balances(&mut self) -> Vec<RecordedTokenBalance> {
        let value = self.balances.pop().unwrap_or_default();
        self.bytes -= value.capacity() * size_of::<RecordedTokenBalance>();
        value
    }

    pub fn recycle_block(&mut self, block: &mut CanonicalBlock) {
        for mut transaction in block.transactions.drain(..) {
            for mut instruction in transaction.instructions.drain(..) {
                instruction.data.clear();
                let bytes = instruction.data.capacity();
                if bytes != 0
                    && bytes <= MAX_RETAINED_BYTES.saturating_sub(self.bytes)
                    && self.data.len() < MAX_BUFFERS
                {
                    self.bytes += bytes;
                    self.data.push(instruction.data);
                }
                instruction.accounts.clear();
                let bytes = instruction.accounts.capacity() * size_of::<[u8; 32]>();
                if bytes != 0
                    && bytes <= MAX_RETAINED_BYTES.saturating_sub(self.bytes)
                    && self.accounts.len() < MAX_BUFFERS
                {
                    self.bytes += bytes;
                    self.accounts.push(instruction.accounts);
                }
            }
            let bytes = transaction.instructions.capacity() * size_of::<ResolvedInstruction>();
            if bytes != 0
                && bytes <= MAX_RETAINED_BYTES.saturating_sub(self.bytes)
                && self.instructions.len() < MAX_BUFFERS
            {
                self.bytes += bytes;
                self.instructions.push(transaction.instructions);
            }
            transaction.token_balances.clear();
            let bytes = transaction.token_balances.capacity() * size_of::<RecordedTokenBalance>();
            if bytes != 0
                && bytes <= MAX_RETAINED_BYTES.saturating_sub(self.bytes)
                && self.balances.len() < MAX_BUFFERS
            {
                self.bytes += bytes;
                self.balances.push(transaction.token_balances);
            }
        }
    }
}

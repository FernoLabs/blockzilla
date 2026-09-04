//! Worker-owned output storage. Recycle once per block, not through a lock per row.
use crate::{CanonicalBlock, RecordedTokenBalance, ResolvedInstruction};

const MAX_RETAINED_BYTES: usize = 8 << 20;
const MAX_BUFFERS: usize = 16_384;

#[derive(Debug, Default)]
pub struct ProjectionPool {
    instructions: Vec<Vec<ResolvedInstruction>>,
    balances: Vec<Vec<RecordedTokenBalance>>,
    bytes: usize,
}

impl ProjectionPool {
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
            transaction.instructions.clear();
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

//! Full-field borrowed metadata consumer for the diagnostic probe.
//! No output collections are retained. Block rewards remain on the owned path.
use of_car_reader::{
    confirmed_block_borrowed::solana::storage::ConfirmedBlock::Reward,
    metadata_decoder::{
        InnerInstructionVisit, ReturnDataVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
    },
};
use quick_protobuf::{BytesReader, MessageRead};
use std::hint::black_box;

#[derive(Default)]
pub(super) struct FullMetadataVisitor {
    failed: bool,
    reward_error: Option<quick_protobuf::Error>,
}
impl FullMetadataVisitor {
    pub(super) fn finish(self) -> quick_protobuf::Result<bool> {
        match self.reward_error {
            Some(error) => Err(error),
            None => Ok(self.failed),
        }
    }
}
impl<'a> TransactionStatusMetaVisitor<'a> for FullMetadataVisitor {
    fn wants_status_error(&self) -> bool {
        true
    }
    fn wants_pre_balances(&self) -> bool {
        true
    }
    fn wants_post_balances(&self) -> bool {
        true
    }
    fn wants_inner_instructions(&self) -> bool {
        true
    }
    fn wants_log_messages(&self) -> bool {
        true
    }
    fn wants_pre_token_balances(&self) -> bool {
        true
    }
    fn wants_post_token_balances(&self) -> bool {
        true
    }
    fn wants_rewards(&self) -> bool {
        true
    }
    fn wants_loaded_addresses(&self) -> bool {
        true
    }
    fn wants_return_data(&self) -> bool {
        true
    }
    fn status_error(&mut self, error: &'a [u8]) {
        self.failed = true;
        black_box(error);
    }
    fn reward_raw(&mut self, bytes: &'a [u8]) {
        // The SDK exposes rewards as raw submessages. Decode every field too;
        // simply touching these bytes would make this a partial decode probe.
        match Reward::from_reader(&mut BytesReader::from_bytes(bytes), bytes) {
            Ok(reward) => {
                black_box(reward);
            }
            Err(error) => {
                self.reward_error = Some(error);
            }
        }
    }
    fn fee(&mut self, value: u64) {
        black_box(value);
    }
    fn inner_instruction_group(&mut self, value: u32) {
        black_box(value);
    }
    fn inner_instruction(&mut self, value: InnerInstructionVisit<'a>) {
        black_box(value);
    }
    fn inner_instructions_none(&mut self, value: bool) {
        black_box(value);
    }
    fn log_message(&mut self, value: &'a str) {
        black_box(value);
    }
    fn log_messages_none(&mut self, value: bool) {
        black_box(value);
    }
    fn pre_token_balance(&mut self, value: TokenBalanceVisit<'a>) {
        black_box(value);
    }
    fn post_token_balance(&mut self, value: TokenBalanceVisit<'a>) {
        black_box(value);
    }
    fn loaded_writable_address(&mut self, value: &'a [u8]) {
        black_box(value);
    }
    fn loaded_readonly_address(&mut self, value: &'a [u8]) {
        black_box(value);
    }
    fn return_data(&mut self, value: ReturnDataVisit<'a>) {
        black_box(value);
    }
    fn return_data_none(&mut self, value: bool) {
        black_box(value);
    }
    fn compute_units_consumed(&mut self, value: u64) {
        black_box(value);
    }
    fn cost_units(&mut self, value: u64) {
        black_box(value);
    }
    fn pre_balance(&mut self, index: usize, value: u64) {
        black_box((index, value));
    }
    fn post_balance(&mut self, index: usize, value: u64) {
        black_box((index, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use of_car_reader::metadata_decoder::visit_protobuf_transaction_status_meta;

    #[test]
    fn full_decode_rejects_malformed_reward_and_log_payloads() {
        // Rewards arrive as raw submessages: failure must reach the caller,
        // even though the SDK's raw callback itself cannot return an error.
        let mut visitor = FullMetadataVisitor::default();
        visit_protobuf_transaction_status_meta(&[0x4a, 1, 0xff], &mut visitor).unwrap();
        assert!(visitor.finish().is_err());

        let mut visitor = FullMetadataVisitor::default();
        assert!(visit_protobuf_transaction_status_meta(&[0x32, 1, 0xff], &mut visitor).is_err());
    }
}

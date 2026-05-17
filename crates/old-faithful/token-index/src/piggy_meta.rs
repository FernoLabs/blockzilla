use of_car_reader::{
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::{
        InnerInstructionVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
        visit_protobuf_transaction_status_meta,
    },
};
use std::borrow::Cow;

pub(crate) fn decode_piggy_meta_from_protobuf(metadata_bytes: &[u8]) -> Result<PiggyMeta<'_>, ()> {
    let mut meta = PiggyMeta::default();
    visit_protobuf_transaction_status_meta(metadata_bytes, &mut meta).map_err(|_| ())?;
    Ok(meta)
}

pub(crate) fn piggy_meta_from_owned(meta: &TransactionStatusMeta) -> PiggyMeta<'static> {
    let mut out = PiggyMeta {
        tx_error: meta.err.is_some(),
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
        ..Default::default()
    };

    out.pre_token_balances.extend(
        meta.pre_token_balances
            .iter()
            .map(PiggyTokenBalance::from_owned),
    );
    out.post_token_balances.extend(
        meta.post_token_balances
            .iter()
            .map(PiggyTokenBalance::from_owned),
    );
    out.loaded_writable_addresses.extend(
        meta.loaded_writable_addresses
            .iter()
            .map(|address| Cow::Owned(address.clone())),
    );
    out.loaded_readonly_addresses.extend(
        meta.loaded_readonly_addresses
            .iter()
            .map(|address| Cow::Owned(address.clone())),
    );

    for group in &meta.inner_instructions {
        for (inner_instruction_index, instruction) in group.instructions.iter().enumerate() {
            out.inner_instructions.push(PiggyInnerInstruction {
                outer_instruction_index: group.index,
                inner_instruction_index: inner_instruction_index as u32,
                stack_height: instruction.stack_height,
                program_id_index: instruction.program_id_index,
                accounts: Cow::Owned(instruction.accounts.clone()),
                data: Cow::Owned(instruction.data.clone()),
            });
        }
    }

    out
}

#[derive(Debug, Default)]
pub(crate) struct PiggyMeta<'a> {
    pub(crate) tx_error: bool,
    pub(crate) compute_units_consumed: Option<u64>,
    pub(crate) cost_units: Option<u64>,
    pub(crate) pre_token_balances: Vec<PiggyTokenBalance<'a>>,
    pub(crate) post_token_balances: Vec<PiggyTokenBalance<'a>>,
    pub(crate) inner_instructions: Vec<PiggyInnerInstruction<'a>>,
    pub(crate) loaded_writable_addresses: Vec<Cow<'a, [u8]>>,
    pub(crate) loaded_readonly_addresses: Vec<Cow<'a, [u8]>>,
}

impl<'a> TransactionStatusMetaVisitor<'a> for PiggyMeta<'a> {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn status_error(&mut self, _err: &'a [u8]) {
        self.tx_error = true;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.pre_token_balances
            .push(PiggyTokenBalance::from_visit(balance));
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.post_token_balances
            .push(PiggyTokenBalance::from_visit(balance));
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
        self.inner_instructions
            .push(PiggyInnerInstruction::from_visit(instruction));
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'a [u8]) {
        self.loaded_writable_addresses.push(Cow::Borrowed(address));
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'a [u8]) {
        self.loaded_readonly_addresses.push(Cow::Borrowed(address));
    }

    #[inline]
    fn compute_units_consumed(&mut self, units: u64) {
        self.compute_units_consumed = Some(units);
    }

    #[inline]
    fn cost_units(&mut self, units: u64) {
        self.cost_units = Some(units);
    }
}

#[derive(Debug)]
pub(crate) struct PiggyTokenBalance<'a> {
    pub(crate) account_index: u32,
    pub(crate) mint: Cow<'a, str>,
    pub(crate) owner: Cow<'a, str>,
    pub(crate) program_id: Cow<'a, str>,
    pub(crate) amount: Option<Cow<'a, str>>,
}

impl<'a> PiggyTokenBalance<'a> {
    fn from_visit(balance: TokenBalanceVisit<'a>) -> Self {
        Self {
            account_index: balance.account_index,
            mint: Cow::Borrowed(balance.mint),
            owner: Cow::Borrowed(balance.owner),
            program_id: Cow::Borrowed(balance.program_id),
            amount: balance
                .ui_token_amount
                .map(|amount| Cow::Borrowed(amount.amount)),
        }
    }

    fn from_owned(balance: &of_car_reader::confirmed_block::TokenBalance) -> Self {
        Self {
            account_index: balance.account_index,
            mint: Cow::Owned(balance.mint.clone()),
            owner: Cow::Owned(balance.owner.clone()),
            program_id: Cow::Owned(balance.program_id.clone()),
            amount: balance
                .ui_token_amount
                .as_ref()
                .map(|amount| Cow::Owned(amount.amount.clone())),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PiggyInnerInstruction<'a> {
    pub(crate) outer_instruction_index: u32,
    pub(crate) inner_instruction_index: u32,
    pub(crate) stack_height: Option<u32>,
    pub(crate) program_id_index: u32,
    pub(crate) accounts: Cow<'a, [u8]>,
    pub(crate) data: Cow<'a, [u8]>,
}

impl<'a> PiggyInnerInstruction<'a> {
    fn from_visit(instruction: InnerInstructionVisit<'a>) -> Self {
        Self {
            outer_instruction_index: instruction.outer_instruction_index,
            inner_instruction_index: instruction.inner_instruction_index,
            stack_height: instruction.stack_height,
            program_id_index: instruction.program_id_index,
            accounts: Cow::Borrowed(instruction.accounts),
            data: Cow::Borrowed(instruction.data),
        }
    }
}

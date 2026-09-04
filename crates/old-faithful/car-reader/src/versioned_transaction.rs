use {
    crate::short_vec::ShortU16,
    std::mem::{MaybeUninit, size_of},
    wincode::{
        ReadResult, SchemaRead,
        config::Config,
        containers::{self},
        error::{invalid_tag_encoding, invalid_value, trailing_bytes},
        io::Reader,
    },
};

const MESSAGE_VERSION_PREFIX: u8 = 0x80;

/// SIMD-0385 caps these in the message itself rather than leaving them implied
/// by the transaction size, so they are decode-time invariants.
const V1_MAX_ADDRESSES: u8 = 64;
const V1_MAX_INSTRUCTIONS: u8 = 64;

/// Config mask bits, in the order their values appear. `PRIORITY_FEE` takes two
/// bits because the value array is counted in four-byte slots and a priority fee
/// is a `u64` — so the set-bit count is exactly the number of slots that follow.
const V1_CONFIG_PRIORITY_FEE: u32 = 0b11;
const V1_CONFIG_COMPUTE_UNIT_LIMIT: u32 = 0b100;
const V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE: u32 = 0b1000;
const V1_CONFIG_HEAP_SIZE: u32 = 0b1_0000;
const V1_CONFIG_KNOWN_BITS: u32 = V1_CONFIG_PRIORITY_FEE
    | V1_CONFIG_COMPUTE_UNIT_LIMIT
    | V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE
    | V1_CONFIG_HEAP_SIZE;

/// Maximum capacity kept by one reusable transaction buffer by default.
///
/// Solana transactions are much smaller than this. The margin also covers
/// allocator capacity rounding without retaining a corrupt input's allocation.
pub const DEFAULT_TRANSACTION_REUSE_BUFFER_LIMIT: usize = 64 << 10;

/// Maximum capacity kept by one reusable transaction workspace by default.
pub const DEFAULT_TRANSACTION_REUSE_TOTAL_LIMIT: usize = 512 << 10;

/// Cumulative allocation activity for one reusable transaction workspace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VersionedTransactionReuseStats {
    pub outer_vector_reuses: u64,
    pub outer_vector_fresh: u64,
    pub inner_buffer_reuses: u64,
    pub inner_buffer_fresh: u64,
    pub growth_events: u64,
    pub discarded_allocations: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SchemaRead)]
#[wincode(assert_zero_copy)]
#[repr(C)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    #[wincode(with = "containers::Vec<u8, ShortU16>")]
    pub accounts: Vec<u8>,
    #[wincode(with = "containers::Vec<u8, ShortU16>")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct LegacyMessage<'a> {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<&'a [u8; 32], ShortU16>")]
    pub account_keys: Vec<&'a [u8; 32]>,
    pub recent_blockhash: &'a [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16>")]
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct MessageAddressTableLookup<'a> {
    pub account_key: &'a [u8; 32],
    #[wincode(with = "containers::Vec<u8, ShortU16>")]
    pub writable_indexes: Vec<u8>,
    #[wincode(with = "containers::Vec<u8, ShortU16>")]
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct V0Message<'a> {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<&'a [u8; 32], ShortU16>")]
    pub account_keys: Vec<&'a [u8; 32]>,
    pub recent_blockhash: &'a [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16>")]
    pub instructions: Vec<CompiledInstruction>,
    #[wincode(with = "containers::Vec<MessageAddressTableLookup<'a>, ShortU16>")]
    pub address_table_lookups: Vec<MessageAddressTableLookup<'a>>,
}

/// The compute budget a v1 message carries in its header, replacing the
/// ComputeBudget instructions a legacy or v0 transaction had to include.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct V1TransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

/// A v1 message (SIMD-0385). There are no address lookup tables: at 4 KiB the
/// whole address list is carried inline, so every account key is present here
/// and none has to be resolved from a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct V1Message<'a> {
    pub header: MessageHeader,
    pub config: V1TransactionConfig,
    pub account_keys: Vec<&'a [u8; 32]>,
    pub recent_blockhash: &'a [u8; 32],
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VersionedMessage<'a> {
    Legacy(LegacyMessage<'a>),
    V0(V0Message<'a>),
    V1(V1Message<'a>),
}

/// Read a v1 message body, the version byte already consumed.
///
/// The layout differs from v0 in ways that matter here: the counts are plain
/// `u8` rather than ShortU16, the addresses are one contiguous run, and the
/// instruction headers are separated from their payloads.
fn read_v1_message<'de, C: Config>(mut reader: impl Reader<'de>) -> ReadResult<V1Message<'de>> {
    let header = <MessageHeader as SchemaRead<'de, C>>::get(reader.by_ref())?;
    let config_mask = u32::from_le_bytes(reader.take_array()?);
    let recent_blockhash = <&'de [u8; 32] as SchemaRead<'de, C>>::get(reader.by_ref())?;
    let num_instructions = reader.take_byte()?;
    let num_addresses = reader.take_byte()?;

    if num_instructions > V1_MAX_INSTRUCTIONS {
        return Err(invalid_value("v1 message exceeds 64 instructions"));
    }
    if num_addresses > V1_MAX_ADDRESSES {
        return Err(invalid_value("v1 message exceeds 64 addresses"));
    }
    if config_mask & !V1_CONFIG_KNOWN_BITS != 0 {
        return Err(invalid_value("v1 config mask sets unknown bits"));
    }
    // Both priority-fee bits travel together. One alone is not a shorter
    // encoding of anything, so it can only be corruption.
    let priority_fee_bits = config_mask & V1_CONFIG_PRIORITY_FEE;
    if priority_fee_bits != 0 && priority_fee_bits != V1_CONFIG_PRIORITY_FEE {
        return Err(invalid_value(
            "v1 config mask has partial priority fee bits",
        ));
    }

    let mut account_keys = Vec::with_capacity(usize::from(num_addresses));
    for _ in 0..num_addresses {
        account_keys.push(<&'de [u8; 32] as SchemaRead<'de, C>>::get(reader.by_ref())?);
    }

    // Values appear in bit order, so the mask alone decides both which fields
    // are present and how many slots to consume.
    let mut config = V1TransactionConfig::default();
    if priority_fee_bits == V1_CONFIG_PRIORITY_FEE {
        config.priority_fee = Some(u64::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_COMPUTE_UNIT_LIMIT != 0 {
        config.compute_unit_limit = Some(u32::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE != 0 {
        config.loaded_accounts_data_size_limit = Some(u32::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_HEAP_SIZE != 0 {
        config.heap_size = Some(u32::from_le_bytes(reader.take_array()?));
    }

    // Every header first, then every payload. A header is four bytes: program
    // index, account count, and data length.
    let mut headers = Vec::with_capacity(usize::from(num_instructions));
    for _ in 0..num_instructions {
        let program_id_index = reader.take_byte()?;
        let num_accounts = reader.take_byte()?;
        let data_len = u16::from_le_bytes(reader.take_array()?);
        headers.push((program_id_index, num_accounts, data_len));
    }

    let mut instructions = Vec::with_capacity(headers.len());
    for (program_id_index, num_accounts, data_len) in headers {
        let accounts = reader.take_borrowed(usize::from(num_accounts))?.to_vec();
        let data = reader.take_borrowed(usize::from(data_len))?.to_vec();
        instructions.push(CompiledInstruction {
            program_id_index,
            accounts,
            data,
        });
    }

    Ok(V1Message {
        header,
        config,
        account_keys,
        recent_blockhash,
        instructions,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct VersionedTransaction<'a> {
    #[wincode(with = "containers::Vec<&'a [u8; 64], ShortU16>")]
    pub signatures: Vec<&'a [u8; 64]>,
    pub message: VersionedMessage<'a>,
}

/// Reusable allocations for transaction and message decoding.
///
/// The decoded value continues to borrow signatures, account keys, and the
/// recent blockhash directly from the input. Only the vectors and instruction
/// byte payloads use this workspace. Call [`Self::recycle_transaction`] or
/// [`Self::recycle_message`] after the decoded value is no longer needed.
///
/// One workspace is most effective when it has at most one decoded value in
/// flight. It remains correct if a caller decodes another value first, but the
/// new decode cannot use allocations that are still held by the first value.
#[derive(Debug)]
pub struct VersionedTransactionReuse {
    signatures: Vec<&'static [u8; 64]>,
    account_keys: Vec<&'static [u8; 32]>,
    instructions: Vec<CompiledInstruction>,
    instruction_accounts: Vec<Vec<u8>>,
    instruction_data: Vec<Vec<u8>>,
    address_table_lookups: Vec<MessageAddressTableLookup<'static>>,
    lookup_writable_indexes: Vec<Vec<u8>>,
    lookup_readonly_indexes: Vec<Vec<u8>>,
    v1_instruction_headers: Vec<(u8, u8, u16)>,
    max_retained_buffer_bytes: usize,
    max_retained_total_bytes: usize,
    stats: VersionedTransactionReuseStats,
}

impl Default for VersionedTransactionReuse {
    fn default() -> Self {
        Self::with_retention_limits(
            DEFAULT_TRANSACTION_REUSE_BUFFER_LIMIT,
            DEFAULT_TRANSACTION_REUSE_TOTAL_LIMIT,
        )
    }
}

impl VersionedTransactionReuse {
    /// Create a workspace with explicit per-buffer and aggregate limits.
    pub const fn with_retention_limits(
        max_retained_buffer_bytes: usize,
        max_retained_total_bytes: usize,
    ) -> Self {
        Self {
            signatures: Vec::new(),
            account_keys: Vec::new(),
            instructions: Vec::new(),
            instruction_accounts: Vec::new(),
            instruction_data: Vec::new(),
            address_table_lookups: Vec::new(),
            lookup_writable_indexes: Vec::new(),
            lookup_readonly_indexes: Vec::new(),
            v1_instruction_headers: Vec::new(),
            max_retained_buffer_bytes,
            max_retained_total_bytes,
            stats: VersionedTransactionReuseStats {
                outer_vector_reuses: 0,
                outer_vector_fresh: 0,
                inner_buffer_reuses: 0,
                inner_buffer_fresh: 0,
                growth_events: 0,
                discarded_allocations: 0,
            },
        }
    }

    /// Decode one complete transaction and use retained allocations.
    #[inline]
    pub fn deserialize_transaction<'de>(
        &mut self,
        src: &'de [u8],
    ) -> ReadResult<VersionedTransaction<'de>> {
        deserialize_versioned_transaction_reusing(src, self)
    }

    /// Decode one complete versioned message and use retained allocations.
    #[inline]
    pub fn deserialize_message<'de>(
        &mut self,
        src: &'de [u8],
    ) -> ReadResult<VersionedMessage<'de>> {
        deserialize_versioned_message_reusing(src, self)
    }

    /// Return all reusable allocations in a decoded transaction.
    pub fn recycle_transaction(&mut self, transaction: VersionedTransaction<'_>) {
        let VersionedTransaction {
            signatures,
            message,
        } = transaction;
        self.retain_signatures(signatures);
        self.recycle_message(message);
    }

    /// Return all reusable allocations in a decoded message.
    pub fn recycle_message(&mut self, message: VersionedMessage<'_>) {
        let mut remaining_budget = self
            .max_retained_total_bytes
            .saturating_sub(self.retained_capacity_bytes());
        match message {
            VersionedMessage::Legacy(message) => {
                self.retain_account_keys(message.account_keys);
                self.retain_instructions(message.instructions, &mut remaining_budget);
            }
            VersionedMessage::V0(message) => {
                self.retain_account_keys(message.account_keys);
                self.retain_instructions(message.instructions, &mut remaining_budget);
                self.retain_lookups(message.address_table_lookups, &mut remaining_budget);
            }
            VersionedMessage::V1(message) => {
                self.retain_account_keys(message.account_keys);
                self.retain_instructions(message.instructions, &mut remaining_budget);
            }
        }
        self.trim_to_total_limit();
    }

    /// Return instruction byte buffers that were moved into a downstream
    /// representation. This lets a converter keep the decode zero-copy move
    /// and return the buffers only after it writes the converted transaction.
    pub fn recycle_instruction_buffers(&mut self, accounts: Vec<u8>, data: Vec<u8>) {
        self.recycle_instruction_buffers_batch([(accounts, data)]);
    }

    /// Return many moved instruction buffers, then enforce the aggregate limit
    /// once. This is the preferred path after a complete transaction is written.
    pub fn recycle_instruction_buffers_batch(
        &mut self,
        buffers: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) {
        let mut remaining_budget = self
            .max_retained_total_bytes
            .saturating_sub(self.retained_capacity_bytes());
        for (accounts, data) in buffers {
            self.retain_byte_buffer_with_budget(
                accounts,
                BytePool::InstructionAccounts,
                &mut remaining_budget,
            );
            self.retain_byte_buffer_with_budget(
                data,
                BytePool::InstructionData,
                &mut remaining_budget,
            );
        }
        self.trim_to_total_limit();
    }

    /// Return v0 lookup index buffers that were moved into a downstream
    /// representation.
    pub fn recycle_lookup_index_buffers(
        &mut self,
        writable_indexes: Vec<u8>,
        readonly_indexes: Vec<u8>,
    ) {
        self.recycle_lookup_index_buffers_batch([(writable_indexes, readonly_indexes)]);
    }

    /// Return many moved v0 lookup buffers, then enforce the aggregate limit
    /// once. This is the preferred path after a complete transaction is written.
    pub fn recycle_lookup_index_buffers_batch(
        &mut self,
        buffers: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    ) {
        let mut remaining_budget = self
            .max_retained_total_bytes
            .saturating_sub(self.retained_capacity_bytes());
        for (writable_indexes, readonly_indexes) in buffers {
            self.retain_byte_buffer_with_budget(
                writable_indexes,
                BytePool::LookupWritable,
                &mut remaining_budget,
            );
            self.retain_byte_buffer_with_budget(
                readonly_indexes,
                BytePool::LookupReadonly,
                &mut remaining_budget,
            );
        }
        self.trim_to_total_limit();
    }

    /// Total capacity, in bytes, that the workspace currently retains.
    pub fn retained_capacity_bytes(&self) -> usize {
        capacity_bytes(&self.signatures)
            .saturating_add(capacity_bytes(&self.account_keys))
            .saturating_add(capacity_bytes(&self.instructions))
            .saturating_add(capacity_bytes(&self.instruction_accounts))
            .saturating_add(capacity_bytes(&self.instruction_data))
            .saturating_add(capacity_bytes(&self.address_table_lookups))
            .saturating_add(capacity_bytes(&self.lookup_writable_indexes))
            .saturating_add(capacity_bytes(&self.lookup_readonly_indexes))
            .saturating_add(capacity_bytes(&self.v1_instruction_headers))
            .saturating_add(
                self.instruction_accounts
                    .iter()
                    .map(Vec::capacity)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.instruction_data
                    .iter()
                    .map(Vec::capacity)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.lookup_writable_indexes
                    .iter()
                    .map(Vec::capacity)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.lookup_readonly_indexes
                    .iter()
                    .map(Vec::capacity)
                    .sum::<usize>(),
            )
    }

    /// Cumulative allocation activity for this workspace.
    pub const fn stats(&self) -> VersionedTransactionReuseStats {
        self.stats
    }

    fn take_signatures<'de>(&mut self) -> Vec<&'de [u8; 64]> {
        shorten_ref_vec(std::mem::take(&mut self.signatures))
    }

    fn take_account_keys<'de>(&mut self) -> Vec<&'de [u8; 32]> {
        shorten_ref_vec(std::mem::take(&mut self.account_keys))
    }

    fn take_instructions(&mut self) -> Vec<CompiledInstruction> {
        std::mem::take(&mut self.instructions)
    }

    fn take_lookups<'de>(&mut self) -> Vec<MessageAddressTableLookup<'de>> {
        shorten_lookup_vec(std::mem::take(&mut self.address_table_lookups))
    }

    fn take_byte_buffer(
        pool: &mut Vec<Vec<u8>>,
        len: usize,
        stats: &mut VersionedTransactionReuseStats,
    ) -> Vec<u8> {
        if len == 0 {
            return Vec::new();
        }
        let best = pool
            .iter()
            .enumerate()
            .filter(|(_, value)| value.capacity() >= len)
            .min_by_key(|(index, value)| (value.capacity(), usize::MAX - index))
            .map(|(index, _)| index)
            .or_else(|| {
                pool.iter()
                    .enumerate()
                    .max_by_key(|(_, value)| value.capacity())
                    .map(|(index, _)| index)
            });
        let mut value = if let Some(index) = best {
            if len != 0 {
                stats.inner_buffer_reuses = stats.inner_buffer_reuses.saturating_add(1);
            }
            pool.swap_remove(index)
        } else {
            if len != 0 {
                stats.inner_buffer_fresh = stats.inner_buffer_fresh.saturating_add(1);
            }
            Vec::new()
        };
        if value.capacity() < len {
            stats.growth_events = stats.growth_events.saturating_add(1);
        }
        value.clear();
        value
    }

    fn record_outer_use(&mut self, capacity: usize, desired_len: usize) {
        if desired_len == 0 {
            return;
        }
        if capacity == 0 {
            self.stats.outer_vector_fresh = self.stats.outer_vector_fresh.saturating_add(1);
        } else {
            self.stats.outer_vector_reuses = self.stats.outer_vector_reuses.saturating_add(1);
        }
        if capacity < desired_len {
            self.stats.growth_events = self.stats.growth_events.saturating_add(1);
        }
    }

    fn retain_signatures(&mut self, mut value: Vec<&[u8; 64]>) {
        value.clear();
        let value = erase_empty_ref_vec(value);
        if !retain_outer_vector(&mut self.signatures, value, self.max_retained_buffer_bytes) {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }

    fn retain_account_keys(&mut self, mut value: Vec<&[u8; 32]>) {
        value.clear();
        let value = erase_empty_ref_vec(value);
        if !retain_outer_vector(
            &mut self.account_keys,
            value,
            self.max_retained_buffer_bytes,
        ) {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }

    fn retain_instructions(
        &mut self,
        mut value: Vec<CompiledInstruction>,
        remaining_budget: &mut usize,
    ) {
        for instruction in value.drain(..).rev() {
            self.retain_byte_buffer_with_budget(
                instruction.accounts,
                BytePool::InstructionAccounts,
                remaining_budget,
            );
            self.retain_byte_buffer_with_budget(
                instruction.data,
                BytePool::InstructionData,
                remaining_budget,
            );
        }
        if !retain_outer_vector(
            &mut self.instructions,
            value,
            self.max_retained_buffer_bytes,
        ) {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }

    fn retain_lookups(
        &mut self,
        mut value: Vec<MessageAddressTableLookup<'_>>,
        remaining_budget: &mut usize,
    ) {
        for lookup in value.drain(..).rev() {
            self.retain_byte_buffer_with_budget(
                lookup.writable_indexes,
                BytePool::LookupWritable,
                remaining_budget,
            );
            self.retain_byte_buffer_with_budget(
                lookup.readonly_indexes,
                BytePool::LookupReadonly,
                remaining_budget,
            );
        }
        let value = erase_empty_lookup_vec(value);
        if !retain_outer_vector(
            &mut self.address_table_lookups,
            value,
            self.max_retained_buffer_bytes,
        ) {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }

    fn retain_byte_buffer_with_budget(
        &mut self,
        mut value: Vec<u8>,
        pool: BytePool,
        remaining_budget: &mut usize,
    ) {
        value.clear();
        if value.capacity() == 0 || value.capacity() > self.max_retained_buffer_bytes {
            if value.capacity() > self.max_retained_buffer_bytes {
                self.stats.discarded_allocations =
                    self.stats.discarded_allocations.saturating_add(1);
            }
            return;
        }
        let max_headers = self
            .max_retained_total_bytes
            .checked_div(4 * size_of::<Vec<u8>>())
            .unwrap_or(0);
        let required_budget = value.capacity().saturating_add(size_of::<Vec<u8>>());
        let target = match pool {
            BytePool::InstructionAccounts => &mut self.instruction_accounts,
            BytePool::InstructionData => &mut self.instruction_data,
            BytePool::LookupWritable => &mut self.lookup_writable_indexes,
            BytePool::LookupReadonly => &mut self.lookup_readonly_indexes,
        };
        if target.len() >= max_headers || required_budget > *remaining_budget {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
            return;
        }
        *remaining_budget = remaining_budget.saturating_sub(required_budget);
        target.push(value);
    }

    fn retain_v1_headers(&mut self, mut value: Vec<(u8, u8, u16)>) {
        value.clear();
        if !retain_outer_vector(
            &mut self.v1_instruction_headers,
            value,
            self.max_retained_buffer_bytes,
        ) {
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }

    fn trim_to_total_limit(&mut self) {
        if capacity_bytes(&self.instruction_accounts) > self.max_retained_buffer_bytes {
            self.instruction_accounts = Vec::new();
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
        if capacity_bytes(&self.instruction_data) > self.max_retained_buffer_bytes {
            self.instruction_data = Vec::new();
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
        if capacity_bytes(&self.lookup_writable_indexes) > self.max_retained_buffer_bytes {
            self.lookup_writable_indexes = Vec::new();
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
        if capacity_bytes(&self.lookup_readonly_indexes) > self.max_retained_buffer_bytes {
            self.lookup_readonly_indexes = Vec::new();
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
        let mut retained_capacity = self.retained_capacity_bytes();
        while retained_capacity > self.max_retained_total_bytes {
            let discarded_inner = self
                .instruction_data
                .pop()
                .or_else(|| self.instruction_accounts.pop())
                .or_else(|| self.lookup_readonly_indexes.pop())
                .or_else(|| self.lookup_writable_indexes.pop());
            if let Some(discarded) = discarded_inner {
                retained_capacity = retained_capacity.saturating_sub(discarded.capacity());
                self.stats.discarded_allocations =
                    self.stats.discarded_allocations.saturating_add(1);
                continue;
            }

            let discarded_capacity = if self.v1_instruction_headers.capacity() != 0 {
                let capacity = capacity_bytes(&self.v1_instruction_headers);
                self.v1_instruction_headers = Vec::new();
                capacity
            } else if self.address_table_lookups.capacity() != 0 {
                let capacity = capacity_bytes(&self.address_table_lookups);
                self.address_table_lookups = Vec::new();
                capacity
            } else if self.instructions.capacity() != 0 {
                let capacity = capacity_bytes(&self.instructions);
                self.instructions = Vec::new();
                capacity
            } else if self.account_keys.capacity() != 0 {
                let capacity = capacity_bytes(&self.account_keys);
                self.account_keys = Vec::new();
                capacity
            } else if self.signatures.capacity() != 0 {
                let capacity = capacity_bytes(&self.signatures);
                self.signatures = Vec::new();
                capacity
            } else if self.instruction_data.capacity() != 0 {
                let capacity = capacity_bytes(&self.instruction_data);
                self.instruction_data = Vec::new();
                capacity
            } else if self.instruction_accounts.capacity() != 0 {
                let capacity = capacity_bytes(&self.instruction_accounts);
                self.instruction_accounts = Vec::new();
                capacity
            } else if self.lookup_readonly_indexes.capacity() != 0 {
                let capacity = capacity_bytes(&self.lookup_readonly_indexes);
                self.lookup_readonly_indexes = Vec::new();
                capacity
            } else if self.lookup_writable_indexes.capacity() != 0 {
                let capacity = capacity_bytes(&self.lookup_writable_indexes);
                self.lookup_writable_indexes = Vec::new();
                capacity
            } else {
                break;
            };
            retained_capacity = retained_capacity.saturating_sub(discarded_capacity);
            self.stats.discarded_allocations = self.stats.discarded_allocations.saturating_add(1);
        }
    }
}

#[derive(Clone, Copy)]
enum BytePool {
    InstructionAccounts,
    InstructionData,
    LookupWritable,
    LookupReadonly,
}

#[inline]
fn capacity_bytes<T>(value: &Vec<T>) -> usize {
    value.capacity().saturating_mul(size_of::<T>())
}

fn retain_outer_vector<T>(dst: &mut Vec<T>, value: Vec<T>, max_retained_bytes: usize) -> bool {
    let value_capacity = capacity_bytes(&value);
    if value_capacity == 0 {
        return true;
    }
    if value_capacity <= max_retained_bytes && value.capacity() > dst.capacity() {
        *dst = value;
        true
    } else {
        false
    }
}

#[inline]
fn shorten_ref_vec<'de, T: ?Sized>(value: Vec<&'static T>) -> Vec<&'de T> {
    value
}

#[inline]
fn shorten_lookup_vec<'de>(
    value: Vec<MessageAddressTableLookup<'static>>,
) -> Vec<MessageAddressTableLookup<'de>> {
    value
}

fn erase_empty_ref_vec<T: ?Sized>(value: Vec<&T>) -> Vec<&'static T> {
    assert!(
        value.is_empty(),
        "reference vector must be empty before lifetime erasure"
    );
    // SAFETY: The vector is empty, so it contains no reference whose lifetime
    // could be extended. This changes only the type of its reusable allocation.
    unsafe { std::mem::transmute(value) }
}

fn erase_empty_lookup_vec(
    value: Vec<MessageAddressTableLookup<'_>>,
) -> Vec<MessageAddressTableLookup<'static>> {
    assert!(
        value.is_empty(),
        "lookup vector must be empty before lifetime erasure"
    );
    // SAFETY: The vector was drained. It contains no lookup and therefore no
    // borrowed account key. Only its allocation is kept for another decode.
    unsafe { std::mem::transmute(value) }
}

/// Decode one complete versioned transaction with reusable owned buffers.
pub fn deserialize_versioned_transaction_reusing<'de>(
    src: &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<VersionedTransaction<'de>> {
    let result = (|| {
        let mut reader = src;
        let signature_count = read_short_u16_len(&mut reader)?;
        let mut signatures = reuse.take_signatures();
        signatures.clear();
        ensure_fixed_items_fit(
            reader,
            signature_count,
            64,
            "truncated transaction signatures",
        )?;
        reuse.record_outer_use(signatures.capacity(), signature_count);
        signatures.reserve(signature_count);
        for _ in 0..signature_count {
            signatures.push(take_array_ref::<64>(&mut reader)?);
        }

        let message = read_versioned_message_reusing(&mut reader, reuse)?;
        if !reader.is_empty() {
            return Err(trailing_bytes());
        }
        Ok(VersionedTransaction {
            signatures,
            message,
        })
    })();
    reuse.trim_to_total_limit();
    result
}

/// Decode one complete versioned message with reusable owned buffers.
pub fn deserialize_versioned_message_reusing<'de>(
    src: &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<VersionedMessage<'de>> {
    let result = (|| {
        let mut reader = src;
        let message = read_versioned_message_reusing(&mut reader, reuse)?;
        if !reader.is_empty() {
            return Err(trailing_bytes());
        }
        Ok(message)
    })();
    reuse.trim_to_total_limit();
    result
}

fn read_versioned_message_reusing<'de>(
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<VersionedMessage<'de>> {
    let first = reader.take_byte()?;
    if first & MESSAGE_VERSION_PREFIX == 0 {
        return read_legacy_message_reusing(first, reader, reuse).map(VersionedMessage::Legacy);
    }

    match first & !MESSAGE_VERSION_PREFIX {
        0 => read_v0_message_reusing(reader, reuse).map(VersionedMessage::V0),
        1 => read_v1_message_reusing(reader, reuse).map(VersionedMessage::V1),
        version => Err(invalid_tag_encoding(version as usize)),
    }
}

fn read_legacy_message_reusing<'de>(
    num_required_signatures: u8,
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<LegacyMessage<'de>> {
    let header = read_message_header_tail(num_required_signatures, reader)?;
    let account_keys = read_account_keys_reusing(reader, reuse)?;
    let recent_blockhash = take_array_ref(reader)?;
    let instructions = read_compiled_instructions_reusing(reader, reuse)?;
    Ok(LegacyMessage {
        header,
        account_keys,
        recent_blockhash,
        instructions,
    })
}

fn read_v0_message_reusing<'de>(
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<V0Message<'de>> {
    let num_required_signatures = reader.take_byte()?;
    let header = read_message_header_tail(num_required_signatures, reader)?;
    let account_keys = read_account_keys_reusing(reader, reuse)?;
    let recent_blockhash = take_array_ref(reader)?;
    let instructions = read_compiled_instructions_reusing(reader, reuse)?;
    let address_table_lookups = read_address_table_lookups_reusing(reader, reuse)?;
    Ok(V0Message {
        header,
        account_keys,
        recent_blockhash,
        instructions,
        address_table_lookups,
    })
}

fn read_v1_message_reusing<'de>(
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<V1Message<'de>> {
    let num_required_signatures = reader.take_byte()?;
    let header = read_message_header_tail(num_required_signatures, reader)?;
    let config_mask = u32::from_le_bytes(reader.take_array()?);
    let recent_blockhash = take_array_ref(reader)?;
    let num_instructions = reader.take_byte()?;
    let num_addresses = reader.take_byte()?;

    validate_v1_counts_and_mask(num_instructions, num_addresses, config_mask)?;

    let mut account_keys = reuse.take_account_keys();
    account_keys.clear();
    let address_count = usize::from(num_addresses);
    ensure_fixed_items_fit(reader, address_count, 32, "truncated v1 account keys")?;
    reuse.record_outer_use(account_keys.capacity(), address_count);
    account_keys.reserve(address_count);
    for _ in 0..address_count {
        account_keys.push(take_array_ref(reader)?);
    }

    let config = read_v1_config(config_mask, reader)?;

    let instruction_count = usize::from(num_instructions);
    ensure_fixed_items_fit(
        reader,
        instruction_count,
        4,
        "truncated v1 instruction headers",
    )?;
    let mut headers = std::mem::take(&mut reuse.v1_instruction_headers);
    headers.clear();
    reuse.record_outer_use(headers.capacity(), instruction_count);
    headers.reserve(instruction_count);
    for _ in 0..instruction_count {
        headers.push((
            reader.take_byte()?,
            reader.take_byte()?,
            u16::from_le_bytes(reader.take_array()?),
        ));
    }

    let mut instructions = reuse.take_instructions();
    instructions.clear();
    reuse.record_outer_use(instructions.capacity(), instruction_count);
    instructions.reserve(instruction_count);
    for (program_id_index, num_accounts, data_len) in headers.drain(..) {
        let accounts = copy_bytes_reusing(
            reader,
            usize::from(num_accounts),
            &mut reuse.instruction_accounts,
            &mut reuse.stats,
        )?;
        let data = copy_bytes_reusing(
            reader,
            usize::from(data_len),
            &mut reuse.instruction_data,
            &mut reuse.stats,
        )?;
        instructions.push(CompiledInstruction {
            program_id_index,
            accounts,
            data,
        });
    }
    reuse.retain_v1_headers(headers);
    reuse.trim_to_total_limit();

    Ok(V1Message {
        header,
        config,
        account_keys,
        recent_blockhash,
        instructions,
    })
}

fn read_message_header_tail(
    num_required_signatures: u8,
    reader: &mut &[u8],
) -> ReadResult<MessageHeader> {
    Ok(MessageHeader {
        num_required_signatures,
        num_readonly_signed_accounts: reader.take_byte()?,
        num_readonly_unsigned_accounts: reader.take_byte()?,
    })
}

fn read_account_keys_reusing<'de>(
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<Vec<&'de [u8; 32]>> {
    let count = read_short_u16_len(reader)?;
    ensure_fixed_items_fit(reader, count, 32, "truncated message account keys")?;
    let mut account_keys = reuse.take_account_keys();
    account_keys.clear();
    reuse.record_outer_use(account_keys.capacity(), count);
    account_keys.reserve(count);
    for _ in 0..count {
        account_keys.push(take_array_ref(reader)?);
    }
    Ok(account_keys)
}

fn read_compiled_instructions_reusing(
    reader: &mut &[u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<Vec<CompiledInstruction>> {
    let count = read_short_u16_len(reader)?;
    ensure_fixed_items_fit(reader, count, 3, "truncated compiled instruction list")?;
    let mut instructions = reuse.take_instructions();
    instructions.clear();
    reuse.record_outer_use(instructions.capacity(), count);
    instructions.reserve(count);
    for _ in 0..count {
        let program_id_index = reader.take_byte()?;
        let accounts_len = read_short_u16_len(reader)?;
        let accounts = copy_bytes_reusing(
            reader,
            accounts_len,
            &mut reuse.instruction_accounts,
            &mut reuse.stats,
        )?;
        let data_len = read_short_u16_len(reader)?;
        let data = copy_bytes_reusing(
            reader,
            data_len,
            &mut reuse.instruction_data,
            &mut reuse.stats,
        )?;
        instructions.push(CompiledInstruction {
            program_id_index,
            accounts,
            data,
        });
    }
    Ok(instructions)
}

fn read_address_table_lookups_reusing<'de>(
    reader: &mut &'de [u8],
    reuse: &mut VersionedTransactionReuse,
) -> ReadResult<Vec<MessageAddressTableLookup<'de>>> {
    let count = read_short_u16_len(reader)?;
    ensure_fixed_items_fit(reader, count, 34, "truncated address table lookups")?;
    let mut lookups = reuse.take_lookups();
    lookups.clear();
    reuse.record_outer_use(lookups.capacity(), count);
    lookups.reserve(count);
    for _ in 0..count {
        let account_key = take_array_ref(reader)?;
        let writable_len = read_short_u16_len(reader)?;
        let writable_indexes = copy_bytes_reusing(
            reader,
            writable_len,
            &mut reuse.lookup_writable_indexes,
            &mut reuse.stats,
        )?;
        let readonly_len = read_short_u16_len(reader)?;
        let readonly_indexes = copy_bytes_reusing(
            reader,
            readonly_len,
            &mut reuse.lookup_readonly_indexes,
            &mut reuse.stats,
        )?;
        lookups.push(MessageAddressTableLookup {
            account_key,
            writable_indexes,
            readonly_indexes,
        });
    }
    Ok(lookups)
}

fn read_v1_config(config_mask: u32, reader: &mut &[u8]) -> ReadResult<V1TransactionConfig> {
    let mut config = V1TransactionConfig::default();
    if config_mask & V1_CONFIG_PRIORITY_FEE == V1_CONFIG_PRIORITY_FEE {
        config.priority_fee = Some(u64::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_COMPUTE_UNIT_LIMIT != 0 {
        config.compute_unit_limit = Some(u32::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_LOADED_ACCOUNTS_DATA_SIZE != 0 {
        config.loaded_accounts_data_size_limit = Some(u32::from_le_bytes(reader.take_array()?));
    }
    if config_mask & V1_CONFIG_HEAP_SIZE != 0 {
        config.heap_size = Some(u32::from_le_bytes(reader.take_array()?));
    }
    Ok(config)
}

fn validate_v1_counts_and_mask(
    num_instructions: u8,
    num_addresses: u8,
    config_mask: u32,
) -> ReadResult<()> {
    if num_instructions > V1_MAX_INSTRUCTIONS {
        return Err(invalid_value("v1 message exceeds 64 instructions"));
    }
    if num_addresses > V1_MAX_ADDRESSES {
        return Err(invalid_value("v1 message exceeds 64 addresses"));
    }
    if config_mask & !V1_CONFIG_KNOWN_BITS != 0 {
        return Err(invalid_value("v1 config mask sets unknown bits"));
    }
    let priority_fee_bits = config_mask & V1_CONFIG_PRIORITY_FEE;
    if priority_fee_bits != 0 && priority_fee_bits != V1_CONFIG_PRIORITY_FEE {
        return Err(invalid_value(
            "v1 config mask has partial priority fee bits",
        ));
    }
    Ok(())
}

fn copy_bytes_reusing(
    reader: &mut &[u8],
    len: usize,
    pool: &mut Vec<Vec<u8>>,
    stats: &mut VersionedTransactionReuseStats,
) -> ReadResult<Vec<u8>> {
    let bytes = reader.take_borrowed(len)?;
    let mut value = VersionedTransactionReuse::take_byte_buffer(pool, len, stats);
    value.extend_from_slice(bytes);
    Ok(value)
}

fn read_short_u16_len(reader: &mut &[u8]) -> ReadResult<usize> {
    let (len, prefix_len) = crate::short_vec::decode_shortu16_len(reader)
        .map_err(|()| invalid_value("invalid ShortU16 sequence length"))?;
    let _ = reader.take_borrowed(prefix_len)?;
    Ok(len)
}

fn ensure_fixed_items_fit(
    reader: &[u8],
    count: usize,
    item_size: usize,
    error: &'static str,
) -> ReadResult<()> {
    let Some(required) = count.checked_mul(item_size) else {
        return Err(invalid_value(error));
    };
    if required > reader.len() {
        return Err(invalid_value(error));
    }
    Ok(())
}

fn take_array_ref<'de, const N: usize>(reader: &mut &'de [u8]) -> ReadResult<&'de [u8; N]> {
    let value = reader.take_borrowed(N)?;
    Ok(value
        .try_into()
        .expect("a borrowed slice with a fixed requested length has that length"))
}

unsafe impl<'de, C: Config> SchemaRead<'de, C> for VersionedMessage<'de> {
    type Dst = VersionedMessage<'de>;

    #[inline(always)]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let first = <u8 as SchemaRead<'de, C>>::get(reader.by_ref())?;

        if first & MESSAGE_VERSION_PREFIX != 0 {
            let version = first & !MESSAGE_VERSION_PREFIX;
            return match version {
                0 => {
                    let msg = <V0Message<'de> as SchemaRead<'de, C>>::get(reader)?;
                    dst.write(VersionedMessage::V0(msg));
                    Ok(())
                }
                1 => {
                    let msg = read_v1_message::<C>(reader)?;
                    dst.write(VersionedMessage::V1(msg));
                    Ok(())
                }
                _ => Err(invalid_tag_encoding(version as usize)),
            };
        }

        let num_readonly_signed_accounts = <u8 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        let num_readonly_unsigned_accounts = <u8 as SchemaRead<'de, C>>::get(reader.by_ref())?;

        let header = MessageHeader {
            num_required_signatures: first,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
        };

        // Zero-copy pubkeys + blockhash
        let account_keys =
            <containers::Vec<&'de [u8; 32], ShortU16> as SchemaRead<'de, C>>::get(reader.by_ref())?;
        let recent_blockhash = <&'de [u8; 32] as SchemaRead<'de, C>>::get(reader.by_ref())?;
        let instructions =
            <containers::Vec<CompiledInstruction, ShortU16> as SchemaRead<'de, C>>::get(reader)?;

        dst.write(VersionedMessage::Legacy(LegacyMessage {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_v1_transaction() -> Vec<u8> {
        let mut bytes = Vec::new();

        // One signature, encoded with Solana's ShortU16 length prefix.
        bytes.push(1);
        bytes.extend_from_slice(&[7; 64]);

        bytes.push(MESSAGE_VERSION_PREFIX | 1);
        bytes.extend_from_slice(&[1, 0, 1]);
        bytes.extend_from_slice(&V1_CONFIG_KNOWN_BITS.to_le_bytes());
        bytes.extend_from_slice(&[9; 32]);
        bytes.push(1); // instructions
        bytes.push(2); // addresses
        bytes.extend_from_slice(&[1; 32]);
        bytes.extend_from_slice(&[2; 32]);

        bytes.extend_from_slice(&42u64.to_le_bytes());
        bytes.extend_from_slice(&1_400_000u32.to_le_bytes());
        bytes.extend_from_slice(&65_536u32.to_le_bytes());
        bytes.extend_from_slice(&262_144u32.to_le_bytes());

        // One instruction header, followed by its account and data payloads.
        bytes.push(1); // program id index
        bytes.push(1); // account count
        bytes.extend_from_slice(&3u16.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&[0xaa, 0xbb, 0xcc]);

        bytes
    }

    #[test]
    fn decodes_v1_message_and_transaction_config() {
        let bytes = encoded_v1_transaction();
        let transaction =
            wincode::deserialize::<VersionedTransaction<'_>>(&bytes).expect("decode v1 tx");

        assert_eq!(transaction.signatures.len(), 1);
        assert_eq!(transaction.signatures[0].as_slice(), &[7; 64]);

        let VersionedMessage::V1(message) = transaction.message else {
            panic!("expected v1 message");
        };
        assert_eq!(
            message.header,
            MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            }
        );
        assert_eq!(message.account_keys.len(), 2);
        assert_eq!(message.account_keys[0].as_slice(), &[1; 32]);
        assert_eq!(message.account_keys[1].as_slice(), &[2; 32]);
        assert_eq!(message.recent_blockhash.as_slice(), &[9; 32]);
        assert_eq!(message.config.priority_fee, Some(42));
        assert_eq!(message.config.compute_unit_limit, Some(1_400_000));
        assert_eq!(message.config.loaded_accounts_data_size_limit, Some(65_536));
        assert_eq!(message.config.heap_size, Some(262_144));
        assert_eq!(message.instructions.len(), 1);
        assert_eq!(message.instructions[0].program_id_index, 1);
        assert_eq!(message.instructions[0].accounts, [0]);
        assert_eq!(message.instructions[0].data, [0xaa, 0xbb, 0xcc]);
    }

    #[test]
    fn reusable_legacy_decode_matches_schema_decode_and_reuses_buffers() {
        let bytes = legacy_transaction_bytes();
        let expected = wincode::deserialize_exact::<VersionedTransaction<'_>>(&bytes).unwrap();
        let mut reuse = VersionedTransactionReuse::default();

        let first = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(first, expected);
        let first_pointers = transaction_buffer_pointers(&first);
        reuse.recycle_transaction(first);
        assert!(reuse.retained_capacity_bytes() > 0);

        let second = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(second, expected);
        assert_eq!(transaction_buffer_pointers(&second), first_pointers);
        reuse.recycle_transaction(second);
    }

    #[test]
    fn reusable_v0_decode_matches_schema_decode_and_reuses_lookups() {
        let bytes = v0_transaction_bytes();
        let expected = wincode::deserialize_exact::<VersionedTransaction<'_>>(&bytes).unwrap();
        let mut reuse = VersionedTransactionReuse::default();

        let first = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(first, expected);
        let first_pointers = transaction_buffer_pointers(&first);
        reuse.recycle_transaction(first);

        let second = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(second, expected);
        assert_eq!(transaction_buffer_pointers(&second), first_pointers);
        reuse.recycle_transaction(second);
    }

    #[test]
    fn reusable_v1_decode_matches_schema_decode() {
        let bytes = v1_transaction_bytes();
        let expected = wincode::deserialize_exact::<VersionedTransaction<'_>>(&bytes).unwrap();
        let mut reuse = VersionedTransactionReuse::default();

        let first = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(first, expected);
        let first_pointers = transaction_buffer_pointers(&first);
        reuse.recycle_transaction(first);

        let second = reuse.deserialize_transaction(&bytes).unwrap();
        assert_eq!(second, expected);
        assert_eq!(transaction_buffer_pointers(&second), first_pointers);
        reuse.recycle_transaction(second);
    }

    #[test]
    fn reusable_decode_rejects_trailing_bytes() {
        let mut bytes = legacy_transaction_bytes();
        bytes.push(0xff);
        let mut reuse = VersionedTransactionReuse::default();
        assert!(reuse.deserialize_transaction(&bytes).is_err());
    }

    #[test]
    fn zero_retention_limits_release_all_buffers() {
        let bytes = v0_transaction_bytes();
        let mut reuse = VersionedTransactionReuse::with_retention_limits(0, 0);
        let transaction = reuse.deserialize_transaction(&bytes).unwrap();
        reuse.recycle_transaction(transaction);
        assert_eq!(reuse.retained_capacity_bytes(), 0);
    }

    #[test]
    fn v1_header_scratch_obeys_small_total_limit() {
        let bytes = v1_transaction_bytes();
        let mut reuse = VersionedTransactionReuse::with_retention_limits(64 << 10, 4);

        let transaction = reuse.deserialize_transaction(&bytes).unwrap();
        assert!(reuse.retained_capacity_bytes() <= 4);
        reuse.recycle_transaction(transaction);
        assert!(reuse.retained_capacity_bytes() <= 4);

        reuse.recycle_instruction_buffers_batch([
            (vec![1; 16], vec![2; 16]),
            (vec![3; 16], vec![4; 16]),
        ]);
        assert!(reuse.retained_capacity_bytes() <= 4);
    }

    #[test]
    fn malformed_instruction_count_does_not_take_or_grow_outer_buffer() {
        let valid = legacy_transaction_bytes();
        let mut reuse = VersionedTransactionReuse::default();
        let transaction = reuse.deserialize_transaction(&valid).unwrap();
        reuse.recycle_transaction(transaction);
        let retained_instruction_capacity = reuse.instructions.capacity();
        assert!(retained_instruction_capacity > 0);

        let mut malformed = vec![1, 0, 1, 0];
        malformed.extend_from_slice(&[0x55; 32]);
        push_short_len(&mut malformed, 40);
        malformed.extend_from_slice(&[0; 40]);
        assert!(reuse.deserialize_message(&malformed).is_err());
        assert_eq!(reuse.instructions.capacity(), retained_instruction_capacity);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TransactionBufferPointers {
        signatures: usize,
        account_keys: usize,
        instructions: usize,
        instruction_accounts: Vec<usize>,
        instruction_data: Vec<usize>,
        lookups: Option<usize>,
        lookup_writable: Vec<usize>,
        lookup_readonly: Vec<usize>,
    }

    fn transaction_buffer_pointers(
        transaction: &VersionedTransaction<'_>,
    ) -> TransactionBufferPointers {
        let (account_keys, instructions, lookups) = match &transaction.message {
            VersionedMessage::Legacy(message) => {
                (&message.account_keys, &message.instructions, None)
            }
            VersionedMessage::V0(message) => (
                &message.account_keys,
                &message.instructions,
                Some(&message.address_table_lookups),
            ),
            VersionedMessage::V1(message) => (&message.account_keys, &message.instructions, None),
        };
        TransactionBufferPointers {
            signatures: transaction.signatures.as_ptr() as usize,
            account_keys: account_keys.as_ptr() as usize,
            instructions: instructions.as_ptr() as usize,
            instruction_accounts: instructions
                .iter()
                .map(|instruction| instruction.accounts.as_ptr() as usize)
                .collect(),
            instruction_data: instructions
                .iter()
                .map(|instruction| instruction.data.as_ptr() as usize)
                .collect(),
            lookups: lookups.map(|value| value.as_ptr() as usize),
            lookup_writable: lookups
                .into_iter()
                .flatten()
                .map(|lookup| lookup.writable_indexes.as_ptr() as usize)
                .collect(),
            lookup_readonly: lookups
                .into_iter()
                .flatten()
                .map(|lookup| lookup.readonly_indexes.as_ptr() as usize)
                .collect(),
        }
    }

    fn legacy_transaction_bytes() -> Vec<u8> {
        let mut bytes = transaction_prefix(0x11);
        bytes.extend_from_slice(&[1, 0, 1]);
        push_short_len(&mut bytes, 2);
        bytes.extend_from_slice(&[0x21; 32]);
        bytes.extend_from_slice(&[0x22; 32]);
        bytes.extend_from_slice(&[0x31; 32]);
        push_instructions(&mut bytes);
        bytes
    }

    fn v0_transaction_bytes() -> Vec<u8> {
        let mut bytes = transaction_prefix(0x12);
        bytes.push(MESSAGE_VERSION_PREFIX);
        bytes.extend_from_slice(&[1, 0, 1]);
        push_short_len(&mut bytes, 2);
        bytes.extend_from_slice(&[0x41; 32]);
        bytes.extend_from_slice(&[0x42; 32]);
        bytes.extend_from_slice(&[0x43; 32]);
        push_instructions(&mut bytes);
        push_short_len(&mut bytes, 1);
        bytes.extend_from_slice(&[0x44; 32]);
        push_short_len(&mut bytes, 2);
        bytes.extend_from_slice(&[3, 5]);
        push_short_len(&mut bytes, 3);
        bytes.extend_from_slice(&[7, 9, 11]);
        bytes
    }

    fn v1_transaction_bytes() -> Vec<u8> {
        let mut bytes = transaction_prefix(0x13);
        bytes.push(MESSAGE_VERSION_PREFIX | 1);
        bytes.extend_from_slice(&[1, 0, 1]);
        bytes.extend_from_slice(&V1_CONFIG_KNOWN_BITS.to_le_bytes());
        bytes.extend_from_slice(&[0x51; 32]);
        bytes.push(2);
        bytes.push(2);
        bytes.extend_from_slice(&[0x52; 32]);
        bytes.extend_from_slice(&[0x53; 32]);
        bytes.extend_from_slice(&123_u64.to_le_bytes());
        bytes.extend_from_slice(&456_u32.to_le_bytes());
        bytes.extend_from_slice(&789_u32.to_le_bytes());
        bytes.extend_from_slice(&1_024_u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 2]);
        bytes.extend_from_slice(&3_u16.to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&[2, 4]);
        bytes.extend_from_slice(&[6, 8, 10]);
        bytes.extend_from_slice(&[12]);
        bytes
    }

    fn transaction_prefix(signature_byte: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_short_len(&mut bytes, 1);
        bytes.extend_from_slice(&[signature_byte; 64]);
        bytes
    }

    fn push_instructions(bytes: &mut Vec<u8>) {
        push_short_len(bytes, 2);
        bytes.push(1);
        push_short_len(bytes, 2);
        bytes.extend_from_slice(&[2, 3]);
        push_short_len(bytes, 3);
        bytes.extend_from_slice(&[4, 5, 6]);
        bytes.push(7);
        push_short_len(bytes, 1);
        bytes.push(8);
        push_short_len(bytes, 2);
        bytes.extend_from_slice(&[9, 10]);
    }

    fn push_short_len(bytes: &mut Vec<u8>, len: u8) {
        assert!(len < 0x80);
        bytes.push(len);
    }
}

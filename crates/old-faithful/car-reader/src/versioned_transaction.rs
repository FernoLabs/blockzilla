use {
    crate::short_vec::ShortU16,
    std::mem::MaybeUninit,
    wincode::{
        ReadResult, SchemaRead,
        config::Config,
        containers::{self},
        error::{invalid_tag_encoding, invalid_value},
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
}

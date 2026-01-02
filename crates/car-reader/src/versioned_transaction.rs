use {
    std::mem::MaybeUninit,
    wincode::{
        ReadResult, SchemaRead,
        containers::{self},
        error::invalid_tag_encoding,
        io::Reader,
        len::ShortU16Len,
    },
};

const MESSAGE_VERSION_PREFIX: u8 = 0x80;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SchemaRead)]
#[repr(C)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub accounts: Vec<u8>,
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct LegacyMessage<'a> {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<&'a [u8; 32], ShortU16Len>")]
    pub account_keys: Vec<&'a [u8; 32]>,
    pub recent_blockhash: &'a [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct MessageAddressTableLookup<'a> {
    pub account_key: &'a [u8; 32],
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub writable_indexes: Vec<u8>,
    #[wincode(with = "containers::Vec<u8, ShortU16Len>")]
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, SchemaRead)]
pub struct V0Message<'a> {
    pub header: MessageHeader,
    #[wincode(with = "containers::Vec<&'a [u8; 32], ShortU16Len>")]
    pub account_keys: Vec<&'a [u8; 32]>,
    pub recent_blockhash: &'a [u8; 32],
    #[wincode(with = "containers::Vec<CompiledInstruction, ShortU16Len>")]
    pub instructions: Vec<CompiledInstruction>,
    #[wincode(with = "containers::Vec<MessageAddressTableLookup<'a>, ShortU16Len>")]
    pub address_table_lookups: Vec<MessageAddressTableLookup<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VersionedMessage<'a> {
    Legacy(LegacyMessage<'a>),
    V0(V0Message<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead)]
pub struct VersionedTransaction<'a> {
    #[wincode(with = "containers::Vec<&'a [u8; 64], ShortU16Len>")]
    pub signatures: Vec<&'a [u8; 64]>,
    pub message: VersionedMessage<'a>,
}

impl<'de> SchemaRead<'de> for VersionedMessage<'de> {
    type Dst = VersionedMessage<'de>;

    #[inline(always)]
    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let first = u8::get(reader)?;

        if first & MESSAGE_VERSION_PREFIX != 0 {
            let version = first & !MESSAGE_VERSION_PREFIX;
            return match version {
                0 => {
                    let msg = V0Message::get(reader)?;
                    dst.write(VersionedMessage::V0(msg));
                    Ok(())
                }
                _ => Err(invalid_tag_encoding(version as usize)),
            };
        }

        let num_readonly_signed_accounts = u8::get(reader)?;
        let num_readonly_unsigned_accounts = u8::get(reader)?;

        let header = MessageHeader {
            num_required_signatures: first,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
        };

        // Zero-copy pubkeys + blockhash
        let account_keys = containers::Vec::<&'de [u8; 32], ShortU16Len>::get(reader)?;
        let recent_blockhash = <&'de [u8; 32]>::get(reader)?;
        let instructions = containers::Vec::<CompiledInstruction, ShortU16Len>::get(reader)?;

        dst.write(VersionedMessage::Legacy(LegacyMessage {
            header,
            account_keys,
            recent_blockhash,
            instructions,
        }));
        Ok(())
    }
}

use {
    solana_address::Address,
    solana_hash::Hash,
    solana_message::{self, legacy, v0, MESSAGE_VERSION_PREFIX},
    solana_signature::Signature,
    solana_transaction::versioned,
    std::mem::MaybeUninit,
    wincode::{
        containers::{self, Pod},
        error::invalid_tag_encoding,
        io::Reader,
        len::ShortU16Len,
        ReadResult, SchemaRead, SchemaWrite,
    },
};

#[derive(SchemaRead, SchemaWrite)]
#[wincode(from = "solana_message::MessageHeader", struct_extensions)]
struct MessageHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(from = "solana_message::compiled_instruction::CompiledInstruction")]
struct CompiledInstruction {
    program_id_index: u8,
    accounts: containers::Vec<Pod<u8>, ShortU16Len>,
    data: containers::Vec<Pod<u8>, ShortU16Len>,
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(from = "legacy::Message", struct_extensions)]
struct LegacyMessage {
    header: MessageHeader,
    account_keys: containers::Vec<Pod<Address>, ShortU16Len>,
    recent_blockhash: Pod<Hash>,
    instructions: containers::Vec<CompiledInstruction, ShortU16Len>,
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(from = "v0::MessageAddressTableLookup")]
struct MessageAddressTableLookup {
    account_key: Pod<Address>,
    writable_indexes: containers::Vec<Pod<u8>, ShortU16Len>,
    readonly_indexes: containers::Vec<Pod<u8>, ShortU16Len>,
}

#[derive(SchemaRead, SchemaWrite)]
#[wincode(from = "v0::Message")]
struct V0Message {
    #[wincode(with = "Pod<_>")]
    header: solana_message::MessageHeader,
    account_keys: containers::Vec<Pod<Address>, ShortU16Len>,
    recent_blockhash: Pod<Hash>,
    instructions: containers::Vec<CompiledInstruction, ShortU16Len>,
    address_table_lookups: containers::Vec<MessageAddressTableLookup, ShortU16Len>,
}

struct VersionedMsg;

impl<'de> SchemaRead<'de> for VersionedMsg {
    type Dst = solana_message::VersionedMessage;

    #[inline(always)]
    fn read(reader: &mut impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let first = u8::get(reader)?;

        // Versioned (v0)
        if first & MESSAGE_VERSION_PREFIX != 0 {
            let version = first & !MESSAGE_VERSION_PREFIX;
            return match version {
                0 => {
                    let msg = V0Message::get(reader)?;
                    dst.write(solana_message::VersionedMessage::V0(msg));
                    Ok(())
                }
                _ => Err(invalid_tag_encoding(version as usize)),
            };
        }

        // Legacy: first byte is num_required_signatures
        let mut msg = MaybeUninit::<legacy::Message>::uninit();
        let header = LegacyMessage::uninit_header_mut(&mut msg);

        MessageHeader::write_uninit_num_required_signatures(first, header);
        MessageHeader::read_num_readonly_signed_accounts(reader, header)?;
        MessageHeader::read_num_readonly_unsigned_accounts(reader, header)?;

        LegacyMessage::read_account_keys(reader, &mut msg)?;
        LegacyMessage::read_recent_blockhash(reader, &mut msg)?;
        LegacyMessage::read_instructions(reader, &mut msg)?;

        dst.write(solana_message::VersionedMessage::Legacy(unsafe {
            msg.assume_init()
        }));
        Ok(())
    }
}

#[derive(SchemaRead)]
#[wincode(from = "versioned::VersionedTransaction")]
pub struct VersionedTransactionSchema {
    signatures: containers::Vec<Pod<Signature>, ShortU16Len>,
    message: VersionedMsg,
}

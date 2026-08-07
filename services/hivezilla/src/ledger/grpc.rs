//! Yellowstone block-to-ledger structural projection.
//!
//! This adapter verifies provider structure and preserves the canonical Solana
//! signed-message serialization. It does not perform the complete promotion
//! signature policy, recompute the PoH chain, supply shred markers/identities,
//! or make a finality claim. It verifies signatures only to disambiguate a
//! lossy versioned-message schema shape; the remaining checks belong to source
//! promotion and finality.

use anyhow::{Context, Result, ensure};
use blockzilla_format::{
    BlockCandidatePartsV1, BlockCandidateV1, PohEntryV1, SignedTransactionEnvelopeV1,
    TransactionsV1,
};
use ed25519_dalek::{Signature, VerifyingKey};
use yellowstone_grpc_proto::prelude::{Message as GrpcMessage, SubscribeUpdateBlock};

use crate::{
    grpc::{bytes32, bytes64, decode_required_hash_string, decode_source_parent_blockhash},
    grpc_raw::validate_complete_poh_block,
};

const VERSIONED_MESSAGE_PREFIX: u8 = 0x80;
const V0_MESSAGE_VERSION: u8 = 0;

/// Project one complete provider observation into the unpromoted ledger shape.
pub(crate) fn project_grpc_ledger_candidate(
    block: &SubscribeUpdateBlock,
) -> Result<BlockCandidateV1> {
    validate_complete_poh_block(block)
        .with_context(|| format!("validate complete gRPC block at slot {}", block.slot))?;

    let mut source_transactions = block.transactions.iter().collect::<Vec<_>>();
    source_transactions.sort_unstable_by_key(|transaction| transaction.index);
    let mut transactions = Vec::with_capacity(source_transactions.len());
    for source in source_transactions {
        let transaction = source.transaction.as_ref().with_context(|| {
            format!(
                "slot {} transaction {} is missing its signed transaction",
                block.slot, source.index
            )
        })?;
        let message = transaction.message.as_ref().with_context(|| {
            format!(
                "slot {} transaction {} is missing its message",
                block.slot, source.index
            )
        })?;
        let header = message.header.as_ref().with_context(|| {
            format!(
                "slot {} transaction {} message is missing its header",
                block.slot, source.index
            )
        })?;
        let required_signatures = usize::try_from(header.num_required_signatures)
            .context("gRPC required-signature count exceeds usize")?;
        ensure!(
            transaction.signatures.len() == required_signatures,
            "slot {} transaction {} has {} signatures, expected {}",
            block.slot,
            source.index,
            transaction.signatures.len(),
            required_signatures
        );

        let signatures = transaction
            .signatures
            .iter()
            .enumerate()
            .map(|(signature_index, signature)| {
                bytes64(signature).with_context(|| {
                    format!(
                        "slot {} transaction {} signature {}",
                        block.slot, source.index, signature_index
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let source_transaction_id = bytes64(&source.signature).with_context(|| {
            format!(
                "slot {} transaction {} top-level signature",
                block.slot, source.index
            )
        })?;
        ensure!(
            signatures.first() == Some(&source_transaction_id),
            "slot {} transaction {} top-level signature does not equal signed transaction signature 0",
            block.slot,
            source.index
        );
        let signed_message_bytes = serialize_grpc_signed_message(message).with_context(|| {
            format!(
                "serialize slot {} transaction {} signed message",
                block.slot, source.index
            )
        })?;
        verify_versioned_message_is_v0(message, &signatures, &signed_message_bytes).with_context(
            || {
                format!(
                    "prove slot {} transaction {} uses the pinned V0 message encoding",
                    block.slot, source.index
                )
            },
        )?;
        transactions.push(SignedTransactionEnvelopeV1 {
            signatures,
            signed_message_bytes,
        });
    }

    let mut source_entries = block.entries.iter().collect::<Vec<_>>();
    source_entries.sort_unstable_by_key(|entry| entry.index);
    let poh_entries = source_entries
        .into_iter()
        .map(|entry| {
            Ok(PohEntryV1 {
                num_hashes: entry.num_hashes,
                hash: bytes32(&entry.hash).with_context(|| {
                    format!("slot {} entry {} PoH hash", block.slot, entry.index)
                })?,
                tx_count: u32::try_from(entry.executed_transaction_count).with_context(|| {
                    format!(
                        "slot {} entry {} transaction count exceeds u32",
                        block.slot, entry.index
                    )
                })?,
                // Not wired for this ingestion path yet; verify-archive-v2-poh's cross-check
                // against the block index detects the unpopulated count and falls back to
                // decompression rather than trusting it.
                signature_count: 0,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    BlockCandidateV1::new(BlockCandidatePartsV1 {
        slot: block.slot,
        parent_slot: block.parent_slot,
        final_poh_hash: decode_required_hash_string(&block.blockhash)
            .with_context(|| format!("decode slot {} final PoH hash", block.slot))?,
        consensus_block_id: None,
        parent_final_poh_hash: decode_source_parent_blockhash(block.slot, &block.parent_blockhash)?,
        parent_consensus_block_id: None,
        transactions: Some(TransactionsV1 {
            entries: transactions,
        }),
        poh_entries: Some(poh_entries),
        block_components: None,
    })
    .with_context(|| {
        format!(
            "construct structural gRPC candidate for slot {}",
            block.slot
        )
    })
}

/// Prove the V0 interpretation of every versioned message retained by the
/// pinned Yellowstone schema.
///
/// `yellowstone-grpc-proto` 12.4 carries only a `versioned` boolean. A V0
/// message with no address-table lookups and a current V1 message therefore
/// have the same retained shape after the V1-only protobuf field is dropped;
/// a later message version could overlap another retained shape. The fee-payer
/// signature over the reconstructed V0 bytes proves the exact interpretation.
/// Complete multi-signature verification remains a promotion responsibility.
/// Failure is terminal: this adapter must never silently rewrite a newer signed
/// message as V0.
fn verify_versioned_message_is_v0(
    message: &GrpcMessage,
    signatures: &[[u8; 64]],
    signed_message_bytes: &[u8],
) -> Result<()> {
    if !message.versioned {
        return Ok(());
    }
    let signature_bytes = signatures
        .first()
        .context("versioned message has no fee-payer signature proving its V0 encoding")?;
    let signer_key = message
        .account_keys
        .first()
        .context("versioned message has no fee-payer account key")?;
    let signer_bytes = bytes32(signer_key).context("fee-payer account key")?;
    let verifying_key =
        VerifyingKey::from_bytes(&signer_bytes).context("decode fee-payer account key")?;
    let signature = Signature::from_bytes(signature_bytes);
    verifying_key
        .verify_strict(signed_message_bytes, &signature)
        .context("versioned message is not a fee-payer-signature-proven V0 message")?;
    Ok(())
}

/// Encode the canonical Solana Legacy or V0 message bytes covered by signatures.
fn serialize_grpc_signed_message(message: &GrpcMessage) -> Result<Vec<u8>> {
    let header = message.header.as_ref().context("message missing header")?;
    let required_signatures = u8::try_from(header.num_required_signatures)
        .context("num_required_signatures exceeds u8")?;
    let readonly_signed = u8::try_from(header.num_readonly_signed_accounts)
        .context("num_readonly_signed_accounts exceeds u8")?;
    let readonly_unsigned = u8::try_from(header.num_readonly_unsigned_accounts)
        .context("num_readonly_unsigned_accounts exceeds u8")?;

    // A Legacy message begins directly with this header byte. Values with the
    // high bit set are version discriminants, so emitting one would produce a
    // different (or unsupported) message rather than a large Legacy header.
    if !message.versioned {
        ensure!(
            required_signatures < VERSIONED_MESSAGE_PREFIX,
            "legacy required-signature count sets the version prefix bit"
        );
    }

    let account_key_count = message.account_keys.len();
    ensure!(
        usize::from(required_signatures) <= account_key_count,
        "required signatures exceed static account keys"
    );
    ensure!(
        readonly_signed <= required_signatures,
        "readonly signed accounts exceed required signatures"
    );
    ensure!(
        usize::from(readonly_unsigned)
            <= account_key_count.saturating_sub(usize::from(required_signatures)),
        "readonly unsigned accounts exceed unsigned static account keys"
    );
    if !message.versioned {
        ensure!(
            message.address_table_lookups.is_empty(),
            "legacy message contains address-table lookups"
        );
    }

    let mut out = Vec::new();
    if message.versioned {
        out.push(VERSIONED_MESSAGE_PREFIX | V0_MESSAGE_VERSION);
    }
    out.extend_from_slice(&[required_signatures, readonly_signed, readonly_unsigned]);
    push_short_vec_len(&mut out, account_key_count, "static account keys")?;
    for (index, key) in message.account_keys.iter().enumerate() {
        out.extend_from_slice(
            &bytes32(key).with_context(|| format!("static account key {index}"))?,
        );
    }
    out.extend_from_slice(&bytes32(&message.recent_blockhash).context("recent blockhash")?);

    push_short_vec_len(&mut out, message.instructions.len(), "instructions")?;
    for (index, instruction) in message.instructions.iter().enumerate() {
        out.push(
            u8::try_from(instruction.program_id_index)
                .with_context(|| format!("instruction {index} program index exceeds u8"))?,
        );
        push_short_vec_len(&mut out, instruction.accounts.len(), "instruction accounts")?;
        out.extend_from_slice(&instruction.accounts);
        push_short_vec_len(&mut out, instruction.data.len(), "instruction data")?;
        out.extend_from_slice(&instruction.data);
    }

    if message.versioned {
        push_short_vec_len(
            &mut out,
            message.address_table_lookups.len(),
            "address-table lookups",
        )?;
        for (index, lookup) in message.address_table_lookups.iter().enumerate() {
            out.extend_from_slice(
                &bytes32(&lookup.account_key)
                    .with_context(|| format!("address-table lookup {index} account"))?,
            );
            push_short_vec_len(
                &mut out,
                lookup.writable_indexes.len(),
                "writable lookup indexes",
            )?;
            out.extend_from_slice(&lookup.writable_indexes);
            push_short_vec_len(
                &mut out,
                lookup.readonly_indexes.len(),
                "readonly lookup indexes",
            )?;
            out.extend_from_slice(&lookup.readonly_indexes);
        }
    }

    Ok(out)
}

fn push_short_vec_len(out: &mut Vec<u8>, len: usize, field: &'static str) -> Result<()> {
    let mut value = u16::try_from(len).with_context(|| format!("{field} length exceeds u16"))?;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use yellowstone_grpc_proto::prelude::{
        MessageHeader, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
        Transaction as GrpcTransaction,
    };

    fn complete_block() -> SubscribeUpdateBlock {
        let message = GrpcMessage {
            header: Some(MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            }),
            account_keys: vec![vec![3; 32]],
            recent_blockhash: vec![4; 32],
            instructions: vec![],
            versioned: false,
            address_table_lookups: vec![],
        };
        let transaction = SubscribeUpdateTransactionInfo {
            signature: vec![7; 64],
            transaction: Some(GrpcTransaction {
                signatures: vec![vec![7; 64]],
                message: Some(message),
            }),
            index: 0,
            ..Default::default()
        };
        SubscribeUpdateBlock {
            slot: 42,
            parent_slot: 41,
            blockhash: "11111111111111111111111111111111".to_owned(),
            parent_blockhash: "11111111111111111111111111111111".to_owned(),
            executed_transaction_count: 1,
            entries_count: 2,
            transactions: vec![transaction],
            // Source order is deliberately reversed; explicit indexes define ledger order.
            entries: vec![
                SubscribeUpdateEntry {
                    slot: 42,
                    index: 1,
                    num_hashes: 7,
                    hash: vec![0; 32],
                    executed_transaction_count: 0,
                    starting_transaction_index: 1,
                },
                SubscribeUpdateEntry {
                    slot: 42,
                    index: 0,
                    num_hashes: 5,
                    hash: vec![0x11; 32],
                    executed_transaction_count: 1,
                    starting_transaction_index: 0,
                },
            ],
            ..Default::default()
        }
    }

    #[test]
    fn projects_complete_legacy_block_without_runtime_or_shred_claims() {
        let candidate = project_grpc_ledger_candidate(&complete_block()).unwrap();
        assert_eq!(candidate.slot(), 42);
        assert_eq!(candidate.parent_slot(), 41);
        assert_eq!(candidate.final_poh_hash(), &[0; 32]);
        assert_eq!(candidate.parent_final_poh_hash(), Some(&[0; 32]));
        assert_eq!(candidate.consensus_block_id(), None);
        assert_eq!(candidate.block_components(), None);

        let transactions = candidate.transactions().unwrap();
        assert_eq!(transactions.entries.len(), 1);
        assert_eq!(transactions.entries[0].signatures, vec![[7; 64]]);
        let mut expected_message = vec![1, 0, 0, 1];
        expected_message.extend_from_slice(&[3; 32]);
        expected_message.extend_from_slice(&[4; 32]);
        expected_message.push(0);
        assert_eq!(
            transactions.entries[0].signed_message_bytes,
            expected_message
        );
        assert_eq!(candidate.poh_entries().unwrap()[0].hash, [0x11; 32]);
        assert_eq!(candidate.poh_entries().unwrap()[1].hash, [0; 32]);
    }

    #[test]
    fn rejects_invalid_signature_length_and_header_relationships() {
        let mut block = complete_block();
        block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .signatures[0] = vec![7; 63];
        let error = project_grpc_ledger_candidate(&block).unwrap_err();
        assert!(format!("{error:#}").contains("signature 0"));

        let mut block = complete_block();
        block.transactions[0].signature[0] ^= 1;
        let error = project_grpc_ledger_candidate(&block).unwrap_err();
        assert!(format!("{error:#}").contains("does not equal signed transaction signature 0"));

        let mut block = complete_block();
        block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .num_readonly_signed_accounts = 2;
        let error = project_grpc_ledger_candidate(&block).unwrap_err();
        assert!(
            format!("{error:#}").contains("readonly signed accounts exceed required signatures")
        );

        let mut block = complete_block();
        let transaction = block.transactions[0].transaction.as_mut().unwrap();
        let message = transaction.message.as_mut().unwrap();
        message.header.as_mut().unwrap().num_required_signatures = 0x80;
        message.account_keys = vec![vec![3; 32]; 0x80];
        transaction.signatures = vec![vec![7; 64]; 0x80];
        let error = project_grpc_ledger_candidate(&block).unwrap_err();
        assert!(format!("{error:#}").contains("sets the version prefix bit"));
    }

    #[test]
    fn serializes_v0_prefix_and_lookup_order() {
        let mut block = complete_block();
        let message = block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .message
            .as_mut()
            .unwrap();
        message.versioned = true;
        message.address_table_lookups.push(Default::default());
        message.address_table_lookups[0].account_key = vec![8; 32];
        message.address_table_lookups[0].writable_indexes = vec![2, 3];
        message.address_table_lookups[0].readonly_indexes = vec![4];
        let signing_key = SigningKey::from_bytes(&[0x24; 32]);
        message.account_keys[0] = signing_key.verifying_key().to_bytes().to_vec();
        let signed_message_bytes = serialize_grpc_signed_message(message).unwrap();
        let signature = signing_key.sign(&signed_message_bytes).to_bytes().to_vec();
        block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .signatures[0] = signature.clone();
        block.transactions[0].signature = signature;

        let candidate = project_grpc_ledger_candidate(&block).unwrap();
        let bytes = &candidate.transactions().unwrap().entries[0].signed_message_bytes;
        let mut expected = vec![0x80, 1, 0, 0, 1];
        expected.extend_from_slice(&signing_key.verifying_key().to_bytes());
        expected.extend_from_slice(&[4; 32]);
        expected.extend_from_slice(&[0, 1]);
        expected.extend_from_slice(&[8; 32]);
        expected.extend_from_slice(&[2, 2, 3, 1, 4]);
        assert_eq!(bytes, &expected);
    }

    #[test]
    fn every_versioned_message_requires_fee_payer_signature_proving_v0_bytes() {
        let mut block = complete_block();
        let transaction = block.transactions[0].transaction.as_mut().unwrap();
        let message = transaction.message.as_mut().unwrap();
        message.versioned = true;
        assert!(message.address_table_lookups.is_empty());

        let signing_key = SigningKey::from_bytes(&[0x42; 32]);
        message.account_keys[0] = signing_key.verifying_key().to_bytes().to_vec();
        let signed_message_bytes = serialize_grpc_signed_message(message).unwrap();
        let signature = signing_key.sign(&signed_message_bytes).to_bytes().to_vec();
        transaction.signatures[0] = signature.clone();
        block.transactions[0].signature = signature;
        project_grpc_ledger_candidate(&block).unwrap();

        block.transactions[0].signature[0] ^= 1;
        block.transactions[0]
            .transaction
            .as_mut()
            .unwrap()
            .signatures[0][0] ^= 1;
        let error = project_grpc_ledger_candidate(&block).unwrap_err();
        assert!(format!("{error:#}").contains("fee-payer-signature-proven V0 message"));
    }

    #[test]
    fn short_vec_length_encoding_matches_canonical_boundaries() {
        for (length, expected) in [
            (0, vec![0]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xff, 0x7f]),
            (16_384, vec![0x80, 0x80, 0x01]),
            (65_535, vec![0xff, 0xff, 0x03]),
        ] {
            let mut bytes = Vec::new();
            push_short_vec_len(&mut bytes, length, "fixture").unwrap();
            assert_eq!(bytes, expected, "length {length}");
        }

        let mut bytes = Vec::new();
        assert!(
            push_short_vec_len(&mut bytes, 65_536, "fixture")
                .unwrap_err()
                .to_string()
                .contains("exceeds u16")
        );
        assert!(bytes.is_empty());
    }
}

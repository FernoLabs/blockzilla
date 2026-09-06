//! Reusable transaction account references for selected indexed token rows.
//!
//! References remain compact registry IDs or inline keys. No registry lookup
//! occurs here. Token-balance `account_index` addresses this message account
//! order: static, loaded writable, then loaded readonly. The balance owner is
//! separate metadata and must not be used as the token account.

use anyhow::{Result, ensure};

use crate::{
    CompactPubkey, CompactV2MessageProjector, CompactV2MessageSchema,
    CompactV2MetadataProjectionLimits, CompactV2MetadataProjector, CompactV2MetadataSchema,
    source_decode::{
        MAX_MESSAGE_ACCOUNTS, MessageAccountEvent, MetadataAccountEvent, MetadataDecodeLimits,
        stream_message_accounts_with_schema, stream_metadata_accounts_with_schema,
    },
};

/// One reusable account lane. Allocate once per worker, then lend its live prefix.
#[derive(Debug)]
pub struct CompactV2AccountReferences {
    references: [CompactPubkey; MAX_MESSAGE_ACCOUNTS],
    len: usize,
}

impl Default for CompactV2AccountReferences {
    fn default() -> Self {
        Self {
            references: [CompactPubkey::Id(0); MAX_MESSAGE_ACCOUNTS],
            len: 0,
        }
    }
}

impl CompactV2AccountReferences {
    /// References in transaction account order, borrowed without a new vector.
    pub fn as_slice(&self) -> &[CompactPubkey] {
        &self.references[..self.len]
    }

    /// Resolve a transaction-local token-balance index to its compact reference.
    pub fn get(&self, account_index: u32) -> Option<&CompactPubkey> {
        usize::try_from(account_index)
            .ok()
            .and_then(|index| self.as_slice().get(index))
    }

    /// Validate complete message and metadata records before exposing references.
    ///
    /// Call only when a transaction has selected token rows. Other transactions
    /// do not need an account-reference projection. Any error empties the view.
    pub fn project(
        &mut self,
        message_bytes: &[u8],
        metadata_bytes: &[u8],
        message_projector: CompactV2MessageProjector,
        metadata_projector: CompactV2MetadataProjector,
    ) -> Result<()> {
        self.len = 0;
        ensure!(
            message_projector.registry_entries() == metadata_projector.registry_entries(),
            "account-reference projectors use different registries"
        );
        let message = message_projector.count_message(message_bytes)?;
        let limits = CompactV2MetadataProjectionLimits::for_message(&message);
        metadata_projector.count(metadata_bytes, limits)?;
        self.project_validated(
            message_bytes,
            metadata_bytes,
            message_projector.schema(),
            metadata_projector.schema(),
            metadata_projector.registry_entries(),
            limits,
        )
    }

    /// Collect references ONLY after the caller has successfully validated the
    /// complete token projection on these exact bytes, schemas, registry, and
    /// message count geometry. The source streamers alone are not admission
    /// validators: the metadata traversal stops before the already-checked tail.
    /// This avoids a second full validation pass for the reader's selected rows.
    pub(crate) fn project_validated(
        &mut self,
        message_bytes: &[u8],
        metadata_bytes: &[u8],
        message_schema: CompactV2MessageSchema,
        metadata_schema: CompactV2MetadataSchema,
        registry_entries: u32,
        limits: CompactV2MetadataProjectionLimits,
    ) -> Result<()> {
        self.len = 0;
        let result = (|| {
            ensure!(
                limits.total_message_accounts <= MAX_MESSAGE_ACCOUNTS,
                "account-reference count exceeds the transaction account bound"
            );
            let expected_static = limits
                .total_message_accounts
                .checked_sub(limits.expected_loaded_writable)
                .and_then(|count| count.checked_sub(limits.expected_loaded_readonly))
                .ok_or_else(|| anyhow::anyhow!("loaded account counts exceed total accounts"))?;
            let mut message_cursor = message_bytes;
            let message = stream_message_accounts_with_schema(
                &mut message_cursor,
                message_schema,
                |event| -> Result<()> {
                    match event {
                        MessageAccountEvent::StaticAccountCount(count) => ensure!(
                            count == expected_static,
                            "static account count differs from validated message geometry"
                        ),
                        MessageAccountEvent::StaticAccount {
                            source_position,
                            key,
                        } => {
                            ensure!(
                                source_position == self.len,
                                "static account order differs from validated message geometry"
                            );
                            self.push(key, registry_entries)?;
                        }
                        MessageAccountEvent::Instruction(_) => {}
                    }
                    Ok(())
                },
            )?;
            ensure!(
                message_cursor.is_empty(),
                "message has trailing account-reference bytes"
            );
            ensure!(
                message.static_account_count == expected_static
                    && message.instruction_count == limits.top_level_instruction_count
                    && message.expected_loaded_writable == limits.expected_loaded_writable
                    && message.expected_loaded_readonly == limits.expected_loaded_readonly,
                "account-reference message differs from validated geometry"
            );
            if limits.expected_loaded_writable == 0 && limits.expected_loaded_readonly == 0 {
                // The complete validated metadata pass already proved both lanes
                // empty. Static-only matches need no second metadata traversal.
                ensure!(
                    self.len == limits.total_message_accounts,
                    "static account-reference count differs from validated geometry"
                );
                return Ok(());
            }
            let mut metadata_cursor = metadata_bytes;
            let metadata = stream_metadata_accounts_with_schema(
                &mut metadata_cursor,
                metadata_schema,
                true,
                MetadataDecodeLimits {
                    total_message_accounts: limits.total_message_accounts,
                    top_level_instruction_count: limits.top_level_instruction_count,
                },
                |event| -> Result<()> {
                    match event {
                        MetadataAccountEvent::LoadedWritableCount(count) => ensure!(
                            count == limits.expected_loaded_writable,
                            "loaded writable count differs from validated message geometry"
                        ),
                        MetadataAccountEvent::LoadedReadonlyCount(count) => ensure!(
                            count == limits.expected_loaded_readonly,
                            "loaded readonly count differs from validated message geometry"
                        ),
                        MetadataAccountEvent::LoadedWritable(key)
                        | MetadataAccountEvent::LoadedReadonly(key) => {
                            self.push(key, registry_entries)?;
                        }
                        MetadataAccountEvent::InnerInstruction(_) => {}
                    }
                    Ok(())
                },
            )?;
            ensure!(
                metadata.loaded_writable_count == limits.expected_loaded_writable
                    && metadata.loaded_readonly_count == limits.expected_loaded_readonly
                    && self.len == limits.total_message_accounts,
                "account-reference metadata differs from validated geometry"
            );
            Ok(())
        })();
        if result.is_err() {
            self.len = 0;
        }
        result
    }

    fn push(&mut self, key: CompactPubkey, registry_entries: u32) -> Result<()> {
        ensure!(
            self.len < MAX_MESSAGE_ACCOUNTS,
            "too many transaction account references"
        );
        if let CompactPubkey::Id(id) = key {
            ensure!(
                id != 0 && id <= registry_entries,
                "account registry ID is outside the admitted registry"
            );
        }
        self.references[self.len] = key;
        self.len += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use blockzilla_archive_v2::{
        ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, ArchiveV2HotV0Message,
    };
    use blockzilla_compact::{
        CompactMessageHeader, CompactMetaV1, CompactTokenBalance, OwnedCompactAddressTableLookup,
        OwnedCompactRecentBlockhash,
    };
    use blockzilla_primitives::wincode_leb128_config;

    use super::*;

    const REGISTRY_ENTRIES: u32 = 8;
    const RAW_SIGNER: CompactPubkey = CompactPubkey::Raw([11; 32]);
    const RAW_LOADED: CompactPubkey = CompactPubkey::Raw([12; 32]);

    fn metadata(loaded: bool) -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![0; if loaded { 4 } else { 1 }],
            post_balances: vec![0; if loaded { 4 } else { 1 }],
            inner_instructions: None,
            logs: None,
            pre_token_balances: if loaded {
                vec![CompactTokenBalance {
                    account_index: 2,
                    mint: Some(CompactPubkey::Id(5)),
                    owner: Some(CompactPubkey::Id(4)),
                    program_id: Some(CompactPubkey::Id(2)),
                    amount: 42,
                    decimals: 6,
                }]
            } else {
                Vec::new()
            },
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: if loaded {
                vec![CompactPubkey::Id(3)]
            } else {
                Vec::new()
            },
            loaded_readonly_addresses: if loaded { vec![RAW_LOADED] } else { Vec::new() },
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn message(loaded: bool) -> Vec<u8> {
        let header = CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let recent_blockhash = OwnedCompactRecentBlockhash::Nonce([0; 32]);
        let value = if loaded {
            ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header,
                account_keys: vec![RAW_SIGNER, CompactPubkey::Id(2)],
                recent_blockhash,
                instructions: Vec::new(),
                address_table_lookups: vec![OwnedCompactAddressTableLookup {
                    account_key: CompactPubkey::Id(6),
                    writable_indexes: vec![0],
                    readonly_indexes: vec![2],
                }],
            })
        } else {
            ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header,
                account_keys: vec![RAW_SIGNER],
                recent_blockhash,
                instructions: Vec::new(),
            })
        };
        wincode::config::serialize(&value, wincode_leb128_config()).unwrap()
    }

    fn project(
        refs: &mut CompactV2AccountReferences,
        message: &[u8],
        metadata: &[u8],
    ) -> Result<()> {
        refs.project(
            message,
            metadata,
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, REGISTRY_ENTRIES),
            CompactV2MetadataProjector::new(
                CompactV2MetadataSchema::CurrentTypedError,
                REGISTRY_ENTRIES,
            ),
        )
    }

    #[test]
    fn preserves_static_loaded_and_raw_order_without_using_balance_owner() {
        let message = message(true);
        let metadata =
            wincode::config::serialize(&metadata(true), wincode_leb128_config()).unwrap();
        let mut refs = CompactV2AccountReferences::default();
        assert!(refs.as_slice().is_empty());
        assert_eq!(refs.get(0), None);
        for message_schema in [
            CompactV2MessageSchema::Current,
            CompactV2MessageSchema::May24PreUnknownFallbacks,
        ] {
            for metadata_schema in [
                CompactV2MetadataSchema::CurrentTypedError,
                CompactV2MetadataSchema::LegacyRawError,
            ] {
                refs.project(
                    &message,
                    &metadata,
                    CompactV2MessageProjector::new(message_schema, REGISTRY_ENTRIES),
                    CompactV2MetadataProjector::new(metadata_schema, REGISTRY_ENTRIES),
                )
                .unwrap();
                assert_eq!(
                    refs.as_slice(),
                    [
                        RAW_SIGNER,
                        CompactPubkey::Id(2),
                        CompactPubkey::Id(3),
                        RAW_LOADED
                    ]
                );
                assert_eq!(refs.get(2), Some(&CompactPubkey::Id(3)));
                assert_eq!(refs.get(4), None);
            }
        }
    }

    #[test]
    fn reuses_fixed_storage_and_hides_old_loaded_references() {
        let mut refs = CompactV2AccountReferences::default();
        let loaded_metadata =
            wincode::config::serialize(&metadata(true), wincode_leb128_config()).unwrap();
        project(&mut refs, &message(true), &loaded_metadata).unwrap();
        let pointer = refs.as_slice().as_ptr();
        let static_metadata =
            wincode::config::serialize(&metadata(false), wincode_leb128_config()).unwrap();
        project(&mut refs, &message(false), &static_metadata).unwrap();
        assert_eq!(refs.as_slice().as_ptr(), pointer);
        assert_eq!(refs.as_slice(), [RAW_SIGNER]);
        assert_eq!(refs.get(1), None);
    }

    #[test]
    fn failed_projection_never_exposes_previous_or_partial_references() {
        let message = message(true);
        let metadata =
            wincode::config::serialize(&metadata(true), wincode_leb128_config()).unwrap();
        let mut refs = CompactV2AccountReferences::default();
        let mut trailing = metadata.clone();
        trailing.push(0);
        for (bad_message, bad_metadata) in [
            (&[][..], metadata.as_slice()),
            (message.as_slice(), &metadata[..metadata.len() - 1]),
            (message.as_slice(), trailing.as_slice()),
        ] {
            project(&mut refs, &message, &metadata).unwrap();
            assert!(project(&mut refs, bad_message, bad_metadata).is_err());
            assert!(refs.as_slice().is_empty());
        }
        project(&mut refs, &message, &metadata).unwrap();
        let wrong_geometry = CompactV2MetadataProjectionLimits {
            total_message_accounts: 4,
            top_level_instruction_count: 0,
            expected_loaded_writable: 0,
            expected_loaded_readonly: 2,
        };
        assert!(
            refs.project_validated(
                &message,
                &metadata,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
                REGISTRY_ENTRIES,
                wrong_geometry,
            )
            .is_err()
        );
        assert!(refs.as_slice().is_empty());
        let limits =
            CompactV2MessageProjector::new(CompactV2MessageSchema::Current, REGISTRY_ENTRIES)
                .count_message(&message)
                .unwrap()
                .count_limits();
        assert!(
            refs.project_validated(
                &message,
                &metadata,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::CurrentTypedError,
                2,
                limits,
            )
            .is_err()
        );
        assert!(refs.as_slice().is_empty());
    }
}

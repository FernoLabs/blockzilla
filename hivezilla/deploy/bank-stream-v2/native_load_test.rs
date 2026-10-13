// Compiled inside the pinned host, with the host's Rust toolchain and trait.
#[cfg(test)]
mod hivezilla_bank_v2_load_test {
    use super::*;
    use solana_runtime::{bank::Bank, genesis_utils::create_genesis_config};
    use solana_rpc::{optimistically_confirmed_bank_tracker::NativeBankStateV2, slot_status_notifier_interface::SlotStatusNotifierInterface};
    #[test]
    #[ignore = "requires the actual bank-v2 plugin and an isolated config"]
    fn native_bank_v2_actual_plugin_load() {
        let config=std::env::var("HIVEZILLA_NATIVE_TEST_CONFIG").expect("isolated config");
        let receipt=std::env::var("HIVEZILLA_NATIVE_TEST_RECEIPT").expect("receipt path");
        let info=create_genesis_config(100);
        let parent=Arc::new(Bank::new_for_tests(&info.genesis_config));parent.freeze();
        let bank=Bank::new_from_parent(parent.clone(),Default::default(),17);bank.freeze();
        let manager=Arc::new(ArcSwap::from_pointee(GeyserPluginManager::default()));
        let name=GeyserPluginManager::load_plugin(&manager,&config).expect("actual dlopen and on_load");
        let notifier=crate::slot_status_notifier::SlotStatusNotifierImpl::new(manager.clone());
        let frozen=NativeBankStateV2::from_bank(&bank,false);
        let confirmed=NativeBankStateV2::from_bank(&bank,true);
        assert_eq!(confirmed.last_valid_block_height,bank.get_blockhash_last_valid_block_height(&bank.last_blockhash()));
        assert_eq!(confirmed.bank_id,bank.bank_id());assert_eq!(confirmed.parent_blockhash,parent.last_blockhash().to_bytes());
        assert!(confirmed.last_valid_block_height.is_some());
        notifier.notify_bank_state_v2(&frozen);notifier.notify_bank_state_v2(&confirmed);
        drop(notifier);
        let old=manager.swap(Arc::new(GeyserPluginManager::default()));
        let mut m=Arc::try_unwrap(old).expect("isolated manager");m.unload();drop(manager);
        std::fs::write(receipt,serde_json::to_vec_pretty(&serde_json::json!({"state":"PASSED","load_result":name,"slot":confirmed.slot,"bank_id":confirmed.bank_id,"parent_slot":confirmed.parent_slot,"blockhash":confirmed.blockhash,"parent_blockhash":confirmed.parent_blockhash,"block_height":confirmed.block_height,"last_valid_block_height":confirmed.last_valid_block_height,"confirmed":confirmed.confirmed,"observed_at_unix_ms":confirmed.observed_at_unix_ms})).unwrap()).unwrap();
    }
}

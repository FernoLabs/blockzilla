use blockzilla_live_producer::ingest;

#[test]
fn ingest_module_reexports_the_validated_public_api() {
    let error = ingest::IngestConfig::from_json(
        r#"{
            "schema_version": 99,
            "cluster_id": "solana-mainnet",
            "max_total_queue_bytes": 1,
            "max_total_queue_events": 1,
            "role": { "mode": "primary", "node_id": "primary-1" },
            "spool": {
                "root": "/var/lib/blockzilla/spool",
                "max_bytes": 1,
                "segment_bytes": 1,
                "reserve_free_bytes": 1,
                "sync": { "max_delay_ms": 1, "max_unsynced_bytes": 1 },
                "full_policy": "fail_closed"
            },
            "sources": []
        }"#,
    )
    .unwrap_err();

    let ingest::IngestConfigLoadError::Validation(error) = error else {
        panic!("expected validation error");
    };
    assert!(error.issues().len() >= 2);
}

#[test]
fn shipped_primary_and_replica_examples_validate() {
    let primary =
        ingest::IngestConfig::from_json(include_str!("../config/ingest-primary.example.json"))
            .unwrap();
    let replica =
        ingest::IngestConfig::from_json(include_str!("../config/ingest-replica.example.json"))
            .unwrap();

    assert_eq!(primary.redacted_summary().role, "primary");
    assert_eq!(
        primary.redacted_summary().schema_version,
        ingest::INGEST_CONFIG_SCHEMA_VERSION
    );
    assert_eq!(primary.redacted_summary().source_count, 0);
    assert_eq!(primary.redacted_summary().enabled_source_count, 0);
    assert_eq!(
        primary
            .redacted_summary()
            .replica_listener
            .unwrap()
            .max_concurrent_requests,
        1
    );
    assert_eq!(replica.redacted_summary().role, "replica");
    assert_eq!(
        replica.redacted_summary().schema_version,
        ingest::INGEST_CONFIG_SCHEMA_VERSION
    );
    assert!(replica.redacted_summary().replica_listener.is_none());
    assert_eq!(replica.redacted_summary().source_count, 0);
    assert_eq!(replica.redacted_summary().enabled_source_count, 0);
    let ingest::IngestRoleConfig::Primary {
        node_id: primary_id,
        replica_listener: Some(listener),
        ..
    } = &primary.role
    else {
        panic!("expected primary role");
    };
    let ingest::IngestRoleConfig::Replica { upstream, .. } = &replica.role else {
        panic!("expected replica role");
    };
    assert_eq!(&upstream.expected_primary_id, primary_id);
    assert!(upstream.batch.max_events <= listener.max_batch_events);
    assert!(upstream.batch.max_bytes <= listener.max_batch_compressed_bytes);
    assert!(upstream.batch.max_uncompressed_bytes <= listener.max_batch_uncompressed_bytes);
    assert!(upstream.batch.max_compressed_event_bytes <= listener.max_compressed_event_bytes);
    assert!(upstream.batch.max_uncompressed_event_bytes <= listener.max_uncompressed_event_bytes);
    assert_eq!(upstream.expected_primary_id, "blockzilla-primary");
    assert_eq!(
        upstream.cumulative_ack_wal_file,
        std::path::Path::new("/data/replication-control/cumulative-ack.wal")
    );
}

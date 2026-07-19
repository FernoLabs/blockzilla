use hivezilla::ingest;

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
    assert_eq!(primary.redacted_summary().enabled_source_count, 2);
    assert_eq!(replica.redacted_summary().role, "replica");
    assert_eq!(replica.redacted_summary().enabled_source_count, 1);
}

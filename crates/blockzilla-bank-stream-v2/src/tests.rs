use super::*;
use crate::fixtures::*;
#[test]
fn strict_version_fields_and_counter_limits() {
    let b = block(1, 17, 7, 3, 10);
    let mut e = serde_json::to_value(&b.envelope).unwrap();
    e["event"]
        .as_object_mut()
        .unwrap()
        .remove("recent_blockhash");
    assert!(serde_json::from_value::<Envelope>(e).is_err());
    let mut e = b.envelope.clone();
    e.schema = "blockzilla-stream-envelope-v1".into();
    assert!(encode(e, &vec![42; REQUIRED.len()]).is_err());
    let mut e = b.envelope.event.clone();
    e.source_sequence = SAFE + 1;
    assert!(e.validate().is_err());
    let mut c = control(&b, 2, "confirmed", 1000).envelope;
    c.marker["files"] = serde_json::json!([]);
    assert!(encode(c, &[]).is_err());
}
#[test]
fn controls_require_the_exact_bank_payload_and_native_expiry() {
    let a = block(1, 17, 7, 3, 10);
    let b = block(2, 17, 8, 3, 10);
    let c = control(&a, 3, "confirmed", 1000);
    c.envelope.event.matches_payload(&a).unwrap();
    assert!(c.envelope.event.matches_payload(&b).is_err());
    let mut c = c.envelope;
    c.event.recent_blockhash.as_mut().unwrap().context_slot = 18;
    assert!(encode(c, &[]).is_err());
    let mut c = control(&a, 3, "confirmed", 1000).envelope;
    c.event.recent_blockhash.as_mut().unwrap().blockhash = "1".into();
    assert!(encode(c, &[]).is_err());
}
#[test]
fn processed_is_not_confirmation_and_late_root_cannot_renew_head() {
    let a = block(1, 10, 7, 9, 10);
    let b = block(3, 17, 8, 10, 11);
    let mut chain = chain::Chain::default();
    chain.add_payload(&a).unwrap();
    assert!(chain.latest.is_none());
    chain
        .apply_control(&control(&a, 2, "root", 1000), &a)
        .unwrap();
    chain.add_payload(&b).unwrap();
    chain
        .apply_control(&control(&b, 4, "confirmed", 1200), &b)
        .unwrap();
    let current = chain.latest.clone().unwrap();
    chain
        .apply_control(&control(&a, 5, "root", 2000), &a)
        .unwrap();
    assert_eq!(chain.latest.unwrap(), current);
}
#[test]
fn invalidation_removes_descendants_and_rooted_invalidation_fails() {
    let root = block(1, 9, 1, 8, 9);
    let a = block(3, 10, 2, 9, 10);
    let b = block(4, 17, 3, 10, 11);
    let mut c = chain::Chain::default();
    c.add_payload(&root).unwrap();
    c.apply_control(&control(&root, 2, "root", 1000), &root)
        .unwrap();
    c.add_payload(&a).unwrap();
    c.add_payload(&b).unwrap();
    c.apply_control(&control(&b, 5, "confirmed", 1200), &b)
        .unwrap();
    c.apply_control(&control(&a, 6, "invalidate", 1300), &a)
        .unwrap();
    assert!(c.latest.is_none() && c.reconciliate);
    assert!(c.can_confirm(&b.envelope.event.bank).is_err());
    assert!(
        c.apply_control(&control(&root, 7, "invalidate", 1400), &root)
            .is_err()
    );
}
#[test]
fn head_source_age_and_cursor_are_strict() {
    let b = block(1, 17, 7, 3, 10);
    let f = control(&b, 2, "confirmed", 1000);
    let latest = ConfirmedObservation::from_frame(&f).unwrap();
    let mut h = Head {
        schema: HEAD.into(),
        profile: PROFILE.into(),
        source: source(),
        cursor: Some(f.cursor.clone()),
        earliest_after: Some(cursor("fixture-session", 0, "-")),
        status: "live".into(),
        sampled_at_unix_ms: 2000,
        source_observed_at_unix_ms: Some(1000),
        source_lag_ms: Some(1000),
        latest_confirmed: Some(latest),
    };
    h.validate(&source(), 2000, 5000).unwrap();
    let mut wrong = source();
    wrong.session_id = "other".into();
    assert!(h.validate(&wrong, 2000, 5000).is_err());
    h.sampled_at_unix_ms = 8000;
    h.source_lag_ms = Some(7000);
    assert!(h.validate(&source(), 8000, 5000).is_err());
    h.status = "stale".into();
    h.validate(&source(), 8000, 5000).unwrap();
    h.sampled_at_unix_ms = 20000;
    h.source_lag_ms = Some(19000);
    assert!(h.validate(&source(), 8000, 5000).is_err());
}
#[test]
fn frame_digest_is_not_a_reserialized_marker_digest() {
    let f = block(1, 17, 7, 3, 10);
    assert_ne!(f.envelope.event.payload_sha256, f.sha256);
    assert_ne!(
        f.envelope.event.payload_sha256,
        digest(&serde_json::to_vec(&f.envelope.marker).unwrap())
    );
    assert_eq!(decode(f.wire.clone()).unwrap().sha256, f.sha256);
}

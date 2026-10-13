use super::*;
pub fn source() -> SourceIdentity {
    SourceIdentity {
        genesis_hash: bs58::encode([2; 32]).into_string(),
        node_identity: bs58::encode([3; 32]).into_string(),
        agave_revision: "fixture-revision".into(),
        session_id: "fixture-session".into(),
    }
}
pub fn block(sequence: u64, slot: u64, bank_id: u64, parent: u64, height: u64) -> Frame {
    let bank = BankIdentity {
        slot,
        bank_id,
        blockhash: [slot as u8; 32],
        parent_slot: parent,
        parent_blockhash: [parent as u8; 32],
    };
    let e = BankEvent {
        sequence,
        source_sequence: sequence,
        kind: "block".into(),
        bank: bank.clone(),
        payload_sha256: digest(b"original marker bytes"),
        payload_frame_sha256: None,
        commitment: Some("processed".into()),
        confirmation_basis: Some("operator-trusted-agave-processed-bank-callback".into()),
        block_height: Some(height),
        recent_blockhash: None,
        observed_at_unix_ms: 1000,
        reason: None,
    };
    let objects: Vec<_> = REQUIRED
        .iter()
        .enumerate()
        .map(|(i, p)| Object {
            path: (*p).into(),
            offset: i,
            bytes: 1,
            sha256: digest(&[42]),
        })
        .collect();
    let src = source();
    let mut marker = serde_json::to_value(&src).unwrap();
    marker["schema"] = "hivezilla-direct-agave-processed-compact-v1".into();
    marker["state"] = "COMPLETE".into();
    marker["source_profile"] = "agave-bank-callbacks-v1".into();
    marker["canonical"] = false.into();
    marker["publication_ready"] = false.into();
    marker["finalized"] = false.into();
    marker["produced_blocks"] = serde_json::json!([bank]);
    marker["files"] = serde_json::to_value(
        objects
            .iter()
            .map(|o| serde_json::json!({"path":o.path,"bytes":o.bytes,"sha256":o.sha256}))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    encode(
        Envelope {
            schema: ENVELOPE.into(),
            profile: PROFILE.into(),
            evidence_included: false,
            marker,
            objects,
            event: e,
        },
        &vec![42; REQUIRED.len()],
    )
    .unwrap()
}
pub fn control(payload: &Frame, sequence: u64, kind: &str, observed: u64) -> Frame {
    let mut e = payload.envelope.event.clone();
    e.sequence = sequence;
    e.source_sequence = sequence;
    e.kind = kind.into();
    e.payload_frame_sha256 = Some(payload.sha256.clone());
    e.observed_at_unix_ms = observed;
    match kind {
        "confirmed" | "root" => {
            e.commitment = Some(
                if kind == "root" {
                    "finalized"
                } else {
                    "confirmed"
                }
                .into(),
            );
            e.confirmation_basis = Some(
                if kind == "root" {
                    "operator-trusted-agave-rooted-bank-callback"
                } else {
                    "operator-trusted-agave-confirmed-bank-callback"
                }
                .into(),
            );
            e.recent_blockhash = Some(RecentBlockhash {
                blockhash: bs58::encode(e.bank.blockhash).into_string(),
                context_slot: e.bank.slot,
                last_valid_block_height: e.block_height.unwrap() + 150,
            });
        }
        "invalidate" => {
            e.commitment = None;
            e.confirmation_basis = None;
            e.block_height = None;
            e.reason = Some("fixture invalidation".into());
        }
        _ => panic!("unsupported fixture"),
    }
    encode(
        Envelope {
            schema: ENVELOPE.into(),
            profile: PROFILE.into(),
            evidence_included: false,
            marker: control_marker(&payload.source, &e),
            objects: vec![],
            event: e,
        },
        &[],
    )
    .unwrap()
}

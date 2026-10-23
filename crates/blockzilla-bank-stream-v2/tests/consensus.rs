use blockzilla_bank_stream_v2::{BankIdentity, SourceIdentity, consensus::*};

fn binding() -> FactBinding {
    FactBinding {
        source: SourceIdentity {
            genesis_hash: "11111111111111111111111111111111".into(),
            node_identity: "11111111111111111111111111111111".into(),
            agave_revision: NATIVE_REVISION.into(),
            session_id: "native-test-1".into(),
        },
        bank: BankIdentity {
            slot: 460_000_100,
            bank_id: 23,
            blockhash: [8; 32],
            parent_slot: 460_000_099,
            parent_blockhash: [7; 32],
        },
        execution_payload_sha256: "ab".repeat(32),
        source_event_sequence: 123,
        source_event_sha256: "cd".repeat(32),
    }
}
fn short(n: usize) -> Vec<u8> {
    let mut value = n;
    let mut out = Vec::new();
    loop {
        let low = (value & 127) as u8;
        value >>= 7;
        out.push(low | if value > 0 { 128 } else { 0 });
        if value == 0 {
            return out;
        }
    }
}
fn aggregate(out: &mut Vec<u8>, byte: u8, bitmap: &[u8], is_short: bool) {
    out.extend_from_slice(&[byte; 96]);
    out.extend(if is_short {
        short(bitmap.len())
    } else {
        (bitmap.len() as u16).to_le_bytes().to_vec()
    });
    out.extend_from_slice(bitmap);
}
// Fixed schema fixture follows Agave87167 entry/block_component.rs240–247,
//299–328 and votor-messages/reward_certificate.rs34–42,75–85. Signatures are
//deliberate test bytes; this fixture makes no BLS or mainnet-capture claim.
fn fixture() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&[9; 32]);
    bytes.extend_from_slice(&1_792_737_800_123_456_789u64.to_le_bytes());
    bytes.push(3);
    bytes.extend_from_slice(b"abc");
    bytes.push(1);
    bytes.extend_from_slice(&459_999_900u64.to_le_bytes());
    bytes.extend_from_slice(&[10; 32]);
    aggregate(&mut bytes, 0x11, &[0x80, 0xff], false);
    bytes.push(1);
    aggregate(&mut bytes, 0x22, &[0x03], false);
    bytes.push(1);
    bytes.extend_from_slice(&459_999_899u64.to_le_bytes());
    aggregate(&mut bytes, 0x33, &[0x55; 128], true);
    bytes.push(1);
    bytes.extend_from_slice(&459_999_898u64.to_le_bytes());
    bytes.extend_from_slice(&[11; 32]);
    aggregate(&mut bytes, 0x44, &[0x66], true);
    bytes
}
fn minimal() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&[3; 32]);
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes
}
#[test]
fn exact_footer_and_portable_fact_round_trip_without_duplicate_storage() {
    let wire = fixture();
    let fact = ConsensusFact::new(binding(), wire.clone()).unwrap();
    let encoded = encode(&fact).unwrap();
    assert_eq!(&encoded[..8], b"BZCNF001");
    assert_eq!(&encoded[8..12], &[1, 0, 1, 0]);
    let n = u32::from_le_bytes(encoded[12..16].try_into().unwrap()) as usize;
    assert_eq!(encoded.len(), HEADER_BYTES + n + wire.len());
    assert_eq!(&encoded[HEADER_BYTES + n..], wire);
    let decoded = decode_bound(&encoded, &binding()).unwrap();
    assert_eq!(decoded, fact);
    assert_eq!(encode(&decoded).unwrap(), encoded);
    let view = decoded.footer().unwrap();
    assert_eq!(view.bank_hash, [9; 32]);
    assert_ne!(view.bank_hash, decoded.binding.bank.blockhash);
    assert_eq!(view.block_producer_time_nanos, 1_792_737_800_123_456_789);
    assert_eq!(view.block_user_agent, b"abc");
    let cert = view.block_final_cert.unwrap();
    assert_eq!(cert.slot, 459_999_900);
    assert_eq!(cert.block_id, ConsensusBlockId([10; 32]));
    assert_ne!(cert.slot, decoded.binding.bank.slot);
    assert_eq!(cert.final_aggregate.signature, &[0x11; 96]);
    assert_eq!(cert.final_aggregate.bitmap, &[0x80, 0xff]);
    assert_eq!(cert.notar_aggregate.unwrap().signature, &[0x22; 96]);
    assert_eq!(
        view.skip_reward_cert.unwrap().aggregate.bitmap,
        &[0x55; 128]
    );
    assert_eq!(
        view.notar_reward_cert.unwrap().block_id,
        ConsensusBlockId([11; 32])
    );
}
#[test]
fn json_preserves_nanos_and_unknown_is_never_tower_or_empty() {
    let bytes = minimal();
    let view = decode_footer(&bytes).unwrap();
    let json = serde_json::to_value(view).unwrap();
    assert_eq!(json["block_producer_time_nanos"], u64::MAX.to_string());
    assert_eq!(json["verification"], "captured_not_verified");
    assert!(json.get("consensus_block_id").is_none());
    assert!(json["block_final_cert"].is_null());
    assert!(decode_footer(&[]).is_err());
    let mut unknown = bytes.clone();
    unknown[0] = 2;
    assert!(decode_footer(&unknown).is_err());
    let mut bad = bytes;
    bad[42] = 2;
    assert!(decode_footer(&bad).is_err());
}
#[test]
fn maximum_user_agent_is_bytes_not_utf8() {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&[0; 32]);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.push(255);
    bytes.extend_from_slice(&[255; 255]);
    bytes.extend_from_slice(&[0, 0, 0]);
    assert_eq!(decode_footer(&bytes).unwrap().block_user_agent, &[255; 255]);
}
#[test]
fn every_native_truncation_and_trailing_byte_is_rejected() {
    let bytes = fixture();
    for cut in 0..bytes.len() {
        assert!(decode_footer(&bytes[..cut]).is_err(), "{cut}");
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(decode_footer(&trailing).is_err());
    assert!(decode_footer(&vec![0; MAX_FOOTER_BYTES + 1]).is_err());
}
#[test]
fn source_bank_parent_payload_and_captured_event_cannot_be_rebound() {
    let encoded = encode(&ConsensusFact::new(binding(), fixture()).unwrap()).unwrap();
    for field in 0..9 {
        let mut other = binding();
        match field {
            0 => other.source.session_id = "native-other".into(),
            1 => other.source.node_identity = "SysvarC1ock11111111111111111111111111111111".into(),
            2 => other.source.genesis_hash = "SysvarC1ock11111111111111111111111111111111".into(),
            3 => other.bank.bank_id += 1,
            4 => other.bank.blockhash[0] ^= 1,
            5 => other.bank.parent_blockhash[0] ^= 1,
            6 => other.execution_payload_sha256 = "ef".repeat(32),
            7 => other.source_event_sequence += 1,
            _ => other.source_event_sha256 = "12".repeat(32),
        }
        assert!(decode_bound(&encoded, &other).is_err(), "{field}");
    }
    let mut other = binding();
    other.source.agave_revision = "00".repeat(20);
    assert!(ConsensusFact::new(other, minimal()).is_err());
    let mut other = binding();
    other.execution_payload_sha256 = "AB".repeat(32);
    assert!(ConsensusFact::new(other, minimal()).is_err());
}
#[test]
fn portable_header_lengths_versions_and_json_are_strict() {
    let good = encode(&ConsensusFact::new(binding(), minimal()).unwrap()).unwrap();
    for cut in 0..good.len() {
        assert!(decode(&good[..cut]).is_err(), "{cut}");
    }
    for index in [0, 8, 10] {
        let mut bad = good.clone();
        bad[index] ^= 2;
        assert!(decode(&bad).is_err());
    }
    for index in [12, 16] {
        let mut bad = good.clone();
        bad[index..index + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bad).is_err());
    }
    let mut bad = good.clone();
    bad.push(0);
    assert!(decode(&bad).is_err());
    assert!(decode(&vec![0; MAX_FACT_BYTES + 1]).is_err());
    let n = u32::from_le_bytes(good[12..16].try_into().unwrap()) as usize;
    let original = std::str::from_utf8(&good[20..20 + n]).unwrap();
    for json in [
        original.replacen('{', "{\"unknown\":1,", 1),
        original.replacen('{', "{\"source_event_sequence\":123,", 1),
    ] {
        let mut bad = good[..20].to_vec();
        bad[12..16].copy_from_slice(&(json.len() as u32).to_le_bytes());
        bad.extend(json.as_bytes());
        bad.extend(&good[20 + n..]);
        assert!(decode(&bad).is_err());
    }
}
#[test]
fn option_tags_and_fixed_vs_short_bitmap_lengths_are_not_interchangeable() {
    let good = fixture();
    // Tag1, hash32, nanos8, user-agent length1+3 => final option at45.
    let mut bad = good.clone();
    bad[45] = 2;
    assert!(decode_footer(&bad).is_err());
    let mut bad = good;
    bad[45 + 1 + 8 + 32 + 96..45 + 1 + 8 + 32 + 96 + 2].copy_from_slice(&65535u16.to_le_bytes());
    assert!(decode_footer(&bad).is_err());
    for short in [
        &[0x80, 0][..],
        &[0x80, 0x80, 0][..],
        &[0x80, 0x80, 4][..],
        &[0xff, 0xff, 0x80][..],
    ] {
        let mut bytes = vec![1];
        bytes.extend([0; 32]);
        bytes.extend(0u64.to_le_bytes());
        bytes.extend([0, 0, 1]);
        bytes.extend(0u64.to_le_bytes());
        bytes.extend([0; 96]);
        bytes.extend(short);
        bytes.push(0);
        assert!(decode_footer(&bytes).is_err());
    }
}
#[test]
fn fixed_schema_fixture_has_stable_digest() {
    assert_eq!(
        blockzilla_bank_stream_v2::digest(&fixture()),
        "45ec69d756fd88adf94d4ffa72ee4657b4ab74810385f8c109590722a0d8b06d"
    );
}

#[test]
fn all_optional_native_certificate_positions_and_fast_finalization() {
    for final_mode in 0..3 {
        for skip in [false, true] {
            for notar in [false, true] {
                let mut b = vec![1];
                b.extend([4; 32]);
                b.extend(123u64.to_le_bytes());
                b.push(0);
                b.push(u8::from(final_mode > 0));
                if final_mode > 0 {
                    b.extend(5u64.to_le_bytes());
                    b.extend([6; 32]);
                    aggregate(&mut b, 7, &[8], false);
                    b.push(u8::from(final_mode == 2));
                    if final_mode == 2 {
                        aggregate(&mut b, 9, &[], false);
                    }
                }
                b.push(u8::from(skip));
                if skip {
                    b.extend(10u64.to_le_bytes());
                    aggregate(&mut b, 11, &[], true);
                }
                b.push(u8::from(notar));
                if notar {
                    b.extend(12u64.to_le_bytes());
                    b.extend([13; 32]);
                    aggregate(&mut b, 14, &[15], true);
                }
                let f = decode_footer(&b).unwrap();
                assert_eq!(f.block_final_cert.is_some(), final_mode > 0);
                assert_eq!(
                    f.block_final_cert
                        .as_ref()
                        .is_some_and(|c| c.notar_aggregate.is_some()),
                    final_mode == 2
                );
                assert_eq!(f.skip_reward_cert.is_some(), skip);
                assert_eq!(f.notar_reward_cert.is_some(), notar);
                let value = ConsensusFact::new(binding(), b).unwrap();
                assert_eq!(decode(&encode(&value).unwrap()).unwrap(), value);
            }
        }
    }
}
#[test]
fn verified_official_footer_grammar_revision_is_also_explicitly_admitted() {
    let mut b = binding();
    b.source.agave_revision = OFFICIAL_REVISION.into();
    let f = ConsensusFact::new(b.clone(), minimal()).unwrap();
    assert_eq!(decode_bound(&encode(&f).unwrap(), &b).unwrap(), f);
}

#[test]
fn independently_wincode_encoded_native_schema_fixture() {
    // Produced by Wincode0.6.1 from the pinned native field declarations,
    // not by this codec's manual fixture builder. Hash/signature wrappers are
    // transparent fixed bytes. This is a schema test, not a signed live footer.
    let wire = include_bytes!("fixtures/agave-4.3.2-footer-v1.wincode");
    assert_eq!(
        blockzilla_bank_stream_v2::digest(wire),
        "32c22ddb2fc144203ebe60c1e87226443d019b103ea798ada38e413f79f98817"
    );
    let v = decode_footer(wire).unwrap();
    assert_eq!(v.bank_hash, [3; 32]);
    assert_eq!(v.block_producer_time_nanos, 1_792_737_800_123_456_789);
    assert_eq!(v.block_user_agent, b"pinned-schema-fixture");
    let c = v.block_final_cert.unwrap();
    assert_eq!(c.slot, 42);
    assert_eq!(c.block_id, ConsensusBlockId([4; 32]));
    assert_eq!(c.final_aggregate.signature, &[0x11; 96]);
    assert_eq!(c.final_aggregate.bitmap, &[0xa5, 0xff]);
    assert!(c.notar_aggregate.is_none());
    assert!(v.skip_reward_cert.is_none());
    assert!(v.notar_reward_cert.is_none());
    let f = ConsensusFact::new(binding(), wire.to_vec()).unwrap();
    assert_eq!(decode(&encode(&f).unwrap()).unwrap(), f);
}

#[test]
fn actual_captured_native_footer_matches_pinned_public_provenance() {
    let wire = include_bytes!("fixtures/captured-footer-3384311167.wincode");
    let proof: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/captured-footer-3384311167.json")).unwrap();
    assert_eq!(
        blockzilla_bank_stream_v2::digest(wire),
        proof["native_footer_sha256"]
    );
    assert_eq!(wire.len() as u64, proof["native_footer_bytes"]);
    let v = decode_footer(wire).unwrap();
    assert_eq!(v.verification, "captured_not_verified");
    assert_eq!(
        v.block_producer_time_nanos.to_string(),
        proof["producer_time_nanos"]
    );
    let json = serde_json::to_value(&v).unwrap();
    assert_eq!(
        json["block_producer_time_nanos"],
        proof["producer_time_nanos"]
    );
    assert!(json.get("carrier_consensus_block_id").is_none());
    let target = v.block_final_cert.unwrap();
    assert_eq!(target.slot, proof["finalization_target_slot"]);
    assert_ne!(target.slot, proof["carrier_slot"]);
    let mut damaged = wire.to_vec();
    damaged.push(0);
    assert!(decode_footer(&damaged).is_err());
}

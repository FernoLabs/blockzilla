use std::collections::BTreeMap;
fn main() {
    let mut counts = BTreeMap::<&str, u64>::new();
    let mut accepted = 0u64;
    let mut mismatches = 0u64;
    let mut check = |kind: &'static str, bytes: [u8; 32], expected_off: bool| {
        *counts.entry(kind).or_default() += 1;
        let a = old::edwards::CompressedEdwardsY(bytes)
            .decompress()
            .map(|p| p.compress().to_bytes());
        let b = new::edwards::CompressedEdwardsY(bytes)
            .decompress()
            .map(|p| p.compress().to_bytes());
        if a.is_some() {
            accepted += 1;
        }
        if a != b || (expected_off && a.is_some()) {
            mismatches += 1;
            eprintln!("mismatch {kind} {bytes:02x?} old={a:?} new={b:?}");
        }
    };
    for line in include_str!("../inputs.txt").lines() {
        let mut fields = line.split_whitespace();
        let kind = fields.next().unwrap();
        let hex = fields.next().unwrap();
        let expected = fields.next().unwrap() == "off";
        let mut bytes = [0u8; 32];
        for (i, x) in bytes.iter_mut().enumerate() {
            *x = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
        }
        check(kind, bytes, expected);
    }
    for p in old::constants::EIGHT_TORSION {
        let mut bytes = p.compress().to_bytes();
        check("small-order", bytes, false);
        bytes[31] ^= 128;
        check("small-order", bytes, false);
    }
    for p in new::constants::EIGHT_TORSION {
        let mut bytes = p.compress().to_bytes();
        check("small-order-new", bytes, false);
        bytes[31] ^= 128;
        check("small-order-new", bytes, false);
    }
    let mut state = 0x4d595df4d0f33173u64;
    for _ in 0..50000 {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_exact_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        check("deterministic-random", bytes, false);
        bytes[31] ^= 128;
        check("deterministic-random", bytes, false);
    }
    drop(check);
    print!(
        "{{\"old\":\"2.1.0\",\"new\":\"5.0.0\",\"comparisons\":{},\"old_accepted\":{},\"mismatches\":{},\"groups\":{{",
        counts.values().sum::<u64>(),
        accepted,
        mismatches
    );
    for (i, (k, v)) in counts.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("\"{k}\":{v}");
    }
    println!("}}}}");
    assert_eq!(mismatches, 0);
}

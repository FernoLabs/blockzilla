# Curve decompression compatibility audit

This standalone audit compares `curve25519-dalek` 2.1.0 and 5.0.0. It is not a
workspace member and adds no old dependency to the application dependency graph.
Run it only when checking a curve dependency migration; it is not a regular test.

From the repository root:

```sh
CARGO_TARGET_DIR=/tmp/blockzilla-curve-audit-target cargo run --release --locked --manifest-path docs/reference/fixtures/curve-upgrade-2026-09-06/Cargo.toml
```

Use `--offline` too when the dependencies are cached. The program checks both
acceptance and canonical recompression bytes. A mismatch prints the input and
both results, then fails. The fixed random generator seed and sign-paired corpus
are in `src/main.rs`.

`inputs.txt` contains 296 field-boundary/sign cases, 510 single-bit/sign cases,
four PDA goldens, and 1,024 PDA bump hashes. Boundary y values are 0 through 64
and `(2^255 - 19) - 64` through `2^255 - 1`, each with both sign bits. The four
PDA goldens come from the [Solana address 2.6.1 tests](https://docs.rs/crate/solana-address/2.6.1/source/src/lib.rs).
Their SHA-256 inputs are concatenated seeds, the BPF upgradeable loader address,
and `ProgramDerivedAddress`. The bump corpus adds each byte 0 through 255 after
the seeds. These fixed hashes let the comparison avoid a separate SHA dependency.

The source review found matching low-255-bit byte decoders, including the same
handling of noncanonical field representatives, and the same square-root validity
and sign-bit steps. The measured run used Rust 1.98.1 on aarch64-apple-darwin.
It is a bounded corpus check, not a proof for every possible input or every backend.
Production replay tests remain the integration check.

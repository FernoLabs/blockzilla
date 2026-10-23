# Portable native compact decoder

This independent crate reads the current native compact projection with the
existing `blockzilla-archive-v2`, `blockzilla-compact`, and primitive Rust schemas.
It uses `ruzstd` for bounded, pure Rust Zstandard decoding. It has no Agave ABI,
network service, or JavaScript host imports. The tiny owned-buffer Wasm ABI is
private to the accompanying SDK.

Build and test on the remote machine, not the operator Mac:

```sh
rustup target add wasm32-unknown-unknown --toolchain 1.98.0
CARGO_BUILD_JOBS=4 RUSTFLAGS='-C target-cpu=native' cargo +1.98.0 test --locked
CARGO_BUILD_JOBS=4 cargo +1.98.0 build --locked --release --target wasm32-unknown-unknown
```

Copy `target/wasm32-unknown-unknown/release/blockzilla_stream_decoder.wasm` to
`../client/decoder.wasm`. Use a separate target directory, low build priority,
and resource bounds on a machine running the validator. This does not install
a validator plugin or change any native capture process.

The JavaScript transport checks frame and object digests before calling the
decoder. This crate checks index geometry, signatures, native block identities,
transaction regions, dictionary references, loaded addresses, execution flags,
and instruction account positions. Native batches use zero-based produced-only
blockhash registries; the first parent is a separate 40-byte hash+slot tail.
No schema probing or canonical archive fallback is used.

The supported result is semantic compact transaction data. Exact signed wire
reconstruction, full RPC rendering, portable log expansion, and a shared stable
live dictionary are separate work. Do not present this projection as canonical
archive data or interpret absent inner metadata as proved absence of CPI calls.

## Native consensus facts

The separate `bz_decode_consensus` export reads `native-consensus-fact-v1`
through the shared Rust codec. Its configuration is the full expected
`FactBinding`, supplied independently by the caller. It returns exact leader
nanoseconds as a decimal string. Captured certificate targets remain separate
from the carrying block. This reader does not verify BLS signatures or change
the existing execution decoder's admission rules.

Copy the Wasm output to `../client/decoder-consensus-v1.wasm` for the opt-in
`consensus.mjs` reader. Keep the existing v1/v2 decoder files separate. See the
[format and delivery plan](../../../docs/architecture/alpenglow-consensus-facts.md).

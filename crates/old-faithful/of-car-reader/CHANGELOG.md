# Changelog

## 0.2.0 - 2026-08-26

### Breaking changes

- Replace `InflationParams::padding` with `InflationParams::storage` and expose
  the decoded genesis fields that occupied the former padding positions.
- Add the exact `genesis.bin` bytes to `GenesisArchive`.
- Replace the public `RawCidRef::normalized_bytes` and `RawCidRef::cbor_bytes`
  fields with accessor methods.
- Set the minimum supported Rust version to 1.96.

### Added

- Decode transaction V1 messages and their SIMD-0385 transaction config.
- Expose raw transaction and metadata frames without decoding their payloads.
- Add CAR CID helpers and reusable data buffers for lossless reconstruction.

### Changed

- Reduce buffer growth and reuse oversized zstd and CAR read buffers.
- Update `solana-short-vec` to 3.3.0 and `wincode` to 0.6.1.
- Vendor `protoc` for reliable package builds.

## 0.1.3 - 2026-05-29

- Add a vendored `protoc` binary for build environments that do not provide
  the compiler.

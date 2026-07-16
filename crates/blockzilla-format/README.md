# blockzilla-format

`blockzilla-format` is the shared Rust library for Blockzilla's archive records,
codecs, indexes, registries, and serving sidecars. The `blockzilla` CLI uses it
to build and read Archive V2; Edgezilla and examples reuse the same types.

Archive V2 is pre-1.0. Pin the Git revision used to produce an archive, and read
the [implemented format reference](../../docs/reference/archive-v2-hot-block-format.md)
before writing an independent reader.

## Scope

- `v2` defines the current Archive V2 records, constants, and block-access
  envelopes.
- `reader`, `writer`, `framed`, and `registry` provide reusable storage
  primitives.
- `program_logs` and `blockzilla-log-parser` support compact, byte-preserving
  runtime-log representations.
- `compact` and `split_compact` retain compatibility types used by existing
  readers; they are not the current public Blockzilla CLI format.

This crate defines representation and decoding rules. It does not own archive
publication, source capture, scheduling, replication, or network serving.

## Validation

```bash
cargo test --locked -p blockzilla-format
```

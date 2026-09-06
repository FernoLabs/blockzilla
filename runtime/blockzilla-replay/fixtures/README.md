# SBPF POC fixture

`relative_call_sbpfv0.so.b64` is the small `relative_call_sbpfv0.so` test
program from `solana-sbpf` 0.21.0. Its source returns `2 * input[0] + 1`; input
byte `1` therefore returns `3`.

The upstream crate is maintained at <https://github.com/anza-xyz/sbpf> and is
distributed under Apache-2.0/MIT terms. The fixture is stored as base64 so it
can be reviewed and reproduced without relying on an opaque binary edit.

# solana-ledger-compat

`solana-ledger-compat` is a minimal façade over shred-related APIs used by Blockzilla.
It lets us keep replay and ingest parsing logic in one place so we can evolve the shred
implementation without touching core replay/hive code.

## Implementations

- `blockzilla-shred`: custom parser used by Blockzilla replay and shred ingestion tooling.

## Building with the façade

Default (current implementation):

```bash
cargo build -p hivezilla
```

If you need a quick smoke-check:

```bash
cargo check -p solana-ledger-compat
```

## Quick check

```bash
cargo check -p solana-ledger-compat
cargo test -p solana-ledger-compat
```

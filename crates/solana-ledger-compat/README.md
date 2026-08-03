# solana-ledger-compat

`solana-ledger-compat` is a minimal façade over shred-related APIs used by Blockzilla.
It lets us keep replay and ingest parsing logic in one place so we can evolve the shred
implementation without touching core replay/hive code.

## Feature backends

- `own`: custom parser used by default for Blockzilla replay and shred ingestion tooling.

## Building with the façade

Default (current production) backend:

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

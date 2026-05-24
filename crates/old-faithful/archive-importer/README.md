# of-archive-importer

Legacy compatibility binary for older Blockzilla archive-conversion scripts.

New Archive V2 work should use the top-level `blockzilla` binary instead:

```bash
cargo run --release -p blockzilla -- build-archive-v2-hot-blocks --help
```

This crate remains in the workspace so existing NAS scripts and historical notes keep building while the commands are migrated.

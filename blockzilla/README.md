# blockzilla

Main CLI for Archive V2 builders, repackers, analyzers, read benchmarks, and token dump benchmarks.

The old compact V1 tools are kept only as legacy subcommands:

- `analyze-compact`
- `dump-compact-log-strings`

New work should use Archive V2 subcommands such as:

```bash
cargo run --release -p blockzilla -- build-archive-v2-hot-blocks --help
cargo run --release -p blockzilla -- bench-archive-v2-hot-blocks --help
cargo run --release -p blockzilla -- dump-token-instructions --help
```

The token dump benchmark intentionally lives here, not in the Old Faithful crate, because it compares raw CAR/CAR.ZST, block-zstd Archive V2, and raw-block Archive V2 as Blockzilla storage formats. Whole-file-zstd raw blocks stay in generic read/size benchmarks only; they are intentionally excluded from token dump analysis.

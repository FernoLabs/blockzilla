# Indexers and archive tools

| Package | Role |
| --- | --- |
| `blockzilla-user-program-index` | Build and query signer-to-program relations; includes the former Firebase command and operational tools. |
| `blockzilla-spyx-query` | Query token postings and market data. |
| `blockzilla-token-transaction-dump` | Extract token transactions. |
| `blockzilla-token-balance-audit` | Verify token balance effects. |
| `blockzilla-dump` | Build and query the dump database. |

Archive formats and readers live in `../crates/`. Old Faithful slot-index tools
stay beside their reader in `../crates/old-faithful/`.

This top-level group contains applications. The parallel `runtime/` group
contains replay work; neither is hidden inside the shared format crates.
`blockzilla-user-program-index` is the single package for the former Firebase
indexer command and signer-to-program library. Its command uses `cli`; retained
operational tools use `developer-tools`. See its
[guide](blockzilla-user-program-index/README.md) and the
[workspace map](../docs/design/workspace-restructure.md).

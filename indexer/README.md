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

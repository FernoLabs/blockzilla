# deBridge delegation candidates

This standalone Rust application uses the Blockzilla Compact V2 low-level
`ArchiveReader`. It scans every archive block row in the requested range. It
does not filter history by the intents program. A standalone SPL Token
approval can create a delegation without an intents-program invocation.

The selected delegate is the deBridge intents State PDA
`73dUbDAWUDZRfXM3DcuRgR4q74nr97MyuBt95738JnMz`. The scanner records Classic Token
and Token-2022 `Approve` (4) and `ApproveChecked` (13) instructions in successful
transactions, including CPI and Token Batch (255) children. CPI records can
include failed calls caught by a parent program. Their execution success is not
independently established. The candidate set can therefore contain additional
accounts that have no delegation. It preserves zero approvals and full unsigned
64-bit amounts as decimal text. It saves exact instruction bytes and account
indices, including Batch parent bytes. Valid trailing approval bytes are kept.
It reads transaction signatures only for matches.

These are historical candidate **token accounts**, not a list of current wallet
delegations. Use current `getMultipleAccounts` reads of these accounts to check
their delegate, delegated amount, token balance, and owner. The scanner does
not replay token state, transfers, revocations, or account closures.

## Build and pilot

The crate has its own workspace and lock file. It does not change the root
workspace. Rust 1.98 is required by its local Blockzilla dependencies.

```sh
cargo test --offline --manifest-path indexer/blockzilla-delegation-audit/Cargo.toml
cargo build --release --offline --manifest-path indexer/blockzilla-delegation-audit/Cargo.toml

nice -n 19 indexer/blockzilla-delegation-audit/target/release/blockzilla-delegation-audit \
  --archive-root /volume1/blockzilla/archive \
  --epoch-start 893 --epoch-end 1049 --start-slot 386160031 \
  --output /path/to/debridge-approvals.sqlite \
  --threads 2 --order descending --checkpoint-blocks 500 --max-blocks 10000 \
  --message-schema post-unknown-instruction-fallbacks-v1 \
  --metadata-schema unmarked-historical-compatibility \
  --source-generation-prefix OPERATOR_SOURCE_LABEL
```

The example schema flags require a matching producer record and a real-data
pilot. They are explicit admission choices. No marker or schema is inferred
from a successful transaction. `current` is the same message enum order as
`post-unknown-instruction-fallbacks-v1`. `may24-pre-unknown-fallbacks` is the
same order as `pre-unknown-instruction-fallbacks-v1`.

For `unmarked-historical-compatibility`, recorded `HAS_ERROR` transactions do
not change candidates and are skipped. Successful `err=None` metadata has the
same wire layout under both historical error grammars. Selected successful
metadata is read with the exact current-layout projector. An unflagged present
error fails the consistency check. The archive format's bounded full-value
ambiguity check is used for that error record. `current-typed-error` and
`legacy-raw-error` are also supported as explicit whole-source policies.

On x86 Linux, the current `gxhash` dependency requires its documented target
features. Use the platform's existing Blockzilla build configuration. Check
the resulting binary's glibc requirements before moving it to another host.

## HTTP range source

Use exactly one of `--archive-root` and `--gateway`. A gateway requires
`--cache`; the operator generation label is used only for local files. HTTP
source admission uses the existing `blockzilla-dump` source implementation.
It binds exact object sizes and strong ETags, caches small metadata and the
public-key registry/index, and reads block frames and selected signatures by
byte range. It does not download the complete block or signature file.

```sh
blockzilla-delegation-audit \
  --gateway http://127.0.0.1:18880 --allow-insecure-http \
  --cache /path/to/debridge-range-cache \
  --epoch-start 1050 --epoch-end 1053 --output /path/to/recent-approvals.sqlite \
  --threads 2 --order descending --max-blocks 10000 \
  --message-schema post-unknown-instruction-fallbacks-v1 \
  --metadata-schema unmarked-historical-compatibility
```

The gateway must meet the reader's existing range and object-identity
contract. A generic HTTP file server is not sufficient unless its strong
ETags, range responses, and conditional requests pass that contract. Public
gateways require HTTPS. `--allow-insecure-http` permits only literal
`127.0.0.1` or `::1`; it does not permit a public HTTP URL. URLs with credentials,
queries, or fragments are rejected.

HTTP source bindings include the admitted strong ETag object set and are
checked again at the epoch boundary. The underlying reader checks each remote
range response against its pinned identity. The scanner does not perform a
full set of network HEAD requests every 500-block commit. Local pinned sources
retain their filesystem check at every commit. The existing local binding
representation is retained for resume.

## Coverage and restart

The default order is newest epoch first. Blocks within each epoch remain in
archive row order. The default worker count is two. Ordered borrowed block
decoding uses a 128 MiB uncompressed batch budget and 32 MiB retained buffers
per worker.

The SQLite transaction commits events, new unique candidates, block receipts,
per-epoch counters, and the next block row together every 500 blocks. The
checkpoint interval can be 500 through 1,000 blocks. A final partial batch is
also committed. Restart with the same arguments to continue from the durable
row. Omit `--max-blocks` for the full run. The maximum applies to new rows across
one invocation, not to each epoch.

The operator label, source directory, schemas, reader binding, and actual
pinned file identities are saved for each epoch. A changed binding rejects
resume. The local files are pinned by filesystem identity and checked before
each commit and at each epoch boundary. This is not a fresh full-file content
hash verification of terabytes of archive data.

An exact static key space without State proves no match without metadata.
Loaded addresses are checked before making that decision for V0 transactions.
Resolved account order is static, loaded writable, then loaded readonly.
The compiled State/token filter avoids resolving every registry key.

For a relevant successful transaction, missing execution metadata, raw
fallbacks, absent CPI recording, incomplete selected instruction bytes, or an
invalid account index stops the scan with `coverage_unknown`. A raw successful
message cannot prove its account space and also stops the scan. No incomplete
block or pending event batch advances the checkpoint.

Default exports are `OUTPUT.candidates.txt` and `OUTPUT.status.json` (the
output database extension is replaced). Use `--candidates` and `--status` to
choose other paths. Status is written at each durable commit. The candidate
file is refreshed at most once per minute during a scan, and always on normal
exit, pilot exit, or failure. `candidate_export_count` records its snapshot
size. Counters come from at most one row per epoch; exports do not scan the
growing block-receipt table.

`history_complete` is true only when every requested epoch has reached its
bound final row. It certifies the scan of the available archive rows. It does
not independently prove that the archive contains every produced ledger block.
Pilot results and failed scans remain partial. Current delegations are never
claimed by this historical scanner.

## Tests

Synthetic exact-format fixtures cover standalone outer approvals, CPI checked
approvals, Token-2022, `u64::MAX`, zero amounts, failed transactions, static State
absence, missing CPI/raw metadata, exact instruction lengths, and V0 loaded
address order, valid trailing data, and Token Batch children with distinct
coordinates. SQLite tests cover atomic batch rollback, duplicate retry,
exact unsigned amount storage, source-bound restart, counters, and exports.

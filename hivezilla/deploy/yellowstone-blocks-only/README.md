# Yellowstone full-block profile

This patch adds a reduced profile to the pinned Yellowstone release. It keeps
the gRPC wire format and the data required for complete blocks. It is active on
`ubuntu@213.239.141.61` as of 2026-10-01. The validator was not restarted.

## Source and build

Use the release, revision, and Rust toolchain in [`upstream.json`](upstream.json).
The Agave validator and this plugin use Rust `1.97.1` and Geyser interface
`4.3.0`. Do not use an arbitrary compiler or another Agave interface version.
The dependency lock file is unchanged.

In a clean checkout of the exact upstream revision:

```sh
git apply --check /path/to/blocks-only.patch
git apply /path/to/blocks-only.patch
cargo +1.97.1 test --locked -p yellowstone-grpc-geyser --lib --jobs 16
cargo +1.97.1 build --release --locked -p yellowstone-grpc-geyser \
  --lib --bin config-check --jobs 16
```

All 168 library tests passed. Two added tests check bank sealing with only
required system accounts and the full-block subscription limits. The patch
also fixes an upstream check that used the account-output limit for entries.

The server build used a separate checkout and release directory. It had a
16-core CPU limit, a 32 GiB memory limit, and lower process priority. The stock
library remains available for rollback. The exact library, config checker,
and patch hashes are in [`evidence/build-manifest.json`](evidence/build-manifest.json).

## Profile

Set the top-level `hivezilla_blocks_only` flag to `true` with the patched
library. An omitted flag retains stock behavior. The mode replaces general
filter limits with these limits:

| Data | Behavior |
| --- | --- |
| Full blocks | One named subscription per client; transactions and entries allowed |
| Block account output and account predicates | Rejected |
| Standalone accounts, transactions, transaction status, deshred transactions | Rejected |
| Slots, block metadata, entries, block footers | One named subscription each per client |
| Snapshot accounts | Disabled, as in the pinned upstream release |
| Contact updates | Disabled |
| Deshred transaction callbacks and address lookup resolution | Disabled |
| Transaction, entry, slot, block metadata, footer callbacks | Kept |
| Ordinary account callbacks | Returned before payload allocation and copying |

Yellowstone must observe these four system accounts to seal each bank:

- `SysvarC1ock11111111111111111111111111111111`
- `SysvarS1otHashes111111111111111111111111111`
- `SysvarS1otHistory11111111111111111111111111`
- `SysvarRecentB1ockHashes11111111111111111111`

The patch uses the upstream account list and keeps its bank-ID-based sealing
rules. It does not remove transaction or entry completeness checks. Account
notifications must remain enabled in Agave so these four writes can arrive.
Ordinary account callback dispatch still occurs; their payloads are not copied
into the plugin's messages, queues, or replay storage.

The request filter engine remains for client and wire compatibility. This
profile rejects account predicates and separate transaction feeds. It does
not replace gRPC with a new protocol.

## Live result

The plugin was reloaded through Agave's local admin interface. The validator
kept PID `471837` and RPC health remained `ok`. The profile keeps the previous
8 gRPC runtime workers, 512 replay slots, local listeners, and compression.

- 32 finalized blocks were captured in 8.38 seconds.
- The capture contained 34,376 block transactions and 36,038 block entries.
- The 30.01-second sample skipped 348,227 ordinary account updates and kept
  464 required system-account updates: **99.87% of account updates skipped**.
- It avoided copying 1,662,079,985 account data bytes, about **55.4 MB/s**.
- The message queue was empty at both sample points. The transaction-mismatch
  counter and untracked-slot drop counter were zero in the deployment check.

The byte rate measures avoided payload copying inside the plugin. It is not a
CPU speedup factor or a reduction in client network traffic. The capture time
must not be compared with an earlier live capture as a throughput benchmark:
the live workload and replay backlog were different.

The metrics endpoint at `127.0.0.1:8999/metrics` adds:

```text
hivezilla_blocks_only
hivezilla_account_updates_skipped_total
hivezilla_account_bytes_skipped_total
hivezilla_account_updates_kept_total
```

These counters reset when the plugin reloads. Skipped account updates are
intentional exclusions, not lost block records.

The exact sample and capture seal are in [`evidence/`](evidence/). The WAL
remains on the server. No archive verification or R2 publication was run.
The capture observed no footer or parent-update events, so it does not prove
those paths or full-epoch coverage.

## Active files and rollback

The prepared root is `/home/ubuntu/live/hivezilla-validator-20260930`.

| File under the prepared root | Purpose |
| --- | --- |
| `releases/yellowstone-blocks-only/libyellowstone_grpc_geyser.so` | Patched plugin |
| `releases/yellowstone-blocks-only/config-check-linux-amd64` | Matching config checker |
| `releases/yellowstone-blocks-only/source/` | Exact checkout and build |
| `config/yellowstone-blocks-only.json` | Validated reduced profile |
| `config/yellowstone.json` | Same profile, saved atomically for future service starts |
| `config/yellowstone-stock-20261001.json` | Original config and library path |

The active plugin name is
`yellowstone-grpc-geyser-16.0.0+solana.4.3.0+bfd1d7e-modified`.
Use `agave-validator --ledger <prepared-root>/ledger plugin list` before any
later change, since the loaded name can change with a build.

To roll back, reload that exact loaded name with the stock config. If a reload
fails after unloading the plugin, use `plugin load <stock-config>` instead.
Restore the stock config atomically to `config/yellowstone.json` after a
successful rollback so the next service start uses the same profile. Never
overwrite a library that the validator has loaded.

Plugin reload causes a brief stream gap and resets the replay buffer. The
bounded capture began after the new profile was active; it does not cover the
reload gap. Use the continuous systemd service described in the
[Tunnel operations guide](../../../docs/architecture/hivezilla-validator-tunnel.md).
The original finite canary start command checks stock release hashes and is
not a deployment path for this custom plugin.

Agave creates optional notifier stages at validator startup. The running
notifier stops calling this plugin's deshred callback and stops address lookup
resolution after reload. A stage created at the original startup can remain
present until a later validator restart. Do not claim that all upstream
deshred processing has stopped during this run.

## Raw shreds

This release has no raw-shred Geyser callback. Deshred transaction callbacks
contain decoded transactions before execution; they are not raw shred bytes.
The pinned source implements a separate `SubscribeDeshred` transaction feed,
despite its README saying that the method is unimplemented. This reduced
profile disables that feed's transaction callbacks and subscriptions. See the
pinned [server implementation](https://github.com/rpcpool/yellowstone-grpc/blob/bfd1d7ee548081b6e36d4238d5ce8f34c94c4fb6/yellowstone-grpc-geyser/src/grpc.rs).

A raw-shred stream needs a separate TVU capture or a validator hook. Preserve
the received shred bytes and source evidence there. Keep it separate from the
finalized execution stream, then use the Hivezilla segment writer outside
validator callbacks. Do not open a shred port or advertise a stream before
that producer exists.

# Hivezilla direct Agave engine

This engine reads Agave execution callbacks. It has no gRPC client, replay buffer,
RPC retry loop, or network connection in the capture path. Agave executes the
transactions. Hivezilla keeps those results and builds Compact V2 blocks from them.
It does not execute transactions a second time.

## Components

- `hivezilla-direct-agave build-processed SESSION OUTPUT --blockzilla PATH --follow`:
  the current live worker. It converts complete banks before root and stores
  an ordered `feed.sqlite` event log. Four bounded workers prepare payloads and
  custody evidence. One writer commits events and assigns resume positions.
  Competing banks at one slot remain separate. `root` and `invalidate` records
  carry no second block payload. Rooting selects the existing payload in the
  archive catalog. Later root and footer evidence stays separate. Retirement
  requires a rooted custody proof that binds the payload and source bytes.
- `libhivezilla_direct_agave.so`: Agave Geyser plugin. The interface and transaction
  status crates use revision `825efd18292aff6ffcf9daa0f7612f21b3531a72` (4.3.0).
- `hivezilla-direct-agave build SESSION OUTPUT --blockzilla PATH --follow`:
  independent compact worker. The default batch is 32 blocks; the worker also
  limits source bytes per batch. It changes output directories at epoch boundaries.
- `blockzilla build-direct-agave-compact NORMALIZED OUTPUT --level 1`:
  the existing Compact V2 writer with a new direct-source receipt.

The plugin name is `hivezilla-direct-agave-v1`. Build with `--features plugin`.
A plugin must use the same Rust compiler and dependency types as its validator.
A successful build alone does not establish dynamic ABI compatibility. Test a
bounded load beside Yellowstone before continuous use.

## Durability and failure contract

Callbacks copy the transaction wire bytes, all native status metadata fields,
entries, block metadata, bank status, bank invalidations, and footer bytes into a
bounded queue. Queue acceptance is **not** a durable acknowledgement. A dedicated
writer commits batches to SQLite with WAL and `synchronous=FULL`. A consumer reads
only committed rows. The durable sequence changes only after that commit.

Queue limits, disk quota, disk reserve, conversion errors, and write errors latch
capture FAILED. A reserved failure file and validator error log show the failure.
The plugin does not silently resume after a dropped callback. It does not stop the
validator. Source coverage is incomplete after any such failure. On an unclean
process exit, the session has no CLOSED marker and its live lock is released.
The status command reports an error. A new session records previous unclean
sessions. No durability claim covers events that were still in memory at a crash.

One capture process owns the source root. One compact worker owns an output root.
The worker never holds a capture lock or modifies the source database. A stopped
or slow worker cannot stop the journal writer until the source disk limits are
reached. Source event payloads use bounded zstd compression. The worker retains
the full source evidence in each compact output before it requests retirement.
The capture writer checks that receipt and deletes only the exact committed
rooted bank records, in bounded FULL transactions. It acknowledges retirement
only after durable deletion. Other forks, bankless records, and late events stay
in the source journal. SQLite reuses free pages. This bounds the active bank
backlog when the worker keeps up; it does not make archive storage unlimited.
Compact objects, audit receipts, startup records, and other fork records still
grow. Set storage retention and upload policy before a long run. A disk reserve
or journal quota failure is visible and cannot silently resume.

## Block acceptance

The processed worker uses `(slot, bank_id)` keys within one source session. It
requires a processed or rooted status, block metadata, dense transaction and
entry indexes, exact entry ranges, signatures, and the final entry hash. It does
not wait for root. A bank can later be invalidated. Published execution bytes
cannot change. This gate is for the pinned Agave/Tower build. Alpenglow footer
and consensus-specific completeness rules are not yet implemented.

The legacy finalized worker requires a rooted callback, block
metadata, every transaction index, every entry index, exact entry transaction
ranges, and the final entry hash. It rejects conflicting duplicates, cleared
banks, and contradictory dead evidence. It checks parent slot and parent hash
between every output block. Incomplete roots at the load boundary are recorded
as a partial start; an incomplete later root stops forward publication.

The worker saves the full source event evidence beside each compact batch.
Compact V2 is a projection: native absent/present token and reward fields,
commission basis points, message hashes, and footer data remain in that evidence.
A batch becomes visible through an atomic directory rename. Only after its
manifest and object hashes pass does the worker save its cursor. A durable
pending-batch record preserves the exact range across a worker crash. An
existing output must pass the same checks before the cursor can advance.
Incremental source audit checks late callbacks against durable per-bank commits.
If the journal copy was retired, the audit reads the retained source evidence.
Conflicting transactions, entries, metadata, roots, and bank invalidations stop
the worker. Normalized scratch is removed only after the compact commit.
Retained source evidence is zstd JSON; the receipt binds its original bytes and
digest as well as the compressed object digest.

The result has `canonical=false` and `publication_ready=false`. Finality means
trust in this validator's rooted bank callback. It is not independent consensus
proof. PoH structure is checked; PoH hashes and transaction signatures are not
recomputed. The mainnet epoch schedule is currently explicit and genesis-bound.

## Explicit verification of stored native output

`blockzilla verify-archive-v2-poh CHUNK --native-agave --threads 1` recomputes the
entries in `available-poh.wincode`. It checks the normalized receipt and compact
commit marker, actual compact headers, blockhash registry, predecessor tail,
transaction order, exact entry signature ranges, every entry hash, and the final
blockhash. Empty signature files are valid for chunks with only ticks. Missing
entries, changed receipts, mismatched parents, and partial results fail the check.
It does not create a canonical `poh.wincode` file or modify compact output.

For a closed epoch, use `deploy/direct-agave/verify_epoch.py` from the repository
root. Its `--output` argument is the session output directory. Supply `--epoch`,
`--verifier`, and a separate `--state` directory. It first compares the full
committed inventory with chunk receipts and registries. It checks every parent
link across chunks and the retained predecessor and successor at epoch boundaries.
Then `--workers N` runs N independent chunk checks, each with one PoH worker.
`--continuity-only` runs only the first phase.

The result database saves completed chunks outside the source tree. A restart
checks the source inventory again and resumes accepted results. `STATUS.json`
shows progress, `CONTINUITY.json` records the first phase, and `RECEIPT.json` is
written only after every chunk passes PoH verification. Failures remain explicit.
Run this work at low priority with CPU and memory limits beside the live validator.

These checks establish consistency of the stored native projection. They do not
independently establish consensus, archive coverage, complete raw shreds, the tick
schedule, or Ed25519 signature validity. Preserve `canonical=false` and
`publication_ready=false`. Source event evidence and existing markers remain
unchanged; verification reports are separate records.

## Raw shreds

Geyser does not supply raw shreds. The existing Agave receive hook remains a
separate source. These compact batches do not claim complete shred evidence.
A full raw-shred retention and FEC validation path is still required to attach
canonical shred evidence. Never label deshred transaction callbacks as shreds.

## Configuration

See `config.example.json`. Use a private absolute library and output path. Check
`hivezilla-direct-agave status SESSION` in service monitoring; an active systemd
process does not establish a healthy capture. Keep Yellowstone active for the
first bounded load and compare live rooted blocks before extending the run.

## Tested build and integration

The live Agave 4.3.0 executable uses Rust 1.97.1. This crate has its own workspace
and lock file because the main CLI pins a different short-vec version. Build on
the server with `cargo +1.97.1 build --release --features plugin --locked
--ignore-rust-version`. The last flag permits shared internal crates that declare
workspace MSRV 1.98; the actual plugin still uses the live compiler.

For the full test run, set `BLOCKZILLA_TEST_BINARY` to the tested direct compact
CLI and run `cargo +1.97.1 test --features plugin --locked --ignore-rust-version`.
The epoch test invokes that real CLI and requires this path. Tests cover source
limits, compression bounds, native metadata, bank and entry checks, durable
retirement, late conflicts, pending batch recovery, and epoch changes. See the
handoff file for actual live results and limits.

# Direct Agave compact engine work, October 2

Historical handoff: the notes below describe an earlier stage. The source-copy
approval block was later resolved by the user. The native engine was built,
tested, and deployed. Yellowstone is stopped. Do not use the old pending approval
or uncompiled status below as current state. Read
[`blockzilla-goal.md`](blockzilla-goal.md) for the owner's current architecture
direction and `hivezilla/deploy/direct-agave/README.md` for the later deployment.
Check live machine reports for current health.

The user requested a real direct Agave compact block builder and then requested
the big model. GPT-6 Astra subagent `/root/direct_agave_engine`, xhigh, owns
`hivezilla/direct-agave`. It implements native bank-aware callback capture,
SQLite FULL-WAL journal, independent compact worker, durable cursor, bounded
queues and byte-sized batches, explicit failures, and epoch output directories.
The code and eleven engine tests are written but have NOT been compiled or run.
Read the crate README and subagent handoff before continuing.

Remote source transfer by the subagent was rejected twice by automatic review.
The review required trusted human approval for the exact source transfer to
`ubuntu@213.239.141.61`. Earlier direct user authorization for that host was
provided, but the child review did not accept it. Do not work around this block.
An asynchronous question is pending: May we copy the new plugin source to
that host, build, and test it there? Wait for the user's answer before doing
that transfer. No private credentials or identity key are part of the source.

Parent implemented `blockzilla build-direct-agave-compact INPUT OUTPUT --level 1`.
Files: `blockzilla/cli/src/archive_v2/direct_agave.rs`, small shared validator
refactors in `execution_canary.rs`, module and CLI registrations. Three new
tests and five existing execution-canary tests PASS remotely. The final
release binary is staged at:

`/home/ubuntu/live/hivezilla-validator-20260930/releases/direct-agave-cli-20261002/blockzilla`

SHA256: `18ee69b6694a5ae7626bac1dc1a1e7415d75ccfd65d1345def3f71437e38c2b1`.
Build/test unit: `hivezilla-direct-cli-entry-release-20261002` succeeded.
The active collector binary and validator have NOT been replaced or restarted.

CLI input: `DIRECT-AGAVE-NORMALIZED.v1.json`, exact strict schema
`hivezilla-direct-agave-normalized-v1`, source profile `agave-bank-callbacks-v1`,
canonical false, finalized true, session_id, pinned agave_revision, identities,
epoch/range, block rows and file identities. Required input files:
`blocks/live-no-registry-blocks.bin`, `index/blockhash_registry.bin`,
`index/signatures.bin`, `evidence/source-events.json`,
`evidence/available-poh.wincode`. Per input file bound 1 GiB; output 512 MiB.
Native source and available PoH are copied into the compact output. The CLI
checks geometry, source identity, signatures, counts, parent links, and entry
geometry; read-back audit is skipped per the user. It atomically writes
`DIRECT-AGAVE-COMPACT.v1.json`, canonical/publication_ready/archive_verified false.
It does not pretend to have gRPC capture or RPC coverage provenance.

Next gate after transfer approval: compile core and plugin against the exact
live Agave source revision and live compiler; run the eleven engine tests,
then build the plugin library. Check compiler ABI compatibility: root MSRV
is 1.98 while the Yellowstone live build reported rustc 1.97.1. Rust trait ABI
requires care; do not assume a plugin compiled with a different compiler is
safe to load. Verify Agave's own compiler and dependencies.

Read-only checks confirmed live Agave plugin management works:
`agave-validator --ledger ROOT/ledger plugin list`, `plugin load CONFIG`,
`plugin unload NAME`. Only Yellowstone is currently loaded. New plugin name
is `hivezilla-direct-agave-v1`. A proposed bounded side-by-side test uses
2 GiB journal, 128 MiB callback queue, 16 MiB max event, 500 GiB disk reserve,
10 ms commit interval, about 90 seconds to pass partial-start roots. Keep
Yellowstone active, monitor RPC health, unload on failure, and compare real
rooted blocks before any permanent activation. No live load was attempted.

Raw shreds still use the old separate receive hook. Its 64 GiB capture hit its
quota at slot 452312718 and does not cover the epoch gap starting 452399584.
The new Geyser execution plugin does not itself preserve complete raw shreds.
Source journal retirement and upload policy remain untested. Do not describe
this as unlimited storage, canonical full-epoch capture, or a finished public
stream service. R2 bucket and credentials remain unknown.

Current validator PID590165, old stuck epoch collector PID664124. At
18:11:45 UTC free disk was 1797 GiB. Old collector remains stuck at 95,488
blocks, all compacted. Its last successful chunk745 sealed at20:49:53UTC Oct1;
coverage finished20:49:57. Next attempt was not started until21:13:59,
already4904slots behind the oldest replay. No kernel OOM log in that period.
Cause of the24-minute pause is not established. Shared capture/compactor mutex
and checkpoint I/O under lock is a design hazard; new journal/worker must not
share blocking compaction operations with source capture.

No git commits or pushes. Preserve unrelated dirty project files. No local
Cargo build; remote builds must use bounded resources and the authorized host.

# Current Blockzilla goal

Updated: 2026-10-13, Europe/Paris.
Source: the owner's explicit architecture statement in this chat.

This is the current product direction. It does not certify that the target
pipeline, public stream, cloud publication, or database is complete.

## 1. Replay and ingestion

The target flow is:

**gossip → shreds → replay → blocks → compact**

- Obtain shreds from the Solana network. The owner describes this entry point
  as gossip. The current receive hook records Turbine and repair packets.
- Replay transactions with a greatly reduced Agave build first. A Blockzilla
  minimal runtime is a possible later step.
- Build complete blocks and compact them directly into the Blockzilla format.
- Use the same pipeline for historical replay from genesis and new live blocks.
- Hivezilla is the ingestion, replay, and compact block builder engine.
- Direct validator integration is the current development path. Recovery and
  historical replay are part of the engine goal, not a separate gRPC product.

## 2. Historical queries

- Serve old read-only data and queries through Cloudflare.
- Keep a small number of dedicated machines for large query indexes, including
  the indexes required for `getTransaction`.
- The owner's sizing estimate for the main index is about 200 GB. It should fit
  in RAM on the machine supplied by the Foundation.
- Use Cloudflare storage and its CDN to distribute immutable data close to users.
- Reduce provider egress and infrastructure costs. Very low egress cost is a
  design target; this memory does not record a measured price or guarantee.
- Put engineering effort into storage formats, indexes, and fast reads.

## 3. Live stream and client framework

- New requested contract, 10 October: implement a separate opt-in
  `native-bank-compact-v2` feed for Ferno. Keep all v1 contracts. Capture exact
  bank confirmation and blockhash-queue expiry in the pinned Agave host; never
  infer confirmation from processed state or time. Reuse each existing processed
  compact payload and send small confirmed/root/invalidation controls. Supply
  an atomic, truthful confirmed head with source age and exact replay boundaries.
  Retain control payload dependencies, use backend Bearer headers, and report
  gaps. The owner's complete build request and acceptance list are saved in
  `docs/blockzilla-confirmed-stream-spec.md`; its JSON contracts are under
  `docs/contracts`. The target remains separate from the measured result below.
  Provider update, 10 October: the separate v2 profile is now live on Foundation
  with the actual patched Agave host and native plugin. The host revision is
  `d688dd0e3a576b048a848e5faf2e8d277f8feb6e`; the tested source session is
  `000000000000000018dd432f0e8f799c-282459`. Exact native confirmation and queue
  expiry, complete payload reuse, bounded replay, backend Bearer routes, and
  atomic head are implemented and tested. V1 contracts remain separate. The
  native ABI change required validator restarts; failed startup and old-host
  sessions remain excluded and retained. Later transport changes kept PID
  2630745 and the same source. The 11-minute Rust collection applied 9,091
  consecutive unique events and decoded 3,099,103 transactions. Consumer restart
  and an eight-second publication pause passed; about 32.4 minutes of replay
  remained available. Callback-to-consumer p95 was 5,387 ms across the whole
  test and 1,563 ms over its final 120 seconds. The proposed 250 ms target was
  not met. JavaScript finite decode/resume passed but did not reach hash
  readiness with full-detail decode. Ferno adapter/wallet/send acceptance,
  sustained JavaScript capacity, a second relay, and full history recovery
  remain unfinished. On 12 October, rooted native compact/custody cloud storage
  and local retirement were enabled after exact remote byte and restore checks.
  This does not prove sustained capacity or complete native epoch coverage. Read
  `hivezilla/deploy/bank-stream-v2/ACCEPTANCE.md` and current server reports for
  the evidence and operating limits. Provider correctness does not establish
  Ferno application readiness or a full uninterrupted epoch.
- Priority confirmed by the owner on 2026-10-04: focus on live stream work now.
  Prepare a separate Rust relay on a second machine, with one upstream feed and
  shared fan-out. Both SSE and HTTP binary delivery for browsers and Node.js are
  required. Reliable QUIC is a candidate native transport; select it by measurement.
  Filtering, subscriptions, and hooks remain local. The relay machine is not yet
  identified. See `docs/architecture/hivezilla-compact-stream-relay.md` for the
  proposed connection direction, access control, checkpoint rules, and measured
  network sizing. A local pilot and a first JavaScript SDK with compact
  transaction decoding are implemented and tested as described below. A public
  private-beta stream runs through the Foundation tunnel. The second relay
  machine, archive recovery, and sustained capacity checks remain unfinished.
- Decision changed by the owner on 2026-10-10: the live stream must send complete
  processed blocks, without waiting for finality. This replaces the earlier
  finalized-first decision of 2026-10-03 and the proposal for two commitment feeds.
  Compact each complete bank once, send its compact payload immediately, and
  reuse that payload when the bank is selected for finalized history. Do not
  replay or compact it again merely because finality changes. Do not send a
  second full block on finalization; transport retries can still repeat bytes.
  Finality selects archive catalog entries and controls retention. Save later
  finality evidence separately from the immutable execution payload. Archive
  catalogs, query indexes, and upload remain work after the payload is built.
  Preserve competing bank identities and parent links; clients must be able to
  distinguish candidates and learn that a published bank was invalidated.
  Small control records are separate from full block delivery. Filtering and
  application rollback remain local. Use an event sequence for resume, since
  several events or banks can share one slot.
  This behavior is now implemented and tested in the explicit
  `native-processed-compact-v1` profile. The public feed changed to that profile
  on 10 October. Rooted custody selects the existing payload and permits exact
  native source retirement. The worker has one ordered event writer and four
  parallel bank builders. Rust and JavaScript clients handle small controls.
  The token SQLite indexer rejects this feed until application undo is built.
  R2 stores the processed frames in a separate namespace. Local compact/evidence
  retirement after upload, full archive query integration, automatic session
  changes, and sustained capacity checks remain unfinished. The validator did
  not restart. Earlier failed captures and their gaps are preserved.
  Later on 10 October, the owner requested URL-key access for demos and a free
  JSON blockhash stream. The public relay now accepts `?api_key=KEY` on private
  read routes. Header access remains supported; the mTLS origin remains unchanged.
  A free `/v1/blockhashes` SSE view sends small JSON bank identities and root or
  invalidation records. It contains no transactions or compact payloads. `/demo`
  shows that view in a browser, with pause/resume and a 100-message display limit.
  Public requests have a separate limit of eight. Metadata encoding is shared
  between readers. Both access options are off by default in the relay code.
  Twenty-two Rust tests, seven public access/resume checks, and real browser
  message flow and pause/resume passed. Only the relay was restarted; capture
  remained active. Evidence is under `hivezilla/stream-relay/evidence/demo-access-20261010`.
  The owner's later request reduces the actual public JSON. A block contains
  only slot, blockhash, and parent_blockhash; root/invalidation contain slot and
  blockhash. The event name and exact resume ID appear once in the SSE envelope.
  The demo shows all remaining SSE lines, without a reduced display projection.
  The private binary feed retains full native bank identities. Parent-hash mismatch
  alone does not prove that a competing processed bank is invalid.
  Alpenglow merges confirmed and finalized commitment, not the processed stage;
  bank identity and invalidation handling remain necessary. See the official
  [Alpenglow migration guide](https://solana.com/upgrades/alpenglow).
- Supply one read-only stream of complete blocks in an extremely compact format.
- The block content is identical for all consumers.
- Move away from the current provider-side gRPC transaction filtering model.
- The provider relays compact blocks efficiently. It does not run a different
  transaction filter pipeline for each client.
- The Blockzilla client framework runs filtering, subscriptions, hooks, and
  WebSockets locally, on infrastructure controlled by the client.
- Clients should not need to operate a large backend merely to receive and
  filter the provider's large gRPC feed.
- The objectives are lower transport cost, simpler scaling, and faster delivery.
  These objectives must be measured on real workloads.

## Long-term product

Publish Blockzilla as a specialized database for the Solana ledger. An archive
service is one use of it. The product should be an alternative to general stacks
such as ClickHouse or RocksDB for this ledger workload.

Use managed read-only distribution and a few strong index machines to reduce
bare-metal operations. Invest the saved time and money in the software.

The owner reports firsthand knowledge that Triton's stream business loses money,
uses large bandwidth charges to offset costs, and requires substantial bare-metal
operations. This is owner-provided motivation, not a verified competitor metric.

## Current implementation boundary

Recorded implementation status, with dates below:

- Earlier update, 10 October: the public processed feed and Foundation R2 frame
  publisher are connected. The validator remains on the same process. The native
  plugin captures complete bank data; the worker converts banks once, then emits
  small root/invalidation records. Resume uses an event sequence. The Rust probe
  and public JavaScript SDK received live processed data. Two public blocks'
  1,178 and 1,127 ordered first signatures and parent identities matched local
  finalized RPC. Native
  reuse/restart tests and transport/decoder tests passed. This does not establish
  a full epoch, application undo, full archive reads, Alpenglow readiness, or
  sustained capacity. The reserve is now 400 GiB. With owner approval, the old
  epoch-1000 CAR input was removed on 10 October: 767,389,334,224 bytes. Free
  disk space after removal was 1,249,992,630,272 bytes (about 1.25 TB). Benchmark
  reports, cache metadata, slot ranges, native files, and comparison results
  remain. The cleanup receipt is in the processed-stream deployment evidence.
  Old capture failures remain visible.
- The following dated entries retain the earlier implementation history.
- A non-voting Agave node and the native Hivezilla plugin run on the Foundation
  machine, `ubuntu@213.239.141.61`.
- The plugin captures bank-aware native execution callbacks and entries. A
  separate worker writes compact blocks from the durable local journal.
- A separate receive hook records raw shreds. Recording packets alone does not
  prove complete shred coverage or replay capability.
- The reward adapter now handles all six reward types in the pinned Agave build.
- Live compact output is working, but complete genesis replay, recovery of all
  missing intervals, and a full uninterrupted epoch have not been proved.
- The overnight epoch-1048 native gap spans slots 452735969 through 452861383.
  A separate local RPC backup saved 85,260 blocks and 90,259,368 transactions.
  Its older 40,123-slot prefix was outside RPC retention. The compact gap has
  not been rebuilt. RPC backup data and native source keep separate provenance.
- R2 upload and the public shared compact-block stream are not yet connected in
  this deployment. Storage remains finite.
- On 2026-10-04 a separate read-only Rust origin and Rust relay were tested and
  started on the Foundation machine as a loopback pilot. The relay initiates
  one HTTP/2 connection with mutual TLS. It supplies binary HTTP, SSE notices,
  and finite identical chunks with API-key headers, bounded replay, and durable
  restart cursors. It does not change native validator callbacks or retention.
  Eleven Rust tests, nine JavaScript unit tests, ten process-level checks, and
  ten live consumers passed. Real Chrome and Node.js received five identical
  chunks through binary and SSE. The transport helper returns compact bytes,
  decode dependencies, and block identities; compact transaction decoding is
  not implemented in that helper. Public routing, a second relay machine,
  archive backfill, QUIC comparison, certificate renewal, and automatic source
  session changes remain unfinished. The feed preserves canonical=false and
  publication_ready=false. See `hivezilla/stream-relay/README.md` and evidence.
- Later on 2026-10-04 the owner confirmed a simple full compact feed first, with
  client-side filtering. Keep the optimized dictionary/index stream and selective
  GET requests as a separate later SDK source. The first shared Rust/Wasm semantic
  decoder and browser/Node SDK are now implemented. Ten Rust tests, fourteen
  JavaScript tests, real Chrome and Node in binary/SSE modes, and independent
  finalized RPC checks passed for four blocks and 4,461 transactions. Program
  filters matched 338 transactions; account filters matched 914. All 658 failed
  executions remained distinguishable. Checkpoint restart and repeated decoder
  buffer ownership were tested. The SDK defaults to small transaction records;
  compact message/metadata details are optional. It still receives the full feed,
  binds keys to chunk-local dictionaries, and does not reduce wire traffic by
  filtering. Complete RPC rendering, exact signed-wire reconstruction, a stable
  live epoch dictionary, and a supported public publication contract remain
  unfinished. The SSE server still sends notices plus chunk GETs; inline events
  with blockhash and transaction signatures remain a proposed receipt surface.
- On 2026-10-04 the owner explicitly requested removal of previous-epoch shreds,
  because the priority is the epoch that can currently be compacted. The cleanup
  removed 1.11 TB of sealed old raw files and retained epoch 1049, including the
  mixed boundary segment. Old raw-retention manifests are now historical and
  cannot prove that deleted old-gap replay input is present. Native compact
  output, journals, ledger, snapshots, and RPC backups were retained. This was
  a one-time authorized deletion, not a standing automatic deletion policy.
  The recorder's current cumulative quota counter does not decrease after
  deletion; retained-byte quota accounting remains unresolved.
- Later on 2026-10-04 the owner requested a simple continuous token index in
  SQLite, with recovery after failure and backfill from Blockzilla. The Rust CLI
  now supplies `index-tokens` and read-only `token-db`. Balance observations,
  transaction receipts, block identities, and the chunk cursor commit together.
  Network failures retry; expired feed positions recover through a saved head
  from published Blockzilla history or retained native compact files. Parent
  slot/hash gaps stop indexing with a BLOCKED job; no missing interval is skipped.
  Nine CLI tests and eleven native decoder tests passed. Ten live blocks matched
  11,638 RPC transaction receipts and 49,722 balance records. Cloudflare epoch 900
  reads and restart yielded six blocks and 10,326 balance records. A native test
  recovered 292 historical blocks, then two live blocks. Independent RPC checks
  matched six of those blocks, including all 6,788 receipts and 2,001 USDC balance
  records. A missing-range test retained its checkpoint, and a partial-recovery
  test proved that a short-run block limit does not hide the recovery failure.
  The separate USDC demo service runs on the Foundation machine and starts at
  boot. Its SQLite file is `ROOT/indexes/token-demo/usdc.sqlite`; its unit and
  evidence are in `hivezilla/stream-relay`. This remains a private development
  consumer of the same noncanonical native projection. The public sample archive
  does not supply the current live epoch. Public routing, automatic source-session
  changes, database retention, and a complete token-account state view remain
  unfinished. See `docs/start/token-indexer.md` and the deployment record.
  The native health check also reported raw shred recording stopped at its
  cumulative quota while compact output continued. Recorded old raw-retention
  evidence cannot establish coverage after that stop. Consumer compact-file
  recovery does not repair raw shred gaps or prove full-epoch capture.
- A later live check at 20:25 Europe/Paris on 2026-10-04 found the origin/relay
  active and the compact worker about three slots behind finalized capture.
  The token consumer remained active but was BACKFILLING, about 4,880 slots
  behind compact output. Over 15 seconds its checkpoint advanced 41 slots while
  compact output advanced 53; its SQLite file was about 18.9 GB. Sustained live
  indexer throughput is therefore not yet established. Do not present the earlier
  finite recovery test as proof of long-run catch-up capacity. Measure the cause
  before changing resource limits or SQLite processing. Raw recording remains
  stopped at its quota, and current-epoch R2 publication remains unconfigured.
- The current full validator is a development starting point. It is not yet the
  reduced Agave or minimal runtime described above.
- On 2026-10-05 the owner requested a separate Hetzner stream relay and live
  publication from the Foundation machine to Foundation R2. The separate host's
  access details remain unknown. The live check found the origin FAILED with
  `attempt to write a readonly database`, the relay FAILED, and the token
  consumer stopped. The source reader now retries transient SQLite failures
  at the same position. Stream and native-history units permit updates only to
  the audit database's shared lock metadata; database rows, WAL, and compact
  files stay read-only. The origin and relay were restarted into a new relay
  spool. The old failed spool and consumer checkpoint remain. The token consumer
  resumed native-file backfill but still falls behind live capture.
- The new Rust `hivezilla-native-stream-publisher.service` is enabled on the
  Foundation machine. It publishes exact native feed frames and ordered receipts
  to `solana-archive-data/native-stream/direct-agave-live-v1/SESSION`, from slot
  453600000, the start of epoch 1050. It continues across later epochs of the
  selected capture session. It retains the native projection limits:
  canonical=false, publication_ready=false, evidence_included=false. It saves
  an intent before upload and commits the cloud head and local checkpoint only
  after the full batch is stored. Up to 16 upload workers handle at most 64
  frames and 64 MiB per batch, under a one-core CPU limit and a 512 MiB memory
  limit. Native source files stay local. Sixteen Rust checks, nine JavaScript
  checks, and ten process checks passed. The real parallel upload saved 84
  blocks in 18.61 seconds, about eight times the earlier serial byte rate.
  A full remote frame matched all 12 retained source files. At 14:16 UTC,
  1,623 blocks and 887,023,581 bytes were published; upload was 1,845 slots
  behind compact output and was catching up. Origin and relay were LIVE at
  slot 453603467. The token consumer was still 156,076 slots behind. Validator
  PID 1106933 and compact worker PID 4105319 did not change. The old native
  tar snapshot backup stays disabled, and the separate CAR archive upload is
  unchanged. Public R2 reads, R2 SDK backfill, a separate Hetzner relay, source
  session changes, and full epoch coverage remain unfinished. See the stream
  README and native deployment record. Check live reports for current progress.
  At the later 14:20 UTC check, 3,249 blocks and 1,657,039,933 bytes were published;
  upload lag had decreased to 1,137 slots. A real publisher restart retained its
  namespace and checkpoint, then advanced sequence 1744 to 2320. Four fresh
  chunks matched across binary HTTP, SSE with GET, and direct chunk GET. The
  R2 publisher was RUNNING and still catching up. The validator and worker PIDs
  remained unchanged. Evidence is in `hivezilla/stream-relay/evidence`.
- On 2026-10-06 the full epoch-1049 native PoH check passed: all 431,667 blocks,
  302,501 chunks, 513,081,322 entries, and 1,079,986,176,000 hash rounds were
  checked. The new public CAR slot list independently confirms all 431,667
  native blocks and the total of 501,801,592 transactions. There are zero missing
  or extra native blocks. The 333 absent slots are also absent from the CAR list;
  they are skipped slots. The full CAR block and signature comparison finished
  on 2026-10-06 and passed: all 431,667 block identities, parent links,
  transaction counts, and all 522,157,527 ordered signatures match, with zero
  mismatches. Public CAR records were sent to Foundation for comparison;
  native files remained on Foundation. Execution metadata was sampled, not
  compared for the whole epoch. The final 146-block sample contains 191,421
  transactions. All tested non-log metadata values match after
  resolving key IDs and sorting reward values without removing duplicates.
  Native logs are truncated for 294 sampled transactions (0.15%); the CAR has
  no truncation marker in that sample. Agave's default log limit is 10,000 bytes,
  and no override is set. This is a source limit; do not claim full log parity.
  Log values differ in 92 sampled blocks. Top-level instruction data and recent
  blockhash bytes were not compared. The completed receipts are saved under
  `hivezilla/direct-agave/evidence/native-car-1049-20261006`. Overall status is
  `LOG_CONTENT_DIFFERENCE`; the full block and signature receipt is `PASSED`.
  Native publication flags remain unchanged.
  A separate live check found capture stopped in epoch 1050 at 02:13:36 UTC,
  with `disk reserve reached; capture stopped with explicit gap`. The compact
  worker is FAILED; the validator remains running. Epoch 1049 is sealed and is
  not changed by this stop. Capture was not restarted during the CAR check.

See `hivezilla/deploy/direct-agave/README.md` and the deployment evidence under
`hivezilla/direct-agave/evidence/`. Query the machine for current status; these
recorded facts are not a live health report.

## Rules for future work

- Design ingestion, replay, archive reads, and live delivery around the same
  compact block model.
- Keep client-specific filtering and subscriptions in the client framework.
- Keep capture failures and coverage gaps visible. Count a full epoch only after
  its coverage is established.
- Keep replay state and source data available long enough to recover a failure.
- Plan cloud upload, local retention, failure alerts, and restart behavior before
  calling continuous full-epoch capture ready.

## Proposed delivery order

This is the earlier broad delivery plan. The owner selected stream work as the
current focus on 2026-10-04; use the stream relay design for its implementation
order. Storage and recovery limits remain required and visible.
Preparing these plans did not change live services or cloud resources.
Resolve the destination inputs below before upload work starts.

1. Make continuous capture recoverable. Add visible failure alerts, an independent
   epoch coverage record, retained replay snapshots and shreds, and a bounded
   upload backlog. Add native-source support to the R2 publisher; its existing
   execution-canary contract does not accept direct Agave compact output.
2. Prove offline recovery on a small range with a snapshot before the range and
   complete shreds. Agave replay must feed the same Hivezilla builder. Compare
   decoded blocks and execution results with the live output. Then complete a
   full epoch, account for every produced finalized block and skipped slot, and
   test restart, slow upload, and unavailable network cases.
3. Supply one processed block stream through immutable compact chunks and an
   ordered event log. Reuse the same payloads for finalized archive reads;
   finality changes catalog membership, not execution bytes. The SDK handles
   chunk fetching, decode, bank identities, invalidation records, local filtering,
   subscriptions, hooks, WebSockets, and durable restart checkpoints.
   Clients receive identical block data; authentication must precede shared
   cache access. Select chunk size by measured request cost and latency.
4. Connect historical queries and the in-memory index service. A signature lookup
   returns a stable object/block/transaction location; Cloudflare supplies the
   archived data. Measure actual index size, memory peaks, and warm/cold query
   latency against the owner's approximate 200 GB assumption.
5. Reduce Agave only after the replay path is proved. Remove unneeded service and
   indexing work while keeping execution, feature history, and fork/finality
   behavior correct. Source full historical ledger input before promising replay
   from genesis. CAR import and shred replay keep separate source provenance.
   A minimal custom runtime comes after execution parity with Agave is proved.
6. Package the formats, catalogs, indexes, readers, and query APIs as the Solana
   ledger database product. Preserve the same client-facing data model through
   later format migrations. Database API packaging can start once the readers
   and query path work; it does not need to wait for a custom execution runtime.

First product milestone: one full finalized epoch accessible through Cloudflare,
with one client that can read history, catch up to the processed feed, keep
candidate blocks separate, apply local filters, stop, and resume without losing
its acknowledged event position.

Keep the existing user preference to skip expensive cryptographic verification
and full archive read-back during performance runs. Keep the lightweight format,
coverage, source identity, and durable commit checks needed for correct recovery.
Measure capture and delivery delay separately from Solana finality delay. Also
measure bytes per block, blocks per second, decode CPU, client RAM, cache hits,
cloud operations, storage growth, and origin traffic.

The Foundation storage input was resolved on 2026-10-04. The existing credential
file is `/Users/augustin/Developement/ferno/ferno-btc/secrets/fondation-s3.txt`;
its endpoint selects R2 bucket `solana-archive-data`. Separate private upload
configurations are installed on the archive and Foundation machines. The stored
Compact V2 backup job was running. On 2026-10-05 the owner restricted uploads to
CAR-derived Compact V2 archives. The separate finite native snapshot backup was
stopped and disabled; its partial remote data and local state were retained.
The stored compact archive now totals 105.50 TB across epochs 0 through 1047.
Epoch 1047 completed compaction and has a verified upload receipt. This does not
establish completed bulk upload, continuous
native R2 publication, public archive reads, or full-epoch coverage. See
`hivezilla/deploy/foundation-upload/README.md` and check live job receipts.
After the owner questioned optional indexes and sidecars, the archive uploader
was stopped for a file review. The owner then selected the query copy with
transaction signatures: 92,898,238,532,602 bytes (92.90 TB). The uploader resumed
with explicit profile `query-with-signatures`, separate durable state, and reboot
recovery. It reuses matching existing objects in the same cloud prefix and omits
PoH/shredding evidence and the optional serving files from future uploads. All
local files and previously uploaded cloud extras remain; selected source size
is not total bucket usage. All 1,048 source inventories and eight old intents
passed; six recovery/scope checks passed. Four query epochs had completion
receipts at the first sample, and a remote signature matched the local source.
The first source checks above are historical. On 12 October, the complete
query upload was verified from Foundation R2: all 1,048 epoch manifests for
epochs 0 through 1047 are present, totaling 92,898,238,532,602 selected bytes.
The last epoch manifest was stored at 06:40:26 Europe/Paris. Three final
manifests matched their selected remote file sizes. No separate full archive
read-back was done. The public query service remains pending. The native
snapshot backup remains disabled.
The owner expects the archive to reach epoch 1054. Its public CAR is available,
but epochs 1048 through 1054 are absent from this completed cloud prefix.
This finite upload is not an automatic latest-epoch pipeline.
At the 18:42 public checks on 12 October, Foundation's newer live source reached
part of epoch 1055, then showed FAILED in v1 and failed in v2. The last native
confirmation was at 12:47:57 Europe/Paris. The matching processed R2 catalog
last advanced at 16:28:33, with 311,864 blocks and 151,819,048,051 bytes. Two
checks found no advance. These frames are not a complete per-epoch query archive.
At that earlier check, the failure cause could not be read: the NAS SSH connection timed out and
Foundation rejected SSH authentication. The 11 October disk-reserve failure is
a dated record, not proof of today's cause. See
`hivezilla/deploy/foundation-upload/evidence/status-20261012.json`.
After the owner added the SSH identity, direct Foundation access worked. The
12 October failure was the 400 GiB disk reserve. Lossless compression of closed
benchmark payloads and removal of unused compiler caches restored space.
Capture resumed as source `000000000000000018ddd9a6bb6fc50e-282459`; the validator
kept PID 2630745 and its host/plugin. Public v1/v2 heads advanced, one real v2
payload matched its SHA-256, and the new processed R2 head advanced from
sequence 2176 to 8064 in the checked interval. The old capture gap remains
explicit; available RPC gap blocks have a separate backup process.
The owner explicitly approved native validation evidence in Foundation R2 and
local retirement after exact verified copies. The tested helper restores all
16 original files in a sample bank. Two storage services keep at least two
hours, wait for cloud/consumer checkpoints and native writer acknowledgement,
verify full bundle readback, and then release exact local compact/custody files.
They use `native-custody/processed-v1/SESSION`. The restored worker is pinned to
the same Agave revision. Rooted storage is active; sustained capacity, invalid
fork storage, automatic source changes, and full archive query integration are
not established by these checks.
A separate persistent Foundation process now handles CAR epochs 1048–1054 in
order. It uses a private memory cache for one public CAR, official input SHA-256,
source slot/count checks, Compact V2 signature coverage, query-only immutable
uploads, and temporary staging release after the remote manifest check. CAR
1048 passed its official download SHA-256 check and started compaction; upload
completion is pending. Disk and memory
guards keep this work below live priority. Existing NAS source files remain.
During recovery, native conversion fell behind. Callback updates are now
aggregated per bank, pending work is indexed, discovery has a 16,384-row bound,
and incomplete banks wait for a block footer before full decoding. Four bank
builders and ordered event commits remain. Native unit and real-builder
restart/reuse tests passed. The current worker is
`native-cloud-retention-fast-r3-20261012`; the validator and plugin stayed on
their existing binaries and PID. The repaired public v2 head reported live
with about one second of source lag at 21:44 UTC. R2 uses larger bounded event
batches and retains its durable intent/checkpoint. Its upload queue was draining.
A persistent priority guard pauses CAR work and old cold storage at native slot
lag above 32 or cloud event lag above 8,192, then resumes below eight slots and
4,096 events. The new live cold storage remains active. CAR 1048 resumed after
conversion caught up. The RPC backup timed out while frozen; its saved files
remain and its tested resume helper records missing pruned blocks explicitly.
It now has no runtime limit and is excluded from freezing. These are finite
recovery checks; uninterrupted capture, automatic source changes and full
native gap recovery remain unproved. Read current reports and
`hivezilla/deploy/foundation-upload/evidence/background-load-20261012.json`.
At 00:17 Europe/Paris on 13 October, native conversion was current, the R2
queue was about 150 events, and CAR 1048 was about 73% compacted with background
work running. Live native storage had retired 33,216 verified banks and released
126.47 GB in that run. The RPC backup retained 41,405 blocks and 50,388,439
transactions. It remains `INCOMPLETE`: 18,940 originally listed produced blocks
are missing after pruning, plus the following RPC boundary check. Its earlier
55,783-slot unavailable prefix also remains explicit. No full native recovery
or uninterrupted coverage is established.
Read `hivezilla/deploy/foundation-upload/evidence/recovery-flow-20261012.json`
and the current machine receipts for later progress.
The later request for Foundation live publication authorizes the separate Rust
native stream publisher described above. It does not resume the finite snapshot
backup or change the CAR query copy's scope.
The owner's later speed question led to a bounded upload test and faster settings
on the archive machine. The initial one-file/two-part upload had an 80 MiB/s
ceiling. The active job now uses two files and eight 32 MiB parts per file, a
400 MiB/s ceiling, a 2 GiB memory bound, and a four-core CPU bound. The approved
query file set, destination, and durable query receipts are unchanged. A 30-second
sample reached about 182 MiB/s of payload transfer and 198 MiB/s of outbound
interface traffic, with no CPU throttling or memory failure. A later payload
sample reached 186 MiB/s. These are short samples, not a sustained whole-archive
guarantee. All six recovery/scope checks passed after the settings change. See
the upload README and throughput evidence.
On 13 October, an external Cloudflare machine/pipeline checker and a read-only
30-second Foundation heartbeat were installed. The working alert label is
Megaloadon; the operating-system hostname remains blockzilla. The external
checker received live heartbeats and public block progress with no current
faults. It uses two new monitor-only tokens and the free blockhash SSE view;
no SSH, bucket, account or private stream key was copied into it. The owner
selected Telegram and the word play name Megaloadon. Its new alert bot and
explicitly confirmed private chat are configured with separate Worker secrets.
A real TEST event matched the successful API receipt and visible Telegram message.
Notifications are active; the latest finite check had no fault or pending alert. A missing heartbeat has a
90-second threshold, with 30-second alarm checks and a one-minute cron backup.
Pipeline faults and recovery have separate confirmation rules and a durable
notification outbox. This checker does not restart the validator or perform
automatic source handoff. See `hivezilla/deploy/foundation-watch/README.md`.
The same review repaired the stalled-capture guard, invalid sample handling and
uncommitted registry restart. It finished 14 old publisher tail events and
verified remote sequence 623694 before disabling the completed drain. The
32 GiB worker limit is now saved in /etc rather than only /run. CAR 1048 has a
checked query/signature upload of 112,620,833,984 bytes; 1049 is downloading.
Restored-file cache retention, source handoff, earlier RPC boundary preservation,
and log rotation remain open. Native gaps and the original query profile remain
explicit. A real machine outage has not been tested.
The same bot now has private read-only `/status` and `/help` commands. Its
separate authenticated webhook, owner-only command queue, durable retries and
real Telegram reply were checked. The pinned host heartbeat now reads fixed CAR
stage logs and checked query/signature receipts. Stage ETA is separate from
rounded recent-run epoch and plan estimates; old progress or paused work
suppresses ETA. At 06:44 Europe/Paris, epochs 1048–1050 had completed selected
uploads totalling 355.94 GB. Epoch 1051 was 66.3% compacted, with about 16 minutes
for that stage and 25–40 minutes to upload completion. The remaining plan through
1054 was about 5–7.5 hours. Native compact lag was zero; cloud lag was 192 events.
The owner later approved read access to only the controller log: owner root,
group ubuntu, mode 0640. A parser fix keeps final transfer rows with no ETA
from reusing old estimates; 28 sender/archive tests passed. The new pinned
heartbeat release sent successfully and the external watcher had no current
faults. At 08:42 Paris, epoch 1051 was uploaded, 1052 was about 54% compacted,
and native compact lag was zero. The validator and live workers were not restarted.
See `hivezilla/deploy/foundation-watch/evidence/telegram-commands-20261013.json`.

## Public HTTPS stream, 5 October 2026

The owner prefers HTTPS access without SSH and will consume the feed with Rust.
The existing Foundation Cloudflare Tunnel now publishes the API-key protected
relay at `https://stream.blockzilla.dev`. Consumers use SSE chunk notices plus
finite binary GETs. A bounded native Rust example decoded 12 blocks and 12,817
transactions, then resumed from its cursor, through the public HTTPS hostname.
All public read routes reject wrong keys. No validator or capture service was
restarted. The example is not a durable reconnect/backfill SDK. Source flags
remain canonical=false and publication_ready=false. Public R2 recovery, CDN
caching, sustained fan-out, and a second relay machine remain unfinished.
See the current deployment record and `docs/start/live-stream.md`.

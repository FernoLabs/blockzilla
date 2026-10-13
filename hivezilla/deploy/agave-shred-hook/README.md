# Hivezilla raw shred hook

Activated on 2026-10-01 on `ubuntu@213.239.141.61`. The validator restarted
once with the modified Agave binary and is now PID `590165`. The recorder
is PID `590048`. Both services remained active through the live test, with
no automatic restart. RPC health returned to `ok` after snapshot loading
and catch-up. The validator identity and genesis hash stayed the same.

## Receive path

```text
Agave UDP receiver
    -> Hivezilla C callback -> bounded packet queue -> IPC worker
    -> original fetch queue -> filtering -> signature checks -> replay

IPC worker -> private Unix socket -> Hivezilla recorder -> local journal
```

The `TapSender` observes the batch before the original fetch queue can evict
it. It does not modify, replace, or consume Agave's packets. It keeps ordinary
and coding packets, duplicates, conflicting bytes, repair traffic, and other
datagrams received on these sockets. There is no client filter engine.

Repair ingress and repair acceptance are separate observations. The second
callback runs only after Agave accepts the repair nonce. It keeps the complete
datagram, including the nonce. Signature validity remains a later validator
decision. Header counts and slots in the capture status are unverified input
observations; they are not validator progress or consensus results.

The hook covers the UDP receive path. The existing `--no-xdp` argument is kept.
Packets lost before the receive batch reaches the hook are outside this
capture. Kernel drop telemetry is not implemented in this first version.

Each receiver preserves its batch order. The common producer queue defines
capture order across receiver threads; this is not physical network arrival
order. Timestamps record adapter observation time per batch, not NIC time.

## Hivezilla plugin interface

[`hivezilla/shred-hook-abi`](../../shred-hook-abi) defines C ABI version 1.
[`hivezilla/shred-hook`](../../shred-hook) owns the producer and the recorder.
The producer exports `hivezilla_shred_hook_v1`, which returns a versioned
function table with initialization, packet notification, and shutdown.
The Agave adapter is [`hivezilla_shred_hook.rs`](hivezilla_shred_hook.rs).

This is our producer library, separate from Yellowstone. A future Hivezilla
Geyser library can export the same raw-shred factory in addition to Agave's
`_create_plugin` entry point. The raw receive hook is still needed in Agave:
the standard Geyser interface does not expose the original UDP datagrams.
This prototype does not implement the Hivezilla Geyser execution callbacks.

The ABI passes only fixed C fields and a bounded borrowed byte range. It does
not pass Rust traits, vectors, allocators, or ownership across the library
boundary. The staged producer uses Rust `1.97.1`, matching Agave. The separate
recorder uses Rust `1.98.0`. A fixture also tested a producer built with Rust
1.98.0 from a test program built with Rust 1.97.1.

Build the minimal producer with Agave's toolchain:

```sh
cargo +1.97.1 build --release --locked -p hivezilla-shred-hook \
  --no-default-features --lib
```

Copy that library to its final release path before building the recorder,
because the recorder build can also produce a library with its own features.

```sh
cargo +1.98.0 build --release --locked -p hivezilla-shred-hook \
  --bin hivezilla-shred-hook-recorder
```

The receiver adapter loads its configured library once before receive threads
start. It keeps the library loaded for the lifetime of the validator process.
It requires both `HIVEZILLA_SHRED_HOOK_LIBRARY` and
`HIVEZILLA_SHRED_HOOK_SOCKET`, or neither. With neither, the normal receive
path runs without capture. A configured library that cannot initialize causes
startup to fail clearly instead of silently starting without capture.

## Bounded writer and failure behavior

The callback copies at most 1,232 payload bytes into a 65,536-packet queue.
It does not wait for disk writes or IPC delivery. One worker owns socket
writes and assigns the IPC sequence. The packet bodies in the queue are
bounded to about 80 MiB, plus allocator and queue overhead.

Queue saturation, invalid adapter metadata, or an IPC failure stops capture.
The adapter reports the failure once and continues Agave's original packet
path. The recorder marks the capture `FAILED`; it does not label the stream
continuous after a loss. This first version requires a validator restart to
reinitialize a failed producer. A recorder restart alone does not reconnect
the producer. This limit must be removed before unattended long captures.

The recorder accepts only a producer with the same user ID and the exact
configured executable path, checked with Linux peer credentials. Its socket
and directories are private. systemd receives readiness only after the socket
is listening, so the validator can start after the recorder is ready.

The recorder uses 64 MiB segments, a 64 GiB total capture limit, and a 500 GiB
free-space reserve. It performs grouped fsyncs and writes progress only after
the data is synced. It never deletes old captures to recover quota. Each
connection starts a new random capture directory; it never resumes an old
capture ID or overwrites an old segment.

## Local capture format

The IPC format is `hivezilla-shred-hook-ipc-v1`. A 52-byte header contains the
packet class, peer address, local/remote ports, sequence, timestamp, and length;
the following bytes are the exact UDP payload. Ordinary receive, repair
receive, and accepted repair have separate class values. A terminal frame
states whether producer shutdown was clean or capture failed.

The local journal uses `hivezilla-protocol::FrameV1` for a prefix hash chain.
Its outer header/footer are explicitly `HZSHWAL1` / `HZSHEND1`. This local
test format is **not a registered HiveSync stream**. It has no custody,
deletion, registry, or canonical publication authority. It must not be labeled
as a registered raw-shred or repair stream while it contains IPC metadata.
A later adapter can extract exact datagrams into registered formats 2 and 3
after stream manifests and gap/custody policy are configured.

The runtime files are:

- `SESSION.json`: source contract, configured identities, and resource limits;
- `STATUS.json`: durable progress, counts, and capture state;
- numbered `.active` / `.wal` files: active and sealed local journal segments;
- `CURRENT.json` in the output root: the current capture directory.

The genesis hash and validator identity are configured source labels. The
recorder itself does not query RPC to verify them. Rollout must bind them to
the running validator, as the existing gRPC capture does.

## Build and fixture evidence

- The modified Agave 4.3.0 release build passed, based on source
  `825efd18292aff6ffcf9daa0f7612f21b3531a72`.
- Seven tests passed on macOS and Ubuntu. They cover duplicate and repair
  bytes, empty/non-shred datagrams, length limits, queue failure, sequence
  gaps, quota, segment rotation, and prefix corruption.
- Clippy passed with warnings denied on the development host.
- A bounded packet mirror captured 256 real TVU packets from the running
  validator. No packets were dropped by the packet mirror's kernel queue.
- The fixture sent those packets through the C ABI, IPC worker, and recorder.
  All 256 payloads matched in the same order, totaling 254,681 bytes. The
  journal had 257 frames, including clean termination.
- The final fixture also tested systemd readiness notification and producer
  executable checks. The service definition passed `systemd-analyze verify`.

The fixture proves packet preservation. The separate live test below proves
that the hook runs inside the validator. No archive verification or R2
publication was run. Exact reports and artifact hashes are in
[`evidence/`](evidence/).

## Live activation result

The [live activation report](evidence/shred-hook-live-activation-20261001.json)
records the executable hash, running process IDs, RPC source binding, durable
capture counts, and the full-block gRPC test. The earlier staged build report
is unchanged.

In a 30.03-second sample after RPC health returned to `ok`:

| Measurement | Result |
| --- | ---: |
| Packet observations | 144,820 |
| UDP payload bytes | 176,017,329 |
| Turbine receive observations | 143,734 |
| Repair receive observations | 543 |
| Accepted repair observations | 543 |
| Data shred headers | 73,039 |
| Coding shred headers | 71,781 |
| Finalized slot increase | 112 |
| Packet observations per second | 4,822.9 |
| Recorder average logical CPU equivalents | 0.0267 |

Accepted repair observations record the same datagram again after nonce
acceptance. These counts are observations, not a count of unique packets.
The CPU sample measures the recorder separately. Producer work is inside the
validator, so this test does not measure total hook overhead against a
baseline. It does not measure upstream socket loss or prove gap-free capture.

The gRPC test sealed 32 finalized blocks in 8.41 seconds. It recorded 32,844
transactions, 34,522 nested block entries, 34,522 standalone entries, and
190 slot events in a 23,194,824-byte WAL. It reported no footer or parent-update
events. Raw capture stayed `ACTIVE` through this test. At the final observation,
it had 2,582,444 packet records and 3,374,074,504 journal bytes.

The recorder and validator drop-in are installed. Both have persistent boot
configuration. The current validator unit still uses its original transient
unit definition with the continuous and shred-hook drop-ins; the persistent
unit is used on the next machine boot. There is no finite runtime limit.
Capture remains bounded to 64 GiB. At the limit, capture stops with a recorded
failure and the validator continues to run.

### Block coverage check

The [2026-10-01 block coverage report](evidence/block-coverage-20261001T130631Z/summary.json)
compares finalized block slots with the Solana public mainnet RPC. The saved
requests and replies are in the same evidence directory. Both endpoints
reported the expected mainnet genesis hash.

- Node range: slots 452,286,812 through 452,296,154, inclusive. Both endpoints
  returned the same 9,331 blocks across 9,343 slots. The reference omitted
  12 slots (0.1284%). Our node missed zero reference blocks.
- Earlier gRPC sample: all 32 reference blocks were present in actual block
  records, covering slots 452,286,927 through 452,286,958. The seal's wider
  first/last slot counters include other event types and do not define block
  coverage.
- New gRPC sample: all 128 reference blocks were present, covering slots
  452,296,221 through 452,296,348. There were no missing or duplicate block
  slots. The sample took 33.89 seconds and used 114,941,582 WAL bytes.

These are finalized slot-set comparisons against one public reference, not
independent consensus or a continuous gRPC coverage guarantee. Raw capture
stayed active, and both services had zero automatic restarts. This check did
not run archive verification or publish data.

To reproduce the adapter integration in a clean Agave checkout:

1. Copy `hivezilla-shred-hook-abi` into the checkout root. Its vendored manifest
   uses explicit package fields, with no inherited workspace lints or nested
   `[workspace]` section.
2. Copy `hivezilla_shred_hook.rs` to `core/src/hivezilla_shred_hook.rs`.
3. Apply [`integration.patch`](integration.patch), including its dependency
   lock changes, to the exact base revision.
4. Build `agave-validator` with Rust `1.97.1` and the modified lock file in a
   separate target directory. Do not replace a running binary in place.

## Activation and rollback

The prepared root is `/home/ubuntu/live/hivezilla-validator-20260930`.
The binaries are staged at:

- `releases/hivezilla-shred-hook/libhivezilla_shred_hook.so`;
- `releases/hivezilla-shred-hook/hivezilla-shred-hook-recorder`;
- `releases/agave-shred-hook-v4.3.0/agave-validator`.

[`hivezilla-shred-hook-recorder.service`](hivezilla-shred-hook-recorder.service)
and [`shred-hook.conf`](shred-hook.conf) are staged deployment files. The
override changes only the executable path and adds the local hook connection;
all existing validator arguments, identity, ledger, and optimized Yellowstone
config are kept.

For a new deployment:

1. Compare the staged files with the recorded hashes. Bind the public identity
   and genesis to the current RPC response. Check current disk reserve.
2. Create the private `captures/shred-hook-live` directory for user `ubuntu`.
   Install the recorder unit and the validator `shred-hook.conf` drop-in.
3. Reload systemd, enable/start the recorder, and wait for its ready state.
4. Restart the validator once. Check its new executable and raw-hook startup
   message. Wait for RPC health and finalized slot progress to recover.
5. Check growing, synced packet counts and no capture failure. Run a bounded
   full-block gRPC capture to check that the execution stream still works.
6. Save a separate live activation report. Keep the fixture report unchanged.

If startup fails, remove only the `shred-hook.conf` drop-in, reload systemd,
and start the original continuous validator service. Preserve capture files
and the stock release. Do not change the validator identity or ledger. Use
systemd for this deployment; the original finite canary commands bind the
stock binary and must not control this custom binary.

The hook stream currently has no public listener, gRPC method, R2 uploader, or
Cloudflare route. The next work is producer recovery without validator restart
and a durable consumer shared with the Hivezilla execution plugin.

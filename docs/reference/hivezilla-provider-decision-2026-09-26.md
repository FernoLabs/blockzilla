# Hivezilla provider decision — 26 September 2026

Status: **test Shyft Build first; no provider has passed a live trial yet.**
This note updates the [25 September review](live-source-providers-2026-09-25.md).
No plan was purchased and no vendor was contacted during this review.

The order of selection is: no byte-based charge or traffic cap, complete recoverable
data, then low cost. Low latency is not a selection target. Start with one provider
and the existing shred reader. Keep reconciliation and Compact V2 creation on the
NAS. Add independent readers later.

## Shortlist

Prices and the linked technical pages were checked on 26 September 2026. These
are vendor claims, not measured reliability results.

| Candidate | Published monthly price | Decision | Open condition |
| --- | --- | --- | --- |
| [Shyft Build](https://shyft.to/solana-rpc-grpc-pricing) | $199; 10 gRPC connections; no bandwidth cap or overage advertised | First trial | The shared plan must accept all blocks with transactions and PoH entries. Test replay on this exact filter. |
| [OrbitFlare Shared gRPC](https://orbitflare.com/pricing) | $500; one source IP; unlimited streaming advertised | Second trial if Shyft fails | Confirm that the shared plan permits this full feed. Block replay remains unverified. |
| [constant-k Nexus](https://www.constant-k.com/pricing/) | $179 | Hold for a separate adapter | Its node rejects `includeEntries: true` on a block filter. Our recorder cannot use it unchanged. |

Shyft publishes an [unfiltered Finalized block example](https://github.com/Shyft-to/solana-defi/blob/main/general-grpc-examples/Rust/block-stream-latency-test/src/blocks.rs),
but it disables entries. Its [block documentation](https://docs.shyft.to/solana-yellowstone-grpc/docs/streaming-blocks-and-blocksmeta)
also says a filter is required. This conflict needs a real subscription test.
Its [replay guide](https://docs.shyft.to/solana-yellowstone-grpc/docs/getting-started/replaying-slots-with-grpc)
advertises about 150 slots, but demonstrates transactions, not full blocks with
entries. Its [refund policy](https://shyft.to/refund-policy) is conditional; do not
treat a paid month as a guaranteed free trial.

OrbitFlare [documents the entry option](https://docs.orbitflare.com/data-streaming/yellowstone-slot-block-monitoring)
and Finalized blocks. This does not prove that our full feed is accepted on the
$500 shared plan.

constant-k requires a [separate entry subscription](https://docs.constant-k.com/kaldera/filters/).
Its [replay documentation](https://docs.constant-k.com/kaldera/replay/) describes a
store of up to 24 hours, with possible gaps. Full blocks and entries are not among
its verified replay types. A separate stream needs a slot-based join and a tested
repair path. Nexus has [unthrottled subscription bandwidth](https://www.constant-k.com/plan_limits/);
the cheaper Blade Runner plan can discard queued updates and is not the choice
for this trial.

These are trial priorities, not production approval. A self-hosted node remains a
fallback if provider data or redistribution terms fail. Owning the node alone does
not prove complete delivery; it also needs monitoring, retention, and repair tests.

## What the old benchmark proves

The Claude Code review was recovered from conversation
`9cb9dc10-90dc-4f68-a80a-7e80b604b9f6`. Its scratchpad `sample3.py` used eight
finalized JSON-RPC blocks, slots 450371767 through 450371774. It estimated protobuf
size and entry counts. It compressed a synthetic payload made from transaction
bytes and selected metadata. It did not capture a live gRPC stream.

Recovered mean values per block:

| Value | Bytes | Meaning |
| --- | ---: | --- |
| Estimated block protobuf | 3,970,642 | Model output, not a serialized `SubscribeUpdate` |
| Synthetic payload | 3,666,895 | Input to the compression experiment |
| Synthetic payload, zstd level 1 | 728,919 | Local compression result |
| Synthetic payload, zstd level 3 | 566,622 | Local compression result |

The earlier 12 MB/s central estimate and 20 MB/s busy estimate equal 96 and
160 Mbit/s. They are useful planning inputs, not measured sustained rates or an
upper bound. They suggest that 2.5 Gbit/s has ample capacity for one live input;
NAS catch-up, R2 transfers, shred traffic, and downstream subscribers also use the
link. Measure their combined load before setting the network requirement.

The old monthly totals and compression ratios do not establish provider billing,
wire traffic, block completeness, or service reliability. No provider comparison
was run with live credentials.

## Trial procedure

1. Obtain a test endpoint, a token-file path, and the exact plan and region. Get
   vendor confirmation that the plan permits an unfiltered continuous block feed
   with transactions, metadata, and entries. Confirm rights for internal projects,
   external live subscribers, and published Compact V2 archives separately.
2. Run a ten-minute compatibility test in a new spool. Retain raw observations,
   including incomplete ones. Do not enable `--require-complete-poh`, which can
   filter observations before the later audit.
3. Stop capture, audit the payloads, and check entry structure. A passing short
   test permits a longer test; it does not establish complete delivery.
4. Run a 24–72 hour capture with measured storage capacity and supervised reconnects.
   Test interruptions both inside and outside the advertised replay window. Save
   all retry reports and resume warnings. An unavailable replay range must remain
   an explicit gap until another source repairs it.
5. After finalization, compare the captured slot and blockhash set with independent
   finalized RPC `getBlocks` and `getBlock` results over the same bounded interval.
   Compare transaction counts and required metadata. A skipped slot is not a
   missing block; a Confirmed fork must not be accepted as the finalized block.
6. Test the complete repair path to the NAS, then Compact V2 creation. Require zero
   unresolved produced-block gaps for the accepted interval. A high uptime number
   or a minimum record count is not a substitute for this comparison.

Compatibility commands, with placeholders replaced by the supplied test access:

```bash
export BLOCKZILLA_GRPC_X_TOKEN_FILE=/run/secrets/provider_probe_x_token
hivezilla record-grpc-raw \
  --endpoint https://<provider-endpoint>:443 \
  --origin-node-id probe-shyft \
  --source-id shyft-build-probe \
  --output-dir /data/probe-shyft \
  --max-blocks 100000 \
  --timeout-secs 600 \
  --resume-coverage-warning-file /data/probe-shyft-resume-warning.json

hivezilla inspect-grpc-raw \
  --output-dir /data/probe-shyft --verify-payloads
hivezilla verify-grpc-raw-poh \
  --output-dir /data/probe-shyft --min-records 1
```

Run the audits only after capture stops. Keep the default free-space reserve;
provide enough storage before starting. A timeout, early stream end, or nonempty
spool does not mean the trial passed. Check the recorder report.

## Current client limits and measurements

- [`record-grpc-raw`](../../hivezilla/service/src/grpc_raw.rs) requests **Confirmed**
  blocks and has no commitment option. It cannot prove Finalized subscription
  support. A separate probe or a reviewed client extension is needed for that test.
- `verify-grpc-raw-poh` checks entry counts and indices, transaction coverage, and
  the final entry hash against the blockhash. It does **not** recompute the PoH
  hash chain. Treat it as a structural check.
- `raw_bytes_written` measures serialized protobuf bytes;
  `compressed_bytes_written` measures compressed local records. Use separate
  network counters for actual wire volume. Record elapsed time, unique blocks,
  gaps, duplicates, reconnect causes, CPU, memory, and disk use.
- The current raw format rejects the new Solana transaction message `config`
  field. Classify such a failure as a client compatibility issue before assigning
  it to the provider.

No usable Shyft, constant-k, or OrbitFlare gRPC trial configuration was found in the
workspace or relevant environment variables. The local Helius RPC token file does
not establish access to one of these gRPC plans. The next live test needs the test
endpoint and token-file path; secret values must stay outside this document.

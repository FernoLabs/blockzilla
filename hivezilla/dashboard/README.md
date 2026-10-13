# Hivezilla validator dashboard

This small Topcoat application shows the current validator readiness report and
selected Prometheus metrics. It reads the readiness JSON files from the local
reports directory and fetches metrics from the validator's loopback endpoint.
It also reads the epoch runner's saved progress report. The epoch table shows
collected blocks, compacted blocks, pending compaction, covered slots, chain
skips, capture gaps, last saved slots, and raw and compact sizes. Counts advance
after a sealed batch is saved, then after its compact archive is complete.
It does not control or restart the validator, change its identity, or publish
data.

The direct Agave table reads `reports/direct-agave-health.json`. It shows durable
capture, compact worker progress, storage limits, and raw shred status. An active
process with a failed journal shows failure. A report more than 45 seconds old
shows `STATUS OLD`. The earlier gRPC run is labelled separately. Use
`HIVEZILLA_DIRECT_PROGRESS_FILE` to set another report path.
When direct capture is configured, its health report replaces the old readiness
state and process ID. A failed or stale check cannot keep an earlier `READY`
value or finalized slot. The old gRPC metrics and execution canary sections are
hidden in this mode.

Topcoat uses release 0.9.0. Topcoat is experimental and may
make breaking changes.

Run it on the validator host with a loopback-only bind:

```sh
HOST=127.0.0.1 PORT=8766 cargo run --release -p hivezilla-validator-dashboard
```

Then open a local SSH tunnel from your workstation:

```sh
ssh -L 8766:127.0.0.1:8766 ubuntu@213.239.141.61
```

Open `http://127.0.0.1:8766`. The page refreshes every 10 seconds. Direct capture
shows the durable health report. The earlier mode shows `Waiting` when the
validator has not started its metrics endpoint yet.

The [validator Tunnel setup](../../docs/architecture/hivezilla-validator-tunnel.md)
records the private dashboard and gRPC connection prepared for the Ubuntu
machine. Cloudflare Access and client enrollment must be completed before a
WARP device can use those addresses.

Set `HIVEZILLA_CANARY_REPORTS_DIR` to use another reports directory. Set
`HIVEZILLA_EPOCH_PROGRESS_FILE` to use another epoch progress file. Its default
is `/home/ubuntu/live/hivezilla-validator-20260930/reports/epoch-progress.json`.
The file must use schema `hivezilla-epoch-progress-v1`. A report older than
180 seconds shows a notice; invalid counters are not displayed. Before the
first sealed batch, missing blocks are shown as `Not checked`.

Set `HIVEZILLA_VALIDATOR_METRICS_URL` to use another metrics URL. The default URL
is `http://127.0.0.1:8999/metrics`. The dashboard limits report and metrics
input sizes and displays metric names and values without their labels.

Check the package with:

```sh
cargo test -p hivezilla-validator-dashboard
```

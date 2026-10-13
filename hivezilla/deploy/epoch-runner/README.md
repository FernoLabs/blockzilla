# Epoch 1047 local run

The full epoch run uses local Yellowstone gRPC blocks on
`ubuntu@213.239.141.61`. It covers slots `452304000..452736000`, with the last
bound excluded. Capture and compaction run in separate workers, in batches of
128 blocks. The validator remains on its existing process and identity.

The run directory is:

```text
/home/ubuntu/live/hivezilla-validator-20260930/epoch-runs/epoch-1047-20261001
```

The installed systemd services are enabled for boot and restart on failure:

| Service | Purpose |
| --- | --- |
| `hivezilla-validator-canary.service` | Existing non-voting node |
| `hivezilla-epoch-1047.service` | Epoch capture and local compaction |
| `hivezilla-epoch-dashboard.service` | Loopback dashboard on port 8766 |

`config.json` and the two epoch service files are copies of installed settings.
They contain public identity data and local paths, without key material. The
collector has an eight-CPU quota, 16 GiB memory limit, and a 500 GiB free-space
reserve. It has no six-hour runtime limit.

The dashboard reads `reports/epoch-progress.json` under the remote root. It
shows collected and compacted block counts, the queue, covered epoch slots,
chain skips, capture gaps, saved sizes, and last saved slots. It refreshes every
10 seconds. Counters reflect sealed batches, not partial files.

The preflight compacted 128 real finalized blocks. Normalization took 6.75
seconds and compaction took 3.88 seconds. A separate 16-block replay probe
started at the exact requested slot. Six runner tests and the first five
dashboard tests passed before the collector was armed. The dashboard update
adds two count-validation tests.

The [deployment report](evidence/epoch-1047-deployment-20261001.json) records
the original armed state, artifact hashes, tests, and probe paths. It is a
point-in-time record, not current run status. The saved `evidence/Cargo.lock`
is the exact lockfile of the isolated Linux runner/dashboard build. The
updated dashboard has its own release directory and build report.

The [first epoch batch](evidence/epoch-1047-first-batch-20261001.json) saved and
compacted all 128 blocks from slot 452304000 through 452304127. The raw capture
and compact evidence share one inode, and disposable normalization was
removed. Two capture attempts at the boundary failed before replay recovered.
No produced block was missing in that saved range. The complete epoch is
still in progress.

On October 1, compaction stopped at batch 230 because its normalized block
file was 539,625,154 bytes, above the old 512 MiB input limit. The input limit
is now 1 GiB per file; publisher outputs still have a 512 MiB limit. The
blocked batch completed after deployment. Failed builder retries now remove
disposable normalized files and retain the receipt, logs, and raw capture.
Cleanup removed 566 duplicate derived copies and recovered about 293 GiB.
One full failed sample remains. Five compactor tests and six runner tests
passed. Only the collector restarted; the validator stayed on its process.
The [recovery snapshot](evidence/compact-bound-after-20261001.json) and
[cleanup report](evidence/compact-bound-cleanup-20261001.json) record this
recovery. Compaction still has a backlog, so use live counters for status.

The recorder update at 16:01 Paris time on October 1 groups disk syncs and
uses 32 MiB adaptive HTTP/2 receive windows. It replaced the reader binary
atomically; the validator and epoch collector kept their process IDs. Fourteen
focused tests passed. In the [same-data replay test](evidence/capture-reader-replay-probe-20261001.json),
both readers saved the same 128 blocks and 168,468 entry events. Total capture
command time fell from 18.81 seconds to 8.03 seconds, a factor of 2.34. This
includes startup and the final seal; it is not a full-epoch speed result.
The [deployment report](evidence/capture-reader-deployment-20261001.json)
records binary and source hashes. The previous binary remains in the remote
`releases/capture-reader-r1-20261001` directory for recovery.
The [post-install check](evidence/capture-reader-after-20261001.json) observed
three new sealed batches with zero new retries. Two were already compacted
and the third was in compaction. No missing produced blocks were reported in
the saved range. This short observation does not prove a complete epoch run.

The [runner contract](../../epoch-runner/README.md) records restart and replay
limits. The plugin has a 512-slot replay window. A long outage can prevent
complete capture. Coverage checks use the local finalized ledger. The
separate 64 GiB raw-shred test does not cover the full epoch.

Archive read-back verification is skipped. Source integrity and writer counts
still have checks. Local execution archives remain unverified and
noncanonical. R2 upload is disabled. A complete run does not imply permission
to publish a canonical epoch archive.

Use the installed service for status. Do not restart the validator to refresh
the dashboard:

```sh
systemctl status hivezilla-epoch-1047.service hivezilla-epoch-dashboard.service
journalctl -u hivezilla-epoch-1047.service --since '10 minutes ago'
cat /home/ubuntu/live/hivezilla-validator-20260930/reports/epoch-progress.json
```

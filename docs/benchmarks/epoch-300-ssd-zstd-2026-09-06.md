# Epoch 300 SSD CAR compression comparison

## Run in progress

The real `read-car` count example runs once on raw CAR, then once on the
existing Zstd level-3 CAR. Both inputs and all outputs are on NAS SSD RAID0
Btrfs. No new archive copy or compression is needed.

- Raw: `/volume2/blockzilla-bench/archive/car/300/epoch-300.car`, 508,337,873,180 bytes.
- Level 3: `/volume2/blockzilla-bench/archive-zstd-trial/car/300/epoch-300.car.zst`, 206,321,123,867 bytes.
- Each slot index is 5,184,000 bytes. The controller checks that both indexes match.
- Results: `/volume2/blockzilla-bench/results/epoch-300-ssd-zstd-20260906-XnnaWZ/`.
- Controller: `car-ssd-zstd-comparison-20260906.py` in that results directory.
- Saved source: [`run_epoch_300_ssd_zstd.py`](../../scripts/run_epoch_300_ssd_zstd.py).
  This NAS-only run script requires a fresh results directory with `read-car`
  and the earlier count result as `baseline.json`; pass that directory as its argument.
- Reader: the same Linux-musl release binary for both inputs, built from the current sample worktree.

Preflight resolves paths, checks SSD filesystem identity and expected sizes,
and rejects a raw CAR that would hide the compressed file in the Zstd input
directory. It does not clear the operating system cache.

Each scan must match the previous epoch-300 count result: 408,989 blocks,
724,730,034 transactions, 983,615,850 instructions, 102,252,970 recorded inner
instructions, and all 48 buckets of 9,000 slots. The controller also checks
decoded CAR length and zero network bytes. This validates the count workload;
it does not replace strict failed-transaction metadata validation.

`status.json` records the active representation or a failure. Progress is in
`raw.progress.log` and `zstd-3.progress.log`. Each completed scan saves a
result JSON. `comparison.json` is written only after both scans succeed.

Metrics include elapsed time, TPS, process CPU time, decoded CAR bytes/time,
and stored CAR size/time. Sampled `/proc` I/O counters are saved separately.
Stored size/time is not a physical disk throughput measurement. One run per
representation is a reference result, not a repeated cold-cache experiment.

## Earlier evidence

The [5 September SSD comparison](epoch-300-car-zstd-level3-2026-09-05-report.md)
measured 817.01 seconds raw and 753.09 seconds compressed: 1.085 times the
throughput and 59.41% less stored space. The new run has not yet established
whether that throughput gain remains with the current reader.

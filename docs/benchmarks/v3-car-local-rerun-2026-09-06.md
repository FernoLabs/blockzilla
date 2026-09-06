# V3 then CAR: local sample run, 6 September 2026

## Scope

Run count-by-slot-hour, USDC, Pump.fun, and FireWatch once for each of epochs
0, 100, 200, 300, 400, 500, 600, 700, 800, 900, and 1000.
Run all 44 V3 jobs first, then all 44 CAR jobs. Do not run network reads,
archive copies, compaction, or archive hashing. Keep all results on the SSD.

## Inputs

All V3 inputs are on SSD. CAR epochs 0, 300, 800, 900, and 1000 are on SSD.
CAR epochs 100, 200, 400, 500, 600, and 700 use the retained HDD files.
Do not present these six CAR measurements as SSD measurements.

The run has its own directory of links to the existing input folders. It does
not move or change the archive files. `input-storage.json` records the exact
source folder and storage type for every epoch and format.

## Readers and measurement

Build eight dedicated Linux release binaries from the current worktree at
base commit `b5499e696e79d9ca8c9c55e2c6960361d90e4f62`, including its existing
uncommitted fixes. The source patch is stored beside the runner on NAS.
The build uses the same AES/SSE2 flags as the completed V2 run.

V3 uses 12 reader workers. CAR uses its existing ordered reader; the runner
does not pass a worker-count option to CAR. USDC and Pump.fun use the same
shared failed-transaction filter as the new V2 run. Count and FireWatch retain
their existing semantics. The CAR build includes the malformed DataFrame
array rejection fix.

Record elapsed time, TPS, logical source bytes/s, output rows and completeness,
and sampled process CPU, memory, and storage reads. Logical bytes/s is not
physical disk throughput. OS caches are not controlled. Preserve all logs.
The runner compares V3 and CAR counts and output files outside reader timings.
It stops on a reader error or result mismatch. Incomplete metadata coverage
is recorded separately from successful reader execution.

## NAS paths

- Control: `/volume2/blockzilla-bench/control/v3-car-patched-20260906/`
- Results: `/volume2/blockzilla-bench/results/all-v3-car-local-patched-20260906/`
- Progress: `status.json` and `summary.tsv` in the results directory.
- Live log: `run.log` in the control directory.
- PID: `runner.pid` in the control directory.

The separate completed V2 baseline is
`/volume2/blockzilla-bench/results/all-v2-ssd-patched-20260906/`.
This run does not automatically compare against that separate directory.

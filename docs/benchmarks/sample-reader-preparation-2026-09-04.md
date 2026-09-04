# NAS sample-reader preparation — 4 September 2026

Status: **ready; full benchmark not started**.

The Linux package is on the NAS at:

```text
/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-final
```

It contains 12 dedicated reader binaries. The plan has 264 jobs: four workloads,
three formats, 11 epochs, and two sources. Format order is Compact V2, Indexer V3,
then CAR. Within each format, disk precedes public network. Only one job runs
at a time. The [run guide](sample-reader-matrix.md) defines the workload and metric
semantics, cache rules, output checks, and article outline.

## Checks completed

- All 12 Linux binaries built. The same binaries started on the NAS and printed
  their usage text. These examples return exit code 1 for `--help`.
- Eight runner tests passed on both the Mac and NAS. They include the 264-job simulated matrix,
  resume, failed-attempt retry, output mismatch, bucket mismatch, size mismatch,
  and identified HTTP HEAD requests. Simulated jobs are not archive benchmarks.
- The final NAS preflight checked 405 local objects and 405 public objects.
  All required files exist. All checked disk/public lengths match.
- No archive payload was read for preflight. No archive was hashed, repaired,
  copied, or uploaded. The disk mirror uses hard links to the existing files.

Stored publication bytes across all 11 epochs:

| Format | Local bytes | Public bytes |
| --- | ---: | ---: |
| CAR, including slot indexes | 5,290,468,522,053 | 5,290,468,522,053 |
| Compact V2 | 986,971,841,284 | 986,971,841,284 |
| Indexer V3 | 982,373,660,897 | 982,373,660,897 |

These are stored file lengths, not predicted network transfer totals. Selective
reads can transfer much less. Matching lengths do not prove matching contents;
the reader runs and application output checks provide the next validation step.

## Issues found and corrected in preparation

The old NAS source mapping selected V3 builds from before the metadata repair
for epochs 200–800. Fourteen objects differed: transaction directories and outcome
planes. The new mapping uses the existing September 1 rebuilt reader sets, which
match public object sizes. This changed benchmark paths, not archive data.

Python's default HTTP client identification received 403 on both the NAS and Mac.
The preflight now identifies itself as `blockzilla-sample-preflight/1`; public HEAD
requests succeed. No Cloudflare rule changed.

The Linux cross-build initially inherited a Mac C header search path. The build
script clears host-specific include and SDK variables for the cross-build.

## Start when approved

On the NAS:

```sh
cd /volume1/blockzilla/benchmark-results/sample-reader-package-20260904-final
nohup bash run-archive-sample-read-matrix.sh --mode both \
  --archive-root "$PWD/archive" --bin-dir "$PWD/bin" \
  --results-root "$PWD/results" > runner.log 2>&1 < /dev/null &
```

Watch `runner.log` and `results/status.json`. Per-job logs include scan blocks/s,
TPS, and ETA where meaningful. Resource samples include CPU, memory, physical
disk reads, and host receive activity. Final reader metrics separate setup,
scan, total time, logical bytes, local bytes, cache reads, and HTTP body bytes.

The source edits remain on `codex/sample-archive-benchmark`; they have not been
committed or pushed in this preparation step. The deployed package includes the
base source revision, source patch, added progress code, scripts, and guide.

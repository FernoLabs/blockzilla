# Full-epoch comparison from Triton

`run_full_epoch.py` runs the CAR decode probe and pinned Jetstreamer once each
on a complete epoch. The current Jetstreamer pin is Git revision
`048d2cd25af949679c56347440b7cf5a9a900063`, whose package version remains 0.7.0.
This is newer than the published 0.7.0 release used in the earlier strict tests.
It uses the same export contract and source as the
[worker sweep](README-manycore.md). Select the worker settings from passing
prefix tests before starting this run.

The default dataset remains epoch 900. Its independent Compact V2 index defines
431,858 canonical blocks and
476,026,811 transactions. The slot range is `[388800000, 389232000)`.
There are 20 canonical blocks with zero transactions. The plan must include
every index row, including these empty blocks.

Recover the full plan from the accepted index before timing:

```sh
python3 bench/car-export/extract_index_plan.py \
  --index /data/epoch-900-archive-v2-blocks.index \
  --epoch 900 --start-slot 388800000 --blocks 431858 \
  --expected-index-sha256 4488c7cf2750c0a7fe11ae798b9d960ed72a0a9e904aa336a30c35198697d55d \
  --output /data/epoch-900-full-plan.json
```

Use the two release binaries built with Rust 1.98.0 and
`RUSTFLAGS='-C target-cpu=native'`, with the allocator and optimization settings
in the worker sweep instructions. Build the current Jetstreamer adapter from
its checked-in Git pin and lockfile, as described in
[its README](../jetstreamer-reference/README.md). Build the separate output
verifier on the test host:

```sh
rustc --edition=2021 -O bench/car-export/verify_export.rs -o /data/verify-export
```

The verifier has no external dependencies. It checks every transaction in the
export, including slot, index, flags, balance array lengths, exact block counts,
and complete file coverage. It rejects extra or missing bytes and blocks larger
than 64 MiB. It reports counters in JSON and writes progress to its error log.
The control writes the TSV block plan that the verifier reads.

Start with explicit worker settings. The numbers below illustrate the command;
replace them with the settings selected by the completed prefix tests:

```sh
python3 bench/car-export/run_full_epoch.py \
  --car-bin /data/bin/car-decode-reference \
  --jetstreamer-bin /data/bin/jetstreamer-common-export-reference \
  --verifier-bin /data/verify-export \
  --plan /data/epoch-900-full-plan.json \
  --results /data/triton-full-epoch-new-run \
  --car-workers 64 --car-parser-workers 8 --jetstreamer-workers 64
```

## Use another complete epoch

Pass `--dataset-manifest /data/dataset-1000.json` with the matching full plan
to select another independently prepared dataset. The manifest must contain:

| Field | Required value |
| --- | --- |
| `epoch` | Epoch number. |
| `start_slot`, `end_slot_exclusive` | Complete 432,000-slot epoch, starting at `epoch * 432000`. |
| `blocks`, `transactions` | Independently checked full canonical counts. |
| `canonical_index_sha256` | SHA-256 of the complete Compact V2 index used to prepare the plan. |
| `car_bytes` | Exact raw CAR object length. |
| `index_bytes`, `index_sha256` | Length and SHA-256 of Triton's complete raw slot-range index; length is 5,184,000 bytes. |
| `source_sample_bytes` | `65536`; this is also the default when the field is absent. |
| `car_samples` | At least three objects with integer `start` and lowercase `sha256` fields. Offsets must be distinct, sorted, and leave 65,536 bytes within the CAR object. |

All hashes must contain 64 lowercase hexadecimal characters. The canonical
Compact V2 index hash and Triton raw slot-index hash are separate checks. The
plan must contain every canonical row and match the manifest's canonical index
hash and counts. Every listed CAR sample is checked; a manifest can contain
17 samples or more. Extra provenance fields are retained in the results.

For epoch 1000, the independently checked counts are 431,781 blocks and
547,899,266 transactions in `[432000000, 432432000)`. Its canonical index hash
was checked against a current complete HTTP download and the recovered cache.
Historical accepted tests provide separate count evidence. Preserve these
evidence sources in the manifest rather than treating the current hash as a
historical full-export hash.

Use a prepared manifest with actual sample hashes:

```sh
python3 bench/car-export/run_full_epoch.py \
  --car-bin /data/bin/car-decode-reference \
  --jetstreamer-bin /data/bin/jetstreamer-common-export-reference \
  --verifier-bin /data/verify-export \
  --dataset-manifest /data/dataset-1000.json \
  --plan /data/epoch-1000-full-plan.json \
  --results /data/triton-full-epoch-1000-new-run \
  --car-workers 128 --car-parser-workers 128 --jetstreamer-workers 128 \
  --continue-after-reader-failure
```

The controller saves the manifest unchanged as `dataset-manifest.json`, records
its SHA-256 and parsed content, and uses its epoch for every reader command,
receipt check, and source URL. Omitting the option retains the epoch-900 values.

The result directory must be new. The default order is CAR, then Jetstreamer.
Use `--order jetstreamer-first` to reverse it. Only one reader runs at a time.
Jetstreamer uses strict error handling by default. Pass
`--jetstreamer-body-retries` to request its explicit `body-transport` policy.
That policy permits at most two exact response-body errors per worker and slot,
shared across raw-header and block reads. Both occur before callbacks for the
next block and require empty callback/export state. Header format errors are fatal.
Every other reported error is fatal. The controller checks the receipt's policy,
upstream Git revision, retry limit, count, and event samples before it checks the
full output. It saves these fields with the reader result. All exact plan,
callback, native structure, hash, and byte comparison checks remain mandatory.
Permitted retries remain in elapsed time; they do not erase old strict failures.
By default, the first failed reader stops the controller. Use
`--continue-after-reader-failure` to attempt both readers independently when
the first one fails. This option does not accept the failed reader or weaken
any output check. Each successful reader must still pass full structural
verification, its own counters, SHA-256 calculation, and source checks.
The overall result remains `FAILED` if either reader fails. No comparison or
summary is accepted unless both readers pass and their complete exports match.
A stop signal always stops the controller; it does not start the next reader.
Stop other builds, tests, and benchmarks before starting. Each reader has a
two-hour limit. Each verification phase has a one-hour limit. These limits are
configurable. Failed runs keep their exports, receipts, and logs.

Allow space for two large exports and the temporary spool for the active
reader. The runner requires at least 512 GiB free before each reader starts.
This is a preflight threshold, not a bound on export size. The CAR data remains
remote; the runner does not save a local CAR file.

Each reader must match every canonical block and transaction count. The native
verifier checks the full structure of each export and compares the vote and
failure totals with that reader's receipt. The control then computes each
SHA-256 and compares the complete exports byte for byte. The final result
requires matching bytes, hashes, lengths, and counters. No previously accepted
full-epoch export hash is assumed.

Reader timing includes input setup, decoding, spool writes, the ordered output
copy, and final file sync. With multiple CAR parsers, the slot-index download
is also inside timing. Verification and source checks are outside timing.
The report records the actual HTTP transport, requested settings, CPU time,
peak RSS, and mean CPU use. Successful CAR transport retries remain included
in time and are reported. Returned transport or decode errors fail the run.
Reported Jetstreamer errors follow the selected strict or bounded body-transport
policy. Its hidden lower-level retries are not
observed by the adapter.

The controller records its current phase and progress in `status.json`.
It also records the inherited Jetstreamer spawn, connection recycle, and
legacy-index controls. An unset value means the pinned upstream default.
Reader and verifier output remain in each case directory. Source checks compare
the full raw slot index, CAR length, and every manifest CAR sample before the run
and after each reader. As in the prefix test, these checks do not pin every
HTTP response or prove complete CAR identity.

There is one full-epoch measurement per reader. Report it as a single run and
keep it separate from repeated prefix results.

Run the local control and verifier tests:

```sh
python3 -m unittest discover -s bench/car-export -p 'test_run*.py'
rustc --edition=2021 --test bench/car-export/verify_export.rs -o /tmp/verify-export-tests
/tmp/verify-export-tests
```

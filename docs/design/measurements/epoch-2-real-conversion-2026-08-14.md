# Real Compact V2 conversion: epoch 2

Date: 2026-08-14

This is the first complete conversion of a real NAS epoch with the parallel
Compact V2 converter. It is a performance and physical-layout measurement. It
is not publication evidence.

## Input and host

- Source: `<archive-root>/epoch-2`
- Source bytes: 3,161,094,936
- Source blocks: 431,988
- Source wire profile: `archive-v2-may24-pre-unknown-fallbacks-v1`
- Host: Intel i5-1235U, 12 logical CPUs, 7.5 GiB RAM
- Converter workers: 8 total: 4 block workers and 4 page workers
- Pipeline memory budget: 1 GiB
- Service memory limit: 6 GiB

The source has no generation manifest. The run therefore used fixture mode
with the historical message schema named explicitly. The source files were
created on 2026-05-24. The message enum changed on 2026-06-25 without an outer
format-version change. A trial with the Current decoder stopped at block 8,069,
slot 872,069, transaction 45. The explicit May24 decoder passed exact signed
message reconstruction for that transaction and for the complete generation.

## Command profile

```text
blockzilla-index-archive-convert SOURCE OUTPUT \
  --workers 8 \
  --pipeline-memory-limit-mib 1024 \
  --epoch 2 \
  --slots-per-epoch 432000 \
  --fixture-source \
  --fixture-message-schema may24-pre-unknown-fallbacks
```

## Result

| Measure | Result |
| --- | ---: |
| Wall time to completed JSON | 486.50 s |
| Service CPU time | 1,865.57 s |
| Average used CPU cores | 3.83 |
| Peak service memory | 4,702,154,752 bytes |
| Blocks | 431,988 |
| Transactions | 25,536,956 |
| Signatures | 25,536,971 |
| PoH entries | 29,569,805 |
| Account references | 127,684,791 |
| Blocks/s | 887.95 |
| Transactions/s | 52,491.14 |
| Source MB/s | 6.50 |
| Target bytes | 4,477,738,216 |
| Target/source | 141.65% |

The physical validator accepted all 26 required objects under archive ID
`57f769a1e1a583beb867610785eb2c1b`. Publication readiness remains false.

## Target byte distribution

| Group | Bytes | Share |
| --- | ---: | ---: |
| Sidecars | 3,544,598,116 | 79.16% |
| Derived indexes | 473,297,243 | 10.57% |
| Ledger columns | 272,674,033 | 6.09% |
| Block catalog | 124,416,704 | 2.78% |
| Runtime columns | 62,723,448 | 1.40% |
| Dictionaries | 18,970 | <0.01% |

The native PoH object is 1,431,927,012 bytes versus 1,068,339,352 bytes for
the legacy sidecar. The native shredding object is 478,300,800 bytes versus
72,524,182 bytes for the legacy sidecar. Keeping both legacy sidecars by hard
link or content reference would reduce the candidate's logical payload to
about 3,708,373,938 bytes, or 117.31% of this early source. More importantly,
it removes the full PoH rewrite and its global in-memory hash index from the
upgrade path.

## Read check

A full point-read check at slot 864,000 passed:

- selected transaction projection: 5,810 bytes touched in 1.67 ms;
- 54 transactions and 270 account references reconstructed;
- only core, accounts, instructions, and signatures were read;
- full 14-column block decode also passed.

These are warm-cache local NAS timings, not remote-read latency measurements.

## Provisional fleet estimate

The dated canonical inventory is 98.126 decimal TB. At the measured aggregate
rate, one serial conversion stream needs about 174.8 days. A 161.414 GB modern
epoch needs about 6.90 hours by source bytes. Scaling the known modern account
reference load gives about 7.94 hours. Use 7-9 hours as a provisional modern
epoch range only after the PoH memory fix.

The current converter cannot run a modern epoch on this host: epoch 822 has an
11.65 GB PoH file, above total RAM. Therefore the current all-archive ETA is
blocked. The 174.8-day value is a conditional one-job projection after bounded
sidecar handling. Two jobs give an ideal lower bound of 87.4 days, but this has
not been tested against NAS I/O and external-sort pressure.

The inventory has no `archive-v2-generation.json` in any of 1,013 epoch
directories. Production conversion also needs manifest/source receipts or an
equivalent immutable schema-vector binding. Do not delete Compact V2 sources
from this candidate.

# Latest CAR and Foundation upload, 2026-10-04

## Latest CAR job

At 20:26 Europe/Paris, the latest published Old Faithful CAR was epoch 1047.
The source returned HTTP 200 for epoch 1047 and HTTP 404 for epochs 1048 and
1049. Requests used the accepted `aria2/1.36.0` user agent.

Source: `https://files.old-faithful.net/1047/epoch-1047.car`.

Published source records:

- CAR length: 867,791,506,384 bytes.
- Produced blocks: 431,687.
- Transactions: 547,418,301.
- First slot: 452,304,000.
- Last slot: 452,735,999.
- Epoch CID: `bafyreia2h7alrctyiqhn52xwiqawr2se2lsrqqkexn3k3jo2rxmyrvtv6m`.

Blockzilla host: `ach@192.168.1.46`, SSH port 22.

The separate user service is
`blockzilla-latest-car-e1047-20261004.service`. Its job directory is
`/volume1/blockzilla/jobs/latest-cars-20261004-e1047`.

The controller uses the converter from the successful epoch-1041–1046 batch.
Its SHA-256 is
`0ab2f742c5c06170ed9f864dc99e46eec3e5727565109761f1f2c191d0ce801d`.
That earlier batch finished on 2026-10-03 at 15:04 Europe/Paris. The old failed
epoch-1035 service records do not describe the newer batch.

The new job has one download lane and one compact lane, eight decode workers,
and low CPU and disk priority. It retains the source CAR. The completed source
will be `/volume1/blockzilla/old-faithful/epoch-1047.car`. The output will be
`/volume1/blockzilla/archive/epoch-1047`.

The downloader must succeed and match the published length. The builder uses
the epoch-1046 archive for predecessor blockhash data. Before final publication,
the controller checks every compact index row against the published source slot
list, the full transaction count, index order, frame bounds, and complete blob
byte coverage. It syncs the output and writes a durable import receipt. It does
not do a separate cryptographic verification or whole-file hash pass.

The service was active, the downloader was running, and compaction was waiting
for the download at the first check. This is a job-start record. It does not
certify completed download or compaction. Check `download.json`, `compact.json`,
`status.json`, and the phase logs in the job directory for current results.
The transient service survives the chat ending. It is not enabled for reboot,
and the controller does not promise automatic recovery after a host restart.

## Foundation upload

The owner's follow-up identified an existing secret. It was found in
`/Users/augustin/Developement/ferno/ferno-btc/secrets/fondation-s3.txt`. Its endpoint
selects the Foundation bucket `solana-archive-data`. Read and write access were
confirmed. Separate private configurations were installed with directory mode
0700 and file mode 0600. The older archive-machine R2 remote is a different
account.

Two separate upload jobs are now enabled for reboot. The stored archive job
starts with epoch 1046 and has 1,047 retained epochs in its initial plan, about
105.34 TB. It will include epoch 1047 after the CAR producer finishes. The native
job backs up a frozen set of committed compact chunks in standard tar packs.
The namespaces, checks, state paths, and limitations are in
`hivezilla/deploy/foundation-upload/README.md`.

The epoch-1046 index was uploaded. Its remote length, header, and first row
matched the local source. The archive transfer was advancing. These observations
do not certify completion of the full upload.

On the Foundation machine, native compact output was advancing in epoch 1049,
with three slots of lag at the first check. About 1.56 TB was free. The retained
native compact directory used about 882 GiB, recovery data about 180 GiB, and
capture data about 246 GiB. These are observations, not upload completion records.
Raw shred recording had stopped at its cumulative quota. The current native
projection remains noncanonical and does not prove full-epoch shred coverage.

The existing execution-canary publisher does not accept the native compact
source contract. The new native job is a snapshot backup. It retains the original
noncanonical markers and source evidence and does not publish a canonical
archive catalog. CAR-derived stored archives and native compact snapshots use
separate destination prefixes. Neither upload job deletes local source data.

## Upload scope correction, 2026-10-05

The owner requested uploads of only CAR-derived Compact V2 archives. The native
snapshot backup service was stopped and disabled. Already uploaded native packs,
its durable state, and all local source data remain available. Live capture and
compaction were not restarted.

The stored Compact V2 upload continues. Its checked source now has 1,048 epochs,
0 through 1047, totaling 105,496,762,499,833 bytes (105.50 TB). This is compact
archive data and its indexes, dictionaries, signatures, and supporting records.
Raw CAR files are excluded. The new controller accepts only known compact
filenames and excludes five old restart directories that would otherwise stop
the upload. All epoch inventories and existing intent bindings passed before
the separate archive upload service was restarted. Four focused tests passed.

Epoch 1047 completed compaction at 00:49:05 Europe/Paris and has a verified remote
completion receipt for 153,003,758,757 bytes. Epochs 1041 through 1047 had upload
receipts at the scope check; epoch 1040 was uploading. See
`hivezilla/deploy/foundation-upload/evidence/scope-20261005.json`.

## Approved query copy with signatures, 2026-10-05

After reviewing optional files, the owner selected the query copy with signatures.
The stored archive upload resumed and is enabled for reboot. It keeps compact
blocks, metadata, indexes, key/hash dictionaries, all signatures, and supporting
records. It excludes PoH and shredding evidence and the three optional serving
files from future uploads. All local files and previously uploaded cloud extras
remain in place. The selected source size is 92,898,238,532,602 bytes (92.90 TB),
a 12.60 TB reduction. This is not total bucket usage because earlier extra files
remain. The native backup is still disabled, and live compaction was not changed.

The tested controller is `archive_upload-query-signatures-20261005.py`, with
`--profile query-with-signatures` and separate state in `state-query-signatures`.
It preserves the old all-file state and reuses matching objects in the same
destination prefix. New query completion manifests have their own filename.
All 1,048 selected inventories and eight old intent bindings passed. Six focused
checks passed, including lost reply recovery and retained original data. At first
verification, four query epochs had completion receipts and a remote 64-byte
signature sample matched its local source. See
`hivezilla/deploy/foundation-upload/evidence/query-signatures-20261005.json`.

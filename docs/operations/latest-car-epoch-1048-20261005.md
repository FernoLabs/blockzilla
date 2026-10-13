# Epoch 1048 CAR download and compaction, 2026-10-05

At 23:03 Europe/Paris, Old Faithful returned HTTP 200 for epoch 1048 and
HTTP 404 for epochs 1049, 1050, and 1051. Epoch 1048 was absent from the local
CAR and compact archive directories. The machine had 58.44 TB of free space.

Source: [epoch 1048 CAR](https://files.old-faithful.net/1048/epoch-1048.car).
The published CAR length is 734,710,698,166 bytes.

The [source recap](https://files.old-faithful.net/1048/1048.recap.yaml) reports:

- 431,808 blocks.
- 498,029,026 transactions.
- First slot 452,736,000 and last slot 453,167,999.
- Zero transactions with missing metadata.
- Epoch CID `bafyreic5hlq6s4vddxiapj5ypbaboetibk4g3u3bqgmn2tdz4bvw26iede`.

The source first returned HTTP 429 for the small validation files. The new
source helper supports bounded retries and respects Retry-After. At job start,
the recap, CID, and full slot list were available. The helper checked their
epoch, CAR length, matching CID, block count, strict slot order, and slot bounds.
Its check passed on the retained epoch-1047 records and rejected a changed CID.
It did not read or hash either full CAR.

The separate user service is
`blockzilla-latest-car-e1048-20261005.service` on
`ach@192.168.1.46`, SSH port 22. Its job directory is
`/volume1/blockzilla/jobs/latest-cars-20261005-e1048`.
It uses the proven epoch-1047 controller, with the new epoch, length, predecessor,
and validated source counts. The converter SHA-256 remains
`0ab2f742c5c06170ed9f864dc99e46eec3e5727565109761f1f2c191d0ce801d`.

The job has one download lane, one compact lane, and eight decode workers.
It retains the input CAR. The final input will be
`/volume1/blockzilla/old-faithful/epoch-1048.car`, and the compact output will be
`/volume1/blockzilla/archive/epoch-1048`. It uses epoch 1047 for predecessor
blockhash data. The existing downloader success and exact-length checks remain.
Before compact publication, it checks every compact index row against the
published slot list, the transaction count, frame bounds, and full blob byte
coverage. It writes the import receipt and syncs the output before the final
directory rename. No extra full-file hash or separate cryptographic check was
added.

At the 23:08 check, the service and downloader were active. The latest download
log sample had 7.5 GiB of 684 GiB and 231 MiB/s, with about 50 minutes left.
Compaction was WAITING_DOWNLOAD. These are startup observations, not completed
download or compaction results. Read the lane JSON records and phase logs for
current progress. The overall status stays STARTING until the existing
controller writes its final result. The transient service survives the end of
the chat; it is not enabled for reboot.

The Foundation archive upload remained active with the same process.
Its frozen cloud plan still contains epochs 0 through 1047. This new job
downloads and compacts epoch 1048 locally; it does not add that epoch to the
existing cloud plan.

Evidence: `hivezilla/deploy/foundation-upload/evidence/latest-car-e1048-20261005.json`.

## Completion checked on 2026-10-06

The download finished on 2026-10-05 at 23:49:21 Europe/Paris. Compaction
finished on 2026-10-06 at 01:43:52 Europe/Paris. At the 08:36 check, both lane
records and the overall job were COMPLETE with no errors. The service had
exited successfully, and the final archive directory and import receipt were
present.

The compact index matched all 431,808 published block slots and 498,029,026
transactions. Its signature count was 519,913,704. The compressed block blob
was 77,746,841,009 bytes; this is not the size of the full compact directory.
The source CAR remains at its final local path. This completion proves the
recorded local import checks. It does not record an epoch-1048 cloud upload.

Completion evidence:
`hivezilla/deploy/foundation-upload/evidence/latest-car-e1048-complete-20261006.json`.

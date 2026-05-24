gm I already have some slot-offset index if that can help.

The current files are `epoch-N-slot-ranges.raw`: one fixed-size slot-to-CAR-range
index per epoch.

Each epoch is constant size because it stores one row for every possible slot:

```text
432,000 slots * 12 bytes = 5.18 MB per epoch
epochs 0..967 = 5.02 GB total
```

The row format is just:

```text
offset:u64_le + len:u32_le
```

Skipped slots are encoded as `offset = 0, len = 0`.

Download link:
https://ug.link/ef755jj41243d0f5/filemgr/share-download/?id=c1035e9dcb0c4b28a2c011671d66e76a

I am also building a v2 index with `previous_blockhash` in the row:

```text
offset:u64_le + len:u32_le + previous_blockhash:[u8;32]
44 bytes per slot = 19.01 MB per epoch
epochs 0..967 = 18.40 GB total
```

That should let `getBlock` do one index read, then one block-range read, with no
extra parent-block lookup just to recover `previousBlockhash`.

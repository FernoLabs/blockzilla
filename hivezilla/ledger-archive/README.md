# hivezilla-ledger-archive

`check-ledger-range` checks one inclusive slot range in an Agave ledger and prints a report. `build-epoch-archive` writes a Compact V2 archive of one epoch. The ledger is opened read-only and is not modified.

```text
hivezilla-ledger-archive check-ledger-range <LEDGER> --start-slot N --end-slot M
hivezilla-ledger-archive build-epoch-archive <LEDGER> <OUTPUT-PARENT> \
  --epoch N --level 1 --read-threads 1
```

`check-ledger-range` applies the slot rules and prints a report.

## build-epoch-archive

Write one epoch from a stopped ledger or a snapshot. The ledger is opened read-only.

The epoch has 432000 slots, from `N * 432000` through `N * 432000 + 431999`. The archive is written to `<OUTPUT-PARENT>/epoch-N`. That directory is created only when the ledger still stores the first slot and a root has reached the last slot. An existing final archive file in that directory stops the command.

`--level` is the zstd effort. The default is 1. `--read-threads` is the number of parallel shred reads, from 1 to 32. The default is 1.

Each rooted block becomes one independent zstd frame in `archive-v2-blocks.zstd`. The command then writes these files in the same directory:

- `archive-v2-blocks.index`
- `signatures.bin`
- `blockhash_registry.bin`
- `vote_hash_registry.bin`
- `poh.wincode`
- `shredding.wincode`
- `registry.bin`
- `prev_blockhash_tail.bin`

A failure removes the temporary files and does not publish the index. The command prints the block count, the proven skips, the abandoned forks, the first block, the last block, the boundary hash, and the output path.

A slot is written only when it is rooted and its shreds are complete. The tool then checks that every transaction has a status, and that the parent slot and parent blockhash chain to the previous written block.

Slots with no `SlotMeta` are not written. A skip reward in the block at slot N+8 records a skip. A notarization reward alone is an error. When that footer contains both rewards, the slot is a skip only if the next rooted block jumps over it. When slot N+8 has no footer, the same parent jump records a skip. A rooted block that is not full, or a hole that the next rooted block descends from, stops the run.

A slot that has data but is not rooted is left out only when the next rooted block jumps over it and slot N+8 carries a skip reward. The same sentence is printed for that case and for a parent-jump hole: `slot N left out, rooted chain jumps to M`. A skip reward alone prints nothing.

The tool does not verify BLS vote signatures and does not recompute the bank hash. Agave already did both before it rooted the slot.

The slot rules were run on one full epoch, epoch 1077, slots 465264000–465695999. The scan finished with 432000 slots scanned and 431628 rooted blocks accepted. The other 372 slots were proven skips or abandoned forks.

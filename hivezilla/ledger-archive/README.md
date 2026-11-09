# hivezilla-ledger-archive

Reads one inclusive slot range from an Agave ledger and writes a Compact V2 archive of the rooted blocks. The ledger is opened read-only and is not modified.

```text
hivezilla-ledger-archive check-ledger-range <LEDGER> --start-slot N --end-slot M
hivezilla-ledger-archive build-archive-v2-from-ledger <LEDGER> <OUTPUT> \
  --start-slot N --end-slot M --level 1 --read-threads 1
```

`check-ledger-range` applies the slot rules and prints a report. `build-archive-v2-from-ledger` writes one independent zstd frame per rooted block, then the index. `--level` is the zstd effort (default 1). `--read-threads` is the number of parallel shred reads, from 1 to 32. A failure removes the temporary files and does not publish the index.

A slot is written only when it is rooted and its shreds are complete. The tool then checks that every transaction has a status, and that the parent slot and parent blockhash chain to the previous written block.

Slots with no `SlotMeta` are not written. A skip reward in the block at slot N+8 records a skip. A notarization reward alone is an error. When that footer contains both rewards, the slot is a skip only if the next rooted block jumps over it. When slot N+8 has no footer, the same parent jump records a skip. A rooted block that is not full, or a hole that the next rooted block descends from, stops the run.

A slot that has data but is not rooted is left out only when the next rooted block jumps over it and slot N+8 carries a skip reward. The same sentence is printed for that case and for a parent-jump hole: `slot N left out, rooted chain jumps to M`. A skip reward alone prints nothing.

The tool does not verify BLS vote signatures and does not recompute the bank hash. Agave already did both before it rooted the slot.

The slot rules were run on one full epoch, epoch 1077, slots 465264000–465695999. The scan finished with 432000 slots scanned and 431628 rooted blocks accepted. The other 372 slots were proven skips or abandoned forks.

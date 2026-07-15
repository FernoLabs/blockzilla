# Epoch 208 PoH Gap Override

## Context

Epoch 208 contains a documented Old Faithful source-data gap where some canonical
blocks have BigTable/RPC block data but no warehouse PoH entries. Slot 89856302
reproduced this: redownloading `epoch-208.car` and rebuilding sidecars still
left the block with zero PoH entries, while RPC returned a canonical blockhash.

## Fix

`build-archive-v2-hot-blocks`, `build-archive-v2-registries`, and
`build-blockhash-registry` now accept `--external-blockhashes <path>`.

The file format is one audited override per line:

```text
slot blockhash provenance [ticks=N hashes_per_tick=N]
```

The override is only accepted when the CAR block has no PoH entries. If a block
has entries and an override is present, the build hard-stops. If a block has no
entries and no override, the build also hard-stops. When `ticks=` and
`hashes_per_tick=` are present, the builder reconstructs tick-only PoH entries
from the previous blockhash and accepts them only if the final reconstructed
hash equals the audited blockhash.

For slot 89856302 the expected row is:

```text
89856302 EUcKXas8biYoZpbRKiRZrqPstyeQC5SGebAAHCYM8R1E rpc-mainnet-empty-poh-gap-epoch-208 ticks=64 hashes_per_tick=12500
```

## Local Reconstruction Check

Using RPC `previousBlockhash` for slot 89856302:

```text
D8tfAZNuBAvM3YjmuCv7wHVyFLMph39A6gVUWFaAdt7t
```

and applying sequential SHA-256 hashing locally reaches the RPC blockhash after
exactly `800000` hashes:

```text
EUcKXas8biYoZpbRKiRZrqPstyeQC5SGebAAHCYM8R1E
```

`800000 = 64 * 12500`, matching the expected tick-only shape for an empty slot.
This proves the total PoH hash count for this empty block, but it should still
be recorded as reconstructed PoH rather than source PoH because the CAR did not
contain the original entry nodes.

# blockzilla-format

`blockzilla-format` contains the shared Archive V2 types, codecs, readers,
writers, indexes, and registries used by Blockzilla and Archive V2 consumers.
It also contains the bounded Replay V1 hot-payload codec used by the ongoing
shred-to-replay work.

Replay V1 payload format 8 is reserved but intentionally not accepted by the
stream/job validators yet. The codec alone is not an activation: registry and
blockhash resolution, signed-message expansion, status evidence, pinned Agave
fixtures, and final-byte replay validation must land first. See the
[Replay Projection V1 specification](../../docs/design/blockzilla-replay-projection-v1.md).

Archive V2 is pre-1.0. Pin the Git revision that produced an archive and follow
the [implemented format reference](../../docs/reference/archive-v2-hot-block-format.md)
before writing an independent reader.

Archive V2 is frozen for compatibility work while the separate
[`blockzilla-index-archive-format`](../blockzilla-index-archive-format/README.md)
crate defines its indexer-first replacement. New Archive V2 payload shapes
must not be added without an explicit migration decision.

The `compact` and `split_compact` modules remain for compatibility with older
readers. The current CLI writes Archive V2.

The versioned `block-time-gaps.bin` codec is also public. It stores sparse slot
and whole-second time discontinuities, their archived boundary times, and a
source SHA-256. Consumers should present these as archive observations rather
than confirmed network outages. See the
[binary format reference](../../docs/reference/block-time-gap-sidecar.md).

## Check

```bash
cargo test --locked -p blockzilla-format
```

# blockzilla-format

`blockzilla-format` contains the shared Archive V2 types, codecs, readers,
writers, indexes, and registries used by Blockzilla and Archive V2 consumers.

Archive V2 is pre-1.0. Pin the Git revision that produced an archive and follow
the [implemented format reference](../../docs/reference/archive-v2-hot-block-format.md)
before writing an independent reader.

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

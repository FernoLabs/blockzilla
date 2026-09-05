# Archive V2 metadata normalization

## Goal

All new Archive V2 generations use one metadata error format:
`current-typed-errors-v1`.

Some historical generations use a raw serialized transaction error when
`CompactMetaV1.err` is present. The current format stores a typed
`CompactTransactionError` directly. A metadata record with no transaction
error has the same wire format in both generations.

The historical raw-error format is a compatibility input only. It is not a
valid output format for a new published generation.

## Format authority

A current generation binds this exact marker in its official generation
manifest:

```text
archive-v2-metadata-schema-current-typed-errors-v1.marker
```

The marker is not sufficient by itself. Before publication, the complete
generation must pass the exact metadata audit. The audit rejects these
conditions:

- a legacy-only metadata record;
- two valid decodes with different values;
- an invalid metadata record;
- a raw metadata fallback.

Normal readers require the marker and use the current-only decoder. An
unmarked historical generation needs the explicit compatibility admission
path.

## Tools

### Source authority inventory

`archive-v2-build-source-authority` creates an external, content-bound
inventory for an unmanifested local generation. It does not change the source.

The inventory binds:

- cluster, epoch, and slot-range identity;
- the complete message profile;
- the explicit historical metadata admission;
- each admitted file name, size, SHA-256 value, and migration role;
- a domain-separated inventory digest.

The tool always includes the four core Archive V2 files. The operator must add
each known sidecar or control file with a repeated `--include` option. The tool
does not authorize files only because they exist in the directory.

The inventory output must be outside the source tree. The tool publishes it
with a no-replace atomic operation and prints its SHA-256 value. Supply both
the inventory path and this SHA-256 value to the normalizer.

### Metadata normalizer

`archive-v2-normalize-metadata` reads one admitted generation and creates one
new private staging directory. It never changes the source.

The normalizer:

1. opens the source through one no-follow directory capability;
2. admits either an official manifest or the external authority inventory;
3. validates the complete source structure and metadata;
4. rewrites only historical transaction-error fields;
5. preserves message bytes, transaction order, signatures, registries, and
   other admitted sidecars;
6. rebuilds block and get-block geometry when compressed frame sizes change;
7. opens and audits the complete target with the strict current-only reader;
8. writes an unpublished candidate record and completion receipt last.

The staging directory does not contain an official generation manifest,
profile marker, or publication lock. Therefore, an incomplete or complete
staging directory is not a published generation.

## Publication boundary

Normalization and publication are separate operations. A publisher must:

1. open the completed candidate through pinned file descriptors;
2. verify every file hash and the normalization receipt;
3. run the complete current metadata audit again;
4. write the exact message and metadata markers;
5. write the official generation manifest last;
6. atomically advance the catalog to the new immutable generation.

Rollback creates a new catalog entry that points to the prior immutable
generation. Do not edit or delete the new or old generation in place.

## NAS rollout rule

Do not normalize or publish an epoch while another process reads its active
path. Finish the current archive dump first. Then use a stable source snapshot
or an equivalent read-only source lease for the complete audit and rewrite.

Run the exact classifier over every epoch before the cutover. Rewrite only
epochs that contain legacy-only records. A current-only epoch still needs a
complete audit before it can receive the current metadata marker.

Epoch 900 contains a confirmed historical raw-error metadata record. Current
evidence does not prove that any other epoch contains this format. Do not infer
the metadata profile from the message profile; they are independent.

## Recovery rules

- Never reuse an existing staging path.
- Keep incomplete staging directories for inspection or quarantine them.
- Treat the external source inventory and its caller-supplied SHA-256 as one
  authority pair.
- Do not publish a candidate without its completion receipt.
- Do not trust unknown or unbound files in a source directory.
- Do not perform an in-place block/index pair swap while readers are active.

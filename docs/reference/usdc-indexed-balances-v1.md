# Indexed USDC balances, version 1

The optional V2 reader example writes selected recorded balances with compact
account references and a separate public-key dictionary. The archive format is
unchanged. The existing `BZUSDC02` balance output remains available.

All integers below are unsigned and big-endian. A file contains its fixed-size
header followed by fixed-size records. There is no SQLite database or implicit
resume. The example refuses existing output paths.

## Source and completion

Both binary headers contain the same target mint and 32-byte source-scope digest.
This digest is SHA-256 of the bytes
`blockzilla.indexed-token-source-metadata.v1\0` followed by the exact bytes of
the `.source.json` sidecar. The `\0` denotes one zero byte.

The source metadata records the source identity, registry entry count, and
admitted registry identity. For local files this is the pinned device, inode,
length, and timestamps. For network sources it is the exact registry URL,
length, and strong ETag. These are metadata bindings, not a claim that the
registry content was hashed. IDs from another source scope cannot be used with
this dictionary.

The `.complete.json` sidecar records both binary schemas, lengths, row counts,
SHA-256 digests, and coverage. It is written only after both binary files finish,
the scan totals agree, and local source pins pass their final check. A completed
file can still have incomplete source coverage, reported separately. Dictionary
entries precede dependent balance rows logically; buffered writes do not promise
power-loss ordering. Record-aligned truncation requires completion and hash
checks, not only a structural parser. The expansion CLI performs these checks.

## Header, 76 bytes

| Offset | Bytes | Value |
| ---: | ---: | --- |
| 0 | 8 | Data magic `BZUSCI01`, or dictionary magic `BZUSDI01` |
| 8 | 4 | Record size: 70 for data, 60 for dictionary |
| 12 | 32 | Selected mint public key |
| 44 | 32 | Source-scope metadata digest |

## IDs and dictionary records, 60 bytes

IDs 1 through `u32::MAX` retain the archive's registry IDs. Zero means an absent
optional reference. Inline raw keys use a separate namespace starting at
`2^32`, assigned in first-observation order. A raw reference and a registry
reference can identify the same public key while retaining different IDs.

Each reference is resolved and written once per output dictionary. Registry
references are compared as numeric IDs. Only inline raw references need a
public-key map. The dictionary grows with the number of distinct references;
this is not a claim of constant-memory account discovery.

| Offset | Bytes | Value |
| ---: | ---: | --- |
| 0 | 8 | Dictionary ID |
| 8 | 32 | Public key |
| 40 | 8 | First-observed epoch |
| 48 | 8 | First-observed slot |
| 56 | 4 | First-observed transaction index |

The dictionary includes token accounts and referenced mint, owner, and program
keys. These observations do not prove account initialization. Failed
transactions do not add discovered accounts. A missing mint contributes coverage
information instead of an invented account mapping. For cross-epoch
reconciliation, compare public keys and retain each source-scoped ID mapping.
An optional future SQL table would need a key such as `(source_scope, id)`;
`id` alone is not unique across archives.

## Balance records, 70 bytes

| Offset | Bytes | Value |
| ---: | ---: | --- |
| 0 | 8 | Epoch |
| 8 | 8 | Slot |
| 16 | 4 | Transaction index |
| 20 | 1 | Side: 0 pre, 1 post |
| 21 | 4 | Original position in the source pre/post balance list |
| 25 | 4 | Transaction-local account position |
| 29 | 8 | Actual token-account ID |
| 37 | 8 | Mint ID |
| 45 | 8 | Owner ID, or zero when absent |
| 53 | 8 | Token-program ID, or zero when absent |
| 61 | 8 | Recorded amount |
| 69 | 1 | Decimals |

The reader maps the transaction-local account position through static accounts,
then writable loaded accounts, then readonly loaded accounts. The owner field
is never used as the token account. Rows retain source order and original
balance positions after mint filtering.

## Expansion and coverage

Expansion resolves the four references through the matching dictionary and
writes the existing `BZUSDC02` rows. That format has no token-account public-key
field, but expansion still checks that the new token-account ID is valid.
Missing references, duplicate IDs, mismatched source scopes, invalid ordering,
invalid side tags, and truncated records cause an error. The CLI also checks
the completion record and streaming file hashes. Output written before an error
is incomplete and must be discarded.

Known failures are excluded. Unknown execution is never treated as failure.
The existing USDC coverage counters and digest remain in the scan report and
completion record. The source API validates complete typed metadata and row
flags before it exposes a block. Fixed worker account storage and reusable flat
balance buffers avoid allocating a small vector for each selected transaction.
Buffers that cross worker boundaries own their data until ordered consumption;
the API lends references for the duration of each callback.

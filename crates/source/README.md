# Byte sources

These crates read immutable named objects. They do not decode archive records.

| Crate | Role |
| --- | --- |
| `blockzilla-source` | Range contract, errors, and safe object names |
| `blockzilla-source-local` | Local, pinned, and overlay sources |
| `blockzilla-source-http` | HTTP ranges, object identity, and bounded retries |
| `blockzilla-source-cache` | Persistent whole-object HTTP cache |

Local and HTTP sources depend on the contract. The cache depends on the
contract and HTTP source. No source crate depends on a format or reader.

HTTP retains the existing archive-gateway manifest route for compatibility.
The cache is a whole-object mirror; it is not a range cache. Archive readers
select which sidecars to cache and keep their own format validation.

# hivezilla-object-store

Provider-neutral immutable object operations shared by Hivezilla custody and
recovery components. The crate supplies deterministic memory and filesystem
implementations for conformance and crash-boundary tests; production provider
adapters are intentionally separate.

Creating or verifying an object through this API does not make data terminally
protected, advance an ACK, authorize source retirement, or commit an archive
catalog entry. Those decisions require their respective protocol records.

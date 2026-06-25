# TODO

- Evaluate `rkyv` for zero-copy / archive-friendly layouts
- Major repo clean up
  - application and tools in there own folder
  - crates/
    - blockzilla/
    - oldfiathtull/
    - hivezilla/

project should expose simple read format cli management for blockziall format
* unpack / repoack
* index reader

# Backlog

- Archive V2 durable nonce follow-up
  - Verify `OwnedCompactRecentBlockhash::Nonce` values against nonce-account state so raw recent-blockhash fallback cannot hide a parser/modeling mistake.
  - Add/extend nonce-account indexing; durable nonce accounts will likely need their own lookup path.

- explore seekable zstd https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md


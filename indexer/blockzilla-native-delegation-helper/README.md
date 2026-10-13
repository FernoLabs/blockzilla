# Native delegation frame helper

This diagnostic reads native processed compact frames and returns decoded JSON
for successful transactions that refer to the configured deBridge state account.

The caller must first select the frame through a frozen rooted ancestry record
and verify the SHA-256 of the complete frame. The helper checks frame bounds,
the native source profile, object layout, required files, and object digests.
These checks do not establish rooted ancestry on their own.

Each input is a four-byte little-endian frame length followed by the frame.
Each output is a four-byte little-endian JSON length followed by JSON bytes.
A frame decode error returns an error JSON record. Invalid transport framing
stops the process. Input frames and output records have finite size limits.

Build and test from this directory with `cargo test --locked` and
`cargo build --release --locked`. This crate has its own workspace and lock file.

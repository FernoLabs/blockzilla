//! Byte-level contracts for the Blockzilla Index Archive.
//!
//! The public surface is explicit. The crate does not glob-export physical
//! rows or legacy formats.

pub mod blobs;
pub mod catalog;
pub mod dictionary;
pub mod header;
pub mod indexes;
pub mod layout;
pub mod ledger;
pub mod runtime;
pub mod sidecars;
pub mod varint;
pub mod wincode;

pub use header::{FILE_HEADER_LEN, FILE_MAGIC, FileHeader, HeaderError};
pub use layout::{
    ArchiveId, ArchiveIdError, CanonicalFact, FORMAT_ID, FORMAT_MAJOR, FileClass, FileEncoding,
    LAYOUT, LayoutError, ObjectRole, ObjectSpec, PathError, Requirement, UnknownObjectRole,
    object_by_path, object_by_role, validate_archive_path, validate_layout,
};

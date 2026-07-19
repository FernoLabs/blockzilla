use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto/raw_replication.proto");

    let manifest_dir =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set"));
    let proto_dir = manifest_dir.join("proto");
    let proto = proto_dir.join("raw_replication.proto");

    // Configure this compiler invocation directly. Mutating the process-wide PROTOC environment
    // is both unnecessary and unsafe when build scripts become multi-threaded.
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(prost, &[proto], &[proto_dir])?;
    Ok(())
}

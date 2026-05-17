use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=src/confirmed_block.proto");

    prost_build::compile_protos(&["src/confirmed_block.proto"], &["src/"])?;

    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"));
    let quick_out_dir = out_dir.join("quick-protobuf");
    std::fs::create_dir_all(&quick_out_dir)?;

    let manifest_dir =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set"));
    let proto = manifest_dir.join("src/confirmed_block.proto");
    let proto_dir = manifest_dir.join("src");
    let protos = [proto];
    let includes = [proto_dir];
    let config = pb_rs::ConfigBuilder::new(&protos, None, Some(&quick_out_dir), &includes)
        .expect("quick-protobuf config")
        .build();
    pb_rs::types::FileDescriptor::run(&config).expect("quick-protobuf codegen");

    Ok(())
}

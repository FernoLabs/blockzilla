use std::io::Result;

fn main() -> Result<()> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    println!("cargo::warning=env {out_dir}");
    prost_build::compile_protos(&["src/confirmed_block.proto"], &["src/"])?;
    Ok(())
}

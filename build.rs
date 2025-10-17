use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["src/confirmed_block.proto"], &["src/"])?;
    Ok(())
}

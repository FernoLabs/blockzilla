fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["src/confirmed_block.proto"], &["src/"])?;
    Ok(())
}

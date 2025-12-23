pub mod compact_block;
pub mod compact_log;
pub mod meta_decode;
pub mod optimized_cbor;
pub mod partial_meta;
pub mod transaction_parser;

pub mod confirmed_block {
    include!(concat!(
        env!("OUT_DIR"),
        "/solana.storage.confirmed_block.rs"
    ));
}

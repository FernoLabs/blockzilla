pub mod node;
pub mod car_block_reader;
pub mod open_epoch;
//pub mod rpc_block;

pub mod confirmed_block {
    include!(concat!(
        env!("OUT_DIR"),
        "/solana.storage.confirmed_block.rs"
    ));
}

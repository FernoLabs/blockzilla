pub mod block_stream;
pub mod car_reader;
pub mod node;
pub mod rpc_block;

pub mod confirmed_block {
    include!(concat!(env!("OUT_DIR"), "/solana.storage.confirmed_block.rs"));
}

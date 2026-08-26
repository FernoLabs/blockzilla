mod block;
mod http;
mod status;

pub(crate) use block::{FetchedBlock, get_block};
pub(crate) use status::get as get_status;

const V2_SLOT_INDEX_PREFIXES: [&str; 2] = ["slot-index-v2", "slot-index"];

pub(super) fn v2_slot_index_keys(epoch: u64) -> [String; 2] {
    V2_SLOT_INDEX_PREFIXES.map(|prefix| format!("{prefix}/epoch-{epoch}-slot-ranges-v2.raw"))
}

#[cfg(test)]
mod tests {
    use super::v2_slot_index_keys;

    #[test]
    fn checks_the_production_v2_prefix_before_the_legacy_prefix() {
        assert_eq!(
            v2_slot_index_keys(7),
            [
                "slot-index-v2/epoch-7-slot-ranges-v2.raw",
                "slot-index/epoch-7-slot-ranges-v2.raw",
            ]
        );
    }
}

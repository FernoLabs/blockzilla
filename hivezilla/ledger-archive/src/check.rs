use anyhow::{bail, Context, Result};

use crate::markers::reward_names;
use crate::source::{LedgerView, REWARD_SLOT_DISTANCE};

/// Blocks and proven skips for one inclusive slot window.
///
/// `check-ledger-range` and `build-archive-v2-from-ledger` both call this
/// function. A check that passes on a snapshot will not later stop the build
/// of that same snapshot for a hole, a missing status, or a missing parent.
#[derive(Debug, Clone)]
pub(crate) struct Inspected {
    pub block_slots: Vec<u64>,
    pub skips: Vec<u64>,
    pub boundary_hash: [u8; 32],
}

pub(crate) fn inspect(view: &LedgerView) -> Result<Inspected> {
    if view.start_slot > view.end_slot {
        bail!(
            "start slot {} is after end slot {}",
            view.start_slot,
            view.end_slot
        );
    }

    let mut block_slots = Vec::new();
    let mut skips = Vec::new();
    for slot in view.start_slot..=view.end_slot {
        if view.slots.contains_key(&slot) {
            accept_block(view, slot, &block_slots)?;
            block_slots.push(slot);
        } else {
            classify_hole(view, slot, &mut skips)?;
        }
    }

    let Some(first_slot) = block_slots.first().copied() else {
        bail!(
            "slots {} to {} have no block",
            view.start_slot,
            view.end_slot
        );
    };
    let boundary_hash = view.slots[&first_slot]
        .previous_blockhash
        .context("first block is missing parent entries")?;

    Ok(Inspected {
        block_slots,
        skips,
        boundary_hash,
    })
}

fn accept_block(view: &LedgerView, slot: u64, produced: &[u64]) -> Result<()> {
    let data = &view.slots[&slot];
    if !data.is_root {
        bail!("slot {slot} has data and is not rooted");
    }
    if !data.is_full {
        bail!("slot {slot} is rooted and is not full");
    }
    let Some(parent_slot) = data.parent_slot else {
        bail!("slot {slot} has no parent slot");
    };
    if parent_slot >= slot {
        bail!("slot {slot} parent {parent_slot} is not before the slot");
    }
    let Some(previous_hash) = data.previous_blockhash else {
        bail!("slot {slot} is missing parent entries");
    };
    if !data.markers.is_empty() && data.consensus_block_id.is_none() {
        bail!("slot {slot} has block markers and no consensus block id");
    }

    if let Some(previous_slot) = produced.last().copied() {
        let previous = &view.slots[&previous_slot];
        if parent_slot != previous.slot {
            bail!(
                "slot {slot} parent {parent_slot} is not the previous block {}",
                previous.slot
            );
        }
        if previous_hash != previous.blockhash {
            bail!(
                "slot {slot} previous blockhash does not match slot {}",
                previous.slot
            );
        }
    } else if (view.start_slot..=view.end_slot).contains(&parent_slot) {
        bail!("slot {slot} parent {parent_slot} is inside the range and is not a block");
    }

    for (index, tx) in data.transactions.iter().enumerate() {
        if tx.status.is_none() {
            bail!("slot {slot} transaction {index} is missing status");
        }
    }
    Ok(())
}

fn classify_hole(view: &LedgerView, slot: u64, skips: &mut Vec<u64>) -> Result<()> {
    let carrier = slot
        .checked_add(REWARD_SLOT_DISTANCE)
        .context("slot + 8 overflows")?;
    let Some(data) = view.slots.get(&carrier) else {
        bail!("slot {slot} has no SlotMeta and slot {carrier} has no footer proof");
    };
    let names = reward_names(&data.markers)
        .with_context(|| format!("slot {carrier} footer cannot prove slot {slot}"))?;
    if names.notars.contains(&slot) {
        bail!("slot {slot} is missing and slot {carrier} has a notarization reward for it");
    }
    if names.skips.contains(&slot) {
        skips.push(slot);
        return Ok(());
    }
    bail!("slot {slot} has no SlotMeta and slot {carrier} does not prove a skip");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{test_block, SlotData};

    fn view(start: u64, end: u64, slots: Vec<SlotData>) -> LedgerView {
        LedgerView {
            start_slot: start,
            end_slot: end,
            slots: slots.into_iter().map(|slot| (slot.slot, slot)).collect(),
        }
    }

    fn header_marker(parent_slot: u64, parent_id: [u8; 32]) -> Vec<u8> {
        let mut payload = vec![1u8];
        payload.extend_from_slice(&parent_slot.to_le_bytes());
        payload.extend_from_slice(&parent_id);
        marker(1, &payload)
    }

    fn skip_footer(skipped_slot: u64) -> Vec<u8> {
        let mut certificates = vec![0, 1];
        certificates.extend_from_slice(&skipped_slot.to_le_bytes());
        certificates.extend(std::iter::repeat_n(7u8, 96));
        certificates.push(0);
        certificates.push(0);
        footer_marker(&certificates)
    }

    fn notar_footer(notarized_slot: u64) -> Vec<u8> {
        let mut certificates = vec![0, 0, 1];
        certificates.extend_from_slice(&notarized_slot.to_le_bytes());
        certificates.extend(std::iter::repeat_n(9u8, 32));
        certificates.extend(std::iter::repeat_n(8u8, 96));
        certificates.push(0);
        footer_marker(&certificates)
    }

    fn footer_marker(certificates: &[u8]) -> Vec<u8> {
        let mut payload = vec![1u8];
        payload.extend(std::iter::repeat_n(0xabu8, 32));
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.push(0);
        payload.extend_from_slice(certificates);
        marker(0, &payload)
    }

    fn marker(variant: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1u16.to_le_bytes());
        out.push(variant);
        out.extend_from_slice(&(u16::try_from(payload.len()).unwrap()).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn missing_status_is_an_error() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.transactions.push(crate::source::SourceTx {
            signatures: vec![[4; 64]],
            message: crate::source::SourceMessage::Legacy(crate::source::SourceMessageBody {
                header: crate::source::SourceHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: Vec::new(),
                recent_blockhash: [1; 32],
                instructions: Vec::new(),
                lookups: Vec::new(),
            }),
            status: None,
        });
        block.entries[0].tx_count = 1;

        let error = inspect(&view(10, 10, vec![block])).unwrap_err();
        assert!(
            error.to_string().contains("missing status"),
            "{error:#}"
        );
    }

    #[test]
    fn missing_parent_entries_are_an_error() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.previous_blockhash = None;
        let error = inspect(&view(10, 10, vec![block])).unwrap_err();
        assert!(
            error.to_string().contains("missing parent entries"),
            "{error:#}"
        );
    }

    #[test]
    fn a_proven_skip_is_omitted() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut second = test_block(12, 10, [5; 32], [4; 32]);
        second.entries[0].hash = [5; 32];
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.markers = vec![skip_footer(11)];
        carrier.is_root = false;

        let inspected = inspect(&view(10, 12, vec![first, second, carrier])).unwrap();
        assert_eq!(inspected.block_slots, vec![10, 12]);
        assert_eq!(inspected.skips, vec![11]);
        assert_eq!(inspected.boundary_hash, [3; 32]);
    }

    #[test]
    fn a_notarized_missing_slot_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.markers = vec![notar_footer(11)];

        let error = inspect(&view(10, 12, vec![first, second, carrier])).unwrap_err();
        assert!(
            error.to_string().contains("notarization reward"),
            "{error:#}"
        );
    }

    #[test]
    fn an_unproven_hole_is_an_error() {
        let block = test_block(10, 9, [4; 32], [3; 32]);
        let error = inspect(&view(10, 11, vec![block])).unwrap_err();
        assert!(error.to_string().contains("does not prove a skip") || error.to_string().contains("no footer proof"), "{error:#}");
    }

    #[test]
    fn a_non_root_slot_is_an_error() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.is_root = false;
        let error = inspect(&view(10, 10, vec![block])).unwrap_err();
        assert!(error.to_string().contains("not rooted"), "{error:#}");
    }

    #[test]
    fn a_rooted_partial_slot_is_an_error() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.is_full = false;
        let error = inspect(&view(10, 10, vec![block])).unwrap_err();
        assert!(error.to_string().contains("not full"), "{error:#}");
    }

    #[test]
    fn markers_without_a_consensus_id_are_an_error() {
        let mut block = test_block(10, 9, [2; 32], [1; 32]);
        block.markers = vec![header_marker(9, [7; 32])];
        let error = inspect(&view(10, 10, vec![block])).unwrap_err();
        assert!(
            error.to_string().contains("no consensus block id"),
            "{error:#}"
        );
    }

}

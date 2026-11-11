use anyhow::{Context, Result, bail};

use crate::markers::reward_names;
use crate::source::{LedgerView, REWARD_SLOT_DISTANCE};

/// Blocks, proven skips, and abandoned forks for one inclusive slot window.
///
/// The check command and the build command apply the same slot rules. A check
/// that passes on a snapshot will not later stop the build of that same
/// snapshot for a hole, a missing status, or a missing parent.
#[derive(Debug, Clone)]
pub(crate) struct Inspected {
    pub block_slots: Vec<u64>,
    pub skips: Vec<u64>,
    /// Slots that have data, are not root, and are left out of the archive.
    /// The next root jumps over each one. Slot N+8 has a skip certificate, or
    /// it has no `SlotMeta`, or it is not root.
    pub forks: Vec<u64>,
    pub boundary_hash: [u8; 32],
}

/// Where `slot` sits relative to the next rooted slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChainRelation {
    /// `next_root`'s parent chain reaches a slot below `slot` without visiting it.
    JumpsOver { next_root: u64 },
    /// `next_root`'s parent chain visits `slot`.
    DescendsFrom { next_root: u64 },
}

/// The previous produced block. The chain check needs its slot and blockhash.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PreviousBlock {
    pub slot: u64,
    pub blockhash: [u8; 32],
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
    let mut forks = Vec::new();
    let mut previous = None;
    for slot in view.start_slot..=view.end_slot {
        if let Some(data) = view.slots.get(&slot) {
            if data.is_root {
                accept_block_against(data, view.start_slot, view.end_slot, previous.as_ref())?;
                previous = Some(PreviousBlock {
                    slot: data.slot,
                    blockhash: data.blockhash,
                });
                block_slots.push(slot);
            } else {
                let relation = chain_relation_in_view(view, slot)?;
                let markers = match relation {
                    ChainRelation::JumpsOver { .. } => {
                        let carrier = slot.checked_add(REWARD_SLOT_DISTANCE);
                        carrier.and_then(|carrier| {
                            view.slots.get(&carrier).map(|data| data.markers.as_slice())
                        })
                    }
                    ChainRelation::DescendsFrom { .. } => None,
                };
                classify_abandoned_fork(slot, relation, markers, &mut forks)?;
            }
        } else {
            let carrier = slot.checked_add(REWARD_SLOT_DISTANCE);
            let markers = carrier
                .and_then(|carrier| view.slots.get(&carrier).map(|data| data.markers.as_slice()));
            let chain = if hole_uses_parent_chain(slot, markers)? {
                full_root_relation_in_view(view, slot)?
            } else {
                None
            };
            classify_hole(slot, markers, chain, &mut skips)?;
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
        forks,
        boundary_hash,
    })
}

/// Follow parent links from `next_root` until the walk passes `slot`.
///
/// The walk stops when a parent is below `slot`. A parent equal to `slot`, or
/// a cursor equal to `slot`, means the rooted chain includes that slot.
pub(crate) fn parent_chain_relation<F>(
    slot: u64,
    next_root: u64,
    mut parent_of: F,
) -> Result<ChainRelation>
where
    F: FnMut(u64) -> Result<Option<u64>>,
{
    if next_root <= slot {
        bail!("next root {next_root} is not after slot {slot}");
    }
    let mut cursor = next_root;
    loop {
        if cursor == slot {
            return Ok(ChainRelation::DescendsFrom { next_root });
        }
        let Some(parent) = parent_of(cursor)? else {
            bail!("slot {cursor} has no parent slot");
        };
        if parent == slot {
            return Ok(ChainRelation::DescendsFrom { next_root });
        }
        if parent < slot {
            return Ok(ChainRelation::JumpsOver { next_root });
        }
        if parent >= cursor {
            bail!("slot {cursor} parent {parent} is not before the slot");
        }
        cursor = parent;
    }
}

fn chain_relation_in_view(view: &LedgerView, slot: u64) -> Result<ChainRelation> {
    let start = slot.checked_add(1).context("slot + 1 overflows")?;
    let Some(next_root) = view
        .slots
        .range(start..)
        .find(|(_, data)| data.is_root)
        .map(|(next_root, _)| *next_root)
    else {
        bail!("slot {slot} has data and is not rooted");
    };
    parent_chain_relation(slot, next_root, |cursor| {
        Ok(view.slots.get(&cursor).and_then(|data| data.parent_slot))
    })
}

/// Leave out a non-root slot when the rooted chain jumps over it.
///
/// A skip certificate in slot N+8 is enough. A notarization certificate does
/// not cancel that skip. When slot N+8 has no `SlotMeta` or is not root, the
/// certificate cannot be read, and the parent jump is the proof. A slot the
/// next root descends from stays an error.
pub(crate) fn classify_abandoned_fork(
    slot: u64,
    chain: ChainRelation,
    carrier_markers: Option<&[Vec<u8>]>,
    forks: &mut Vec<u64>,
) -> Result<()> {
    if let ChainRelation::DescendsFrom { next_root } = chain {
        bail!("slot {slot} has data and is not rooted, and slot {next_root} descends from it");
    }
    let carrier = slot
        .checked_add(REWARD_SLOT_DISTANCE)
        .context("slot + 8 overflows")?;
    let Some(markers) = carrier_markers else {
        forks.push(slot);
        return Ok(());
    };
    let names = reward_names(markers)
        .with_context(|| format!("slot {carrier} footer cannot prove slot {slot}"))?;
    if names.skips.contains(&slot) {
        forks.push(slot);
        return Ok(());
    }
    bail!("slot {slot} has data and is not rooted, and slot {carrier} does not prove a skip");
}

pub(crate) fn accept_block_against(
    data: &crate::source::SlotData,
    start_slot: u64,
    end_slot: u64,
    previous: Option<&PreviousBlock>,
) -> Result<()> {
    let slot = data.slot;
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

    if let Some(previous) = previous {
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
    } else if (start_slot..=end_slot).contains(&parent_slot) {
        bail!("slot {slot} parent {parent_slot} is inside the range and is not a block");
    }

    for (index, tx) in data.transactions.iter().enumerate() {
        if tx.status.is_none() {
            bail!("slot {slot} transaction {index} is missing status");
        }
    }
    Ok(())
}

/// A hole walks parents when slot N+8 has no footer, and when that footer
/// carries both a skip reward and a notarization reward for `slot`.
pub(crate) fn hole_uses_parent_chain(slot: u64, markers: Option<&[Vec<u8>]>) -> Result<bool> {
    let Some(markers) = markers else {
        return Ok(true);
    };
    let carrier = slot
        .checked_add(REWARD_SLOT_DISTANCE)
        .context("slot + 8 overflows")?;
    let names = reward_names(markers)
        .with_context(|| format!("slot {carrier} footer cannot prove slot {slot}"))?;
    Ok(names.skips.contains(&slot) && names.notars.contains(&slot))
}

/// Prove a slot that has no `SlotMeta`.
///
/// The footer of slot N+8 is the first proof. A skip certificate records a
/// skip. A notarization certificate is an error. A footer that names neither
/// is an error. Those three results ignore `chain`.
///
/// A footer that carries both rewards uses `chain`. A jump records a skip. A
/// chain that includes this slot is an error. No later root is an error.
///
/// When slot N+8 has no `SlotMeta` or is not root, `chain` is the parent walk
/// from the next full rooted slot. A jump over this slot records a skip. A
/// chain that includes this slot is an error. `chain == None` keeps the
/// missing-footer error.
pub(crate) fn classify_hole(
    slot: u64,
    carrier_markers: Option<&[Vec<u8>]>,
    chain: Option<ChainRelation>,
    skips: &mut Vec<u64>,
) -> Result<()> {
    let carrier = slot
        .checked_add(REWARD_SLOT_DISTANCE)
        .context("slot + 8 overflows")?;
    let Some(markers) = carrier_markers else {
        return classify_hole_by_parent(slot, carrier, chain, skips);
    };
    let names = reward_names(markers)
        .with_context(|| format!("slot {carrier} footer cannot prove slot {slot}"))?;
    if names.skips.contains(&slot) && names.notars.contains(&slot) {
        return match chain {
            Some(ChainRelation::JumpsOver { .. }) => {
                skips.push(slot);
                Ok(())
            }
            Some(ChainRelation::DescendsFrom { next_root }) => {
                bail!("slot {slot} has no SlotMeta and slot {next_root} descends from it");
            }
            None => bail!("slot {slot} has no SlotMeta and no rooted slot follows it"),
        };
    }
    if names.notars.contains(&slot) {
        bail!("slot {slot} is missing and slot {carrier} has a notarization reward for it");
    }
    if names.skips.contains(&slot) {
        skips.push(slot);
        return Ok(());
    }
    bail!("slot {slot} has no SlotMeta and slot {carrier} does not prove a skip");
}

fn classify_hole_by_parent(
    slot: u64,
    carrier: u64,
    chain: Option<ChainRelation>,
    skips: &mut Vec<u64>,
) -> Result<()> {
    match chain {
        Some(ChainRelation::JumpsOver { .. }) => {
            skips.push(slot);
            Ok(())
        }
        Some(ChainRelation::DescendsFrom { next_root }) => {
            bail!("slot {slot} has no SlotMeta and slot {next_root} descends from it");
        }
        None => bail!("slot {slot} has no SlotMeta and slot {carrier} has no footer proof"),
    }
}

/// Parent walk from the first rooted slot after `slot`.
///
/// A rooted slot that is not full is an error. No later root returns `None`.
fn full_root_relation_in_view(view: &LedgerView, slot: u64) -> Result<Option<ChainRelation>> {
    let start = slot.checked_add(1).context("slot + 1 overflows")?;
    let Some((next_root, data)) = view.slots.range(start..).find(|(_, data)| data.is_root) else {
        return Ok(None);
    };
    if !data.is_full {
        bail!("slot {next_root} is rooted and is not full");
    }
    parent_chain_relation(slot, *next_root, |cursor| {
        Ok(view.slots.get(&cursor).and_then(|data| data.parent_slot))
    })
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{SlotData, test_block};

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

    fn skip_and_notar_footer(slot: u64) -> Vec<u8> {
        let mut certificates = vec![0, 1];
        certificates.extend_from_slice(&slot.to_le_bytes());
        certificates.extend(std::iter::repeat_n(7u8, 96));
        certificates.push(0);
        certificates.push(1);
        certificates.extend_from_slice(&slot.to_le_bytes());
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
        assert!(error.to_string().contains("missing status"), "{error:#}");
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
        assert_eq!(inspected.forks, Vec::<u64>::new());
        assert_eq!(inspected.boundary_hash, [3; 32]);
    }

    #[test]
    fn an_abandoned_fork_with_skip_and_notar_is_omitted() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut fork = test_block(11, 10, [8; 32], [4; 32]);
        fork.is_root = false;
        let next = test_block(13, 10, [5; 32], [4; 32]);
        let mut fork_carrier = test_block(19, 18, [6; 32], [5; 32]);
        fork_carrier.is_root = false;
        fork_carrier.markers = vec![skip_footer(11), notar_footer(11)];
        let mut hole_carrier = test_block(20, 19, [7; 32], [6; 32]);
        hole_carrier.is_root = false;
        hole_carrier.markers = vec![skip_footer(12)];

        let inspected = inspect(&view(
            10,
            13,
            vec![first, fork, next, fork_carrier, hole_carrier],
        ))
        .unwrap();
        assert_eq!(inspected.block_slots, vec![10, 13]);
        assert_eq!(inspected.forks, vec![11]);
        assert_eq!(inspected.skips, vec![12]);
        assert_eq!(inspected.boundary_hash, [3; 32]);
    }

    #[test]
    fn a_parent_walk_jumps_over_a_slot_after_one_intermediate() {
        let parents = [(14u64, 13u64), (13, 10)];
        let relation = parent_chain_relation(11, 14, |cursor| {
            Ok(parents
                .iter()
                .find(|(slot, _)| *slot == cursor)
                .map(|(_, parent)| *parent))
        })
        .unwrap();
        assert_eq!(relation, ChainRelation::JumpsOver { next_root: 14 });
    }

    #[test]
    fn a_parent_walk_finds_the_slot_one_step_down() {
        let relation = parent_chain_relation(11, 13, |cursor| {
            Ok(match cursor {
                13 => Some(12),
                12 => Some(11),
                _ => None,
            })
        })
        .unwrap();
        assert_eq!(relation, ChainRelation::DescendsFrom { next_root: 13 });
    }

    #[test]
    fn a_non_root_ancestor_of_the_next_root_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut fork = test_block(11, 10, [8; 32], [4; 32]);
        fork.is_root = false;
        let next = test_block(12, 11, [5; 32], [8; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_footer(11)];

        let error = inspect(&view(10, 12, vec![first, fork, next, carrier])).unwrap_err();
        assert!(error.to_string().contains("descends from it"), "{error:#}");
    }

    #[test]
    fn a_parent_jump_omits_a_fork_when_the_footer_slot_is_absent() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut fork = test_block(11, 10, [8; 32], [4; 32]);
        fork.is_root = false;
        let next = test_block(14, 10, [5; 32], [4; 32]);

        let inspected = inspect(&view(10, 14, vec![first, fork, next])).unwrap();
        assert_eq!(inspected.block_slots, vec![10, 14]);
        assert_eq!(inspected.forks, vec![11]);
        assert_eq!(inspected.skips, vec![12, 13]);
        assert_eq!(inspected.boundary_hash, [3; 32]);
    }

    #[test]
    fn a_chain_jump_without_a_skip_certificate_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut fork = test_block(11, 10, [8; 32], [4; 32]);
        fork.is_root = false;
        let next = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![notar_footer(11)];

        let error = inspect(&view(10, 12, vec![first, fork, next, carrier])).unwrap_err();
        assert!(
            error.to_string().contains("does not prove a skip"),
            "{error:#}"
        );
    }

    #[test]
    fn a_skip_and_notar_reward_with_a_parent_jump_is_a_skip() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let second = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_and_notar_footer(11)];

        let inspected = inspect(&view(10, 12, vec![first, second, carrier])).unwrap();
        assert_eq!(inspected.block_slots, vec![10, 12]);
        assert_eq!(inspected.skips, vec![11]);
    }

    #[test]
    fn a_skip_and_notar_reward_whose_next_block_descends_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let next = test_block(12, 11, [5; 32], [4; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_and_notar_footer(11)];

        let error = inspect(&view(10, 12, vec![first, next, carrier])).unwrap_err();
        assert!(error.to_string().contains("descends from it"), "{error:#}");
    }

    #[test]
    fn a_skip_and_notar_reward_without_a_later_root_is_an_error() {
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_and_notar_footer(11)];

        let error = inspect(&view(11, 19, vec![carrier])).unwrap_err();
        assert!(
            error.to_string().contains("no rooted slot follows"),
            "{error:#}"
        );
    }

    #[test]
    fn a_skip_and_notar_reward_followed_by_a_partial_root_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut next = test_block(12, 10, [5; 32], [4; 32]);
        next.is_full = false;
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_and_notar_footer(11)];

        let error = inspect(&view(10, 12, vec![first, next, carrier])).unwrap_err();
        assert!(
            error.to_string().contains("rooted and is not full"),
            "{error:#}"
        );
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
    fn a_parent_jump_proves_a_run_of_holes() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let next = test_block(14, 10, [5; 32], [4; 32]);
        let inspected = inspect(&view(10, 14, vec![first, next])).unwrap();
        assert_eq!(inspected.block_slots, vec![10, 14]);
        assert_eq!(inspected.skips, vec![11, 12, 13]);
        assert_eq!(inspected.forks, Vec::<u64>::new());
    }

    #[test]
    fn a_hole_whose_next_block_descends_from_it_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let next = test_block(12, 11, [5; 32], [4; 32]);
        let error = inspect(&view(10, 12, vec![first, next])).unwrap_err();
        assert!(error.to_string().contains("descends from it"), "{error:#}");
    }

    #[test]
    fn a_hole_followed_by_a_partial_root_is_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let mut next = test_block(12, 10, [5; 32], [4; 32]);
        next.is_full = false;
        let error = inspect(&view(10, 12, vec![first, next])).unwrap_err();
        assert!(
            error.to_string().contains("rooted and is not full"),
            "{error:#}"
        );
    }

    #[test]
    fn a_footer_that_does_not_name_the_hole_stays_an_error() {
        let first = test_block(10, 9, [4; 32], [3; 32]);
        let next = test_block(12, 10, [5; 32], [4; 32]);
        let mut carrier = test_block(19, 18, [6; 32], [5; 32]);
        carrier.is_root = false;
        carrier.markers = vec![skip_footer(99)];
        let error = inspect(&view(10, 12, vec![first, next, carrier])).unwrap_err();
        assert!(
            error.to_string().contains("does not prove a skip"),
            "{error:#}"
        );
    }

    #[test]
    fn an_unproven_hole_is_an_error() {
        let block = test_block(10, 9, [4; 32], [3; 32]);
        let error = inspect(&view(10, 11, vec![block])).unwrap_err();
        assert!(
            error.to_string().contains("does not prove a skip")
                || error.to_string().contains("no footer proof"),
            "{error:#}"
        );
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

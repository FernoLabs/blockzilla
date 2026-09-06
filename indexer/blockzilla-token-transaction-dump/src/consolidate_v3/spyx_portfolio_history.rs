//! Forward-only, bounded history for the SPYx authority-portfolio heuristic.
//!
//! The replay owns discovery and public-balance state. This module owns the
//! deterministic proportional allocator and sparse history collection. A
//! caller must submit only state that was known at the sample transaction.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};

pub(super) const HISTORY_SLOT_WINDOW_WIDTH: u64 = 216_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct CandidatePrincipal {
    pub(super) observed_deposited_principal: u128,
    pub(super) observed_returned_principal: u128,
    pub(super) deposit_transaction_count: u64,
    pub(super) return_transaction_count: u64,
}

impl CandidatePrincipal {
    pub(super) fn net_principal(self) -> u128 {
        self.observed_deposited_principal
            .saturating_sub(self.observed_returned_principal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClaimAllocation {
    pub(super) authority_registry_id: u32,
    pub(super) evidence: CandidatePrincipal,
    pub(super) attributed_claim: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CustodyAllocation {
    pub(super) candidate_net_principal: u128,
    pub(super) attributed_claim: u128,
    pub(super) allocations: Vec<ClaimAllocation>,
}

/// Allocate one custody balance with the same largest-remainder rule used by
/// the final authority report. Registry ID is the deterministic tie-breaker.
pub(super) fn allocate_candidate_claims(
    direct_custody_balance: u128,
    mut candidates: Vec<(u32, CandidatePrincipal)>,
) -> Result<CustodyAllocation> {
    candidates.retain(|(_, evidence)| evidence.net_principal() != 0);
    candidates.sort_unstable_by_key(|(authority_registry_id, _)| *authority_registry_id);
    ensure!(
        candidates.windows(2).all(|pair| pair[0].0 != pair[1].0),
        "candidate authority is duplicated for one custody owner"
    );

    let candidate_net_principal =
        candidates
            .iter()
            .try_fold(0u128, |sum, (_, evidence)| -> Result<u128> {
                sum.checked_add(evidence.net_principal())
                    .context("custody candidate principal overflow")
            })?;
    let attributed_claim = direct_custody_balance.min(candidate_net_principal);
    let mut provisional =
        Vec::<(u32, CandidatePrincipal, u128, u128)>::with_capacity(candidates.len());
    let mut provisional_total = 0u128;
    if candidate_net_principal != 0 {
        for (authority_registry_id, evidence) in candidates {
            let product = evidence
                .net_principal()
                .checked_mul(attributed_claim)
                .context("candidate proportional claim product overflow")?;
            let allocation = product
                .checked_div(candidate_net_principal)
                .context("candidate proportional claim divisor is zero")?;
            let remainder = product
                .checked_rem(candidate_net_principal)
                .context("candidate proportional claim divisor is zero")?;
            provisional_total = provisional_total
                .checked_add(allocation)
                .context("candidate proportional claim total overflow")?;
            provisional.push((authority_registry_id, evidence, allocation, remainder));
        }
    }

    let rounding_remainder = attributed_claim
        .checked_sub(provisional_total)
        .context("candidate proportional claims exceed custody cap")?;
    ensure!(
        rounding_remainder <= u128::try_from(provisional.len())?,
        "candidate proportional allocation has an invalid rounding remainder"
    );
    let mut remainder_order = (0..provisional.len()).collect::<Vec<_>>();
    remainder_order.sort_unstable_by(|left, right| {
        provisional[*right]
            .3
            .cmp(&provisional[*left].3)
            .then_with(|| provisional[*left].0.cmp(&provisional[*right].0))
    });
    for index in remainder_order
        .into_iter()
        .take(usize::try_from(rounding_remainder)?)
    {
        provisional[index].2 = provisional[index]
            .2
            .checked_add(1)
            .context("candidate proportional claim rounding overflow")?;
    }

    let allocations = provisional
        .into_iter()
        .map(
            |(authority_registry_id, evidence, attributed_claim, _)| ClaimAllocation {
                authority_registry_id,
                evidence,
                attributed_claim,
            },
        )
        .collect::<Vec<_>>();
    let allocated_total = allocations.iter().try_fold(0u128, |sum, allocation| {
        sum.checked_add(allocation.attributed_claim)
            .context("allocated candidate claim total overflow")
    })?;
    ensure!(
        allocated_total == attributed_claim,
        "candidate claims do not equal their custody cap"
    );

    Ok(CustodyAllocation {
        candidate_net_principal,
        attributed_claim,
        allocations,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PortfolioClaimComponentState {
    pub(super) custody_owner_registry_id: u32,
    pub(super) program_id: Option<[u8; 32]>,
    pub(super) evidence: CandidatePrincipal,
    pub(super) attributed_claim: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PortfolioState {
    pub(super) direct_public_balance: u128,
    pub(super) claim_components: Vec<PortfolioClaimComponentState>,
}

impl PortfolioState {
    pub(super) fn normalize_and_validate(&mut self) -> Result<()> {
        self.claim_components
            .sort_unstable_by_key(|component| component.custody_owner_registry_id);
        ensure!(
            self.claim_components
                .windows(2)
                .all(|pair| pair[0].custody_owner_registry_id != pair[1].custody_owner_registry_id),
            "portfolio has duplicate custody components"
        );
        ensure!(
            self.claim_components.iter().all(|component| {
                component.attributed_claim <= component.evidence.net_principal()
            }),
            "portfolio component claim exceeds candidate principal"
        );
        Ok(())
    }

    pub(super) fn estimated_defi_claim(&self) -> Result<u128> {
        self.claim_components
            .iter()
            .try_fold(0u128, |sum, component| {
                sum.checked_add(component.attributed_claim)
                    .context("portfolio estimated claim overflow")
            })
    }

    pub(super) fn estimated_total_exposure(&self) -> Result<u128> {
        self.direct_public_balance
            .checked_add(self.estimated_defi_claim()?)
            .context("portfolio estimated total exposure overflow")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct HistoryLocation {
    pub(super) transaction_id: u64,
    pub(super) slot: u64,
    pub(super) block_time: Option<i64>,
}

impl HistoryLocation {
    pub(super) const fn slot_window(self) -> u64 {
        self.slot / HISTORY_SLOT_WINDOW_WIDTH
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PortfolioAggregateState {
    direct_public_balance: u128,
    estimated_defi_claim: u128,
}

impl PortfolioAggregateState {
    fn from_portfolio(state: &PortfolioState) -> Result<Self> {
        Ok(Self {
            direct_public_balance: state.direct_public_balance,
            estimated_defi_claim: state.estimated_defi_claim()?,
        })
    }

    fn estimated_total_exposure(self) -> Result<u128> {
        self.direct_public_balance
            .checked_add(self.estimated_defi_claim)
            .context("portfolio history estimated total exposure overflow")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryPointValue {
    location: HistoryLocation,
    state: PortfolioAggregateState,
}

#[derive(Debug, Default)]
pub(super) struct AuthorityPortfolioHistoryCollector {
    previous: BTreeMap<u32, PortfolioAggregateState>,
    points_by_authority: BTreeMap<u32, Vec<HistoryPointValue>>,
    last_capture: Option<HistoryLocation>,
    sample_count: u64,
}

impl AuthorityPortfolioHistoryCollector {
    /// Capture one forward state. Ordinary captures write only changed rows.
    /// A final capture writes every current row so the artifact can be checked
    /// directly against the final authority report.
    pub(super) fn capture(
        &mut self,
        location: HistoryLocation,
        current: BTreeMap<u32, PortfolioState>,
        force_all_current: bool,
    ) -> Result<()> {
        ensure!(
            self.last_capture.is_none_or(|previous| previous < location),
            "authority portfolio history locations are not in canonical order"
        );
        let mut aggregate_current = BTreeMap::new();
        for (authority_registry_id, mut state) in current {
            state.normalize_and_validate()?;
            aggregate_current.insert(
                authority_registry_id,
                PortfolioAggregateState::from_portfolio(&state)?,
            );
        }

        let authority_ids = self
            .previous
            .keys()
            .chain(aggregate_current.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for authority_registry_id in authority_ids {
            let current_state = aggregate_current
                .get(&authority_registry_id)
                .copied()
                .unwrap_or_default();
            let changed = self.previous.get(&authority_registry_id) != Some(&current_state);
            if changed
                || (force_all_current && aggregate_current.contains_key(&authority_registry_id))
            {
                self.points_by_authority
                    .entry(authority_registry_id)
                    .or_default()
                    .push(HistoryPointValue {
                        location,
                        state: current_state,
                    });
            }
        }
        self.previous = aggregate_current;
        self.last_capture = Some(location);
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .context("authority portfolio history sample count overflow")?;
        Ok(())
    }

    pub(super) fn validate_final(
        &self,
        final_state: &BTreeMap<u32, PortfolioState>,
        final_location: HistoryLocation,
    ) -> Result<()> {
        ensure!(
            self.last_capture == Some(final_location),
            "authority portfolio history has no exact final capture"
        );
        ensure!(
            self.previous.len() == final_state.len(),
            "authority portfolio history final authority count differs from the final portfolio"
        );
        for (&authority_registry_id, state) in final_state {
            let aggregate = PortfolioAggregateState::from_portfolio(state)?;
            ensure!(
                self.previous.get(&authority_registry_id) == Some(&aggregate),
                "authority portfolio history final totals differ from the final portfolio"
            );
            let last = self
                .points_by_authority
                .get(&authority_registry_id)
                .and_then(|points| points.last())
                .context("final portfolio authority has no history point")?;
            ensure!(
                last.location == final_location && last.state == aggregate,
                "authority portfolio history final point differs from the final portfolio"
            );
        }
        Ok(())
    }

    pub(super) fn into_report(
        self,
        registry: &[u8],
        source_binding: AuthorityPortfolioHistorySourceBinding,
        transactions_scanned: u64,
    ) -> Result<AuthorityPortfolioHistoryReport> {
        let point_count =
            self.points_by_authority
                .values()
                .try_fold(0u64, |sum, points| -> Result<u64> {
                    sum.checked_add(u64::try_from(points.len())?)
                        .context("authority portfolio history point count overflow")
                })?;
        let series = self
            .points_by_authority
            .into_iter()
            .map(|(authority_registry_id, points)| {
                let authority = registry_key(registry, authority_registry_id)?;
                let points = points
                    .into_iter()
                    .map(serialize_point)
                    .collect::<Result<Vec<_>>>()?;
                Ok(AuthorityPortfolioHistorySeriesReport {
                    authority: bs58::encode(authority).into_string(),
                    points,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(AuthorityPortfolioHistoryReport {
            schema_version: 2,
            artifact_kind: "spyx_authority_portfolio_history",
            source_binding,
            coverage: AuthorityPortfolioHistoryCoverage {
                complete: true,
                method: "forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2",
                slot_window_width: HISTORY_SLOT_WINDOW_WIDTH,
                transactions_scanned,
                state_samples: self.sample_count,
                authority_series: u64::try_from(series.len())?,
                history_points: point_count,
                final_sample_matches_current_portfolio: true,
                definitions: AuthorityPortfolioHistoryDefinitions {
                    sampling: "one aggregate tuple at the last clean replay transaction in each non-empty 216,000-slot window, with rows omitted when direct balance and estimated claim are unchanged; every final authority has an additional exact final point",
                    estimated_defi_claim: "the candidate principal and physical custody cap available at that sample only; later deposits, returns, custody balances, and labels are never projected backward",
                    direct_public_balance: "the exact transaction-final public SPYx balance of the on-curve authority at the sample",
                },
            },
            point_fields: HISTORY_POINT_FIELDS,
            series,
        })
    }
}

const HISTORY_POINT_FIELDS: [&str; 5] = [
    "transaction_id",
    "slot",
    "block_time",
    "direct_public_balance_raw",
    "estimated_defi_claim_raw",
];

fn serialize_point(point: HistoryPointValue) -> Result<AuthorityPortfolioHistoryPointReport> {
    // Validate the derived total even though it is not duplicated in every tuple.
    let _estimated_total_exposure = point.state.estimated_total_exposure()?;
    Ok(AuthorityPortfolioHistoryPointReport(
        point.location.transaction_id,
        point.location.slot,
        point.location.block_time,
        point.state.direct_public_balance.to_string(),
        point.state.estimated_defi_claim.to_string(),
    ))
}

fn registry_key(registry: &[u8], registry_id: u32) -> Result<[u8; 32]> {
    ensure!(registry_id != 0, "history registry ID is zero");
    let start = usize::try_from(registry_id - 1)?
        .checked_mul(32)
        .context("history registry offset overflow")?;
    let bytes = registry
        .get(start..start + 32)
        .context("history registry ID exceeds the registry")?;
    Ok(bytes.try_into().expect("validated 32-byte registry key"))
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct AuthorityPortfolioHistorySourceBinding {
    pub(super) mint: String,
    pub(super) first_epoch: u64,
    pub(super) last_epoch: u64,
    pub(super) manifest_sha256: String,
    pub(super) transactions_sha256: String,
    pub(super) registry_sha256: String,
    pub(super) replay_state_sha256: String,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioHistoryDefinitions {
    sampling: &'static str,
    estimated_defi_claim: &'static str,
    direct_public_balance: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioHistoryCoverage {
    complete: bool,
    method: &'static str,
    slot_window_width: u64,
    transactions_scanned: u64,
    state_samples: u64,
    authority_series: u64,
    history_points: u64,
    final_sample_matches_current_portfolio: bool,
    definitions: AuthorityPortfolioHistoryDefinitions,
}

#[derive(Debug, serde::Serialize)]
pub(super) struct AuthorityPortfolioHistoryReport {
    schema_version: u16,
    artifact_kind: &'static str,
    source_binding: AuthorityPortfolioHistorySourceBinding,
    coverage: AuthorityPortfolioHistoryCoverage,
    point_fields: [&'static str; 5],
    series: Vec<AuthorityPortfolioHistorySeriesReport>,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioHistorySeriesReport {
    authority: String,
    points: Vec<AuthorityPortfolioHistoryPointReport>,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioHistoryPointReport(u64, u64, Option<i64>, String, String);

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(net: u128) -> CandidatePrincipal {
        CandidatePrincipal {
            observed_deposited_principal: net,
            observed_returned_principal: 0,
            deposit_transaction_count: 1,
            return_transaction_count: 0,
        }
    }

    fn location(transaction_id: u64, slot: u64) -> HistoryLocation {
        HistoryLocation {
            transaction_id,
            slot,
            block_time: Some(i64::try_from(slot).unwrap()),
        }
    }

    fn source_binding() -> AuthorityPortfolioHistorySourceBinding {
        AuthorityPortfolioHistorySourceBinding {
            mint: bs58::encode([250; 32]).into_string(),
            first_epoch: 900,
            last_epoch: 901,
            manifest_sha256: "11".repeat(32),
            transactions_sha256: "22".repeat(32),
            registry_sha256: "33".repeat(32),
            replay_state_sha256: "44".repeat(32),
        }
    }

    #[test]
    fn largest_remainder_caps_custody_and_uses_registry_id_tie_break() {
        let allocation = allocate_candidate_claims(
            2,
            vec![(30, principal(1)), (10, principal(1)), (20, principal(1))],
        )
        .unwrap();
        assert_eq!(allocation.candidate_net_principal, 3);
        assert_eq!(allocation.attributed_claim, 2);
        assert_eq!(
            allocation
                .allocations
                .iter()
                .map(|value| (value.authority_registry_id, value.attributed_claim))
                .collect::<Vec<_>>(),
            [(10, 1), (20, 1), (30, 0)]
        );
    }

    #[test]
    fn uncapped_allocation_returns_each_candidate_principal() {
        let allocation =
            allocate_candidate_claims(100, vec![(2, principal(30)), (1, principal(20))]).unwrap();
        assert_eq!(allocation.candidate_net_principal, 50);
        assert_eq!(allocation.attributed_claim, 50);
        assert_eq!(
            allocation
                .allocations
                .iter()
                .map(|value| (value.authority_registry_id, value.attributed_claim))
                .collect::<Vec<_>>(),
            [(1, 20), (2, 30)]
        );
    }

    #[test]
    fn sparse_history_does_not_project_new_authority_backward() {
        let mut collector = AuthorityPortfolioHistoryCollector::default();
        let first = BTreeMap::from([(
            1,
            PortfolioState {
                direct_public_balance: 5,
                claim_components: Vec::new(),
            },
        )]);
        collector
            .capture(location(1, 216_001), first, false)
            .unwrap();

        let final_state = BTreeMap::from([
            (
                1,
                PortfolioState {
                    direct_public_balance: 5,
                    claim_components: Vec::new(),
                },
            ),
            (
                2,
                PortfolioState {
                    direct_public_balance: 0,
                    claim_components: vec![PortfolioClaimComponentState {
                        custody_owner_registry_id: 3,
                        program_id: None,
                        evidence: principal(9),
                        attributed_claim: 7,
                    }],
                },
            ),
        ]);
        let final_location = location(2, 432_001);
        collector
            .capture(final_location, final_state.clone(), true)
            .unwrap();
        collector
            .validate_final(&final_state, final_location)
            .unwrap();

        assert_eq!(collector.points_by_authority[&1].len(), 2);
        assert_eq!(collector.points_by_authority[&2].len(), 1);
        assert_eq!(
            collector.points_by_authority[&2][0].location,
            final_location
        );
    }

    #[test]
    fn sparse_history_writes_a_zero_tombstone() {
        let mut collector = AuthorityPortfolioHistoryCollector::default();
        collector
            .capture(
                location(1, 1),
                BTreeMap::from([(
                    1,
                    PortfolioState {
                        direct_public_balance: 5,
                        claim_components: Vec::new(),
                    },
                )]),
                false,
            )
            .unwrap();
        collector
            .capture(location(2, 216_001), BTreeMap::new(), false)
            .unwrap();
        let points = &collector.points_by_authority[&1];
        assert_eq!(points.len(), 2);
        assert_eq!(points[1].state, PortfolioAggregateState::default());
    }

    #[test]
    fn sparse_history_ignores_component_only_changes() {
        let mut collector = AuthorityPortfolioHistoryCollector::default();
        collector
            .capture(
                location(1, 1),
                BTreeMap::from([(
                    1,
                    PortfolioState {
                        direct_public_balance: 5,
                        claim_components: vec![PortfolioClaimComponentState {
                            custody_owner_registry_id: 2,
                            program_id: Some([2; 32]),
                            evidence: principal(9),
                            attributed_claim: 7,
                        }],
                    },
                )]),
                false,
            )
            .unwrap();
        collector
            .capture(
                location(2, 216_001),
                BTreeMap::from([(
                    1,
                    PortfolioState {
                        direct_public_balance: 5,
                        claim_components: vec![PortfolioClaimComponentState {
                            custody_owner_registry_id: 3,
                            program_id: Some([3; 32]),
                            evidence: principal(20),
                            attributed_claim: 7,
                        }],
                    },
                )]),
                false,
            )
            .unwrap();

        assert_eq!(collector.points_by_authority[&1].len(), 1);
    }

    #[test]
    fn schema_two_serializes_fixed_aggregate_tuples() {
        let mut collector = AuthorityPortfolioHistoryCollector::default();
        let final_state = BTreeMap::from([(
            1,
            PortfolioState {
                direct_public_balance: 5,
                claim_components: vec![PortfolioClaimComponentState {
                    custody_owner_registry_id: 2,
                    program_id: None,
                    evidence: principal(9),
                    attributed_claim: 7,
                }],
            },
        )]);
        let final_location = HistoryLocation {
            transaction_id: 11,
            slot: 216_001,
            block_time: None,
        };
        collector
            .capture(final_location, final_state.clone(), true)
            .unwrap();
        collector
            .validate_final(&final_state, final_location)
            .unwrap();
        let report = collector
            .into_report(&[1; 32], source_binding(), 12)
            .unwrap();
        let report = serde_json::to_value(report).unwrap();

        assert_eq!(report["schema_version"], 2);
        assert_eq!(
            report["coverage"]["method"],
            "forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2"
        );
        assert_eq!(
            report["point_fields"],
            serde_json::json!([
                "transaction_id",
                "slot",
                "block_time",
                "direct_public_balance_raw",
                "estimated_defi_claim_raw"
            ])
        );
        assert_eq!(
            report["series"][0]["points"][0],
            serde_json::json!([11, 216_001, null, "5", "7"])
        );
        let serialized = serde_json::to_string(&report).unwrap();
        for removed_field in [
            "source_epoch",
            "source_block_id",
            "tx_index",
            "slot_window",
            "claim_components",
            "estimated_total_exposure_raw",
        ] {
            assert!(!serialized.contains(&format!("\"{removed_field}\":")));
        }
    }
}

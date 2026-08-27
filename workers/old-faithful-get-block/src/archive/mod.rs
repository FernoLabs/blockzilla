mod block;
mod http;
mod status;

use crate::error::{FetchError, FetchResult};
use worker::Env;

pub(crate) use block::{FetchedBlock, get_block};
pub(crate) use status::get as get_status;

const VERIFIED_V2_SLOT_INDEX_PREFIX: &str = "slot-index-v2-verified";
const FALLBACK_V2_SLOT_INDEX_PREFIXES: [&str; 2] = ["slot-index-v2", "slot-index"];
const SLOT_INDEX_FALLBACK_ENV: &str = "OF_SLOT_INDEX_FALLBACK";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SlotIndexFallbackPolicy {
    VerifiedOnly,
    ValidatedV2,
    ValidatedLegacy,
}

impl SlotIndexFallbackPolicy {
    pub(super) fn allows_legacy(self) -> bool {
        self == Self::ValidatedLegacy
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::VerifiedOnly => "verified-only",
            Self::ValidatedV2 => "validated-v2",
            Self::ValidatedLegacy => "validated-legacy",
        }
    }
}

pub(super) fn slot_index_fallback_policy(env: &Env) -> FetchResult<SlotIndexFallbackPolicy> {
    let configured = env
        .var(SLOT_INDEX_FALLBACK_ENV)
        .ok()
        .map(|value| value.to_string());
    parse_slot_index_fallback_policy(configured.as_deref()).map_err(|reason| {
        FetchError::Configuration {
            name: SLOT_INDEX_FALLBACK_ENV.to_string(),
            reason,
        }
    })
}

fn parse_slot_index_fallback_policy(
    configured: Option<&str>,
) -> Result<SlotIndexFallbackPolicy, String> {
    match configured {
        None | Some("verified-only") => Ok(SlotIndexFallbackPolicy::VerifiedOnly),
        Some("validated-v2") => Ok(SlotIndexFallbackPolicy::ValidatedV2),
        Some("validated-legacy") => Ok(SlotIndexFallbackPolicy::ValidatedLegacy),
        Some(value) => Err(format!(
            "unsupported value {value:?}; expected verified-only, validated-v2, or validated-legacy"
        )),
    }
}

pub(super) fn verified_v2_slot_index_key(epoch: u64) -> String {
    format!("{VERIFIED_V2_SLOT_INDEX_PREFIX}/epoch-{epoch}-slot-ranges-v2.raw")
}

pub(super) fn v2_slot_index_keys(epoch: u64, policy: SlotIndexFallbackPolicy) -> Vec<String> {
    let mut keys = vec![verified_v2_slot_index_key(epoch)];
    if policy != SlotIndexFallbackPolicy::VerifiedOnly {
        keys.extend(
            FALLBACK_V2_SLOT_INDEX_PREFIXES
                .map(|prefix| format!("{prefix}/epoch-{epoch}-slot-ranges-v2.raw")),
        );
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::{SlotIndexFallbackPolicy, parse_slot_index_fallback_policy, v2_slot_index_keys};

    #[test]
    fn verified_only_never_selects_an_old_prefix() {
        assert_eq!(
            v2_slot_index_keys(7, SlotIndexFallbackPolicy::VerifiedOnly),
            vec!["slot-index-v2-verified/epoch-7-slot-ranges-v2.raw"]
        );
    }

    #[test]
    fn validated_v2_fallback_keeps_verified_first() {
        assert_eq!(
            v2_slot_index_keys(7, SlotIndexFallbackPolicy::ValidatedV2),
            vec![
                "slot-index-v2-verified/epoch-7-slot-ranges-v2.raw",
                "slot-index-v2/epoch-7-slot-ranges-v2.raw",
                "slot-index/epoch-7-slot-ranges-v2.raw",
            ]
        );
    }

    #[test]
    fn fallback_policy_is_strict_and_fail_closed_by_default() {
        assert_eq!(
            parse_slot_index_fallback_policy(None).unwrap(),
            SlotIndexFallbackPolicy::VerifiedOnly
        );
        assert_eq!(
            parse_slot_index_fallback_policy(Some("verified-only")).unwrap(),
            SlotIndexFallbackPolicy::VerifiedOnly
        );
        assert_eq!(
            parse_slot_index_fallback_policy(Some("validated-v2")).unwrap(),
            SlotIndexFallbackPolicy::ValidatedV2
        );
        assert_eq!(
            parse_slot_index_fallback_policy(Some("validated-legacy")).unwrap(),
            SlotIndexFallbackPolicy::ValidatedLegacy
        );
        assert!(parse_slot_index_fallback_policy(Some("")).is_err());
        assert!(parse_slot_index_fallback_policy(Some("true")).is_err());
    }

    #[test]
    fn only_the_explicit_legacy_policy_allows_legacy_indexes() {
        assert!(!SlotIndexFallbackPolicy::VerifiedOnly.allows_legacy());
        assert!(!SlotIndexFallbackPolicy::ValidatedV2.allows_legacy());
        assert!(SlotIndexFallbackPolicy::ValidatedLegacy.allows_legacy());
    }
}

//! Epoch date calendar: merges a bundled reference calendar (checked in,
//! covers genesis through ~mid-2026) with any live-authoritative dates the
//! scheduler emits, extrapolates a tail estimate through the latest
//! tracked epoch, and buckets everything into day-by-day year grids for
//! the `/calendar` page.
//!
//! Faithful port of
//! `apps/blockzilla-watcher/src/lib/{epoch-calendar,epoch-year-calendar}.ts`
//! -- function names and semantics mirror those files; see them for the
//! reference implementation this must stay compatible with. Two
//! deliberate simplifications from the Svelte version:
//! - No separate live-capture merge (`snapshot.live[]`): this dashboard
//!   already only reads `snapshot.epochs` for epoch state elsewhere (see
//!   `state.rs`), so the calendar does the same rather than introducing a
//!   second epoch-status model just for this page.
//! - Eleven fine-grained tones collapse to five color buckets (reusing the
//!   emerald/cyan/amber/rose/zinc palette already established everywhere
//!   else in this app) since the original distinction doesn't survive at
//!   7x8px cell size anyway -- the precise sub-status is still in the
//!   tooltip text, nothing is hidden, just not separately colored.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::snapshot::{BlockTimeGapIndex, CalendarPrecision, EpochCalendarEntry, EpochStatus};

const REFERENCE_CALENDAR_JSON: &str = include_str!("assets/mainnet-epoch-calendar.json");
const SECONDS_PER_DAY: i64 = 86_400;
const SLOTS_PER_EPOCH: u64 = 432_000;
const FALLBACK_SLOT_MS: u64 = 400;

#[derive(serde::Deserialize)]
struct ReferenceEnvelope {
    epoch_calendar: Vec<EpochCalendarEntry>,
}

/// The bundled reference calendar, parsed once. This is the same static
/// dataset `apps/blockzilla-watcher` ships (`src/lib/data/mainnet-epoch-calendar.json`,
/// copied verbatim into `assets/`) -- chain history doesn't change, so
/// there's nothing to keep in sync beyond periodically re-copying a newer
/// export to extend its coverage.
pub fn reference_calendar() -> &'static [EpochCalendarEntry] {
    static REFERENCE: OnceLock<Vec<EpochCalendarEntry>> = OnceLock::new();
    REFERENCE
        .get_or_init(|| {
            serde_json::from_str::<ReferenceEnvelope>(REFERENCE_CALENDAR_JSON)
                .expect("bundled reference calendar must parse")
                .epoch_calendar
        })
        .as_slice()
}

/// Merges `authoritative` over `reference` by epoch (authoritative wins on
/// conflict), sorted by epoch. Mirrors `mergeEpochCalendars`.
pub fn merge_calendars(reference: &[EpochCalendarEntry], authoritative: &[EpochCalendarEntry]) -> Vec<EpochCalendarEntry> {
    let mut merged: BTreeMap<u32, EpochCalendarEntry> = BTreeMap::new();
    for entry in reference {
        merged.insert(entry.epoch, entry.clone());
    }
    for entry in authoritative {
        merged.insert(entry.epoch, entry.clone());
    }
    merged.into_values().collect()
}

/// Extrapolates entries through `through_epoch` using the median of the
/// most recent (up to 16) known epoch durations, falling back to
/// `SLOTS_PER_EPOCH * FALLBACK_SLOT_MS` if there's no usable history yet.
/// Mirrors `extendEpochCalendarTail`.
pub fn extend_tail(calendar: &[EpochCalendarEntry], through_epoch: Option<u32>) -> Vec<EpochCalendarEntry> {
    let mut extended: Vec<EpochCalendarEntry> = calendar.to_vec();
    extended.sort_by_key(|entry| entry.epoch);

    let Some(through_epoch) = through_epoch else {
        return extended;
    };
    let Some(last) = extended.last().cloned() else {
        return extended;
    };
    if through_epoch <= last.epoch {
        return extended;
    }

    let mut durations: Vec<u64> = extended
        .iter()
        .filter_map(|entry| entry.end_unix_secs.map(|end| end.saturating_sub(entry.start_unix_secs)))
        .filter(|&duration| duration > 0)
        .collect();
    let recent = durations.split_off(durations.len().saturating_sub(16));
    let mut recent = recent;
    recent.sort_unstable();
    let fallback_duration = SLOTS_PER_EPOCH * FALLBACK_SLOT_MS / 1_000;
    let epoch_duration = median(&recent).unwrap_or(fallback_duration);
    if epoch_duration == 0 {
        return extended;
    }

    let last_index = extended.len() - 1;
    let mut start = match last.end_unix_secs {
        None => {
            let new_start = last.start_unix_secs + epoch_duration;
            extended[last_index].end_unix_secs = Some(new_start);
            extended[last_index].precision = CalendarPrecision::Estimated;
            new_start
        }
        Some(end) => end,
    };

    for epoch in (last.epoch + 1)..=through_epoch {
        let end = start + epoch_duration;
        extended.push(EpochCalendarEntry {
            epoch,
            start_unix_secs: start,
            end_unix_secs: if epoch == through_epoch { None } else { Some(end) },
            precision: CalendarPrecision::Estimated,
        });
        start = end;
    }
    extended
}

fn median(sorted: &[u64]) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 { sorted[mid] } else { (sorted[mid - 1] + sorted[mid]) / 2 })
}

/// Which color bucket / tooltip text a day's primary epoch gets. Higher
/// `priority()` wins when more than one epoch's date range touches the
/// same UTC day. Mirrors `DAY_TONE_PRIORITY` / `historicalVisualState` in
/// the Svelte app, collapsed from eleven tones to five colors -- see the
/// module doc.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DayTone {
    Untracked,
    Complete,
    FirstSeenComplete,
    LegacyComplete,
    Queued,
    Ready,
    Finalizing,
    Active,
    Missing,
    Attention,
    Failed,
}

impl DayTone {
    fn priority(self) -> u8 {
        match self {
            DayTone::Untracked => 0,
            DayTone::Complete => 1,
            DayTone::FirstSeenComplete => 2,
            DayTone::LegacyComplete => 3,
            DayTone::Queued => 5,
            DayTone::Ready => 7,
            DayTone::Finalizing => 8,
            DayTone::Active => 9,
            DayTone::Missing => 10,
            DayTone::Attention => 11,
            DayTone::Failed => 12,
        }
    }

    /// The shared dot/tone-dot color token this app already uses
    /// everywhere else (`components.rs::state_dot_class`) -- kept as one
    /// enum -> color mapping so a change to the palette only happens once.
    pub fn color(self) -> &'static str {
        match self {
            DayTone::Complete | DayTone::FirstSeenComplete | DayTone::LegacyComplete => "emerald",
            DayTone::Active | DayTone::Ready | DayTone::Finalizing => "cyan",
            DayTone::Queued | DayTone::Untracked => "zinc",
            DayTone::Missing | DayTone::Attention => "amber",
            DayTone::Failed => "rose",
        }
    }
}

fn historical_tone(epoch: &EpochStatus) -> DayTone {
    match epoch.state.as_str() {
        "complete" => {
            if epoch.registry_order.as_deref() == Some("first_seen") {
                DayTone::FirstSeenComplete
            } else if is_legacy_no_access_message(epoch.message.as_deref()) {
                DayTone::LegacyComplete
            } else {
                DayTone::Complete
            }
        }
        "scanning" => DayTone::Active,
        "scan_ready" => DayTone::Ready,
        "finalizing" => DayTone::Finalizing,
        "queued" => DayTone::Queued,
        "failed" => DayTone::Failed,
        "blocked" if has_missing_source_message(epoch.message.as_deref()) => DayTone::Missing,
        _ => DayTone::Attention,
    }
}

fn historical_label(epoch: &EpochStatus) -> String {
    match historical_tone(epoch) {
        DayTone::FirstSeenComplete => "complete · recompactable".to_string(),
        DayTone::LegacyComplete => "legacy complete".to_string(),
        DayTone::Missing => "source missing".to_string(),
        _ if epoch.state == "blocked" => "needs action".to_string(),
        _ => crate::snapshot::humanize(&epoch.state),
    }
}

fn is_legacy_no_access_message(message: Option<&str>) -> bool {
    let Some(message) = message else { return false };
    let normalized = message.to_lowercase();
    normalized.contains("legacy")
        && (normalized.contains("no-access")
            || normalized.contains("no access")
            || normalized.contains("block-access")
            || normalized.contains("block access"))
}

fn has_missing_source_message(message: Option<&str>) -> bool {
    message.map(|message| message.to_lowercase().contains("input car is missing")).unwrap_or(false)
}

/// One day's rendered cell: which epoch(s) touch it, the winning tone/color,
/// and the block-time-interruption overlay for that day, if any. Mirrors
/// `EpochYearDay`. Every day of every included year gets one of these --
/// `content` is `None` for days before the first tracked epoch started or
/// days in the future, which the UI renders as a plain empty placeholder
/// square (matching the Svelte template's `{#if day.primary_epoch !==
/// null && !day.future} ... {:else} <span class="empty-day"> ... {/if}`)
/// so the grid keeps its filled rectangular shape instead of having holes.
#[derive(Clone, Debug)]
pub struct CalendarDay {
    pub date: String,
    pub week: u32,
    pub weekday: u32,
    pub today: bool,
    pub future: bool,
    pub content: Option<CalendarDayContent>,
}

#[derive(Clone, Debug)]
pub struct CalendarDayContent {
    pub epochs: Vec<u32>,
    pub color: &'static str,
    pub label: String,
    pub interruption: Option<InterruptionSummary>,
    pub interruption_coverage: InterruptionCoverage,
    pub estimated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterruptionCoverage {
    Covered,
    Missing,
    Outside,
    Unavailable,
}

#[derive(Clone, Debug)]
pub struct InterruptionSummary {
    pub count: u32,
    pub longest_secs: u64,
    pub boundary_count: u32,
}

pub struct CalendarYear {
    pub year: i32,
    pub week_count: u32,
    pub epoch_count: usize,
    pub archived_epoch_count: usize,
    pub interruption_day_count: usize,
    pub missing_interruption_coverage_day_count: usize,
    pub months: Vec<(String, u32)>,
    pub days: Vec<CalendarDay>,
}

/// Builds one grid per calendar year touched by `epochs`, from the first
/// tracked epoch's start date through `now_unix_secs`. Mirrors
/// `buildEpochYearCalendars`.
pub fn build_years(
    epochs: &[EpochStatus],
    calendar: &[EpochCalendarEntry],
    now_unix_secs: u64,
    interruption_index: Option<&BlockTimeGapIndex>,
) -> Vec<CalendarYear> {
    if now_unix_secs == 0 {
        return Vec::new();
    }
    let now = now_unix_secs as i64;

    let mut timings: Vec<&EpochCalendarEntry> = calendar.iter().collect();
    timings.sort_by_key(|entry| entry.epoch);
    let epoch_by_number: BTreeMap<u32, &EpochStatus> = epochs.iter().map(|epoch| (epoch.epoch, epoch)).collect();

    // date -> epochs touching that day, most-urgent tone first.
    let mut day_epochs: BTreeMap<String, Vec<(u32, DayTone, String, bool)>> = BTreeMap::new();
    let mut first_day: Option<i64> = None;

    for (index, timing) in timings.iter().enumerate() {
        let start = timing.start_unix_secs as i64;
        if start > now {
            continue;
        }
        let (tone, label) = match epoch_by_number.get(&timing.epoch) {
            Some(epoch) => (historical_tone(epoch), historical_label(epoch)),
            None => (DayTone::Untracked, "untracked".to_string()),
        };
        let next_start = timings.get(index + 1).map(|next| next.start_unix_secs as i64);
        let open_end = match next_start {
            None => now,
            Some(next_start) => now.min(start.max(next_start - 1)),
        };
        let end = timing.end_unix_secs.map(|end| end as i64).unwrap_or(open_end).min(now);
        let start_day = utc_day_start(start);
        let end_day = utc_day_start(end);
        first_day = Some(first_day.map_or(start_day, |first| first.min(start_day)));

        let estimated = timing.precision == CalendarPrecision::Estimated;
        let mut day = start_day;
        while day <= end_day {
            day_epochs.entry(iso_date(day)).or_default().push((timing.epoch, tone, label.clone(), estimated));
            day += SECONDS_PER_DAY;
        }
    }

    let Some(first_day) = first_day else {
        return Vec::new();
    };
    let (first_year, ..) = civil_from_days(first_day / SECONDS_PER_DAY);
    let (last_year, ..) = civil_from_days(now / SECONDS_PER_DAY);
    let today = iso_date(utc_day_start(now));

    // Per-epoch (not per-day) counts, keyed by the epoch's *start* year --
    // this is what the year header's "N/M archived" summary counts.
    // Deliberately separate from `day_epochs` above: an epoch whose date
    // range crosses a year boundary would otherwise be double-counted (it
    // touches days in both years) or counted once per day it spans instead
    // of once per epoch. Mirrors `buildEpochYearCalendars`'s `yearStatuses`.
    let mut epoch_counts: BTreeMap<i32, (usize, usize)> = BTreeMap::new(); // year -> (total, archived)
    for timing in &timings {
        let start = timing.start_unix_secs as i64;
        if start > now {
            continue;
        }
        let (start_year, ..) = civil_from_days(start.div_euclid(SECONDS_PER_DAY));
        let archived = epoch_by_number
            .get(&timing.epoch)
            .is_some_and(|epoch| matches!(historical_tone(epoch), DayTone::Complete | DayTone::FirstSeenComplete | DayTone::LegacyComplete));
        let entry = epoch_counts.entry(start_year).or_insert((0, 0));
        entry.0 += 1;
        if archived {
            entry.1 += 1;
        }
    }

    let interruption_by_date: BTreeMap<String, &crate::snapshot::BlockTimeInterruptionDay> = interruption_index
        .map(|index| {
            index
                .days
                .iter()
                .map(|day| (iso_date(utc_day_start(day.day_start_unix_secs as i64)), day))
                .collect()
        })
        .unwrap_or_default();
    let missing_interruption_epochs: std::collections::BTreeSet<u32> =
        interruption_index.map(|index| index.coverage.missing_epochs.iter().copied().collect()).unwrap_or_default();

    let mut years = Vec::new();
    for year in first_year..=last_year {
        let (epoch_count, archived_epoch_count) = epoch_counts.get(&year).copied().unwrap_or((0, 0));
        years.push(build_year(
            year,
            epoch_count,
            archived_epoch_count,
            &day_epochs,
            &today,
            interruption_index,
            &interruption_by_date,
            &missing_interruption_epochs,
        ));
    }
    years
}

#[allow(clippy::too_many_arguments)]
fn build_year(
    year: i32,
    epoch_count: usize,
    archived_epoch_count: usize,
    day_epochs: &BTreeMap<String, Vec<(u32, DayTone, String, bool)>>,
    today: &str,
    interruption_index: Option<&BlockTimeGapIndex>,
    interruption_by_date: &BTreeMap<String, &crate::snapshot::BlockTimeInterruptionDay>,
    missing_interruption_epochs: &std::collections::BTreeSet<u32>,
) -> CalendarYear {
    let year_start_days = days_from_civil(year, 1, 1);
    let next_year_start_days = days_from_civil(year + 1, 1, 1);
    let first_weekday = weekday_from_days(year_start_days);

    let mut days = Vec::new();
    let mut interruption_day_count = 0usize;
    let mut missing_interruption_coverage_day_count = 0usize;

    for day_index in year_start_days..next_year_start_days {
        let date = iso_date(day_index * SECONDS_PER_DAY);
        let day_of_year = (day_index - year_start_days) as u32;
        let week = (first_weekday + day_of_year) / 7;

        let mut entries = day_epochs.get(&date).cloned().unwrap_or_default();
        entries.sort_by(|left, right| right.1.priority().cmp(&left.1.priority()).then(right.0.cmp(&left.0)));
        let primary = entries.first().cloned();
        // `entries` stays priority-sorted (needed to pick `primary` above),
        // but the displayed epoch list re-sorts ascending -- matches the
        // second `.sort()` in `buildEpochYearCalendars`'s day-building loop.
        let mut epochs: Vec<u32> = entries.iter().map(|entry| entry.0).collect();
        epochs.sort_unstable();

        let interruption = interruption_by_date.get(&date).map(|day| InterruptionSummary {
            count: day.interruption_count,
            longest_secs: day.longest_interruption_secs,
            boundary_count: day.boundary_interruption_count,
        });
        let coverage = interruption_coverage_for(&epochs, interruption_index, missing_interruption_epochs);
        if interruption.is_some() {
            interruption_day_count += 1;
        }
        let future = date.as_str() > today;
        if primary.is_some() && !future && coverage == InterruptionCoverage::Missing {
            missing_interruption_coverage_day_count += 1;
        }

        // A day only gets colored content if it has a primary epoch and
        // isn't in the future; otherwise it's an empty placeholder square
        // (still present in `days` so the grid stays a filled rectangle --
        // mirrors the Svelte template's `{#if day.primary_epoch !== null
        // && !day.future} ... {:else} <span class="empty-day">`).
        let content = (!future).then(|| primary.map(|(_, tone, label, estimated)| CalendarDayContent {
            epochs,
            color: tone.color(),
            label,
            interruption,
            interruption_coverage: coverage,
            estimated,
        })).flatten();
        days.push(CalendarDay {
            date: date.clone(),
            week,
            weekday: weekday_from_days(day_index),
            today: date == today,
            future,
            content,
        });
    }

    let week_count = days.last().map(|day| day.week + 1).unwrap_or(0);
    let months = (0..12)
        .map(|month| {
            let first = days_from_civil(year, month + 1, 1);
            let day_of_year = (first - year_start_days) as u32;
            (month_label(month), (first_weekday + day_of_year) / 7)
        })
        .collect();

    CalendarYear {
        year,
        week_count,
        epoch_count,
        archived_epoch_count,
        interruption_day_count,
        missing_interruption_coverage_day_count,
        months,
        days,
    }
}

fn interruption_coverage_for(
    epochs: &[u32],
    index: Option<&BlockTimeGapIndex>,
    missing_epochs: &std::collections::BTreeSet<u32>,
) -> InterruptionCoverage {
    let Some(index) = index else { return InterruptionCoverage::Unavailable };
    if epochs.is_empty() {
        return InterruptionCoverage::Outside;
    }
    if epochs.iter().any(|&epoch| epoch < index.coverage.start_epoch || epoch > index.coverage.end_epoch) {
        return InterruptionCoverage::Outside;
    }
    if epochs.iter().any(|epoch| missing_epochs.contains(epoch)) {
        InterruptionCoverage::Missing
    } else {
        InterruptionCoverage::Covered
    }
}

fn utc_day_start(unix_secs: i64) -> i64 {
    unix_secs.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

fn iso_date(unix_secs: i64) -> String {
    let (year, month, day) = civil_from_days(unix_secs.div_euclid(SECONDS_PER_DAY));
    format!("{year:04}-{month:02}-{day:02}")
}

fn month_label(zero_based_month: u32) -> String {
    const NAMES: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    NAMES[zero_based_month as usize % 12].to_string()
}

/// 1970-01-01 was a Thursday (weekday 4 in JS's Sunday=0 numbering); days
/// since the epoch advance the weekday at the same fixed offset.
fn weekday_from_days(days_since_epoch: i64) -> u32 {
    (days_since_epoch + 4).rem_euclid(7) as u32
}

/// Civil (year, 1-based month, 1-based day) from days since the Unix
/// epoch. Same algorithm as `components.rs::format_unix_timestamp` -- see
/// Howard Hinnant's `civil_from_days`:
/// <https://howardhinnant.github.io/date_algorithms.html>.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    (year as i32, month as u32, day as u32)
}

/// Days since the Unix epoch for a civil (year, 1-based month, 1-based
/// day). Inverse of `civil_from_days`, same Hinnant algorithm
/// (`days_from_civil`).
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 }.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_and_back_roundtrip_known_dates() {
        // 1970-01-01 is day 0 by definition.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        // 2026-08-05 (session "today") -- cross-check both directions.
        let days = days_from_civil(2026, 8, 5);
        assert_eq!(civil_from_days(days), (2026, 8, 5));
        // A leap-year boundary.
        assert_eq!(civil_from_days(days_from_civil(2024, 2, 29)), (2024, 2, 29));
        assert_eq!(civil_from_days(days_from_civil(2024, 3, 1)), (2024, 3, 1));
    }

    #[test]
    fn weekday_matches_known_reference_days() {
        // 1970-01-01 was a Thursday (4 in Sunday=0 numbering).
        assert_eq!(weekday_from_days(0), 4);
        // 2000-01-01 was a Saturday (6).
        assert_eq!(weekday_from_days(days_from_civil(2000, 1, 1)), 6);
    }

    #[test]
    fn reference_calendar_parses_and_is_sorted_by_epoch() {
        let calendar = reference_calendar();
        assert!(calendar.len() > 900, "expected the full bundled history, got {}", calendar.len());
        for window in calendar.windows(2) {
            assert!(window[0].epoch < window[1].epoch, "reference calendar must be epoch-ordered");
        }
    }

    #[test]
    fn merge_calendars_prefers_authoritative_on_conflict() {
        let reference = vec![EpochCalendarEntry {
            epoch: 5,
            start_unix_secs: 100,
            end_unix_secs: Some(200),
            precision: CalendarPrecision::Estimated,
        }];
        let authoritative = vec![EpochCalendarEntry {
            epoch: 5,
            start_unix_secs: 101,
            end_unix_secs: Some(201),
            precision: CalendarPrecision::Observed,
        }];
        let merged = merge_calendars(&reference, &authoritative);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].precision, CalendarPrecision::Observed);
        assert_eq!(merged[0].start_unix_secs, 101);
    }

    #[test]
    fn extend_tail_extrapolates_through_the_latest_tracked_epoch() {
        let calendar = vec![
            EpochCalendarEntry { epoch: 1, start_unix_secs: 0, end_unix_secs: Some(1000), precision: CalendarPrecision::Observed },
            EpochCalendarEntry { epoch: 2, start_unix_secs: 1000, end_unix_secs: None, precision: CalendarPrecision::Observed },
        ];
        let extended = extend_tail(&calendar, Some(4));
        assert_eq!(extended.len(), 4);
        assert_eq!(extended[1].end_unix_secs, Some(2000), "open-ended entry gets closed at the estimated epoch duration");
        assert_eq!(extended[1].precision, CalendarPrecision::Estimated);
        assert_eq!(extended[3].epoch, 4);
        assert_eq!(extended[3].end_unix_secs, None, "the final requested epoch stays open-ended");
    }

    #[test]
    fn build_years_marks_complete_epoch_day_and_skips_future_days() {
        let epochs = vec![EpochStatus { epoch: 1, state: "complete".into(), ..Default::default() }];
        let calendar = vec![EpochCalendarEntry {
            epoch: 1,
            start_unix_secs: 1_700_000_000, // 2023-11-14T22:13:20Z
            end_unix_secs: Some(1_700_000_000 + 200_000),
            precision: CalendarPrecision::Observed,
        }];
        let now = 1_700_000_000 + 200_000;
        let years = build_years(&epochs, &calendar, now, None);
        let total_days: usize = years.iter().map(|year| year.days.len()).sum();
        assert!(total_days > 0, "expected at least one rendered day");
        let touched = years.iter().flat_map(|year| &year.days).find(|day| day.content.as_ref().is_some_and(|content| content.epochs.contains(&1)));
        assert!(touched.is_some(), "the epoch's date range must produce at least one calendar day");
        assert_eq!(touched.unwrap().content.as_ref().unwrap().color, "emerald");
        for year in &years {
            for day in &year.days {
                assert!(
                    day.content.is_none() || day.date <= iso_date(now as i64),
                    "no day with content should be in the future: {}",
                    day.date
                );
                assert_eq!(day.future, day.date > iso_date(now as i64));
            }
        }
    }

    #[test]
    fn archived_epoch_count_is_per_epoch_not_per_day() {
        // An epoch spanning 5 days must count once toward `archived_epoch_count`,
        // not once per day it touches -- caught during review: an earlier draft
        // incremented the counter inside the day loop.
        let epochs = vec![EpochStatus { epoch: 1, state: "complete".into(), ..Default::default() }];
        let five_days = 5 * SECONDS_PER_DAY as u64;
        let calendar = vec![EpochCalendarEntry {
            epoch: 1,
            start_unix_secs: 1_700_000_000,
            end_unix_secs: Some(1_700_000_000 + five_days),
            precision: CalendarPrecision::Observed,
        }];
        let now = 1_700_000_000 + five_days;
        let years = build_years(&epochs, &calendar, now, None);
        assert_eq!(years.len(), 1, "a 5-day span shouldn't cross a year boundary in this fixture");
        assert_eq!(years[0].epoch_count, 1);
        assert_eq!(years[0].archived_epoch_count, 1);
        assert!(years[0].days.len() >= 5, "expected multiple rendered days for the one epoch");
    }

    #[test]
    fn multi_epoch_day_lists_epochs_ascending_even_though_priority_order_differs() {
        // Two epochs touch the same day: epoch 0 (complete, lower priority)
        // and epoch 1 (also complete, but higher epoch number breaks the
        // priority tie so it becomes primary). The displayed epoch list
        // must still read ascending (0, 1), not primary-first (1, 0).
        let epochs = vec![
            EpochStatus { epoch: 0, state: "complete".into(), ..Default::default() },
            EpochStatus { epoch: 1, state: "complete".into(), ..Default::default() },
        ];
        let calendar = vec![
            EpochCalendarEntry { epoch: 0, start_unix_secs: 1_700_000_000, end_unix_secs: Some(1_700_000_000 + 100), precision: CalendarPrecision::Observed },
            EpochCalendarEntry { epoch: 1, start_unix_secs: 1_700_000_000, end_unix_secs: Some(1_700_000_000 + 100), precision: CalendarPrecision::Observed },
        ];
        let years = build_years(&epochs, &calendar, 1_700_000_000 + 100, None);
        let day = years.iter().flat_map(|year| &year.days).find(|day| day.content.is_some()).unwrap();
        assert_eq!(day.content.as_ref().unwrap().epochs, vec![0, 1]);
    }

    #[test]
    fn epoch_crossing_a_year_boundary_is_attributed_to_its_start_year_only() {
        // 2023-12-30 -> 2024-01-03: an epoch whose range crosses a year
        // boundary must only be counted in the year it *started*, even
        // though its date range produces rendered days in both years.
        let start = days_from_civil(2023, 12, 30) * SECONDS_PER_DAY;
        let end = days_from_civil(2024, 1, 4) * SECONDS_PER_DAY;
        let epochs = vec![EpochStatus { epoch: 1, state: "complete".into(), ..Default::default() }];
        let calendar = vec![EpochCalendarEntry {
            epoch: 1,
            start_unix_secs: start as u64,
            end_unix_secs: Some(end as u64),
            precision: CalendarPrecision::Observed,
        }];
        let years = build_years(&epochs, &calendar, end as u64, None);
        let by_year: BTreeMap<i32, &CalendarYear> = years.iter().map(|year| (year.year, year)).collect();
        assert_eq!(by_year[&2023].epoch_count, 1, "counted in its start year");
        assert_eq!(by_year[&2023].archived_epoch_count, 1);
        assert_eq!(by_year.get(&2024).map(|year| year.epoch_count).unwrap_or(0), 0, "not double-counted in the year it merely continues into");
        // But the grid still renders a colored cell on the 2024 side too.
        assert!(by_year[&2024].days.iter().any(|day| day.content.as_ref().is_some_and(|content| content.epochs.contains(&1))));
    }
}

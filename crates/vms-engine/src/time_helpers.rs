//! Time-related helpers shared across the engine.

use chrono_tz::Tz;

/// Normalise a cron expression to the 6-field format required by `croner`
/// (the parser used by `tokio-cron-scheduler`).
///
/// Standard 5-field cron (`min hour dom month dow`) is promoted to 6-field
/// by prepending a `0` seconds field. Expressions that already have 6+ fields
/// are returned unchanged.
pub fn normalize_cron(expr: &str) -> String {
    if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    }
}

/// Parse an IANA timezone string (e.g. `"America/New_York"`).
///
/// Returns `Tz::UTC` and logs a warning if the string is not recognised.
pub fn parse_iana_tz(timezone: &str) -> Tz {
    timezone.parse().unwrap_or_else(|_| {
        tracing::warn!(timezone, "Unknown IANA timezone, falling back to UTC");
        Tz::UTC
    })
}

/// Convert a naive local date/time in `tz` to its UTC instant, resolving
/// DST transitions conservatively: an ambiguous (clocks-back) time picks
/// the earlier of its two instants; a nonexistent (clocks-forward) time is
/// nudged an hour later before retrying, falling back to treating it as
/// already UTC if that still doesn't resolve.
pub fn local_to_utc(tz: Tz, naive: chrono::NaiveDateTime) -> chrono::DateTime<chrono::Utc> {
    use chrono::TimeZone;
    match tz.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&chrono::Utc),
        chrono::LocalResult::Ambiguous(earlier, _later) => earlier.with_timezone(&chrono::Utc),
        chrono::LocalResult::None => tz
            .from_local_datetime(&(naive + chrono::Duration::hours(1)))
            .single()
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(|| chrono::Utc.from_utc_datetime(&naive)),
    }
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_cron_promotes_five_field() {
        assert_eq!(normalize_cron("0 2 * * *"), "0 0 2 * * *");
    }

    #[test]
    fn normalize_cron_keeps_six_field() {
        assert_eq!(normalize_cron("0 0 2 * * *"), "0 0 2 * * *");
    }

    #[test]
    fn parse_iana_tz_known() {
        let tz = parse_iana_tz("America/New_York");
        assert_eq!(tz, chrono_tz::America::New_York);
    }

    #[test]
    fn parse_iana_tz_unknown_falls_back_to_utc() {
        let tz = parse_iana_tz("Not/A/Timezone");
        assert_eq!(tz, Tz::UTC);
    }

    #[test]
    fn local_to_utc_converts_a_positive_offset() {
        // Tehran is UTC+03:30 with no DST. Local midnight is the previous
        // UTC day, 20:30.
        let midnight = chrono::NaiveDate::from_ymd_opt(2026, 9, 22)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let utc = local_to_utc(chrono_tz::Asia::Tehran, midnight);
        assert_eq!(utc.to_string(), "2026-09-21 20:30:00 UTC");
    }

    #[test]
    fn local_to_utc_resolves_an_ambiguous_dst_fallback_time() {
        // 2026-11-01 01:30 America/New_York occurs twice (DST ends at 2am).
        // Must resolve to a valid instant rather than panicking.
        let ambiguous = chrono::NaiveDate::from_ymd_opt(2026, 11, 1)
            .unwrap()
            .and_hms_opt(1, 30, 0)
            .unwrap();
        let utc = local_to_utc(chrono_tz::America::New_York, ambiguous);
        assert!(utc.to_string().starts_with("2026-11-01"));
    }
}

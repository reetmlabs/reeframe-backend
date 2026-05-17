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
        tracing::warn!(timezone, "Unknown IANA timezone — falling back to UTC");
        Tz::UTC
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

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
}

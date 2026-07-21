//! Parameter parsing and validation helpers for `read_jetstream`.
//!
//! This module holds the pure, network-free logic that turns user-supplied
//! parameters into typed configuration: the [`StartSpec`] delivery-policy
//! selector, NATS [`subject_matches`] token matching, and DuckDB timestamp
//! parsing ([`parse_timestamp_micros`]). Keeping these free of I/O makes them
//! directly unit-testable without a live NATS server.

use async_nats::jetstream;

use crate::error::ScanError;

/// The starting delivery point for a consumer, mirroring JetStream's
/// `DeliverPolicy`. Selected by the `start` parameter and honored only when a
/// consumer is first created (an existing durable consumer resumes from its
/// stored cursor regardless).
///
/// `ByStartSeq`/`ByStartTime` reuse the `start_seq`/`start_time` parameters,
/// resolved to the concrete policy in [`StartSpec::into_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartSpec {
    All,
    New,
    Last,
    ByStartSeq,
    ByStartTime,
}

impl StartSpec {
    /// Parse the `start` parameter. Unknown values are rejected.
    pub fn parse(value: &str) -> Result<Self, ScanError> {
        match value {
            "all" => Ok(Self::All),
            "new" => Ok(Self::New),
            "last" => Ok(Self::Last),
            "by_start_seq" => Ok(Self::ByStartSeq),
            "by_start_time" => Ok(Self::ByStartTime),
            other => Err(ScanError::InvalidStart {
                value: other.to_string(),
            }),
        }
    }

    /// Resolve to a concrete JetStream [`DeliverPolicy`], pulling the start
    /// sequence/time from the parameters the `by_start_*` variants depend on.
    ///
    /// `start_time_micros` is epoch microseconds (DuckDB TIMESTAMP, treated as
    /// UTC); it is converted to the `OffsetDateTime` the policy requires.
    ///
    /// [`DeliverPolicy`]: jetstream::consumer::DeliverPolicy
    pub fn into_policy(
        self,
        start_seq: Option<u64>,
        start_time_micros: Option<i64>,
    ) -> Result<jetstream::consumer::DeliverPolicy, ScanError> {
        use jetstream::consumer::DeliverPolicy;
        match self {
            Self::All => Ok(DeliverPolicy::All),
            Self::New => Ok(DeliverPolicy::New),
            Self::Last => Ok(DeliverPolicy::Last),
            Self::ByStartSeq => {
                let start_sequence = start_seq.ok_or(ScanError::StartNeedsStartSeq)?;
                Ok(DeliverPolicy::ByStartSequence { start_sequence })
            }
            Self::ByStartTime => {
                let micros = start_time_micros.ok_or(ScanError::StartNeedsStartTime)?;
                let nanos = (micros as i128) * 1_000;
                let start_time = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
                    .map_err(|_| ScanError::StartNeedsStartTime)?;
                Ok(DeliverPolicy::ByStartTime { start_time })
            }
        }
    }
}

/// Match a NATS subject against a filter using NATS token semantics.
///
/// Tokens are separated by `.`. The wildcard `*` matches exactly one token;
/// `>` matches one or more trailing tokens and is only valid as the final
/// token. This is true NATS matching, not substring matching.
pub fn subject_matches(filter: &str, subject: &str) -> bool {
    let mut f = filter.split('.');
    let mut s = subject.split('.');

    loop {
        match (f.next(), s.next()) {
            // `>` matches one or more trailing tokens: requires a token here.
            (Some(">"), Some(_)) => return true,
            (Some(">"), None) => return false,
            (Some("*"), Some(_)) => continue,
            (Some(ft), Some(st)) if ft == st => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Parse DuckDB's canonical `TIMESTAMP` string rendering into microseconds
/// since the Unix epoch, treating the value as UTC (DuckDB `TIMESTAMP` is
/// timezone-naive; NATS server times are UTC, so this keeps the two consistent).
///
/// Accepts `YYYY-MM-DD HH:MM:SS` with optional fractional seconds.
pub fn parse_timestamp_micros(s: &str) -> Result<i64, ScanError> {
    use time::format_description::BorrowedFormatItem;
    use time::macros::format_description;
    use time::PrimitiveDateTime;

    const WITH_FRACTION: &[BorrowedFormatItem<'_>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
    const WHOLE_SECONDS: &[BorrowedFormatItem<'_>] =
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

    let parsed = PrimitiveDateTime::parse(s, WITH_FRACTION)
        .or_else(|_| PrimitiveDateTime::parse(s, WHOLE_SECONDS))
        .map_err(|_| ScanError::BadTimestamp {
            value: s.to_string(),
        })?;

    let nanos = parsed.assume_utc().unix_timestamp_nanos();
    Ok((nanos / 1_000) as i64)
}

#[cfg(test)]
mod tests {
    use super::{parse_timestamp_micros, subject_matches, StartSpec};
    use async_nats::jetstream::consumer::DeliverPolicy;

    #[test]
    fn exact_match() {
        assert!(subject_matches("orders.new", "orders.new"));
        assert!(!subject_matches("orders.new", "orders.shipped"));
    }

    #[test]
    fn single_token_wildcard() {
        assert!(subject_matches("orders.*", "orders.new"));
        assert!(subject_matches("orders.*", "orders.shipped"));
        // `*` matches exactly one token, not multiple.
        assert!(!subject_matches("orders.*", "orders.us.new"));
        // ...and not zero tokens.
        assert!(!subject_matches("orders.*", "orders"));
    }

    #[test]
    fn multi_token_wildcard() {
        assert!(subject_matches("orders.>", "orders.new"));
        assert!(subject_matches("orders.>", "orders.us.new"));
        // `>` requires at least one trailing token.
        assert!(!subject_matches("orders.>", "orders"));
    }

    #[test]
    fn mixed_wildcards() {
        assert!(subject_matches("orders.*.new", "orders.us.new"));
        assert!(!subject_matches("orders.*.new", "orders.us.shipped"));
        assert!(subject_matches("*.>", "orders.us.new"));
    }

    #[test]
    fn length_mismatch() {
        assert!(!subject_matches("orders", "orders.new"));
        assert!(!subject_matches("orders.new.extra", "orders.new"));
    }

    #[test]
    fn timestamp_with_fraction() {
        // 2026-07-14 21:14:00.366769 UTC
        assert_eq!(
            parse_timestamp_micros("2026-07-14 21:14:00.366769").unwrap(),
            1_784_063_640_366_769
        );
    }

    #[test]
    fn timestamp_whole_seconds() {
        // 2030-01-01 00:00:00 UTC
        assert_eq!(
            parse_timestamp_micros("2030-01-01 00:00:00").unwrap(),
            1_893_456_000_000_000
        );
    }

    #[test]
    fn timestamp_invalid() {
        assert!(parse_timestamp_micros("not a timestamp").is_err());
    }

    #[test]
    fn start_spec_parses_known_values() {
        assert_eq!(StartSpec::parse("all").unwrap(), StartSpec::All);
        assert_eq!(StartSpec::parse("new").unwrap(), StartSpec::New);
        assert_eq!(StartSpec::parse("last").unwrap(), StartSpec::Last);
        assert_eq!(
            StartSpec::parse("by_start_seq").unwrap(),
            StartSpec::ByStartSeq
        );
        assert_eq!(
            StartSpec::parse("by_start_time").unwrap(),
            StartSpec::ByStartTime
        );
    }

    #[test]
    fn start_spec_rejects_unknown() {
        assert!(StartSpec::parse("newest").is_err());
        assert!(StartSpec::parse("").is_err());
    }

    #[test]
    fn start_spec_simple_policies_ignore_params() {
        assert!(matches!(
            StartSpec::All.into_policy(None, None).unwrap(),
            DeliverPolicy::All
        ));
        assert!(matches!(
            StartSpec::New.into_policy(Some(5), Some(1)).unwrap(),
            DeliverPolicy::New
        ));
        assert!(matches!(
            StartSpec::Last.into_policy(None, None).unwrap(),
            DeliverPolicy::Last
        ));
    }

    #[test]
    fn start_spec_by_start_seq_uses_start_seq() {
        match StartSpec::ByStartSeq.into_policy(Some(42), None).unwrap() {
            DeliverPolicy::ByStartSequence { start_sequence } => assert_eq!(start_sequence, 42),
            other => panic!("expected ByStartSequence, got {other:?}"),
        }
        // Missing start_seq is an error.
        assert!(StartSpec::ByStartSeq.into_policy(None, None).is_err());
    }

    #[test]
    fn start_spec_by_start_time_uses_start_time() {
        // 2030-01-01 00:00:00 UTC == 1_893_456_000_000_000 micros.
        let micros = 1_893_456_000_000_000;
        match StartSpec::ByStartTime
            .into_policy(None, Some(micros))
            .unwrap()
        {
            DeliverPolicy::ByStartTime { start_time } => {
                assert_eq!(start_time.unix_timestamp_nanos(), (micros as i128) * 1_000);
            }
            other => panic!("expected ByStartTime, got {other:?}"),
        }
        // Missing start_time is an error.
        assert!(StartSpec::ByStartTime.into_policy(None, None).is_err());
    }
}

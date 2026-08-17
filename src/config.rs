//! Parameter parsing and validation helpers for `read_jetstream`.
//!
//! This module holds the pure, network-free logic that turns user-supplied
//! parameters into typed configuration: the [`StartSpec`] delivery-policy
//! selector, NATS [`subject_matches`] token matching, DuckDB timestamp parsing
//! ([`parse_timestamp_micros`]), and the [`BindParams`] aggregate that pulls
//! every named parameter off a [`BindInfo`] and cross-validates it. Keeping
//! these free of I/O makes them directly unit-testable without a live NATS
//! server.

use async_nats::jetstream;
use duckdb::vtab::BindInfo;

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

/// The DuckDB type presented for the `payload` column, selected by the `format`
/// parameter.
///
/// This is a whole-payload retype, independent of the `json_extract`/`proto_*`
/// extraction params (which add their own columns). [`Json`](Self::Json) emits
/// VARCHAR aliased as `JSON` so the `json` extension operators (`->`, `->>`)
/// apply without a cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Blob,
    Text,
    Json,
}

impl PayloadFormat {
    /// Parse the `format` parameter. Unknown values are rejected.
    pub fn parse(value: &str) -> Result<Self, ScanError> {
        match value {
            "blob" => Ok(Self::Blob),
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(ScanError::InvalidFormat {
                value: other.to_string(),
            }),
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

/// Every parsed and cross-validated `read_jetstream` parameter except the
/// derived protobuf descriptor/fields and the live consumer setup, which are
/// built from these params in `bind`.
///
/// Construct via [`BindParams::from_bind`], which performs all extraction and
/// validation so `bind` is left with column declaration and consumer creation.
pub struct BindParams {
    pub stream: String,
    pub url: String,
    pub subject: Option<String>,
    pub start_seq: Option<u64>,
    pub end_seq: Option<u64>,
    pub start_time_micros: Option<i64>,
    pub end_time_micros: Option<i64>,
    pub json_fields: Vec<String>,
    pub format: PayloadFormat,
    pub ignore_errors: bool,
    pub proto_file: Option<String>,
    pub proto_message: Option<String>,
    pub proto_paths: Vec<String>,
    pub ephemeral: bool,
    pub durable: Option<String>,
    pub batch: u64,
    pub max_messages: Option<u64>,
    pub start: Option<StartSpec>,
}

impl BindParams {
    /// True when the call uses the protobuf decode path (any proto_ parameter
    /// supplied). When true, `proto_file` and `proto_message` must both be set.
    pub fn using_proto(&self) -> bool {
        !self.proto_paths.is_empty() || self.proto_file.is_some() || self.proto_message.is_some()
    }

    /// True when the call uses a JetStream consumer (ephemeral or durable)
    /// rather than the stateless Direct Get scan.
    pub fn is_consumer(&self) -> bool {
        self.ephemeral || self.durable.is_some()
    }

    /// Extract and cross-validate every `read_jetstream` parameter from a
    /// [`BindInfo`]. Returns [`ScanError`] for any invalid combination.
    pub fn from_bind(bind: &BindInfo) -> Result<Self, ScanError> {
        let json_fields: Vec<String> = bind
            .get_named_parameter("json_extract")
            .and_then(|v| v.to_list())
            .map(|items| items.iter().map(|v| v.to_string()).collect())
            .unwrap_or_default();

        let ignore_errors = bind
            .get_named_parameter("ignore_errors")
            .map(|v| v.to_bool())
            .unwrap_or(false);

        let proto_file = bind
            .get_named_parameter("proto_file")
            .map(|v| v.to_string());
        let proto_message = bind
            .get_named_parameter("proto_message")
            .map(|v| v.to_string());
        let proto_paths: Vec<String> = bind
            .get_named_parameter("proto_extract")
            .and_then(|v| v.to_list())
            .map(|items| items.iter().map(|v| v.to_string()).collect())
            .unwrap_or_default();

        let format = bind
            .get_named_parameter("format")
            .map(|v| PayloadFormat::parse(&v.to_string()))
            .transpose()?
            .unwrap_or_default();

        if !json_fields.is_empty() && !proto_paths.is_empty() {
            return Err(ScanError::DecodeConflict);
        }
        if Self::proto_incomplete(
            proto_file.is_some(),
            proto_message.is_some(),
            !proto_paths.is_empty(),
        )? {
            // format => 'json' serializes the whole message, so it needs no
            // named fields; proto_extract is otherwise required.
            if proto_paths.is_empty() && format != PayloadFormat::Json {
                return Err(ScanError::ProtoNoFields);
            }
        }

        let stream = bind.get_parameter(0).to_string();

        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| super::DEFAULT_URL.to_string());

        let subject = bind
            .get_named_parameter("subject")
            .map(|v| {
                let s = v.to_string();
                if async_nats::Subject::from(s.clone()).is_valid() {
                    Ok(s)
                } else {
                    Err(ScanError::InvalidSubject { value: s })
                }
            })
            .transpose()?;

        let start_seq = bind.get_named_parameter("start_seq").map(|v| v.to_uint64());
        let end_seq = bind.get_named_parameter("end_seq").map(|v| v.to_uint64());
        // TIMESTAMP values cannot be read via the primitive integer getters,
        // so parse DuckDB's canonical string rendering into epoch microseconds.
        let start_time_micros = bind
            .get_named_parameter("start_time")
            .map(|v| parse_timestamp_micros(&v.to_string()))
            .transpose()?;
        let end_time_micros = bind
            .get_named_parameter("end_time")
            .map(|v| parse_timestamp_micros(&v.to_string()))
            .transpose()?;

        let ephemeral = bind
            .get_named_parameter("ephemeral")
            .map(|v| v.to_bool())
            .unwrap_or(false);
        let durable = bind
            .get_named_parameter("durable")
            .map(|v| v.to_string())
            .filter(|s| !s.is_empty());

        let batch_param = bind.get_named_parameter("batch").map(|v| v.to_uint64());
        let max_messages = bind
            .get_named_parameter("max_messages")
            .map(|v| v.to_uint64());

        let is_consumer = ephemeral || durable.is_some();
        if ephemeral && durable.is_some() {
            return Err(ScanError::ModeConflict);
        }
        if batch_param.is_some() && !is_consumer {
            return Err(ScanError::ConsumerOnlyParam { param: "batch" });
        }
        if max_messages.is_some() && !is_consumer {
            return Err(ScanError::ConsumerOnlyParam {
                param: "max_messages",
            });
        }
        if batch_param == Some(0) {
            return Err(ScanError::ZeroBatch);
        }

        let start = bind
            .get_named_parameter("start")
            .map(|v| StartSpec::parse(&v.to_string()))
            .transpose()?;

        Ok(Self {
            stream,
            url,
            subject,
            start_seq,
            end_seq,
            start_time_micros,
            end_time_micros,
            json_fields,
            format,
            ignore_errors,
            proto_file,
            proto_message,
            proto_paths,
            ephemeral,
            durable,
            batch: batch_param.unwrap_or(super::DEFAULT_BATCH),
            max_messages,
            start,
        })
    }

    /// Validate the proto_file/proto_message/proto_extract triple, returning
    /// `Ok(true)` when the protobuf path is in use, `Ok(false)` when it is not,
    /// and `Err` for any incomplete combination.
    fn proto_incomplete(
        has_file: bool,
        has_message: bool,
        has_paths: bool,
    ) -> Result<bool, ScanError> {
        let using = has_file || has_message || has_paths;
        if !using {
            return Ok(false);
        }
        match (has_file, has_message) {
            (false, true) => Err(ScanError::ProtoIncomplete {
                present: "proto_message",
                missing: "proto_file",
            }),
            (true, false) => Err(ScanError::ProtoIncomplete {
                present: "proto_file",
                missing: "proto_message",
            }),
            (false, false) => Err(ScanError::ProtoIncomplete {
                present: "proto_extract",
                missing: "proto_file and proto_message",
            }),
            (true, true) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_timestamp_micros, subject_matches, PayloadFormat, StartSpec};
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
    fn payload_format_parses_known_values() {
        assert_eq!(PayloadFormat::parse("blob").unwrap(), PayloadFormat::Blob);
        assert_eq!(PayloadFormat::parse("text").unwrap(), PayloadFormat::Text);
        assert_eq!(PayloadFormat::parse("json").unwrap(), PayloadFormat::Json);
    }

    #[test]
    fn payload_format_rejects_unknown() {
        assert!(PayloadFormat::parse("bytes").is_err());
        assert!(PayloadFormat::parse("").is_err());
    }

    #[test]
    fn payload_format_defaults_to_blob() {
        assert_eq!(PayloadFormat::default(), PayloadFormat::Blob);
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

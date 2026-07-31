//! Error types for the NATS/JetStream scan layer.
//!
//! [`ScanError`] covers parameter validation, connection/stream lookup, and
//! consumer creation. It is converted to `Box<dyn Error>` at the
//! [`VTab`](duckdb::vtab::VTab) boundary, which DuckDB renders as the query
//! error message.

use std::error::Error;

/// Errors surfaced from the NATS/JetStream layer.
///
/// These are converted to `Box<dyn Error>` at the [`VTab`](duckdb::vtab::VTab)
/// boundary, which DuckDB renders as the query error message.
#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("failed to connect to NATS at {url}: {source}")]
    Connect {
        url: String,
        #[source]
        source: async_nats::ConnectError,
    },
    #[error("failed to look up stream '{stream}': {source}")]
    StreamInfo {
        stream: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("failed to create consumer on stream '{stream}': {source}")]
    Consumer {
        stream: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("could not parse timestamp '{value}'")]
    BadTimestamp { value: String },
    #[error("invalid NATS subject filter '{value}'")]
    InvalidSubject { value: String },
    #[error("json_extract and proto_extract cannot be used together")]
    DecodeConflict,
    #[error(
        "json_extract payload on stream '{stream}' at seq {seq} is not valid JSON{hint}. \
         Set ignore_errors => true to skip undecodable payloads."
    )]
    NonJsonPayload {
        stream: String,
        seq: u64,
        /// Leading `": ..."` hint suggesting the protobuf decoder, or empty.
        hint: String,
    },
    #[error(
        "proto payload on stream '{stream}' at seq {seq} failed to decode as the configured \
         message{hint}. Set ignore_errors => true to skip undecodable payloads."
    )]
    NonProtoPayload {
        stream: String,
        seq: u64,
        /// Leading `": ..."` hint suggesting the JSON decoder, or empty.
        hint: String,
    },
    #[error("{present} requires {missing} (both are needed to decode a protobuf payload)")]
    ProtoIncomplete {
        /// The proto parameter that was supplied.
        present: &'static str,
        /// The proto parameter that must also be supplied.
        missing: &'static str,
    },
    #[error("proto_file/proto_message require a non-empty proto_extract list")]
    ProtoNoFields,
    #[error("durable and ephemeral consumer modes are mutually exclusive")]
    ModeConflict,
    #[error("ack requires durable mode (ack => true only applies to durable consumers)")]
    AckRequiresDurable,
    #[error("{param} requires a consumer mode (set ephemeral => true or durable => 'name')")]
    ConsumerOnlyParam { param: &'static str },
    #[error("batch must be greater than zero")]
    ZeroBatch,
    #[error(
        "invalid start policy '{value}' (expected all, new, last, by_start_seq, by_start_time)"
    )]
    InvalidStart { value: String },
    #[error("start => 'by_start_seq' requires start_seq")]
    StartNeedsStartSeq,
    #[error("start => 'by_start_time' requires start_time")]
    StartNeedsStartTime,
    #[error("invalid format '{value}' (expected blob, text, or json)")]
    InvalidFormat { value: String },
    #[error(
        "payload on stream '{stream}' at seq {seq} is not valid UTF-8 (format => 'text'). \
         Set ignore_errors => true to skip undecodable payloads, or use format => 'blob'."
    )]
    NonUtf8Payload { stream: String, seq: u64 },
}

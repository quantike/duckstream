//! duckstream: a DuckDB extension for querying NATS JetStream.
//!
//! The `read_jetstream` table function is a bounded read with three modes: a
//! stateless Direct Get scan by sequence/time range, and ephemeral or durable
//! pull-consumer drains (see the crate README for the SQL contract).
//!
//! These bounded modes do not need the full async→sync bridge (background task +
//! bounded channel) that unbounded tail will require: each mode owns a Tokio
//! runtime in its init data and drives it with `Runtime::block_on` from the
//! synchronous `func` callback.
//!
//! # Module layout
//!
//! `lib.rs` holds only the DuckDB [`VTab`] glue (bind/init/func, parameter
//! declarations) and the C-API entrypoint. The reusable surfaces live in
//! focused modules so upcoming work (notably `read_jetstream_tail`) can build on
//! them:
//!
//! - [`error`]: the [`ScanError`](error::ScanError) type shared across modes.
//! - [`config`]: pure parameter parsing/validation ([`StartSpec`](config::StartSpec),
//!   subject matching, timestamp parsing).
//! - [`stream`]: async JetStream stream/consumer I/O (consumer creation, time→sequence resolution).
//! - [`row`]: the `Bytes`-backed [`Row`](row::Row) buffered between message
//!   acquisition and column writing.
//! - [`output`]: writing rows + extracted JSON/proto values into DuckDB vectors.
//! - [`proto`]: runtime protobuf compilation and decoding.

use std::error::Error;
use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{duckdb_entrypoint_c_api, Connection, Result};

use async_nats::jetstream;
use async_nats::HeaderMap;
use prost_reflect::MessageDescriptor;
use tokio::runtime::Runtime;

mod config;
mod error;
mod output;
mod proto;
mod row;
mod stream;

use config::{subject_matches, PayloadFormat, StartSpec};
use proto::ProtoField;
use row::Row;

/// DuckDB's standard vector size; `func` emits at most this many rows per call.
const VECTOR_SIZE: usize = 2048;

/// Default NATS server URL when the `url` parameter is not provided.
const DEFAULT_URL: &str = "nats://localhost:4222";

/// Default consumer fetch batch size (messages per `fetch` request) when the
/// `batch` parameter is not provided. Applies to ephemeral and durable modes.
const DEFAULT_BATCH: u64 = 256;

/// Ceiling on scan teardown (see [`ReadJetstreamInitData`]'s [`Drop`]). Matches
/// `async-nats`'s default `connection_timeout` of 5s.
const SCAN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Apply the optional `max_messages` hard cap to a consumer's pending count.
///
/// Returns the number of messages the drain should emit: `pending` when no cap
/// is set, otherwise `min(pending, cap)`. This is both the drain bound and the
/// reported query cardinality.
fn capped_pending(pending: u64, max_messages: Option<u64>) -> u64 {
    match max_messages {
        Some(cap) => pending.min(cap),
        None => pending,
    }
}

/// Compute the number of messages to request in a single consumer `fetch`.
///
/// A `func` call emits at most one output vector, so the fetch is bounded by
/// the smallest of: messages still owed (`remaining`, already capped by
/// `max_messages`), the user-requested `batch` size, and DuckDB's vector size.
fn fetch_want(remaining: u64, batch: u64, vector_size: usize) -> usize {
    remaining.min(batch).min(vector_size as u64) as usize
}

/// The DuckDB type for the `payload` column, given the requested `format` and
/// whether JSON extraction adds extra columns.
///
/// With `format` unset, extracting JSON implies a text payload, so the default
/// BLOB becomes VARCHAR rather than staying BLOB.
fn payload_column_type(format: PayloadFormat, json_fields: &[String]) -> LogicalTypeHandle {
    let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
    match format {
        PayloadFormat::Blob if json_fields.is_empty() => {
            LogicalTypeHandle::from(LogicalTypeId::Blob)
        }
        PayloadFormat::Blob | PayloadFormat::Text => varchar(),
        PayloadFormat::Json => {
            let mut t = varchar();
            t.set_alias("JSON");
            t
        }
    }
}

/// Serialize a [`HeaderMap`] to a JSON string, dropping NATS system headers
/// (`Nats-*`) that the scan path's Direct Get response includes but the
/// consumer path does not. Returns `None` when the map is empty after
/// filtering, so both paths yield SQL NULL for headerless messages.
fn serialize_headers(map: &HeaderMap) -> Option<String> {
    let filtered: std::collections::HashMap<&str, &Vec<_>> = map
        .iter()
        .filter(|(name, _)| !AsRef::<str>::as_ref(name).starts_with("Nats-"))
        .map(|(name, values)| (AsRef::<str>::as_ref(name), values))
        .collect();
    if filtered.is_empty() {
        return None;
    }
    serde_json::to_string(&filtered).ok()
}

/// Declare every result column on `bind`: the five base columns, the payload
/// column, and one extra column per JSON/proto extraction path. Extra columns
/// are named by the verbatim field path, dots preserved (e.g. `order.id`), so
/// callers quote them in SQL.
fn declare_columns(bind: &BindInfo, p: &config::BindParams, proto_fields: &[ProtoField]) {
    let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
    let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);
    let timestamp = || LogicalTypeHandle::from(LogicalTypeId::Timestamp);

    bind.add_result_column("stream", varchar());
    bind.add_result_column("subject", varchar());
    bind.add_result_column("seq", ubigint());
    bind.add_result_column("ts_nats", timestamp());
    bind.add_result_column("payload", payload_column_type(p.format, &p.json_fields));

    for field in &p.json_fields {
        bind.add_result_column(field, varchar());
    }
    for field in proto_fields {
        bind.add_result_column(&field.path, LogicalTypeHandle::from(field.column_type));
    }

    // Appended last so the hardcoded extra-column base index (5) in `func`
    // stays stable regardless of whether headers are enabled.
    if p.headers {
        let mut t = varchar();
        t.set_alias("JSON");
        bind.add_result_column("headers", t);
    }
}

/// Build the pull consumer (and the runtime that owns it) for a consumer-mode
/// read, and report its `num_pending` as the query cardinality.
///
/// `num_pending` is a point-in-time snapshot taken when the consumer was
/// created, not a guarantee, so it is reported as an estimate. `max_messages`
/// caps both the drain and the reported cardinality.
fn create_consumer_setup(
    bind: &BindInfo,
    p: &config::BindParams,
) -> Result<ConsumerSetup, Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;

    // Default to All: drain everything currently in the stream.
    let deliver_policy = p
        .start
        .unwrap_or(StartSpec::All)
        .into_policy(p.start_seq, p.start_time_micros)?;

    let (consumer, pending) = runtime.block_on(stream::create_consumer(
        &p.url,
        &p.stream,
        p.subject.as_deref(),
        p.durable.as_deref(),
        deliver_policy,
    ))?;
    let pending = capped_pending(pending, p.max_messages);
    bind.set_cardinality(pending, false);
    Ok(ConsumerSetup {
        runtime,
        consumer,
        pending,
        batch: p.batch,
    })
}

/// Pull up to [`VECTOR_SIZE`] messages from the active source into a common
/// `Vec<Row>`, advancing the source cursor and setting `state.done` when the
/// source is exhausted.
///
/// A [`Row`] holds reference-counted `Bytes` for the subject and payload, so
/// buffering clones no message bytes. The buffer decouples acquisition from
/// column writing, which the tail path (an async channel drain) will need.
fn acquire_rows(state: &mut ScanState, runtime: &Runtime) -> Vec<Row> {
    let subject_filter = state.subject.clone();
    let headers_enabled = state.headers;
    let mut rows: Vec<Row> = Vec::with_capacity(VECTOR_SIZE);

    match &mut state.source {
        Source::Scan {
            stream,
            current_seq,
            end_seq,
            ..
        } => {
            while rows.len() < VECTOR_SIZE && *current_seq <= *end_seq {
                let seq = *current_seq;
                *current_seq = seq.saturating_add(1);

                // Prefer Direct Get (replica-served); fall back to the
                // leader-only raw API for streams without `allow_direct`. A
                // missing sequence (deleted/purged) is skipped.
                //
                // PERF: one blocking network round-trip per message. On large
                // ranges this is the dominant cost. A batched Direct Get
                // (multi-message request) or a pull-consumer drain would cut
                // the round-trips dramatically; the consumer path below
                // already fetches in batches for this reason.
                let fetched = runtime.block_on(async {
                    match stream.direct_get(seq).await {
                        Ok(msg) => Some(msg),
                        Err(_) => stream.get_raw_message(seq).await.ok(),
                    }
                });
                let Some(msg) = fetched else {
                    continue;
                };

                // Scan filtering is client-side (no server consumer).
                if let Some(filter) = &subject_filter {
                    if !subject_matches(filter, msg.subject.as_str()) {
                        continue;
                    }
                }

                rows.push(
                    Row::new(
                        msg.subject,
                        msg.sequence,
                        (msg.time.unix_timestamp_nanos() / 1_000) as i64,
                        msg.payload,
                    )
                    .with_headers(
                        headers_enabled
                            .then(|| serialize_headers(&msg.headers))
                            .flatten(),
                    ),
                );
            }
            if *current_seq > *end_seq {
                state.done = true;
            }
        }
        Source::Consumer {
            consumer,
            remaining,
            batch,
        } => {
            use futures_util::StreamExt;

            let want = fetch_want(*remaining, *batch, VECTOR_SIZE);
            if want > 0 {
                // PERF: one `fetch` request per func call (per output vector).
                // Fine for bounded drains, but each request has round-trip
                // latency; for very large streams a persistent pull
                // subscription reused across func calls would amortize that.
                let fetched: Vec<Row> = runtime.block_on(async {
                    let mut out = Vec::with_capacity(want);
                    // `fetch` uses no_wait: returns what is available now and
                    // ends, so this drains without blocking indefinitely.
                    if let Ok(mut msgs) = consumer.fetch().max_messages(want).messages().await {
                        while let Some(Ok(msg)) = msgs.next().await {
                            let (seq, ts_micros) = match msg.info() {
                                Ok(info) => (
                                    info.stream_sequence,
                                    (info.published.unix_timestamp_nanos() / 1_000) as i64,
                                ),
                                Err(_) => (0, 0),
                            };
                            // At-least-once: acking before the row reaches
                            // DuckDB means a query cancelled after this point
                            // still counts the message as consumed. A crash
                            // between fetch and ack (or an ignored ack error)
                            // just redelivers on the next run.
                            let _ = msg.ack().await;
                            out.push(
                                Row::new(
                                    msg.message.subject.clone(),
                                    seq,
                                    ts_micros,
                                    msg.message.payload.clone(),
                                )
                                .with_headers(
                                    headers_enabled
                                        .then_some(msg.message.headers.as_ref())
                                        .flatten()
                                        .and_then(serialize_headers),
                                ),
                            );
                        }
                    }
                    out
                });

                *remaining = remaining.saturating_sub(fetched.len() as u64);
                rows = fetched;
            }
            // `fetch`'s no_wait returns only what is available now, so an
            // empty batch ends the drain even if `remaining > 0`: the seed
            // came from a point-in-time `num_pending` estimate, and a bounded
            // drain does not wait for messages that arrive later.
            if *remaining == 0 || rows.is_empty() {
                state.done = true;
            }
        }
    }

    rows
}

/// Parsed configuration for a scan-mode `read_jetstream` call.
struct ReadJetstreamBindData {
    stream: String,
    url: String,
    /// Optional NATS subject filter (token-based wildcards `*` and `>`).
    subject: Option<String>,
    /// Inclusive sequence lower bound, if the user supplied `start_seq`.
    start_seq: Option<u64>,
    /// Inclusive sequence upper bound, if the user supplied `end_seq`.
    end_seq: Option<u64>,
    /// Inclusive time lower bound (microseconds since Unix epoch).
    start_time_micros: Option<i64>,
    /// Inclusive time upper bound (microseconds since Unix epoch).
    end_time_micros: Option<i64>,
    /// JSON field paths to extract as extra columns (dot-notation for nesting).
    json_fields: Vec<String>,
    /// DuckDB type presented for the `payload` column (`blob`/`text`/`json`).
    format: PayloadFormat,
    /// When true, undecodable payloads leave extracted columns NULL instead of
    /// failing the query.
    ignore_errors: bool,
    /// Compiled protobuf message descriptor, if `proto_file`/`proto_message`
    /// were provided.
    proto_descriptor: Option<MessageDescriptor>,
    /// Resolved protobuf field paths + their DuckDB column types.
    proto_fields: Vec<ProtoField>,
    /// True when the read uses an ephemeral JetStream consumer instead of the
    /// stateless Direct Get scan path.
    ephemeral: bool,
    /// Durable consumer name, if the read uses durable mode. Mutually exclusive
    /// with `ephemeral`.
    durable: Option<String>,
    /// Consumer + runtime created during bind (for cardinality), moved out by
    /// init. `Mutex<Option<_>>` because bind data is only borrowed as `&`.
    consumer_setup: Mutex<Option<ConsumerSetup>>,
    /// When true, the `headers` column is declared and populated.
    headers: bool,
}

/// A pull consumer (ephemeral or durable) and the runtime that owns it, created
/// in bind so the consumer's `num_pending` can be reported as query
/// cardinality.
struct ConsumerSetup {
    runtime: Runtime,
    consumer: jetstream::consumer::PullConsumer,
    pending: u64,
    /// Messages requested per `fetch` (the `batch` parameter).
    batch: u64,
}

/// Execution state, shared across DuckDB worker threads via the init data's
/// mutex.
struct ReadJetstreamInitData {
    /// `Option` so [`Drop`] can `take` it for [`Runtime::shutdown_timeout`];
    /// always `Some` outside teardown.
    runtime: Option<Runtime>,
    inner: Mutex<ScanState>,
}

/// Drain the scan connection, then bound the runtime shutdown.
///
/// Dropping the [`Runtime`] otherwise blocks the DuckDB thread until
/// `async-nats`'s background connection task stops on a broker-side idle
/// timeout, delaying process exit (issue #10). The drain closes the connection
/// so that task exits at once. [`Client::drain`] only enqueues the command and
/// returns, so the ceiling comes from [`Runtime::shutdown_timeout`], not the
/// drain: an unresponsive broker cannot delay exit past [`SCAN_DRAIN_TIMEOUT`].
///
/// Only the scan path holds a client; consumers have nothing to drain.
impl Drop for ReadJetstreamInitData {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if let Ok(state) = self.inner.get_mut() {
            if let Source::Scan { client, .. } = &state.source {
                let client = client.clone();
                let _ = runtime.block_on(client.drain());
            }
        }
        runtime.shutdown_timeout(SCAN_DRAIN_TIMEOUT);
    }
}

/// Mutable read progress, guarded by the init data's mutex.
///
/// Despite the name, this backs both the Direct Get scan path and the ephemeral
/// consumer path; [`Source`] holds the source-specific cursor.
struct ScanState {
    stream_name: String,
    /// Optional subject filter. Applied client-side in scan mode; applied
    /// server-side (the consumer's `filter_subject`) in consumer mode, where it
    /// stays `None` here.
    subject: Option<String>,
    /// JSON field paths to extract as extra columns.
    json_fields: Vec<String>,
    /// DuckDB type presented for the `payload` column (`blob`/`text`/`json`).
    format: PayloadFormat,
    /// When true, undecodable payloads yield NULL columns instead of erroring.
    ignore_errors: bool,
    /// Compiled protobuf descriptor + resolved field paths, if using protobuf.
    proto_descriptor: Option<MessageDescriptor>,
    proto_fields: Vec<ProtoField>,
    /// When true, emit a `headers` column with serialized message headers.
    headers: bool,
    source: Source,
    done: bool,
}

impl ScanState {
    /// Construct a `ScanState` from the bind data plus a resolved source and
    /// the initial `done` flag. The shared config fields are copied from the
    /// bind data so the lock on `init.inner` is the only state `func` needs.
    fn from_bind(bind_data: &ReadJetstreamBindData, source: Source, done: bool) -> Self {
        Self {
            stream_name: bind_data.stream.clone(),
            subject: bind_data.subject.clone(),
            json_fields: bind_data.json_fields.clone(),
            format: bind_data.format,
            ignore_errors: bind_data.ignore_errors,
            proto_descriptor: bind_data.proto_descriptor.clone(),
            proto_fields: bind_data.proto_fields.clone(),
            headers: bind_data.headers,
            source,
            done,
        }
    }
}

/// The source of messages for a read.
///
/// This enum is the extension seam for new read modes: unbounded tail will add
/// a variant here (a background producer feeding a bounded channel) without
/// disturbing the scan/consumer drains.
enum Source {
    /// Stateless Direct Get scan over a bounded sequence range.
    Scan {
        /// Retained so teardown can drain it; see [`ReadJetstreamInitData`]'s
        /// [`Drop`].
        client: async_nats::Client,
        /// Boxed: `Stream` is large, and an unboxed variant trips
        /// `clippy::large_enum_variant`.
        stream: Box<jetstream::stream::Stream>,
        /// Next sequence to fetch (inclusive).
        current_seq: u64,
        /// Last sequence to fetch (inclusive).
        end_seq: u64,
    },
    /// Bounded drain of a pull consumer (ephemeral or durable), up to the
    /// messages that existed when the consumer was created.
    Consumer {
        /// Boxed for the same reason as [`Source::Scan`]'s `stream`.
        consumer: Box<jetstream::consumer::PullConsumer>,
        /// Messages left to drain. Seeded from the consumer's `num_pending` at
        /// creation, capped by `max_messages` when set. Reaching zero ends the
        /// read.
        remaining: u64,
        /// Messages requested per `fetch` request (the `batch` parameter),
        /// clamped each call to whatever is still `remaining`.
        batch: u64,
    },
}

/// The `read_jetstream` table function. See the crate README for the full contract.
struct ReadJetstream;

impl VTab for ReadJetstream {
    type InitData = ReadJetstreamInitData;
    type BindData = ReadJetstreamBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let p = config::BindParams::from_bind(bind)?;

        let (proto_descriptor, proto_fields) = if p.using_proto() {
            let file = p.proto_file.as_deref().unwrap();
            let message = p.proto_message.as_deref().unwrap();
            let descriptor = proto::compile_proto(file, message)?;
            let fields = p
                .proto_paths
                .iter()
                .map(|path| proto::field_column(&descriptor, path))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            (Some(descriptor), fields)
        } else {
            (None, Vec::new())
        };

        declare_columns(bind, &p, &proto_fields);

        // Creating a durable consumer persists server-side state, and bind runs
        // even for EXPLAIN, so `EXPLAIN SELECT ... durable => 'x'` creates the
        // durable. This mirrors the ephemeral path and is the pragmatic
        // trade-off for cardinality reporting, since `set_cardinality` is
        // bind-only.
        let consumer_setup = if p.is_consumer() {
            Some(create_consumer_setup(bind, &p)?)
        } else {
            None
        };

        Ok(ReadJetstreamBindData {
            stream: p.stream,
            url: p.url,
            subject: p.subject,
            start_seq: p.start_seq,
            end_seq: p.end_seq,
            start_time_micros: p.start_time_micros,
            end_time_micros: p.end_time_micros,
            json_fields: p.json_fields,
            format: p.format,
            ignore_errors: p.ignore_errors,
            proto_descriptor,
            proto_fields,
            ephemeral: p.ephemeral,
            durable: p.durable,
            consumer_setup: Mutex::new(consumer_setup),
            headers: p.headers,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*info.get_bind_data::<ReadJetstreamBindData>() };

        // Consumer mode (ephemeral or durable): reuse the runtime + consumer
        // created during bind. Subject filtering is server-side for consumers.
        if bind_data.ephemeral || bind_data.durable.is_some() {
            let setup = bind_data
                .consumer_setup
                .lock()
                .unwrap()
                .take()
                .expect("consumer setup missing from bind data");

            let mut state = ScanState::from_bind(
                bind_data,
                Source::Consumer {
                    consumer: Box::new(setup.consumer),
                    remaining: setup.pending,
                    batch: setup.batch,
                },
                setup.pending == 0,
            );
            state.subject = None;

            return Ok(ReadJetstreamInitData {
                runtime: Some(setup.runtime),
                inner: Mutex::new(state),
            });
        }

        // Scan mode: own a fresh runtime and resolve the sequence window.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        let scan = runtime.block_on(stream::open_scan(
            &bind_data.url,
            &bind_data.stream,
            bind_data.start_seq,
            bind_data.end_seq,
            bind_data.start_time_micros,
            bind_data.end_time_micros,
        ))?;

        let done = scan.end_seq == 0 || scan.current_seq > scan.end_seq;
        let state = ScanState::from_bind(
            bind_data,
            Source::Scan {
                client: scan.client,
                stream: Box::new(scan.stream),
                current_seq: scan.current_seq,
                end_seq: scan.end_seq,
            },
            done,
        );

        Ok(ReadJetstreamInitData {
            runtime: Some(runtime),
            inner: Mutex::new(state),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let mut state = init.inner.lock().unwrap();

        if state.done {
            output.set_len(0);
            return Ok(());
        }

        let runtime = init.runtime.as_ref().expect("runtime present outside drop");
        let rows = acquire_rows(&mut state, runtime);

        // PERF: these clones happen on every func call (once per output vector).
        // They exist to release the `state` mutex borrow before writing to the
        // output vectors. The field lists are small, but for hot paths this
        // config could be hoisted into the init data as shared, read-only data
        // (e.g. Arc) and borrowed instead of cloned.
        let stream_name = state.stream_name.clone();
        let json_fields = state.json_fields.clone();
        let format = state.format;
        let ignore_errors = state.ignore_errors;
        let proto_descriptor = state.proto_descriptor.clone();
        let proto_fields = state.proto_fields.clone();
        let headers = state.headers;
        drop(state);

        // JSON and proto extraction are mutually exclusive, so the headers
        // column (when enabled) follows whichever list is present.
        let extra_count = json_fields.len().max(proto_fields.len());

        let mut writer = output::RowWriter {
            stream_name: &stream_name,
            format,
            ignore_errors,
            json_fields: &json_fields,
            proto_descriptor: proto_descriptor.as_ref(),
            proto_fields: &proto_fields,
            base: output::BaseVectorsMut {
                stream: output.flat_vector(0),
                subject: output.flat_vector(1),
                seq: output.flat_vector(2),
                ts: output.flat_vector(3),
                payload: output.flat_vector(4),
            },
            // Extra columns follow the five base columns at index 5+. JSON and
            // proto extraction are mutually exclusive, so both start at 5.
            json_vecs: (0..json_fields.len())
                .map(|i| output.flat_vector(5 + i))
                .collect(),
            proto_vecs: (0..proto_fields.len())
                .map(|i| output.flat_vector(5 + i))
                .collect(),
            headers_vec: headers.then(|| output.flat_vector(5 + extra_count)),
        };

        for (n, row) in rows.iter().enumerate() {
            writer.write_row(n, row)?;
        }

        output.set_len(rows.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);
        let timestamp = || LogicalTypeHandle::from(LogicalTypeId::Timestamp);
        let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);
        let varchar_list = || LogicalTypeHandle::list(&varchar());

        Some(vec![
            ("url".to_string(), varchar()),
            ("subject".to_string(), varchar()),
            ("durable".to_string(), varchar()),
            ("ephemeral".to_string(), boolean()),
            ("start_seq".to_string(), ubigint()),
            ("end_seq".to_string(), ubigint()),
            ("start_time".to_string(), timestamp()),
            ("end_time".to_string(), timestamp()),
            ("start".to_string(), varchar()),
            ("batch".to_string(), ubigint()),
            ("max_messages".to_string(), ubigint()),
            ("json_extract".to_string(), varchar_list()),
            ("format".to_string(), varchar()),
            ("ignore_errors".to_string(), boolean()),
            ("proto_file".to_string(), varchar()),
            ("proto_message".to_string(), varchar()),
            ("proto_extract".to_string(), varchar_list()),
            ("headers".to_string(), boolean()),
        ])
    }
}

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ReadJetstream>("read_jetstream")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{capped_pending, fetch_want, serialize_headers, DEFAULT_BATCH};
    use async_nats::{header, HeaderMap};

    #[test]
    fn max_messages_caps_pending() {
        // No cap leaves the pending count untouched.
        assert_eq!(capped_pending(1000, None), 1000);
        // A cap below pending clamps the drain.
        assert_eq!(capped_pending(1000, Some(10)), 10);
        // A cap at or above pending is a no-op.
        assert_eq!(capped_pending(5, Some(5)), 5);
        assert_eq!(capped_pending(5, Some(100)), 5);
        // A zero cap yields an empty drain.
        assert_eq!(capped_pending(1000, Some(0)), 0);
    }

    #[test]
    fn fetch_want_is_bounded_by_the_smallest_limit() {
        // Batch is the binding constraint (smaller than remaining and vector).
        assert_eq!(fetch_want(1000, DEFAULT_BATCH, 2048), 256);
        // Remaining is the binding constraint (near the end of a drain).
        assert_eq!(fetch_want(10, DEFAULT_BATCH, 2048), 10);
        // The vector size caps an oversized batch.
        assert_eq!(fetch_want(1_000_000, 100_000, 2048), 2048);
        // Nothing left to fetch.
        assert_eq!(fetch_want(0, DEFAULT_BATCH, 2048), 0);
        // A batch of one requests a single message at a time.
        assert_eq!(fetch_want(1000, 1, 2048), 1);
    }

    #[test]
    fn serialize_headers_filters_nats_system_headers() {
        let mut map = HeaderMap::new();
        map.insert("Content-Type", "application/json");
        map.insert(header::NATS_SUBJECT, "orders.us.1");
        map.insert(header::NATS_SEQUENCE, "1");
        let json = serialize_headers(&map).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 1);
        assert!(obj.contains_key("Content-Type"));
        // No Nats-* keys survive filtering.
        assert!(!json.contains("Nats-"));
    }

    #[test]
    fn serialize_headers_none_for_empty_map() {
        assert_eq!(serialize_headers(&HeaderMap::new()), None);
    }

    #[test]
    fn serialize_headers_none_when_only_system_headers() {
        let mut map = HeaderMap::new();
        map.insert(header::NATS_SUBJECT, "orders.us.1");
        map.insert(header::NATS_SEQUENCE, "1");
        map.insert(header::NATS_TIME_STAMP, "2026-01-01T00:00:00Z");
        assert_eq!(serialize_headers(&map), None);
    }

    #[test]
    fn serialize_headers_multi_value_as_array() {
        let mut map = HeaderMap::new();
        map.insert("X-Trace-Id", "abc");
        map.append("X-Trace-Id", "def");
        let json = serialize_headers(&map).unwrap();
        assert_eq!(json, r#"{"X-Trace-Id":["abc","def"]}"#);
    }
}

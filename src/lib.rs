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

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{duckdb_entrypoint_c_api, Connection, Result};

use async_nats::jetstream;
use prost_reflect::MessageDescriptor;
use tokio::runtime::Runtime;

mod config;
mod error;
mod output;
mod proto;
mod row;
mod stream;

use config::{parse_timestamp_micros, subject_matches, StartSpec};
use error::ScanError;
use output::{
    json_extract_string, looks_like_json, non_json_hint, non_proto_hint, write_proto_value,
};
use proto::{ProtoField, ProtoValue};
use row::Row;

/// DuckDB's standard vector size; `func` emits at most this many rows per call.
const VECTOR_SIZE: usize = 2048;

/// Default NATS server URL when the `url` parameter is not provided.
const DEFAULT_URL: &str = "nats://localhost:4222";

/// Default consumer fetch batch size (messages per `fetch` request) when the
/// `batch` parameter is not provided. Applies to ephemeral and durable modes.
const DEFAULT_BATCH: u64 = 256;

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
}

/// A pull consumer (ephemeral or durable) and the runtime that owns it, created
/// in bind so the consumer's `num_pending` can be reported as query
/// cardinality.
struct ConsumerSetup {
    runtime: Runtime,
    consumer: jetstream::consumer::PullConsumer,
    pending: u64,
    /// Whether to ack each message on emit (durable + `ack => true`).
    ack: bool,
    /// Messages requested per `fetch` (the `batch` parameter).
    batch: u64,
}

/// Execution state, shared across DuckDB worker threads via the init data's
/// mutex.
struct ReadJetstreamInitData {
    runtime: Runtime,
    inner: Mutex<ScanState>,
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
    /// When true, undecodable payloads yield NULL columns instead of erroring.
    ignore_errors: bool,
    /// Compiled protobuf descriptor + resolved field paths, if using protobuf.
    proto_descriptor: Option<MessageDescriptor>,
    proto_fields: Vec<ProtoField>,
    source: Source,
    done: bool,
}

/// The source of messages for a read.
///
/// This enum is the extension seam for new read modes: unbounded tail will add
/// a variant here (a background producer feeding a bounded channel) without
/// disturbing the scan/consumer drains.
enum Source {
    /// Stateless Direct Get scan over a bounded sequence range.
    Scan {
        stream: jetstream::stream::Stream,
        /// Next sequence to fetch (inclusive).
        current_seq: u64,
        /// Last sequence to fetch (inclusive).
        end_seq: u64,
    },
    /// Bounded drain of a pull consumer (ephemeral or durable), up to the
    /// messages that existed when the consumer was created.
    Consumer {
        consumer: jetstream::consumer::PullConsumer,
        /// Messages left to drain. Seeded from the consumer's `num_pending` at
        /// creation, capped by `max_messages` when set. Reaching zero ends the
        /// read.
        remaining: u64,
        /// Messages requested per `fetch` request (the `batch` parameter),
        /// clamped each call to whatever is still `remaining`.
        batch: u64,
        /// Ack each message on emit (durable + `ack => true`), advancing the
        /// stored cursor. At-least-once: a query cancelled mid-drain leaves
        /// un-acked messages for redelivery on the next run.
        ack: bool,
    },
}

/// The `read_jetstream` table function. See the crate README for the full contract.
struct ReadJetstream;

impl VTab for ReadJetstream {
    type InitData = ReadJetstreamInitData;
    type BindData = ReadJetstreamBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);
        let timestamp = || LogicalTypeHandle::from(LogicalTypeId::Timestamp);

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

        // Validate the decode-parameter combination up front.
        if !json_fields.is_empty() && !proto_paths.is_empty() {
            return Err(Box::new(ScanError::DecodeConflict));
        }
        let using_proto =
            !proto_paths.is_empty() || proto_file.is_some() || proto_message.is_some();
        if using_proto {
            if proto_file.is_none() || proto_message.is_none() {
                return Err(Box::new(ScanError::ProtoIncomplete));
            }
            if proto_paths.is_empty() {
                return Err(Box::new(ScanError::ProtoNoFields));
            }
        }

        let (proto_descriptor, proto_fields) = if using_proto {
            let file = proto_file.as_deref().unwrap();
            let message = proto_message.as_deref().unwrap();
            let descriptor = proto::compile_proto(file, message)?;
            let fields = proto_paths
                .iter()
                .map(|p| proto::field_column(&descriptor, p))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            (Some(descriptor), fields)
        } else {
            (None, Vec::new())
        };

        bind.add_result_column("stream", varchar());
        bind.add_result_column("subject", varchar());
        bind.add_result_column("seq", ubigint());
        bind.add_result_column("ts_nats", timestamp());

        // `payload` is BLOB by default, but becomes VARCHAR when extracting JSON
        // (the payload is then known-valid UTF-8 text, avoiding BLOB validation).
        // Protobuf payloads stay BLOB (binary wire format).
        let payload_type = if json_fields.is_empty() {
            LogicalTypeHandle::from(LogicalTypeId::Blob)
        } else {
            varchar()
        };
        bind.add_result_column("payload", payload_type);

        // Extra columns are named by the verbatim field path, dots preserved
        // (e.g. `order.id`), so callers quote them in SQL.
        for field in &json_fields {
            bind.add_result_column(field, varchar());
        }
        for field in &proto_fields {
            bind.add_result_column(&field.path, LogicalTypeHandle::from(field.column_type));
        }

        let stream = bind.get_parameter(0).to_string();

        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| DEFAULT_URL.to_string());

        let subject = bind
            .get_named_parameter("subject")
            .map(|v| {
                let s = v.to_string();
                // Validate structural NATS subject rules up front (wildcards are
                // allowed); fail fast rather than silently matching nothing.
                if async_nats::Subject::from(s.clone()).is_valid() {
                    Ok(s)
                } else {
                    Err(ScanError::InvalidSubject { value: s })
                }
            })
            .transpose()?;
        let start_seq = bind.get_named_parameter("start_seq").map(|v| v.to_uint64());
        let end_seq = bind.get_named_parameter("end_seq").map(|v| v.to_uint64());
        // TIMESTAMP values cannot be read via the primitive integer getters, so
        // parse DuckDB's canonical string rendering into epoch microseconds.
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
        let ack = bind
            .get_named_parameter("ack")
            .map(|v| v.to_bool())
            .unwrap_or(false);

        // `batch` (fetch request size) and `max_messages` (hard row cap) only
        // apply to consumer modes; the scan path fetches one message per
        // Direct Get and reads the full requested range.
        let batch = bind.get_named_parameter("batch").map(|v| v.to_uint64());
        let max_messages = bind
            .get_named_parameter("max_messages")
            .map(|v| v.to_uint64());

        let is_consumer = ephemeral || durable.is_some();

        if ephemeral && durable.is_some() {
            return Err(Box::new(ScanError::ModeConflict));
        }
        if ack && durable.is_none() {
            return Err(Box::new(ScanError::AckRequiresDurable));
        }
        if batch.is_some() && !is_consumer {
            return Err(Box::new(ScanError::ConsumerOnlyParam { param: "batch" }));
        }
        if max_messages.is_some() && !is_consumer {
            return Err(Box::new(ScanError::ConsumerOnlyParam {
                param: "max_messages",
            }));
        }
        if batch == Some(0) {
            return Err(Box::new(ScanError::ZeroBatch));
        }
        let batch = batch.unwrap_or(DEFAULT_BATCH);

        // The `start` policy selects a consumer's starting point at creation.
        // It only applies to consumer modes; `by_start_*` reuse start_seq/time.
        let start = bind
            .get_named_parameter("start")
            .map(|v| StartSpec::parse(&v.to_string()))
            .transpose()?;

        // For consumer mode, create the consumer now (in bind) so we can report
        // its `num_pending` as the query cardinality, driving DuckDB's progress
        // bar. The consumer handle and the runtime that owns it are carried
        // forward to init/func.
        //
        // Note: creating a durable consumer persists server-side state, and bind
        // runs even for EXPLAIN, so `EXPLAIN SELECT ... durable => 'x'` will
        // create the durable. This mirrors the ephemeral path (which likewise
        // creates its consumer in bind) and is the pragmatic trade-off for
        // cardinality reporting, since `set_cardinality` is bind-only.
        let consumer_setup = if ephemeral || durable.is_some() {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?;

            // Default to All: drain everything currently in the stream.
            let deliver_policy = start
                .unwrap_or(StartSpec::All)
                .into_policy(start_seq, start_time_micros)?;

            let (consumer, pending) = runtime.block_on(stream::create_consumer(
                &url,
                &stream,
                subject.as_deref(),
                durable.as_deref(),
                deliver_policy,
                ack,
            ))?;
            // num_pending is a point-in-time snapshot taken when the consumer was
            // created, not a guarantee, so report it as an estimate (is_exact =
            // false). This still drives the progress bar while avoiding an
            // overconfident cardinality that could mislead join planning.
            //
            // `max_messages` caps the drain (and the reported cardinality): the
            // read stops after at most that many rows even if more are pending.
            let pending = capped_pending(pending, max_messages);
            bind.set_cardinality(pending, false);
            Some(ConsumerSetup {
                runtime,
                consumer,
                pending,
                ack,
                batch,
            })
        } else {
            None
        };

        Ok(ReadJetstreamBindData {
            stream,
            url,
            subject,
            start_seq,
            end_seq,
            start_time_micros,
            end_time_micros,
            json_fields,
            ignore_errors,
            proto_descriptor,
            proto_fields,
            ephemeral,
            durable,
            consumer_setup: Mutex::new(consumer_setup),
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*info.get_bind_data::<ReadJetstreamBindData>() };

        // Consumer mode (ephemeral or durable): reuse the runtime + consumer
        // created during bind.
        if bind_data.ephemeral || bind_data.durable.is_some() {
            let setup = bind_data
                .consumer_setup
                .lock()
                .unwrap()
                .take()
                .expect("consumer setup missing from bind data");

            let state = ScanState {
                stream_name: bind_data.stream.clone(),
                // Subject filtering is server-side for consumers, so no
                // client-side filter is applied during the drain.
                subject: None,
                json_fields: bind_data.json_fields.clone(),
                ignore_errors: bind_data.ignore_errors,
                proto_descriptor: bind_data.proto_descriptor.clone(),
                proto_fields: bind_data.proto_fields.clone(),
                done: setup.pending == 0,
                source: Source::Consumer {
                    consumer: setup.consumer,
                    remaining: setup.pending,
                    batch: setup.batch,
                    ack: setup.ack,
                },
            };

            return Ok(ReadJetstreamInitData {
                runtime: setup.runtime,
                inner: Mutex::new(state),
            });
        }

        // Scan mode: own a fresh runtime and resolve the sequence window.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        // Resolve the effective sequence window up front so a bad URL or
        // unknown stream fails the query at init rather than mid-scan.
        let state =
            runtime.block_on(async {
                let client = async_nats::connect(&bind_data.url)
                    .await
                    .map_err(|source| ScanError::Connect {
                        url: bind_data.url.clone(),
                        source,
                    })?;
                let context = jetstream::new(client);

                let stream = context.get_stream(&bind_data.stream).await.map_err(|e| {
                    ScanError::StreamInfo {
                        stream: bind_data.stream.clone(),
                        source: Box::new(e),
                    }
                })?;
                let info = stream.cached_info();
                let first_sequence = info.state.first_sequence;
                let last_sequence = info.state.last_sequence;

                // Start from the stream's first sequence, then tighten by the
                // explicit seq bound and/or the resolved time bound (whichever is
                // more restrictive).
                let mut current_seq = bind_data
                    .start_seq
                    .unwrap_or(first_sequence)
                    .max(first_sequence);
                let mut end_seq = bind_data
                    .end_seq
                    .unwrap_or(last_sequence)
                    .min(last_sequence);

                if let Some(start_micros) = bind_data.start_time_micros {
                    let resolved = stream::resolve_time_to_seq(
                        &stream,
                        start_micros,
                        first_sequence,
                        last_sequence,
                        true,
                    )
                    .await;
                    if let Some(seq) = resolved {
                        current_seq = current_seq.max(seq);
                    } else {
                        // No message at or after start_time: empty result.
                        end_seq = 0;
                    }
                }

                if let Some(end_micros) = bind_data.end_time_micros {
                    let resolved = stream::resolve_time_to_seq(
                        &stream,
                        end_micros,
                        first_sequence,
                        last_sequence,
                        false,
                    )
                    .await;
                    if let Some(seq) = resolved {
                        end_seq = end_seq.min(seq);
                    } else {
                        end_seq = 0;
                    }
                }

                Ok::<_, ScanError>(ScanState {
                    stream_name: bind_data.stream.clone(),
                    subject: bind_data.subject.clone(),
                    json_fields: bind_data.json_fields.clone(),
                    ignore_errors: bind_data.ignore_errors,
                    proto_descriptor: bind_data.proto_descriptor.clone(),
                    proto_fields: bind_data.proto_fields.clone(),
                    done: last_sequence == 0 || end_seq == 0 || current_seq > end_seq,
                    source: Source::Scan {
                        stream,
                        current_seq,
                        end_seq,
                    },
                })
            })?;

        Ok(ReadJetstreamInitData {
            runtime,
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

        // Reduce each source's messages to a common [`Row`] so the
        // column-writing below is source-agnostic. A [`Row`] holds
        // reference-counted `Bytes` for the subject and payload, so buffering
        // clones no message bytes — only refcount bumps. The buffer also
        // decouples acquisition from column writing, which the tail path (an
        // async channel drain) will need.
        let subject_filter = state.subject.clone();
        let mut rows: Vec<Row> = Vec::with_capacity(VECTOR_SIZE);

        match &mut state.source {
            Source::Scan {
                stream,
                current_seq,
                end_seq,
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
                    let fetched = init.runtime.block_on(async {
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

                    rows.push(Row::new(
                        msg.subject,
                        msg.sequence,
                        (msg.time.unix_timestamp_nanos() / 1_000) as i64,
                        msg.payload,
                    ));
                }
                if *current_seq > *end_seq {
                    state.done = true;
                }
            }
            Source::Consumer {
                consumer,
                remaining,
                batch,
                ack,
            } => {
                use futures_util::StreamExt;

                let ack = *ack;
                let want = fetch_want(*remaining, *batch, VECTOR_SIZE);
                if want > 0 {
                    // PERF: one `fetch` request per func call (per output vector).
                    // Fine for bounded drains, but each request has round-trip
                    // latency; for very large streams a persistent pull
                    // subscription reused across func calls would amortize that.
                    let fetched: Vec<Row> = init.runtime.block_on(async {
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
                                if ack {
                                    let _ = msg.ack().await;
                                }
                                out.push(Row::new(
                                    msg.message.subject.clone(),
                                    seq,
                                    ts_micros,
                                    msg.message.payload.clone(),
                                ));
                            }
                        }
                        out
                    });

                    *remaining = remaining.saturating_sub(fetched.len() as u64);
                    rows = fetched;
                }
                if *remaining == 0 || rows.is_empty() {
                    state.done = true;
                }
            }
        }

        // PERF: these clones happen on every func call (once per output vector).
        // They exist to release the `state` mutex borrow before writing to the
        // output vectors. The field lists are small, but for hot paths this
        // config could be hoisted into the init data as shared, read-only data
        // (e.g. Arc) and borrowed instead of cloned.
        let stream_name = state.stream_name.clone();
        let json_fields = state.json_fields.clone();
        let ignore_errors = state.ignore_errors;
        let proto_descriptor = state.proto_descriptor.clone();
        let proto_fields = state.proto_fields.clone();

        let stream_vec = output.flat_vector(0);
        let subject_vec = output.flat_vector(1);
        let mut seq_vec = output.flat_vector(2);
        let mut ts_vec = output.flat_vector(3);
        let payload_vec = output.flat_vector(4);

        // Extra columns follow the five base columns at index 5+. JSON and
        // proto extraction are mutually exclusive, so both loops start at 5.
        let mut json_vecs: Vec<_> = (0..json_fields.len())
            .map(|i| output.flat_vector(5 + i))
            .collect();
        let mut proto_vecs: Vec<_> = (0..proto_fields.len())
            .map(|i| output.flat_vector(5 + i))
            .collect();

        for (n, row) in rows.iter().enumerate() {
            stream_vec.insert(n, stream_name.as_str());
            subject_vec.insert(n, row.subject.as_str());
            // Safety: n < VECTOR_SIZE and the vectors are sized for
            // STANDARD_VECTOR_SIZE; rows are written sequentially from 0.
            unsafe {
                seq_vec.as_mut_slice::<u64>()[n] = row.seq;
                ts_vec.as_mut_slice::<i64>()[n] = row.ts_micros;
            }
            payload_vec.insert(n, row.payload.as_ref());

            if !json_fields.is_empty() {
                let doc: Option<serde_json::Value> = serde_json::from_slice(&row.payload).ok();
                if doc.is_none() && !ignore_errors {
                    return Err(Box::new(ScanError::NonJsonPayload {
                        stream: stream_name.clone(),
                        seq: row.seq,
                        hint: non_json_hint(&row.payload),
                    }));
                }
                for (i, path) in json_fields.iter().enumerate() {
                    match doc.as_ref().and_then(|d| json_extract_string(d, path)) {
                        Some(s) => json_vecs[i].insert(n, s.as_str()),
                        None => json_vecs[i].set_null(n),
                    }
                }
            }

            if let Some(descriptor) = &proto_descriptor {
                // Protobuf decode is permissive: JSON text often decodes to junk
                // rather than failing, so also treat a JSON lead byte as an error.
                let decoded = proto::decode_message(descriptor, &row.payload);
                if !ignore_errors && (decoded.is_none() || looks_like_json(&row.payload)) {
                    return Err(Box::new(ScanError::NonProtoPayload {
                        stream: stream_name.clone(),
                        seq: row.seq,
                        hint: non_proto_hint(&row.payload),
                    }));
                }
                for (i, field) in proto_fields.iter().enumerate() {
                    let value = decoded
                        .as_ref()
                        .map(|d| proto::extract_value(d, &field.path))
                        .unwrap_or(ProtoValue::Null);
                    write_proto_value(&mut proto_vecs[i], n, value);
                }
            }
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
            ("ack".to_string(), boolean()),
            ("batch".to_string(), ubigint()),
            ("max_messages".to_string(), ubigint()),
            ("json_extract".to_string(), varchar_list()),
            ("ignore_errors".to_string(), boolean()),
            ("proto_file".to_string(), varchar()),
            ("proto_message".to_string(), varchar()),
            ("proto_extract".to_string(), varchar_list()),
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
    use super::{capped_pending, fetch_want, DEFAULT_BATCH};

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
}

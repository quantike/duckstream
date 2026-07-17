//! duckstream: a DuckDB extension for querying NATS JetStream.
//!
//! Step 2 implements **scan mode** of the `read_nats` table function: a
//! stateless, bounded read of a JetStream stream by sequence range using the
//! JetStream Direct Get API. Consumer modes (ephemeral/durable) and
//! `read_nats_tail` are added in later steps.
//!
//! Scan mode does not need the full async→sync bridge (background task +
//! bounded channel) that unbounded tail requires. It owns a Tokio runtime in
//! its init data and drives a simple sequential `direct_get` loop with
//! `Runtime::block_on` from the synchronous `func` callback.

use std::error::Error;
use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, FlatVector, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{duckdb_entrypoint_c_api, Connection, Result};

use async_nats::jetstream;
use prost_reflect::MessageDescriptor;
use tokio::runtime::Runtime;

mod proto;
use proto::{ProtoField, ProtoValue};

/// DuckDB's standard vector size; `func` emits at most this many rows per call.
const VECTOR_SIZE: usize = 2048;

/// Default NATS server URL when the `url` parameter is not provided.
const DEFAULT_URL: &str = "nats://localhost:4222";

/// Errors surfaced from the NATS/JetStream layer.
///
/// These are converted to `Box<dyn Error>` at the [`VTab`] boundary, which
/// DuckDB renders as the query error message.
#[derive(Debug, thiserror::Error)]
enum ScanError {
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
    #[error("proto_extract requires both proto_file and proto_message")]
    ProtoIncomplete,
    #[error("proto_file/proto_message require a non-empty proto_extract list")]
    ProtoNoFields,
}

/// Parsed configuration for a scan-mode `read_nats` call.
///
/// Consumer-mode and decode parameters are declared in
/// [`ReadNats::named_parameters`] but not yet interpreted.
struct ReadNatsBindData {
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
    /// Compiled protobuf message descriptor, if `proto_file`/`proto_message`
    /// were provided.
    proto_descriptor: Option<MessageDescriptor>,
    /// Resolved protobuf field paths + their DuckDB column types.
    proto_fields: Vec<ProtoField>,
    /// True when the read uses an ephemeral JetStream consumer instead of the
    /// stateless Direct Get scan path.
    ephemeral: bool,
    /// Consumer + runtime created during bind (for cardinality), moved out by
    /// init. `Mutex<Option<_>>` because bind data is only borrowed as `&`.
    consumer_setup: Mutex<Option<ConsumerSetup>>,
}

/// The ephemeral consumer and the runtime that owns it, created in bind so the
/// consumer's `num_pending` can be reported as query cardinality.
struct ConsumerSetup {
    runtime: Runtime,
    consumer: jetstream::consumer::PullConsumer,
    pending: u64,
}

/// Execution state, shared across DuckDB worker threads via the init data's
/// mutex.
struct ReadNatsInitData {
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
    /// Compiled protobuf descriptor + resolved field paths, if using protobuf.
    proto_descriptor: Option<MessageDescriptor>,
    proto_fields: Vec<ProtoField>,
    source: Source,
    done: bool,
}

/// The source of messages for a read.
enum Source {
    /// Stateless Direct Get scan over a bounded sequence range.
    Scan {
        stream: jetstream::stream::Stream,
        /// Next sequence to fetch (inclusive).
        current_seq: u64,
        /// Last sequence to fetch (inclusive).
        end_seq: u64,
    },
    /// Bounded drain of an ephemeral pull consumer, up to the messages that
    /// existed when the consumer was created.
    Consumer {
        consumer: jetstream::consumer::PullConsumer,
        /// Messages left to drain (from the consumer's `num_pending` at
        /// creation). Reaching zero ends the read.
        remaining: u64,
    },
}

/// The `read_nats` table function. See the crate README for the full contract.
struct ReadNats;

impl VTab for ReadNats {
    type InitData = ReadNatsInitData;
    type BindData = ReadNatsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);
        let timestamp = || LogicalTypeHandle::from(LogicalTypeId::Timestamp);

        let json_fields: Vec<String> = bind
            .get_named_parameter("json_extract")
            .and_then(|v| v.to_list())
            .map(|items| items.iter().map(|v| v.to_string()).collect())
            .unwrap_or_default();

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

        // Compile the proto schema (if any) and resolve each extract path to a
        // typed column at bind time.
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

        // Extracted JSON fields become extra VARCHAR columns named by the
        // verbatim field path (dots preserved; nested values render as JSON).
        for field in &json_fields {
            bind.add_result_column(field, varchar());
        }

        // Extracted protobuf fields become extra columns with the schema-derived
        // DuckDB type, named by the verbatim field path.
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

        // For consumer mode, create the ephemeral consumer now (in bind) so we
        // can report its `num_pending` as the query cardinality. This drives
        // DuckDB's built-in query progress bar. The consumer handle and the
        // runtime that owns it are carried forward to init/func.
        let consumer_setup = if ephemeral {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?;
            let (consumer, pending) =
                runtime.block_on(create_ephemeral_consumer(&url, &stream, subject.as_deref()))?;
            // num_pending is a point-in-time snapshot taken when the consumer was
            // created, not a guarantee, so report it as an estimate (is_exact =
            // false). This still drives the progress bar while avoiding an
            // overconfident cardinality that could mislead join planning.
            bind.set_cardinality(pending, false);
            Some(ConsumerSetup {
                runtime,
                consumer,
                pending,
            })
        } else {
            None
        };

        Ok(ReadNatsBindData {
            stream,
            url,
            subject,
            start_seq,
            end_seq,
            start_time_micros,
            end_time_micros,
            json_fields,
            proto_descriptor,
            proto_fields,
            ephemeral,
            consumer_setup: Mutex::new(consumer_setup),
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*info.get_bind_data::<ReadNatsBindData>() };

        // Consumer mode: reuse the runtime + consumer created during bind.
        if bind_data.ephemeral {
            let setup = bind_data
                .consumer_setup
                .lock()
                .unwrap()
                .take()
                .expect("ephemeral consumer setup missing from bind data");

            let state = ScanState {
                stream_name: bind_data.stream.clone(),
                // Subject filtering is server-side for consumers, so no
                // client-side filter is applied during the drain.
                subject: None,
                json_fields: bind_data.json_fields.clone(),
                proto_descriptor: bind_data.proto_descriptor.clone(),
                proto_fields: bind_data.proto_fields.clone(),
                done: setup.pending == 0,
                source: Source::Consumer {
                    consumer: setup.consumer,
                    remaining: setup.pending,
                },
            };

            return Ok(ReadNatsInitData {
                runtime: setup.runtime,
                inner: Mutex::new(state),
            });
        }

        // Scan mode: own a fresh runtime and resolve the sequence window.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        // Connect and resolve the effective sequence window up front (fail fast
        // on a bad URL or unknown stream, mirroring the C++ extension).
        let state = runtime.block_on(async {
            let client = async_nats::connect(&bind_data.url)
                .await
                .map_err(|source| ScanError::Connect {
                    url: bind_data.url.clone(),
                    source,
                })?;
            let context = jetstream::new(client);

            let stream =
                context
                    .get_stream(&bind_data.stream)
                    .await
                    .map_err(|e| ScanError::StreamInfo {
                        stream: bind_data.stream.clone(),
                        source: Box::new(e),
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
                let resolved =
                    resolve_time_to_seq(&stream, start_micros, first_sequence, last_sequence, true)
                        .await;
                if let Some(seq) = resolved {
                    current_seq = current_seq.max(seq);
                } else {
                    // No message at or after start_time: empty result.
                    end_seq = 0;
                }
            }

            if let Some(end_micros) = bind_data.end_time_micros {
                let resolved =
                    resolve_time_to_seq(&stream, end_micros, first_sequence, last_sequence, false)
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

        Ok(ReadNatsInitData {
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

        // Collect up to one vector's worth of rows from the active source. Each
        // row is reduced to (subject, seq, ts_micros, payload) so the column
        // writing below is identical regardless of source.
        //
        // PERF: this materializes an owned copy of every message (String subject
        // + Vec<u8> payload) into an intermediate Vec before writing to the
        // output vectors. It decouples message acquisition from column writing
        // but doubles the per-message allocation/copy. A faster design would
        // write each message straight into the output vectors as it is fetched,
        // avoiding the intermediate Vec and the payload clone entirely.
        let subject_filter = state.subject.clone();
        let mut rows: Vec<(String, u64, i64, Vec<u8>)> = Vec::with_capacity(VECTOR_SIZE);

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

                    rows.push((
                        msg.subject.to_string(),
                        msg.sequence,
                        (msg.time.unix_timestamp_nanos() / 1_000) as i64,
                        msg.payload.to_vec(),
                    ));
                }
                if *current_seq > *end_seq {
                    state.done = true;
                }
            }
            Source::Consumer {
                consumer,
                remaining,
            } => {
                use futures_util::StreamExt;

                let want = (*remaining).min(VECTOR_SIZE as u64) as usize;
                if want > 0 {
                    // PERF: one `fetch` request per func call (per output vector).
                    // Fine for bounded drains, but each request has round-trip
                    // latency; for very large streams a persistent pull
                    // subscription reused across func calls would amortize that.
                    let fetched: Vec<_> = init.runtime.block_on(async {
                        let mut out = Vec::with_capacity(want);
                        // `fetch` uses no_wait: returns what is available now and
                        // ends, so this drains without blocking indefinitely.
                        if let Ok(mut batch) = consumer.fetch().max_messages(want).messages().await
                        {
                            while let Some(Ok(msg)) = batch.next().await {
                                let (seq, ts_micros) = match msg.info() {
                                    Ok(info) => (
                                        info.stream_sequence,
                                        (info.published.unix_timestamp_nanos() / 1_000) as i64,
                                    ),
                                    Err(_) => (0, 0),
                                };
                                out.push((
                                    msg.subject.to_string(),
                                    seq,
                                    ts_micros,
                                    msg.payload.to_vec(),
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
        let proto_descriptor = state.proto_descriptor.clone();
        let proto_fields = state.proto_fields.clone();

        let stream_vec = output.flat_vector(0);
        let subject_vec = output.flat_vector(1);
        let mut seq_vec = output.flat_vector(2);
        let mut ts_vec = output.flat_vector(3);
        let payload_vec = output.flat_vector(4);

        // Extra columns for extracted JSON fields, in declared order after the
        // five base columns.
        let mut json_vecs: Vec<_> = (0..json_fields.len())
            .map(|i| output.flat_vector(5 + i))
            .collect();

        // Extra columns for extracted protobuf fields (mutually exclusive with
        // JSON, so they also begin at index 5).
        let mut proto_vecs: Vec<_> = (0..proto_fields.len())
            .map(|i| output.flat_vector(5 + i))
            .collect();

        for (n, (subject, seq, ts_micros, payload)) in rows.iter().enumerate() {
            stream_vec.insert(n, stream_name.as_str());
            subject_vec.insert(n, subject.as_str());
            // Safety: n < VECTOR_SIZE and the vectors are sized for
            // STANDARD_VECTOR_SIZE; rows are written sequentially from 0.
            unsafe {
                seq_vec.as_mut_slice::<u64>()[n] = *seq;
                ts_vec.as_mut_slice::<i64>()[n] = *ts_micros;
            }
            payload_vec.insert(n, payload.as_slice());

            if !json_fields.is_empty() {
                // Parse once per message; a payload that is not valid JSON leaves
                // every extracted column NULL for this row (the row is still
                // emitted with its base columns).
                let doc: Option<serde_json::Value> = serde_json::from_slice(payload).ok();
                for (i, path) in json_fields.iter().enumerate() {
                    match doc.as_ref().and_then(|d| json_extract_string(d, path)) {
                        Some(s) => json_vecs[i].insert(n, s.as_str()),
                        None => json_vecs[i].set_null(n),
                    }
                }
            }

            if let Some(descriptor) = &proto_descriptor {
                // Decode once per message; an undecodable payload leaves every
                // extracted column NULL for this row (row still emitted).
                let decoded = proto::decode_message(descriptor, payload);
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
            ("deliver".to_string(), varchar()),
            ("ack".to_string(), boolean()),
            ("batch".to_string(), ubigint()),
            ("max_messages".to_string(), ubigint()),
            ("json_extract".to_string(), varchar_list()),
            ("proto_file".to_string(), varchar()),
            ("proto_message".to_string(), varchar()),
            ("proto_extract".to_string(), varchar_list()),
        ])
    }
}

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ReadNats>("read_nats")?;
    Ok(())
}

/// Connect and create an ephemeral pull consumer that will deliver every
/// message currently in the stream (`DeliverPolicy::All`), optionally filtered
/// by subject server-side. Returns the consumer and its `num_pending` count at
/// creation, which bounds the drain and drives the query progress bar.
async fn create_ephemeral_consumer(
    url: &str,
    stream_name: &str,
    subject: Option<&str>,
) -> Result<(jetstream::consumer::PullConsumer, u64), ScanError> {
    use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};

    let client = async_nats::connect(url)
        .await
        .map_err(|source| ScanError::Connect {
            url: url.to_string(),
            source,
        })?;
    let context = jetstream::new(client);

    let stream = context
        .get_stream(stream_name)
        .await
        .map_err(|e| ScanError::StreamInfo {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;

    let config = pull::Config {
        durable_name: None, // ephemeral
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::None,
        filter_subject: subject.unwrap_or("").to_string(),
        // Reap the server-side consumer shortly after we stop draining.
        inactive_threshold: std::time::Duration::from_secs(30),
        ..Default::default()
    };

    let consumer = stream
        .create_consumer(config)
        .await
        .map_err(|e| ScanError::Consumer {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;

    let pending = consumer.cached_info().num_pending;
    Ok((consumer, pending))
}

/// Fetch a single message's server timestamp, in microseconds since the Unix
/// epoch, trying Direct Get first and falling back to the leader-only raw API.
async fn message_time_micros(stream: &jetstream::stream::Stream, seq: u64) -> Option<i64> {
    let msg = match stream.direct_get(seq).await {
        Ok(msg) => msg,
        Err(_) => stream.get_raw_message(seq).await.ok()?,
    };
    Some((msg.time.unix_timestamp_nanos() / 1_000) as i64)
}

/// Resolve a timestamp to a stream sequence by binary search over the sequence
/// space, since JetStream offers no direct timestamp lookup.
///
/// With `lower_bound = true`, returns the first sequence whose message time is
/// `>= target_micros` (for `start_time`). With `lower_bound = false`, returns
/// the last sequence whose message time is `<= target_micros` (for `end_time`).
/// Returns `None` when no sequence satisfies the bound.
///
/// Timestamps in a stream are monotonically non-decreasing with sequence, which
/// makes the search well-defined. Deleted sequences in the middle are handled
/// by probing outward to the nearest existing message.
///
/// PERF: cache probed timestamps. Each probe is a network round-trip
/// (`get_raw_message`/`direct_get`), and the search re-probes overlapping
/// sequences across iterations; memoizing seq -> timestamp would cut the
/// round-trips, and resolving both `start_time` and `end_time` could share one
/// cache.
async fn resolve_time_to_seq(
    stream: &jetstream::stream::Stream,
    target_micros: i64,
    first_seq: u64,
    last_seq: u64,
    lower_bound: bool,
) -> Option<u64> {
    if last_seq < first_seq {
        return None;
    }

    let mut lo = first_seq;
    let mut hi = last_seq;
    let mut result: Option<u64> = None;

    while lo <= hi {
        let mid = lo + (hi - lo) / 2;

        // Probe outward from `mid` to find the nearest existing message, since
        // `mid` itself may be a deleted/purged sequence.
        let probe = probe_time_at_or_after(stream, mid, hi).await;
        let Some((seq, micros)) = probe else {
            // No live message from mid..=hi; shrink the upper half.
            if mid == first_seq {
                break;
            }
            hi = mid - 1;
            continue;
        };

        if lower_bound {
            if micros >= target_micros {
                result = Some(seq);
                if seq == first_seq {
                    break;
                }
                hi = seq - 1;
            } else {
                lo = seq + 1;
            }
        } else if micros <= target_micros {
            result = Some(seq);
            lo = seq + 1;
        } else {
            if seq == first_seq {
                break;
            }
            hi = seq - 1;
        }
    }

    result
}

/// Find the first existing message at or after `from`, up to `to`, returning
/// its sequence and timestamp (microseconds).
async fn probe_time_at_or_after(
    stream: &jetstream::stream::Stream,
    from: u64,
    to: u64,
) -> Option<(u64, i64)> {
    let mut seq = from;
    while seq <= to {
        if let Some(micros) = message_time_micros(stream, seq).await {
            return Some((seq, micros));
        }
        seq += 1;
    }
    None
}

/// Write an extracted protobuf value into a typed DuckDB flat vector at `row`.
///
/// The column type was chosen at bind time to match the field's protobuf kind,
/// so each [`ProtoValue`] variant lines up with the vector's storage. A
/// [`ProtoValue::Null`] (missing field or undecodable payload) sets SQL NULL.
fn write_proto_value(vec: &mut FlatVector, row: usize, value: ProtoValue) {
    // Safety: `row` < VECTOR_SIZE and vectors are sized for STANDARD_VECTOR_SIZE;
    // rows are written sequentially from 0.
    match value {
        ProtoValue::Null => vec.set_null(row),
        ProtoValue::Bool(b) => unsafe { vec.as_mut_slice::<bool>()[row] = b },
        ProtoValue::I32(v) => unsafe { vec.as_mut_slice::<i32>()[row] = v },
        ProtoValue::I64(v) => unsafe { vec.as_mut_slice::<i64>()[row] = v },
        ProtoValue::U32(v) => unsafe { vec.as_mut_slice::<u32>()[row] = v },
        ProtoValue::U64(v) => unsafe { vec.as_mut_slice::<u64>()[row] = v },
        ProtoValue::F32(v) => unsafe { vec.as_mut_slice::<f32>()[row] = v },
        ProtoValue::F64(v) => unsafe { vec.as_mut_slice::<f64>()[row] = v },
        ProtoValue::Text(s) => vec.insert(row, s.as_str()),
        ProtoValue::Bytes(b) => vec.insert(row, b.as_slice()),
    }
}

/// Extract a value from a JSON document by dot-separated path and render it as
/// a string suitable for a VARCHAR column.
///
/// Path segments navigate nested objects (`order.id` descends `order` then
/// `id`). Scalars render naturally (`42`, `2.5`, `true`, unquoted strings);
/// nested objects and arrays render as compact JSON text so they can be
/// re-parsed or `CAST` downstream. Returns `None` when the path is absent or
/// resolves to JSON `null`, which the caller maps to SQL NULL.
fn json_extract_string(doc: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = doc;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }

    match current {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        // Numbers and booleans render via their natural JSON form (no trailing
        // zeros for integers), everything else (objects/arrays) as JSON text.
        other => Some(other.to_string()),
    }
}

/// Match a NATS subject against a filter using NATS token semantics.
///
/// Tokens are separated by `.`. The wildcard `*` matches exactly one token;
/// `>` matches one or more trailing tokens and is only valid as the final
/// token. This is true NATS matching, not substring matching.
fn subject_matches(filter: &str, subject: &str) -> bool {
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
fn parse_timestamp_micros(s: &str) -> Result<i64, ScanError> {
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
    use super::{json_extract_string, parse_timestamp_micros, subject_matches};

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
    fn json_scalar_extraction() {
        let doc = serde_json::json!({"status": "new", "count": 42, "ratio": 2.5, "ok": true});
        assert_eq!(json_extract_string(&doc, "status").as_deref(), Some("new"));
        // Integers render without trailing zeros (unlike the old C++ behavior).
        assert_eq!(json_extract_string(&doc, "count").as_deref(), Some("42"));
        assert_eq!(json_extract_string(&doc, "ratio").as_deref(), Some("2.5"));
        assert_eq!(json_extract_string(&doc, "ok").as_deref(), Some("true"));
    }

    #[test]
    fn json_nested_descent() {
        let doc = serde_json::json!({"order": {"id": 7, "customer": {"name": "Ada"}}});
        assert_eq!(json_extract_string(&doc, "order.id").as_deref(), Some("7"));
        assert_eq!(
            json_extract_string(&doc, "order.customer.name").as_deref(),
            Some("Ada")
        );
    }

    #[test]
    fn json_nested_object_as_text() {
        let doc = serde_json::json!({"order": {"id": 7}});
        // A path resolving to an object renders as compact JSON text.
        assert_eq!(
            json_extract_string(&doc, "order").as_deref(),
            Some(r#"{"id":7}"#)
        );
    }

    #[test]
    fn json_missing_and_null_are_none() {
        let doc = serde_json::json!({"a": 1, "n": null});
        assert_eq!(json_extract_string(&doc, "missing"), None);
        assert_eq!(json_extract_string(&doc, "a.deeper"), None);
        assert_eq!(json_extract_string(&doc, "n"), None);
    }
}

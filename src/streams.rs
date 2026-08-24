//! The `jetstream_streams` table function: JetStream stream catalog, one row
//! per stream.
//!
//! Two selection modes share the row shape: an optional named `stream`
//! selector maps to `Context::get_stream` (exact lookup, one row), and
//! omitting it maps to `Context::streams()` (paged `STREAM.LIST` enumeration).
//! Both cannot be one positional-or-nothing parameter because the C API has no
//! optional positional arguments, so the selector is named, in the style of
//! `duckdb_tables(database => 'memory')`.
//!
//! The entire fetch happens in `init`; `func` only writes buffered rows, so
//! the init data holds no runtime or connection.

use std::error::Error;
use std::sync::Mutex;

use async_nats::jetstream;
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result};
use futures_util::StreamExt;

use crate::error::ScanError;

/// The `jetstream_streams` table function.
struct JetstreamStreams;

/// Stream metadata in its SQL-facing form, fully converted from
/// [`jetstream::stream::Info`] at fetch time so `func` does pure vector
/// writing with no network types in scope.
struct StreamInfo {
    stream: String,
    created: i64,
    messages: u64,
    bytes: u64,
    first_seq: u64,
    first_ts: i64,
    last_seq: u64,
    last_ts: i64,
    consumer_count: u64,
    subjects_count: u64,
    deleted_count: Option<u64>,
    retention: &'static str,
    storage: &'static str,
    discard: &'static str,
    max_messages: i64,
    max_bytes: i64,
    max_message_size: i64,
    num_replicas: u32,
    sealed: bool,
    allow_direct: bool,
    description: Option<String>,
    subjects: Vec<String>,
}

impl StreamInfo {
    fn from_jetstream(info: &jetstream::stream::Info) -> Self {
        let config = &info.config;
        let state = &info.state;
        Self {
            stream: config.name.clone(),
            created: to_micros(info.created),
            messages: state.messages,
            bytes: state.bytes,
            first_seq: state.first_sequence,
            first_ts: to_micros(state.first_timestamp),
            last_seq: state.last_sequence,
            last_ts: to_micros(state.last_timestamp),
            consumer_count: state.consumer_count as u64,
            subjects_count: state.subjects_count,
            deleted_count: state.deleted_count,
            retention: retention_name(config.retention),
            storage: storage_name(config.storage),
            discard: discard_name(config.discard),
            max_messages: config.max_messages,
            max_bytes: config.max_bytes,
            max_message_size: i64::from(config.max_message_size),
            num_replicas: config.num_replicas as u32,
            sealed: config.sealed,
            allow_direct: config.allow_direct,
            description: config.description.clone(),
            subjects: config.subjects.clone(),
        }
    }
}

/// Convert a server timestamp to microseconds since the Unix epoch.
fn to_micros(odt: time::OffsetDateTime) -> i64 {
    (odt.unix_timestamp_nanos() / 1_000) as i64
}

fn retention_name(policy: jetstream::stream::RetentionPolicy) -> &'static str {
    match policy {
        jetstream::stream::RetentionPolicy::Limits => "limits",
        jetstream::stream::RetentionPolicy::Interest => "interest",
        jetstream::stream::RetentionPolicy::WorkQueue => "workqueue",
    }
}

fn storage_name(storage: jetstream::stream::StorageType) -> &'static str {
    match storage {
        jetstream::stream::StorageType::File => "file",
        jetstream::stream::StorageType::Memory => "memory",
    }
}

fn discard_name(policy: jetstream::stream::DiscardPolicy) -> &'static str {
    match policy {
        jetstream::stream::DiscardPolicy::Old => "old",
        jetstream::stream::DiscardPolicy::New => "new",
    }
}

/// Connect to NATS and return the client plus a JetStream context on it. The
/// caller drains the client before the owning runtime shuts down.
async fn connect(url: &str) -> Result<(async_nats::Client, jetstream::Context), ScanError> {
    let client = async_nats::connect(url)
        .await
        .map_err(|source| ScanError::Connect {
            url: url.to_string(),
            source,
        })?;
    let context = jetstream::new(client.clone());
    Ok((client, context))
}

/// Fetch one stream's info by exact name.
async fn fetch_stream(
    context: &jetstream::Context,
    stream_name: &str,
) -> Result<StreamInfo, ScanError> {
    let stream = context
        .get_stream(stream_name)
        .await
        .map_err(|e| ScanError::StreamInfo {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;
    Ok(StreamInfo::from_jetstream(stream.cached_info()))
}

/// Fetch every stream's info via the paged `STREAM.LIST` enumeration.
async fn fetch_all_streams(context: &jetstream::Context) -> Result<Vec<StreamInfo>, ScanError> {
    let mut rows = Vec::new();
    let mut streams = context.streams();
    while let Some(item) = streams.next().await {
        let info = item.map_err(|e| ScanError::StreamList {
            source: Box::new(e),
        })?;
        rows.push(StreamInfo::from_jetstream(&info));
    }
    Ok(rows)
}

struct JetstreamStreamsBindData {
    /// Exact stream selector; `None` enumerates all streams.
    stream: Option<String>,
    url: String,
}

/// Buffered rows plus the emit cursor. `func` drains at most
/// [`crate::VECTOR_SIZE`] rows per call.
struct JetstreamStreamsInitData {
    rows: Mutex<Vec<StreamInfo>>,
    cursor: Mutex<usize>,
}

/// Register the table function with the connection.
pub(crate) fn register(con: &Connection) -> Result<()> {
    con.register_table_function::<JetstreamStreams>("jetstream_streams")
}

impl VTab for JetstreamStreams {
    type InitData = JetstreamStreamsInitData;
    type BindData = JetstreamStreamsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let stream = bind.get_named_parameter("stream").map(|v| v.to_string());
        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| crate::DEFAULT_URL.to_string());

        declare_columns(bind);
        if stream.is_some() {
            // A point-in-time snapshot of exactly one stream; enumeration
            // size is unknown at bind time.
            bind.set_cardinality(1, true);
        }

        Ok(JetstreamStreamsBindData { stream, url })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*info.get_bind_data::<JetstreamStreamsBindData>() };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        let fetched = runtime.block_on(async {
            let (client, context) = connect(&bind_data.url).await?;
            let result = match &bind_data.stream {
                Some(name) => fetch_stream(&context, name).await.map(|row| vec![row]),
                None => fetch_all_streams(&context).await,
            };
            let _ = client.drain().await;
            result
        });
        runtime.shutdown_timeout(crate::SCAN_DRAIN_TIMEOUT);

        Ok(JetstreamStreamsInitData {
            rows: Mutex::new(fetched?),
            cursor: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init = func.get_init_data();
        let rows = init.rows.lock().unwrap();
        let mut cursor = init.cursor.lock().unwrap();

        let remaining = rows.len().saturating_sub(*cursor);
        let want = remaining.min(crate::VECTOR_SIZE);
        if want == 0 {
            output.set_len(0);
            return Ok(());
        }

        // Accumulates the list-column child offset across rows; see
        // [`write_row`].
        let mut list_offset = 0usize;
        for n in 0..want {
            write_row(output, n, &rows[*cursor + n], &mut list_offset);
        }
        *cursor += want;
        output.set_len(want);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        Some(vec![
            ("stream".to_string(), varchar()),
            ("url".to_string(), varchar()),
        ])
    }
}

/// Declare the result columns. The vector indices in [`write_row`] follow this
/// order, so the two lists must change together.
fn declare_columns(bind: &BindInfo) {
    let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
    let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);
    let bigint = || LogicalTypeHandle::from(LogicalTypeId::Bigint);
    let uinteger = || LogicalTypeHandle::from(LogicalTypeId::UInteger);
    let timestamp = || LogicalTypeHandle::from(LogicalTypeId::Timestamp);
    let boolean = || LogicalTypeHandle::from(LogicalTypeId::Boolean);

    bind.add_result_column("stream", varchar());
    bind.add_result_column("created", timestamp());
    bind.add_result_column("messages", ubigint());
    bind.add_result_column("bytes", ubigint());
    bind.add_result_column("first_seq", ubigint());
    bind.add_result_column("first_ts", timestamp());
    bind.add_result_column("last_seq", ubigint());
    bind.add_result_column("last_ts", timestamp());
    bind.add_result_column("consumer_count", ubigint());
    bind.add_result_column("subjects_count", ubigint());
    bind.add_result_column("deleted_count", ubigint());
    bind.add_result_column("retention", varchar());
    bind.add_result_column("storage", varchar());
    bind.add_result_column("discard", varchar());
    bind.add_result_column("max_messages", bigint());
    bind.add_result_column("max_bytes", bigint());
    bind.add_result_column("max_message_size", bigint());
    bind.add_result_column("num_replicas", uinteger());
    bind.add_result_column("sealed", boolean());
    bind.add_result_column("allow_direct", boolean());
    bind.add_result_column("description", varchar());
    bind.add_result_column("subjects", LogicalTypeHandle::list(&varchar()));
}

/// Write one metadata row into the output chunk's vectors at index `row`.
/// Indices mirror [`declare_columns`]'s column order.
///
/// `list_offset` accumulates the list column's child offset across rows in
/// this vector: list entries append into one shared child vector, so each
/// row's entry must start where the previous row's ended.
fn write_row(output: &mut DataChunkHandle, row: usize, info: &StreamInfo, list_offset: &mut usize) {
    output.flat_vector(0).insert(row, info.stream.as_str());
    write_i64(output, 1, row, info.created);
    write_u64(output, 2, row, info.messages);
    write_u64(output, 3, row, info.bytes);
    write_u64(output, 4, row, info.first_seq);
    write_i64(output, 5, row, info.first_ts);
    write_u64(output, 6, row, info.last_seq);
    write_i64(output, 7, row, info.last_ts);
    write_u64(output, 8, row, info.consumer_count);
    write_u64(output, 9, row, info.subjects_count);
    match info.deleted_count {
        Some(n) => write_u64(output, 10, row, n),
        None => output.flat_vector(10).set_null(row),
    }
    output.flat_vector(11).insert(row, info.retention);
    output.flat_vector(12).insert(row, info.storage);
    output.flat_vector(13).insert(row, info.discard);
    write_i64(output, 14, row, info.max_messages);
    write_i64(output, 15, row, info.max_bytes);
    write_i64(output, 16, row, info.max_message_size);
    write_u32(output, 17, row, info.num_replicas);
    write_bool(output, 18, row, info.sealed);
    write_bool(output, 19, row, info.allow_direct);
    match &info.description {
        Some(d) => output.flat_vector(20).insert(row, d.as_str()),
        None => output.flat_vector(20).set_null(row),
    }
    write_str_list(output, 21, row, &info.subjects, list_offset);
}

// Safety: `row` < VECTOR_SIZE and vectors are sized for STANDARD_VECTOR_SIZE;
// rows are written sequentially from 0.

fn write_i64(output: &DataChunkHandle, idx: usize, row: usize, value: i64) {
    let mut vec = output.flat_vector(idx);
    unsafe { vec.as_mut_slice::<i64>()[row] = value };
}

fn write_u64(output: &DataChunkHandle, idx: usize, row: usize, value: u64) {
    let mut vec = output.flat_vector(idx);
    unsafe { vec.as_mut_slice::<u64>()[row] = value };
}

fn write_u32(output: &DataChunkHandle, idx: usize, row: usize, value: u32) {
    let mut vec = output.flat_vector(idx);
    unsafe { vec.as_mut_slice::<u32>()[row] = value };
}

fn write_bool(output: &DataChunkHandle, idx: usize, row: usize, value: bool) {
    let mut vec = output.flat_vector(idx);
    unsafe { vec.as_mut_slice::<bool>()[row] = value };
}

fn write_str_list(
    output: &DataChunkHandle,
    idx: usize,
    row: usize,
    items: &[String],
    offset: &mut usize,
) {
    let mut list = output.list_vector(idx);
    let child = list.child(*offset + items.len());
    for (i, item) in items.iter().enumerate() {
        child.insert(*offset + i, item.as_str());
    }
    list.set_entry(row, *offset, items.len());
    list.set_len(*offset + items.len());
    *offset += items.len();
}

#[cfg(test)]
mod tests {
    use super::{discard_name, retention_name, storage_name, to_micros, StreamInfo};
    use async_nats::jetstream::stream::{DiscardPolicy, Info, RetentionPolicy, StorageType};
    use time::OffsetDateTime;

    /// Deserialize a server-shaped JSON object into an async-nats `Info`;
    /// `Info` has a private field, so this is the only construction path.
    fn info_from_json(json: serde_json::Value) -> Info {
        serde_json::from_value(json).expect("deserialize stream Info")
    }

    #[test]
    fn maps_server_info_to_row() {
        let info = info_from_json(serde_json::json!({
            "config": {
                "name": "ORDERS",
                "subjects": ["orders.>"],
                "retention": "workqueue",
                "storage": "memory",
                "discard": "new",
                "max_msgs": 1000,
                "max_bytes": 4096,
                "max_msg_size": 1024,
                "num_replicas": 3,
                "sealed": true,
                "allow_direct": true,
                "description": "order events"
            },
            "created": "2026-01-01T00:00:00Z",
            "state": {
                "messages": 42,
                "bytes": 10240,
                "first_seq": 5,
                "first_ts": "2026-01-01T00:00:01Z",
                "last_seq": 46,
                "last_ts": "2026-01-01T00:00:09Z",
                "consumer_count": 2,
                "num_subjects": 7,
                "num_deleted": 3
            }
        }));
        let row = StreamInfo::from_jetstream(&info);

        assert_eq!(row.stream, "ORDERS");
        assert_eq!(row.created, 1_767_225_600_000_000);
        assert_eq!(row.messages, 42);
        assert_eq!(row.bytes, 10240);
        assert_eq!(row.first_seq, 5);
        assert_eq!(row.first_ts, 1_767_225_601_000_000);
        assert_eq!(row.last_seq, 46);
        assert_eq!(row.last_ts, 1_767_225_609_000_000);
        assert_eq!(row.consumer_count, 2);
        assert_eq!(row.subjects_count, 7);
        assert_eq!(row.deleted_count, Some(3));
        assert_eq!(row.retention, "workqueue");
        assert_eq!(row.storage, "memory");
        assert_eq!(row.discard, "new");
        assert_eq!(row.max_messages, 1000);
        assert_eq!(row.max_bytes, 4096);
        assert_eq!(row.max_message_size, 1024);
        assert_eq!(row.num_replicas, 3);
        assert!(row.sealed);
        assert!(row.allow_direct);
        assert_eq!(row.description.as_deref(), Some("order events"));
        assert_eq!(row.subjects, vec!["orders.>".to_string()]);
    }

    #[test]
    fn absent_optionals_map_to_none() {
        let info = info_from_json(serde_json::json!({
            "config": {
                "name": "ORDERS",
                "retention": "limits",
                "storage": "file",
                "discard": "old",
                "num_replicas": 1,
                // The server sends -1 for an unset limit.
                "max_msgs": -1,
                "max_bytes": -1,
                "max_msg_size": -1
            },
            "created": "2026-01-01T00:00:00Z",
            "state": {
                "messages": 0,
                "bytes": 0,
                "first_seq": 1,
                "first_ts": "2026-01-01T00:00:00Z",
                "last_seq": 0,
                "last_ts": "2026-01-01T00:00:00Z",
                "consumer_count": 0
            }
        }));
        let row = StreamInfo::from_jetstream(&info);

        assert_eq!(row.deleted_count, None);
        assert_eq!(row.description, None);
        assert!(row.subjects.is_empty());
        assert_eq!(row.max_messages, -1);
        assert_eq!(row.max_bytes, -1);
        assert_eq!(row.max_message_size, -1);
    }

    #[test]
    fn enum_names_match_nats_terms() {
        assert_eq!(retention_name(RetentionPolicy::Limits), "limits");
        assert_eq!(retention_name(RetentionPolicy::Interest), "interest");
        assert_eq!(retention_name(RetentionPolicy::WorkQueue), "workqueue");
        assert_eq!(storage_name(StorageType::File), "file");
        assert_eq!(storage_name(StorageType::Memory), "memory");
        assert_eq!(discard_name(DiscardPolicy::Old), "old");
        assert_eq!(discard_name(DiscardPolicy::New), "new");
    }

    #[test]
    fn timestamps_keep_microsecond_precision() {
        let odt = OffsetDateTime::parse(
            "2026-01-01T00:00:00.123456Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        assert_eq!(to_micros(odt), 1_767_225_600_123_456);
    }
}

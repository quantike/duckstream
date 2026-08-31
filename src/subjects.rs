//! The `jetstream_subjects` table function: per-subject message counts for one
//! stream, one row per distinct subject.
//!
//! Counts come from the paged `STREAM.INFO` subjects API
//! (`Stream::info_builder().subjects(filter).fetch()`), not the ordinary
//! stream info (`State::subjects` is `pub(crate)` in async-nats, which is why
//! [`crate::streams`] cannot surface them). The server returns exact subject
//! literals, never wildcard patterns.
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

/// The `jetstream_subjects` table function.
struct JetstreamSubjects;

/// One subject count in its SQL-facing form, converted at fetch time so `func`
/// does pure vector writing with no network types in scope.
struct SubjectCount {
    subject: String,
    messages: u64,
}

/// Fetch every subject count for `stream`, filtered server-side by `filter`
/// (token wildcards `*` and `>` allowed; `None` counts all subjects).
///
/// The server omits subjects from `STREAM.INFO` state unless the filter is
/// non-empty, so `None` is sent as `>` (matches every subject).
///
/// The drain trusts [`jetstream::stream::InfoWithSubjects`]'s own termination
/// (its internal `pages_done` flag) rather than counting rows, since the
/// server pages by offset until its reported `total` is reached.
async fn fetch_subjects(
    context: &jetstream::Context,
    stream_name: &str,
    filter: Option<&str>,
) -> Result<Vec<SubjectCount>, ScanError> {
    // The builder's `fetch` issues its own STREAM.INFO, so a no-info handle
    // avoids a duplicate existence-check round trip.
    let stream =
        context
            .get_stream_no_info(stream_name)
            .await
            .map_err(|e| ScanError::StreamInfo {
                stream: stream_name.to_string(),
                source: Box::new(e),
            })?;

    let mut pages = stream
        .info_builder()
        .subjects(filter.unwrap_or(">"))
        .fetch()
        .await
        .map_err(|e| ScanError::SubjectsList {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;
    let mut rows = Vec::new();
    while let Some(item) = pages.next().await {
        let (subject, count) = item.map_err(|e| ScanError::SubjectsList {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;
        rows.push(SubjectCount {
            subject,
            messages: count as u64,
        });
    }
    Ok(rows)
}

/// Normalize and validate the `subject` filter parameter.
///
/// An absent or empty filter means "all subjects"; anything else must be a
/// structurally valid subject. Token wildcards `*` and `>` are allowed, with
/// `>` only as the final token, per NATS wildcard rules.
fn parse_subject_filter(raw: Option<String>) -> Result<Option<String>, ScanError> {
    let filter = raw.filter(|s| !s.is_empty());
    if let Some(f) = filter.as_deref() {
        if !async_nats::Subject::from(f).is_valid() {
            return Err(ScanError::InvalidSubject {
                value: f.to_string(),
            });
        }
        if f.split('.').rev().skip(1).any(|token| token == ">") {
            return Err(ScanError::InvalidSubject {
                value: f.to_string(),
            });
        }
    }
    Ok(filter)
}

struct JetstreamSubjectsBindData {
    stream: String,
    /// Server-side subject filter (token wildcards `*` and `>` allowed).
    subject: Option<String>,
    url: String,
}

/// Buffered rows plus the emit cursor. `func` drains at most
/// [`crate::VECTOR_SIZE`] rows per call.
struct JetstreamSubjectsInitData {
    stream: String,
    rows: Mutex<Vec<SubjectCount>>,
    cursor: Mutex<usize>,
}

/// Register the table function with the connection.
pub(crate) fn register(con: &Connection) -> Result<()> {
    con.register_table_function::<JetstreamSubjects>("jetstream_subjects")
}

impl VTab for JetstreamSubjects {
    type InitData = JetstreamSubjectsInitData;
    type BindData = JetstreamSubjectsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let stream = bind.get_parameter(0).to_string();
        let subject =
            parse_subject_filter(bind.get_named_parameter("subject").map(|v| v.to_string()))?;
        let url = bind
            .get_named_parameter("url")
            .map(|v| v.to_string())
            .unwrap_or_else(|| crate::DEFAULT_URL.to_string());

        declare_columns(bind);

        Ok(JetstreamSubjectsBindData {
            stream,
            subject,
            url,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*info.get_bind_data::<JetstreamSubjectsBindData>() };

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;

        let fetched = runtime.block_on(async {
            let (client, context) = crate::streams::connect(&bind_data.url).await?;
            let result =
                fetch_subjects(&context, &bind_data.stream, bind_data.subject.as_deref()).await;
            let _ = client.drain().await;
            result
        });
        runtime.shutdown_timeout(crate::SCAN_DRAIN_TIMEOUT);

        Ok(JetstreamSubjectsInitData {
            stream: bind_data.stream.clone(),
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

        for n in 0..want {
            write_row(output, n, &init.stream, &rows[*cursor + n]);
        }
        *cursor += want;
        output.set_len(want);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
        Some(vec![
            ("subject".to_string(), varchar()),
            ("url".to_string(), varchar()),
        ])
    }
}

/// Declare the result columns. The vector indices in [`write_row`] follow this
/// order, so the two lists must change together.
fn declare_columns(bind: &BindInfo) {
    let varchar = || LogicalTypeHandle::from(LogicalTypeId::Varchar);
    let ubigint = || LogicalTypeHandle::from(LogicalTypeId::UBigint);

    bind.add_result_column("stream", varchar());
    bind.add_result_column("subject", varchar());
    bind.add_result_column("messages", ubigint());
}

// Safety: `row` < VECTOR_SIZE and vectors are sized for STANDARD_VECTOR_SIZE;
// rows are written sequentially from 0.

/// Write one row into the output chunk's vectors at index `row`. Indices
/// mirror [`declare_columns`]'s column order.
fn write_row(output: &mut DataChunkHandle, row: usize, stream: &str, entry: &SubjectCount) {
    output.flat_vector(0).insert(row, stream);
    output.flat_vector(1).insert(row, entry.subject.as_str());
    let mut vec = output.flat_vector(2);
    unsafe { vec.as_mut_slice::<u64>()[row] = entry.messages };
}

#[cfg(test)]
mod tests {
    use super::parse_subject_filter;
    use crate::error::ScanError;

    #[test]
    fn absent_filter_passes_through() {
        assert_eq!(parse_subject_filter(None).unwrap(), None);
    }

    #[test]
    fn empty_filter_means_all_subjects() {
        assert_eq!(parse_subject_filter(Some(String::new())).unwrap(), None);
    }

    #[test]
    fn wildcard_filters_are_valid() {
        assert_eq!(
            parse_subject_filter(Some("orders.us.>".to_string())).unwrap(),
            Some("orders.us.>".to_string())
        );
        assert_eq!(
            parse_subject_filter(Some("orders.*".to_string())).unwrap(),
            Some("orders.*".to_string())
        );
    }

    #[test]
    fn fwc_must_be_final_token() {
        assert_eq!(
            parse_subject_filter(Some(">".to_string())).unwrap(),
            Some(">".to_string())
        );
        for bad in ["orders.>.us", ">.orders", "orders.>.*"] {
            assert!(
                matches!(
                    parse_subject_filter(Some(bad.to_string())).unwrap_err(),
                    ScanError::InvalidSubject { .. }
                ),
                "'{bad}' should be invalid"
            );
        }
    }

    #[test]
    fn invalid_filter_is_rejected() {
        for bad in [
            "orders..us",
            ".orders",
            "orders.",
            "orders us",
            "orders\tus",
        ] {
            assert!(
                matches!(
                    parse_subject_filter(Some(bad.to_string())).unwrap_err(),
                    ScanError::InvalidSubject { .. }
                ),
                "'{bad}' should be invalid"
            );
        }
    }
}

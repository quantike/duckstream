//! End-to-end integration tests for the `read_jetstream` table function.
//!
//! These run real SQL against the built `.duckdb_extension` and a live NATS
//! JetStream broker; see [`harness`] for how the broker, seeding, and the
//! DuckDB subprocess are wired. They are kept out of the fast `cargo test
//! --lib` gate because they need a built extension and a broker.
//!
//! To run: `make debug` (or set `DUCKSTREAM_EXT`), have a `duckdb` binary on
//! `PATH`, and either Docker or `NATS_URL`. Missing any of these skips rather
//! than fails.

mod harness;

use harness::Case;

/// Scan a sequence range with the Direct Get path and snapshot the rows.
#[test]
fn scan_range() {
    Case::new("scan_range")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1,"total":10.5}"#)
        .publish("orders.us.2", br#"{"id":2,"total":20.0}"#)
        .publish("orders.eu.3", br#"{"id":3,"total":30.0}"#)
        .run();
}

/// Server-side subject filter plus JSON extraction into typed columns.
#[test]
fn subject_filter_json() {
    Case::new("subject_filter_json")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1,"total":10.5}"#)
        .publish("orders.us.2", br#"{"id":2,"total":20.0}"#)
        .publish("orders.eu.3", br#"{"id":3,"total":30.0}"#)
        .run();
}

/// Ephemeral consumer drains the whole stream and reports a count.
#[test]
fn ephemeral_count() {
    Case::new("ephemeral_count")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1,"total":10.5}"#)
        .publish("orders.us.2", br#"{"id":2,"total":20.0}"#)
        .publish("orders.eu.3", br#"{"id":3,"total":30.0}"#)
        .run();
}

/// `format => 'json'` lets the json operators navigate the payload in SQL.
#[test]
fn format_json() {
    Case::new("format_json")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1,"total":10.5}"#)
        .publish("orders.us.2", br#"{"id":2,"total":20.0}"#)
        .run();
}

/// `headers => true` surfaces message headers as a JSON column. Messages
/// without headers yield NULL. The JSON alias lets `->`/`->>` navigate values.
#[test]
fn message_headers() {
    Case::new("message_headers")
        .stream(&["orders.>"])
        .publish_with_headers(
            "orders.us.1",
            br#"{"id":1}"#,
            vec![
                (
                    "Content-Type".to_string(),
                    vec!["application/json".to_string()],
                ),
                ("X-Trace-Id".to_string(), vec!["abc".to_string()]),
            ],
        )
        .publish("orders.us.2", br#"{"id":2}"#)
        .run();
}

/// Durable consumer resume: the first run drains all seeded messages and acks
/// them; new messages are then published; the second run sees only those.
#[test]
fn durable_resume() {
    Case::new("durable_resume")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1,"total":10.5}"#)
        .publish("orders.us.2", br#"{"id":2,"total":20.0}"#)
        .query("durable_resume.sql", "durable_resume_first")
        .publish("orders.us.3", br#"{"id":3,"total":30.0}"#)
        .publish("orders.eu.4", br#"{"id":4,"total":40.0}"#)
        .query("durable_resume.sql", "durable_resume_second")
        .run_script();
}

/// Protobuf field extraction with the schema supplied as a pre-compiled
/// `FileDescriptorSet` (`buf build -o` artifact form) instead of `.proto`
/// source. The payloads derive from the committed `proto_descriptors.proto`.
#[test]
fn proto_descriptors() {
    use harness::ProtoFieldValue as V;

    Case::new("proto_descriptors")
        .stream(&["orders.>"])
        .publish_proto(
            "orders.us.1",
            "shop.Order",
            &[
                ("id", V::U64(1)),
                ("total", V::F64(10.5)),
                ("status", V::Text("new")),
            ],
        )
        .publish_proto(
            "orders.us.2",
            "shop.Order",
            &[
                ("id", V::U64(2)),
                ("total", V::F64(20.0)),
                ("status", V::Text("shipped")),
            ],
        )
        .run();
}

/// `jetstream_subjects` reports per-subject message counts for one stream,
/// one row per distinct subject, and applies the server-side `subject` filter.
#[test]
fn subjects() {
    Case::new("subjects")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1}"#)
        .publish("orders.us.2", br#"{"id":2}"#)
        .publish("orders.eu.3", br#"{"id":3}"#)
        .run();
}

/// `jetstream_subjects` must emit past one `func` chunk: more distinct
/// subjects than the extension's `VECTOR_SIZE` (2048), with `count(*)`
/// proving the chunk boundary loses or duplicates no rows. Server-side
/// paging stays untested here: the server's page limit is 100,000 subjects
/// (`JSMaxSubjectDetails`), too slow to seed, and the drain loop is a plain
/// `while let` over the library's stream.
#[test]
fn subjects_multi_chunk() {
    let mut case = Case::new("subjects_multi_chunk").stream(&["orders.>"]);
    for i in 0..2049 {
        case = case.publish(&format!("orders.{i}"), br#"{"id":1}"#);
    }
    case.run();
}

/// `jetstream_streams` reports every stream's configuration and state, one row
/// per stream, and supports exact single-stream selection. Timestamps vary per
/// run and the shared broker holds other cases' streams, so the enumeration
/// query selects only stable columns and filters to this case's stream.
#[test]
fn streams() {
    Case::new("streams")
        .stream(&["orders.>"])
        .publish("orders.us.1", br#"{"id":1}"#)
        .publish("orders.us.2", br#"{"id":2}"#)
        .run();
}

//! The intermediate row representation buffered between message acquisition and
//! DuckDB column writing.
//!
//! `func` acquires messages from a [`Source`](crate::Source) (a Direct Get
//! scan, a consumer drain, and — for tail — a background channel), buffers them
//! into a `Vec<Row>`, then writes the buffer into DuckDB's output vectors. The
//! buffer decouples acquisition from column writing, which the tail path needs
//! anyway (it drains an async channel).
//!
//! [`Row`] deliberately holds `bytes::Bytes`/[`Subject`] rather than
//! `String`/`Vec<u8>`. `async-nats` hands out `subject: Subject` (a `Bytes`
//! wrapper) and `payload: Bytes`, both of which are reference-counted: cloning
//! them into a [`Row`] is an atomic refcount bump, not a heap allocation and
//! copy. This removes the two per-message allocations the previous
//! `(String, u64, i64, Vec<u8>)` tuple incurred while buffering.

use async_nats::Subject;
use bytes::Bytes;

/// A single message reduced to the common shape all sources share, buffered
/// before being written into DuckDB's output vectors.
///
/// Both [`Row::subject`] and [`Row::payload`] are backed by reference-counted
/// [`Bytes`], so constructing a `Row` from an `async-nats` message clones no
/// payload bytes — it only bumps refcounts. `subject` derefs to `&str` and
/// `payload` derefs to `&[u8]`, matching DuckDB's `insert` signatures directly.
#[derive(Debug, Clone)]
pub struct Row {
    /// Message subject. A [`Subject`] is an immutable, `Bytes`-backed UTF-8
    /// string; cloning it is O(1).
    pub subject: Subject,
    /// JetStream stream sequence for this message.
    pub seq: u64,
    /// Server publish time, microseconds since the Unix epoch.
    pub ts_micros: i64,
    /// Raw message payload. [`Bytes`] is reference-counted; cloning is O(1)
    /// and shares the underlying buffer.
    pub payload: Bytes,
    /// Message headers serialized as a JSON string, or `None` when the message
    /// had no headers (or only NATS system headers on the scan path).
    pub headers: Option<String>,
}

impl Row {
    /// Construct a row, taking ownership of the (cheaply cloned) subject and
    /// payload handles from a message.
    pub fn new(subject: Subject, seq: u64, ts_micros: i64, payload: Bytes) -> Self {
        Self {
            subject,
            seq,
            ts_micros,
            payload,
            headers: None,
        }
    }

    /// Attach serialized headers, or leave `None` for empty/system-only headers.
    pub fn with_headers(mut self, headers: Option<String>) -> Self {
        self.headers = headers;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_clone_shares_payload_buffer() {
        let payload = Bytes::from_static(b"hello world");
        let row = Row::new(Subject::from("orders.new"), 7, 123, payload.clone());
        let cloned = row.clone();

        // Cloning shares the same underlying allocation (no deep copy).
        assert_eq!(row.payload.as_ptr(), cloned.payload.as_ptr());
        assert_eq!(cloned.subject.as_str(), "orders.new");
        assert_eq!(cloned.seq, 7);
        assert_eq!(cloned.ts_micros, 123);
    }

    #[test]
    fn subject_and_payload_deref_for_duckdb_insert() {
        let row = Row::new(
            Subject::from("a.b.c"),
            1,
            0,
            Bytes::from_static(b"\x00\x01"),
        );
        // Deref targets match FlatVector::insert's &str / &[u8] arguments.
        let _s: &str = &row.subject;
        let _b: &[u8] = &row.payload;
        assert_eq!(&row.payload[..], &[0u8, 1u8]);
    }

    #[test]
    fn with_headers_attaches_or_clears() {
        let row = Row::new(Subject::from("a.b"), 1, 0, Bytes::from_static(b"x"));
        assert_eq!(row.headers, None);

        let with = row.with_headers(Some(r#"{"X-Trace":"abc"}"#.to_string()));
        assert_eq!(with.headers.as_deref(), Some(r#"{"X-Trace":"abc"}"#));

        let without = with.with_headers(None);
        assert_eq!(without.headers, None);
    }
}

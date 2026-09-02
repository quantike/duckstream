//! Writing buffered [`Row`](crate::row::Row)s into DuckDB output vectors,
//! including the JSON and protobuf field-extraction that populates the extra
//! columns.
//!
//! [`RowWriter`] bundles the per-`func`-call config and output vectors so
//! `func`'s body stays a thin loop. The per-value helpers ([`json_extract_string`],
//! [`write_proto_value`], [`write_utf8_payload`]) remain public for reuse.

use duckdb::core::{FlatVector, Inserter};
use prost_reflect::{DynamicMessage, MessageDescriptor};

use crate::config::PayloadFormat;
use crate::error::ScanError;
use crate::proto::{self, ProtoField, ProtoValue};
use crate::row::Row;

/// Write an extracted protobuf value into the typed flat vector chosen at
/// bind time to match the field's kind.
pub fn write_proto_value(vec: &mut FlatVector, row: usize, value: ProtoValue) {
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

/// Bundles the per-`func`-call config and output vectors so `func` stays a
/// thin loop.
///
/// Extra-column vectors follow the five base columns at index 5+; JSON and
/// proto extraction are mutually exclusive, so both lists start there, with
/// `headers` last.
pub struct RowWriter<'a> {
    pub stream_name: &'a str,
    pub format: PayloadFormat,
    pub ignore_errors: bool,
    pub json_fields: &'a [String],
    pub proto_descriptor: Option<&'a MessageDescriptor>,
    pub proto_fields: &'a [ProtoField],
    /// Configured subject filter, consumed by [`non_proto_hint`] only; rows
    /// reaching the writer are already filtered.
    pub subject_filter: Option<&'a str>,
    pub base: BaseVectorsMut<'a>,
    pub json_vecs: Vec<FlatVector<'a>>,
    pub proto_vecs: Vec<FlatVector<'a>>,
    pub headers_vec: Option<FlatVector<'a>>,
}

/// Mutable handles to the base-column vectors (indices 0-4), split out so
/// [`RowWriter`] borrows them distinctly from the extra-column vectors.
pub struct BaseVectorsMut<'a> {
    pub stream: FlatVector<'a>,
    pub subject: FlatVector<'a>,
    pub seq: FlatVector<'a>,
    pub ts: FlatVector<'a>,
    pub payload: FlatVector<'a>,
}

impl<'a> RowWriter<'a> {
    /// Write one row into every output vector at index `n`.
    pub fn write_row(&mut self, n: usize, row: &Row) -> Result<(), ScanError> {
        self.base.stream.insert(n, self.stream_name);
        self.base.subject.insert(n, row.subject.as_str());
        // Safety: n < VECTOR_SIZE and the vectors are sized for
        // STANDARD_VECTOR_SIZE; rows are written sequentially from 0.
        unsafe {
            self.base.seq.as_mut_slice::<u64>()[n] = row.seq;
            self.base.ts.as_mut_slice::<i64>()[n] = row.ts_micros;
        }

        // Decoded once to drive the error check, the extracted columns, and the
        // format => 'json' payload path.
        let proto_decoded = if let Some(descriptor) = self.proto_descriptor {
            let decoded = proto::decode_message(descriptor, &row.payload);
            let is_instance = decoded
                .as_ref()
                .is_some_and(|d| proto::is_message_instance(d, &row.payload));
            if !self.ignore_errors && !is_instance {
                return Err(ScanError::NonProtoPayload {
                    stream: self.stream_name.to_string(),
                    seq: row.seq,
                    hint: non_proto_hint(&row.payload, self.subject_filter),
                });
            }
            Some(decoded)
        } else {
            None
        };

        self.write_payload(n, row, &proto_decoded)?;

        if !self.json_fields.is_empty() {
            self.write_json_columns(n, row)?;
        }

        if let Some(decoded) = &proto_decoded {
            self.write_proto_columns(n, decoded.as_ref());
        }

        self.write_headers(n, row);
        Ok(())
    }

    fn write_payload(
        &mut self,
        n: usize,
        row: &Row,
        proto_decoded: &Option<Option<DynamicMessage>>,
    ) -> Result<(), ScanError> {
        let has_proto = self.proto_descriptor.is_some();
        match (self.format, has_proto) {
            (PayloadFormat::Blob, _) => self.base.payload.insert(n, row.payload.as_ref()),
            (PayloadFormat::Text, _) => {
                write_utf8_payload(&mut self.base.payload, n, &row.payload, self.ignore_errors)
                    .map_err(|_| ScanError::NonUtf8Payload {
                        stream: self.stream_name.to_string(),
                        seq: row.seq,
                    })?;
            }
            (PayloadFormat::Json, true) => {
                let json = proto_decoded
                    .as_ref()
                    .and_then(|d| d.as_ref())
                    .and_then(proto::message_to_json);
                match json {
                    Some(s) => self.base.payload.insert(n, s.as_str()),
                    None => self.base.payload.set_null(n),
                }
            }
            (PayloadFormat::Json, false) => {
                // Emitted unparsed; DuckDB validates JSON at query time, so
                // only non-UTF-8 is rejected here.
                write_utf8_payload(&mut self.base.payload, n, &row.payload, self.ignore_errors)
                    .map_err(|_| ScanError::NonUtf8Payload {
                        stream: self.stream_name.to_string(),
                        seq: row.seq,
                    })?;
            }
        }
        Ok(())
    }

    fn write_json_columns(&mut self, n: usize, row: &Row) -> Result<(), ScanError> {
        let doc: Option<serde_json::Value> = serde_json::from_slice(&row.payload).ok();
        if doc.is_none() && !self.ignore_errors {
            return Err(ScanError::NonJsonPayload {
                stream: self.stream_name.to_string(),
                seq: row.seq,
                hint: non_json_hint(&row.payload),
            });
        }
        for (i, path) in self.json_fields.iter().enumerate() {
            match doc.as_ref().and_then(|d| json_extract_string(d, path)) {
                Some(s) => self.json_vecs[i].insert(n, s.as_str()),
                None => self.json_vecs[i].set_null(n),
            }
        }
        Ok(())
    }

    fn write_proto_columns(&mut self, n: usize, decoded: Option<&DynamicMessage>) {
        for (i, field) in self.proto_fields.iter().enumerate() {
            let value = decoded
                .map(|d| proto::extract_value(d, &field.path))
                .unwrap_or(ProtoValue::Null);
            write_proto_value(&mut self.proto_vecs[i], n, value);
        }
    }

    fn write_headers(&mut self, n: usize, row: &Row) {
        let Some(vec) = self.headers_vec.as_mut() else {
            return;
        };
        match row.headers.as_deref() {
            Some(s) => vec.insert(n, s),
            None => vec.set_null(n),
        }
    }
}

/// Marker for non-UTF-8 without `ignore_errors`; the caller raises the typed
/// error with stream/seq context.
#[derive(Debug)]
pub struct NonUtf8Payload;

/// Write a raw payload into a VARCHAR vector as UTF-8 text, shared by
/// `format => 'text'` and non-proto `format => 'json'`.
///
/// SQL NULL when `ignore_errors` drops an undecodable payload;
/// [`NonUtf8Payload`] otherwise, leaving the slot untouched.
pub fn write_utf8_payload(
    vec: &mut FlatVector,
    row: usize,
    payload: &[u8],
    ignore_errors: bool,
) -> Result<(), NonUtf8Payload> {
    match std::str::from_utf8(payload) {
        Ok(s) => {
            vec.insert(row, s);
            Ok(())
        }
        Err(_) if ignore_errors => {
            vec.set_null(row);
            Ok(())
        }
        Err(_) => Err(NonUtf8Payload),
    }
}

/// Extract a value by dot-separated path and render it as a VARCHAR string.
///
/// Scalars render naturally (`42`, `2.5`, `true`, unquoted strings); objects
/// and arrays render as compact JSON text. Returns `None` for absent paths and
/// JSON `null`, which the caller maps to SQL NULL.
pub fn json_extract_string(doc: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = doc;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }

    match current {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Hint appended to a JSON decode error, suggesting the protobuf decoder when
/// the payload looks binary. Returns a leading `": ..."` fragment or empty.
///
/// Checks the raw first byte, not a whitespace-skipped one: the common protobuf
/// tag byte `0x0A` is ASCII newline, so skipping whitespace would discard the
/// exact bytes worth flagging.
pub fn non_json_hint(payload: &[u8]) -> String {
    if payload.first().is_some_and(|&b| b < 0x20) {
        ": the payload looks binary (e.g. protobuf); use proto_file or \
         proto_descriptors, proto_message, and proto_extract instead of json_extract"
            .to_string()
    } else {
        String::new()
    }
}

/// Hint appended to a protobuf decode error: suggests the JSON decoder when
/// the payload looks like JSON, and warns that a wildcard subject filter can
/// span several proto message types. Returns a leading `": ..."` fragment or
/// empty.
pub fn non_proto_hint(payload: &[u8], subject_filter: Option<&str>) -> String {
    let mut parts: Vec<&str> = Vec::new();

    if matches!(lead_byte(payload), Some(b'{') | Some(b'[')) {
        parts.push(
            "the payload looks like JSON; use json_extract instead of proto_file or \
             proto_descriptors, proto_message, and proto_extract",
        );
    }
    if subject_filter.is_some_and(|f| f.contains('*') || f.contains('>')) {
        parts.push(
            "a wildcard subject filter can match payloads of several proto message types; \
             query each type with its own subject filter and proto_message, and union \
             the results",
        );
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(": {}", parts.join("; "))
    }
}

/// First non-ASCII-whitespace byte of `payload`, if any.
fn lead_byte(payload: &[u8]) -> Option<u8> {
    payload.iter().find(|b| !b.is_ascii_whitespace()).copied()
}

#[cfg(test)]
mod tests {
    use super::{json_extract_string, non_json_hint, non_proto_hint};

    #[test]
    fn json_scalar_extraction() {
        let doc = serde_json::json!({"status": "new", "count": 42, "ratio": 2.5, "ok": true});
        assert_eq!(json_extract_string(&doc, "status").as_deref(), Some("new"));
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

    #[test]
    fn hint_flags_protobuf_like_binary() {
        // 0x0A: protobuf field-1 tag, and ASCII newline, which the raw
        // first-byte check must not skip.
        let proto = b"\x0A\x26c1fb1bb4-e595-4db7-a434";
        assert!(non_json_hint(proto).contains("protobuf"));
    }

    #[test]
    fn hint_is_empty_for_json() {
        assert_eq!(non_json_hint(b"{\"a\":1}"), "");
        assert_eq!(non_json_hint(b"  [1, 2, 3]"), "");
    }

    #[test]
    fn proto_hint_flags_json_text() {
        assert!(non_proto_hint(b"{\"a\":1}", None).contains("json_extract"));
        assert!(non_proto_hint(b"   [1,2]", None).contains("json_extract"));
    }

    #[test]
    fn proto_hint_is_empty_for_binary_without_wildcards() {
        assert_eq!(non_proto_hint(b"\x0A\x26abc", None), "");
        assert_eq!(non_proto_hint(b"\x0A\x26abc", Some("orders.new")), "");
    }

    #[test]
    fn proto_hint_flags_wildcard_subject_filter() {
        for filter in ["orders.*", "orders.>", "orders.*.created"] {
            assert!(
                non_proto_hint(b"\x0A\x26abc", Some(filter)).contains("wildcard"),
                "{filter}"
            );
        }
    }

    #[test]
    fn proto_hint_combines_json_and_wildcard() {
        let hint = non_proto_hint(b"{\"a\":1}", Some("orders.*"));
        assert!(hint.contains("json_extract"));
        assert!(hint.contains("wildcard"));
        assert!(hint.starts_with(": "));
    }
}

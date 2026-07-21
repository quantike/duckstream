//! Writing buffered [`Row`](crate::row::Row)s into DuckDB output vectors,
//! including the JSON and protobuf field-extraction that populates the extra
//! columns.
//!
//! Base columns (stream, subject, seq, ts, payload) are written directly by
//! `func`. This module holds the per-value helpers for the optional extracted
//! columns: [`json_extract_string`] pulls a dot-path value out of a JSON
//! document as text, and [`write_proto_value`] writes a decoded protobuf value
//! into a typed flat vector.

use duckdb::core::{FlatVector, Inserter};

use crate::proto::ProtoValue;

/// Write an extracted protobuf value into a typed DuckDB flat vector at `row`.
///
/// The column type was chosen at bind time to match the field's protobuf kind,
/// so each [`ProtoValue`] variant lines up with the vector's storage. A
/// [`ProtoValue::Null`] (missing field or undecodable payload) sets SQL NULL.
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

/// Extract a value from a JSON document by dot-separated path and render it as
/// a string suitable for a VARCHAR column.
///
/// Path segments navigate nested objects (`order.id` descends `order` then
/// `id`). Scalars render naturally (`42`, `2.5`, `true`, unquoted strings);
/// nested objects and arrays render as compact JSON text so they can be
/// re-parsed or `CAST` downstream. Returns `None` when the path is absent or
/// resolves to JSON `null`, which the caller maps to SQL NULL.
pub fn json_extract_string(doc: &serde_json::Value, path: &str) -> Option<String> {
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

/// Hint appended to a JSON decode error, suggesting the protobuf decoder when
/// the payload looks binary. Returns a leading `": ..."` fragment or empty.
///
/// Checks the raw first byte, not a whitespace-skipped one: the common protobuf
/// tag byte `0x0A` is ASCII newline, so skipping whitespace would discard the
/// exact bytes worth flagging.
pub fn non_json_hint(payload: &[u8]) -> String {
    if payload.first().is_some_and(|&b| b < 0x20) {
        ": the payload looks binary (e.g. protobuf); use proto_file, \
         proto_message, and proto_extract instead of json_extract"
            .to_string()
    } else {
        String::new()
    }
}

/// Hint appended to a protobuf decode error, suggesting the JSON decoder when
/// the payload looks like JSON. Returns a leading `": ..."` fragment or empty.
pub fn non_proto_hint(payload: &[u8]) -> String {
    if looks_like_json(payload) {
        ": the payload looks like JSON; use json_extract instead of proto_file, \
         proto_message, and proto_extract"
            .to_string()
    } else {
        String::new()
    }
}

/// Text for a lazily-emitted `format => 'json'` payload, or `None` to write SQL
/// NULL.
///
/// The bytes are emitted unparsed (DuckDB validates at query time). With
/// `ignore_errors`, a payload whose lead byte does not open a JSON object/array
/// is dropped to NULL so a mixed stream does not fail a scan. This is a lead-byte
/// sniff, not a parse: subtly-malformed JSON still reaches DuckDB.
pub fn json_payload_text(payload: &[u8], ignore_errors: bool) -> Option<&str> {
    if ignore_errors && !looks_like_json(payload) {
        return None;
    }
    std::str::from_utf8(payload).ok()
}

/// Whether the payload's first non-whitespace byte opens a JSON object or array.
///
/// Protobuf decoding is permissive and often succeeds on JSON bytes, so this
/// positive check flags a JSON stream that a decode would otherwise accept. It
/// misses top-level JSON scalars, which are rare in practice.
pub fn looks_like_json(payload: &[u8]) -> bool {
    matches!(lead_byte(payload), Some(b'{') | Some(b'['))
}

/// First non-ASCII-whitespace byte of `payload`, if any.
fn lead_byte(payload: &[u8]) -> Option<u8> {
    payload.iter().find(|b| !b.is_ascii_whitespace()).copied()
}

#[cfg(test)]
mod tests {
    use super::{
        json_extract_string, json_payload_text, looks_like_json, non_json_hint, non_proto_hint,
    };

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

    #[test]
    fn hint_flags_protobuf_like_binary() {
        // 0x0A: protobuf field 1, length-delimited. Also ASCII newline.
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
        assert!(non_proto_hint(b"{\"a\":1}").contains("json_extract"));
        assert!(non_proto_hint(b"   [1,2]").contains("json_extract"));
    }

    #[test]
    fn proto_hint_is_empty_for_binary() {
        assert_eq!(non_proto_hint(b"\x0A\x26abc"), "");
    }

    #[test]
    fn json_sniff_matches_object_and_array_only() {
        assert!(looks_like_json(b"{\"a\":1}"));
        assert!(looks_like_json(b"\n\t [1]"));
        assert!(!looks_like_json(b"\x0A\x26abc"));
        // A bare JSON string/number at top level is intentionally not sniffed.
        assert!(!looks_like_json(b"\"hello\""));
        assert!(!looks_like_json(b""));
    }

    #[test]
    fn json_payload_text_passes_through_without_ignore() {
        assert_eq!(json_payload_text(b"{\"a\":1}", false), Some("{\"a\":1}"));
        assert_eq!(json_payload_text(b"not json", false), Some("not json"));
    }

    #[test]
    fn json_payload_text_skips_non_json_under_ignore() {
        assert_eq!(json_payload_text(b"\x0A\x26abc", true), None);
        assert_eq!(json_payload_text(b"not json", true), None);
        assert_eq!(json_payload_text(b"  [1,2]", true), Some("  [1,2]"));
    }

    #[test]
    fn json_payload_text_rejects_invalid_utf8() {
        assert_eq!(json_payload_text(&[0xff, 0xfe], false), None);
    }
}

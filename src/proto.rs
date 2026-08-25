//! Runtime protobuf decoding for the `proto_extract` parameter of `read_jetstream`.
//!
//! Unlike JSON extraction (which is schemaless and emits VARCHAR), a protobuf
//! `.proto` schema is supplied at bind time, so each extracted field maps to a
//! concrete DuckDB column type. The pipeline is pure Rust with no external
//! `protoc` binary:
//!
//!   1. [`compile_proto`] turns a `.proto` file into a [`MessageDescriptor`]
//!      via `protox` + `prost-reflect`, while [`load_descriptors`] builds one
//!      from a pre-compiled `FileDescriptorSet` (`buf build -o`, or
//!      `protoc --descriptor_set_out --include_imports`).
//!   2. [`field_column`] reflects a (possibly nested) field path into its
//!      DuckDB [`LogicalTypeId`].
//!   3. [`decode_message`] + [`extract_value`] dynamically decode a payload and
//!      pull out a typed value per field path.

use std::error::Error;
use std::path::Path;

use duckdb::core::LogicalTypeId;
use prost::Message as _;
use prost_reflect::{Cardinality, DescriptorPool, DynamicMessage, Kind, MessageDescriptor, Value};

/// Errors from compiling a `.proto` schema or resolving a field path.
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("failed to compile proto file '{file}': {source}")]
    Compile {
        file: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("message type '{message}' not found in '{file}'")]
    MessageNotFound { message: String, file: String },
    #[error("failed to load descriptor set '{file}': {source}")]
    LoadDescriptors {
        file: String,
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    #[error("field path '{path}' not found in message '{message}'")]
    FieldNotFound { path: String, message: String },
    #[error("field path '{path}' descends into non-message field '{segment}'")]
    NotAMessage { path: String, segment: String },
}

/// A resolved `proto_extract` field: its dot path and the DuckDB column type it
/// maps to (chosen at bind time from the schema).
#[derive(Debug, Clone)]
pub struct ProtoField {
    pub path: String,
    pub column_type: LogicalTypeId,
}

/// Compile a `.proto` file at runtime (pure Rust, no `protoc`) and return the
/// descriptor for the requested message type.
///
/// `protox` resolves imports relative to include paths, so the file's parent
/// directory is used as the include root and the file is referenced by name.
pub fn compile_proto(file: &str, message: &str) -> Result<MessageDescriptor, ProtoError> {
    let path = Path::new(file);
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let include = dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    let file_name = path.file_name().map(Path::new).unwrap_or(path);

    let fds =
        protox::compile([file_name], [include.as_path()]).map_err(|e| ProtoError::Compile {
            file: file.to_string(),
            source: Box::new(e),
        })?;

    let pool = DescriptorPool::from_file_descriptor_set(fds).map_err(|e| ProtoError::Compile {
        file: file.to_string(),
        source: Box::new(e),
    })?;

    pool.get_message_by_name(message)
        .ok_or_else(|| ProtoError::MessageNotFound {
            message: message.to_string(),
            file: file.to_string(),
        })
}

/// Load a pre-compiled `FileDescriptorSet` and return the descriptor for the
/// requested message type.
///
/// Accepts the output of `buf build -o` (a buf image is wire-compatible with
/// `FileDescriptorSet`; its extra metadata decodes as unknown fields) and of
/// `protoc --descriptor_set_out --include_imports`. Decoding is lenient, so a
/// truncated or empty set decodes with no files and the lookup fails with
/// [`ProtoError::MessageNotFound`].
pub fn load_descriptors(file: &str, message: &str) -> Result<MessageDescriptor, ProtoError> {
    let bytes = std::fs::read(file).map_err(|e| ProtoError::LoadDescriptors {
        file: file.to_string(),
        source: Box::new(e),
    })?;
    let fds = prost_types::FileDescriptorSet::decode(bytes.as_slice()).map_err(|e| {
        ProtoError::LoadDescriptors {
            file: file.to_string(),
            source: Box::new(e),
        }
    })?;

    let pool =
        DescriptorPool::from_file_descriptor_set(fds).map_err(|e| ProtoError::LoadDescriptors {
            file: file.to_string(),
            source: Box::new(e),
        })?;

    pool.get_message_by_name(message)
        .ok_or_else(|| ProtoError::MessageNotFound {
            message: message.to_string(),
            file: file.to_string(),
        })
}

/// Resolve a dot-separated field path against a message descriptor into a
/// [`ProtoField`] carrying the mapped DuckDB column type.
pub fn field_column(root: &MessageDescriptor, path: &str) -> Result<ProtoField, ProtoError> {
    let mut current = root.clone();
    let mut segments = path.split('.').peekable();

    while let Some(segment) = segments.next() {
        let field =
            current
                .get_field_by_name(segment)
                .ok_or_else(|| ProtoError::FieldNotFound {
                    path: path.to_string(),
                    message: root.full_name().to_string(),
                })?;

        let is_last = segments.peek().is_none();
        if is_last {
            return Ok(ProtoField {
                path: path.to_string(),
                column_type: column_type_for(&field.kind(), field.cardinality()),
            });
        }

        // Intermediate segment must be a singular message to descend into.
        match field.kind() {
            Kind::Message(m) => current = m,
            _ => {
                return Err(ProtoError::NotAMessage {
                    path: path.to_string(),
                    segment: segment.to_string(),
                })
            }
        }
    }

    Err(ProtoError::FieldNotFound {
        path: path.to_string(),
        message: root.full_name().to_string(),
    })
}

/// Map a protobuf field kind + cardinality to a DuckDB logical type.
///
/// Scalars map to their natural DuckDB type. Repeated fields, enums, nested
/// messages, and maps render as VARCHAR (enum name, or JSON text) since DuckDB
/// LIST/STRUCT support for these is deferred.
fn column_type_for(kind: &Kind, cardinality: Cardinality) -> LogicalTypeId {
    if cardinality == Cardinality::Repeated {
        // Repeated fields render as JSON-array text for now.
        return LogicalTypeId::Varchar;
    }
    match kind {
        Kind::Double => LogicalTypeId::Double,
        Kind::Float => LogicalTypeId::Float,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => LogicalTypeId::Integer,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => LogicalTypeId::Bigint,
        Kind::Uint32 | Kind::Fixed32 => LogicalTypeId::UInteger,
        Kind::Uint64 | Kind::Fixed64 => LogicalTypeId::UBigint,
        Kind::Bool => LogicalTypeId::Boolean,
        Kind::String => LogicalTypeId::Varchar,
        Kind::Bytes => LogicalTypeId::Blob,
        Kind::Enum(_) => LogicalTypeId::Varchar,
        Kind::Message(_) => LogicalTypeId::Varchar,
    }
}

/// Dynamically decode a payload against the message descriptor.
pub fn decode_message(descriptor: &MessageDescriptor, payload: &[u8]) -> Option<DynamicMessage> {
    DynamicMessage::decode(descriptor.clone(), payload).ok()
}

/// Whether decoded bytes are really an instance of the declared type.
///
/// Foreign bytes that still parse as valid wire format land in unknown fields.
/// Accept an empty payload (all-default message), any present declared field,
/// or a decode that left no unknown field. The last case admits a proto3
/// message carrying only explicit default-valued scalars, which set no field.
pub fn is_message_instance(message: &DynamicMessage, payload: &[u8]) -> bool {
    payload.is_empty()
        || message.fields().next().is_some()
        || message.unknown_fields().next().is_none()
}

/// Serialize an already-decoded message to JSON, for `format => 'json'` on a
/// protobuf stream.
///
/// Field names are verbatim from the `.proto` (snake_case), so `->>` paths and
/// `proto_extract` column names agree. Value encodings follow canonical
/// proto-JSON: 64-bit integers as strings, `bytes` as base64, enums by name.
/// `None` means serialization failed.
pub fn message_to_json(message: &DynamicMessage) -> Option<String> {
    use prost_reflect::SerializeOptions;

    let options = SerializeOptions::new().use_proto_field_name(true);
    let mut buf = Vec::new();
    let mut serializer = serde_json::Serializer::new(&mut buf);
    message
        .serialize_with_options(&mut serializer, &options)
        .ok()?;
    String::from_utf8(buf).ok()
}

/// A typed value extracted from a decoded protobuf message, ready to be written
/// into a DuckDB column. `None` variants and a missing path both mean SQL NULL.
#[derive(Debug)]
pub enum ProtoValue {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

/// Extract a value by dot path from a decoded message.
pub fn extract_value(msg: &DynamicMessage, path: &str) -> ProtoValue {
    let mut segments = path.split('.');
    let first = match segments.next() {
        Some(s) => s,
        None => return ProtoValue::Null,
    };

    let mut value = msg.get_field_by_name(first).map(|c| c.into_owned());
    for seg in segments {
        value = match value {
            Some(Value::Message(m)) => m.get_field_by_name(seg).map(|c| c.into_owned()),
            _ => None,
        };
    }

    match value {
        Some(v) => convert(&v),
        None => ProtoValue::Null,
    }
}

/// Convert a reflected protobuf value into a [`ProtoValue`]. Enums render as
/// their value name; repeated fields and nested messages render as JSON text.
fn convert(v: &Value) -> ProtoValue {
    match v {
        Value::Bool(b) => ProtoValue::Bool(*b),
        Value::I32(n) => ProtoValue::I32(*n),
        Value::I64(n) => ProtoValue::I64(*n),
        Value::U32(n) => ProtoValue::U32(*n),
        Value::U64(n) => ProtoValue::U64(*n),
        Value::F32(n) => ProtoValue::F32(*n),
        Value::F64(n) => ProtoValue::F64(*n),
        Value::String(s) => ProtoValue::Text(s.clone()),
        Value::Bytes(b) => ProtoValue::Bytes(b.to_vec()),
        Value::EnumNumber(_) | Value::List(_) | Value::Map(_) | Value::Message(_) => {
            ProtoValue::Text(value_to_json_text(v))
        }
    }
}

/// Best-effort JSON-ish rendering for values that do not map to a scalar column
/// (enums by number, repeated fields, nested messages, maps).
fn value_to_json_text(v: &Value) -> String {
    match v {
        Value::Bool(b) => b.to_string(),
        Value::I32(n) => n.to_string(),
        Value::I64(n) => n.to_string(),
        Value::U32(n) => n.to_string(),
        Value::U64(n) => n.to_string(),
        Value::F32(n) => n.to_string(),
        Value::F64(n) => n.to_string(),
        Value::String(s) => format!("{s:?}"),
        Value::Bytes(b) => format!("{b:?}"),
        Value::EnumNumber(n) => n.to_string(),
        Value::List(items) => {
            let parts: Vec<_> = items.iter().map(value_to_json_text).collect();
            format!("[{}]", parts.join(","))
        }
        Value::Map(entries) => {
            let parts: Vec<_> = entries
                .iter()
                .map(|(k, val)| format!("{k:?}:{}", value_to_json_text(val)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Message(m) => {
            let parts: Vec<_> = m
                .fields()
                .map(|(f, val)| format!("{:?}:{}", f.name(), value_to_json_text(val)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const SCHEMA: &str = r#"
        syntax = "proto3";
        package t;
        message Inner { string name = 1; }
        message Msg {
            uint64 id = 1;
            double amount = 2;
            bool ok = 3;
            string label = 4;
            Inner inner = 5;
            repeated string tags = 6;
        }
    "#;

    /// Write the schema to a temp file and compile it, returning the descriptor.
    ///
    /// Each call gets its own directory. Tests run in parallel within one
    /// process, so a PID-only path would race: `File::create` truncates the
    /// shared file while another test reads it mid-write, compiling an empty
    /// unit whose `t.Msg` is then not found.
    fn compile_test_schema() -> MessageDescriptor {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "duckstream_proto_test_{}_{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.proto");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(SCHEMA.as_bytes()).unwrap();
        compile_proto(path.to_str().unwrap(), "t.Msg").unwrap()
    }

    /// Compile the schema and write its `FileDescriptorSet` to a fresh temp
    /// file, returning the path. Mirrors `buf build -o` output.
    fn write_test_descriptors() -> std::path::PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "duckstream_proto_fds_{}_{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let proto_path = dir.join("t.proto");
        std::fs::write(&proto_path, SCHEMA).unwrap();
        let fds = protox::compile(["t.proto"], [dir.as_path()]).unwrap();
        let fds_path = dir.join("descriptors.binpb");
        std::fs::write(&fds_path, fds.encode_to_vec()).unwrap();
        fds_path
    }

    #[test]
    fn descriptors_load_and_map_types() {
        let path = write_test_descriptors();
        let desc = load_descriptors(path.to_str().unwrap(), "t.Msg").unwrap();
        assert_eq!(
            field_column(&desc, "id").unwrap().column_type,
            LogicalTypeId::UBigint
        );
        assert_eq!(
            field_column(&desc, "inner.name").unwrap().column_type,
            LogicalTypeId::Varchar
        );

        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("label", Value::String("hi".into()));
        let bytes = msg.encode_to_vec();
        let decoded = decode_message(&desc, &bytes).unwrap();
        assert!(matches!(
            extract_value(&decoded, "label"),
            ProtoValue::Text(ref s) if s == "hi"
        ));
    }

    #[test]
    fn descriptors_missing_message_is_named_error() {
        let path = write_test_descriptors();
        let err = load_descriptors(path.to_str().unwrap(), "t.Nope").unwrap_err();
        assert!(matches!(err, ProtoError::MessageNotFound { .. }));
    }

    #[test]
    fn descriptors_missing_file_is_read_error() {
        let err = load_descriptors("/nonexistent/descriptors.binpb", "t.Msg").unwrap_err();
        assert!(matches!(err, ProtoError::LoadDescriptors { .. }));
    }

    #[test]
    fn descriptors_garbage_bytes_fail_to_pool() {
        // Decodes as a FileDescriptorSet whose single FileDescriptorProto has
        // name length 4 but no name bytes; the pool rejects it.
        let dir =
            std::env::temp_dir().join(format!("duckstream_proto_badfds_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.binpb");
        std::fs::write(&path, [0x0A, 0x02, 0x0A, 0x04]).unwrap();
        let err = load_descriptors(path.to_str().unwrap(), "t.Msg").unwrap_err();
        assert!(matches!(err, ProtoError::LoadDescriptors { .. }));
    }

    #[test]
    fn maps_scalar_types_to_duckdb() {
        let desc = compile_test_schema();
        assert_eq!(
            field_column(&desc, "id").unwrap().column_type,
            LogicalTypeId::UBigint
        );
        assert_eq!(
            field_column(&desc, "amount").unwrap().column_type,
            LogicalTypeId::Double
        );
        assert_eq!(
            field_column(&desc, "ok").unwrap().column_type,
            LogicalTypeId::Boolean
        );
        assert_eq!(
            field_column(&desc, "label").unwrap().column_type,
            LogicalTypeId::Varchar
        );
        // Nested message and repeated fields render as VARCHAR for now.
        assert_eq!(
            field_column(&desc, "inner").unwrap().column_type,
            LogicalTypeId::Varchar
        );
        assert_eq!(
            field_column(&desc, "tags").unwrap().column_type,
            LogicalTypeId::Varchar
        );
        // Nested scalar via dot path.
        assert_eq!(
            field_column(&desc, "inner.name").unwrap().column_type,
            LogicalTypeId::Varchar
        );
    }

    #[test]
    fn rejects_bad_paths() {
        let desc = compile_test_schema();
        assert!(field_column(&desc, "nope").is_err());
        // Descending into a scalar is an error.
        assert!(field_column(&desc, "label.x").is_err());
    }

    #[test]
    fn decode_and_extract_values() {
        let desc = compile_test_schema();

        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("id", Value::U64(99));
        msg.set_field_by_name("amount", Value::F64(1.5));
        msg.set_field_by_name("ok", Value::Bool(true));
        msg.set_field_by_name("label", Value::String("hi".into()));
        let inner_desc = desc.parent_pool().get_message_by_name("t.Inner").unwrap();
        let mut inner = DynamicMessage::new(inner_desc);
        inner.set_field_by_name("name", Value::String("deep".into()));
        msg.set_field_by_name("inner", Value::Message(inner));

        let bytes = msg.encode_to_vec();
        let decoded = decode_message(&desc, &bytes).unwrap();

        assert!(matches!(extract_value(&decoded, "id"), ProtoValue::U64(99)));
        assert!(matches!(extract_value(&decoded, "amount"), ProtoValue::F64(v) if v == 1.5));
        assert!(matches!(
            extract_value(&decoded, "ok"),
            ProtoValue::Bool(true)
        ));
        assert!(matches!(extract_value(&decoded, "label"), ProtoValue::Text(ref s) if s == "hi"));
        // Nested descent.
        assert!(
            matches!(extract_value(&decoded, "inner.name"), ProtoValue::Text(ref s) if s == "deep")
        );
        // Missing field -> Null.
        assert!(matches!(
            extract_value(&decoded, "missing"),
            ProtoValue::Null
        ));
    }

    #[test]
    fn message_to_json_uses_verbatim_field_names() {
        let desc = compile_test_schema();

        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("id", Value::U64(99));
        msg.set_field_by_name("amount", Value::F64(1.5));
        msg.set_field_by_name("ok", Value::Bool(true));
        msg.set_field_by_name("label", Value::String("hi".into()));
        let inner_desc = desc.parent_pool().get_message_by_name("t.Inner").unwrap();
        let mut inner = DynamicMessage::new(inner_desc);
        inner.set_field_by_name("name", Value::String("deep".into()));
        msg.set_field_by_name("inner", Value::Message(inner));

        let bytes = msg.encode_to_vec();

        let message = decode_message(&desc, &bytes).unwrap();
        let json = message_to_json(&message).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["label"], serde_json::json!("hi"));
        assert_eq!(parsed["ok"], serde_json::json!(true));
        assert_eq!(parsed["inner"]["name"], serde_json::json!("deep"));
        // 64-bit integers serialize as strings (canonical proto-JSON).
        assert_eq!(parsed["id"], serde_json::json!("99"));
    }

    #[test]
    fn message_to_json_returns_none_on_garbage() {
        let desc = compile_test_schema();
        // A lone run of high continuation bytes is an invalid varint tag.
        assert!(decode_message(&desc, &[0xff, 0xff, 0xff, 0xff]).is_none());
    }

    #[test]
    fn real_message_is_an_instance() {
        let desc = compile_test_schema();
        let mut msg = DynamicMessage::new(desc.clone());
        msg.set_field_by_name("id", Value::U64(99));
        let bytes = msg.encode_to_vec();
        let decoded = decode_message(&desc, &bytes).unwrap();
        assert!(is_message_instance(&decoded, &bytes));
    }

    #[test]
    fn empty_payload_is_the_empty_instance() {
        // An all-default message encodes to zero bytes; the empty payload is a
        // valid instance, not foreign data.
        let desc = compile_test_schema();
        let decoded = decode_message(&desc, &[]).unwrap();
        assert!(is_message_instance(&decoded, &[]));
    }

    #[test]
    fn explicit_default_scalar_is_an_instance() {
        // A proto3 default-valued scalar written on the wire (tag 0x08, value 0)
        // sets no field yet leaves no unknown field: still a real instance.
        let desc = compile_test_schema();
        let payload = [0x08u8, 0x00];
        let decoded = decode_message(&desc, &payload).unwrap();
        assert!(decoded.fields().next().is_none());
        assert!(is_message_instance(&decoded, &payload));
    }

    /// The guard as `func` applies it: decode, then require a real instance.
    fn reads_as_instance(desc: &MessageDescriptor, payload: &[u8]) -> bool {
        decode_message(desc, payload).is_some_and(|d| is_message_instance(&d, payload))
    }

    #[test]
    fn json_object_is_not_an_instance() {
        // Foreign to the wire format: the decoder either errors or fills only
        // unknown fields. Either way, rejected.
        let desc = compile_test_schema();
        assert!(!reads_as_instance(&desc, br#"{"id":"99"}"#));
        assert!(!reads_as_instance(&desc, br#"  [1, 2, 3]"#));
    }

    #[test]
    fn json_scalar_is_not_an_instance() {
        // False negative under the old sniff: a scalar has no `{`/`[` lead byte.
        let desc = compile_test_schema();
        assert!(!reads_as_instance(&desc, b"42"));
        assert!(!reads_as_instance(&desc, b"\"foo\""));
    }

    #[test]
    fn group_lead_byte_with_declared_field_is_an_instance() {
        // False positive under the old sniff: `{` (0x7B) is a valid start-group
        // tag, yet the message also sets a declared field.
        let desc = compile_test_schema();
        let start_group_15 = 0x7Bu8; // (15 << 3) | 3
        let end_group_15 = 0x7Cu8; // (15 << 3) | 4
        let id_field = [0x08u8, 0x01]; // (1 << 3) | 0, varint 1
        let mut payload = vec![start_group_15, end_group_15];
        payload.extend_from_slice(&id_field);
        assert_eq!(payload[0], b'{');

        let decoded = decode_message(&desc, &payload).unwrap();
        assert!(is_message_instance(&decoded, &payload));
    }
}

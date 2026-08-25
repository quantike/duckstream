//! Test harness: broker provisioning, seeding, and the SQL-file snapshot runner.
//!
//! A [`Case`] declares fixture data in Rust and points at a committed `.sql`
//! file under `tests/cases/`; the harness seeds a stream, runs the SQL through
//! the `duckdb` CLI with the extension loaded, and snapshots the csv output
//! with `insta`.
//!
//! The CLI is driven as a subprocess rather than an in-process
//! [`duckdb::Connection`] because this crate builds `duckdb` with the
//! `loadable-extension` feature, which replaces DuckDB's C-API entrypoints and
//! panics when used as a client. Cargo unifies that feature into every test
//! target in the package, so an in-process connection is impossible here.
//!
//! Cases share one broker but each uses a unique stream name and subject prefix
//! (`it_<name>.`), so they run in parallel without overlapping subject
//! namespaces (which JetStream forbids across streams).

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use prost::Message as _;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::SyncRunner;
use testcontainers_modules::testcontainers::{Container, ImageExt};

/// A live broker plus the connection URL the extension and seeder both use.
struct Broker {
    url: String,
    /// Kept alive for the process lifetime; `None` when `NATS_URL` is used.
    _container: Option<Container<Nats>>,
}

/// The shared broker for this test process. Started lazily on first use.
static BROKER: OnceLock<Option<Broker>> = OnceLock::new();

/// Resolve the broker: `NATS_URL` if set, otherwise a JetStream container.
///
/// Returns `None` only when a container is required but Docker is unavailable,
/// which the caller treats as a skip rather than a failure.
fn broker() -> Option<&'static Broker> {
    BROKER
        .get_or_init(|| {
            if let Ok(url) = std::env::var("NATS_URL") {
                return Some(Broker {
                    url,
                    _container: None,
                });
            }
            let cmd = NatsServerCmd::default().with_jetstream();
            match Nats::default().with_cmd(&cmd).start() {
                Ok(container) => {
                    let port = container.get_host_port_ipv4(4222).ok()?;
                    Some(Broker {
                        url: format!("nats://127.0.0.1:{port}"),
                        _container: Some(container),
                    })
                }
                Err(err) => {
                    eprintln!("SKIP: could not start NATS container ({err}); set NATS_URL to use a local broker");
                    None
                }
            }
        })
        .as_ref()
}

/// Locate the built extension: `DUCKSTREAM_EXT`, else the default debug path.
fn extension_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("DUCKSTREAM_EXT") {
        let p = PathBuf::from(p);
        return p.exists().then_some(p);
    }
    let p =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("build/debug/duckstream.duckdb_extension");
    p.exists().then_some(p)
}

/// The `duckdb` binary to invoke: `DUCKDB_BIN`, else `duckdb` on `PATH`.
fn duckdb_bin() -> String {
    std::env::var("DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string())
}

/// A single fixture message to publish.
struct Message {
    subject: String,
    payload: Vec<u8>,
    headers: Option<Vec<(String, Vec<String>)>>,
}

/// A scalar field value for [`Case::publish_proto`]. Covers the field kinds
/// the committed `.proto` fixtures use; add variants as new fixtures need
/// them.
pub enum ProtoFieldValue {
    U64(u64),
    F64(f64),
    Text(&'static str),
}

/// One ordered step in a case's script.
enum Step {
    /// Publish a batch of messages to the stream.
    Publish(Vec<Message>),
    /// Run a committed `.sql` file and snapshot its output under this name.
    Query { sql_file: String, snapshot: String },
}

/// A declarative end-to-end case: stream config plus an ordered script that
/// interleaves publishing and querying.
pub struct Case {
    name: String,
    subjects: Vec<String>,
    steps: Vec<Step>,
}

impl Case {
    /// Start a case named `name`; the name derives the isolated stream name and
    /// the default snapshot name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            subjects: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Declare the stream's subjects, relative to the case's subject prefix.
    /// A declared `orders.>` becomes `it_<name>.orders.>` on the broker, so
    /// cases cannot overlap subject namespaces. Use `${SUBJECT_PREFIX}` in the
    /// `.sql` to match (the harness substitutes `it_<name>`).
    pub fn stream(mut self, subjects: &[&str]) -> Self {
        self.subjects = subjects
            .iter()
            .map(|s| format!("{}.{s}", self.prefix()))
            .collect();
        self
    }

    /// Append a publish step. Publishes between queries model messages arriving
    /// mid-case (e.g. new messages a durable consumer should pick up on rerun).
    pub fn publish(mut self, subject: &str, payload: &[u8]) -> Self {
        let msg = Message {
            subject: format!("{}.{subject}", self.prefix()),
            payload: payload.to_vec(),
            headers: None,
        };
        match self.steps.last_mut() {
            Some(Step::Publish(batch)) => batch.push(msg),
            _ => self.steps.push(Step::Publish(vec![msg])),
        }
        self
    }

    /// Append a publish step with message headers. Each `(name, values)` pair
    /// maps to a NATS header; multiple values produce a multi-valued header.
    pub fn publish_with_headers(
        mut self,
        subject: &str,
        payload: &[u8],
        headers: Vec<(String, Vec<String>)>,
    ) -> Self {
        let msg = Message {
            subject: format!("{}.{subject}", self.prefix()),
            payload: payload.to_vec(),
            headers: Some(headers),
        };
        match self.steps.last_mut() {
            Some(Step::Publish(batch)) => batch.push(msg),
            _ => self.steps.push(Step::Publish(vec![msg])),
        }
        self
    }

    /// Append a publish step whose payload is a protobuf message built from the
    /// case's committed schema.
    ///
    /// Compiles `tests/cases/<case>.proto` with `protox`, sets each
    /// `(field, value)` pair on a [`prost_reflect::DynamicMessage`], and
    /// encodes it. The fixture derives from the schema file, so editing the
    /// `.proto` without updating the values fails here rather than producing
    /// stale bytes.
    pub fn publish_proto(
        self,
        subject: &str,
        message: &str,
        fields: &[(&str, ProtoFieldValue)],
    ) -> Self {
        let pool = compile_case_pool(&self.name);
        let descriptor = pool
            .get_message_by_name(message)
            .unwrap_or_else(|| panic!("message '{message}' not found in {}.proto", self.name));

        let mut msg = prost_reflect::DynamicMessage::new(descriptor);
        for (name, value) in fields {
            let value = match value {
                ProtoFieldValue::U64(v) => prost_reflect::Value::U64(*v),
                ProtoFieldValue::F64(v) => prost_reflect::Value::F64(*v),
                ProtoFieldValue::Text(s) => prost_reflect::Value::String((*s).to_string()),
            };
            msg.set_field_by_name(name, value);
        }
        self.publish(subject, &msg.encode_to_vec())
    }

    /// Append a query step: run `tests/cases/<sql_file>` and snapshot it under
    /// `snapshot`. For multi-query cases, call this once per query in order.
    pub fn query(mut self, sql_file: &str, snapshot: &str) -> Self {
        self.steps.push(Step::Query {
            sql_file: sql_file.to_string(),
            snapshot: snapshot.to_string(),
        });
        self
    }

    /// Run the single-query convenience form: execute `tests/cases/<name>.sql`
    /// and snapshot it under `<name>`. Any earlier [`Case::publish`] calls are
    /// the seed.
    pub fn run(mut self) {
        let sql_file = format!("{}.sql", self.name);
        let snapshot = self.name.clone();
        self.steps.push(Step::Query { sql_file, snapshot });
        self.execute();
    }

    /// Create the stream, then execute the script in order.
    pub fn run_script(self) {
        self.execute();
    }

    /// The case's subject prefix and stream-name stem, e.g. `it_scan_range`.
    fn prefix(&self) -> String {
        format!("it_{}", self.name)
    }

    fn execute(self) {
        // Cheapest check first: no point starting Docker if there is nothing to load.
        let Some(ext) = extension_path() else {
            eprintln!(
                "SKIP: extension not built; run `make debug` or set DUCKSTREAM_EXT (looked for build/debug/duckstream.duckdb_extension)"
            );
            return;
        };
        let Some(broker) = broker() else {
            return; // skip: no broker and no Docker
        };

        let prefix = self.prefix();
        let stream = prefix.to_uppercase();
        create_stream(&broker.url, &stream, &self.subjects);

        for step in &self.steps {
            match step {
                Step::Publish(batch) => publish(&broker.url, batch),
                Step::Query { sql_file, snapshot } => {
                    let sql = load_sql(sql_file, &broker.url, &stream, &prefix);
                    let output = run_duckdb(&duckdb_bin(), &ext, &sql);
                    insta::assert_snapshot!(snapshot.clone(), output);
                }
            }
        }
    }
}

/// Connect to NATS, retrying briefly to absorb the gap between the container's
/// readiness log and its mapped port accepting TCP.
async fn connect_with_retry(url: &str) -> async_nats::Client {
    let mut last_err = None;
    for _ in 0..50 {
        match async_nats::connect(url).await {
            Ok(client) => return client,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    panic!("seed: connect to nats at {url}: {}", last_err.unwrap());
}

/// Build a short-lived current-thread runtime for a seeding operation.
fn seed_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build seeding runtime")
}

/// Create the stream fresh, deleting this case's own stream first (its name is
/// unique, so it cannot touch another case's data).
fn create_stream(url: &str, stream: &str, subjects: &[String]) {
    seed_runtime().block_on(async {
        let client = connect_with_retry(url).await;
        let js = async_nats::jetstream::new(client);

        let _ = js.delete_stream(stream).await;
        js.create_stream(async_nats::jetstream::stream::Config {
            name: stream.to_string(),
            subjects: subjects.to_vec(),
            storage: async_nats::jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("seed: create stream");
    });
}

/// Publish a batch, awaiting each ack so ordering and durability hold before
/// the next step (a query) observes them.
fn publish(url: &str, messages: &[Message]) {
    seed_runtime().block_on(async {
        let client = connect_with_retry(url).await;
        let js = async_nats::jetstream::new(client.clone());
        for m in messages {
            let ack = if let Some(ref headers) = m.headers {
                let mut h = async_nats::HeaderMap::new();
                for (name, values) in headers {
                    for (i, value) in values.iter().enumerate() {
                        if i == 0 {
                            h.insert(name.as_str(), value.as_str());
                        } else {
                            h.append(name.as_str(), value.as_str());
                        }
                    }
                }
                js.publish_with_headers(m.subject.clone(), h, m.payload.clone().into())
                    .await
                    .expect("seed: publish")
            } else {
                js.publish(m.subject.clone(), m.payload.clone().into())
                    .await
                    .expect("seed: publish")
            };
            ack.await.expect("seed: publish ack");
        }
    });
}

/// Read a committed `.sql` file and substitute `${NATS_URL}`, `${STREAM}`,
/// `${SUBJECT_PREFIX}`, and `${DESCRIPTORS}`.
///
/// `${DESCRIPTORS}` expands to a temp file holding the `FileDescriptorSet`
/// compiled from the case's `tests/cases/<name>.proto` (the artifact form
/// `buf build -o` produces). The path is stable per test binary, so parallel
/// harness runs cannot race: a concurrent writer truncates and rewrites
/// identical bytes.
///
/// The placeholders keep the files runnable by hand: export the same env and
/// the SQL still reads naturally. `${DESCRIPTORS}` is the exception; it must
/// point at a pre-built `FileDescriptorSet`, which the harness compiles from
/// the case's `.proto` here.
fn load_sql(file: &str, url: &str, stream: &str, prefix: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("tests/cases").join(file);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let descriptors = raw
        .contains("${DESCRIPTORS}")
        .then(|| compile_case_descriptors(&root, file.strip_suffix(".sql").unwrap_or(file)));
    let mut sql = raw
        .replace("${NATS_URL}", url)
        .replace("${STREAM}", stream)
        .replace("${SUBJECT_PREFIX}", prefix);
    if let Some(path) = descriptors {
        sql = sql.replace("${DESCRIPTORS}", &path.display().to_string());
    }
    sql
}

/// Compile `tests/cases/<case>.proto` into a `FileDescriptorSet` under the
/// temp dir and return its path.
fn compile_case_descriptors(root: &std::path::Path, case: &str) -> PathBuf {
    let proto = root.join("tests/cases").join(format!("{case}.proto"));
    let fds = compile_proto_file(&proto);
    let path = std::env::temp_dir().join(format!("duckstream_case_{case}.binpb"));
    std::fs::write(&path, fds.encode_to_vec())
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    path
}

/// Compile a `.proto` file into a `FileDescriptorSet`, resolving imports
/// relative to the file's own directory.
fn compile_proto_file(proto: &std::path::Path) -> prost_types::FileDescriptorSet {
    protox::compile(
        [proto.file_name().unwrap()],
        [proto.parent().unwrap().as_os_str()],
    )
    .unwrap_or_else(|e| panic!("compile {}: {e}", proto.display()))
}

/// Compile `tests/cases/<case>.proto` into a descriptor pool for building
/// fixture messages.
fn compile_case_pool(case: &str) -> prost_reflect::DescriptorPool {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let proto = root.join("tests/cases").join(format!("{case}.proto"));
    prost_reflect::DescriptorPool::from_file_descriptor_set(compile_proto_file(&proto))
        .expect("pool from compiled schema")
}

/// Run SQL through the DuckDB CLI with the extension loaded, in csv output mode
/// for stable snapshots. Combines stdout and stderr so errors are snapshotted too.
fn run_duckdb(bin: &str, ext: &std::path::Path, sql: &str) -> String {
    let script = format!("LOAD '{}';\n{sql}", ext.display());
    let out = Command::new(bin)
        .arg("-unsigned")
        .arg("-batch")
        .arg("-csv")
        .arg("-c")
        .arg(&script)
        .output()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    if stderr.trim().is_empty() {
        stdout.to_string()
    } else {
        format!("{stdout}--- stderr ---\n{stderr}")
    }
}

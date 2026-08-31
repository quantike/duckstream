# duckstream

[![Rust](https://github.com/quantike/duckstream/actions/workflows/rust.yml/badge.svg)](https://github.com/quantike/duckstream/actions/workflows/rust.yml)
[![Main Distribution Pipeline](https://github.com/quantike/duckstream/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/quantike/duckstream/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for querying [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) with SQL.

Written in Rust on top of DuckDB's [C Extension API](https://duckdb.org/docs/stable/clients/c/overview), targeting
DuckDB v1.5.5.

## Features

- Bounded reads over a JetStream stream by sequence (`start_seq`/`end_seq`) or timestamp (`start_time`/`end_time`).
- Three read modes: a stateless scan (JetStream Direct Get), an ephemeral consumer that drains everything currently in
  the stream, and a durable consumer that persists a cursor and resumes from where the last run left off. Both consumer
  modes report progress.
- Subject filtering with NATS token semantics (`*` matches one token, `>` matches trailing tokens).
- JSON extraction: name JSON field paths (dot notation for nested fields) and get one column each. Scalars render as
  their natural value, nested values as JSON text.
- Protocol Buffers extraction: supply a schema at query time, either a `.proto` source file (compiled in pure Rust,
  no `protoc` binary required) or a pre-compiled descriptor set (`buf build -o`,
  `protoc --descriptor_set_out --include_imports`), and get columns whose types are derived from the schema
  (`UBIGINT`, `DOUBLE`, `BOOLEAN`, and so on), including nested-field descent.
- Stream metadata: `jetstream_streams()` returns the configuration and live state of every stream (message counts,
  sequence bounds, retention, limits), one row per stream, or a single stream via `stream =>`.
- Per-subject message counts: `jetstream_subjects(stream)` returns one row per distinct subject in a stream, with an
  optional server-side `subject =>` filter. Useful for finding hot subjects before a filtered read.

## SQL API

The extension registers three table functions: `read_jetstream` (message reads), `jetstream_streams` (stream
catalog), and `jetstream_subjects` (per-subject counts). `read_jetstream` returns five base columns (`stream`,
`subject`, `seq`, `ts_nats`, `payload`) plus one extra column per extracted JSON or protobuf field, and an optional
`headers` column (when `headers => true`).

### `read_jetstream(stream, ...)`

A bounded read that always completes. It runs in one of three modes:

- Scan (default): a stateless read by sequence or timestamp range, using the JetStream Direct Get API.
- Ephemeral consumer (`ephemeral => true`): creates a throwaway JetStream consumer that drains every message in the
  stream up to the moment of the query, then completes.
- Durable consumer (`durable => 'name'`): creates or attaches a named, server-persisted consumer. Each run reads only
  the messages that arrived since the last run, resuming from the stored cursor.

Both consumer modes report their message count as the query cardinality, so DuckDB shows a progress bar, and apply the
subject filter server-side.

| Parameter       | Type      | Description                                                                                                                                                                   |
| --------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `stream`        | VARCHAR   | Stream name (positional, required).                                                                                                                                           |
| `url`           | VARCHAR   | NATS server URL. Default `nats://localhost:4222`.                                                                                                                             |
| `ephemeral`     | BOOLEAN   | `true` reads via an ephemeral consumer instead of a scan. Default `false`.                                                                                                    |
| `durable`       | VARCHAR   | Consumer name. Selects durable mode. Mutually exclusive with `ephemeral`.                                                                                                     |
| `subject`       | VARCHAR   | NATS subject filter, wildcards allowed (`*`, `>`). Applied client-side in scan mode, server-side in consumer modes.                                                           |
| `start_seq`     | UBIGINT   | Start sequence (inclusive). Scan mode, or `start => 'by_start_seq'`.                                                                                                          |
| `end_seq`       | UBIGINT   | End sequence (inclusive). Scan mode.                                                                                                                                          |
| `start_time`    | TIMESTAMP | Start time (inclusive). Scan mode, or `start => 'by_start_time'`.                                                                                                             |
| `end_time`      | TIMESTAMP | End time (inclusive). Scan mode.                                                                                                                                              |
| `start`         | VARCHAR   | Starting point for a new consumer: `all` (default), `new`, `last`, `by_start_seq`, `by_start_time`. Honored only when the consumer is first created. Consumer modes.          |
| `batch`         | UBIGINT   | Messages requested per `fetch` while draining. Default `256`. Consumer modes.                                                                                                 |
| `max_messages`  | UBIGINT   | Hard cap on the number of rows returned. Consumer modes.                                                                                                                      |
| `json_extract`  | VARCHAR[] | JSON field paths, each mapped to a VARCHAR column.                                                                                                                            |
| `proto_file`    | VARCHAR   | Path to a `.proto` schema file. Mutually exclusive with `proto_descriptors`.                                                                                                  |
| `proto_descriptors` | VARCHAR | Path to a pre-compiled `FileDescriptorSet` (a `buf build -o` image works: it is wire-compatible). Mutually exclusive with `proto_file`.                                   |
| `proto_message` | VARCHAR   | Message type name within the schema.                                                                                                                                          |
| `proto_extract` | VARCHAR[] | Protobuf field paths, each mapped to a schema-typed column.                                                                                                                   |
| `format`        | VARCHAR   | Type of the `payload` column: `blob` (default), `text`, or `json`. `json` emits VARCHAR aliased `JSON`, so the `json` extension operators (`->`, `->>`) apply without a cast. |
| `ignore_errors` | BOOLEAN   | When `true`, payloads that fail to decode leave the affected columns NULL instead of failing the query. Default `false`.                                                      |
| `headers`       | BOOLEAN   | `true` adds a `headers` column (VARCHAR aliased `JSON`) containing message headers serialized as a JSON object. Messages without headers yield NULL. NATS system headers (`Nats-*`) are filtered out. Default `false`. |

`json_extract` and `proto_extract` cannot be used together. `proto_extract` requires `proto_message` and one schema
source (`proto_file` or `proto_descriptors`). `durable` and `ephemeral` cannot be used together. `batch` and `max_messages` apply only to consumer
modes. Durable consumers ack each message on emit, advancing the cursor (at-least-once: a cancelled query redelivers
unacked messages on the next run).

`format` sets the type of the whole `payload` column and is independent of the `json_extract`/`proto_*` projections
(which add their own columns), so it composes with either. `format => 'json'` on a JSON stream is lazy: bytes are
emitted unparsed and DuckDB validates on access, except that `ignore_errors` drops clearly-non-JSON payloads to NULL.
`format => 'text'` requires valid UTF-8; a non-UTF-8 payload fails the query unless `ignore_errors => true`, which drops
it to NULL. Supplying a proto schema and `proto_message` with `format => 'json'` decodes each message and serializes it to the
`payload` column (field names verbatim from the `.proto`), so `proto_extract` is not required in that case.

By default, a payload that does not match the chosen decoder fails the query, naming the stream and sequence and
pointing at the other decoder. This catches pointing `json_extract` at a protobuf stream, or `proto_*` at a JSON stream.
Set `ignore_errors => true` to tolerate mixed streams; undecodable rows are still emitted with NULL extracted columns.

```sql
-- Scan a sequence range. payload defaults to BLOB (renders as \xNN-escaped hex);
-- use format => 'json' (or 'text') to read it as text.
SELECT seq, subject, payload
FROM read_jetstream('ORDERS', start_seq => 1, end_seq => 100, format => 'json');

-- Ephemeral consumer: read everything currently in the stream (with a progress bar)
SELECT count(*) FROM read_jetstream('ORDERS', ephemeral => true);

-- Durable consumer: each run reads only new messages (acks are automatic)
SELECT * FROM read_jetstream('ORDERS', durable => 'nightly_etl');

-- Cap the drain and tune the fetch batch size
SELECT * FROM read_jetstream('ORDERS', ephemeral => true, max_messages => 1000, batch => 500);

-- Filter by subject and extract JSON fields as columns
SELECT "order.id", total
FROM read_jetstream('ORDERS',
    subject      => 'orders.us.*',
    json_extract => ['order.id', 'total']);

-- Extract protobuf fields with schema-derived types (no CAST needed)
SELECT sum(total)
FROM read_jetstream('ORDERS',
    proto_file    => 'order.proto',
    proto_message => 'shop.Order',
    proto_extract => ['id', 'total']);

-- Same, but against a descriptor set from a schema pipeline
-- (buf build -o descriptors.binpb, or protoc --descriptor_set_out --include_imports)
SELECT sum(total)
FROM read_jetstream('ORDERS',
    proto_descriptors => 'descriptors.binpb',
    proto_message     => 'shop.Order',
    proto_extract     => ['id', 'total']);

-- Emit the payload as JSON and navigate it in SQL with -> / ->>
SELECT payload->>'$.customer.name' AS customer, payload->>'$.total' AS total
FROM read_jetstream('ORDERS', format => 'json');

-- Surface message headers as a JSON column (opt-in)
SELECT
    headers->'$.Content-Type'->>'$[0]' AS content_type,
    headers->'$.X-Trace-Id'->>'$[0]'   AS trace_id
FROM read_jetstream('ORDERS', headers => true);
```

### `jetstream_streams()`

The stream catalog, one row per stream. Called with no selector it enumerates every stream on the server; with
`stream =>` it looks up one stream exactly (an unknown name fails the query with a 404-style error). A point-in-time
snapshot taken at query start.

| Column               | Type      | Description                                                            |
| -------------------- | --------- | ---------------------------------------------------------------------- |
| `stream`             | VARCHAR   | Stream name.                                                           |
| `created`            | TIMESTAMP | When the stream was created.                                           |
| `messages`           | UBIGINT   | Number of messages currently in the stream.                            |
| `bytes`              | UBIGINT   | Total bytes of stored messages.                                        |
| `first_seq`          | UBIGINT   | Lowest sequence still present.                                         |
| `first_ts`           | TIMESTAMP | Timestamp of the oldest message.                                       |
| `last_seq`           | UBIGINT   | Last sequence assigned.                                                |
| `last_ts`            | TIMESTAMP | Timestamp of the newest message.                                       |
| `consumer_count`     | UBIGINT   | Number of consumers on the stream.                                     |
| `subjects_count`     | UBIGINT   | Number of distinct subjects.                                           |
| `deleted_count`      | UBIGINT   | Deleted message count, or NULL when the server does not report one.     |
| `retention`          | VARCHAR   | `limits`, `interest`, or `workqueue`.                                  |
| `storage`            | VARCHAR   | `file` or `memory`.                                                    |
| `discard`            | VARCHAR   | `old` or `new`.                                                        |
| `max_messages`       | BIGINT    | Message limit; -1 means unlimited.                                     |
| `max_bytes`          | BIGINT    | Byte limit; -1 means unlimited.                                        |
| `max_message_size`   | BIGINT    | Largest accepted message; -1 means unlimited.                          |
| `num_replicas`       | UINTEGER  | Replica count.                                                         |
| `sealed`             | BOOLEAN   | Whether the stream is sealed.                                          |
| `allow_direct`       | BOOLEAN   | Whether Direct Get is enabled.                                         |
| `description`        | VARCHAR   | Stream description, or NULL.                                           |
| `subjects`           | VARCHAR[] | Subject filters configured on the stream.                             |

Parameters: `stream` (VARCHAR, optional, exact single-stream lookup) and `url` (VARCHAR, default
`nats://localhost:4222`).

```sql
-- Every stream on the server
SELECT stream, messages, last_seq FROM jetstream_streams() ORDER BY stream;

-- One stream: check state and find the last sequence
SELECT messages, last_seq, retention FROM jetstream_streams(stream => 'ORDERS');
```

### `jetstream_subjects(stream)`

Per-subject message counts for one stream, one row per distinct subject (exact subject literals, never wildcard
patterns). A point-in-time snapshot taken at query start. Useful for finding hot subjects before a filtered
`read_jetstream` read, or verifying that a stream's subject coverage matches expectations.

| Column     | Type    | Description                                 |
| ---------- | ------- | ------------------------------------------- |
| `stream`   | VARCHAR | Stream name.                                |
| `subject`  | VARCHAR | Distinct subject literal.                   |
| `messages` | UBIGINT | Messages currently stored on that subject.  |

Parameters: `stream` (VARCHAR, positional, required), `subject` (VARCHAR, optional, server-side filter with token
wildcards `*` and `>`), and `url` (VARCHAR, default `nats://localhost:4222`).

```sql
-- Hot subjects first
SELECT subject, messages
FROM jetstream_subjects('ORDERS')
ORDER BY messages DESC;

-- Only the US subtree
SELECT * FROM jetstream_subjects('ORDERS', subject => 'orders.us.>');
```

## Building

This is a DuckDB loadable extension, so `cargo build` alone is not enough. DuckDB loads a `.duckdb_extension` file
carrying a version-matched metadata footer, which is appended by DuckDB's `extension-ci-tools` flow. Clone with
submodules, then use the `Makefile`:

```sh
git clone --recurse-submodules https://github.com/quantike/duckstream.git
make configure   # set up a Python venv and the DuckDB test runner, detect the platform
make debug       # cargo build, then append the .duckdb_extension footer
```

The built extension is written to `build/debug/duckstream.duckdb_extension`. Use `make release` for an optimized build.

The extension is built against a single DuckDB version (v1.5.5) because it uses DuckDB's unstable C API via `duckdb-rs`.
The produced binary loads only into that exact DuckDB version.

### Loading

A locally built (unsigned) extension requires the `-unsigned` flag. macOS refuses to load an extension by relative path,
so use an absolute path:

```sh
duckdb -unsigned
```

```sql
LOAD '/absolute/path/to/duckstream/build/debug/duckstream.duckdb_extension';
SELECT * FROM read_jetstream('ORDERS');
```

Or preload it when launching from the repo root:

```sh
duckdb -unsigned -cmd "LOAD '$PWD/build/debug/duckstream.duckdb_extension'"
```

## Testing

```sh
cargo test --lib   # Rust unit tests (network-free)
cargo test --test integration   # SQL + broker: load, register, scan, snapshot
```

The unit tests need no broker. The integration tests build the extension and run real SQL against a live NATS JetStream
broker, asserting on snapshotted output.

### Integration tests

`tests/integration.rs` runs the built `.duckdb_extension` against a live NATS JetStream broker and asserts on real SQL
output. Each case seeds a stream with `async-nats`, runs a committed `.sql` file (under `tests/cases/`) through the
`duckdb` CLI (csv output) with the extension loaded, and snapshots the result with [`insta`](https://insta.rs). The
extension is driven as a subprocess because this crate builds `duckdb` with the `loadable-extension` feature, which
cannot also be used as an in-process client. Cases share one broker but each uses a unique stream name and subject
prefix, so they run in parallel without colliding.

```sh
make debug                    # build the extension the tests load
cargo test --test integration
```

Requirements:

- A built extension at `build/debug/duckstream.duckdb_extension`, or `DUCKSTREAM_EXT` pointing at one. If missing, the
  tests skip rather than fail.
- A `duckdb` binary on `PATH` (or `DUCKDB_BIN`), matching the build version (v1.5.5).
- A broker: either Docker (a `nats:2.10.14 --jetstream` container is started automatically), or set `NATS_URL` to reuse
  a local `nats-server -js`:

  ```sh
  nats-server -js &
  NATS_URL=nats://localhost:4222 cargo test --test integration
  ```

Snapshots live in `tests/snapshots/` and are reviewed with `cargo insta review`. The `.sql` files stay runnable by hand;
they use `${NATS_URL}`, `${STREAM}`, and `${SUBJECT_PREFIX}` placeholders the harness substitutes. Descriptor cases also
use `${DESCRIPTORS}`, which the harness compiles from the case's `.proto`; to run those by hand, point it at a pre-built
`FileDescriptorSet` (for example `buf build -o out.binpb`). CI runs this suite in
`integration.yml`.

Note: against the Docker broker the scan-mode cases take ~30s each because the extension's scan path holds its NATS
connection open at query end (the `duckdb` process only exits once that connection tears down). Against a local
`NATS_URL` broker the same suite finishes in well under a second, so it is the fastest way to run the tests locally.

## License

MIT. See [LICENSE](LICENSE).

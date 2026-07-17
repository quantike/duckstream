# duckstream

[![Rust](https://github.com/quantike/duckstream/actions/workflows/rust.yml/badge.svg)](https://github.com/quantike/duckstream/actions/workflows/rust.yml)
[![Main Distribution Pipeline](https://github.com/quantike/duckstream/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/quantike/duckstream/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for querying [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) with SQL.

Written in Rust on top of DuckDB's [C Extension API](https://duckdb.org/docs/stable/clients/c/overview),
targeting DuckDB v1.5.4.

## Features

- Bounded reads over a JetStream stream by sequence (`start_seq`/`end_seq`) or timestamp
  (`start_time`/`end_time`).
- Two read modes: a stateless scan (JetStream Direct Get) and an ephemeral consumer that drains
  everything currently in the stream and reports progress.
- Subject filtering with NATS token semantics (`*` matches one token, `>` matches trailing tokens).
- JSON extraction: name JSON field paths (dot notation for nested fields) and get one column each.
  Scalars render as their natural value, nested values as JSON text.
- Protocol Buffers extraction: supply a `.proto` schema at query time (compiled in pure Rust, no
  `protoc` binary required) and get columns whose types are derived from the schema (`UBIGINT`,
  `DOUBLE`, `BOOLEAN`, and so on), including nested-field descent.

## SQL API

The extension registers the `read_nats` table function. It returns five base columns (`stream`,
`subject`, `seq`, `ts_nats`, `payload`) plus one extra column per extracted JSON or protobuf field.

### `read_nats(stream, ...)`

A bounded read that always completes. It runs in one of two modes:

- Scan (default): a stateless read by sequence or timestamp range, using the JetStream Direct Get
  API.
- Ephemeral consumer (`ephemeral => true`): creates a throwaway JetStream consumer that drains every
  message in the stream up to the moment of the query, then completes. This mode reports its message
  count as the query cardinality, so DuckDB shows a progress bar, and applies the subject filter
  server-side.

| Parameter       | Type      | Description                                                                                             |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------- |
| `stream`        | VARCHAR   | Stream name (positional, required).                                                                     |
| `url`           | VARCHAR   | NATS server URL. Default `nats://localhost:4222`.                                                       |
| `ephemeral`     | BOOLEAN   | `true` reads via an ephemeral consumer instead of a scan. Default `false`.                              |
| `subject`       | VARCHAR   | NATS subject filter, wildcards allowed (`*`, `>`). Applied client-side in scan mode, server-side in ephemeral mode. |
| `start_seq`     | UBIGINT   | Start sequence (inclusive). Scan mode.                                                                  |
| `end_seq`       | UBIGINT   | End sequence (inclusive). Scan mode.                                                                    |
| `start_time`    | TIMESTAMP | Start time (inclusive). Scan mode.                                                                      |
| `end_time`      | TIMESTAMP | End time (inclusive). Scan mode.                                                                        |
| `json_extract`  | VARCHAR[] | JSON field paths, each mapped to a VARCHAR column.                                                      |
| `proto_file`    | VARCHAR   | Path to a `.proto` schema file.                                                                         |
| `proto_message` | VARCHAR   | Message type name within the schema.                                                                    |
| `proto_extract` | VARCHAR[] | Protobuf field paths, each mapped to a schema-typed column.                                             |

`json_extract` and `proto_extract` cannot be used together. `proto_extract` requires both
`proto_file` and `proto_message`.

```sql
-- Scan a sequence range
SELECT seq, subject, payload
FROM read_nats('ORDERS', start_seq => 1, end_seq => 100);

-- Ephemeral consumer: read everything currently in the stream (with a progress bar)
SELECT count(*) FROM read_nats('ORDERS', ephemeral => true);

-- Filter by subject and extract JSON fields as columns
SELECT "order.id", total
FROM read_nats('ORDERS',
    subject      => 'orders.us.*',
    json_extract => ['order.id', 'total']);

-- Extract protobuf fields with schema-derived types (no CAST needed)
SELECT sum(total)
FROM read_nats('ORDERS',
    proto_file    => 'order.proto',
    proto_message => 'shop.Order',
    proto_extract => ['id', 'total']);
```

## Building

This is a DuckDB loadable extension, so `cargo build` alone is not enough. DuckDB loads a
`.duckdb_extension` file carrying a version-matched metadata footer, which is appended by DuckDB's
`extension-ci-tools` flow. Clone with submodules, then use the `Makefile`:

```sh
git clone --recurse-submodules https://github.com/quantike/duckstream.git
make configure   # set up a Python venv and the DuckDB test runner, detect the platform
make debug       # cargo build, then append the .duckdb_extension footer
```

The built extension is written to `build/debug/duckstream.duckdb_extension`. Use `make release` for
an optimized build.

The extension is built against a single DuckDB version (v1.5.4) because it uses DuckDB's unstable C
API via `duckdb-rs`. The produced binary loads only into that exact DuckDB version.

### Loading

A locally built (unsigned) extension requires the `-unsigned` flag. macOS refuses to load an
extension by relative path, so use an absolute path:

```sh
duckdb -unsigned
```

```sql
LOAD '/absolute/path/to/duckstream/build/debug/duckstream.duckdb_extension';
SELECT * FROM read_nats('ORDERS');
```

Or preload it when launching from the repo root:

```sh
duckdb -unsigned -cmd "LOAD '$PWD/build/debug/duckstream.duckdb_extension'"
```

## Testing

```sh
cargo test        # Rust unit tests
make test_debug   # SQL tests via DuckDB's SQLLogicTest runner
```

The SQL tests verify that the extension loads and registers its functions. Behavioral tests against
a live NATS server are run locally, since CI has no broker.

## License

MIT. See [LICENSE](LICENSE).

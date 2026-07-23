# adbc-driver-monetdb

[ADBC](https://arrow.apache.org/adbc/) driver for [MonetDB](https://www.monetdb.org/), written in Rust.

Arrow-native reads and writes for MonetDB: polars, pandas, and every other ADBC consumer get
columnar result sets (MonetDB's binary result-set protocol decoded directly into Arrow record
batches) and bulk ingestion (`COPY BINARY ... ON CLIENT` streamed from Arrow buffers) through one
standard interface.

## Usage

```python
import polars as pl
from adbc_driver_monetdb import dbapi

with dbapi.connect("monetdb://user:password@localhost:50000/db") as conn:
    df = pl.read_database("SELECT * FROM trades", conn)
    df.write_database("trades_copy", conn, if_table_exists="append", engine="adbc")

# or resolved from the URI scheme:
df = pl.read_database_uri("SELECT 1", "monetdb://localhost:50000/db", engine="adbc")
# TLS URIs resolve through the bundled adbc_driver_monetdbs shim:
secure_df = pl.read_database_uri("SELECT 1", "monetdbs://localhost:50000/db", engine="adbc")
```

The DB-API connection starts with autocommit disabled, as required by PEP 249. Call
`conn.commit()` to persist a transaction, or pass `autocommit=True` explicitly. Closing a
connection, including by leaving its context manager, rolls back uncommitted work.
Consume or close a query's result stream before executing another statement or changing
transaction state on the same connection. Use independent connections for parallel queries;
ADBC permits drivers to block or reject concurrent statements on one connection, and MonetDB's
single MAPI channel shares transaction state between every statement on that connection.

The same connection works directly with pandas 3:

```python
import pandas as pd

with dbapi.connect("monetdb://user:password@localhost:50000/db") as conn:
    df = pd.read_sql("SELECT * FROM trades", conn, dtype_backend="pyarrow")
    df.to_sql("trades_copy", conn, if_exists="append", index=False)
```

## Credentials and read-only access

SQLAlchemy-style URIs work: userinfo in `monetdb://user:password@localhost:50000/db` is
percent-decoded and stripped from the URI before it reaches the protocol layer. URIs end up in
shell history, logs, and tracebacks, so prefer supplying credentials separately through the
standard ADBC database options `username` and `password` (also available as
`adbc_driver_manager.DatabaseOptions.USERNAME` / `.PASSWORD`), which override any URI userinfo:

```python
import os

from adbc_driver_monetdb import dbapi

with dbapi.connect(
    "monetdb://localhost:50000/db",
    db_kwargs={
        "username": os.environ["MONETDB_USER"],
        "password": os.environ["MONETDB_PASSWORD"],
    },
) as conn:
    ...
```

The password option is write-only: reading it back through `get_option` is an error.

MonetDB has no per-connection read-only mode — the server rejects read-only transactions
outright (`42000!Readonly transactions not supported`), so setting the ADBC option
`adbc.connection.readonly` to `true` returns `NotImplemented` (see the waived-surface table
below). For read-only access, connect as a user that holds only `SELECT` privileges; the
server then enforces this for every statement on the connection:

```sql
CREATE USER reader WITH PASSWORD 'secret' NAME 'Reporting' SCHEMA sys;
GRANT SELECT ON sys.trades TO reader;
```

## Configuration and timeouts

Timeout values are integer seconds. The default connection deadline is 30 seconds and the default
idle write timeout is 60 seconds. Read and operation timeouts are disabled by default so a healthy
long-running query is not terminated merely because it exceeds a client-side wall-clock limit.
Zero explicitly selects no deadline; negative values and values above the portable socket limit of
4,294,967 seconds are rejected. A connection timeout covers DNS, every address attempt, TCP or Unix
connection, TLS, authentication, redirects, and initial driver metadata. A timeout or cancellation
closes the MAPI session, so the partially read connection cannot be reused.

The URI names are `connect_timeout`, `read_timeout`, `write_timeout`, and
`operation_timeout`. Unknown query names are rejected so misspelled settings cannot be silently
ignored. This is the only configuration channel available to `polars.read_database_uri`:

```python
df = pl.read_database_uri(
    "SELECT * FROM trades",
    "monetdb://localhost:50000/db?connect_timeout=10&operation_timeout=120",
    engine="adbc",
)
```

The same settings can be supplied separately through ADBC options. Database options override
the URI; connection options override the database defaults; statement options override the
connection for that statement.

```python
import polars as pl

from adbc_driver_monetdb import ConnectionOptions, DatabaseOptions, StatementOptions, dbapi

with dbapi.connect(
    "monetdb://localhost:50000/db",
    db_kwargs={
        DatabaseOptions.CONNECT_TIMEOUT: "10",
        DatabaseOptions.OPERATION_TIMEOUT: "120",
    },
    conn_kwargs={
        ConnectionOptions.READ_TIMEOUT: "30",
        ConnectionOptions.READ_PREFETCH: "true",
        ConnectionOptions.WRITE_BATCH_ROWS: "100000",
    },
) as conn:
    with conn.cursor(
        adbc_stmt_kwargs={
            StatementOptions.OPERATION_TIMEOUT: "15",
            StatementOptions.READ_BATCH_ROWS: "65536",
            StatementOptions.READ_PREFETCH: "true",
        }
    ) as cursor:
        frame = pl.read_database("SELECT * FROM trades", cursor)
```

`polars.read_database(query, connection)` selects ADBC from the supplied DB-API connection or
cursor; it has no `engine="adbc"` parameter. Polars calls `connection.cursor()` without
`adbc_stmt_kwargs`, so use a preconfigured cursor for statement-specific settings. Its
`execute_options` are forwarded to `Cursor.execute`: use a sequence for positional `?` values
and a dictionary for named `:name` values.
`adbc_stmt_kwargs` is not an execute option. Polars' `batch_size` does not configure
`adbc.monetdb.read_batch_rows`, and `DataFrame.write_database(..., engine_options=...)` supplies
ingestion arguments rather than connection or timeout options. The read batch default is 131,072
rows, matching PyArrow Dataset Scanner's default. Read prefetch is enabled by default and overlaps
the next bounded binary window fetch with decoding the current window; set
`adbc.monetdb.read_prefetch` to `"false"` to use the sequential diagnostic path. Ingestion
preserves the incoming Arrow stream's batches by default; setting
`adbc.monetdb.write_batch_rows` to a positive value zero-copy slices larger input batches before
each COPY, while zero keeps the upstream boundaries. Set it through `conn_kwargs` as above when
`DataFrame.write_database` creates its own cursor, or through `adbc_stmt_kwargs` for a directly
managed cursor.

The examples use strings because `adbc-driver-manager` publishes string-valued type hints for
database and connection option mappings. Statement options also accept native integers through the
ADBC integer option path.

The DB-API module reports `threadsafety = 1`: threads may share the module, but each thread should
use its own connection and cursors. Cancellation is connection-scoped: it interrupts the operation
currently using that connection, not necessarily the statement object on which `cancel` was called.
MAPI cannot safely resume a partially read response, so cancellation closes the session permanently;
close that connection and open another one before issuing more work.

The native DB-API parameter style is `qmark` (`?`). Named `:name` parameters are also supported
when a parameter dictionary is supplied, including SQLAlchemy expressions compiled to a SQL
string:

```python
from sqlalchemy import Integer, bindparam, cast, select

value = cast(bindparam("value", value=21), Integer)
compiled = select((value + value).label("value")).compile()
frame = pl.read_database(
    str(compiled),
    conn,
    execute_options={"parameters": compiled.params},
)
```

## Support policy

- MonetDB **Dec2025 (11.55) and newer**, little-endian servers only
- Python **3.13+** (one abi3 wheel per platform), polars **1.42+**, pandas **3.0+**,
  adbc-driver-manager **1.11+**
- Platforms: Linux x86_64 + aarch64 (manylinux), macOS arm64, Windows x86_64

## ADBC release baseline

Feature parity means all required rows below are implemented and tested, not that every optional
ADBC entry point must be synthesized for a backend that cannot use it. This follows ADBC's own
[driver feature matrix](https://arrow.apache.org/adbc/current/driver/status.html): for example,
the stable PostgreSQL driver does not support partitioned results or Substrait, while still
covering the common SQL-driver baseline.

| Required surface | Status |
|---|---|
| SQL query/update execution, Arrow streams, affected-row counts, and execute schema | Supported |
| Prepared statements, positional/named binds, parameter schemas, and `executemany` | Supported |
| Bulk ingest: create, append, replace, create-append, target schema, and temporary tables | Supported |
| Transactions, autocommit, commit, rollback, and current-schema get/set | Supported |
| `GetInfo`, `GetObjects`, `GetTableSchema`, and `GetTableTypes` | Supported |
| TLS and authentication | Certificate file/hash and client certificates are integration-tested; the rustls system-root path is supported |
| Configurable connect/read/write/operation timeouts and cross-thread cancellation | Supported; timeout/cancel closes the session |
| SQLSTATE diagnostics and semantic ADBC statuses | Supported before streaming starts; mid-stream errors retain the server diagnostics in their message, but Arrow stream exceptions cannot expose structured SQLSTATE fields |
| Python DB-API, Polars URI/connection/cursor paths, pandas, wheels, and source builds | Supported |

These optional or backend-inapplicable surfaces are explicitly outside the release gate. Their
entry points are still tested to return ADBC `NotImplemented`, rather than a transport, parser,
or argument error.

| Explicitly waived surface | Reason |
|---|---|
| Partitioned and incremental results | MAPI exposes one sequential result channel |
| Substrait plans | MonetDB accepts SQL, not Substrait plans |
| `GetStatistics` and `GetStatisticNames` | Optional federation metadata, not part of the common SQL-driver baseline |
| Progress and maximum-progress reporting | MAPI does not expose compatible progress metadata |
| Read-only `true` and isolation-level options | The server rejects read-only transactions (`42000!Readonly transactions not supported`), so there is no per-connection control to map; read-only `false` is accepted. Use a `SELECT`-only user instead (see Credentials) |
| Setting the current catalog and cross-catalog ingest | A MAPI session is attached to one database; the current catalog remains readable |

The reusable ADBC validation suite currently passes against Dec2025-SP3. Its skips cover
explicitly waived cross-catalog/statistics behavior and negative-scale decimals that MonetDB
itself does not support. Polars' announced future unknown-extension behavior is exercised in CI;
HUGEINT and TIMETZ remain round-trippable when loaded as extension types. The
`monetdb.hugeint` extension uses Arrow `Decimal128(38, 0)`, so its supported domain is
−(10^38−1) through 10^38−1; wider values in MonetDB's signed 128-bit domain return a bounded
conversion error instead of silently changing the public Arrow type.

Backend-specific type boundaries are explicit:

| MonetDB type | Query results | Parameter binding | Bulk ingest |
|---|---|---|---|
| GEOMETRY | Cast to `VARCHAR` in SQL | Not implemented | Not implemented |
| Legacy INET | Cast to `VARCHAR` in SQL | Not implemented | `NotImplemented` |
| INET4 / INET6 | UTF-8 Arrow extension values | Supported | `NotImplemented` |
| OID | One-row results; multi-row results require a `VARCHAR` cast | Supported as bounded `UInt64` | `NotImplemented` |

MonetDB does not expose compatible `Xexportbin` or `COPY BINARY` representations for the waived
paths. The driver returns bounded errors instead of adding a lossy text-protocol fallback.

## dbc packages and driver-manager loading

Each release target also builds a standalone, non-Python `cdylib` for a flat `dbc` package.
Installing that archive makes
the driver discoverable as `monetdb` by C/C++, Go, R, Ruby, Rust, Python, and other ADBC driver
managers. `dbc` writes the installed ADBC TOML manifest with an absolute shared-library path; a
relative `Driver.shared` path is not portable.

Linux dbc libraries are built in the pinned `manylinux_2_28` environment and support glibc 2.28
or newer. Windows dbc libraries statically link the Visual C++ runtime. Release CI audits both
constraints before packaging.

```sh
uv run python packaging/generate_licenses.py
uv run python packaging/dbc/build_package.py \
    --library target/release/libadbc_monetdb.dylib \
    --platform macos_arm64 --out-dir dist/dbc --license THIRD_PARTY_LICENSES
ADBC_DRIVER_PATH="$PWD/.adbc-drivers" \
    uvx --from dbc dbc install --no-verify dist/dbc/monetdb_macos_arm64_v*.tar.gz
```

Locally built and GitHub Release archives are unsigned, hence `--no-verify` for direct archive
installation. Every GitHub Release includes `SHA256SUMS`; verify the archive checksum before
installing it.

Polars can use a dbc-installed driver without the `adbc-driver-monetdb` Python package when the
connection is created by the Python driver manager (which is still required by polars' ADBC engine):

```python
import polars as pl
from adbc_driver_manager import dbapi

# The driver itself was installed with: dbc install monetdb
with dbapi.connect(
    driver="monetdb",
    db_kwargs={"uri": "monetdb://user:password@localhost:50000/db"},
) as conn:
    frame = pl.read_database("SELECT * FROM trades", connection=conn)
    frame.write_database("trades_copy", connection=conn, engine="adbc")
```

The URI-string conveniences `pl.read_database_uri(..., engine="adbc")` and
`DataFrame.write_database(..., connection="monetdb://...")` import the driver package by URI
scheme and therefore require the Python distribution. The wheel includes both
`adbc_driver_monetdb` and the TLS alias `adbc_driver_monetdbs`, so both `monetdb://` and
`monetdbs://` work through those conveniences. Both distribution channels are intentional and
ship the same native driver.

## Repository layout

| Path | |
|---|---|
| `crates/adbc-monetdb` | the ADBC driver (cdylib exporting `AdbcDriverMonetdbInit`) |
| `crates/monetdb-arrow` | MonetDB binary wire format ⇄ Arrow conversion |
| `monetdb-rust` | [our fork](https://github.com/wlaur/monetdb-rust) of [MonetDB/monetdb-rust](https://github.com/MonetDB/monetdb-rust) (git submodule, MPL-2.0) — MAPI protocol layer |
| `adbc_driver_monetdb` | Python shim over `adbc-driver-manager`; ships the cdylib as `adbc_driver_monetdb._native` |
| `packaging/dbc` | platform manifests and builder for non-Python driver-manager packages |

## Development

See [CONTRIBUTING.md](https://github.com/wlaur/adbc-driver-monetdb/blob/main/CONTRIBUTING.md) for bug, security, feature-request, and contribution
guidance.

```sh
git clone --recurse-submodules https://github.com/wlaur/adbc-driver-monetdb
uv sync                                # installs deps + builds the extension via maturin
uv run pytest -m "not integration and not local_only"  # python tests (no server needed)
cargo test --workspace                 # rust tests

# integration tests against a real server:
docker compose -f compose.yaml up -d
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
    uv run pytest -m "integration and not local_only"

# manual ~30 GiB logical float32 ingest (8M rows x 1,000 columns, 100k batches):
MONETDB_RUN_LOCAL_BENCHMARK=1 \
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
    uv run pytest tests/test_local_benchmark.py -m local_only -q -s

# run the reusable ADBC conformance suite:
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
    uv run pytest tests/validation
```

Lint/typecheck: `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`,
`cargo clippy --workspace --all-targets`, `cargo fmt --all --check`.

## License

The driver is MIT and the included `monetdb` protocol crate (`monetdb-rust`, our fork of
MonetDB/monetdb-rust) is MPL-2.0. The distribution's license expression is therefore
`MIT AND MPL-2.0`; both license texts and the corresponding-source notice are included in wheels
and source distributions.

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
```

The DB-API connection starts with autocommit disabled, as required by PEP 249. Call
`conn.commit()`, use the connection context manager, or pass `autocommit=True` explicitly.
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

## Configuration and timeouts

Timeout values are integer seconds. Defaults are a 30-second absolute connection deadline,
60-second idle read and write timeouts, and a 300-second absolute operation deadline. Zero
explicitly selects no deadline; negative values and values above the portable socket limit of
4,294,967 seconds are rejected. A connection timeout covers DNS, every address attempt, TCP or
Unix connection, TLS, authentication, redirects, and initial driver metadata. A timeout or
cancellation closes the MAPI session, so the partially read connection cannot be reused.

The URI names are `connect_timeout`, `read_timeout`, `write_timeout`, and
`operation_timeout`. This is the only configuration channel available to
`polars.read_database_uri`:

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
    conn_kwargs={ConnectionOptions.READ_TIMEOUT: "30"},
) as conn:
    with conn.cursor(
        adbc_stmt_kwargs={
            StatementOptions.OPERATION_TIMEOUT: "15",
            StatementOptions.BATCH_ROWS: 65_536,
        }
    ) as cursor:
        frame = pl.read_database("SELECT * FROM trades", cursor)
```

`polars.read_database(query, connection)` selects ADBC from the supplied DB-API connection or
cursor; it has no `engine="adbc"` parameter. Polars calls `connection.cursor()` without
`adbc_stmt_kwargs`, so use a preconfigured cursor for statement-specific settings. Its
`execute_options` are forwarded to `Cursor.execute`: positional `?` values work with
`execute_options={"parameters": (...)}`, while dict/named parameters are unsupported.
`adbc_stmt_kwargs` is not an execute option. Polars' `batch_size` does not configure
`adbc.monetdb.batch_rows`, and `DataFrame.write_database(..., engine_options=...)` supplies
ingestion arguments rather than connection or timeout options.

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
| Prepared statements, positional binds, parameter schemas, and `executemany` | Supported |
| Bulk ingest: create, append, replace, create-append, target schema, and temporary tables | Supported |
| Transactions, autocommit, commit, rollback, and current-schema get/set | Supported |
| `GetInfo`, `GetObjects`, `GetTableSchema`, and `GetTableTypes` | Supported |
| TLS and authentication | System roots, certificate file/hash, and client certificates |
| Finite connect/read/write/operation timeouts and cross-thread cancellation | Supported; timeout/cancel closes the session |
| SQLSTATE diagnostics and semantic ADBC statuses | Supported |
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
| Read-only `true` and isolation-level options | MonetDB does not expose matching per-connection controls through this interface; read-only `false` is accepted |
| Setting the current catalog and cross-catalog ingest | A MAPI session is attached to one database; the current catalog remains readable |
| Named parameter binding | MonetDB prepared statements and this DB-API use positional `?` parameters |

The reusable ADBC validation suite currently passes against Dec2025-SP3. Its skips cover
explicitly waived cross-catalog/statistics behavior and negative-scale decimals that MonetDB
itself does not support. Polars' announced future unknown-extension behavior is exercised in CI;
HUGEINT and TIMETZ remain round-trippable when loaded as extension types.

## Non-Python driver managers

Each platform wheel can also be converted to a flat `dbc` package. Installing that archive makes
the driver discoverable as `monetdb` by C/C++, Go, R, Ruby, Rust, Python, and other ADBC driver
managers. `dbc` writes the installed ADBC TOML manifest with an absolute shared-library path; a
relative `Driver.shared` path is not portable.

```sh
uv run python packaging/dbc/build_package.py \
    --wheel dist/adbc_driver_monetdb-0.1.0-cp313-abi3-macosx_11_0_arm64.whl \
    --platform macos_arm64 --out-dir dist/dbc
ADBC_DRIVER_PATH="$PWD/.adbc-drivers" \
    uvx --from dbc dbc install --no-verify dist/dbc/monetdb_macos_arm64_v0.1.0.tar.gz
```

The repository packages are unsigned development artifacts, hence `--no-verify`. A registry
release should be signed and installed without that flag.

## Repository layout

| Path | |
|---|---|
| `crates/adbc-monetdb` | the ADBC driver (cdylib exporting `AdbcDriverMonetdbInit`) |
| `crates/monetdb-arrow` | MonetDB binary wire format ⇄ Arrow conversion |
| `monetdb-rust` | [our fork](https://github.com/wlaur/monetdb-rust) of [MonetDB/monetdb-rust](https://github.com/MonetDB/monetdb-rust) (git submodule, MPL-2.0) — MAPI protocol layer |
| `adbc_driver_monetdb` | Python shim over `adbc-driver-manager`; ships the cdylib as `adbc_driver_monetdb._native` |
| `packaging/dbc` | platform manifests and builder for non-Python driver-manager packages |

## Development

```sh
git clone --recurse-submodules https://github.com/wlaur/adbc-driver-monetdb
uv sync                                # installs deps + builds the extension via maturin
uv run pytest -m "not integration"     # python tests (no server needed)
cargo test --workspace                 # rust tests

# integration tests against a real server:
docker run -d --platform linux/amd64 -p 50000:50000 \
    -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=test \
    monetdb/monetdb:Dec2025-SP3@sha256:a71e6e8c8402beadc51aebf944b465ee5b185c7ae4a9e6808b5d9133ee921786
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test uv run pytest -m integration

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

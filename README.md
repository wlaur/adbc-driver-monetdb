# adbc-driver-monetdb

[ADBC](https://arrow.apache.org/adbc/) driver for [MonetDB](https://www.monetdb.org/), written in Rust.

Arrow-native reads and writes for MonetDB: polars, pandas, and every other ADBC consumer get
columnar result sets (MonetDB's binary result-set protocol decoded directly into Arrow record
batches) and bulk ingestion (`COPY BINARY ... ON CLIENT` streamed from Arrow buffers) through one
standard interface.

> [!IMPORTANT]
> This release pins `adbc_core` and `adbc_ffi` 0.23 to a
> [public backport](https://github.com/wlaur/arrow-adbc/commit/5fc505640f1c445f5c5c315fc8377ab79cbe566c).
> The crates.io Rust exporter cannot carry a known affected-row count alongside an Arrow result
> stream, although the standard ADBC C API returns both. The backport adds that missing internal
> Rust capability without changing the ADBC ABI. It is compiled into every wheel and DBC library;
> only source builds fetch the pinned commit.
>
> **TODO:** Get this generic capability fixed upstream—by merging the backport or adopting Apache's
> alternative—and then remove the fork pin and return to the official Apache `adbc_core` and
> `adbc_ffi` crates.io releases.

## Installation

Install the Python package:

```sh
uv add adbc-driver-monetdb
```

For standalone driver-manager use, download the archive for your platform from the
[GitHub Releases](https://github.com/wlaur/adbc-driver-monetdb/releases) page, verify it against
the release's `SHA256SUMS`, and install the unsigned local archive:

```sh
uvx --from dbc dbc install --no-verify /path/to/monetdb_PLATFORM_vVERSION.tar.gz
```

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
    },
) as conn:
    with conn.cursor(
        adbc_stmt_kwargs={
            StatementOptions.OPERATION_TIMEOUT: "15",
            StatementOptions.READ_BATCH_ROWS: "65536",
            StatementOptions.READ_PREFETCH: "true",
        }
    ) as cursor:
        df = pl.read_database("SELECT * FROM trades", cursor)
```

`polars.read_database(query, connection)` selects ADBC from the supplied DB-API connection or
cursor; it has no `engine="adbc"` parameter. Polars calls `connection.cursor()` without
`adbc_stmt_kwargs`, so use a preconfigured cursor for statement-specific settings. Its
`execute_options` are forwarded to `Cursor.execute`: use a sequence for positional `?` values
and a dictionary for named `:name` values.
`adbc_stmt_kwargs` is not an execute option. Polars' `batch_size` does not configure
`adbc.monetdb.read_batch_rows`, and `DataFrame.write_database(..., engine_options=...)` supplies
ingestion arguments rather than connection or timeout options. The read batch default is 131,072
rows, matching PyArrow Dataset Scanner's default. Read prefetch is enabled by default and can hold
up to about three windows at once: one decoding, one buffered, and one in flight. Abandoning a
stream can therefore waste up to two fetched windows; increasing `read_batch_rows` also increases
this memory bound. Closing a reader waits briefly for its worker and then detaches a fetch that is
still in flight; it does not implicitly cancel and permanently close the session. The next
statement on that connection may wait for the detached fetch or its configured read/operation
timeout. Use `adbc_cancel()` when session destruction is intended, or set
`adbc.monetdb.read_prefetch` to `"false"` when prompt pool reuse matters more than fetch/decode
overlap.

Ingestion automatically chooses the fewest COPY windows that fit a 512 MiB encoded-byte budget.
For constrained append targets whose fixed-width wire rows are at most 16 bytes, the budget is
raised to 2 GiB.
The driver estimates wire bytes per row from the first 4,096 rows, coalesces small upstream Arrow
batches, splits large ones without copying their buffers, and adapts after each window. Target
windows contain at least 4,096 rows when available; the encoded-byte budget, rather than an
arbitrary row cap, bounds larger windows. The narrow-row constraint budget avoids repeated index
maintenance across tens of millions of rows without imposing the larger memory bound on
variable-width, wide, or ordinary append-only tables. On Linux both ceilings are reduced to a
fixed fraction of a finite cgroup memory limit when needed. Each requested column is then encoded
directly into bounded
1 MiB encoder chunks, which the protocol layer coalesces into 16 MiB upload messages; null-free
signed integers and 32/64-bit floats borrow their Arrow value buffers after validation instead of
copying them. Peak encoding memory is therefore bounded by the upload buffers rather than the
logical window size.

`adbc.monetdb.write_window_bytes` changes the byte budget; zero selects the automatic default.
`adbc.monetdb.write_batch_rows` remains a diagnostic row-count override and disables adaptation.
Both options can be set on a connection or statement, with statement values taking precedence.
Applications normally need neither: producer batch boundaries do not define COPY boundaries.
After an ingest, the read-only statement option `adbc.monetdb.ingest_stats` returns JSON containing
the input-batch and COPY counts, coalesced/split window counts, encoded bytes, bounded in-flight
bytes, window trajectories, transaction scope, and poison state.

An append to an existing table inside an explicit transaction executes directly for every Arrow
stream window. This avoids MonetDB retaining and replaying a full table version for an operation
savepoint. If a client-side stream or encoding error occurs after one or more completed COPY
windows, subsequent reads remain available but `commit()` raises `InvalidState` until
`rollback()` removes the partial append. Server errors retain their DB-API exception and SQLSTATE;
MonetDB itself leaves that transaction aborted until rollback. Autocommit ingestion wraps the
complete stream in an internal transaction, rolls it back on error, and restores the connection.

Two advanced statement or connection options change that contract. Setting
`adbc.monetdb.ingest_atomicity` to `"savepoint"` preserves earlier caller work and rolls back only
the failed ingest, but MonetDB Dec2025 may write an extra full copy of a sub-million-row append to
its WAL. The default `"transaction"` scope avoids that amplification and blocks commit after a
partial client-side failure. Setting `adbc.monetdb.ingest_partial` to `"allow"` permits committing
completed windows after such a failure; the default `"block"` is the safe behavior.

Positional prepared statements are cached per connection by exact SQL text, so consumers such as
SQLAlchemy can create a fresh cursor for each execution without making MonetDB compile the same
plan again. The least-recently-used cache holds at most 128 plans; eviction queues server-side
deallocation, and closing the connection releases the session and every remaining plan.
Whitespace variants are intentionally different keys. Schema-changing statements issued through
the connection invalidate the cache, and externally invalidated plans are prepared again and
retried once when MonetDB can recover without rolling back user work. MonetDB aborts an explicit
transaction when `EXECUTE` reports a missing prepared plan, so a stale plan in that state follows
the normal database-error contract: roll back before retrying. One-row bound DML executes directly;
multi-row bound DML retains a savepoint so the whole parameter batch remains atomic.

`dbapi.Binary` accepts bytes-like values (`bytes`, `bytearray`, and `memoryview`) and returns
`bytes`. Text is rejected with `TypeError`; encode text explicitly before binding it as binary.

## Performance expectations

The Arrow-native path is designed for columnar reads and bulk ingestion. A one-row parameterized
DML execution takes the direct prepared-statement path; parameter batches with two or more rows
use a savepoint so the complete batch stays atomic. The login keeps MonetDB's normal inline reply
window, so small result sets are decoded from the initial response instead of forcing another
fetch.

Very small queries can still be slower than pymonetdb. pymonetdb returns Python tuples directly,
whereas an ADBC query must build an Arrow schema and buffers across the native boundary before the
driver manager converts those buffers back to DB-API tuples. That fixed Arrow/FFI cost is inherent
when a caller asks an Arrow-native ADBC driver for row-oriented Python objects; bypassing it would
make DB-API results disagree with the native ADBC stream. Prefer Arrow consumers such as Polars,
pandas with the PyArrow backend, or `fetch_arrow_table()` when result size makes that fixed cost
material.

The small-query comparison is reproducible against the same server and host:

```sh
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
MONETDB_RUN_LATENCY_BENCHMARK=1 \
uv run pytest tests/test_local_benchmark.py::test_local_short_query_latency_against_pymonetdb -q -s
```

The examples use strings because `adbc-driver-manager` publishes string-valued type hints for
database and connection option mappings. Statement options also accept native integers through the
ADBC integer option path.

The DB-API module reports `threadsafety = 1`: threads may share the module, but each thread should
use its own connection and cursors. Cancellation is connection-scoped: it interrupts the operation
currently using that connection, not necessarily the statement object on which `cancel` was called.
MAPI cannot safely resume a partially read response, so cancellation closes the session permanently;
close that connection and open another one before issuing more work.

## Client information

Client information is sent at login by default. The `client` value in
[`sys.sessions`](https://www.monetdb.org/documentation-Dec2025/user-guide/sql-catalog/users-roles-privileges-sessions/)
identifies this driver and its protocol library, for example
`adbc_driver_monetdb 0.8.8 / monetdb-rust 0.2.2-wlaur.1`. The Python shim uses the basename of
`sys.argv[0]` as the default `application`. Hostname and process id are also sent by default, as
they are by pymonetdb and libmapi; use `client_info=false` if that host metadata should not leave
the client.

The URI parameters are `client_application`, `client_remark`, and `client_info`. The equivalent
pre-connect database options are `adbc.monetdb.client_application`,
`adbc.monetdb.client_remark`, and `adbc.monetdb.client_info`; database options override URI
values. Application and remark values cannot contain newlines.

```python
from adbc_driver_monetdb import DatabaseOptions, dbapi

with dbapi.connect(
    "monetdb://localhost:50000/db?client_application=nightly-load",
    db_kwargs={DatabaseOptions.CLIENT_REMARK: "warehouse refresh"},
) as conn:
    session = conn.execute(
        "SELECT hostname, application, client, clientpid, remark "
        "FROM sys.sessions WHERE sessionid = current_sessionid()"
    ).fetchone()
```

For a post-connect update, call
[`sys.setclientinfo`](https://www.monetdb.org/documentation/admin-guide/monitoring/session-procedures/):

```sql
CALL sys.setclientinfo('ClientRemark', 'phase 2');
```

The native DB-API parameter style is `qmark` (`?`). Named `:name` parameters are also supported
when a parameter dictionary is supplied, including SQLAlchemy expressions compiled to a SQL
string:

```python
from sqlalchemy import Integer, bindparam, cast, select

value = cast(bindparam("value", value=21), Integer)
compiled = select((value + value).label("value")).compile()
df = pl.read_database(
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
conversion error instead of silently changing the public Arrow type. Cast those wider values to
`VARCHAR` in SQL when their full textual representation is required.

MonetDB `JSON` query results use Arrow's canonical
[`arrow.json`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json) extension with
UTF-8 string storage. Under the supported storage policy, Polars loads it as `pl.String`; the
driver does not expand it to `pl.Struct` because one JSON column can contain objects, arrays,
scalars, and JSON `null` with different shapes. Applications that know an object schema can opt in:

```python
decoded = df.with_columns(
    pl.col("payload").str.json_decode(
        dtype=pl.Struct({"id": pl.Int64, "label": pl.String}),
    )
)
```

MonetDB functions declared to return JSON—including `json.filter`, `json.keyarray`, and
`json.valuearray`—preserve `arrow.json`. Functions such as `json.text`, `json.number`,
`json."integer"`, `json.length`, and the JSON predicates return their declared scalar Arrow types.
This follows [MonetDB's JSON model](https://www.monetdb.org/documentation-Dec2025/user-guide/sql-manual/data-types/json-types/),
where JSON is a validated string subtype.

Backend-specific type boundaries are explicit:

| MonetDB type | Query results | Parameter binding | Bulk ingest |
|---|---|---|---|
| GEOMETRY | Cast to `VARCHAR` in SQL | Not implemented | Not implemented |
| Legacy INET | Cast to `VARCHAR` in SQL | Not implemented | `NotImplemented` |
| INET4 / INET6 | UTF-8 Arrow extension values | Supported | Supported |
| OID | One-row results; multi-row results require a `VARCHAR` cast | Supported as bounded `UInt64` | `NotImplemented` |

`Date64` ingest accepts only whole-day millisecond values; intra-day values must use an Arrow
timestamp type. MonetDB does not expose compatible `Xexportbin` or `COPY BINARY` representations
for the remaining waived paths. The driver returns bounded errors instead of adding a lossy
text-protocol fallback.

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

# The driver itself was installed from a downloaded GitHub Release archive as shown above.
with dbapi.connect(
    driver="monetdb",
    db_kwargs={"uri": "monetdb://user:password@localhost:50000/db"},
) as conn:
    df = pl.read_database("SELECT * FROM trades", connection=conn)
    df.write_database("trades_copy", connection=conn, engine="adbc")
```

The URI-string conveniences `pl.read_database_uri(..., engine="adbc")` and
`DataFrame.write_database(..., connection="monetdb://...")` import the driver package by URI
scheme and therefore require the Python distribution. The wheel includes both
`adbc_driver_monetdb` and the TLS alias `adbc_driver_monetdbs`, so both `monetdb://` and
`monetdbs://` work through those conveniences. This alias follows
[Polars' scheme-to-module lookup](https://github.com/pola-rs/polars/blob/py-1.42.1/py-polars/src/polars/io/database/_utils.py#L123-L191);
it is not a second ADBC driver or Python distribution.

ADBC itself treats the canonical
[`uri` option](https://arrow.apache.org/adbc/current/format/specification.html#changelog) and
[driver loading](https://arrow.apache.org/adbc/current/format/driver_manifests.html) separately.
The wheel aliases load the same native entrypoint, and the standalone DBC package has one
`monetdb` manifest. DBC users select `driver="monetdb"` and may pass either URI scheme unchanged
to that driver.

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
# compose.yaml pins the native ARM64 Dec2025-SP3 wlaur/monetdb-container image
docker compose -f compose.yaml up -d
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
    uv run pytest -m "integration and not local_only"

# manual ~30 GiB logical float32 ingest (8M rows x 1,000 columns, 100k batches);
# reports final and peak dbfarm growth plus peak filesystem growth:
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

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

### DuckDB

DuckDB's community [`adbc` extension](https://duckdb.org/community_extensions/extensions/adbc)
can query and attach MonetDB through the standalone driver. Install the platform-specific DBC
archive as shown under [Installation](#installation) before starting DuckDB. Installing only the
Python wheel does not register the `monetdb` driver manifest that DuckDB's embedded ADBC driver
manager discovers.

```sql
INSTALL adbc FROM community;
LOAD adbc;

SELECT *
FROM read_adbc(
    'monetdb://user:password@localhost:50000/db',
    'SELECT * FROM trades WHERE trade_date >= DATE ''2026-01-01'''
);

ATTACH 'monetdb://user:password@localhost:50000/db' AS monet (TYPE adbc);
SELECT * FROM monet.sys.trades;
```

Prefer an
[ADBC connection profile](https://arrow.apache.org/adbc/current/format/connection_profiles.html)
so credentials stay out of SQL and connection history. Use a profile when selecting this driver
for a `monetdbs://` TLS URI: a raw URI makes the driver manager search for a driver named
`monetdbs`, while the standalone package intentionally installs one manifest named `monetdb`.
For example, save this as `monetdb_local.toml` in an ADBC profile directory:

```toml
profile_version = 1
driver = "monetdb"

[Options]
uri = "monetdbs://localhost:50000/db"
username = "{{ env_var(MONETDB_USER) }}"
password = "{{ env_var(MONETDB_PASSWORD) }}"
```

Then use the profile for either interface:

```sql
SELECT * FROM read_adbc('profile://monetdb_local', 'SELECT * FROM trades');
ATTACH 'profile://monetdb_local' AS monet (TYPE adbc);
```

The DuckDB extension operates in autocommit mode, does not automatically push projections or
predicates into attached-table queries, and restricts concurrent ADBC operations within one
process and mixed ADBC reads and writes within one statement. Put filters and projections in the
SQL passed to `read_adbc` when remote pushdown matters.

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
raised by the client transport closes the MAPI session, so the partially read connection cannot
be reused. Those terminal errors include the binary error detail
`adbc.monetdb.connection_terminal=true`. A server-reported SQLSTATE timeout or cancellation does
not carry that marker and leaves the session reusable when MonetDB does.

The URI names are `connect_timeout`, `read_timeout`, `write_timeout`, and
`operation_timeout`. Unknown query names are rejected so misspelled settings cannot be silently
ignored. This is the only configuration channel available to `pl.read_database_uri`:

```python
import polars as pl

df = pl.read_database_uri(
    "SELECT * FROM trades",
    "monetdb://localhost:50000/db?connect_timeout=10&operation_timeout=120",
    engine="adbc",
)
```

The same settings can be supplied separately through ADBC options. Database options override
the URI; connection options override the database defaults; statement options override the
connection for that statement.
`DatabaseOptions`, `ConnectionOptions`, and `StatementOptions` enumerate the supported keys.
`DatabaseOptionValues`, `ConnectionOptionValues`, and `StatementOptionValues` are exported
`TypedDict` shapes for applications that want editor completion for dictionary-based
configuration.

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

`pl.read_database(query, connection)` selects ADBC from the supplied DB-API connection or
cursor; it has no `engine="adbc"` parameter. Polars calls `connection.cursor()` without
`adbc_stmt_kwargs`, so use a preconfigured cursor for statement-specific settings. Its
`execute_options` are forwarded to `Cursor.execute`: use a sequence for positional `?` values
and a dictionary for named `:name` values.
`adbc_stmt_kwargs` is not an execute option. Polars' `batch_size` does not configure the driver,
and `DataFrame.write_database(..., engine_options=...)` supplies ingestion arguments rather than
connection or timeout options. Result windows start at 64 MiB on local connections and 128 MiB
when the measured round trip is at least 5 ms, capped by available host or cgroup memory. A
fixed-width result may use a larger guarded budget for one complete export granule; variable-width
estimates adapt from observed data. Set `adbc.monetdb.read_window_bytes` to choose another byte
target, or use `adbc.monetdb.read_batch_rows` as a diagnostic row override that disables byte
adaptation. Non-zero row overrides are rounded to a preferred export boundary with a warning, and
the effective value is returned by `get_option`. Both options are available on connections and
statements; `read_window_bytes` is also accepted in a URI.

Read prefetch is enabled by default and can hold up to about three byte-bounded windows at once:
one decoding, one buffered, and one in flight. Abandoning a stream can therefore waste up to two
fetched windows. Closing a reader waits briefly for its worker and then detaches a fetch that is
still in flight; it does not implicitly cancel and permanently close the session. The next
statement on that connection may wait for the detached fetch or its configured read/operation
timeout. Use `adbc_cancel()` when session destruction is intended, or set
`adbc.monetdb.read_prefetch` to `"false"` when prompt pool reuse matters more than fetch/decode
overlap. After a read, the read-only statement option `adbc.monetdb.read_stats` reports the chosen
budget, row and byte counts per window, observed row widths, prefetch use, and buffer reuse as JSON.

Appending to an existing table matches stream columns to destination columns by name,
case-insensitively, on every route and at every stream size. Exact spelling wins when a quoted
destination has columns that differ only by case. The stream may therefore present its columns in
any order, and it may supply a subset of the table's columns: a column it does not supply takes its
`DEFAULT`, or `NULL` when it has none — which the server rejects for a `NOT NULL` column. A stream
column that names no destination column, two stream columns that name the same destination column,
and a column whose type does not match its destination are all rejected before any data is sent.
Create, replace, and create-append modes that actually create the table still build it from the
stream's own schema.

Ingestion uses a 512 MiB logical encoded-byte window.
The driver measures every upstream Arrow batch, coalesces small batches, and splits large ones
without copying their buffers so each window stays within the byte budget. A single row larger
than the budget is the only possible overrun. Tiered compaction keeps metadata bounded without
repeatedly recopying the accumulated tail. Every automatic logical target is capped by a fraction
of the lower of the host's physical memory and a finite cgroup limit.

Each requested column is encoded directly into bounded 1 MiB encoder chunks, which the protocol
layer coalesces into 16 MiB upload messages; null-free signed integers and 32/64-bit floats borrow
their Arrow value buffers after validation instead of copying them. The protocol scatter-writes
MAPI headers with the borrowed message payload instead of constructing another message-sized
buffer, so the reported conservative streaming-buffer peak is about 17 MiB plus framing headers,
independent of logical window size.

Large logical windows do not imply equally large resident staging buffers. Arrow input is encoded
in groups of at most 16 MiB. Each new window samples up to 256 KiB per column and compares ordinary
LZ4 compression with byte-plane shuffle followed by LZ4 for fixed-width values. It retains the
smaller representation only when compression saves at least 12.5%. Selection follows the
observed bytes, not a type-name or sortedness heuristic; timestamp and smooth numeric byte
patterns consequently benefit while random values do not. `auto` and `none` both allow shuffled
client storage; `auto` sends frames directly only when every column independently qualifies as
plain LZ4. `lz4` forces the wire representation and therefore does not shuffle. During upload, at
most one 1 MiB encoded piece is decompressed and unshuffled into reusable anonymous mappings
before it is passed to the normal MonetDB binary protocol. Compression and shuffle reuse one
workspace on the prefetch thread. Finished compressed bytes are copied into 16 MiB anonymous
arena slabs owned by that window, bounding allocator metadata and avoiding one allocation or
mapping per column chunk. Client-only chunks use direct LZ4 blocks; wire-eligible chunks use LZ4
frames. Neither representation is an on-disk cache.

If a column stops meeting the savings threshold, it falls back to raw retention. For a window
that is not materially compressible, null-free batches may remain in Arrow form and be encoded in
bounded pieces on a worker while earlier pieces upload. Batches containing nulls stay in the raw
encoded form instead, avoiding deferred null-sentinel conversion on the upload path. Below the
5 ms remote-link threshold, incompressible windows close at 64 MiB and every window closes once
its physical retained storage reaches 128 MiB. Schemas with at least 512 columns use a tighter
48 MiB local physical limit, which also bounds their incompressible windows. Compressible data can
consequently fill more of the 512 MiB logical window without increasing the physical bound. Above
the threshold, both physical limits rise to the effective logical target to avoid repeating
per-column exchanges. The measured round trip, schema-sensitive physical limit, and all three
effective budgets are reported in ingest stats.

On Linux, a finite cgroup limit also protects concurrent ingests before their prefetch workers
allocate memory. Each active ingest reserves two physical windows, two staging groups, and 64 MiB
of producer headroom against 90% of the cgroup limit. A request that does not fit is returned as
an ADBC error; its connection is not killed by the cgroup OOM handler. Reservations are
process-wide and are released when the producer exits. If an operation timeout detaches a producer
that is still running, its reservation remains charged until that worker stops; releasing it
earlier would let a later ingest allocate against memory that is still in use. Admission failures
carry SQLSTATE `HY001`.

Compared with an application that stages complete column files on disk, a very wide,
incompressible stream may consequently produce more COPY windows and use more MonetDB memory while
the server consolidates or reloads unconstrained appends. Raising the window would retain more
Arrow data in client RAM; spilling the encoded columns would recreate the disk-staging path. The
automatic cap deliberately favors bounded client memory and no client-side staging files.
Compressible streams avoid this tradeoff because their large logical windows remain small in
physical memory.

COPY-sized appends to an existing table with a primary-key, unique, foreign-key, or check
constraint use an unconstrained staging table. Bounded windows are copied into that table, then
one `INSERT … SELECT` applies the complete stream to the real target and validates its constraints
once. MonetDB 11.55.7 and newer use a session-local table. Versions 11.55.0–11.55.6 use a uniquely
named transactional `UNLOGGED` table in the target schema because those releases can lose a local
temporary-table definition after a prepared statement; that compatibility path requires
`CREATE TABLE` in the target schema. The staging table is dropped on success and its ingest
savepoint removes it on failure. Temporary-table targets on those older releases remain direct.
Upgrade to 11.55.7 or set `adbc.monetdb.constrained_append=direct` when schema creation is not
available. Unconstrained targets and the small prepared-INSERT route remain direct. Set
`adbc.monetdb.constrained_append=direct` only to diagnose server behavior or when independent
measurements show that repeated target COPYs are preferable; `auto` is the general-purpose
default. The option is accepted at database, connection, and statement scope and as
`constrained_append` in a URI.

Staging temporarily materializes the incoming rows separately from the target, so its server
memory and disk peak can grow with the append. The `direct` setting avoids that duplicate state but
may spend substantially longer maintaining target constraints for every COPY window. Choose it
only from measurements on the intended server and workload.

`adbc.monetdb.write_window_bytes` changes the logical, physical, and incompressible-data budgets
together; zero selects the automatic defaults. `adbc.monetdb.write_batch_rows` remains a
diagnostic row-count override and disables byte-window adaptation. Both options can be set on a
connection or statement, with statement values taking precedence. `write_window_bytes` is also
accepted as a URI query parameter for applications whose only configuration surface is a
connection URI. Applications normally need none of these: producer batch boundaries do not
define COPY boundaries.

MonetDB Dec2025 and newer can decompress LZ4 frames during client COPY. The default
`adbc.monetdb.wire_compression=auto` sends a window's plain LZ4 frames on the wire only when every
column passed the compression threshold; otherwise it sends ordinary binary COPY from compressed,
raw, or retained-Arrow client storage. Byte-shuffled blocks remain client-local because the server
does not undo that transform.
Set the option to `lz4` to compress every column for a bandwidth-constrained link, or `none` to
disable wire compression while retaining the same client-storage probe. The
option is accepted at database, connection, and statement scope and as `wire_compression` in the
URI. Ingest stats report the choice for every window in `window_wire_compression`.

A complete, single-batch ingest of at most 100 rows uses one cached prepared INSERT script when
both its encoded input and rendered SQL fit conservative fixed limits. Rendering stops
incrementally at 8 MiB, and the input gate leaves room for string escaping and BLOB hex expansion.
This avoids one client-file exchange per column and is especially important for tiny wide
appends. The row threshold increases from the measured connection round trip on higher-latency
links, while the byte limits remain fixed. Set
`adbc.monetdb.ingest_insert_rows=0` to force COPY, or set a positive floor on the connection or
statement; the same key without the `adbc.monetdb.` prefix is accepted in a URI. An explicit
`write_batch_rows` setting continues to force the COPY scheduler. Explicit values supplied for an
identity column do not advance MonetDB's identity sequence on either INSERT or COPY; applications
mixing explicit identity values with generated ones must manage that sequence.

After an ingest, the read-only statement option `adbc.monetdb.ingest_stats` returns JSON containing
the chosen path, measured round trip, effective INSERT threshold, logical and physical stored
bytes, storage and wire-compression mode per window, prepared-cache hits, input-batch and COPY
counts, coalesced/split window counts, and transaction scope. `target_copy_count`,
`staging_copy_count`, and `final_move_count` distinguish writes to the real target from internal
staging work. Physical-memory fields include
per-window stored, staging, retained-Arrow pinned, and scratch bytes plus single-window and
prefetch-overlap high-water estimates.

Window construction runs one logical window ahead of the active COPY on a zero-capacity handoff.
This overlaps Arrow production and encoding with the current upload while bounding lookahead to
one window. Reader failures and worker panics reported by the prefetch worker are returned before
ingest completion. After any ingest error, a worker that has not stopped is detached rather than
joined indefinitely. During ingest streaming, the statement operation timeout is one deadline
across producer waits, COPY windows, and a staged final move. A detached producer cannot
indefinitely hold the connection lock; its cgroup memory reservation remains charged until the
worker exits.

For Parquet input, install `adbc-driver-monetdb[pyarrow]` and use
`ParquetArrowStream`. It decodes one physical row group at a time and bounds emitted batches by
rows and estimated bytes. Long streams ask PyArrow's allocator to return unused pages after every
2 GiB of decoded row-group data, and every stream does so when it finishes or closes. Set
`reclaim_bytes=None` to disable interval reclamation; reclaiming after each small row group can
materially reduce throughput. Path inputs use an owned PyArrow `OSFile` instead of mapping the
whole Parquet file. Column-parallel decoding is an explicit `use_threads=True` opt-in; the default
keeps decoding and the driver's compression worker from competing for CPU and memory.

```python
from adbc_driver_monetdb import ParquetArrowStream, dbapi

with dbapi.connect("monetdb://localhost:50000/db") as connection:
    with connection.cursor() as cursor:
        with ParquetArrowStream(
            "trades.parquet",
            epoch_columns={"observed_at": "ms"},
        ) as stream:
            inserted_rows = cursor.adbc_ingest("trades", stream, mode="append")
            assert inserted_rows == stream.num_rows
```

`epoch_columns` can reinterpret nullable integer columns as Arrow timestamps in seconds,
milliseconds, microseconds, or nanoseconds, or as day-based Arrow dates. Values and nulls are
preserved and transformed batch by batch, so files with integer epoch storage do not need an
eager Polars conversion. `row_groups` preserves the caller's order but rejects duplicates, which
prevents silently ingesting one physical row group twice.

`PolarsArrowStream` remains useful for non-Parquet lazy pipelines. Its capacity-one handoff is
backpressured, but Polars 1.43's Parquet source can decode ahead proportional to the dataset even
when the sink is blocked. `POLARS_ROW_GROUP_PREFETCH_SIZE=1` reduces that read-ahead but does not
make it strictly bounded; prefer `ParquetArrowStream` for Parquet until Polars exposes a source
memory budget ([Polars issue #28569](https://github.com/pola-rs/polars/issues/28569)). The base
driver remains importable without either optional dependency.

An unconstrained append to an existing table inside an explicit transaction executes directly for
every Arrow stream window. This avoids MonetDB retaining and replaying a full table version for an
operation savepoint. If a client-side stream or encoding error occurs after one or more completed
target COPY windows, subsequent reads remain available but `commit()` raises `InvalidState` until
`rollback()` removes the partial append. The same guard applies to raw SQL `COMMIT`, and a
successful raw SQL `ROLLBACK` clears it. A staged constrained append changes the target only in its
final move, so producer and constraint failures roll back to the ingest savepoint and preserve
earlier caller work. Server errors retain their DB-API exception and SQLSTATE. Autocommit
ingestion wraps the complete stream in an internal transaction, rolls it back on error, and
restores the connection.

Create and replace ingests inside a caller-managed transaction retain an operation savepoint so a
failure can preserve earlier caller work. MonetDB may retain more server storage for that safety;
use autocommit for an independent bulk load when it does not need to share the caller's transaction.

Two advanced statement or connection options change that contract. Setting
`adbc.monetdb.ingest_atomicity` to `"savepoint"` preserves earlier caller work and rolls back only
the failed ingest, but MonetDB Dec2025 may write an extra full copy of a sub-million-row append to
its WAL. The default `"transaction"` scope avoids that amplification and blocks commit after a
partial client-side failure. Setting `adbc.monetdb.ingest_partial` to `"allow"` permits committing
completed windows after such a failure; the default `"block"` is the safe behavior.

Positional prepared statements are cached per connection by exact SQL text, so consumers such as
SQLAlchemy can create a fresh cursor for each execution without making MonetDB compile the same
plan again. The least-recently-used cache holds 512 plans by default; set
`adbc.monetdb.prepared_cache_capacity` as a database/connection option or
`prepared_cache_capacity` in the URI when a workload needs a different positive bound. Eviction
queues server-side deallocation, and closing the connection releases the session and every
remaining plan.
Whitespace variants are intentionally different keys. Schema-changing statements issued through
the connection invalidate the cache, and externally invalidated plans are prepared again and
retried once when MonetDB can recover without rolling back user work. MonetDB aborts an explicit
transaction when `EXECUTE` reports a missing prepared plan, so a stale plan in that state follows
the normal database-error contract: roll back before retrying. One-row bound DML executes directly;
multi-row bound DML retains a savepoint so the whole parameter batch remains atomic.
When MonetDB cannot infer parameter or result types during `PREPARE`, including parameters in
function arguments and conditional result expressions, the driver uses its typed literal-binding
path for that statement. The fallback applies to server diagnostics rather than particular SQL
functions, and safely quotes values according to their Arrow types.

`dbapi.Binary` accepts bytes-like values (`bytes`, `bytearray`, and `memoryview`) and returns
`bytes`. Text is rejected with `TypeError`; encode text explicitly before binding it as binary.

### Tuning guide

Defaults are intended for mixed analytical workloads. Change a setting only with a workload-level
measurement; statement scope is preferable when one operation is exceptional.

| Option | Default | Scope | Tune when |
|---|---:|---|---|
| `read_window_bytes` | `0` (64 MiB local, 128 MiB at ≥5 ms) | database, connection, statement, URI | Set another result byte target for a measured memory/network constraint |
| `read_batch_rows` | `0` (disabled) | connection, statement | Diagnostic row override; it is normalized to an export boundary and disables byte adaptation |
| `read_prefetch` | `true` | connection, statement | Disable when promptly returning a connection to a pool matters more than fetch/decode overlap |
| `write_window_bytes` | `0` (adaptive) | database, connection, statement, URI | Set a byte budget only for measured memory/network constraints; it changes all write budgets together |
| `write_batch_rows` | `0` (disabled) | connection, statement | Diagnostic exact-row override; it disables byte adaptation and is not a normal production setting |
| `ingest_insert_rows` | 100, latency-adaptive | database, connection, statement, URI | Set `0` to compare/force COPY or raise a floor for measured high-latency tiny writes |
| `wire_compression` | `auto` | database, connection, statement, URI | Use `lz4` for bandwidth-bound links or `none` to rule out server decompression while keeping client storage compression |
| `constrained_append` | `auto` | database, connection, statement, URI | Use `direct` only to diagnose or benchmark repeated target COPY behavior |
| `ingest_atomicity` | `transaction` | connection, statement | Use `savepoint` for direct unconstrained appends when preserving prior caller work outweighs MonetDB WAL amplification |
| `ingest_partial` | `block` | connection, statement | Use `allow` only when committing completed direct target windows after a producer failure is intentional |
| `prepared_cache_capacity` | 512 | database, connection, URI | Adjust for a measured working set of distinct prepared SQL statements |
| `bind_by_name` | `false` | statement | Enable for Arrow parameters whose fields map to named `:parameter` slots; DB-API dictionaries do this automatically |

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
ADBC integer option path. `DatabaseOptionValues`, `ConnectionOptionValues`, and
`StatementOptionValues` are optional `TypedDict` shapes for editor completion of driver-specific
keys and accepted values; the corresponding `*Options` enums provide the runtime key constants.

The DB-API module reports `threadsafety = 1`: threads may share the module, but each thread should
use its own connection and cursors. Cancellation is connection-scoped: it interrupts the operation
currently using that connection, not necessarily the statement object on which `cancel` was called.
MAPI cannot safely resume a partially read response, so cancellation closes the session permanently;
close that connection and open another one before issuing more work.

## Client information

Client information is sent at login by default. The `client` value in
[`sys.sessions`](https://www.monetdb.org/documentation-Dec2025/user-guide/sql-catalog/users-roles-privileges-sessions/)
identifies this driver and its protocol library, for example
`adbc_driver_monetdb 0.11.3 / monetdb-rust 0.2.2-wlaur.1`. The Python shim uses the basename of
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
| Configurable connect/read/write/operation timeouts and cross-thread cancellation | Supported; terminal client failures carry a structured connection marker, while recoverable server SQLSTATE failures do not |
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
| INET (unsized) | Cast to `VARCHAR` in SQL | Not implemented | `NotImplemented` |
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
connection is created by the Python driver manager (which is still required by Polars' ADBC engine):

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

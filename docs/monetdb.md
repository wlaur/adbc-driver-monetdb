---
{}
---

{{ cross_reference|safe }}
# MonetDB Driver {{ version }}

{{ heading|safe }}

This Arrow Database Connectivity driver reads MonetDB's binary result protocol directly into
Arrow record batches and writes Arrow data with `COPY BINARY ... ON CLIENT`.

## Installation

Install the Python package for polars URI-scheme integration:

```console
$ uv add adbc-driver-monetdb
```

For standalone driver-manager use, download the archive for your platform from this version's
GitHub Release, verify it against `SHA256SUMS`, and install the unsigned local archive:

```console
$ uvx --from dbc dbc install --no-verify /path/to/monetdb_PLATFORM_v{{ version }}.tar.gz
```

## Connecting

```python
from adbc_driver_manager import dbapi

connection = dbapi.connect(
    driver="monetdb",
    db_kwargs={"uri": "monetdb://user:password@localhost:50000/db"},
)
```

The driver supports MonetDB Dec2025 (11.55) and newer on little-endian servers. URI credentials
may be replaced or overridden with the standard `username` and `password` database options.

## Timeouts and cancellation

The URI accepts `connect_timeout`, `read_timeout`, `write_timeout`, and `operation_timeout`, all in
integer seconds. The corresponding ADBC option names end in `_timeout_seconds`. Connect defaults
to 30 seconds, write defaults to 60 seconds, and read and operation timeouts default to disabled.
Zero disables any timeout explicitly.

Timeout and cancellation close the MAPI session. Cancellation is connection-scoped and may
interrupt whichever statement currently owns the connection; open a new connection afterward.

## Bulk ingestion

The driver measures and coalesces producer batches into COPY windows using a 512 MiB encoded-byte
budget, raised to 2 GiB for constrained append targets with fixed-width wire rows no larger than
16 bytes, then streams each requested column in bounded messages. Oversized producer batches are
split without copying, and a single row larger than the configured budget is the only possible
overrun. Producer batch size therefore does not need to match the driver window. This avoids
repeated index maintenance across tens of millions of narrow rows while wide, variable-width, and
ordinary append-only tables retain the smaller bound. In finite Linux cgroups, the ceilings are
reduced to one eighth and one quarter of the limit, respectively. The connection and statement
option `adbc.monetdb.write_window_bytes` changes the byte budget and the incompressible-data floor;
the diagnostic `adbc.monetdb.write_batch_rows` option forces an exact row count instead. On
automatic settings, measured network latency and very wide tables raise the 64 MiB incompressible
floor to avoid repeating per-column COPY exchanges and server-plan compilation. The local-width
cutoff is 512 columns, selected conservatively from a repeated 100–1,000-column calibration.

With `wire_compression=none`, each window samples up to 256 KiB per column and chooses between
ordinary LZ4 and byte-plane shuffle plus LZ4 from the observed bytes. `auto` probes plain LZ4 so a
profitable frame can go directly to the server, while `lz4` forces that representation.
Compressible streams can release their Arrow batches while filling the logical window.
Incompressible later batches stay as Arrow and are encoded in bounded pieces on a worker while
earlier pieces upload. Client-only retention uses direct LZ4 blocks; wire-eligible data uses LZ4
frames. The compressed form is never persisted.

The default `adbc.monetdb.wire_compression=auto` sends a window's plain LZ4 frames directly to
MonetDB only when every column already benefits; otherwise it retains Arrow and sends ordinary
binary COPY. `lz4` forces plain LZ4 for bandwidth-constrained links. `none` enables client-only
byte shuffling for a lower-memory retention path; shuffled blocks are decoded locally because
MonetDB does not reverse that transform. The same key is available as `wire_compression` in the
URI, and `window_wire_compression` in ingest stats makes every per-window decision observable.

A complete single-batch ingest of at most 100 rows and 8 MiB uses the cached prepared INSERT path;
the row threshold adapts upward on measured higher-latency connections. Set
`adbc.monetdb.ingest_insert_rows=0` to force COPY. The URI accepts `write_window_bytes`,
`ingest_insert_rows`, `prepared_cache_capacity`, and `wire_compression` for URI-only consumers.

In a caller-managed transaction, a client-side failure after completed COPY windows makes commit
fail until rollback, including through raw transaction SQL, so a partial append is not
accidentally committed. Advanced callers may set
`adbc.monetdb.ingest_atomicity=savepoint` to roll back only the ingest, or
`adbc.monetdb.ingest_partial=allow` to permit a partial commit. Statement option
`adbc.monetdb.ingest_stats` returns post-execution JSON including the chosen path, measured round
trip, effective thresholds, physical stored bytes, per-window storage and wire modes, and
prepared-cache hits.

## Client information

Client information is enabled by default. URI parameters `client_application`, `client_remark`,
and `client_info` map to the database options `adbc.monetdb.client_application`,
`adbc.monetdb.client_remark`, and `adbc.monetdb.client_info`. The session is visible in
`sys.sessions`; set `client_info=false` to suppress the hostname, process id, application, driver,
and remark values.

## Python TLS URI resolution

The wheel includes `adbc_driver_monetdbs` as a re-export of `adbc_driver_monetdb` because Polars
derives the Python module name `adbc_driver_<scheme>` from URI strings. This keeps `monetdbs://`
usable through Polars' URI convenience without defining a second ADBC driver,
distribution, native library, or DBC manifest. The standalone driver remains named `monetdb`;
ADBC treats driver loading and the `uri` database option separately.

## Feature and type support

{{ features|safe }}

### Types

{{ types|safe }}

MonetDB `JSON` query results use Arrow's canonical
[`arrow.json`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#json) extension with
UTF-8 string storage. Under the supported storage policy, Polars loads that storage as `String`;
it is not automatically expanded to `Struct` because rows may contain objects, arrays, scalars,
and JSON `null` with different shapes. Applications that know an object schema can explicitly use
`str.json_decode(dtype=pl.Struct(...))`.

Functions declared to return JSON—including `json.filter`, `json.keyarray`, and
`json.valuearray`—preserve `arrow.json`. Functions declared to return strings, numbers, integers,
or booleans use those scalar Arrow types. This matches
[MonetDB's validated-string JSON model](https://www.monetdb.org/documentation-Dec2025/user-guide/sql-manual/data-types/json-types/).

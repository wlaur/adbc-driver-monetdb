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

The driver coalesces producer batches into adaptive COPY windows using a 512 MiB encoded-byte
budget, then streams each requested column in bounded messages. Producer batch size therefore
does not need to match the driver window. The connection and statement option
`adbc.monetdb.write_window_bytes` changes the byte budget; the diagnostic
`adbc.monetdb.write_batch_rows` option forces an exact row count instead.

In a caller-managed transaction, a client-side failure after completed COPY windows makes commit
fail until rollback, so a partial append is not accidentally committed. Advanced callers may set
`adbc.monetdb.ingest_atomicity=savepoint` to roll back only the ingest, or
`adbc.monetdb.ingest_partial=allow` to permit a partial commit. Statement option
`adbc.monetdb.ingest_stats` returns post-execution JSON observability.

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

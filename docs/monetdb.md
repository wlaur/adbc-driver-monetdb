---
{}
---

{{ cross_reference|safe }}
# MonetDB Driver {{ version }}

{{ heading|safe }}

This Arrow Database Connectivity driver reads MonetDB's binary result protocol directly into
Arrow record batches and writes Arrow data with `COPY BINARY ... ON CLIENT`.

## Installation

Install the driver with `dbc install monetdb`, or install the `adbc-driver-monetdb` Python package
for polars URI-scheme integration.

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

## Feature and type support

{{ features|safe }}

### Types

{{ types|safe }}

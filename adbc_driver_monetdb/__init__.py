"""ADBC driver for MonetDB."""

from enum import StrEnum
from importlib.metadata import PackageNotFoundError, version
from typing import Literal, TypedDict

from adbc_driver_monetdb import _native
from adbc_driver_monetdb.arrow import (
    DEFAULT_ARROW_BATCH_BYTES,
    DEFAULT_ARROW_BATCH_ROWS,
    recommended_arrow_batch_rows,
)
from adbc_driver_monetdb.parquet import (
    DEFAULT_PARQUET_RECLAIM_BYTES,
    ParquetArrowStream,
    ParquetEpochUnit,
    ParquetSource,
)
from adbc_driver_monetdb.polars import (
    DEFAULT_POLARS_BATCH_BYTES,
    DEFAULT_POLARS_BATCH_ROWS,
    PolarsArrowStream,
)

try:
    __version__ = version("adbc-driver-monetdb")
except PackageNotFoundError:  # pragma: no cover - source checkout without an installed wheel
    __version__ = "0.0.0"

ENTRYPOINT = _native.__adbc_entrypoint__


class DatabaseOptions(StrEnum):
    USERNAME = "username"
    PASSWORD = "password"
    CONNECT_TIMEOUT = "adbc.monetdb.connect_timeout_seconds"
    READ_TIMEOUT = "adbc.monetdb.read_timeout_seconds"
    WRITE_TIMEOUT = "adbc.monetdb.write_timeout_seconds"
    OPERATION_TIMEOUT = "adbc.monetdb.operation_timeout_seconds"
    CLIENT_APPLICATION = "adbc.monetdb.client_application"
    CLIENT_REMARK = "adbc.monetdb.client_remark"
    CLIENT_INFO = "adbc.monetdb.client_info"
    WRITE_WINDOW_BYTES = "adbc.monetdb.write_window_bytes"
    INGEST_INSERT_ROWS = "adbc.monetdb.ingest_insert_rows"
    PREPARED_CACHE_CAPACITY = "adbc.monetdb.prepared_cache_capacity"
    WIRE_COMPRESSION = "adbc.monetdb.wire_compression"


class ConnectionOptions(StrEnum):
    READ_TIMEOUT = "adbc.monetdb.read_timeout_seconds"
    WRITE_TIMEOUT = "adbc.monetdb.write_timeout_seconds"
    OPERATION_TIMEOUT = "adbc.monetdb.operation_timeout_seconds"
    READ_BATCH_ROWS = "adbc.monetdb.read_batch_rows"
    READ_PREFETCH = "adbc.monetdb.read_prefetch"
    WRITE_BATCH_ROWS = "adbc.monetdb.write_batch_rows"
    WRITE_WINDOW_BYTES = "adbc.monetdb.write_window_bytes"
    INGEST_INSERT_ROWS = "adbc.monetdb.ingest_insert_rows"
    PREPARED_CACHE_CAPACITY = "adbc.monetdb.prepared_cache_capacity"
    WIRE_COMPRESSION = "adbc.monetdb.wire_compression"
    INGEST_PARTIAL = "adbc.monetdb.ingest_partial"
    INGEST_ATOMICITY = "adbc.monetdb.ingest_atomicity"


class StatementOptions(StrEnum):
    READ_TIMEOUT = "adbc.monetdb.read_timeout_seconds"
    WRITE_TIMEOUT = "adbc.monetdb.write_timeout_seconds"
    OPERATION_TIMEOUT = "adbc.monetdb.operation_timeout_seconds"
    READ_BATCH_ROWS = "adbc.monetdb.read_batch_rows"
    READ_PREFETCH = "adbc.monetdb.read_prefetch"
    WRITE_BATCH_ROWS = "adbc.monetdb.write_batch_rows"
    WRITE_WINDOW_BYTES = "adbc.monetdb.write_window_bytes"
    INGEST_INSERT_ROWS = "adbc.monetdb.ingest_insert_rows"
    WIRE_COMPRESSION = "adbc.monetdb.wire_compression"
    INGEST_PARTIAL = "adbc.monetdb.ingest_partial"
    INGEST_ATOMICITY = "adbc.monetdb.ingest_atomicity"
    INGEST_STATS = "adbc.monetdb.ingest_stats"


DatabaseOptionValues = TypedDict(
    "DatabaseOptionValues",
    {
        "username": str,
        "password": str,
        "adbc.monetdb.connect_timeout_seconds": str | int,
        "adbc.monetdb.read_timeout_seconds": str | int,
        "adbc.monetdb.write_timeout_seconds": str | int,
        "adbc.monetdb.operation_timeout_seconds": str | int,
        "adbc.monetdb.client_application": str,
        "adbc.monetdb.client_remark": str,
        "adbc.monetdb.client_info": str | bool,
        "adbc.monetdb.write_window_bytes": str | int,
        "adbc.monetdb.ingest_insert_rows": str | int,
        "adbc.monetdb.prepared_cache_capacity": str | int,
        "adbc.monetdb.wire_compression": Literal["none", "auto", "lz4"],
    },
    total=False,
)
ConnectionOptionValues = TypedDict(
    "ConnectionOptionValues",
    {
        "adbc.monetdb.read_timeout_seconds": str | int,
        "adbc.monetdb.write_timeout_seconds": str | int,
        "adbc.monetdb.operation_timeout_seconds": str | int,
        "adbc.monetdb.read_batch_rows": str | int,
        "adbc.monetdb.read_prefetch": str | bool,
        "adbc.monetdb.write_batch_rows": str | int,
        "adbc.monetdb.write_window_bytes": str | int,
        "adbc.monetdb.ingest_insert_rows": str | int,
        "adbc.monetdb.prepared_cache_capacity": str | int,
        "adbc.monetdb.wire_compression": Literal["none", "auto", "lz4"],
        "adbc.monetdb.ingest_partial": Literal["block", "allow"],
        "adbc.monetdb.ingest_atomicity": Literal["transaction", "savepoint"],
    },
    total=False,
)
StatementOptionValues = TypedDict(
    "StatementOptionValues",
    {
        "adbc.monetdb.read_timeout_seconds": str | int,
        "adbc.monetdb.write_timeout_seconds": str | int,
        "adbc.monetdb.operation_timeout_seconds": str | int,
        "adbc.monetdb.read_batch_rows": str | int,
        "adbc.monetdb.read_prefetch": str | bool,
        "adbc.monetdb.write_batch_rows": str | int,
        "adbc.monetdb.write_window_bytes": str | int,
        "adbc.monetdb.ingest_insert_rows": str | int,
        "adbc.monetdb.wire_compression": Literal["none", "auto", "lz4"],
        "adbc.monetdb.ingest_partial": Literal["block", "allow"],
        "adbc.monetdb.ingest_atomicity": Literal["transaction", "savepoint"],
    },
    total=False,
)


def driver_path() -> str:
    """Filesystem path of the compiled ADBC driver shared library.

    The driver is shipped as the extension module ``adbc_driver_monetdb._native``;
    the ADBC driver manager loads that file by path.
    """
    file = getattr(_native, "__file__", None)
    if not isinstance(file, str):
        raise RuntimeError("compiled driver module has no filesystem path")
    return file


__all__ = [
    "DEFAULT_ARROW_BATCH_BYTES",
    "DEFAULT_ARROW_BATCH_ROWS",
    "DEFAULT_PARQUET_RECLAIM_BYTES",
    "DEFAULT_POLARS_BATCH_BYTES",
    "DEFAULT_POLARS_BATCH_ROWS",
    "ENTRYPOINT",
    "ConnectionOptionValues",
    "ConnectionOptions",
    "DatabaseOptionValues",
    "DatabaseOptions",
    "ParquetArrowStream",
    "ParquetEpochUnit",
    "ParquetSource",
    "PolarsArrowStream",
    "StatementOptionValues",
    "StatementOptions",
    "__version__",
    "driver_path",
    "recommended_arrow_batch_rows",
]

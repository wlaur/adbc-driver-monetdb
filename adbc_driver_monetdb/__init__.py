"""ADBC driver for MonetDB."""

from enum import StrEnum
from importlib.metadata import PackageNotFoundError, version

from adbc_driver_monetdb import _native

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


class ConnectionOptions(StrEnum):
    READ_TIMEOUT = "adbc.monetdb.read_timeout_seconds"
    WRITE_TIMEOUT = "adbc.monetdb.write_timeout_seconds"
    OPERATION_TIMEOUT = "adbc.monetdb.operation_timeout_seconds"
    READ_BATCH_ROWS = "adbc.monetdb.read_batch_rows"
    WRITE_BATCH_ROWS = "adbc.monetdb.write_batch_rows"


class StatementOptions(StrEnum):
    READ_TIMEOUT = "adbc.monetdb.read_timeout_seconds"
    WRITE_TIMEOUT = "adbc.monetdb.write_timeout_seconds"
    OPERATION_TIMEOUT = "adbc.monetdb.operation_timeout_seconds"
    READ_BATCH_ROWS = "adbc.monetdb.read_batch_rows"
    WRITE_BATCH_ROWS = "adbc.monetdb.write_batch_rows"


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
    "ENTRYPOINT",
    "ConnectionOptions",
    "DatabaseOptions",
    "StatementOptions",
    "__version__",
    "driver_path",
]

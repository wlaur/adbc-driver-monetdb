"""PEP 249 (DB-API 2.0) interface to MonetDB, backed by adbc_driver_manager."""

import sys
from pathlib import Path
from urllib.parse import parse_qsl

from adbc_driver_manager import dbapi as _dbapi

from adbc_driver_monetdb import ENTRYPOINT, DatabaseOptions, driver_path

apilevel = _dbapi.apilevel
threadsafety = _dbapi.threadsafety
paramstyle = "qmark"

Warning = _dbapi.Warning
Error = _dbapi.Error
InterfaceError = _dbapi.InterfaceError
DatabaseError = _dbapi.DatabaseError
DataError = _dbapi.DataError
OperationalError = _dbapi.OperationalError
IntegrityError = _dbapi.IntegrityError
InternalError = _dbapi.InternalError
ProgrammingError = _dbapi.ProgrammingError
NotSupportedError = _dbapi.NotSupportedError

Date = _dbapi.Date
Time = _dbapi.Time
Timestamp = _dbapi.Timestamp
DateFromTicks = _dbapi.DateFromTicks
TimeFromTicks = _dbapi.TimeFromTicks
TimestampFromTicks = _dbapi.TimestampFromTicks
STRING = _dbapi.STRING
BINARY = _dbapi.BINARY
NUMBER = _dbapi.NUMBER
DATETIME = _dbapi.DATETIME
ROWID = _dbapi.ROWID
Connection = _dbapi.Connection
Cursor = _dbapi.Cursor


def connect(
    uri: str,
    /,
    *,
    autocommit: bool = False,
    db_kwargs: dict[str, str] | None = None,
    conn_kwargs: dict[str, str] | None = None,
) -> Connection:
    """Connect to MonetDB.

    ``uri`` is a ``monetdb://`` or ``monetdbs://`` URL, e.g.
    ``monetdb://user:password@localhost:50000/database``.
    """
    database_options = dict(db_kwargs or {})
    client_application = str(DatabaseOptions.CLIENT_APPLICATION)
    query = uri.partition("?")[2].partition("#")[0]
    uri_has_application = any(key == "client_application" for key, _ in parse_qsl(query, keep_blank_values=True))
    if client_application not in database_options and not uri_has_application:
        database_options[client_application] = Path(sys.argv[0]).name if sys.argv else ""
    database_options["uri"] = uri
    return _dbapi.connect(
        driver=driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs=database_options,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )


__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

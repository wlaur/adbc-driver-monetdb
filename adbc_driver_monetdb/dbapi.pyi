from collections.abc import Mapping
from typing import Literal, Self

import pyarrow as pa
from adbc_driver_manager.dbapi import (
    BINARY as BINARY,
    DATETIME as DATETIME,
    NUMBER as NUMBER,
    ROWID as ROWID,
    STRING as STRING,
    DataError as DataError,
    DatabaseError as DatabaseError,
    Date as Date,
    DateFromTicks as DateFromTicks,
    Error as Error,
    IntegrityError as IntegrityError,
    InterfaceError as InterfaceError,
    InternalError as InternalError,
    NotSupportedError as NotSupportedError,
    OperationalError as OperationalError,
    ProgrammingError as ProgrammingError,
    Time as Time,
    TimeFromTicks as TimeFromTicks,
    Timestamp as Timestamp,
    TimestampFromTicks as TimestampFromTicks,
    Warning as Warning,
)
from adbc_driver_manager.dbapi import Connection as _ManagerConnection
from adbc_driver_manager.dbapi import Cursor as _ManagerCursor
from adbc_driver_manager._lib import AdbcConnection as _ManagerAdbcConnection
from adbc_driver_manager._lib import AdbcStatement as _ManagerAdbcStatement

apilevel: str
threadsafety: int
paramstyle: Literal["qmark"]

class _AdbcConnection(_ManagerAdbcConnection):
    def get_option(
        self,
        key: bytes | str,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str: ...
    def set_options(self, **kwargs: bytes | float | int | str | bool | None) -> None: ...
    def set_autocommit(self, enabled: bool) -> None: ...

class _AdbcStatement(_ManagerAdbcStatement):
    def bind(self, data: object, schema: object | None = None) -> None: ...
    def execute_query(self) -> tuple[object, int]: ...
    def execute_update(self) -> int: ...
    def get_option_float(self, key: bytes | str) -> float: ...
    def prepare(self) -> None: ...
    def set_options(self, **kwargs: str) -> None: ...
    def set_sql_query(self, query: str) -> None: ...

class Cursor(_ManagerCursor):
    @property
    def description(self) -> list[tuple[object, ...]] | None: ...
    @property
    def adbc_statement(self) -> _AdbcStatement: ...
    def execute(self, operation: bytes | str, parameters: object = None) -> Self: ...
    def executemany(self, operation: bytes | str, seq_of_parameters: object) -> None: ...
    def fetchone(self) -> tuple[object, ...] | None: ...
    def fetchmany(self, size: int | None = None) -> list[tuple[object, ...]]: ...
    def fetchall(self) -> list[tuple[object, ...]]: ...
    def adbc_ingest(
        self,
        table_name: str,
        data: object,
        mode: Literal["append", "create", "replace", "create_append"] = "create",
        *,
        catalog_name: str | None = None,
        db_schema_name: str | None = None,
        temporary: bool = False,
    ) -> int: ...
    def adbc_execute_partitions(
        self,
        operation: bytes | str,
        parameters: object = None,
    ) -> tuple[list[bytes], pa.Schema]: ...
    def adbc_execute_schema(
        self,
        operation: bytes | str,
        parameters: object = None,
    ) -> pa.Schema: ...
    def adbc_prepare(self, operation: bytes | str) -> pa.Schema | None: ...

class Connection(_ManagerConnection):
    @property
    def adbc_connection(self) -> _AdbcConnection: ...
    def cursor(self, *, adbc_stmt_kwargs: Mapping[str, object] | None = None) -> Cursor: ...
    def execute(
        self,
        operation: bytes | str,
        parameters: object = None,
        *,
        adbc_stmt_kwargs: Mapping[str, object] | None = None,
    ) -> Cursor: ...
    def adbc_get_info(self) -> dict[str | int, object]: ...
    def adbc_get_objects(
        self,
        *,
        depth: Literal["all", "catalogs", "db_schemas", "tables", "columns"] = "all",
        catalog_filter: str | None = None,
        db_schema_filter: str | None = None,
        table_name_filter: str | None = None,
        table_types_filter: list[str] | None = None,
        column_name_filter: str | None = None,
    ) -> pa.RecordBatchReader: ...
    def adbc_get_statistics(
        self,
        *,
        catalog_filter: str | None = None,
        db_schema_filter: str | None = None,
        table_name_filter: str | None = None,
        approximate: bool = True,
    ) -> pa.RecordBatchReader: ...
    def adbc_get_statistic_names(self) -> pa.RecordBatchReader: ...
    def adbc_get_table_schema(
        self,
        table_name: str,
        *,
        catalog_filter: str | None = None,
        db_schema_filter: str | None = None,
    ) -> pa.Schema: ...
    def adbc_get_table_types(self) -> list[str]: ...

def connect(
    uri: str,
    /,
    *,
    autocommit: bool = False,
    db_kwargs: dict[str, str] | None = None,
    conn_kwargs: dict[str, str] | None = None,
) -> Connection: ...

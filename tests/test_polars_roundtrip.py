import gc
import os
import subprocess
import sys
import textwrap
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread
from typing import Protocol, cast
from uuid import UUID

import adbc_driver_manager
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from server_version import MONETDB_SERVER_VERSION
from sqlalchemy import Integer, bindparam, select
from sqlalchemy import cast as sql_cast

from adbc_driver_monetdb import StatementOptions, dbapi


class _ArrowField(Protocol):
    @property
    def metadata(self) -> dict[bytes, bytes] | None: ...


class _ArrowSchema(Protocol):
    def field(self, name: str) -> _ArrowField: ...


class _ArrowBatch(Protocol):
    def __arrow_c_array__(self) -> tuple[object, object]: ...


class _ArrowTable(Protocol):
    @property
    def schema(self) -> _ArrowSchema: ...


class _PyArrow(Protocol):
    def schema(self, fields: Sequence[object]) -> _ArrowSchema: ...

    def field(
        self,
        name: str,
        data_type: object,
        *,
        metadata: dict[bytes, bytes],
    ) -> object: ...

    def binary(self, width: int) -> object: ...

    def int32(self) -> object: ...

    def duration(self, unit: str) -> object: ...

    def uint64(self) -> object: ...

    def array(self, values: Sequence[object], *, type: object) -> object: ...

    def record_batch(self, arrays: Sequence[object], *, schema: _ArrowSchema) -> _ArrowBatch: ...


arrow = cast(_PyArrow, pa)


@pytest.mark.integration
def test_read_database(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        df = pl.read_database("SELECT 42 AS answer, 'monetdb' AS name", conn)
    assert df.shape == (1, 2)
    assert df.get_column("answer").item() == 42
    assert df.get_column("name").item() == "monetdb"


@pytest.mark.integration
def test_polars_uri_resolution_and_streaming_batches(monetdb_uri: str) -> None:
    frame = pl.read_database_uri("SELECT 42 AS answer", monetdb_uri, engine="adbc")
    assert frame.get_column("answer").item() == 42

    with dbapi.connect(monetdb_uri) as conn:
        batches = list(
            pl.read_database(
                "SELECT value FROM sys.generate_series(1, 300001)",
                conn,
                iter_batches=True,
            )
        )
    assert [batch.height for batch in batches] == [131_072, 131_072, 37_856]

    with dbapi.connect(monetdb_uri, db_kwargs={"uri": "not-the-positional-uri"}) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.integration
def test_polars_preconfigured_cursor_and_execute_options(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with connection.cursor(adbc_stmt_kwargs={StatementOptions.BATCH_ROWS: "2"}) as cursor:
            batches = list(
                pl.read_database(
                    "SELECT value FROM sys.generate_series(1, 6)",
                    cursor,
                    iter_batches=True,
                )
            )
        parameterized = pl.read_database(
            "SELECT CAST(? AS INT) AS value",
            connection,
            execute_options={"parameters": (42,)},
        )
    assert [batch.height for batch in batches] == [2, 2, 1]
    assert parameterized.get_column("value").to_list() == [42]


@pytest.mark.integration
def test_sqlalchemy_compiled_named_parameters(monetdb_uri: str) -> None:
    value = sql_cast(bindparam("value", value=21), Integer)
    compiled = select((value + value).label("value")).compile()
    assert compiled.params == {"value": 21}
    with dbapi.connect(monetdb_uri) as connection:
        frame = pl.read_database(
            str(compiled),
            connection,
            execute_options={"parameters": compiled.params},
        )
    assert frame.get_column("value").to_list() == [42]


@pytest.mark.integration
def test_one_row_query_executes_once(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("DROP SEQUENCE IF EXISTS adbc_single_execution")
            cursor.execute("CREATE SEQUENCE adbc_single_execution AS BIGINT START WITH 1")
            cursor.execute("SELECT NEXT VALUE FOR adbc_single_execution")
            assert cursor.fetchone() == (1,)
            cursor.execute("SELECT NEXT VALUE FOR adbc_single_execution")
            assert cursor.fetchone() == (2,)
        finally:
            cursor.execute("DROP SEQUENCE IF EXISTS adbc_single_execution")


@pytest.mark.integration
def test_one_row_all_supported_types(monetdb_uri: str) -> None:
    projection = r"""
        SELECT TRUE AS b,
               CAST(-7 AS TINYINT) AS i8,
               CAST(123456789012345678901234567890 AS HUGEINT) AS hi,
               CAST(-9 AS DECIMAL(2, 0)) AS d2,
               CAST(-1.25 AS DECIMAL(4, 2)) AS d4,
               CAST(1.23 AS DECIMAL(9, 2)) AS d9,
               CAST(123456789012.345678 AS DECIMAL(18, 6)) AS d18,
               CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38, 10)) AS d38,
               R'tail\' AS s,
               BLOB '00ff' AS blob_v,
               DATE '2038-01-19' AS date_v,
               TIME '23:59:59.123456' AS time_v,
               TIMESTAMP '2100-02-28 01:02:03.123456' AS ts_v,
               TIMESTAMPTZ '2025-01-01 00:30:00+01:00' AS tstz_v,
               INTERVAL '14' MONTH AS month_v,
               INTERVAL '1.234' SECOND AS sec_v,
               UUID '444fcb84-9a7d-4fe1-adfa-7eae290328c3' AS uuid_v,
               CAST('127.0.0.1' AS INET4) AS inet4_v,
               CAST('::1' AS INET6) AS inet6_v,
               JSON '{"x":"ä"}' AS json_v,
               URL 'https://example.com/ä' AS url_v,
               '' AS empty_v,
               CAST(NULL AS STRING) AS null_v
    """
    expected = (
        True,
        -7,
        Decimal("123456789012345678901234567890"),
        Decimal("-9"),
        Decimal("-1.25"),
        Decimal("1.23"),
        Decimal("123456789012.345678"),
        Decimal("1234567890123456789012345678.1234567890"),
        "tail\\",
        b"\x00\xff",
        date(2038, 1, 19),
        time(23, 59, 59, 123456),
        datetime(2100, 2, 28, 1, 2, 3, 123456),
        datetime(2024, 12, 31, 23, 30, tzinfo=UTC),
        14,
        timedelta(milliseconds=1234),
        UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3"),
        "127.0.0.1",
        "::1",
        '{"x":"ä"}',
        "https://example.com/ä",
        "",
        None,
    )
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM ({projection}) AS scalar_values WHERE FALSE")
        assert cursor.fetchall() == []
        zero_description = cast(object, cursor.description)

        cursor.execute(f"{projection} LIMIT 1")
        assert cursor.fetchone() == expected
        one_description = cast(object, cursor.description)

        cursor.execute(f"{projection} FROM sys.generate_series(1, 3) LIMIT 2")
        assert cursor.fetchall() == [expected, expected]
        two_description = cast(object, cursor.description)
        assert two_description == one_description == zero_description


@pytest.mark.integration
def test_long_variable_width_inline_and_binary(monetdb_uri: str) -> None:
    projection = "SELECT REPEAT('ä', 65537) AS text_v, CAST(REPEAT('ab', 65537) AS BLOB) AS blob_v"
    expected = ("ä" * 65_537, b"\xab" * 65_537)
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(f"{projection} LIMIT 1")
        assert cursor.fetchone() == expected

        cursor.execute(f"{projection} FROM sys.generate_series(1, 3) LIMIT 2")
        assert cursor.fetchall() == [expected, expected]


@pytest.mark.integration
def test_parameterless_reads_do_not_create_prepared_statements(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query("SELECT 42")
        cursor.adbc_statement.prepare()
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
        cursor.execute("SELECT COUNT(*) FROM sys.prepared_statements")
        assert cursor.fetchone() == (0,)


@pytest.mark.integration
def test_failed_authentication_does_not_poison_driver(monetdb_uri: str) -> None:
    wrong_password = monetdb_uri.replace(":monetdb@", ":definitely-wrong@")
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="login rejected") as rejected:
        dbapi.connect(wrong_password)
    assert rejected.value.status_code == adbc_driver_manager.AdbcStatusCode.UNAUTHENTICATED
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)


@pytest.mark.integration
def test_non_binary_type_error_recommends_cast(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        with pytest.raises(
            adbc_driver_manager.DataError,
            match=r"GEOMETRY.*cast the column to VARCHAR",
        ):
            conn.execute("SELECT ST_Point(1, 2) AS geom")
        casted = pl.read_database(
            "SELECT CAST(ST_Point(1, 2) AS VARCHAR(100)) AS geom",
            conn,
        )
    assert casted.get_column("geom").item() == "POINT (1 2)"


@pytest.mark.integration
def test_empty_and_null_results(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        empty = pl.read_database("SELECT CAST(NULL AS INT) AS value WHERE FALSE", conn)
        values = pl.read_database(
            "SELECT * FROM (VALUES (1, 'a'), (2, CAST(NULL AS VARCHAR(2))), (3, 'c')) AS t(i, s) ORDER BY i",
            conn,
        )
    assert empty.schema == {"value": pl.Int32}
    assert empty.height == 0
    assert values.to_dict(as_series=False) == {"i": [1, 2, 3], "s": ["a", None, "c"]}


@pytest.mark.integration
def test_metadata_and_schema_apis(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        info = conn.adbc_get_info()
        assert info["vendor_name"] == "MonetDB"
        assert info["vendor_version"] == MONETDB_SERVER_VERSION
        assert info["driver_name"] == "adbc-driver-monetdb"
        assert info["driver_adbc_version"] == 1_001_000
        assert "TABLE" in conn.adbc_get_table_types()
        assert "LOCAL TEMPORARY TABLE" in conn.adbc_get_table_types()
        table_schema = cast(
            object,
            conn.adbc_get_table_schema("table_types", db_schema_filter="sys"),
        )
        assert str(table_schema) == ("table_type_id: int16 not null\ntable_type_name: string not null")
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_timezone")
            assert cursor.fetchone() == (timedelta(0),)
            query_schema = cast(
                object,
                cursor.adbc_execute_schema("SELECT CAST(1 AS INT) AS value WHERE FALSE"),
            )
            assert str(query_schema) == "value: int32"
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="escape"):
                conn.adbc_get_objects(db_schema_filter="invalid\\")


@pytest.mark.integration
def test_clob_and_char_catalog_types_map_to_arrow_strings(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE catalog_strings(c CLOB, fixed CHAR(8))")
            schema = cast(
                object,
                conn.adbc_get_table_schema("catalog_strings"),
            )
            assert str(schema) == "c: string\nfixed: string"
        finally:
            cursor.execute("DROP TABLE IF EXISTS catalog_strings")


@pytest.mark.integration
def test_user_prepare_statement_has_an_actionable_error(monetdb_uri: str) -> None:
    with (
        dbapi.connect(monetdb_uri) as conn,
        conn.cursor() as cursor,
        pytest.raises(adbc_driver_manager.ProgrammingError, match=r"Statement::prepare"),
    ):
        cursor.execute("PREPARE SELECT 1")


@pytest.mark.integration
def test_declared_decimal_schema_is_not_narrowed_by_prepare_statistics(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE decimal_schema_probe (idx INT NOT NULL, value DECIMAL(10, 2))")
            cursor.execute("INSERT INTO decimal_schema_probe VALUES (1, 9999999.99)")
            table_schema = cast(
                object,
                conn.adbc_get_table_schema("decimal_schema_probe"),
            )
            assert str(table_schema) == "idx: int32 not null\nvalue: decimal128(10, 2)"
            query_schema = cast(
                object,
                cursor.adbc_execute_schema("SELECT value FROM decimal_schema_probe ORDER BY idx"),
            )
            assert str(query_schema) == "value: decimal128(10, 2)"
        finally:
            cursor.execute("DROP TABLE IF EXISTS decimal_schema_probe")


@pytest.mark.integration
def test_execute_schema_does_not_retype_an_aliased_cast(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE cast_alias_probe(value INT)")
            schema = cursor.adbc_execute_schema("SELECT CAST(value AS BIGINT) AS value FROM cast_alias_probe")
            assert str(schema) == "value: int64"
        finally:
            cursor.execute("DROP TABLE IF EXISTS cast_alias_probe")


@pytest.mark.integration
def test_execute_schema_has_no_side_effect_and_preserves_bind(monetdb_uri: str) -> None:
    query = "INSERT INTO execute_schema_probe VALUES (?)"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE execute_schema_probe(value INT)")
            schema = cast(
                object,
                cursor.adbc_execute_schema(query, [42]),
            )
            assert str(schema) == ""
            cursor.execute(query)
            cursor.execute("SELECT value FROM execute_schema_probe")
            assert cursor.fetchall() == [(42,)]
        finally:
            cursor.execute("DROP TABLE IF EXISTS execute_schema_probe")


@pytest.mark.integration
def test_untyped_parameter_fallback_is_eager_and_metadata_only(
    monetdb_uri: str,
) -> None:
    query = "INSERT INTO untyped_parameter_probe SELECT 1 FROM (SELECT ? AS ignored) AS parameter_row"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            schema = cast(
                object,
                cursor.adbc_execute_schema("SELECT ? AS value", [42]),
            )
            assert str(schema) == "value: null"
            mixed_schema = cast(
                object,
                cursor.adbc_execute_schema("SELECT ? AS unknown, CAST(1 AS INT) AS known", [42]),
            )
            assert str(mixed_schema) == "unknown: null\nknown: int32"
            cursor.execute("SELECT ? AS value")
            assert cursor.fetchone() == (42,)

            cursor.execute("CREATE TABLE untyped_parameter_probe(value INT)")
            parameters = pl.DataFrame({"ignored": ["a", "b", "c"]})
            cursor.execute(query, parameters)
            cursor.execute("SELECT COUNT(*) FROM untyped_parameter_probe")
            assert cursor.fetchone() == (3,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS untyped_parameter_probe")


@pytest.mark.integration
def test_get_objects_with_columns_constraints_and_filters(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE objects_parent(id INT PRIMARY KEY, label VARCHAR(10))")
            cursor.execute(
                "CREATE TABLE objects_child("
                "id INT, parent_id INT, "
                "CONSTRAINT objects_fk FOREIGN KEY(parent_id) REFERENCES objects_parent(id), "
                "CONSTRAINT objects_unique UNIQUE(id))"
            )
            rows = cast(
                list[object],
                conn.adbc_get_objects(
                    depth="all",
                    db_schema_filter="sys",
                    table_name_filter="objects_%",
                )
                .read_all()
                .to_pylist(),
            )
            rendered = repr(rows)
            assert "objects_parent_id_pkey" in rendered
            assert "objects_fk" in rendered
            assert "FOREIGN KEY" in rendered
            assert "objects_parent" in rendered
            assert "parent_id" in rendered
            assert "xdbc_data_type': 4" in rendered
            empty = cast(
                list[object],
                conn.adbc_get_objects(catalog_filter="missing_catalog").read_all().to_pylist(),
            )
            assert empty == []
        finally:
            cursor.execute("DROP TABLE IF EXISTS objects_child")
            cursor.execute("DROP TABLE IF EXISTS objects_parent")


@pytest.mark.integration
def test_prepared_parameters_and_executemany(monetdb_uri: str) -> None:
    malicious = "x'); DROP TABLE parameter_rows; --"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            parameter_schema = cast(
                object,
                cursor.adbc_prepare("SELECT ? + ?"),
            )
            rendered_parameter_schema = str(parameter_schema)
            assert "0: decimal128(38, 0)" in rendered_parameter_schema
            assert "1: decimal128(38, 0)" in rendered_parameter_schema
            assert rendered_parameter_schema.count("monetdb.hugeint") == 2
            cursor.execute("SELECT :value + :value", {"value": 21})
            assert cursor.fetchone() == (42,)
            cursor.execute(
                "SELECT ? AS i, '?' AS literal_qmark, ? AS s, ? AS b, "
                "CAST(? AS DECIMAL(9, 2)) AS d, ? AS dt, ? AS tm, ? AS ts, ? AS tstz, ? AS iv",
                [
                    42,
                    malicious,
                    b"\x00\xff",
                    Decimal("1.23"),
                    date(2025, 12, 31),
                    time(1, 2, 3, 123456),
                    datetime(2025, 12, 31, 1, 2, 3, 123456),
                    datetime(2025, 12, 31, 1, 2, 3, 123456, tzinfo=UTC),
                    timedelta(milliseconds=1234),
                ],
            )
            assert cursor.fetchone() == (
                42,
                "?",
                malicious,
                b"\x00\xff",
                Decimal("1.23"),
                date(2025, 12, 31),
                time(1, 2, 3, 123456),
                datetime(2025, 12, 31, 1, 2, 3, 123456),
                datetime(2025, 12, 31, 1, 2, 3, 123456, tzinfo=UTC),
                timedelta(milliseconds=1234),
            )
            cursor.execute("CREATE TABLE parameter_rows(i INT, s STRING)")
            cursor.executemany(
                "INSERT INTO parameter_rows VALUES (?, ?)",
                [(1, malicious), (2, None), (3, "?")],
            )
            cursor.executemany(
                "INSERT INTO parameter_rows VALUES (:i, :s)",
                [{"s": "named", "i": 4}, {"i": 5, "s": None}],
            )
            cursor.execute("SELECT i, s FROM parameter_rows ORDER BY i")
            assert cursor.fetchall() == [
                (1, malicious),
                (2, None),
                (3, "?"),
                (4, "named"),
                (5, None),
            ]
        finally:
            cursor.execute("DROP TABLE IF EXISTS parameter_rows")


@pytest.mark.integration
def test_native_prepared_statement_lifecycle(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        with conn.cursor() as prepared:
            schema = cast(
                object,
                prepared.adbc_prepare("SELECT 1 + ? AS value"),
            )
            assert "0: decimal128(38, 0)" in str(schema)
            assert "monetdb.hugeint" in str(schema)
            with conn.cursor() as audit:
                audit.execute("SELECT COUNT(*) FROM sys.prepared_statements")
                row = audit.fetchone()
                assert row is not None
                assert row[0] == 1
            prepared.execute("SELECT 1 + ? AS value", [41])
            row = prepared.fetchone()
            assert row is not None
            assert row[0] == 42
        with conn.cursor() as audit:
            audit.execute("SELECT COUNT(*) FROM sys.prepared_statements")
            row = audit.fetchone()
            assert row is not None
            assert row[0] == 0


@pytest.mark.integration
def test_parameterized_prepared_statement_requires_binding(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query("SELECT ?")
        cursor.adbc_statement.prepare()
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="parameters are not bound") as caught:
            cursor.adbc_statement.execute_query()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_STATE


@pytest.mark.integration
def test_query_replaces_ingest_statement_mode(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE query_after_ingest(value INT)")
            assert cursor.adbc_ingest(
                "previous_ingest_target",
                pa.record_batch({"value": [1]}),
                mode="create",
            ) == 1
            cursor.executemany("INSERT INTO query_after_ingest VALUES (?)", [(2,), (3,)])

            cursor.execute("SELECT value FROM previous_ingest_target")
            assert cursor.fetchall() == [(1,)]
            cursor.execute("SELECT value FROM query_after_ingest ORDER BY value")
            assert cursor.fetchall() == [(2,), (3,)]
        finally:
            cursor.execute("DROP TABLE IF EXISTS previous_ingest_target")
            cursor.execute("DROP TABLE IF EXISTS query_after_ingest")


@pytest.mark.integration
def test_ingest_rejects_stream_execution(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_options(**{"adbc.ingest.target_table": "unused"})
        cursor.adbc_statement.bind(pa.record_batch({"value": [1]}))
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="ingestion requires ExecuteUpdate") as caught:
            cursor.adbc_statement.execute_query()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_STATE


@pytest.mark.integration
def test_multi_statement_queries_are_rejected_and_update_scripts_are_split_safely(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE injection_multi_guard(value INT)")
            cursor.adbc_statement.set_sql_query("SELECT 1; DROP TABLE injection_multi_guard")
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="multiple SQL statements") as caught:
                cursor.adbc_statement.execute_query()
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
            cursor.execute("SELECT COUNT(*) FROM injection_multi_guard")
            assert cursor.fetchone() == (0,)

            cursor.adbc_statement.set_sql_query(
                "INSERT INTO injection_multi_guard VALUES (1); "
                "INSERT INTO injection_multi_guard VALUES (2 /* ; ÅÄÖ */); "
                "INSERT INTO injection_multi_guard VALUES (3) -- ; in comment\n;"
            )
            assert cursor.adbc_statement.execute_update() == 3
            cursor.execute("SELECT value FROM injection_multi_guard ORDER BY value")
            assert cursor.fetchall() == [(1,), (2,), (3,)]
        finally:
            cursor.execute("DROP TABLE IF EXISTS injection_multi_guard")


@pytest.mark.integration
def test_prepare_sql_cannot_be_smuggled_through_execute_update(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query(
            "CREATE TABLE prepare_smuggle_guard(value INT); /* hidden */ PrEpArE SELECT 1"
        )
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="does not accept a PREPARE"):
            cursor.adbc_statement.execute_update()
        with pytest.raises(adbc_driver_manager.Error):
            cursor.execute("SELECT * FROM prepare_smuggle_guard")


@pytest.mark.integration
def test_failed_prepare_recovers_user_transaction(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=False) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query("SELECT ? +")
        with pytest.raises(adbc_driver_manager.Error):
            cursor.adbc_statement.prepare()
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)
        conn.rollback()


@pytest.mark.integration
def test_write_database_roundtrip(monetdb_uri: str) -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [1.5, None, 3.0],
            "name": ["a", "b", None],
        }
    )
    with dbapi.connect(monetdb_uri) as conn:
        try:
            df.write_database("roundtrip_smoke", conn, if_table_exists="replace", engine="adbc")
            back = pl.read_database("SELECT id, value, name FROM roundtrip_smoke ORDER BY id", conn)
            assert back.equals(df)
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS roundtrip_smoke")


@pytest.mark.integration
def test_pandas_roundtrip(monetdb_uri: str) -> None:
    frame = pd.DataFrame({"id": [1, 2, 3], "name": ["a", None, "c"]})
    with dbapi.connect(monetdb_uri) as conn:
        try:
            assert (
                frame.to_sql(
                    "pandas_smoke",
                    conn,  # pyright: ignore[reportArgumentType]
                    if_exists="replace",
                    index=False,
                )
                == 3
            )
            result = pd.read_sql(
                "SELECT id, name FROM pandas_smoke ORDER BY id",
                conn,  # pyright: ignore[reportArgumentType]
                dtype_backend="pyarrow",
            )
            assert result.to_dict(orient="list") == {
                "id": [1, 2, 3],
                "name": ["a", None, "c"],
            }
        finally:
            conn.execute("DROP TABLE IF EXISTS pandas_smoke")


@pytest.mark.integration
def test_ingest_modes_and_temporary_table(monetdb_uri: str) -> None:
    first = pl.concat(
        [pl.DataFrame({"value": [1]}), pl.DataFrame({"value": [2]})],
        rechunk=False,
    )
    assert first.n_chunks() == 2
    second = pl.DataFrame({"value": [3]})
    with dbapi.connect(monetdb_uri, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS ingest_modes")
            cursor.execute("DROP TABLE IF EXISTS ingest_temporary")
        try:
            with conn.cursor() as cursor:
                assert cursor.adbc_ingest("ingest_modes", first, mode="create") == 2
                assert cursor.adbc_ingest("ingest_modes", second, mode="append") == 1
                assert cursor.adbc_ingest("ingest_modes", second, mode="create_append") == 1
                assert cursor.adbc_ingest("ingest_temporary", first, mode="create", temporary=True) == 2
            values = pl.read_database("SELECT value FROM ingest_modes ORDER BY value", conn)
            temporary = pl.read_database("SELECT value FROM ingest_temporary ORDER BY value", conn)
            assert values.get_column("value").to_list() == [1, 2, 3, 3]
            assert temporary.get_column("value").to_list() == [1, 2]
            assert first.write_database("ingest_modes", conn, if_table_exists="replace", engine="adbc") == 2
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS ingest_modes")
            conn.cursor().execute("DROP TABLE IF EXISTS ingest_temporary")


@pytest.mark.integration
def test_temporary_ingest_never_mutates_same_named_permanent_table(
    monetdb_uri: str,
) -> None:
    initial = pl.DataFrame({"value": [10]})
    replacement = pl.DataFrame({"value": [20, 30]})
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE ingest_collision(value INT)")
            cursor.execute("INSERT INTO ingest_collision VALUES (7)")

            with pytest.raises(adbc_driver_manager.Error):
                cursor.adbc_ingest(
                    "ingest_collision",
                    initial,
                    mode="append",
                    temporary=True,
                )
            cursor.execute("SELECT value FROM sys.ingest_collision")
            assert cursor.fetchone() == (7,)

            assert (
                cursor.adbc_ingest(
                    "ingest_collision",
                    initial,
                    mode="create_append",
                    temporary=True,
                )
                == 1
            )
            assert (
                cursor.adbc_ingest(
                    "ingest_collision",
                    replacement,
                    mode="replace",
                    temporary=True,
                )
                == 2
            )
            cursor.execute("SELECT value FROM tmp.ingest_collision ORDER BY value")
            assert cursor.fetchall() == [(20,), (30,)]
            cursor.execute("SELECT value FROM sys.ingest_collision")
            assert cursor.fetchone() == (7,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS tmp.ingest_collision")
            cursor.execute("DROP TABLE IF EXISTS sys.ingest_collision")


@pytest.mark.integration
def test_generated_sql_escapes_hostile_identifiers_and_metadata_filters(
    monetdb_uri: str,
) -> None:
    hostile_table = 'injected"; DROP TABLE injection_identifier_guard; --'
    quoted_table = '"' + hostile_table.replace('"', '""') + '"'
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE injection_identifier_guard(value INT)")
            assert (
                cursor.adbc_ingest(
                    hostile_table,
                    pl.DataFrame({"value": [42]}),
                    mode="create",
                )
                == 1
            )
            cursor.execute(f"SELECT value FROM {quoted_table}")
            assert cursor.fetchone() == (42,)

            rows = conn.adbc_get_objects(
                depth="tables",
                db_schema_filter="sys' OR 1=1 --",
                table_name_filter="missing' OR 1=1 --",
            ).read_all()
            assert rows.to_pylist() == [{"catalog_name": "test", "catalog_db_schemas": []}]
            cursor.execute("SELECT COUNT(*) FROM injection_identifier_guard")
            assert cursor.fetchone() == (0,)
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            cursor.execute("DROP TABLE IF EXISTS injection_identifier_guard")


@pytest.mark.integration
def test_nordic_characters_in_queries_identifiers_and_parameters(monetdb_uri: str) -> None:
    table = "räksmörgås_ÅÄÖ"
    quoted_table = '"räksmörgås_ÅÄÖ"'
    quoted_column = '"värde_åäö"'
    text = "ÅÄÖ åäö och blåbär"
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            assert (
                cursor.adbc_ingest(
                    table,
                    pl.DataFrame({"värde_åäö": [text]}),
                    mode="create",
                )
                == 1
            )
            cursor.execute(f"SELECT {quoted_column}, ? FROM {quoted_table}", [text])
            assert cursor.fetchone() == (text, text)
            objects = conn.adbc_get_objects(
                depth="tables",
                db_schema_filter="sys",
                table_name_filter=table,
            ).read_all()
            assert table in repr(objects.to_pylist())
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")


@pytest.mark.integration
def test_keyword_case_is_insensitive_without_changing_literals_or_labels(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.execute(
            'sElEcT ? AS "MiXeD_ÅÄÖ", \'SeLeCt ÅäÖ\' AS "LiTeRaL"',
            ["VaLuE_ÅäÖ"],
        )
        assert cursor.description is not None
        assert [column[0] for column in cursor.description] == [
            "MiXeD_ÅÄÖ",
            "LiTeRaL",
        ]
        assert cursor.fetchone() == ("VaLuE_ÅäÖ", "SeLeCt ÅäÖ")


@pytest.mark.integration
def test_massive_query_with_unicode_and_parameters(monetdb_uri: str) -> None:
    prefix = "/* BI generated ÅÄÖ | dimension=\"MiXeD_ÅÄÖ\" literal='SeLeCt ÅäÖ' | "
    suffix = '*/\nsElEcT ? AS "MiXeD_ÅÄÖ", \'SeLeCt ÅäÖ\' AS "LiTeRaL_ÅÄÖ"'
    query = prefix + ("predicate_ÅÄÖ = value_åäö | " * 150_000) + suffix
    assert len(query.encode()) > 4 * 1024 * 1024
    assert query.startswith(prefix)
    assert query.endswith(suffix)

    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.execute(query, ["VaLuE_ÅäÖ"])
        assert cursor.description is not None
        assert [column[0] for column in cursor.description] == [
            "MiXeD_ÅÄÖ",
            "LiTeRaL_ÅÄÖ",
        ]
        assert cursor.fetchone() == ("VaLuE_ÅäÖ", "SeLeCt ÅäÖ")


@pytest.mark.integration
def test_double_quoted_identifiers_are_case_sensitive(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute('CREATE TABLE quoted_case_probe("CaseSensitive_ÅÄÖ" INT)')
            cursor.execute("INSERT INTO quoted_case_probe VALUES (42)")
            cursor.execute('SELECT "CaseSensitive_ÅÄÖ" FROM quoted_case_probe')
            assert cursor.fetchone() == (42,)
            with pytest.raises(adbc_driver_manager.ProgrammingError) as caught:
                cursor.execute('SELECT "casesensitive_åäö" FROM quoted_case_probe')
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
            assert "identifier 'casesensitive_åäö' unknown" in str(caught.value)
        finally:
            cursor.execute("DROP TABLE IF EXISTS quoted_case_probe")


@pytest.mark.integration
def test_current_schema_and_unqualified_table_schema_are_live(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE SCHEMA live_schema")
            cursor.execute("CREATE TABLE live_schema.live_table(value INT)")
            cursor.execute("SET SCHEMA live_schema")
            assert conn.adbc_connection.get_option("adbc.connection.db_schema") == "live_schema"
            schema = conn.adbc_get_table_schema("live_table")
            assert schema.names == ["value"]
        finally:
            cursor.execute("SET SCHEMA sys")
            cursor.execute("DROP SCHEMA live_schema CASCADE")


@pytest.mark.integration
def test_append_rejects_destination_schema_mismatch_without_writes(monetdb_uri: str) -> None:
    frame = pl.DataFrame({"value": pl.Series([1, 2, 3], dtype=pl.Int32)})
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE append_mismatch(value SMALLINT)")
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="destination type is SMALLINT"):
                cursor.adbc_ingest("append_mismatch", frame, mode="append")
            cursor.execute("SELECT COUNT(*) FROM append_mismatch")
            assert cursor.fetchone() == (0,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS append_mismatch")


@pytest.mark.integration
def test_executemany_is_atomic_in_autocommit_mode(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE atomic_rows(value INT PRIMARY KEY)")
            with pytest.raises(adbc_driver_manager.Error):
                cursor.executemany(
                    "INSERT INTO atomic_rows VALUES (?)",
                    [(1,), (2,), (1,)],
                )
            cursor.execute("SELECT COUNT(*) FROM atomic_rows")
            assert cursor.fetchone() == (0,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS atomic_rows")


@pytest.mark.integration
def test_empty_parameter_streams_are_successful_noops(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE empty_parameters(value INT)")
            cursor.executemany("INSERT INTO empty_parameters VALUES (?)", [])
            cursor.execute("SELECT ? AS value", pl.DataFrame({"value": pl.Series([], dtype=pl.Int32)}))
            assert cursor.fetchall() == []
            assert cursor.description is not None
            assert [column[0] for column in cursor.description] == ["value"]
            cursor.execute("SELECT COUNT(*) FROM empty_parameters")
            assert cursor.fetchone() == (0,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS empty_parameters")


@pytest.mark.integration
def test_enabling_autocommit_commits_the_open_transaction(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE autocommit_transition(value INT)")
    try:
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO autocommit_transition VALUES (1)")
            conn.adbc_connection.set_autocommit(True)
            with dbapi.connect(monetdb_uri, autocommit=True) as audit, audit.cursor() as audit_cursor:
                audit_cursor.execute("SELECT COUNT(*) FROM autocommit_transition")
                assert audit_cursor.fetchone() == (1,)
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup, cleanup.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS autocommit_transition")


@pytest.mark.integration
def test_failed_ingest_uses_a_savepoint_inside_user_transaction(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE savepoint_prior(value INT)")
    try:
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO savepoint_prior VALUES (7)")
            poisoned = pl.DataFrame({"value": pl.Series([float("inf")], dtype=pl.Float64)})
            with pytest.raises(adbc_driver_manager.DataError, match="non-finite"):
                cursor.adbc_ingest("savepoint_target", poisoned, mode="replace")
            cursor.execute("SELECT value FROM savepoint_prior")
            assert cursor.fetchone() == (7,)
            conn.commit()
        with dbapi.connect(monetdb_uri, autocommit=True) as audit, audit.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM savepoint_prior")
            assert cursor.fetchone() == (1,)
            cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name = 'savepoint_target'")
            assert cursor.fetchone() == (0,)
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup, cleanup.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS savepoint_target")
            cursor.execute("DROP TABLE IF EXISTS savepoint_prior")


@pytest.mark.integration
def test_dbapi_default_transaction_rolls_back(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE rollback_probe(value INT)")
    try:
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO rollback_probe VALUES (1)")
            conn.rollback()
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM rollback_probe")
            assert cursor.fetchone() == (0,)
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup, cleanup.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS rollback_probe")


@pytest.mark.integration
def test_dtype_matrix_roundtrip(monetdb_uri: str) -> None:
    frame = pl.DataFrame(
        [
            pl.Series("bool", [True, None, False], dtype=pl.Boolean),
            pl.Series("i8", [1, None, -2], dtype=pl.Int8),
            pl.Series("i16", [1, None, -2], dtype=pl.Int16),
            pl.Series("i32", [1, None, -2], dtype=pl.Int32),
            pl.Series("i64", [1, None, -2], dtype=pl.Int64),
            pl.Series("u8", [1, None, 255], dtype=pl.UInt8),
            pl.Series("u16", [1, None, 65_535], dtype=pl.UInt16),
            pl.Series("u32", [1, None, 4_294_967_295], dtype=pl.UInt32),
            pl.Series("u64", [1, None, 18_446_744_073_709_551_615], dtype=pl.UInt64),
            pl.Series("f32", [1.5, None, -2.25], dtype=pl.Float32),
            pl.Series("f64", [1.5, None, -2.25], dtype=pl.Float64),
            pl.Series("str", ["a", None, "a"], dtype=pl.String),
            pl.Series("bin", [b"a", None, b""], dtype=pl.Binary),
            pl.Series("date", [date(1970, 1, 1), None, date(2025, 12, 31)], dtype=pl.Date),
            pl.Series("time", [time(1, 2, 3, 123456), None, time(23, 59, 59, 999999)], dtype=pl.Time),
            pl.Series(
                "ts",
                [datetime(1970, 1, 1, 1, 2, 3, 123456), None, datetime(2025, 12, 31, 23, 59, 59, 999999)],
                dtype=pl.Datetime("us"),
            ),
            pl.Series(
                "tstz",
                [
                    datetime(1970, 1, 1, 1, 2, 3, 123456, tzinfo=UTC),
                    None,
                    datetime(2025, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            ),
            pl.Series("dur", [timedelta(milliseconds=1234), None, timedelta(milliseconds=-5)], dtype=pl.Duration("ms")),
            pl.Series("dec", [Decimal("1.23"), None, Decimal("-4.56")], dtype=pl.Decimal(9, 2)),
        ]
    )
    expected = frame.with_columns(
        pl.col("u8").cast(pl.Int16),
        pl.col("u16").cast(pl.Int32),
        pl.col("u32").cast(pl.Int64),
        pl.col("u64").cast(pl.Decimal(38, 0)),
    )
    with dbapi.connect(monetdb_uri) as conn:
        try:
            assert frame.write_database("dtype_matrix", conn, if_table_exists="replace", engine="adbc") == 3
            back = pl.read_database("SELECT * FROM dtype_matrix", conn)
            assert back.equals(expected)
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS dtype_matrix")


@pytest.mark.integration
def test_non_finite_floats_are_rejected_on_write_and_bind(monetdb_uri: str) -> None:
    frame = pl.DataFrame(
        {
            "f32": pl.Series([float("nan"), None, 1.5], dtype=pl.Float32),
            "f64": pl.Series([float("nan"), None, 2.5], dtype=pl.Float64),
        }
    )
    with (
        dbapi.connect(monetdb_uri) as conn,
        pytest.raises(adbc_driver_manager.DataError, match="non-finite"),
    ):
        frame.write_database("nan_semantics", conn, if_table_exists="replace", engine="adbc")
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        for value in [float("nan"), float("inf"), float("-inf")]:
            with pytest.raises(adbc_driver_manager.DataError, match="non-finite"):
                cursor.execute("SELECT ?", [value])


@pytest.mark.integration
def test_extension_parameters_and_hugeint_timetz_append_back(monetdb_uri: str) -> None:
    extension = b"ARROW:extension:name"
    schema = arrow.schema(
        [
            arrow.field("u", arrow.binary(16), metadata={extension: b"arrow.uuid"}),
            arrow.field("m", arrow.int32(), metadata={extension: b"monetdb.interval_month"}),
            arrow.field("d", arrow.duration("ms"), metadata={extension: b"monetdb.interval_day"}),
            arrow.field("o", arrow.uint64(), metadata={extension: b"monetdb.oid"}),
        ]
    )
    batch = arrow.record_batch(
        [
            arrow.array([UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3").bytes], type=arrow.binary(16)),
            arrow.array([18], type=arrow.int32()),
            arrow.array([86_401_500], type=arrow.duration("ms")),
            arrow.array([42], type=arrow.uint64()),
        ],
        schema=schema,
    )
    ingest_schema = arrow.schema(
        [
            arrow.field("u", arrow.binary(16), metadata={extension: b"arrow.uuid"}),
            arrow.field("m", arrow.int32(), metadata={extension: b"monetdb.interval_month"}),
            arrow.field("d", arrow.duration("ms"), metadata={extension: b"monetdb.interval_day"}),
        ]
    )
    ingest_batch = arrow.record_batch(
        [
            arrow.array([UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3").bytes], type=arrow.binary(16)),
            arrow.array([18], type=arrow.int32()),
            arrow.array([86_400_000], type=arrow.duration("ms")),
        ],
        schema=ingest_schema,
    )
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE extension_bind(u UUID, m INTERVAL MONTH, d INTERVAL DAY, o OID)")
            cursor.execute("CREATE TABLE extension_ingest(u UUID, m INTERVAL MONTH, d INTERVAL DAY)")
            cursor.execute("CREATE TABLE extension_oid_ingest(u UUID, m INTERVAL MONTH, d INTERVAL DAY, o OID)")
            statement = cursor.adbc_statement
            statement.set_sql_query("INSERT INTO extension_bind VALUES (?, ?, ?, ?)")
            schema_capsule, array_capsule = batch.__arrow_c_array__()
            statement.bind(array_capsule, schema_capsule)
            assert statement.execute_update() == 1
            cursor.execute("SELECT u, m, d, o FROM extension_bind")
            assert cursor.fetchone() == (
                UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3"),
                18,
                timedelta(days=1, milliseconds=1_500),
                42,
            )
            cursor.execute("SELECT o FROM extension_bind")
            oid_table = cast(
                _ArrowTable,
                cursor.fetch_arrow_table(),
            )
            assert oid_table.schema.field("o").metadata == {extension: b"monetdb.oid"}
            assert cursor.adbc_ingest("extension_ingest", ingest_batch, mode="append") == 1
            cursor.execute("SELECT u, m, d FROM extension_ingest")
            assert cursor.fetchone() == (
                UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3"),
                18,
                timedelta(days=1),
            )
            with pytest.raises(adbc_driver_manager.NotSupportedError, match="OID"):
                cursor.adbc_ingest("extension_oid_ingest", batch, mode="append")

            cursor.execute("CREATE TABLE extension_source(h HUGEINT, t TIME WITH TIME ZONE)")
            cursor.execute("CREATE TABLE extension_dest(h HUGEINT, t TIME WITH TIME ZONE)")
            cursor.execute(
                "INSERT INTO extension_source VALUES "
                "(123456789012345678901234567890, TIMETZ '12:34:56.123456+00:00'), "
                "(-7, TIMETZ '01:02:03+00:00')"
            )
            cursor.execute("SELECT h, t FROM extension_source ORDER BY h")
            table = cast(
                _ArrowTable,
                cursor.fetch_arrow_table(),
            )
            assert table.schema.field("h").metadata == {extension: b"monetdb.hugeint"}
            assert table.schema.field("t").metadata == {extension: b"monetdb.timetz"}
            assert cursor.adbc_ingest("extension_dest", table, mode="append") == 2
            cursor.execute("SELECT CAST(h AS STRING) FROM extension_dest ORDER BY h")
            assert cursor.fetchall() == [
                ("-7",),
                ("123456789012345678901234567890",),
            ]
        finally:
            cursor.execute("DROP TABLE IF EXISTS extension_dest")
            cursor.execute("DROP TABLE IF EXISTS extension_source")
            cursor.execute("DROP TABLE IF EXISTS extension_ingest")
            cursor.execute("DROP TABLE IF EXISTS extension_oid_ingest")
            cursor.execute("DROP TABLE IF EXISTS extension_bind")


@pytest.mark.integration
def test_polars_future_unknown_extension_behavior(monetdb_uri: str) -> None:
    code = textwrap.dedent(
        """
        import os
        import polars as pl
        from adbc_driver_monetdb import dbapi

        with dbapi.connect(os.environ["MONETDB_TEST_URI"], autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS polars_future_extension_source")
                cursor.execute("DROP TABLE IF EXISTS polars_future_extension_target")
                cursor.execute(
                    "CREATE TABLE polars_future_extension_source(h HUGEINT, t TIME WITH TIME ZONE)"
                )
                cursor.execute(
                    "CREATE TABLE polars_future_extension_target(h HUGEINT, t TIME WITH TIME ZONE)"
                )
                cursor.execute(
                    "INSERT INTO polars_future_extension_source "
                    "VALUES (123456789012345678901234567890, TIMETZ '12:34:56.123456+00:00')"
                )
            try:
                frame = pl.read_database(
                    "SELECT h, t FROM polars_future_extension_source", connection
                )
                assert "monetdb.hugeint" in str(frame.schema["h"])
                assert "monetdb.timetz" in str(frame.schema["t"])
                assert frame.write_database(
                    "polars_future_extension_target",
                    connection,
                    if_table_exists="append",
                    engine="adbc",
                ) == 1
                back = pl.read_database(
                    "SELECT CAST(h AS STRING) AS h FROM polars_future_extension_target",
                    connection,
                )
                assert back.get_column("h").to_list() == ["123456789012345678901234567890"]
            finally:
                with connection.cursor() as cursor:
                    cursor.execute("DROP TABLE IF EXISTS polars_future_extension_target")
                    cursor.execute("DROP TABLE IF EXISTS polars_future_extension_source")
        print("future extension behavior passed")
        """
    )
    environment = os.environ.copy()
    environment["MONETDB_TEST_URI"] = monetdb_uri
    environment["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_extension"
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        env=environment,
    )
    assert "future extension behavior passed" in completed.stdout


@pytest.mark.integration
def test_variable_width_types_cross_batch_boundary(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        batches = list(
            pl.read_database(
                "SELECT CAST(value AS STRING) AS text, "
                "CAST(value AS DECIMAL(18, 2)) / 100 AS amount "
                "FROM sys.generate_series(1, 131074)",
                conn,
                iter_batches=True,
            )
        )
    assert [batch.height for batch in batches] == [131_072, 1]
    assert batches[0].row(0) == ("1", Decimal("0.01"))
    assert batches[1].row(0) == ("131073", Decimal("1310.73"))


@pytest.mark.integration
def test_categorical_and_enum_ingest(monetdb_uri: str) -> None:
    frame = pl.DataFrame(
        {
            "category": pl.Series(["a", "b", "a", None], dtype=pl.Categorical),
            "enum": pl.Series(["x", "y", "x", None], dtype=pl.Enum(["x", "y"])),
        }
    )
    with dbapi.connect(monetdb_uri) as conn:
        try:
            assert frame.write_database("categoricals", conn, if_table_exists="replace", engine="adbc") == 4
            back = pl.read_database("SELECT * FROM categoricals", conn)
            assert back.to_dict(as_series=False) == {
                "category": ["a", "b", "a", None],
                "enum": ["x", "y", "x", None],
            }
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS categoricals")


@pytest.mark.integration
def test_parallel_connections_and_interleaved_cursors(monetdb_uri: str) -> None:
    def separate_connection(value: int) -> int:
        with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT ? + 1", [value])
            row = cursor.fetchone()
            assert row is not None
            return cast(int, row[0])

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(separate_connection, range(8))) == list(range(1, 9))

    with dbapi.connect(monetdb_uri) as connection:

        def shared_connection(value: int) -> int:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT SUM(value) + ? FROM sys.generate_series(1, 1001)",
                    [value],
                )
                row = cursor.fetchone()
                assert row is not None
                return cast(int, row[0])

        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(shared_connection, range(8))) == [500_500 + value for value in range(8)]


@pytest.mark.integration
@pytest.mark.skipif(not Path("/proc/self/statm").exists(), reason="RSS assertion uses Linux procfs")
def test_wide_numeric_read_avoids_double_residency(monetdb_uri: str) -> None:
    def rss_bytes() -> int:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")

    columns = ", ".join(f"CAST(value + {index} AS REAL) AS v{index}" for index in range(256))
    with dbapi.connect(monetdb_uri) as conn:
        pl.read_database("SELECT 1", conn)
        gc.collect()
        baseline = rss_bytes()
        peak = baseline
        stop = Event()

        def sample() -> None:
            nonlocal peak
            while not stop.wait(0.001):
                peak = max(peak, rss_bytes())

        sampler = Thread(target=sample, daemon=True)
        sampler.start()
        try:
            frame = pl.read_database(
                f"SELECT {columns} FROM sys.generate_series(0, 40000)",
                conn,
            )
        finally:
            stop.set()
            sampler.join(timeout=5)
        assert not sampler.is_alive()
        assert frame.shape == (40_000, 256)
        materialized = frame.estimated_size()
        assert peak - baseline < materialized * 3 // 2 + 16 * 1024 * 1024


@pytest.mark.integration
@pytest.mark.skipif(not Path("/proc/self/statm").exists(), reason="RSS assertion uses Linux procfs")
def test_repeated_query_rss_is_bounded(monetdb_uri: str) -> None:
    def rss_bytes() -> int:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")

    with dbapi.connect(monetdb_uri) as conn:
        for _ in range(10):
            pl.read_database("SELECT value FROM sys.generate_series(1, 10001)", conn)
        gc.collect()
        baseline = rss_bytes()
        for _ in range(200):
            frame = pl.read_database(
                "SELECT value, CAST(value AS STRING) AS text FROM sys.generate_series(1, 10001)",
                conn,
            )
            assert frame.height == 10_000
        gc.collect()
        assert rss_bytes() - baseline < 64 * 1024 * 1024

import gc
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

import adbc_driver_manager
import pandas as pd
import polars as pl
import pytest

from adbc_driver_monetdb import dbapi


@pytest.mark.integration
def test_read_database(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        # polars' connection union references optional deps we don't install -> partially unknown
        df = pl.read_database("SELECT 42 AS answer, 'monetdb' AS name", conn)  # pyright: ignore[reportUnknownMemberType]
    assert df.shape == (1, 2)
    assert df.get_column("answer").item() == 42
    assert df.get_column("name").item() == "monetdb"


@pytest.mark.integration
def test_polars_uri_resolution_and_streaming_batches(monetdb_uri: str) -> None:
    frame = pl.read_database_uri("SELECT 42 AS answer", monetdb_uri, engine="adbc")
    assert frame.get_column("answer").item() == 42

    with dbapi.connect(monetdb_uri) as conn:
        batches = list(
            pl.read_database(  # pyright: ignore[reportUnknownMemberType]
                "SELECT value FROM sys.generate_series(1, 300001)",
                conn,
                iter_batches=True,
            )
        )
    assert [batch.height for batch in batches] == [131_072, 131_072, 37_856]


@pytest.mark.integration
def test_one_row_query_executes_once(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("DROP SEQUENCE IF EXISTS adbc_single_execution")  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("CREATE SEQUENCE adbc_single_execution AS BIGINT START WITH 1")  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("SELECT NEXT VALUE FOR adbc_single_execution")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (1,)  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("SELECT NEXT VALUE FOR adbc_single_execution")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (2,)  # pyright: ignore[reportUnknownMemberType]
        finally:
            cursor.execute("DROP SEQUENCE IF EXISTS adbc_single_execution")  # pyright: ignore[reportUnknownMemberType]


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
        cursor.execute(  # pyright: ignore[reportUnknownMemberType]
            f"SELECT * FROM ({projection}) AS scalar_values WHERE FALSE"
        )
        assert cursor.fetchall() == []  # pyright: ignore[reportUnknownMemberType]
        zero_description = cast(object, cursor.description)  # pyright: ignore[reportUnknownMemberType]

        cursor.execute(f"{projection} LIMIT 1")  # pyright: ignore[reportUnknownMemberType]
        assert cursor.fetchone() == expected  # pyright: ignore[reportUnknownMemberType]
        one_description = cast(object, cursor.description)  # pyright: ignore[reportUnknownMemberType]

        cursor.execute(  # pyright: ignore[reportUnknownMemberType]
            f"{projection} FROM sys.generate_series(1, 3) LIMIT 2"
        )
        assert cursor.fetchall() == [expected, expected]  # pyright: ignore[reportUnknownMemberType]
        two_description = cast(object, cursor.description)  # pyright: ignore[reportUnknownMemberType]
        assert two_description == one_description == zero_description


@pytest.mark.integration
def test_long_variable_width_inline_and_binary(monetdb_uri: str) -> None:
    projection = "SELECT REPEAT('ä', 65537) AS text_v, CAST(REPEAT('ab', 65537) AS BLOB) AS blob_v"
    expected = ("ä" * 65_537, b"\xab" * 65_537)
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(f"{projection} LIMIT 1")  # pyright: ignore[reportUnknownMemberType]
        assert cursor.fetchone() == expected  # pyright: ignore[reportUnknownMemberType]

        cursor.execute(  # pyright: ignore[reportUnknownMemberType]
            f"{projection} FROM sys.generate_series(1, 3) LIMIT 2"
        )
        assert cursor.fetchall() == [expected, expected]  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_parameterless_reads_do_not_create_prepared_statements(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query("SELECT 42")  # pyright: ignore[reportUnknownMemberType]
        cursor.adbc_statement.prepare()  # pyright: ignore[reportUnknownMemberType]
        cursor.execute("SELECT 42")  # pyright: ignore[reportUnknownMemberType]
        assert cursor.fetchone() == (42,)  # pyright: ignore[reportUnknownMemberType]
        cursor.execute("SELECT COUNT(*) FROM sys.prepared_statements")  # pyright: ignore[reportUnknownMemberType]
        assert cursor.fetchone() == (0,)  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_failed_authentication_does_not_poison_driver(monetdb_uri: str) -> None:
    wrong_password = monetdb_uri.replace(":monetdb@", ":definitely-wrong@")
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="login rejected"):
        dbapi.connect(wrong_password)
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT 1")  # pyright: ignore[reportUnknownMemberType]
        assert cursor.fetchone() == (1,)  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_non_binary_type_error_recommends_cast(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        with pytest.raises(
            adbc_driver_manager.DataError,
            match=r"GEOMETRY.*cast the column to VARCHAR",
        ):
            conn.execute("SELECT ST_Point(1, 2) AS geom")  # pyright: ignore[reportUnknownMemberType]
        casted = pl.read_database(  # pyright: ignore[reportUnknownMemberType]
            "SELECT CAST(ST_Point(1, 2) AS VARCHAR(100)) AS geom",
            conn,
        )
    assert casted.get_column("geom").item() == "POINT (1 2)"


@pytest.mark.integration
def test_empty_and_null_results(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        empty = pl.read_database("SELECT CAST(NULL AS INT) AS value WHERE FALSE", conn)  # pyright: ignore[reportUnknownMemberType]
        values = pl.read_database(  # pyright: ignore[reportUnknownMemberType]
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
        assert info["vendor_version"] == "11.55.7"
        assert info["driver_name"] == "adbc-driver-monetdb"
        assert info["driver_adbc_version"] == 1_001_000
        assert "TABLE" in conn.adbc_get_table_types()
        assert "LOCAL TEMPORARY TABLE" in conn.adbc_get_table_types()
        table_schema = cast(
            object,
            conn.adbc_get_table_schema(  # pyright: ignore[reportUnknownMemberType]
                "table_types", db_schema_filter="sys"
            ),
        )
        assert str(table_schema) == ("table_type_id: int16 not null\ntable_type_name: string not null")
        with conn.cursor() as cursor:
            query_schema = cast(
                object,
                cursor.adbc_execute_schema(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT CAST(1 AS INT) AS value WHERE FALSE"
                ),
            )
            assert str(query_schema) == "value: int32"


@pytest.mark.integration
def test_declared_decimal_schema_is_not_narrowed_by_prepare_statistics(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                "CREATE TABLE decimal_schema_probe (idx INT NOT NULL, value DECIMAL(10, 2))"
            )
            cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                "INSERT INTO decimal_schema_probe VALUES (1, 9999999.99)"
            )
            table_schema = cast(
                object,
                conn.adbc_get_table_schema("decimal_schema_probe"),  # pyright: ignore[reportUnknownMemberType]
            )
            assert str(table_schema) == "idx: int32 not null\nvalue: decimal128(10, 2)"
            query_schema = cast(
                object,
                cursor.adbc_execute_schema(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT value FROM decimal_schema_probe ORDER BY idx"
                ),
            )
            assert str(query_schema) == "value: decimal128(10, 2)"
        finally:
            cursor.execute("DROP TABLE IF EXISTS decimal_schema_probe")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_execute_schema_has_no_side_effect_and_preserves_bind(monetdb_uri: str) -> None:
    query = "INSERT INTO execute_schema_probe VALUES (?)"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE execute_schema_probe(value INT)")  # pyright: ignore[reportUnknownMemberType]
            schema = cast(
                object,
                cursor.adbc_execute_schema(query, [42]),  # pyright: ignore[reportUnknownMemberType]
            )
            assert str(schema) == ""
            cursor.execute(query)  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("SELECT value FROM execute_schema_probe")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchall() == [(42,)]  # pyright: ignore[reportUnknownMemberType]
        finally:
            cursor.execute("DROP TABLE IF EXISTS execute_schema_probe")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_untyped_parameter_fallback_is_eager_and_metadata_only(
    monetdb_uri: str,
) -> None:
    query = "INSERT INTO untyped_parameter_probe SELECT 1 FROM (SELECT ? AS ignored) AS parameter_row"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            schema = cast(
                object,
                cursor.adbc_execute_schema(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT ? AS value", [42]
                ),
            )
            assert str(schema) == "value: null"
            cursor.execute("SELECT ? AS value")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (42,)  # pyright: ignore[reportUnknownMemberType]

            cursor.execute("CREATE TABLE untyped_parameter_probe(value INT)")  # pyright: ignore[reportUnknownMemberType]
            parameters = pl.DataFrame({"ignored": ["a", "b", "c"]})
            cursor.execute(query, parameters)  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("SELECT COUNT(*) FROM untyped_parameter_probe")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (3,)  # pyright: ignore[reportUnknownMemberType]
        finally:
            cursor.execute("DROP TABLE IF EXISTS untyped_parameter_probe")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_get_objects_with_columns_constraints_and_filters(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                "CREATE TABLE objects_parent(id INT PRIMARY KEY, label VARCHAR(10))"
            )
            cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                "CREATE TABLE objects_child("
                "id INT, parent_id INT, "
                "CONSTRAINT objects_fk FOREIGN KEY(parent_id) REFERENCES objects_parent(id), "
                "CONSTRAINT objects_unique UNIQUE(id))"
            )
            rows = cast(
                list[object],
                conn.adbc_get_objects(  # pyright: ignore[reportUnknownMemberType]
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
                conn.adbc_get_objects(  # pyright: ignore[reportUnknownMemberType]
                    catalog_filter="missing_catalog"
                )
                .read_all()
                .to_pylist(),
            )
            assert empty == []
        finally:
            cursor.execute("DROP TABLE IF EXISTS objects_child")  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("DROP TABLE IF EXISTS objects_parent")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_prepared_parameters_and_executemany(monetdb_uri: str) -> None:
    malicious = "x'); DROP TABLE parameter_rows; --"
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            parameter_schema = cast(
                object,
                cursor.adbc_prepare("SELECT ? + ?"),  # pyright: ignore[reportUnknownMemberType]
            )
            assert str(parameter_schema) == "0: decimal128(38, 0)\n1: decimal128(38, 0)"
            cursor.execute(  # pyright: ignore[reportUnknownMemberType]
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
            assert cursor.fetchone() == (  # pyright: ignore[reportUnknownMemberType]
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
            cursor.execute("CREATE TABLE parameter_rows(i INT, s STRING)")  # pyright: ignore[reportUnknownMemberType]
            cursor.executemany(  # pyright: ignore[reportUnknownMemberType]
                "INSERT INTO parameter_rows VALUES (?, ?)",
                [(1, malicious), (2, None), (3, "?")],
            )
            cursor.execute("SELECT i, s FROM parameter_rows ORDER BY i")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchall() == [  # pyright: ignore[reportUnknownMemberType]
                (1, malicious),
                (2, None),
                (3, "?"),
            ]
        finally:
            cursor.execute("DROP TABLE IF EXISTS parameter_rows")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_native_prepared_statement_lifecycle(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        with conn.cursor() as prepared:
            schema = cast(
                object,
                prepared.adbc_prepare("SELECT 1 + ? AS value"),  # pyright: ignore[reportUnknownMemberType]
            )
            assert str(schema) == "0: decimal128(38, 0)"
            with conn.cursor() as audit:
                audit.execute("SELECT COUNT(*) FROM sys.prepared_statements")  # pyright: ignore[reportUnknownMemberType]
                row = cast(
                    tuple[object, ...] | None,
                    audit.fetchone(),  # pyright: ignore[reportUnknownMemberType]
                )
                assert row is not None
                assert row[0] == 1
            prepared.execute("SELECT 1 + ? AS value", [41])  # pyright: ignore[reportUnknownMemberType]
            row = cast(
                tuple[object, ...] | None,
                prepared.fetchone(),  # pyright: ignore[reportUnknownMemberType]
            )
            assert row is not None
            assert row[0] == 42
        with conn.cursor() as audit:
            audit.execute("SELECT COUNT(*) FROM sys.prepared_statements")  # pyright: ignore[reportUnknownMemberType]
            row = cast(
                tuple[object, ...] | None,
                audit.fetchone(),  # pyright: ignore[reportUnknownMemberType]
            )
            assert row is not None
            assert row[0] == 0


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
            df.write_database("roundtrip_smoke", conn, if_table_exists="replace", engine="adbc")  # pyright: ignore[reportUnknownMemberType]
            back = pl.read_database(  # pyright: ignore[reportUnknownMemberType]
                "SELECT id, value, name FROM roundtrip_smoke ORDER BY id", conn
            )
            assert back.equals(df)
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS roundtrip_smoke")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_pandas_roundtrip(monetdb_uri: str) -> None:
    frame = pd.DataFrame({"id": [1, 2, 3], "name": ["a", None, "c"]})
    with dbapi.connect(monetdb_uri) as conn:
        try:
            assert (
                frame.to_sql(  # pyright: ignore[reportUnknownMemberType]
                    "pandas_smoke", conn, if_exists="replace", index=False
                )
                == 3
            )
            result = pd.read_sql(  # pyright: ignore[reportUnknownMemberType]
                "SELECT id, name FROM pandas_smoke ORDER BY id",
                conn,
                dtype_backend="pyarrow",
            )
            assert result.to_dict(orient="list") == {
                "id": [1, 2, 3],
                "name": ["a", None, "c"],
            }
        finally:
            conn.execute("DROP TABLE IF EXISTS pandas_smoke")  # pyright: ignore[reportUnknownMemberType]


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
            cursor.execute("DROP TABLE IF EXISTS ingest_modes")  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("DROP TABLE IF EXISTS ingest_temporary")  # pyright: ignore[reportUnknownMemberType]
        try:
            with conn.cursor() as cursor:
                assert cursor.adbc_ingest("ingest_modes", first, mode="create") == 2  # pyright: ignore[reportUnknownMemberType]
                assert cursor.adbc_ingest("ingest_modes", second, mode="append") == 1  # pyright: ignore[reportUnknownMemberType]
                assert cursor.adbc_ingest("ingest_modes", second, mode="create_append") == 1  # pyright: ignore[reportUnknownMemberType]
                assert cursor.adbc_ingest("ingest_temporary", first, mode="create", temporary=True) == 2  # pyright: ignore[reportUnknownMemberType]
            values = pl.read_database("SELECT value FROM ingest_modes ORDER BY value", conn)  # pyright: ignore[reportUnknownMemberType]
            temporary = pl.read_database("SELECT value FROM ingest_temporary ORDER BY value", conn)  # pyright: ignore[reportUnknownMemberType]
            assert values.get_column("value").to_list() == [1, 2, 3, 3]
            assert temporary.get_column("value").to_list() == [1, 2]
            assert first.write_database("ingest_modes", conn, if_table_exists="replace", engine="adbc") == 2  # pyright: ignore[reportUnknownMemberType]
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS ingest_modes")  # pyright: ignore[reportUnknownMemberType]
            conn.cursor().execute("DROP TABLE IF EXISTS ingest_temporary")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_append_rejects_destination_schema_mismatch_without_writes(monetdb_uri: str) -> None:
    frame = pl.DataFrame({"value": pl.Series([1, 2, 3], dtype=pl.Int32)})
    with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE append_mismatch(value SMALLINT)")  # pyright: ignore[reportUnknownMemberType]
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="destination type is SMALLINT"):
                cursor.adbc_ingest("append_mismatch", frame, mode="append")  # pyright: ignore[reportUnknownMemberType]
            cursor.execute("SELECT COUNT(*) FROM append_mismatch")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (0,)  # pyright: ignore[reportUnknownMemberType]
        finally:
            cursor.execute("DROP TABLE IF EXISTS append_mismatch")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_executemany_is_atomic_in_autocommit_mode(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as conn, conn.cursor() as cursor:
        try:
            cursor.execute("CREATE TABLE atomic_rows(value INT PRIMARY KEY)")  # pyright: ignore[reportUnknownMemberType]
            with pytest.raises(adbc_driver_manager.Error):
                cursor.executemany(  # pyright: ignore[reportUnknownMemberType]
                    "INSERT INTO atomic_rows VALUES (?)",
                    [(1,), (2,), (1,)],
                )
            cursor.execute("SELECT COUNT(*) FROM atomic_rows")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (0,)  # pyright: ignore[reportUnknownMemberType]
        finally:
            cursor.execute("DROP TABLE IF EXISTS atomic_rows")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_dbapi_default_transaction_rolls_back(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup, setup.cursor() as cursor:
        cursor.execute("CREATE TABLE rollback_probe(value INT)")  # pyright: ignore[reportUnknownMemberType]
    try:
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("INSERT INTO rollback_probe VALUES (1)")  # pyright: ignore[reportUnknownMemberType]
            conn.rollback()
        with dbapi.connect(monetdb_uri) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM rollback_probe")  # pyright: ignore[reportUnknownMemberType]
            assert cursor.fetchone() == (0,)  # pyright: ignore[reportUnknownMemberType]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup, cleanup.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS rollback_probe")  # pyright: ignore[reportUnknownMemberType]


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
            assert frame.write_database("dtype_matrix", conn, if_table_exists="replace", engine="adbc") == 3  # pyright: ignore[reportUnknownMemberType]
            back = pl.read_database("SELECT * FROM dtype_matrix", conn)  # pyright: ignore[reportUnknownMemberType]
            assert back.equals(expected)
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS dtype_matrix")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_float_nan_is_rejected_on_write(monetdb_uri: str) -> None:
    frame = pl.DataFrame(
        {
            "f32": pl.Series([float("nan"), None, 1.5], dtype=pl.Float32),
            "f64": pl.Series([float("nan"), None, 2.5], dtype=pl.Float64),
        }
    )
    with (
        dbapi.connect(monetdb_uri) as conn,
        pytest.raises(adbc_driver_manager.DataError, match="NaN is MonetDB"),
    ):
        frame.write_database("nan_semantics", conn, if_table_exists="replace", engine="adbc")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_variable_width_types_cross_batch_boundary(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        batches = list(
            pl.read_database(  # pyright: ignore[reportUnknownMemberType]
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
            assert frame.write_database("categoricals", conn, if_table_exists="replace", engine="adbc") == 4  # pyright: ignore[reportUnknownMemberType]
            back = pl.read_database("SELECT * FROM categoricals", conn)  # pyright: ignore[reportUnknownMemberType]
            assert back.to_dict(as_series=False) == {
                "category": ["a", "b", "a", None],
                "enum": ["x", "y", "x", None],
            }
        finally:
            conn.cursor().execute("DROP TABLE IF EXISTS categoricals")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
def test_parallel_connections_and_interleaved_cursors(monetdb_uri: str) -> None:
    def separate_connection(value: int) -> int:
        with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT ? + 1", [value])  # pyright: ignore[reportUnknownMemberType]
            row = cast(
                tuple[object, ...] | None,
                cursor.fetchone(),  # pyright: ignore[reportUnknownMemberType]
            )
            assert row is not None
            return cast(int, row[0])

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(separate_connection, range(8))) == list(range(1, 9))

    with dbapi.connect(monetdb_uri) as connection:

        def shared_connection(value: int) -> int:
            with connection.cursor() as cursor:
                cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT SUM(value) + ? FROM sys.generate_series(1, 1001)",
                    [value],
                )
                row = cast(
                    tuple[object, ...] | None,
                    cursor.fetchone(),  # pyright: ignore[reportUnknownMemberType]
                )
                assert row is not None
                return cast(int, row[0])

        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(shared_connection, range(8))) == [500_500 + value for value in range(8)]


@pytest.mark.integration
@pytest.mark.skipif(not Path("/proc/self/statm").exists(), reason="RSS assertion uses Linux procfs")
def test_repeated_query_rss_is_bounded(monetdb_uri: str) -> None:
    def rss_bytes() -> int:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")

    with dbapi.connect(monetdb_uri) as conn:
        for _ in range(10):
            pl.read_database("SELECT value FROM sys.generate_series(1, 10001)", conn)  # pyright: ignore[reportUnknownMemberType]
        gc.collect()
        baseline = rss_bytes()
        for _ in range(200):
            frame = pl.read_database(  # pyright: ignore[reportUnknownMemberType]
                "SELECT value, CAST(value AS STRING) AS text FROM sys.generate_series(1, 10001)",
                conn,
            )
            assert frame.height == 10_000
        gc.collect()
        assert rss_bytes() - baseline < 64 * 1024 * 1024

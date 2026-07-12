from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import cast

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
        assert str(table_schema) == "table_type_id: int16\ntable_type_name: string"
        with conn.cursor() as cursor:
            query_schema = cast(
                object,
                cursor.adbc_execute_schema(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT CAST(1 AS INT) AS value WHERE FALSE"
                ),
            )
            assert str(query_schema) == "value: int32"


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
                assert row[0] == 2
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
            assert row[0] == 1


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
    with dbapi.connect(monetdb_uri) as conn:
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

import json
from typing import Literal

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import ConnectionOptions, StatementOptions, dbapi

TYPE_MATRIX_PROJECTION = """
    SELECT CAST(-7 AS TINYINT) AS i8,
           CAST(-300 AS SMALLINT) AS i16,
           CAST(-70000 AS INT) AS i32,
           CAST(-5000000000 AS BIGINT) AS i64,
           CAST(123456789012345678901234567890 AS HUGEINT) AS i128,
           TRUE AS b,
           CAST(1.25 AS REAL) AS r,
           CAST(-2.5 AS DOUBLE) AS d,
           CAST(123456789012.345678 AS DECIMAL(18, 6)) AS amount,
           CAST('' AS VARCHAR(8)) AS empty_text,
           CAST(NULL AS VARCHAR(8)) AS null_text,
           BLOB '00FF' AS raw,
           UUID '444fcb84-9a7d-4fe1-adfa-7eae290328c3' AS uuid_v,
           JSON '{"nested":{"value":"ä"}}' AS json_v,
           URL 'https://example.com/ä' AS url_v,
           CAST('127.0.0.1' AS INET4) AS inet4_v,
           CAST('::1' AS INET6) AS inet6_v,
           DATE '2026-07-28' AS date_v,
           TIME '23:59:59.123456' AS time_v,
           TIMESTAMP '2026-07-28 12:34:56.123456' AS timestamp_v,
           TIMETZ '12:34:56.123456+00:00' AS timetz_v,
           TIMESTAMPTZ '2026-07-28 12:34:56.123456+00:00' AS timestamptz_v,
           INTERVAL '14' MONTH AS month_v,
           INTERVAL '1' DAY AS day_v,
           INTERVAL '1.234' SECOND AS second_v
"""


def _stats(cursor: dbapi.Cursor) -> dict[str, object]:
    return json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))


@pytest.mark.integration
def test_ingest_routes_single_small_batch_by_rows_and_bytes(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS ingest_route")
        cursor.execute("CREATE TABLE ingest_route(value INT)")
        connection.commit()

        hundred = pa.record_batch({"value": pa.array(range(100), type=pa.int32())})
        assert cursor.adbc_ingest("ingest_route", hundred, mode="append") == 100
        assert _stats(cursor)["path"] == "insert"

        hundred_one = pa.record_batch({"value": pa.array(range(100, 201), type=pa.int32())})
        assert cursor.adbc_ingest("ingest_route", hundred_one, mode="append") == 101
        assert _stats(cursor)["path"] == "copy"

        large = pa.record_batch({"value": pa.array(["x" * (9 * 1024 * 1024)])})
        assert cursor.adbc_ingest("ingest_route_large", large, mode="replace") == 1
        assert _stats(cursor)["path"] == "copy"

        hex_expanded = pa.record_batch({"value": pa.array([b"x" * (5 * 1024 * 1024)])})
        assert cursor.adbc_ingest("ingest_route_blob", hex_expanded, mode="replace") == 1
        assert _stats(cursor)["path"] == "copy"


@pytest.mark.integration
def test_ingest_insert_override_precedence_and_multibatch_routing(monetdb_uri: str) -> None:
    batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch, batch])
    with dbapi.connect(
        monetdb_uri,
        conn_kwargs={ConnectionOptions.INGEST_INSERT_ROWS: "0"},
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS ingest_route_override")
            cursor.execute("CREATE TABLE ingest_route_override(value INT)")
            connection.commit()
            assert cursor.adbc_ingest("ingest_route_override", batch, mode="append") == 1
            assert _stats(cursor)["path"] == "copy"

        with connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: "2"}) as cursor:
            assert cursor.adbc_ingest("ingest_route_override", batch, mode="append") == 1
            assert _stats(cursor)["path"] == "insert"
            assert cursor.adbc_ingest("ingest_route_override", reader, mode="append") == 2
            assert _stats(cursor)["path"] == "copy"


@pytest.mark.integration
def test_uri_tuning_and_prepared_cache_observability(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    uri = (
        f"{monetdb_uri}{separator}ingest_insert_rows=2&prepared_cache_capacity=1&"
        "read_window_bytes=4194304&write_window_bytes=8388608&constrained_append=direct"
    )
    batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
    with dbapi.connect(uri) as connection:
        assert connection.adbc_connection.get_option_int(str(ConnectionOptions.INGEST_INSERT_ROWS)) == 2
        assert connection.adbc_connection.get_option_int(str(ConnectionOptions.PREPARED_CACHE_CAPACITY)) == 1
        assert connection.adbc_connection.get_option_int(str(ConnectionOptions.READ_WINDOW_BYTES)) == 4_194_304
        assert connection.adbc_connection.get_option_int(str(ConnectionOptions.WRITE_WINDOW_BYTES)) == 8_388_608
        assert connection.adbc_connection.get_option(str(ConnectionOptions.CONSTRAINED_APPEND)) == "direct"
        with connection.cursor() as cursor:
            for table in ["ingest_cache_a", "ingest_cache_b"]:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(f"CREATE TABLE {table}(value INT)")
            connection.commit()

            assert cursor.adbc_ingest("ingest_cache_a", batch, mode="append") == 1
            assert _stats(cursor)["prepared_cache_hits"] == 0
            assert cursor.adbc_ingest("ingest_cache_a", batch, mode="append") == 1
            assert _stats(cursor)["prepared_cache_hits"] == 1
            assert cursor.adbc_ingest("ingest_cache_b", batch, mode="append") == 1
            assert _stats(cursor)["prepared_cache_hits"] == 0
            assert cursor.adbc_ingest("ingest_cache_a", batch, mode="append") == 1
            assert _stats(cursor)["prepared_cache_hits"] == 0


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["create", "replace", "create_append", "append"])
def test_tiny_insert_path_supports_every_ingest_mode(
    monetdb_uri: str,
    mode: Literal["append", "create", "replace", "create_append"],
) -> None:
    batch = pa.record_batch({"value": pa.array([1, 2], type=pa.int32())})
    table = f"ingest_insert_{mode}"
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS "{table}"')
        if mode == "append":
            cursor.execute(f'CREATE TABLE "{table}"(value INT)')
        connection.commit()

        assert cursor.adbc_ingest(table, batch, mode=mode) == 2
        assert _stats(cursor)["path"] == "insert"
        assert cursor.execute(f'SELECT value FROM "{table}" ORDER BY value').fetchall() == [
            (1,),
            (2,),
        ]


@pytest.mark.integration
def test_insert_path_failure_is_atomic_and_does_not_poison_commit(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_insert_atomic")
        setup.execute("CREATE TABLE ingest_insert_atomic(value INT PRIMARY KEY)")
        setup.execute("INSERT INTO ingest_insert_atomic VALUES (1)")

    batch = pa.record_batch({"value": pa.array([2, 1], type=pa.int32())})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.IntegrityError):
            cursor.adbc_ingest("ingest_insert_atomic", batch, mode="append")
        assert _stats(cursor)["path"] == "insert"
        assert _stats(cursor)["poisoned"] is False
        cursor.execute("INSERT INTO ingest_insert_atomic VALUES (3)")
        connection.commit()

    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute("SELECT value FROM ingest_insert_atomic ORDER BY value").fetchall() == [(1,), (3,)]


@pytest.mark.integration
def test_nonfinite_float_is_rejected_before_copy_null_sentinel_encoding(
    monetdb_uri: str,
) -> None:
    batch = pa.record_batch({"value": pa.array([float("nan")], type=pa.float32())})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS ingest_nan_route")
        cursor.execute("CREATE TABLE ingest_nan_route(value REAL)")
        connection.commit()

        with pytest.raises(adbc_driver_manager.DataError, match="non-finite"):
            cursor.adbc_ingest("ingest_nan_route", batch, mode="append")
        assert cursor.execute("SELECT COUNT(*) FROM ingest_nan_route").fetchone() == (0,)


@pytest.mark.integration
def test_insert_and_copy_routes_store_the_same_type_matrix(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(TYPE_MATRIX_PROJECTION)
        source = cursor.fetch_arrow_table()
        assert isinstance(source, pa.Table)
        batch = source.to_batches()[0]

        assert cursor.adbc_ingest("ingest_type_matrix_insert", batch, mode="replace") == 1
        assert _stats(cursor)["path"] == "insert"
        with connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: 0}) as copy:
            assert copy.adbc_ingest("ingest_type_matrix_copy", batch, mode="replace") == 1
            assert _stats(copy)["path"] == "copy"

        cursor.execute("SELECT * FROM ingest_type_matrix_insert")
        inserted = cursor.fetch_arrow_table()
        cursor.execute("SELECT * FROM ingest_type_matrix_copy")
        copied = cursor.fetch_arrow_table()
        assert isinstance(inserted, pa.Table)
        assert isinstance(copied, pa.Table)
        assert inserted.equals(copied)


@pytest.mark.integration
@pytest.mark.parametrize("path", ["insert", "copy"])
def test_explicit_identity_values_do_not_advance_the_sequence(
    monetdb_uri: str,
    path: Literal["insert", "copy"],
) -> None:
    table = f"ingest_identity_{path}"
    batch = pa.record_batch(
        {
            "id": pa.array([2], type=pa.int64()),
            "value": pa.array(["bulk"]),
        }
    )
    threshold = 100 if path == "insert" else 0
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(
            f"CREATE TABLE {table}(id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, value VARCHAR(8))"
        )
        cursor.execute(f"INSERT INTO {table}(value) VALUES ('first')")
        with connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: threshold}) as ingest:
            assert ingest.adbc_ingest(table, batch, mode="append") == 1
            expected_path = "insert" if path == "insert" else "staged_copy"
            assert _stats(ingest)["path"] == expected_path
        with pytest.raises(adbc_driver_manager.IntegrityError):
            cursor.execute(f"INSERT INTO {table}(value) VALUES ('next')")
        assert cursor.execute(f"SELECT id, value FROM {table} ORDER BY id").fetchall() == [
            (1, "first"),
            (2, "bulk"),
        ]

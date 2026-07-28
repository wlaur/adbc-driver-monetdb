import json
import random
from typing import cast

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import (
    ConnectionOptions,
    DatabaseOptions,
    StatementOptions,
    dbapi,
)

MIB = 1024 * 1024


def _stats(cursor: dbapi.Cursor) -> dict[str, object]:
    return json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))


def _random_int64_batch(rows: int, seed: int) -> pa.RecordBatch:
    values = pa.py_buffer(random.Random(seed).randbytes(rows * 8))
    array = pa.Int64Array.from_buffers(pa.int64(), rows, [None, values])
    return pa.record_batch({"value": array})


@pytest.mark.integration
def test_wire_compression_option_inheritance_and_validation(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    with dbapi.connect(monetdb_uri) as defaults:
        assert defaults.adbc_database.get_option(str(DatabaseOptions.WIRE_COMPRESSION)) == "auto"
        assert defaults.adbc_connection.get_option(str(ConnectionOptions.WIRE_COMPRESSION)) == "auto"
        with defaults.cursor() as cursor:
            assert cursor.adbc_statement.get_option(str(StatementOptions.WIRE_COMPRESSION)) == "auto"

    with dbapi.connect(f"{monetdb_uri}{separator}wire_compression=auto") as connection:
        assert connection.adbc_database.get_option(str(DatabaseOptions.WIRE_COMPRESSION)) == "auto"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.WIRE_COMPRESSION)) == "auto"
        with connection.cursor() as inherited:
            assert inherited.adbc_statement.get_option(str(StatementOptions.WIRE_COMPRESSION)) == "auto"
        with connection.cursor(adbc_stmt_kwargs={StatementOptions.WIRE_COMPRESSION: "lz4"}) as overridden:
            assert overridden.adbc_statement.get_option(str(StatementOptions.WIRE_COMPRESSION)) == "lz4"
        with pytest.raises(adbc_driver_manager.ProgrammingError, match=r"none.*auto.*lz4"):
            connection.cursor(adbc_stmt_kwargs={StatementOptions.WIRE_COMPRESSION: "gzip"})


@pytest.mark.integration
def test_server_lz4_matches_uncompressed_copy_for_supported_types(monetdb_uri: str) -> None:
    source_query = """
        SELECT CAST(-5000000000 AS BIGINT) AS integer_value,
               CAST(123456789012.345678 AS DECIMAL(18, 6)) AS decimal_value,
               CAST('' AS VARCHAR(8)) AS empty_text,
               CAST(NULL AS VARCHAR(8)) AS null_text,
               BLOB '00FF' AS raw,
               UUID '444fcb84-9a7d-4fe1-adfa-7eae290328c3' AS uuid_value,
               DATE '2026-07-28' AS date_value,
               TIME '23:59:59.123456' AS time_value,
               TIMESTAMP '2026-07-28 12:34:56.123456' AS timestamp_value
    """
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(source_query)
        source = cursor.fetch_arrow_table()
        assert isinstance(source, pa.Table)
        batch = source.to_batches()[0]

        with connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WIRE_COMPRESSION: "none",
            }
        ) as plain:
            assert plain.adbc_ingest("wire_plain_types", batch, mode="replace") == 1
            assert _stats(plain)["window_wire_compression"] == ["none"]
        with connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WIRE_COMPRESSION: "lz4",
            }
        ) as compressed:
            assert compressed.adbc_ingest("wire_lz4_types", batch, mode="replace") == 1
            assert _stats(compressed)["window_wire_compression"] == ["lz4"]

        cursor.execute("SELECT * FROM wire_plain_types")
        plain_result = cursor.fetch_arrow_table()
        cursor.execute("SELECT * FROM wire_lz4_types")
        compressed_result = cursor.fetch_arrow_table()
        assert isinstance(plain_result, pa.Table)
        assert isinstance(compressed_result, pa.Table)
        assert plain_result.equals(compressed_result)


@pytest.mark.integration
def test_forced_wire_lz4_accepts_concatenated_frames(monetdb_uri: str) -> None:
    batch = _random_int64_batch(500_000, 20260728)
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WRITE_WINDOW_BYTES: 8 * MIB,
                StatementOptions.WIRE_COMPRESSION: "lz4",
            }
        ) as cursor,
    ):
        assert cursor.adbc_ingest("wire_lz4_frames", batch, mode="replace") == batch.num_rows
        stats = _stats(cursor)
        assert stats["window_wire_compression"] == ["lz4"]
        assert cast(int, stats["encoded_bytes"]) == batch.num_rows * 8
        assert cursor.execute("SELECT COUNT(*) FROM wire_lz4_frames").fetchone() == (batch.num_rows,)


@pytest.mark.integration
@pytest.mark.parametrize("compressible_first", [True, False])
def test_auto_wire_compression_is_decided_per_window(
    monetdb_uri: str,
    compressible_first: bool,
) -> None:
    rows = 4 * MIB // 8
    compressible = pa.record_batch({"value": pa.array([0] * rows, type=pa.int64())})
    incompressible = _random_int64_batch(rows, 20260729)
    batches = [compressible, incompressible] if compressible_first else [incompressible, compressible]
    reader = pa.RecordBatchReader.from_batches(compressible.schema, batches)
    table = f"wire_auto_windows_{str(compressible_first).lower()}"
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WRITE_WINDOW_BYTES: 4 * MIB,
                StatementOptions.WIRE_COMPRESSION: "auto",
            }
        ) as cursor,
    ):
        assert cursor.adbc_ingest(table, reader, mode="replace") == rows * 2
        stats = _stats(cursor)
        expected = ["lz4", "none"] if compressible_first else ["none", "lz4"]
        assert stats["window_wire_compression"] == expected
        assert stats["copy_count"] == 2
        assert cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (rows * 2,)

import os
import subprocess

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import dbapi


def _database_farm_bytes() -> int | None:
    container = os.environ.get("MONETDB_TEST_CONTAINER")
    if container is None:
        return None
    result = subprocess.run(
        ["docker", "exec", container, "du", "-sb", "/var/monetdb5/dbfarm"],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(result.stdout.split()[0])


@pytest.mark.integration
def test_repeated_wide_appends_have_bounded_transaction_storage(monetdb_uri: str) -> None:
    rows = 4_096
    columns = 786
    calls = 24
    logical_bytes = rows * columns * calls * 4
    batch = pa.record_batch(
        [
            pa.array(
                ((row * 31 + column * 17) % 100_003 / 37 for row in range(rows)),
                type=pa.float32(),
            )
            for column in range(columns)
        ],
        names=[f"v{column:04d}" for column in range(columns)],
    )

    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS repeated_wide_append")
        connection.commit()
        assert cursor.adbc_ingest("repeated_wide_append", batch.slice(0, 0), mode="create") == 0
        connection.commit()
        baseline = _database_farm_bytes()
        samples: list[int] = []
        for _ in range(calls):
            assert cursor.adbc_ingest("repeated_wide_append", batch, mode="append") == rows
            current = _database_farm_bytes()
            if current is not None:
                samples.append(current)
        cursor.execute("SELECT COUNT(*) FROM repeated_wide_append")
        assert cursor.fetchone() == (rows * calls,)
        connection.commit()
        committed = _database_farm_bytes()

    if baseline is not None and committed is not None:
        samples.append(committed)
        assert max(samples) - baseline < logical_bytes * 3

    with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
        cleanup.execute("DROP TABLE repeated_wide_append")


@pytest.mark.integration
def test_single_copy_append_server_error_aborts_dbapi_transaction_until_rollback(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_constraint_error")
        setup.execute("CREATE TABLE ingest_constraint_error(value INT PRIMARY KEY)")
        setup.execute("INSERT INTO ingest_constraint_error VALUES (1)")

    try:
        with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
            cursor.execute("INSERT INTO ingest_constraint_error VALUES (2)")
            duplicate = pa.table({"value": pa.array([3, 3], type=pa.int32())})
            with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
                cursor.adbc_ingest("ingest_constraint_error", duplicate, mode="append")
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
            assert caught.value.sqlstate == "40002"
            with pytest.raises(adbc_driver_manager.ProgrammingError) as aborted:
                cursor.execute("SELECT value FROM ingest_constraint_error")
            assert aborted.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_STATE
            assert aborted.value.sqlstate == "25005"
            connection.rollback()
            cursor.execute("SELECT value FROM ingest_constraint_error ORDER BY value")
            assert cursor.fetchall() == [(1,)]
            cursor.execute("INSERT INTO ingest_constraint_error VALUES (4)")
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_constraint_error ORDER BY value").fetchall() == [(1,), (4,)]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_constraint_error")


@pytest.mark.integration
def test_single_copy_append_server_error_rolls_back_internal_autocommit_transaction(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS ingest_autocommit_error")
            connection.execute("CREATE TABLE ingest_autocommit_error(value INT PRIMARY KEY)")
            connection.execute("INSERT INTO ingest_autocommit_error VALUES (1)")
            duplicate = pa.table({"value": pa.array([2, 2], type=pa.int32())})
            with connection.cursor() as cursor:
                with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
                    cursor.adbc_ingest("ingest_autocommit_error", duplicate, mode="append")
                assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
                assert caught.value.sqlstate == "40002"
                cursor.execute("SELECT value FROM ingest_autocommit_error")
                assert cursor.fetchall() == [(1,)]
        finally:
            connection.execute("DROP TABLE IF EXISTS ingest_autocommit_error")


@pytest.mark.integration
def test_multi_batch_append_server_error_rolls_back_to_operation_savepoint(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_stream_error")
        setup.execute("CREATE TABLE ingest_stream_error(value INT PRIMARY KEY)")

    try:
        batches = [
            pa.record_batch({"value": pa.array([2], type=pa.int32())}),
            pa.record_batch({"value": pa.array([3, 3], type=pa.int32())}),
        ]
        reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
        with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
            cursor.execute("INSERT INTO ingest_stream_error VALUES (1)")
            with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
                cursor.adbc_ingest("ingest_stream_error", reader, mode="append")
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
            assert caught.value.sqlstate == "40002"
            cursor.execute("SELECT value FROM ingest_stream_error")
            assert cursor.fetchall() == [(1,)]
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_stream_error").fetchall() == [(1,)]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_stream_error")

from collections.abc import Iterator

import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi


@pytest.fixture
def write_target(monetdb_uri: str) -> Iterator[str]:
    table = "write_savepoint_visibility"
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute(f"DROP TABLE IF EXISTS {table}")
        setup.execute(f"CREATE TABLE {table}(id INT PRIMARY KEY, value INT, label STRING)")
        setup.execute(f"INSERT INTO {table} VALUES (1, 0, 'old'), (2, 0, 'old'), (3, 0, 'old'), (4, 0, 'old')")
    try:
        yield table
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.integration
@pytest.mark.parametrize("commit", [False, True])
def test_repeated_bound_update_batches_preserve_every_write(monetdb_uri: str, write_target: str, commit: bool) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        for value in (1, 2):
            cursor.executemany(
                f"UPDATE {write_target} SET value=? WHERE id=?", [(value, 2 * value - 1), (value, 2 * value)]
            )
            assert cursor.rowcount == 2
            expected = [(1, 1), (2, 1), (3, 2 if value == 2 else 0), (4, 2 if value == 2 else 0)]
            assert cursor.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == expected
        if commit:
            connection.commit()
        else:
            connection.rollback()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == (
            [(1, 1), (2, 1), (3, 2), (4, 2)] if commit else [(1, 0), (2, 0), (3, 0), (4, 0)]
        )


@pytest.mark.integration
@pytest.mark.parametrize("commit", [False, True])
@pytest.mark.parametrize("route", ["bound", "insert", "copy"])
def test_deleted_committed_keys_can_be_reinserted_in_the_same_transaction(
    monetdb_uri: str, write_target: str, commit: bool, route: str
) -> None:
    options: dict[str, object] = {}
    if route == "copy":
        options[str(StatementOptions.INGEST_INSERT_ROWS)] = 0
        options[str(StatementOptions.WRITE_BATCH_ROWS)] = 1
    data = pa.table(
        {"id": pa.array([1, 2], type=pa.int32()), "value": pa.array([1, 2], type=pa.int32()), "label": ["new", None]}
    )
    with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
        cursor.execute(f"DELETE FROM {write_target} WHERE id <= 2")
        if route == "bound":
            cursor.executemany(f"INSERT INTO {write_target} VALUES (?, ?, ?)", [(1, 1, "new"), (2, 2, None)])
            assert cursor.rowcount == 2
        else:
            assert cursor.adbc_ingest(write_target, data, mode="append") == 2
        expected = [(1, 1, "new"), (2, 2, None), (3, 0, "old"), (4, 0, "old")]
        assert cursor.execute(f"SELECT * FROM {write_target} ORDER BY id").fetchall() == expected
        if commit:
            connection.commit()
        else:
            connection.rollback()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT * FROM {write_target} ORDER BY id").fetchall() == (
            expected if commit else [(1, 0, "old"), (2, 0, "old"), (3, 0, "old"), (4, 0, "old")]
        )


@pytest.mark.integration
@pytest.mark.parametrize("recovery", ["api", "sql"])
def test_bound_batch_client_failure_blocks_partial_commit_and_recovers(
    monetdb_uri: str, write_target: str, recovery: str
) -> None:
    import adbc_driver_manager

    batch = pa.record_batch({"value": pa.array([2, 2], type=pa.int32()), "id": pa.array([3, 4], type=pa.int32())})

    def parameters() -> Iterator[pa.RecordBatch]:
        yield batch
        raise RuntimeError("intentional parameter failure")

    reader = pa.RecordBatchReader.from_batches(batch.schema, parameters())

    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(f"UPDATE {write_target} SET value=1 WHERE id <= 2")
        with pytest.raises(Exception, match="intentional parameter failure"):
            cursor.executemany(f"UPDATE {write_target} SET value=? WHERE id=?", reader)
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
            connection.commit()
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
            cursor.execute("COMMIT")
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
            connection.adbc_connection.set_options(**{"adbc.connection.autocommit": "true"})
        if recovery == "api":
            connection.rollback()
        else:
            cursor.execute("ROLLBACK")
        assert cursor.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 0),
        ]
        cursor.execute(f"UPDATE {write_target} SET value=3 WHERE id=4")
        connection.commit()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 3),
        ]


@pytest.mark.integration
@pytest.mark.parametrize("route", ["insert", "copy"])
def test_append_preflight_failure_preserves_committable_prior_writes(
    monetdb_uri: str, write_target: str, route: str
) -> None:
    import adbc_driver_manager

    options: dict[str, object] = {}
    if route == "copy":
        options[str(StatementOptions.INGEST_INSERT_ROWS)] = 0
    data = pa.table({"id": pa.array([5], type=pa.int32()), "value": ["wrong"], "label": ["new"]})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
        cursor.execute(f"UPDATE {write_target} SET value=1 WHERE id=1")
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="type"):
            cursor.adbc_ingest(write_target, data, mode="append")
        assert cursor.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 1),
            (2, 0),
            (3, 0),
            (4, 0),
        ]
        connection.commit()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 1),
            (2, 0),
            (3, 0),
            (4, 0),
        ]


@pytest.mark.integration
def test_clean_bound_batch_client_failure_recovers_operation_without_poisoning(
    monetdb_uri: str, write_target: str
) -> None:
    batch = pa.record_batch({"value": pa.array([2, 2], type=pa.int32()), "id": pa.array([3, 4], type=pa.int32())})

    def parameters() -> Iterator[pa.RecordBatch]:
        yield batch
        raise RuntimeError("intentional parameter failure")

    reader = pa.RecordBatchReader.from_batches(batch.schema, parameters())
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(Exception, match="intentional parameter failure"):
            cursor.executemany(f"UPDATE {write_target} SET value=? WHERE id=?", reader)
        assert cursor.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 0),
        ]
        cursor.execute(f"UPDATE {write_target} SET value=3 WHERE id=4")
        connection.commit()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 3),
        ]


@pytest.mark.integration
def test_partial_append_opt_in_cannot_commit_after_a_server_constraint_error(
    monetdb_uri: str, write_target: str
) -> None:
    import adbc_driver_manager

    options: dict[str, object] = {
        str(StatementOptions.CONSTRAINED_APPEND): "direct",
        str(StatementOptions.WRITE_BATCH_ROWS): 1,
        str(StatementOptions.INGEST_PARTIAL): "allow",
    }
    data = pa.table({"id": pa.array([5, 6, 6], type=pa.int32()), "value": pa.array([2, 2, 2], type=pa.int32())})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
        cursor.execute(f"UPDATE {write_target} SET value=1 WHERE id=1")
        with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
            cursor.adbc_ingest(write_target, data, mode="append")
        assert caught.value.sqlstate == "40002"
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
            connection.commit()
        connection.rollback()
        assert cursor.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 0),
        ]
        connection.commit()


@pytest.mark.integration
def test_repeated_updates_preserve_writes_with_a_committed_temporary_table(monetdb_uri: str, write_target: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE LOCAL TEMPORARY TABLE write_scope_scratch(value INT) ON COMMIT DELETE ROWS")
        connection.commit()
        for value in (1, 2):
            cursor.executemany(
                f"UPDATE {write_target} SET value=? WHERE id=?", [(value, 2 * value - 1), (value, 2 * value)]
            )
        assert cursor.execute("SELECT COUNT(*) FROM write_scope_scratch").fetchone() == (0,)
        connection.commit()
    with dbapi.connect(monetdb_uri, autocommit=True) as audit:
        assert audit.execute(f"SELECT id, value FROM {write_target} ORDER BY id").fetchall() == [
            (1, 1),
            (2, 1),
            (3, 2),
            (4, 2),
        ]

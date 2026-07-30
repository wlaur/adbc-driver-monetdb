import json
import os
import subprocess
from collections.abc import Iterator

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi


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


def _reader_that_fails_after(batch: pa.RecordBatch) -> pa.RecordBatchReader:
    def batches() -> Iterator[pa.RecordBatch]:
        yield batch
        raise RuntimeError("intentional upstream failure")

    return pa.RecordBatchReader.from_batches(batch.schema, batches())


@pytest.mark.integration
def test_upstream_failure_after_copy_blocks_commit_until_rollback(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_partial_append")
        setup.execute("CREATE TABLE ingest_partial_append(value INT)")
        setup.execute("INSERT INTO ingest_partial_append VALUES (1)")

    try:
        batch = pa.record_batch({"value": pa.array([3, 4], type=pa.int32())})
        with (
            dbapi.connect(monetdb_uri) as connection,
            connection.cursor(adbc_stmt_kwargs={StatementOptions.WRITE_BATCH_ROWS: "2"}) as cursor,
        ):
            cursor.execute("INSERT INTO ingest_partial_append VALUES (2)")
            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_partial_append",
                    _reader_that_fails_after(batch),
                    mode="append",
                )
            cursor.execute("SELECT value FROM ingest_partial_append ORDER BY value")
            assert cursor.fetchall() == [(1,), (2,), (3,), (4,)]
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required") as caught:
                connection.commit()
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_STATE
            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert stats["copy_count"] == 1
            assert stats["poisoned"] is True
            connection.rollback()
            cursor.execute("INSERT INTO ingest_partial_append VALUES (5)")
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_partial_append ORDER BY value").fetchall() == [(1,), (5,)]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_partial_append")


@pytest.mark.integration
def test_raw_transaction_sql_obeys_and_clears_ingest_poison(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_raw_transaction")
        setup.execute("CREATE TABLE ingest_raw_transaction(value INT)")

    try:
        batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
        options: dict[str, object] = {str(StatementOptions.WRITE_BATCH_ROWS): "1"}
        with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_raw_transaction",
                    _reader_that_fails_after(batch),
                    mode="append",
                )

            cursor.adbc_statement.set_sql_query("-- raw update\nCOMMIT")
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
                cursor.adbc_statement.execute_update()
            cursor.adbc_statement.set_sql_query("/* recover */ ROLLBACK")
            cursor.adbc_statement.execute_update()

            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_raw_transaction",
                    _reader_that_fails_after(batch),
                    mode="append",
                )
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
                cursor.execute("/* raw query */ COMMIT")
            cursor.execute("-- recover\nROLLBACK")

            cursor.execute("INSERT INTO ingest_raw_transaction VALUES (2)")
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_raw_transaction").fetchall() == [(2,)]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_raw_transaction")


@pytest.mark.integration
def test_savepoint_atomicity_preserves_prior_caller_work(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_savepoint_append")
        setup.execute("CREATE TABLE ingest_savepoint_append(value INT)")

    try:
        batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int32())})
        options: dict[str, object] = {
            str(StatementOptions.WRITE_BATCH_ROWS): "2",
            str(StatementOptions.INGEST_ATOMICITY): "savepoint",
        }
        with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
            cursor.execute("INSERT INTO ingest_savepoint_append VALUES (1)")
            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_savepoint_append",
                    _reader_that_fails_after(batch),
                    mode="append",
                )
            cursor.execute("SELECT value FROM ingest_savepoint_append")
            assert cursor.fetchall() == [(1,)]
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_savepoint_append").fetchall() == [(1,)]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_savepoint_append")


@pytest.mark.integration
def test_allow_partial_escape_hatch_leaves_commit_under_caller_control(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_allow_partial")
        setup.execute("CREATE TABLE ingest_allow_partial(value INT)")

    try:
        batch = pa.record_batch({"value": pa.array([2, 3], type=pa.int32())})
        options: dict[str, object] = {
            str(StatementOptions.WRITE_BATCH_ROWS): "2",
            str(StatementOptions.INGEST_PARTIAL): "allow",
        }
        with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
            cursor.execute("INSERT INTO ingest_allow_partial VALUES (1)")
            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_allow_partial",
                    _reader_that_fails_after(batch),
                    mode="append",
                )
            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert stats["copy_count"] == 1
            assert stats["poisoned"] is False
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_allow_partial ORDER BY value").fetchall() == [
                (1,),
                (2,),
                (3,),
            ]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_allow_partial")


@pytest.mark.integration
def test_default_scheduler_coalesces_small_upstream_batches(monetdb_uri: str) -> None:
    batches = [
        pa.record_batch({"value": pa.array(range(start, start + 1_000), type=pa.int32())})
        for start in range(0, 10_000, 1_000)
    ]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        try:
            assert cursor.adbc_ingest("ingest_coalesced_batches", reader, mode="replace") == 10_000
            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert stats["input_batches"] == 10
            assert stats["coalesced_windows"] == 1
            assert stats["copy_count"] == 1
            assert stats["encoded_bytes"] == 40_000
        finally:
            cursor.execute("DROP TABLE IF EXISTS ingest_coalesced_batches")


@pytest.mark.integration
def test_default_scheduler_distinguishes_logical_and_physical_budgets(
    monetdb_uri: str,
) -> None:
    batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        try:
            cursor.adbc_ingest("ingest_regular_budget", batch, mode="replace")
            regular = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))[
                "window_budget_bytes"
            ]

            cursor.execute("CREATE TABLE ingest_unconstrained_budget(value INT)")
            cursor.adbc_ingest("ingest_unconstrained_budget", batch, mode="append")
            unconstrained = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))[
                "window_budget_bytes"
            ]

            cursor.execute("CREATE TABLE ingest_constrained_budget(value INT PRIMARY KEY)")
            cursor.adbc_ingest("ingest_constrained_budget", batch, mode="append")
            constrained = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))[
                "window_budget_bytes"
            ]

            cursor.execute("CREATE TABLE ingest_variable_constraint_budget(value STRING PRIMARY KEY)")
            cursor.adbc_ingest(
                "ingest_variable_constraint_budget",
                pa.record_batch({"value": pa.array(["one"], type=pa.string())}),
                mode="append",
            )
            variable = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))[
                "window_budget_bytes"
            ]

            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert regular <= 512 * 1024 * 1024
            assert unconstrained == regular
            assert constrained <= 2 * 1024 * 1024 * 1024
            assert constrained >= unconstrained
            assert variable == regular
            assert stats["physical_window_budget_bytes"] <= 128 * 1024 * 1024
            assert stats["incompressible_window_budget_bytes"] <= 64 * 1024 * 1024
        finally:
            cursor.execute("DROP TABLE IF EXISTS ingest_regular_budget")
            cursor.execute("DROP TABLE IF EXISTS ingest_unconstrained_budget")
            cursor.execute("DROP TABLE IF EXISTS ingest_constrained_budget")
            cursor.execute("DROP TABLE IF EXISTS ingest_variable_constraint_budget")


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
def test_staged_single_copy_constraint_error_preserves_the_caller_transaction(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_constraint_error")
        setup.execute("CREATE TABLE ingest_constraint_error(value INT PRIMARY KEY)")
        setup.execute("INSERT INTO ingest_constraint_error VALUES (1)")

    try:
        with (
            dbapi.connect(monetdb_uri) as connection,
            connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: "0"}) as cursor,
        ):
            cursor.execute("INSERT INTO ingest_constraint_error VALUES (2)")
            cursor.execute("SELECT value FROM ingest_constraint_error WHERE value = ?", [1])
            assert cursor.fetchall() == [(1,)]
            duplicate = pa.table({"value": pa.array([3, 3], type=pa.int32())})
            with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
                cursor.adbc_ingest("ingest_constraint_error", duplicate, mode="append")
            assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
            assert caught.value.sqlstate == "40002"
            cursor.execute("SELECT value FROM ingest_constraint_error ORDER BY value")
            assert cursor.fetchall() == [(1,), (2,)]
            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert stats["path"] == "staged_copy"
            assert stats["staging_copy_count"] == 1
            assert stats["target_copy_count"] == 0
            assert stats["final_move_count"] == 0
            assert stats["poisoned"] is False
            cursor.execute("INSERT INTO ingest_constraint_error VALUES (4)")
            cursor.execute("CREATE TABLE ingest_after_constraint_error(value INT)")
            cursor.execute("DROP TABLE ingest_after_constraint_error")
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_constraint_error ORDER BY value").fetchall() == [
                (1,),
                (2,),
                (4,),
            ]
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
def test_staged_multi_batch_constraint_error_preserves_the_caller_transaction(
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
            cursor.execute("INSERT INTO ingest_stream_error VALUES (4)")
            connection.commit()

        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT value FROM ingest_stream_error ORDER BY value").fetchall() == [
                (1,),
                (4,),
            ]
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_stream_error")


@pytest.mark.integration
def test_multi_batch_append_server_error_rolls_back_internal_autocommit_transaction(
    monetdb_uri: str,
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS ingest_autocommit_stream_error")
            connection.execute("CREATE TABLE ingest_autocommit_stream_error(value INT PRIMARY KEY)")
            connection.execute("INSERT INTO ingest_autocommit_stream_error VALUES (1)")
            batches = [
                pa.record_batch({"value": pa.array([2], type=pa.int32())}),
                pa.record_batch({"value": pa.array([3, 3], type=pa.int32())}),
            ]
            reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
            with connection.cursor() as cursor:
                with pytest.raises(adbc_driver_manager.IntegrityError) as caught:
                    cursor.adbc_ingest(
                        "ingest_autocommit_stream_error",
                        reader,
                        mode="append",
                    )
                assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
                assert caught.value.sqlstate == "40002"
                cursor.execute("SELECT value FROM ingest_autocommit_stream_error")
                assert cursor.fetchall() == [(1,)]
        finally:
            connection.execute("DROP TABLE IF EXISTS ingest_autocommit_stream_error")


@pytest.mark.integration
def test_constrained_append_stages_many_windows_and_moves_to_the_target_once(
    monetdb_uri: str,
) -> None:
    batch = pa.record_batch(
        {
            "left_id": pa.array([1, 1, 2, 2], type=pa.int32()),
            "right_id": pa.array([1, 2, 1, 2], type=pa.int32()),
            "value": pa.array(["a", "b", "c", "d"]),
        }
    )
    options: dict[str, object] = {
        str(StatementOptions.INGEST_INSERT_ROWS): 0,
        str(StatementOptions.WRITE_BATCH_ROWS): 2,
    }
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS ingest_composite_stage")
            connection.execute(
                "CREATE TABLE ingest_composite_stage("
                "left_id INT, right_id INT, value STRING, "
                "PRIMARY KEY(left_id, right_id))"
            )
            with connection.cursor(adbc_stmt_kwargs=options) as cursor:
                assert cursor.adbc_ingest("ingest_composite_stage", batch, mode="append") == 4
                stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
                assert stats["path"] == "staged_copy"
                assert stats["copy_count"] == 2
                assert stats["staging_copy_count"] == 2
                assert stats["target_copy_count"] == 0
                assert stats["final_move_count"] == 1
                assert cursor.execute(
                    "SELECT left_id, right_id, value FROM ingest_composite_stage ORDER BY left_id, right_id"
                ).fetchall() == [
                    (1, 1, "a"),
                    (1, 2, "b"),
                    (2, 1, "c"),
                    (2, 2, "d"),
                ]
                assert cursor.execute(
                    "SELECT COUNT(*) FROM tmp._tables WHERE name LIKE 'adbc_ingest_stage_%'"
                ).fetchone() == (0,)
                assert cursor.execute(
                    "SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'adbc_ingest_stage_%'"
                ).fetchone() == (0,)
        finally:
            connection.execute("DROP TABLE IF EXISTS ingest_composite_stage")


@pytest.mark.integration
def test_direct_constrained_append_knob_retains_partial_write_protection(
    monetdb_uri: str,
) -> None:
    batch = pa.record_batch({"value": pa.array([2], type=pa.int32())})
    options: dict[str, object] = {
        str(StatementOptions.CONSTRAINED_APPEND): "direct",
        str(StatementOptions.WRITE_BATCH_ROWS): 1,
    }
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_direct_constraint")
        setup.execute("CREATE TABLE ingest_direct_constraint(value INT PRIMARY KEY)")

    try:
        with dbapi.connect(monetdb_uri) as connection, connection.cursor(adbc_stmt_kwargs=options) as cursor:
            with pytest.raises(Exception, match="intentional upstream failure"):
                cursor.adbc_ingest(
                    "ingest_direct_constraint",
                    _reader_that_fails_after(batch),
                    mode="append",
                )
            stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
            assert stats["path"] == "copy"
            assert stats["target_copy_count"] == 1
            assert stats["staging_copy_count"] == 0
            assert stats["final_move_count"] == 0
            assert stats["poisoned"] is True
            with pytest.raises(adbc_driver_manager.ProgrammingError, match="ROLLBACK is required"):
                connection.commit()
            connection.rollback()
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_direct_constraint")


@pytest.mark.integration
def test_ingest_preserves_drop_on_commit_temporary_table(monetdb_uri: str) -> None:
    # An ON COMMIT DROP temporary table must survive an ingest into an unrelated table:
    # releasing the internal savepoint ends its lifetime, so the savepoint has to be
    # retained until the caller's transaction ends, exactly as for ON COMMIT DELETE ROWS.
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS ingest_drop_on_commit_target")
        setup.execute("CREATE TABLE ingest_drop_on_commit_target(value INT)")

    try:
        batch = pa.record_batch({"value": pa.array([1, 2], type=pa.int32())})
        with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
            cursor.execute("CREATE LOCAL TEMPORARY TABLE ingest_drop_scratch(value INT) ON COMMIT DROP")
            cursor.adbc_ingest("ingest_drop_on_commit_target", batch, mode="append")

            cursor.execute("SELECT COUNT(*) FROM ingest_drop_scratch")
            assert cursor.fetchone() == (0,)

            connection.rollback()
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS ingest_drop_on_commit_target")

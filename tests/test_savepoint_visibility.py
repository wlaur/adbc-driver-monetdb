import json
from collections.abc import Iterator
from uuid import uuid4

import pytest

from adbc_driver_monetdb import StatementOptions, dbapi


@pytest.fixture
def committed_row(monetdb_uri: str) -> Iterator[str]:
    table = f"savepoint_visibility_{uuid4().hex}"
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER)")
        cursor.execute(f"INSERT INTO {table} VALUES (1, 10)")
    try:
        yield table
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE {table}")


@pytest.mark.integration
@pytest.mark.parametrize("explicit_prepare", [False, True])
def test_bound_read_excludes_deleted_committed_row(
    monetdb_uri: str, committed_row: str, explicit_prepare: bool
) -> None:
    with dbapi.connect(monetdb_uri, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {committed_row} WHERE id = 1")
            assert cursor.rowcount == 1
        with connection.cursor() as cursor:
            sql = f"SELECT value FROM {committed_row} WHERE id = ?"
            if explicit_prepare:
                cursor.adbc_prepare(sql)
            cursor.execute(sql, [1])
            assert cursor.fetchall() == []
        connection.rollback()


@pytest.mark.integration
@pytest.mark.parametrize("boundary", ["commit", "rollback", "sql_commit", "sql_rollback", "autocommit"])
def test_deferred_verification_resumes_after_transaction_boundary(
    monetdb_uri: str, committed_row: str, boundary: str
) -> None:
    with dbapi.connect(monetdb_uri, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {committed_row} WHERE id = 1")
        query = f"SELECT value FROM {committed_row} WHERE id = ?"
        with connection.cursor() as cursor:
            cursor.execute(query, [1])
            assert cursor.fetchall() == []
            status = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.PREPARE_STATUS)))
            assert status["path"] == "literal"
        if boundary == "commit":
            connection.commit()
        elif boundary == "rollback":
            connection.rollback()
        elif boundary == "autocommit":
            connection.adbc_connection.set_options(**{"adbc.connection.autocommit": "true"})
        else:
            with connection.cursor() as cursor:
                cursor.execute(boundary.removeprefix("sql_").upper())
        with connection.cursor() as cursor:
            cursor.execute(query, [1])
            assert cursor.fetchall() == ([(10,)] if "rollback" in boundary else [])
            status = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.PREPARE_STATUS)))
            assert status["path"] == "prepared"


@pytest.mark.integration
def test_autocommit_write_does_not_defer_prepared_verification(monetdb_uri: str, committed_row: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"UPDATE {committed_row} SET value = 20 WHERE id = 1")
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT value FROM {committed_row} WHERE id = ?", [1])
            assert cursor.fetchall() == [(20,)]
            status = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.PREPARE_STATUS)))
            assert status["path"] == "prepared"


@pytest.mark.integration
def test_repeated_autocommit_false_preserves_pending_write_state(monetdb_uri: str, committed_row: str) -> None:
    with dbapi.connect(monetdb_uri, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {committed_row} WHERE id = 1")
        connection.adbc_connection.set_options(**{"adbc.connection.autocommit": "false"})
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT value FROM {committed_row} WHERE id = ?", [1])
            assert cursor.fetchall() == []
        connection.rollback()


@pytest.mark.integration
@pytest.mark.parametrize("operation", ["insert", "update", "commented_delete", "bound_delete", "cte_delete"])
def test_first_prepared_read_observes_pending_writes(monetdb_uri: str, committed_row: str, operation: str) -> None:
    with dbapi.connect(monetdb_uri, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        with connection.cursor() as cursor:
            if operation == "insert":
                cursor.execute(f"INSERT INTO {committed_row} VALUES (2, 20)")
                identity, expected = 2, [(20,)]
            elif operation == "update":
                cursor.execute(f"UPDATE {committed_row} SET value = 20 WHERE id = 1")
                identity, expected = 1, [(20,)]
            elif operation == "commented_delete":
                cursor.execute(f"/* deletion */ -- pending write\nDELETE FROM {committed_row} WHERE id = 1")
                identity, expected = 1, []
            elif operation == "cte_delete":
                cursor.execute(
                    f"WITH selected AS (SELECT CAST(? AS INTEGER) AS id) "
                    f"DELETE FROM {committed_row} WHERE id IN (SELECT id FROM selected)",
                    [1],
                )
                identity, expected = 1, []
            else:
                cursor.execute(f"DELETE FROM {committed_row} WHERE id = ?", [1])
                identity, expected = 1, []
        with connection.cursor() as cursor:
            cursor.adbc_prepare(f"SELECT value FROM {committed_row} WHERE id = ?")
            cursor.execute(f"SELECT value FROM {committed_row} WHERE id = ?", [identity])
            assert cursor.fetchall() == expected
        connection.rollback()


@pytest.mark.integration
def test_verified_plan_remains_prepared_after_pending_delete(monetdb_uri: str, committed_row: str) -> None:
    with dbapi.connect(monetdb_uri, conn_kwargs={"adbc.monetdb.prepare_threshold": "1"}) as connection:
        query = f"SELECT value FROM {committed_row} WHERE id = ?"
        with connection.cursor() as cursor:
            cursor.execute(query, [1])
            assert cursor.fetchall() == [(10,)]
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {committed_row} WHERE id = 1")
        with connection.cursor() as cursor:
            cursor.execute(query, [1])
            assert cursor.fetchall() == []
            status = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.PREPARE_STATUS)))
            assert status["path"] == "prepared"
        connection.rollback()


@pytest.mark.integration
def test_repeated_publication_after_replacing_committed_row(monetdb_uri: str, committed_row: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {committed_row} WHERE id = 1")
        for value in (20, 30):
            with connection.cursor() as cursor:
                query = f"SELECT value FROM {committed_row} WHERE id = ?"
                cursor.adbc_prepare(query)
                cursor.execute(query, [1])
                current = cursor.fetchall()
            with connection.cursor() as cursor:
                if current:
                    cursor.execute(f"UPDATE {committed_row} SET value = ? WHERE id = ?", [value, 1])
                else:
                    cursor.execute(f"INSERT INTO {committed_row} VALUES (?, ?)", [1, value])
                assert cursor.rowcount == 1
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT value FROM {committed_row} WHERE id = 1")
            assert cursor.fetchall() == [(30,)]
        connection.rollback()

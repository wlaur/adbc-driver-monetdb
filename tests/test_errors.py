from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import DatabaseOptions, dbapi


def test_dbapi_exception_hierarchy_matches_pep_249() -> None:
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.Error, Exception)
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    for exception_type in (
        dbapi.DataError,
        dbapi.OperationalError,
        dbapi.IntegrityError,
        dbapi.InternalError,
        dbapi.ProgrammingError,
        dbapi.NotSupportedError,
    ):
        assert issubclass(exception_type, dbapi.DatabaseError)


def _uri_with_credentials(uri: str, username: str, password: str) -> str:
    parsed = urlsplit(uri)
    hostname = parsed.hostname
    if hostname is None:
        raise ValueError("integration URI has no hostname")
    if ":" in hostname:
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    credentials = f"{quote(username, safe='')}:{quote(password, safe='')}@"
    return urlunsplit((parsed.scheme, f"{credentials}{hostname}{port}", parsed.path, parsed.query, parsed.fragment))


def _uri_without_credentials(uri: str) -> str:
    parsed = urlsplit(uri)
    hostname = parsed.hostname
    if hostname is None:
        raise ValueError("integration URI has no hostname")
    if ":" in hostname:
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunsplit((parsed.scheme, f"{hostname}{port}", parsed.path, parsed.query, parsed.fragment))


@pytest.mark.integration
def test_dbapi_classifies_server_sqlstates(monetdb_uri: str) -> None:
    suffix = uuid4().hex
    duplicate_table = f"adbc_duplicate_{suffix}"
    constraint_table = f"adbc_constraint_{suffix}"
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f"CREATE TABLE {duplicate_table}(value INT)")
            cursor.execute(f"CREATE TABLE {constraint_table}(value INT PRIMARY KEY)")
            cursor.execute(f"INSERT INTO {constraint_table} VALUES (1)")

            with pytest.raises(adbc_driver_manager.ProgrammingError) as syntax:
                cursor.execute("SELEC 1")
            assert syntax.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
            assert syntax.value.sqlstate == "42000"

            with pytest.raises(adbc_driver_manager.ProgrammingError) as duplicate:
                cursor.execute(f"CREATE TABLE {duplicate_table}(value INT)")
            assert duplicate.value.status_code == adbc_driver_manager.AdbcStatusCode.ALREADY_EXISTS
            assert duplicate.value.sqlstate == "42S01"

            with pytest.raises(adbc_driver_manager.IntegrityError) as constraint:
                cursor.execute(f"INSERT INTO {constraint_table} VALUES (1)")
            assert constraint.value.status_code == adbc_driver_manager.AdbcStatusCode.INTEGRITY
            assert constraint.value.sqlstate == "40002"
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {constraint_table}")
            cursor.execute(f"DROP TABLE IF EXISTS {duplicate_table}")


@pytest.mark.integration
def test_dbapi_classifies_permission_denial(monetdb_uri: str) -> None:
    suffix = uuid4().hex
    username = f"adbc_permission_{suffix}"
    password = f"adbc-{suffix}"
    table = f"adbc_private_{suffix}"
    restricted_uri = _uri_with_credentials(monetdb_uri, username, password)

    with dbapi.connect(monetdb_uri, autocommit=True) as admin:
        admin.execute(f"CREATE TABLE {table}(value INT)")
        admin.execute(f"CREATE USER {username} WITH PASSWORD '{password}' NAME 'ADBC permission test' SCHEMA sys")
    try:
        with dbapi.connect(restricted_uri, autocommit=True) as restricted:
            with pytest.raises(adbc_driver_manager.ProgrammingError) as denied:
                restricted.execute(f"SELECT * FROM sys.{table}")
            assert denied.value.status_code == adbc_driver_manager.AdbcStatusCode.UNAUTHORIZED
            assert denied.value.sqlstate == "42000"
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as admin:
            admin.execute(f"DROP USER IF EXISTS {username}")
            admin.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.integration
def test_authentication_channels_and_password_readback(monetdb_uri: str) -> None:
    suffix = uuid4().hex
    username = f"adbc_auth_{suffix}"
    password = f"adbc-{suffix}:@/%?#[]"
    bare_uri = _uri_without_credentials(monetdb_uri)
    userinfo_uri = _uri_with_credentials(bare_uri, username, password)
    wrong_userinfo_uri = _uri_with_credentials(bare_uri, "wrong-user", "wrong-password")
    credentials = {"username": username, "password": password}

    with dbapi.connect(monetdb_uri, autocommit=True) as admin:
        admin.execute(f"CREATE USER {username} WITH PASSWORD '{password}' NAME 'ADBC auth test' SCHEMA sys")
    try:
        with dbapi.connect(userinfo_uri, autocommit=True) as connection:
            assert connection.execute("SELECT current_user").fetchone() == (username,)
            returned_uri = urlsplit(connection.adbc_database.get_option("uri"))
            assert returned_uri.username is None
            assert returned_uri.password is None
        with dbapi.connect(bare_uri, autocommit=True, db_kwargs=credentials) as connection:
            assert connection.execute("SELECT current_user").fetchone() == (username,)
            with pytest.raises(adbc_driver_manager.NotSupportedError, match="cannot be read back"):
                connection.adbc_database.get_option(DatabaseOptions.PASSWORD)
        with dbapi.connect(wrong_userinfo_uri, autocommit=True, db_kwargs=credentials) as connection:
            assert connection.execute("SELECT current_user").fetchone() == (username,)
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as admin:
            admin.execute(f"DROP USER IF EXISTS {username}")


@pytest.mark.integration
@pytest.mark.parametrize("operation", ["update", "delete", "truncate"])
def test_write_conflict_is_operational_and_recovers_after_rollback(monetdb_uri: str, operation: str) -> None:
    table = f"adbc_conflict_{uuid4().hex}"
    statements = {
        "update": f"UPDATE {table} SET value=30 WHERE id=1",
        "delete": f"DELETE FROM {table} WHERE id=1",
        "truncate": f"TRUNCATE TABLE {table}",
    }
    expected_rows = {"update": [(1, 30), (2, 2)], "delete": [(2, 2)], "truncate": []}
    with dbapi.connect(monetdb_uri, autocommit=True) as admin:
        admin.execute(f"CREATE TABLE {table}(id INT PRIMARY KEY, value INT)")
        admin.execute(f"INSERT INTO {table} VALUES (1, 1), (2, 2)")
        try:
            with dbapi.connect(monetdb_uri) as reader, dbapi.connect(monetdb_uri) as writer:
                assert reader.execute(f"SELECT value FROM {table} WHERE id=1").fetchone() == (1,)
                writer.execute(f"UPDATE {table} SET value=20 WHERE id=1")
                writer.commit()
                with pytest.raises(
                    adbc_driver_manager.OperationalError, match="conflict with another transaction"
                ) as conflict:
                    reader.execute(statements[operation])
                assert conflict.value.status_code == adbc_driver_manager.AdbcStatusCode.UNKNOWN
                assert conflict.value.sqlstate == "42000"
                assert ("adbc.monetdb.connection_terminal", b"true") not in conflict.value.details
                reader.rollback()
                reader.execute(statements[operation])
                reader.commit()
                assert (
                    reader.execute(f"SELECT id, value FROM {table} ORDER BY id").fetchall() == expected_rows[operation]
                )
        finally:
            admin.execute(f"DROP TABLE IF EXISTS {table}")

import os
import socket
import sys
from pathlib import Path

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import DatabaseOptions, __version__, dbapi

SESSION_QUERY = (
    "SELECT hostname, application, client, clientpid, remark FROM sys.sessions WHERE sessionid = current_sessionid()"
)


@pytest.mark.integration
def test_default_client_info_identifies_the_driver(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        row = connection.execute(SESSION_QUERY).fetchone()
        assert row is not None
        hostname, application, client, clientpid, remark = row

        assert hostname == socket.gethostname()
        assert application == Path(sys.argv[0]).name
        assert isinstance(client, str)
        assert client.startswith(f"adbc_driver_monetdb {__version__} / monetdb-rust ")
        assert clientpid == os.getpid()
        assert remark is None
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_INFO) == "true"
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_REMARK) == ""
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_APPLICATION) == Path(sys.argv[0]).name


@pytest.mark.integration
def test_database_client_info_options_override_uri_values(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    uri = f"{monetdb_uri}{separator}client_application=from-uri&client_remark=uri-remark"
    with dbapi.connect(
        uri,
        db_kwargs={
            DatabaseOptions.CLIENT_APPLICATION: "from-option",
            DatabaseOptions.CLIENT_REMARK: "option-remark",
        },
    ) as connection:
        row = connection.execute(SESSION_QUERY).fetchone()
        assert row is not None
        assert row[1] == "from-option"
        assert row[4] == "option-remark"
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_APPLICATION) == "from-option"
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_REMARK) == "option-remark"


@pytest.mark.integration
def test_uri_client_info_values_are_read_back(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    uri = f"{monetdb_uri}{separator}client_application=uri-app&client_remark=uri-remark"
    with dbapi.connect(uri) as connection:
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_APPLICATION) == "uri-app"
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_REMARK) == "uri-remark"


@pytest.mark.integration
def test_client_info_can_be_disabled(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    with dbapi.connect(f"{monetdb_uri}{separator}client_info=false") as connection:
        assert connection.execute(SESSION_QUERY).fetchone() == (None, None, None, None, None)
        assert connection.adbc_database.get_option(DatabaseOptions.CLIENT_INFO) == "false"


@pytest.mark.integration
def test_client_remark_can_be_updated_after_connect(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        connection.execute("CALL sys.setclientinfo('ClientRemark', 'phase 2')")
        row = connection.execute(SESSION_QUERY).fetchone()
        assert row is not None
        assert row[4] == "phase 2"


@pytest.mark.parametrize(
    "option",
    [DatabaseOptions.CLIENT_APPLICATION, DatabaseOptions.CLIENT_REMARK],
)
def test_client_info_database_options_reject_newlines(option: DatabaseOptions) -> None:
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="must not contain newlines") as caught:
        dbapi.connect(
            "monetdb://127.0.0.1:1/test",
            db_kwargs={option: "first\nsecond"},
        )
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT


def test_client_prefix_is_not_a_driver_uri_option() -> None:
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="client_prefix") as caught:
        dbapi.connect("monetdb://127.0.0.1:1/test?client_prefix=impersonate")
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT


def test_python_client_info_enums_match_native_option_names() -> None:
    assert DatabaseOptions.CLIENT_APPLICATION == "adbc.monetdb.client_application"
    assert DatabaseOptions.CLIENT_REMARK == "adbc.monetdb.client_remark"
    assert DatabaseOptions.CLIENT_INFO == "adbc.monetdb.client_info"

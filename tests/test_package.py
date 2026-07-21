import os
import socket
from pathlib import Path
from threading import Thread

import adbc_driver_manager
import adbc_driver_manager.dbapi as manager_dbapi
import pytest

import adbc_driver_monetdb
import adbc_driver_monetdbs
import adbc_driver_monetdbs.dbapi as secure_dbapi
from adbc_driver_monetdb import dbapi


def test_version() -> None:
    assert adbc_driver_monetdb.__version__


def test_driver_path_exists() -> None:
    assert Path(adbc_driver_monetdb.driver_path()).exists()
    from adbc_driver_monetdb import _native

    assert _native.__adbc_entrypoint__ == adbc_driver_monetdb.ENTRYPOINT


def test_dbapi_module_surface() -> None:
    assert dbapi.apilevel == "2.0"
    assert dbapi.paramstyle == "qmark"
    assert dbapi.threadsafety == 1
    assert dbapi.Connection is manager_dbapi.Connection
    assert dbapi.Error is manager_dbapi.Error
    assert adbc_driver_monetdbs.ENTRYPOINT == adbc_driver_monetdb.ENTRYPOINT
    assert adbc_driver_monetdbs.driver_path is adbc_driver_monetdb.driver_path
    assert secure_dbapi.connect is dbapi.connect


def test_connect_rejects_invalid_uri() -> None:
    with pytest.raises(adbc_driver_manager.ProgrammingError):
        dbapi.connect("not-a-monetdb-uri")


def test_connect_rejects_unreachable_tcp_without_poisoning_driver() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        with pytest.raises(adbc_driver_manager.OperationalError, match=r"refused|connect|Connection"):
            dbapi.connect(f"monetdb://127.0.0.1:{port}/test?connect_timeout=1")
    with pytest.raises(adbc_driver_manager.ProgrammingError):
        dbapi.connect("still-not-a-uri")


def test_connect_rejects_unreachable_localhost_as_io() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with pytest.raises(adbc_driver_manager.OperationalError) as caught:
        dbapi.connect(f"monetdb://localhost:{port}/test?connect_timeout=2")
    assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.IO


@pytest.mark.skipif(os.name == "nt", reason="Unix sockets are not available on Windows")
def test_connect_rejects_missing_unix_socket(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sock"
    with pytest.raises(adbc_driver_manager.OperationalError):
        dbapi.connect(f"monetdb:///test?sock={missing}")
    with pytest.raises(adbc_driver_manager.ProgrammingError):
        dbapi.connect("still-not-a-uri")


def test_connect_reports_tls_handshake_failure_cleanly() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(10)
        port = listener.getsockname()[1]

        def accept_one() -> None:
            connection, _ = listener.accept()
            connection.close()

        thread = Thread(target=accept_one)
        thread.start()
        with pytest.raises(adbc_driver_manager.OperationalError):
            dbapi.connect(f"monetdbs://127.0.0.1:{port}/test?connect_timeout=1")
        thread.join(timeout=10)
        assert not thread.is_alive()

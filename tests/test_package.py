import os
import socket
from pathlib import Path

import adbc_driver_manager
import pytest

import adbc_driver_monetdb
from adbc_driver_monetdb import dbapi


def test_version() -> None:
    assert adbc_driver_monetdb.__version__


def test_driver_path_exists() -> None:
    assert Path(adbc_driver_monetdb.driver_path()).exists()


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


@pytest.mark.skipif(os.name == "nt", reason="Unix sockets are not available on Windows")
def test_connect_rejects_missing_unix_socket(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sock"
    with pytest.raises(adbc_driver_manager.OperationalError, match=r"No such file|not found|cannot find"):
        dbapi.connect(f"monetdb:///test?sock={missing}")


def test_connect_reports_disabled_tls_cleanly() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        with pytest.raises(adbc_driver_manager.ProgrammingError, match=r"TLS.*not.*enabled|TLS.*unsupported"):
            dbapi.connect(f"monetdbs://127.0.0.1:{port}/test?connect_timeout=1")

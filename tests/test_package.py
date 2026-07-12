from pathlib import Path

import pytest

import adbc_driver_monetdb
from adbc_driver_monetdb import dbapi


def test_version() -> None:
    assert adbc_driver_monetdb.__version__


def test_driver_path_exists() -> None:
    assert Path(adbc_driver_monetdb.driver_path()).exists()


def test_connect_reports_not_implemented() -> None:
    # exercises the full chain (python shim -> driver manager -> dlopen -> FFI ->
    # driver skeleton) without needing a server: connection setup is the first
    # unimplemented step and must surface its error message verbatim
    with pytest.raises(Exception, match="not implemented yet"):
        dbapi.connect("monetdb://monetdb:monetdb@localhost:50000/test")

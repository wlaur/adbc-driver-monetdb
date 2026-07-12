from pathlib import Path

import pytest

import adbc_driver_monetdb
from adbc_driver_monetdb import dbapi


def test_version() -> None:
    assert adbc_driver_monetdb.__version__


def test_driver_path_exists() -> None:
    assert Path(adbc_driver_monetdb.driver_path()).exists()


def test_connect_rejects_invalid_uri() -> None:
    with pytest.raises(Exception, match="relative URL without a base"):
        dbapi.connect("not-a-monetdb-uri")

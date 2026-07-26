import os

os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_storage"

import time

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import dbapi


def _wait_for_monetdb(uri: str) -> None:
    separator = "&" if "?" in uri else "?"
    readiness_uri = f"{uri}{separator}connect_timeout=2"
    for _ in range(60):
        try:
            dbapi.connect(readiness_uri).close()
            break
        except adbc_driver_manager.OperationalError:
            time.sleep(2)
    else:
        pytest.fail(f"MonetDB did not become ready at {uri} within 120 seconds")


@pytest.fixture(scope="session")
def monetdb_uri() -> str:
    """URI of a running MonetDB server for integration tests.

    Start the pinned native ARM64 image with:
    docker compose -f compose.yaml up -d
    """
    uri = os.environ.get("MONETDB_TEST_URI")
    if uri is None:
        pytest.skip("MONETDB_TEST_URI is not set")
    _wait_for_monetdb(uri)
    return uri


@pytest.fixture(scope="session")
def remote_monetdb() -> tuple[str, str, str, str]:
    test_uri = os.environ.get("MONETDB_REMOTE_TEST_URI")
    server_uri = os.environ.get("MONETDB_REMOTE_SERVER_URI")
    if test_uri is None or server_uri is None:
        pytest.skip("MONETDB_REMOTE_TEST_URI and MONETDB_REMOTE_SERVER_URI are not set")
    _wait_for_monetdb(test_uri)
    return (
        test_uri,
        server_uri,
        os.environ.get("MONETDB_REMOTE_USER", "monetdb"),
        os.environ.get("MONETDB_REMOTE_PASSWORD", "monetdb"),
    )

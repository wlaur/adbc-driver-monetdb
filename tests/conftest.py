import os

os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_storage"

import time

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import dbapi


@pytest.fixture(scope="session")
def monetdb_uri() -> str:
    """URI of a running MonetDB server for integration tests.

    Start one with: docker compose -f compose.yaml up -d
    """
    uri = os.environ.get("MONETDB_TEST_URI")
    if uri is None:
        pytest.skip("MONETDB_TEST_URI is not set")
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
    return uri

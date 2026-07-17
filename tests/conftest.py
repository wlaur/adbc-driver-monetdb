import os

import pytest


@pytest.fixture(scope="session")
def monetdb_uri() -> str:
    """URI of a running MonetDB server for integration tests.

    Start one with: docker compose up -d
    """
    uri = os.environ.get("MONETDB_TEST_URI")
    if uri is None:
        pytest.skip("MONETDB_TEST_URI is not set")
    return uri

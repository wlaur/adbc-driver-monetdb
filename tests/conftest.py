import os

import pytest


@pytest.fixture(scope="session")
def monetdb_uri() -> str:
    """URI of a running MonetDB server for integration tests.

    Start one with:
        docker run -d -p 50000:50000 -e MDB_DB_ADMIN_PASS=monetdb \
            -e MDB_CREATE_DBS=test monetdb/monetdb:Dec2025-SP3
    """
    uri = os.environ.get("MONETDB_TEST_URI")
    if uri is None:
        pytest.skip("MONETDB_TEST_URI is not set")
    return uri

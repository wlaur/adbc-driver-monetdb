import os

import pytest


@pytest.fixture(scope="session")
def monetdb_uri() -> str:
    """URI of a running MonetDB server for integration tests.

    Start one with:
        docker run -d --platform linux/amd64 -p 50000:50000 \
            -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=test \
            monetdb/monetdb:Dec2025-SP3@sha256:a71e6e8c8402beadc51aebf944b465ee5b185c7ae4a9e6808b5d9133ee921786
    """
    uri = os.environ.get("MONETDB_TEST_URI")
    if uri is None:
        pytest.skip("MONETDB_TEST_URI is not set")
    return uri

import pytest
from adbc_drivers_validation.tests import ingest as ingest_tests  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import get_quirks

pytestmark = pytest.mark.integration


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    ingest_tests.generate_tests([get_quirks()], metafunc)


class TestIngest(ingest_tests.TestIngest):
    pass

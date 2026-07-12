import pytest
from adbc_drivers_validation.tests import query as query_tests  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import get_quirks

pytestmark = pytest.mark.integration


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    query_tests.generate_tests([get_quirks()], metafunc)


class TestQuery(query_tests.TestQuery):
    pass

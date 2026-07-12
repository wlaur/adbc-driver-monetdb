import pytest
from adbc_drivers_validation.tests import connection as connection_tests  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import get_quirks

pytestmark = pytest.mark.integration


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    connection_tests.generate_tests([get_quirks()], metafunc)


class TestConnection(connection_tests.TestConnection):
    pass

import pytest
from adbc_drivers_validation.tests import statement as statement_tests  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import get_quirks

pytestmark = pytest.mark.integration


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    statement_tests.generate_tests([get_quirks()], metafunc)


class TestStatement(statement_tests.TestStatement):
    pass

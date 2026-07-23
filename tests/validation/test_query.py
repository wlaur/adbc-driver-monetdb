import pytest
from adbc_drivers_validation.tests import query as query_tests  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import get_quirks


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if metafunc.function.__name__ not in {"test_lint_query", "test_show_queries"}:
        metafunc.definition.add_marker(pytest.mark.integration)
    query_tests.generate_tests([get_quirks()], metafunc)


class TestQuery(query_tests.TestQuery):
    pass

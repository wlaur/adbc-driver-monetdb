import pytest
from adbc_drivers_validation import model  # pyright: ignore[reportMissingTypeStubs]
from adbc_drivers_validation.tests import conftest as validation_conftest  # pyright: ignore[reportMissingTypeStubs]
from adbc_drivers_validation.tests.conftest import (  # pyright: ignore[reportMissingTypeStubs]  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from adbc_driver_monetdb import driver_path as monetdb_driver_path

from .monetdb import get_quirks


def pytest_addoption(parser: pytest.Parser) -> None:
    validation_conftest.pytest_addoption(parser)


@pytest.fixture(scope="session")
def driver(request: pytest.FixtureRequest) -> model.DriverQuirks:
    assert str(request.param).startswith("monetdb:")
    return get_quirks()


@pytest.fixture(scope="session")
def driver_path(driver: model.DriverQuirks) -> str:
    del driver
    return monetdb_driver_path()

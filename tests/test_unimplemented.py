from typing import cast

import pytest

from adbc_driver_monetdb import dbapi


@pytest.mark.integration
@pytest.mark.xfail(strict=False, reason="MAPI has no query cancellation mechanism")
def test_statement_cancel(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.adbc_cancel()


@pytest.mark.integration
@pytest.mark.xfail(strict=False, reason="MAPI exposes one sequential result channel")
def test_partitioned_results(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        partitions, schema = cast(
            tuple[list[bytes], object],
            cursor.adbc_execute_partitions("SELECT 1"),  # pyright: ignore[reportUnknownMemberType]
        )
        assert partitions
        assert schema is not None


@pytest.mark.integration
@pytest.mark.xfail(strict=False, reason="MonetDB does not accept Substrait plans")
def test_substrait_plan(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(b"substrait-plan")  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
@pytest.mark.xfail(strict=False, reason="ADBC statistics metadata is not implemented")
def test_statistics(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        connection.adbc_get_statistics().read_all()  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.integration
@pytest.mark.xfail(strict=False, reason="ADBC statistic names are not implemented")
def test_statistic_names(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        connection.adbc_get_statistic_names().read_all()  # pyright: ignore[reportUnknownMemberType]

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import dbapi


@pytest.mark.integration
def test_statement_cancel(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="no active operation") as caught:
            cursor.adbc_cancel()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_STATE


@pytest.mark.integration
def test_partitioned_results(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="partitioned results") as caught:
            cursor.adbc_execute_partitions("SELECT 1")  # pyright: ignore[reportUnknownMemberType]
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_substrait_plan(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="Substrait plans") as caught:
            cursor.execute(b"substrait-plan")  # pyright: ignore[reportUnknownMemberType]
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_statistics(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="get_statistics") as caught:
            connection.adbc_get_statistics().read_all()  # pyright: ignore[reportUnknownMemberType]
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_statistic_names(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="get_statistic_names") as caught:
            connection.adbc_get_statistic_names().read_all()  # pyright: ignore[reportUnknownMemberType]
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED

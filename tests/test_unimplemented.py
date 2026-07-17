import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import dbapi


def _assert_not_implemented(error: adbc_driver_manager.Error) -> None:
    assert error.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("adbc.connection.readonly", "true"),
        ("adbc.connection.transaction.isolation_level", "adbc.connection.transaction.isolation.serializable"),
        ("adbc.connection.catalog", "other"),
    ],
)
def test_waived_connection_options_are_not_implemented(monetdb_uri: str, key: str, value: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with pytest.raises(adbc_driver_manager.NotSupportedError) as caught:
            connection.adbc_connection.set_options(**{key: value})
        _assert_not_implemented(caught.value)


@pytest.mark.integration
def test_read_only_false_is_accepted(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        connection.adbc_connection.set_options(**{"adbc.connection.readonly": "false"})
        assert connection.adbc_connection.get_option("adbc.connection.readonly") == "false"


@pytest.mark.integration
def test_cross_catalog_ingest_is_not_implemented(monetdb_uri: str) -> None:
    batch = pa.record_batch({"value": [1]})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError) as caught:
            cursor.adbc_ingest("never_created", batch, catalog_name="other")
        _assert_not_implemented(caught.value)


@pytest.mark.integration
def test_incremental_execution_is_not_implemented(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError) as caught:
            cursor.adbc_statement.set_options(**{"adbc.statement.exec.incremental": "true"})
        _assert_not_implemented(caught.value)


@pytest.mark.integration
@pytest.mark.parametrize("key", ["adbc.statement.exec.progress", "adbc.statement.exec.max_progress"])
def test_progress_is_not_implemented(monetdb_uri: str, key: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError) as caught:
            cursor.adbc_statement.get_option_float(key)
        _assert_not_implemented(caught.value)


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
            cursor.adbc_execute_partitions("SELECT 1")
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_substrait_plan(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="Substrait plans") as caught:
            cursor.execute(b"substrait-plan")
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_statistics(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="get_statistics") as caught:
            connection.adbc_get_statistics().read_all()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


@pytest.mark.integration
def test_statistic_names(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="get_statistic_names") as caught:
            connection.adbc_get_statistic_names().read_all()
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED

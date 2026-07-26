from typing import Any, cast

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
def test_legacy_inet_ingest_is_not_implemented(monetdb_uri: str) -> None:
    field = pa.field(
        "address",
        pa.string(),
        metadata={b"ARROW:extension:name": b"monetdb.inet"},
    )
    batch = pa.record_batch([pa.array(["127.0.0.1"])], schema=pa.schema([field]))
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="INET") as caught:
            cursor.adbc_ingest("never_created", batch, mode="create")
        _assert_not_implemented(caught.value)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("extension_name", "values"),
    [
        ("monetdb.inet4", ["127.0.0.1", "192.0.2.10", None]),
        ("monetdb.inet6", ["::1", "2001:db8::1", None]),
    ],
)
def test_inet_address_ingest_round_trips(
    monetdb_uri: str,
    extension_name: str,
    values: list[str | None],
) -> None:
    field = pa.field(
        "address",
        pa.string(),
        metadata={b"ARROW:extension:name": extension_name.encode()},
    )
    batch = pa.record_batch([pa.array(values)], schema=pa.schema([field]))
    table_name = f"roundtrip_{extension_name.removeprefix('monetdb.')}"
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        try:
            assert cursor.adbc_ingest(table_name, batch, mode="create") == len(values)
            cursor.execute(f"SELECT address FROM {table_name} ORDER BY address NULLS LAST")
            result = cursor.fetch_arrow_table()
            actual = result.column("address").to_pylist()
            assert actual[-1] is None
            assert set(actual[:-1]) == set(values[:-1])
            result_field = cast(Any, result.schema).field("address")
            assert result_field.metadata == {b"ARROW:extension:name": extension_name.encode()}
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


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
def test_idle_statement_cancel_is_idempotent(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.adbc_cancel()
        cursor.adbc_cancel()
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)


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

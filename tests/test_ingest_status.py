from uuid import uuid4

import adbc_driver_manager
import polars as pl
import pytest

from adbc_driver_monetdb import dbapi


@pytest.mark.integration
def test_ingest_schema_mismatch_status_depends_on_mode(monetdb_uri: str) -> None:
    table = f"adbc_ingest_status_{uuid4().hex}"
    initial = pl.DataFrame({"value": [1]})
    mismatched = pl.DataFrame({"value": [1], "extra": [2]})

    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            assert cursor.adbc_ingest(table, initial, mode="create_append") == 1

            with pytest.raises(adbc_driver_manager.ProgrammingError) as create_append:
                cursor.adbc_ingest(
                    table,
                    mismatched,
                    mode="create_append",
                )
            assert create_append.value.status_code == adbc_driver_manager.AdbcStatusCode.ALREADY_EXISTS

            with pytest.raises(adbc_driver_manager.ProgrammingError) as append:
                cursor.adbc_ingest(
                    table,
                    mismatched,
                    mode="append",
                )
            assert append.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
        finally:
            cursor.execute(f'DROP TABLE IF EXISTS "{table}"')


@pytest.mark.integration
def test_ingest_accepts_unquoted_identifier_case_folding(monetdb_uri: str) -> None:
    table = f"adbc_ingest_case_{uuid4().hex}"
    frame = pl.DataFrame({"MixedCase": [1, 2, 3]})

    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f'CREATE TABLE "{table}" (MixedCase BIGINT)')
            assert cursor.adbc_ingest(table, frame, mode="append") == 3
            cursor.execute(f'SELECT mixedcase FROM "{table}" ORDER BY mixedcase')
            assert cursor.fetch_arrow_table().column("mixedcase").to_pylist() == [1, 2, 3]
        finally:
            cursor.execute(f'DROP TABLE IF EXISTS "{table}"')

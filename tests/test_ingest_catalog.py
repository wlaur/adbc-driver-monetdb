import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

TYPE_MATRIX_DDL = """
    i8 TINYINT,
    i16 SMALLINT,
    i32 INT,
    i64 BIGINT,
    i128 HUGEINT,
    b BOOLEAN,
    r REAL,
    d DOUBLE,
    d2 DECIMAL(2, 0),
    d4 DECIMAL(4, 2),
    d9 DECIMAL(9, 2),
    d18 DECIMAL(18, 6),
    d38 DECIMAL(38, 10),
    c CHAR(8),
    v VARCHAR(32),
    cl CLOB,
    bl BLOB,
    u UUID,
    j JSON,
    url URL,
    ip4 INET4,
    ip6 INET6,
    da DATE,
    ti TIME,
    ts TIMESTAMP,
    titz TIME WITH TIME ZONE,
    tstz TIMESTAMP WITH TIME ZONE,
    im INTERVAL MONTH,
    id INTERVAL DAY,
    isec INTERVAL SECOND
"""


def _empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([], schema=schema)


@pytest.mark.integration
def test_append_catalog_descriptors_equal_result_header_type_matrix(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS ingest_catalog_matrix")
        cursor.execute(f"CREATE TABLE ingest_catalog_matrix({TYPE_MATRIX_DDL})")
        schema = cursor.adbc_execute_schema("SELECT * FROM ingest_catalog_matrix WHERE FALSE")
        with connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: 0}) as ingest:
            assert (
                ingest.adbc_ingest(
                    "ingest_catalog_matrix",
                    _empty_batch(schema),
                    mode="append",
                )
                == 0
            )


@pytest.mark.integration
def test_append_catalog_resolution_matches_monetdb_name_resolution(monetdb_uri: str) -> None:
    int_batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
    string_batch = pa.record_batch({"value": pa.array(["tmp"])})
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS ingest_catalog_schema CASCADE")
        cursor.execute("CREATE SCHEMA ingest_catalog_schema")
        cursor.execute("CREATE TABLE ingest_catalog_schema.target(value INT)")
        cursor.execute("CREATE TABLE ingest_catalog_schema.schema_only(value INT)")
        cursor.execute("CREATE LOCAL TEMPORARY TABLE target(value VARCHAR(8)) ON COMMIT PRESERVE ROWS")
        cursor.execute('CREATE TABLE ingest_catalog_schema."Mixed Table"("value" INT)')
        connection.commit()

        assert cursor.adbc_ingest("target", string_batch, mode="append", temporary=True) == 1
        assert (
            cursor.adbc_ingest(
                "target",
                int_batch,
                mode="append",
                db_schema_name="ingest_catalog_schema",
            )
            == 1
        )
        assert (
            cursor.adbc_ingest(
                "Mixed Table",
                int_batch,
                mode="append",
                db_schema_name="ingest_catalog_schema",
            )
            == 1
        )
        assert cursor.execute("SELECT value FROM tmp.target").fetchall() == [("tmp",)]
        assert cursor.execute("SELECT value FROM ingest_catalog_schema.target").fetchall() == [(1,)]

        cursor.execute("SET SCHEMA ingest_catalog_schema")
        assert cursor.adbc_ingest("schema_only", int_batch, mode="append") == 1
        assert cursor.execute("SELECT COUNT(*) FROM ingest_catalog_schema.schema_only").fetchone() == (1,)


@pytest.mark.integration
def test_append_catalog_rejects_missing_tables_and_views_clearly(monetdb_uri: str) -> None:
    batch = pa.record_batch({"value": pa.array([1], type=pa.int32())})
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP VIEW IF EXISTS ingest_catalog_view")
        cursor.execute("CREATE VIEW ingest_catalog_view AS SELECT 1 AS value")
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="not a writable table"):
            cursor.adbc_ingest("ingest_catalog_view", batch, mode="append")
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="does not exist"):
            cursor.adbc_ingest("ingest_catalog_missing", batch, mode="append")

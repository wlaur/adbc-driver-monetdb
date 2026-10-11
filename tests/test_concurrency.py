from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("prefetch", ["true", "false"])
def test_stream_and_transaction_keep_snapshot_across_concurrent_commit(monetdb_uri: str, prefetch: str) -> None:
    table = f"snapshot_{uuid4().hex}"
    with dbapi.connect(monetdb_uri, autocommit=True) as admin:
        admin.execute(f"CREATE TABLE {table}(id INT, value INT)")
        admin.execute(f"INSERT INTO {table} SELECT value, 1 FROM sys.generate_series(0,10000)")
        try:
            with dbapi.connect(monetdb_uri) as reader, dbapi.connect(monetdb_uri) as writer:
                with reader.cursor(
                    adbc_stmt_kwargs={
                        StatementOptions.READ_BATCH_ROWS: "1024",
                        StatementOptions.READ_PREFETCH: prefetch,
                    }
                ) as cursor:
                    cursor.execute(f"SELECT id, value FROM {table} ORDER BY id")
                    stream = cursor.fetch_record_batch()
                    first = stream.read_next_batch()
                    writer.execute(f"UPDATE {table} SET value=2")
                    writer.commit()
                    result = pa.Table.from_batches([first, *stream])
                    assert result.column("id").to_pylist() == list(range(10000))
                    assert result.column("value").to_pylist() == [1] * 10000
                assert reader.execute(f"SELECT MIN(value), MAX(value) FROM {table}").fetchone() == (1, 1)
                reader.commit()
                assert reader.execute(f"SELECT MIN(value), MAX(value) FROM {table}").fetchone() == (2, 2)
        finally:
            admin.execute(f"DROP TABLE {table}")


def test_parallel_staged_ingests_keep_connection_state_independent(monetdb_uri: str) -> None:
    tables = [f"parallel_ingest_{uuid4().hex}" for _ in range(4)]
    barrier = Barrier(len(tables), timeout=10)
    with dbapi.connect(monetdb_uri, autocommit=True) as admin:
        for table in tables:
            admin.execute(f"CREATE TABLE {table}(id INT PRIMARY KEY, value INT)")
        try:

            def worker(table: str) -> None:
                with dbapi.connect(monetdb_uri) as connection:
                    barrier.wait()
                    for index in range(5):
                        batch = pa.record_batch(
                            {
                                "id": pa.array(range(index * 6, (index + 1) * 6), type=pa.int32()),
                                "value": pa.array([index] * 6, type=pa.int32()),
                            }
                        )
                        with connection.cursor(adbc_stmt_kwargs={StatementOptions.WRITE_BATCH_ROWS: "2"}) as cursor:
                            assert cursor.adbc_ingest(table, batch, mode="append") == 6
                        connection.commit()
                    assert connection.execute(f"SELECT COUNT(*), SUM(value) FROM {table}").fetchone() == (30, 60)

            with ThreadPoolExecutor(max_workers=len(tables)) as pool:
                list(pool.map(worker, tables))
        finally:
            for table in tables:
                admin.execute(f"DROP TABLE {table}")

import gc
import multiprocessing
import os
import sys
import time
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from adbc_driver_monetdb import ConnectionOptions, StatementOptions, dbapi


def _positive_environment_integer(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _peak_rss_bytes() -> int | None:
    if sys.platform == "win32":
        return None
    import resource

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


def _stage_parquet(
    parquet: Path,
    rows: int,
    columns: int,
    batch_rows: int,
) -> tuple[float, int | None]:
    started = time.perf_counter()
    schema = pa.schema([(f"v{index:04d}", pa.float32()) for index in range(columns)])
    with pq.ParquetWriter(parquet, schema, compression="zstd") as writer:
        for start in range(0, rows, batch_rows):
            current_rows = min(batch_rows, rows - start)
            shared = pa.array(
                (float((start + index) % 1_000) for index in range(current_rows)),
                type=pa.float32(),
            )
            table = pa.Table.from_arrays([shared] * columns, schema=schema)
            writer.write_table(table, row_group_size=current_rows)
            del table, shared
            gc.collect()
            pa.default_memory_pool().release_unused()
    return time.perf_counter() - started, _peak_rss_bytes()


@pytest.mark.integration
@pytest.mark.local_only
def test_local_30_gib_staged_parquet_ingest(
    monetdb_uri: str,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    if os.environ.get("MONETDB_RUN_LOCAL_BENCHMARK") != "1":
        pytest.skip("set MONETDB_RUN_LOCAL_BENCHMARK=1 to run the local benchmark")

    rows = _positive_environment_integer("MONETDB_BENCH_ROWS", 8_000_000)
    columns = _positive_environment_integer("MONETDB_BENCH_COLUMNS", 1_000)
    batch_rows = _positive_environment_integer("MONETDB_BENCH_BATCH_ROWS", 100_000)
    logical_bytes = rows * columns * 4
    parquet = tmp_path / "local-30-gib.parquet"
    request.addfinalizer(lambda: parquet.unlink(missing_ok=True))

    with dbapi.connect(
        monetdb_uri,
        autocommit=True,
        conn_kwargs={ConnectionOptions.WRITE_BATCH_ROWS: str(batch_rows)},
    ) as preflight:
        assert preflight.adbc_connection.get_option_int(str(ConnectionOptions.WRITE_BATCH_ROWS)) == batch_rows
        with preflight.cursor() as cursor:
            assert cursor.adbc_statement.get_option_int(str(StatementOptions.WRITE_BATCH_ROWS)) == batch_rows

    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
    ) as executor:
        stage_seconds, stage_peak_rss = executor.submit(
            _stage_parquet,
            parquet,
            rows,
            columns,
            batch_rows,
        ).result()

    parquet_file = pq.ParquetFile(parquet)

    def record_batches() -> Iterator[pa.RecordBatch]:
        for batch in parquet_file.iter_batches(  # pyright: ignore[reportUnknownMemberType]
            batch_size=batch_rows,
            use_threads=False,
        ):
            yield batch
            del batch
            gc.collect()
            pa.default_memory_pool().release_unused()

    reader = pa.RecordBatchReader.from_batches(parquet_file.schema_arrow, record_batches())
    with dbapi.connect(
        monetdb_uri,
        autocommit=True,
        conn_kwargs={ConnectionOptions.WRITE_BATCH_ROWS: str(batch_rows)},
    ) as connection:
        connection.execute("DROP TABLE IF EXISTS local_large_ingest_benchmark")
        try:
            ingest_started = time.perf_counter()
            with connection.cursor() as cursor:
                affected = cursor.adbc_ingest(
                    "local_large_ingest_benchmark",
                    reader,
                    mode="create",
                )
            ingest_seconds = time.perf_counter() - ingest_started
            result = connection.execute(
                f"""
                SELECT COUNT(*), MIN(v0000), MAX(v{columns - 1:04d}),
                       CAST(SUM(CAST(v{columns // 2:04d} AS BIGINT)) AS BIGINT)
                FROM local_large_ingest_benchmark
                """
            ).fetchone()
        finally:
            connection.execute("DROP TABLE IF EXISTS local_large_ingest_benchmark")

    cycles, remainder = divmod(rows, 1_000)
    expected_sum = cycles * 999 * 1_000 // 2 + remainder * (remainder - 1) // 2
    expected_batches = (rows + batch_rows - 1) // batch_rows
    assert affected == rows
    assert result == (rows, 0.0, 999.0, expected_sum)

    peak_rss = _peak_rss_bytes()
    stage_rss_text = "unavailable" if stage_peak_rss is None else f"{stage_peak_rss / 2**30:.2f} GiB"
    ingest_rss_text = "unavailable" if peak_rss is None else f"{peak_rss / 2**30:.2f} GiB"
    print(
        f"\nlogical={logical_bytes / 2**30:.2f} GiB "
        f"parquet={parquet.stat().st_size / 2**20:.1f} MiB "
        f"batches={expected_batches} stage={stage_seconds:.2f}s "
        f"ingest={ingest_seconds:.2f}s peak_stage_rss={stage_rss_text} "
        f"peak_ingest_rss={ingest_rss_text}"
    )

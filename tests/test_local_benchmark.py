import gc
import multiprocessing
import os
import statistics
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pymonetdb
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


def _median_microseconds_per_call(
    call: Callable[[int], None],
    *,
    rounds: int,
    calls: int,
    reset: Callable[[], object] | None = None,
) -> float:
    call(25)
    samples: list[float] = []
    gc.disable()
    try:
        for _ in range(rounds):
            if reset is not None:
                reset()
            started = time.perf_counter_ns()
            call(calls)
            samples.append((time.perf_counter_ns() - started) / calls / 1_000)
    finally:
        gc.enable()
    return statistics.median(samples)


@pytest.mark.integration
@pytest.mark.local_only
def test_local_short_query_latency_against_pymonetdb(monetdb_uri: str) -> None:
    if os.environ.get("MONETDB_RUN_LATENCY_BENCHMARK") != "1":
        pytest.skip("set MONETDB_RUN_LATENCY_BENCHMARK=1 to run the latency benchmark")

    rounds = _positive_environment_integer("MONETDB_BENCH_ROUNDS", 7)
    calls = _positive_environment_integer("MONETDB_BENCH_CALLS", 500)
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as adbc_connection,
        pymonetdb.connect(monetdb_uri, autocommit=True) as pymonetdb_connection,
    ):
        adbc_cursor = adbc_connection.cursor()
        pymonetdb_cursor = pymonetdb_connection.cursor()
        for table in ("latency_adbc", "latency_pymonetdb"):
            adbc_cursor.execute(f"DROP TABLE IF EXISTS {table}")
            adbc_cursor.execute(f"CREATE TABLE {table}(a INT, b INT)")

        def adbc_insert(count: int) -> None:
            for value in range(count):
                adbc_cursor.execute(
                    "INSERT INTO latency_adbc VALUES (?, ?)",
                    (value, value + 1),
                )

        def pymonetdb_insert(count: int) -> None:
            for value in range(count):
                pymonetdb_cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                    "INSERT INTO latency_pymonetdb VALUES (%s, %s)",
                    (value, value + 1),
                )

        def adbc_select(count: int) -> None:
            for _ in range(count):
                adbc_cursor.execute("SELECT value FROM sys.generate_series(1, 11)")
                assert len(adbc_cursor.fetchall()) == 10

        def pymonetdb_select(count: int) -> None:
            for _ in range(count):
                pymonetdb_cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT value FROM sys.generate_series(1, 11)"
                )
                assert len(pymonetdb_cursor.fetchall()) == 10  # pyright: ignore[reportUnknownArgumentType]

        def reset_pymonetdb_insert() -> None:
            pymonetdb_cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                "TRUNCATE TABLE latency_pymonetdb"
            )

        measurements = {
            "adbc_insert_2param": _median_microseconds_per_call(
                adbc_insert,
                rounds=rounds,
                calls=calls,
                reset=lambda: adbc_cursor.execute("TRUNCATE TABLE latency_adbc"),
            ),
            "pymonetdb_insert_2param": _median_microseconds_per_call(
                pymonetdb_insert,
                rounds=rounds,
                calls=calls,
                reset=reset_pymonetdb_insert,
            ),
            "adbc_select_10": _median_microseconds_per_call(
                adbc_select,
                rounds=rounds,
                calls=calls,
            ),
            "pymonetdb_select_10": _median_microseconds_per_call(
                pymonetdb_select,
                rounds=rounds,
                calls=calls,
            ),
        }
        for table in ("latency_adbc", "latency_pymonetdb"):
            adbc_cursor.execute(f"DROP TABLE {table}")

    print("\n" + " ".join(f"{name}={microseconds:.1f}us" for name, microseconds in measurements.items()))


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

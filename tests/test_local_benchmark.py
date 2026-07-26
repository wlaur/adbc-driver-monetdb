import datetime
import gc
import multiprocessing
import os
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
import pymonetdb
import pytest

from adbc_driver_monetdb import ConnectionOptions, StatementOptions, dbapi


@dataclass(frozen=True)
class DatabaseDiskUsage:
    farm_bytes: int
    filesystem_used_bytes: int


def _benchmark_container() -> str:
    configured = os.environ.get("MONETDB_BENCH_CONTAINER")
    if configured is not None:
        return configured
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", "monetdb"],
        cwd=Path(__file__).parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    container = result.stdout.strip()
    if not container:
        raise RuntimeError("the local MonetDB Compose service is not running; start it or set MONETDB_BENCH_CONTAINER")
    return container


def _database_disk_usage(container: str) -> DatabaseDiskUsage:
    farm = subprocess.run(
        ["docker", "exec", container, "du", "-sb", "/var/monetdb5/dbfarm"],
        check=True,
        capture_output=True,
        text=True,
    )
    filesystem = subprocess.run(
        ["docker", "exec", container, "df", "-B1", "--output=used", "/var/monetdb5/dbfarm"],
        check=True,
        capture_output=True,
        text=True,
    )
    return DatabaseDiskUsage(
        farm_bytes=int(farm.stdout.split()[0]),
        filesystem_used_bytes=int(filesystem.stdout.splitlines()[-1]),
    )


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
        adbc_cursor.execute("DROP TABLE IF EXISTS latency_point")
        adbc_cursor.execute("CREATE TABLE latency_point(id INT PRIMARY KEY)")
        adbc_cursor.execute("INSERT INTO latency_point SELECT value FROM sys.generate_series(0, 100000)")

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

        def adbc_select_one(count: int) -> None:
            for _ in range(count):
                adbc_cursor.execute("SELECT 1")
                assert adbc_cursor.fetchone() == (1,)

        def pymonetdb_select_one(count: int) -> None:
            for _ in range(count):
                pymonetdb_cursor.execute("SELECT 1")  # pyright: ignore[reportUnknownMemberType]
                assert pymonetdb_cursor.fetchone() == (1,)

        def adbc_point_query(count: int) -> None:
            for value in range(count):
                adbc_cursor.execute(
                    "SELECT id FROM latency_point WHERE id = ?",
                    (value % 100_000,),
                )
                assert adbc_cursor.fetchone() == (value % 100_000,)

        def pymonetdb_point_query(count: int) -> None:
            for value in range(count):
                pymonetdb_cursor.execute(  # pyright: ignore[reportUnknownMemberType]
                    "SELECT id FROM latency_point WHERE id = %s",
                    (value % 100_000,),
                )
                assert pymonetdb_cursor.fetchone() == (value % 100_000,)

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
            "adbc_select_1": _median_microseconds_per_call(
                adbc_select_one,
                rounds=rounds,
                calls=calls,
            ),
            "pymonetdb_select_1": _median_microseconds_per_call(
                pymonetdb_select_one,
                rounds=rounds,
                calls=calls,
            ),
            "adbc_parameterized_point": _median_microseconds_per_call(
                adbc_point_query,
                rounds=rounds,
                calls=calls,
            ),
            "pymonetdb_parameterized_point": _median_microseconds_per_call(
                pymonetdb_point_query,
                rounds=rounds,
                calls=calls,
            ),
        }
        for table in ("latency_adbc", "latency_pymonetdb"):
            adbc_cursor.execute(f"DROP TABLE {table}")
        adbc_cursor.execute("DROP TABLE latency_point")

    print("\n" + " ".join(f"{name}={microseconds:.1f}us" for name, microseconds in measurements.items()))


@pytest.mark.integration
@pytest.mark.local_only
def test_local_executemany_batching(monetdb_uri: str) -> None:
    if os.environ.get("MONETDB_RUN_LATENCY_BENCHMARK") != "1":
        pytest.skip("set MONETDB_RUN_LATENCY_BENCHMARK=1 to run the latency benchmark")

    rounds = _positive_environment_integer("MONETDB_BENCH_ROUNDS", 7)
    rows = _positive_environment_integer("MONETDB_BENCH_ROWS", 4_096)
    parameters = [(value, value + 1) for value in range(rows)]
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS executemany_latency")
        cursor.execute("CREATE TABLE executemany_latency(a INT, b INT)")

        def reset() -> None:
            cursor.execute("TRUNCATE TABLE executemany_latency")

        def repeated_execute(_: int) -> None:
            for row in parameters:
                cursor.execute("INSERT INTO executemany_latency VALUES (?, ?)", row)

        def batched_executemany(_: int) -> None:
            cursor.executemany("INSERT INTO executemany_latency VALUES (?, ?)", parameters)

        repeated = _median_microseconds_per_call(
            repeated_execute,
            rounds=rounds,
            calls=1,
            reset=reset,
        )
        batched = _median_microseconds_per_call(
            batched_executemany,
            rounds=rounds,
            calls=1,
            reset=reset,
        )
        cursor.execute("SELECT COUNT(*), SUM(a), SUM(b) FROM executemany_latency")
        assert cursor.fetchone() == (rows, rows * (rows - 1) // 2, rows * (rows + 1) // 2)
        cursor.execute("DROP TABLE executemany_latency")

    print(
        f"\nexecutemany_rows={rows} repeated_execute={repeated / 1_000:.1f}ms "
        f"batched_executemany={batched / 1_000:.1f}ms speedup={repeated / batched:.1f}x"
    )


@pytest.mark.integration
@pytest.mark.local_only
def test_local_temporal_materialization_against_pymonetdb(monetdb_uri: str) -> None:
    if os.environ.get("MONETDB_RUN_TEMPORAL_BENCHMARK") != "1":
        pytest.skip("set MONETDB_RUN_TEMPORAL_BENCHMARK=1 to run the temporal benchmark")

    rounds = _positive_environment_integer("MONETDB_BENCH_ROUNDS", 3)
    rows = _positive_environment_integer("MONETDB_BENCH_ROWS", 1_000_000)
    queries: dict[str, tuple[str, type[object]]] = {
        "timestamp": (
            (
                "SELECT TIMESTAMP '2024-01-01 00:00:00' + value * INTERVAL '0.001' SECOND "
                f"FROM sys.generate_series(0, {rows})"
            ),
            datetime.datetime,
        ),
        "timestamptz": (
            (
                "SELECT TIMESTAMP WITH TIME ZONE '2024-01-01 00:00:00+00:00' "
                "+ value * INTERVAL '0.001' SECOND "
                f"FROM sys.generate_series(0, {rows})"
            ),
            datetime.datetime,
        ),
        "time": (
            f"SELECT TIME '00:00:00' + MOD(value, 86400) * INTERVAL '1' SECOND FROM sys.generate_series(0, {rows})",
            datetime.time,
        ),
    }

    with (
        dbapi.connect(monetdb_uri, autocommit=True) as adbc_connection,
        pymonetdb.connect(monetdb_uri, autocommit=True) as pymonetdb_connection,
        adbc_connection.cursor() as adbc_cursor,
    ):
        pymonetdb_cursor = pymonetdb_connection.cursor()

        def adbc_fetch(query: str) -> list[tuple[object, ...]]:
            adbc_cursor.execute(query)
            return adbc_cursor.fetchall()

        def pymonetdb_fetch(query: str) -> list[tuple[object, ...]]:
            pymonetdb_cursor.execute(query)  # pyright: ignore[reportUnknownMemberType]
            return cast(list[tuple[object, ...]], pymonetdb_cursor.fetchall())

        fetchers: dict[str, Callable[[str], list[tuple[object, ...]]]] = {
            "adbc": adbc_fetch,
            "pymonetdb": pymonetdb_fetch,
        }
        measurements: dict[str, dict[str, list[float]]] = {
            query_name: {client: [] for client in fetchers} for query_name in queries
        }

        for query_index, (query_name, (query, expected_type)) in enumerate(queries.items()):
            for fetch in fetchers.values():
                warmup = fetch(query.replace(f"generate_series(0, {rows})", "generate_series(0, 1000)"))
                assert len(warmup) == 1_000
                assert isinstance(warmup[0][0], expected_type)

            for round_index in range(rounds):
                order = ("adbc", "pymonetdb") if (query_index + round_index) % 2 == 0 else ("pymonetdb", "adbc")
                for client in order:
                    gc.collect()
                    gc.disable()
                    try:
                        started = time.perf_counter()
                        result = fetchers[client](query)
                        elapsed = time.perf_counter() - started
                    finally:
                        gc.enable()
                    assert len(result) == rows
                    assert isinstance(result[0][0], expected_type)
                    assert isinstance(result[-1][0], expected_type)
                    measurements[query_name][client].append(elapsed)
                    del result

    rendered: list[str] = [f"temporal_rows={rows}"]
    for query_name, clients in measurements.items():
        adbc = statistics.median(clients["adbc"])
        pymonetdb_seconds = statistics.median(clients["pymonetdb"])
        rendered.append(
            f"{query_name}_adbc={adbc:.3f}s "
            f"{query_name}_pymonetdb={pymonetdb_seconds:.3f}s "
            f"{query_name}_ratio={adbc / pymonetdb_seconds:.2f}x"
        )
    print("\n" + " ".join(rendered))


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
    benchmark_container = _benchmark_container()
    disk_baseline = _database_disk_usage(benchmark_container)
    disk_samples = [disk_baseline]
    stop_disk_sampling = threading.Event()

    def sample_disk_usage() -> None:
        while not stop_disk_sampling.wait(0.25):
            disk_samples.append(_database_disk_usage(benchmark_container))

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
            disk_sampler = threading.Thread(target=sample_disk_usage, daemon=True)
            disk_sampler.start()
            ingest_started = time.perf_counter()
            try:
                with connection.cursor() as cursor:
                    affected = cursor.adbc_ingest(
                        "local_large_ingest_benchmark",
                        reader,
                        mode="create",
                    )
                ingest_seconds = time.perf_counter() - ingest_started
            finally:
                stop_disk_sampling.set()
                disk_sampler.join()
                disk_samples.append(_database_disk_usage(benchmark_container))
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
    peak_farm_bytes = max(sample.farm_bytes for sample in disk_samples)
    peak_filesystem_bytes = max(sample.filesystem_used_bytes for sample in disk_samples)
    final_disk = disk_samples[-1]
    print(
        f"\nlogical={logical_bytes / 2**30:.2f} GiB "
        f"parquet={parquet.stat().st_size / 2**20:.1f} MiB "
        f"batches={expected_batches} stage={stage_seconds:.2f}s "
        f"ingest={ingest_seconds:.2f}s peak_stage_rss={stage_rss_text} "
        f"peak_ingest_rss={ingest_rss_text} "
        f"farm_growth={(final_disk.farm_bytes - disk_baseline.farm_bytes) / 2**30:.2f} GiB "
        f"peak_farm_growth={(peak_farm_bytes - disk_baseline.farm_bytes) / 2**30:.2f} GiB "
        f"peak_filesystem_growth="
        f"{(peak_filesystem_bytes - disk_baseline.filesystem_used_bytes) / 2**30:.2f} GiB"
    )

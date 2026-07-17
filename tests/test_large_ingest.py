import gc
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from adbc_driver_monetdb import ConnectionOptions, StatementOptions, dbapi


@pytest.mark.integration
def test_tall_polars_insert_from_staged_parquet(
    monetdb_uri: str,
    tmp_path: Path,
) -> None:
    rows = 10_000_000
    parquet = tmp_path / "tall.parquet"
    pl.DataFrame(
        {
            "row_id": pl.arange(0, rows, eager=True, dtype=pl.Int32),
            "value": pl.arange(0, rows, eager=True, dtype=pl.Int32).cast(pl.Float32),
        }
    ).write_parquet(parquet)

    frame = pl.scan_parquet(parquet).collect(engine="streaming")
    with dbapi.connect(
        monetdb_uri,
        conn_kwargs={ConnectionOptions.WRITE_BATCH_ROWS: "100000"},
    ) as connection:
        try:
            assert connection.adbc_connection.get_option_int(str(ConnectionOptions.WRITE_BATCH_ROWS)) == 100_000
            affected = frame.write_database(
                "large_tall_ingest",
                connection,
                if_table_exists="replace",
                engine="adbc",
            )
            result = pl.read_database(
                """
                SELECT COUNT(*) AS n,
                       MIN(row_id) AS min_id,
                       MAX(row_id) AS max_id,
                       CAST(SUM(CAST(value AS BIGINT)) AS BIGINT) AS total
                FROM large_tall_ingest
                """,
                connection,
            ).row(0)
        finally:
            connection.execute("DROP TABLE IF EXISTS large_tall_ingest")

    assert affected == rows
    assert result == (rows, 0, rows - 1, rows * (rows - 1) // 2)


@pytest.mark.integration
def test_wide_parquet_scan_streams_realistic_arrow_batches(
    monetdb_uri: str,
    tmp_path: Path,
) -> None:
    rows = 200_000
    columns = 1_000
    batch_rows = 100_000
    parquet = tmp_path / "wide.parquet"

    shared = pa.array((float(index % 1_000) for index in range(rows)), type=pa.float32())
    pq.write_table(  # pyright: ignore[reportUnknownMemberType]
        pa.table({f"v{index:04d}": shared for index in range(columns)}),
        parquet,
        compression="zstd",
        row_group_size=batch_rows,
    )
    del shared
    gc.collect()

    lazy = pl.scan_parquet(parquet)
    schema = lazy.collect_schema().to_arrow()
    observed_batch_rows: list[int] = []

    def batches() -> Iterator[pa.RecordBatch]:
        for frame in lazy.collect_batches(chunk_size=batch_rows, engine="streaming"):
            observed_batch_rows.append(frame.height)
            record_batches = frame.rechunk().to_arrow().to_batches(max_chunksize=batch_rows)
            assert len(record_batches) == 1
            yield record_batches[0]

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    with dbapi.connect(monetdb_uri) as connection:
        try:
            with connection.cursor(adbc_stmt_kwargs={StatementOptions.WRITE_BATCH_ROWS: str(batch_rows)}) as cursor:
                affected = cursor.adbc_ingest("large_wide_ingest", reader, mode="replace")
            result = connection.execute(
                """
                SELECT COUNT(*), MIN(v0000), MAX(v0999), SUM(CAST(v0500 AS BIGINT))
                FROM large_wide_ingest
                """
            ).fetchone()
        finally:
            connection.execute("DROP TABLE IF EXISTS large_wide_ingest")

    assert affected == rows
    assert observed_batch_rows == [batch_rows, batch_rows]
    assert result == (rows, 0.0, 999.0, 99_900_000)

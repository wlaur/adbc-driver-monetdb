import json
import random
from typing import cast

import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

MIB = 1024 * 1024


def _stats(cursor: dbapi.Cursor) -> dict[str, object]:
    return json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))


def _random_int64_batch(rows: int) -> pa.RecordBatch:
    values = pa.py_buffer(random.Random(20260728).randbytes(rows * 8))
    array = pa.Int64Array.from_buffers(pa.int64(), rows, [None, values])
    return pa.record_batch({"value": array})


@pytest.mark.integration
def test_explicit_byte_window_is_the_incompressible_authority(monetdb_uri: str) -> None:
    batch = _random_int64_batch(10_000_000)
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WRITE_WINDOW_BYTES: 32 * MIB,
            }
        ) as cursor,
    ):
        assert cursor.adbc_ingest("ingest_window_authority", batch, mode="replace") == batch.num_rows
        stats = _stats(cursor)

    assert stats["path"] == "copy"
    assert stats["window_budget_bytes"] == 32 * MIB
    assert stats["physical_window_budget_bytes"] == 32 * MIB
    assert stats["incompressible_window_budget_bytes"] == 32 * MIB
    assert stats["copy_count"] == 3
    window_bytes = cast(list[int], stats["window_bytes"])
    assert all(value <= 32 * MIB for value in window_bytes)
    assert "arrow" in cast(list[str], stats["window_storage"])


@pytest.mark.integration
def test_ingest_storage_stats_account_for_lz4_raw_and_arrow(monetdb_uri: str) -> None:
    timestamps = pa.array(
        (1_700_000_000_000_000 + index * 1_000 for index in range(1_000_000)),
        type=pa.timestamp("us"),
    )
    cases = [
        ("lz4", pa.record_batch({"value": timestamps})),
        ("raw", _random_int64_batch(1_000_000)),
        ("arrow", _random_int64_batch(10_000_000)),
    ]
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        for expected_storage, batch in cases:
            with connection.cursor(
                adbc_stmt_kwargs={
                    StatementOptions.INGEST_INSERT_ROWS: 0,
                    StatementOptions.WRITE_WINDOW_BYTES: 128 * MIB,
                    StatementOptions.WIRE_COMPRESSION: "none",
                }
            ) as cursor:
                assert (
                    cursor.adbc_ingest(
                        f"ingest_storage_{expected_storage}",
                        batch,
                        mode="replace",
                    )
                    == batch.num_rows
                )
                stats = _stats(cursor)
            assert stats["window_storage"] == [expected_storage]
            assert stats["window_wire_compression"] == ["none"]
            assert isinstance(stats["stored_bytes"], int)
            assert isinstance(stats["encoded_bytes"], int)
            if expected_storage == "lz4":
                assert stats["stored_bytes"] < stats["encoded_bytes"]
            else:
                assert stats["stored_bytes"] == stats["encoded_bytes"]
            assert isinstance(stats["peak_window_physical_bytes"], int)
            assert isinstance(stats["peak_prefetch_physical_bytes"], int)
            for field in (
                "window_physical_stored_bytes",
                "window_staging_bytes",
                "window_retained_arrow_pinned_bytes",
                "window_scratch_bytes",
            ):
                assert len(cast(list[int], stats[field])) == cast(int, stats["copy_count"])
            assert stats["peak_prefetch_physical_bytes"] >= stats["peak_window_physical_bytes"]


@pytest.mark.integration
def test_auto_uses_shuffled_storage_without_mislabeling_wire_compression(
    monetdb_uri: str,
) -> None:
    rows = 1_000_000
    batch = pa.record_batch(
        {
            "time": pa.array(
                (1_700_000_000_000_000 + index * 1_000 for index in range(rows)),
                type=pa.timestamp("us"),
            ),
        }
    )
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.INGEST_INSERT_ROWS: 0,
                StatementOptions.WIRE_COMPRESSION: "auto",
            }
        ) as cursor,
    ):
        assert cursor.adbc_ingest("auto_shuffle_storage", batch, mode="replace") == rows
        stats = _stats(cursor)

    assert stats["window_storage"] == ["lz4"]
    assert stats["window_wire_compression"] == ["none"]
    assert cast(int, stats["stored_bytes"]) < cast(int, stats["encoded_bytes"])

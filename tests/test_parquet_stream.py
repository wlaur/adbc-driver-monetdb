import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from adbc_driver_monetdb import DEFAULT_PARQUET_RECLAIM_BYTES, ParquetArrowStream


def write_parquet(path: Path) -> pa.Table:
    table = pa.table(
        {
            "id": pa.array(range(11), type=pa.int64()),
            "flag": pa.array([index % 2 == 0 for index in range(11)]),
        }
    )
    pq.write_table(table, path, row_group_size=4)  # pyright: ignore[reportUnknownMemberType]
    return table


def test_parquet_stream_reads_bounded_batches_by_row_group(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    expected = write_parquet(path)

    stream = ParquetArrowStream(path, batch_rows=3)
    reader = pa.RecordBatchReader.from_stream(stream)
    batches = list(reader)

    assert [batch.num_rows for batch in batches] == [3, 1, 3, 1, 3]
    assert pa.Table.from_batches(batches) == expected
    assert stream.batch_rows == 3
    assert stream.num_rows == 11
    assert stream.row_groups == (0, 1, 2)
    assert not stream.use_threads
    assert stream.rows_read == 11
    assert stream.schema == expected.schema
    assert stream.reclaim_bytes == DEFAULT_PARQUET_RECLAIM_BYTES


def test_parquet_stream_can_select_row_groups(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    expected = write_parquet(path).slice(4, 7)

    with ParquetArrowStream(path, row_groups=range(1, 3)) as stream:
        actual = pa.Table.from_batches(pa.RecordBatchReader.from_stream(stream))

    assert actual == expected
    assert stream.num_rows == 7
    assert stream.rows_read == 7


def test_parquet_stream_reinterprets_integer_epoch_columns(tmp_path: Path) -> None:
    path = tmp_path / "epochs.parquet"
    table = pa.table(
        {
            "at": pa.array([0, 1, None, 86_400], type=pa.int32()),
            "on": pa.array([0, 1, None, 20_000], type=pa.uint16()),
            "value": pa.array([1.0, 2.0, 3.0, 4.0]),
        }
    )
    pq.write_table(table, path, row_group_size=2)  # pyright: ignore[reportUnknownMemberType]

    with ParquetArrowStream(path, epoch_columns={"at": "s", "on": "day"}) as stream:
        assert stream.schema.types[:2] == [pa.timestamp("s"), pa.date32()]
        actual = pa.Table.from_batches(pa.RecordBatchReader.from_stream(stream))
        assert stream.epoch_columns == {"at": "s", "on": "day"}

    assert actual.schema.types[:2] == [pa.timestamp("s"), pa.date32()]
    assert actual.column("at").cast(pa.int64()).to_pylist() == [0, 1, None, 86_400]
    assert actual.column("on").cast(pa.int32()).to_pylist() == [0, 1, None, 20_000]
    assert actual.column("value") == table.column("value")


def test_parquet_stream_leaves_caller_owned_file_objects_open(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    expected = write_parquet(path)
    source = io.BytesIO(path.read_bytes())

    with ParquetArrowStream(source) as stream:
        actual = pa.Table.from_batches(pa.RecordBatchReader.from_stream(stream))

    assert actual == expected
    assert not source.closed


def test_parquet_stream_releases_arrow_pool_once_after_all_row_groups(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CountingPool:
        calls = 0

        def release_unused(self) -> None:
            self.calls += 1

    class CountingArrow:
        pool = CountingPool()

        @classmethod
        def default_memory_pool(cls) -> CountingPool:
            return cls.pool

    path = tmp_path / "input.parquet"
    write_parquet(path)
    stream = ParquetArrowStream(path, batch_rows=3)
    monkeypatch.setattr(stream, "_arrow", CountingArrow)

    assert sum(batch.num_rows for batch in pa.RecordBatchReader.from_stream(stream)) == 11
    assert CountingArrow.pool.calls == 1


def test_parquet_stream_reclaims_long_streams_at_decoded_byte_intervals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CountingPool:
        calls = 0

        def release_unused(self) -> None:
            self.calls += 1

    class CountingArrow:
        pool = CountingPool()

        @classmethod
        def default_memory_pool(cls) -> CountingPool:
            return cls.pool

    path = tmp_path / "input.parquet"
    write_parquet(path)
    stream = ParquetArrowStream(path, batch_rows=3, reclaim_bytes=1)
    monkeypatch.setattr(stream, "_arrow", CountingArrow)

    assert sum(batch.num_rows for batch in pa.RecordBatchReader.from_stream(stream)) == 11
    assert CountingArrow.pool.calls == 4


def test_parquet_stream_rejects_invalid_configuration(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    write_parquet(path)

    with pytest.raises(ValueError, match="batch_rows must be positive"):
        ParquetArrowStream(path, batch_rows=0)
    with pytest.raises(ValueError, match="batch_bytes must be positive"):
        ParquetArrowStream(path, batch_bytes=0)
    with pytest.raises(ValueError, match="reclaim_bytes must be positive or None"):
        ParquetArrowStream(path, reclaim_bytes=0)
    with pytest.raises(IndexError, match="row group 3"):
        ParquetArrowStream(path, row_groups=[3])
    with pytest.raises(ValueError, match="epoch column 'missing'"):
        ParquetArrowStream(path, epoch_columns={"missing": "s"})
    with pytest.raises(TypeError, match="epoch column 'flag' must be integer"):
        ParquetArrowStream(path, epoch_columns={"flag": "s"})


def test_parquet_stream_close_releases_an_unconsumed_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CountingPool:
        calls = 0

        def release_unused(self) -> None:
            self.calls += 1

    class CountingArrow:
        pool = CountingPool()

        @classmethod
        def default_memory_pool(cls) -> CountingPool:
            return cls.pool

    path = tmp_path / "input.parquet"
    write_parquet(path)
    stream = ParquetArrowStream(path, batch_rows=3)
    monkeypatch.setattr(stream, "_arrow", CountingArrow)
    reader = pa.RecordBatchReader.from_stream(stream)

    assert reader.read_next_batch().num_rows == 3
    stream.close()
    stream.close()

    remaining = list(reader)

    assert sum(batch.num_rows for batch in remaining) < 8
    assert stream.rows_read < stream.num_rows
    assert CountingArrow.pool.calls == 1

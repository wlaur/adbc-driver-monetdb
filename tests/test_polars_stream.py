from collections.abc import Callable
from threading import Event
from unittest.mock import Mock

import polars as pl
import pyarrow as pa

from adbc_driver_monetdb import DEFAULT_POLARS_BATCH_BYTES, DEFAULT_POLARS_BATCH_ROWS, PolarsArrowStream


def test_polars_arrow_stream_preserves_schema_and_counts_rows() -> None:
    frame = pl.LazyFrame({"id": range(10), "label": ["value"] * 10})
    stream = PolarsArrowStream(frame, batch_rows=3)

    reader = pa.RecordBatchReader.from_stream(stream)
    batches = list(reader)

    assert sum(batch.num_rows for batch in batches) == 10
    assert all(batch.schema == frame.collect_schema().to_arrow() for batch in batches)
    assert stream.rows_read == 10


def test_polars_arrow_stream_applies_backpressure_and_default_batch_bound() -> None:
    frame = Mock()
    produced_second = Event()

    def sink_batches(receive: Callable[[pl.DataFrame], bool], **kwargs: object) -> None:
        assert kwargs["chunk_size"] == DEFAULT_POLARS_BATCH_ROWS
        assert not receive(pl.DataFrame({"value": [1]}))
        produced_second.set()
        assert not receive(pl.DataFrame({"value": [2]}))

    frame.sink_batches.side_effect = sink_batches
    frame.collect_schema.return_value = pl.Schema({"value": pl.Int64})
    stream = PolarsArrowStream(frame)
    reader = pa.RecordBatchReader.from_stream(stream)

    assert next(reader).column("value").to_pylist() == [1]
    assert not produced_second.is_set()
    assert next(reader).column("value").to_pylist() == [2]
    assert produced_second.is_set()
    assert list(reader) == []


def test_polars_arrow_stream_bounds_wide_fixed_width_batches_by_bytes() -> None:
    frame = pl.LazyFrame({f"value_{index}": [index] for index in range(1_000)})

    stream = PolarsArrowStream(frame)

    estimated_row_bytes = 1_000 * 9
    assert stream.batch_rows == DEFAULT_POLARS_BATCH_BYTES // estimated_row_bytes
    assert stream.batch_rows < DEFAULT_POLARS_BATCH_ROWS

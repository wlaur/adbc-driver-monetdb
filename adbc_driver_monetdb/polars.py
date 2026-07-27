from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from queue import Full, Queue
from threading import Event, Thread
from types import CapsuleType
from typing import TYPE_CHECKING, Protocol, cast

from adbc_driver_monetdb.arrow import (
    DEFAULT_ARROW_BATCH_BYTES,
    DEFAULT_ARROW_BATCH_ROWS,
    recommended_arrow_batch_rows,
)

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

DEFAULT_POLARS_BATCH_ROWS: int = DEFAULT_ARROW_BATCH_ROWS
DEFAULT_POLARS_BATCH_BYTES: int = DEFAULT_ARROW_BATCH_BYTES


@dataclass(frozen=True, slots=True)
class _ProducerError:
    error: BaseException


class _ArrowStreamReader(Protocol):
    def __arrow_c_stream__(self, requested_schema: CapsuleType | None = None) -> CapsuleType: ...


class PolarsArrowStream:
    """Backpressured Arrow stream over a Polars lazy query."""

    def __init__(
        self,
        frame: pl.LazyFrame,
        *,
        batch_rows: int = DEFAULT_POLARS_BATCH_ROWS,
        batch_bytes: int = DEFAULT_POLARS_BATCH_BYTES,
    ) -> None:
        if batch_rows <= 0:
            raise ValueError("batch_rows must be positive")
        if batch_bytes <= 0:
            raise ValueError("batch_bytes must be positive")

        try:
            import polars as pl
            import pyarrow as pa
        except ImportError as exc:
            raise ModuleNotFoundError(
                "PolarsArrowStream requires the optional Polars dependencies; install 'adbc-driver-monetdb[polars]'"
            ) from exc

        self._frame = frame
        self._compat_level = pl.CompatLevel.newest()
        self._rows_read = 0
        schema = frame.collect_schema().to_arrow(compat_level=self._compat_level)
        self._batch_rows = recommended_arrow_batch_rows(
            schema,
            max_rows=batch_rows,
            max_bytes=batch_bytes,
        )
        self._reader = cast(_ArrowStreamReader, pa.RecordBatchReader.from_batches(schema, self._iter_batches()))

    @property
    def batch_rows(self) -> int:
        return self._batch_rows

    @property
    def rows_read(self) -> int:
        return self._rows_read

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> CapsuleType:
        if requested_schema is not None and not isinstance(requested_schema, CapsuleType):
            raise TypeError("requested_schema must be an Arrow schema capsule")
        return self._reader.__arrow_c_stream__(requested_schema)

    def _iter_batches(self) -> Iterator[pa.RecordBatch]:
        queue: Queue[pl.DataFrame | _ProducerError | None] = Queue(maxsize=1)
        consumed = Event()
        stopped = Event()

        def put(item: pl.DataFrame | _ProducerError | None) -> bool:
            while not stopped.is_set():
                try:
                    queue.put(item, timeout=0.1)
                    return True
                except Full:
                    pass
            return False

        def receive(batch: pl.DataFrame) -> bool:
            if not put(batch):
                return True
            while not stopped.is_set():
                if consumed.wait(timeout=0.1):
                    consumed.clear()
                    return False
            return True

        def produce() -> None:
            try:
                self._frame.sink_batches(
                    receive,
                    chunk_size=self._batch_rows,
                    lazy=False,
                    engine="streaming",
                )
            except BaseException as error:
                put(_ProducerError(error))
            finally:
                put(None)

        producer = Thread(target=produce, name="monetdb-polars-producer", daemon=True)
        producer.start()
        try:
            while (item := queue.get()) is not None:
                if isinstance(item, _ProducerError):
                    raise item.error
                self._rows_read += item.height
                yield from item.to_arrow(compat_level=self._compat_level).to_batches()
                consumed.set()
        finally:
            stopped.set()
            consumed.set()
            producer.join()


__all__ = [
    "DEFAULT_POLARS_BATCH_BYTES",
    "DEFAULT_POLARS_BATCH_ROWS",
    "PolarsArrowStream",
]

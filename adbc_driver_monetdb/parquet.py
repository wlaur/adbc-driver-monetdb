from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from types import CapsuleType
from typing import IO, TYPE_CHECKING, Any, Literal, Protocol, cast

from adbc_driver_monetdb.arrow import (
    DEFAULT_ARROW_BATCH_BYTES,
    DEFAULT_ARROW_BATCH_ROWS,
    recommended_arrow_batch_rows,
)

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.parquet as pq

ParquetSource = str | Path | IO[bytes]
type ParquetEpochUnit = Literal["s", "ms", "us", "ns", "day"]
DEFAULT_PARQUET_RECLAIM_BYTES = 2 * 1024 * 1024 * 1024


class _ArrowStreamReader(Protocol):
    @property
    def schema(self) -> pa.Schema: ...

    def __arrow_c_stream__(self, requested_schema: CapsuleType | None = None) -> CapsuleType: ...

    def close(self) -> None: ...


class ParquetArrowStream:
    """Bounded Arrow stream that decodes one Parquet row group at a time."""

    def __init__(
        self,
        source: ParquetSource,
        *,
        batch_rows: int = DEFAULT_ARROW_BATCH_ROWS,
        batch_bytes: int = DEFAULT_ARROW_BATCH_BYTES,
        row_groups: Sequence[int] | None = None,
        use_threads: bool = False,
        reclaim_bytes: int | None = DEFAULT_PARQUET_RECLAIM_BYTES,
        epoch_columns: Mapping[str, ParquetEpochUnit] | None = None,
    ) -> None:
        if batch_rows <= 0:
            raise ValueError("batch_rows must be positive")
        if batch_bytes <= 0:
            raise ValueError("batch_bytes must be positive")
        if reclaim_bytes is not None and reclaim_bytes <= 0:
            raise ValueError("reclaim_bytes must be positive or None")

        try:
            import pyarrow as pa
            import pyarrow.compute as pc
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ModuleNotFoundError(
                "ParquetArrowStream requires PyArrow; install 'adbc-driver-monetdb[pyarrow]'"
            ) from exc

        owned_source: pa.NativeFile | None = None
        parquet_source: ParquetSource | pa.NativeFile = source
        if isinstance(source, (str, Path)):
            owned_source = pa.OSFile(str(source), "rb")
            parquet_source = owned_source
        try:
            parquet_file = pq.ParquetFile(parquet_source)
        except BaseException:
            if owned_source is not None:
                owned_source.close()
            raise
        selected_row_groups = tuple(range(parquet_file.num_row_groups)) if row_groups is None else tuple(row_groups)
        for row_group in selected_row_groups:
            if row_group < 0 or row_group >= parquet_file.num_row_groups:
                parquet_file.close()
                if owned_source is not None:
                    owned_source.close()
                raise IndexError(f"row group {row_group} is outside [0, {parquet_file.num_row_groups})")

        source_schema = parquet_file.schema_arrow
        transformed_schema = source_schema
        transforms: list[tuple[int, str, pa.DataType, ParquetEpochUnit]] = []
        try:
            for name, unit in (epoch_columns or {}).items():
                index = source_schema.get_field_index(name)
                if index < 0:
                    raise ValueError(f"epoch column {name!r} is not present in the Parquet schema")
                source_field = cast(Any, source_schema).field(index)
                source_type = cast("pa.DataType", source_field.type)
                if not pa.types.is_integer(source_type):
                    raise TypeError(f"epoch column {name!r} must be integer, got {source_type}")
                target_type = pa.date32() if unit == "day" else pa.timestamp(unit)
                target_field = cast(
                    Any,
                    pa.field(
                        name,
                        target_type,
                        nullable=source_field.nullable,
                        metadata=source_field.metadata,
                    ),
                )
                transformed_schema = cast(Any, transformed_schema).set(index, target_field)
                transforms.append((index, name, target_type, unit))
        except BaseException:
            parquet_file.close()
            if owned_source is not None:
                owned_source.close()
            raise

        self._arrow = pa
        self._compute = pc
        self._file: pq.ParquetFile | None = parquet_file
        self._owned_source: pa.NativeFile | None = owned_source
        self._row_groups = selected_row_groups
        self._use_threads = use_threads
        self._reclaim_bytes = reclaim_bytes
        self._epoch_columns: dict[str, ParquetEpochUnit] = dict(epoch_columns or {})
        self._transforms = tuple(transforms)
        self._rows_read = 0
        self._closed = False
        self._num_rows = sum(parquet_file.metadata.row_group(row_group).num_rows for row_group in selected_row_groups)
        self._batch_rows = recommended_arrow_batch_rows(
            transformed_schema,
            max_rows=batch_rows,
            max_bytes=batch_bytes,
        )
        self._reader = cast(
            _ArrowStreamReader,
            pa.RecordBatchReader.from_batches(transformed_schema, self._iter_batches()),
        )

    @property
    def schema(self) -> pa.Schema:
        return self._reader.schema

    @property
    def batch_rows(self) -> int:
        return self._batch_rows

    @property
    def num_rows(self) -> int:
        return self._num_rows

    @property
    def row_groups(self) -> tuple[int, ...]:
        return self._row_groups

    @property
    def use_threads(self) -> bool:
        return self._use_threads

    @property
    def rows_read(self) -> int:
        return self._rows_read

    @property
    def reclaim_bytes(self) -> int | None:
        return self._reclaim_bytes

    @property
    def epoch_columns(self) -> Mapping[str, ParquetEpochUnit]:
        return self._epoch_columns

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._reader.close()
        self._close_file()

    def _close_file(self) -> None:
        parquet_file = self._file
        self._file = None
        if parquet_file is not None:
            parquet_file.close()
        owned_source = self._owned_source
        self._owned_source = None
        if owned_source is not None:
            owned_source.close()

    def __enter__(self) -> ParquetArrowStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> CapsuleType:
        if requested_schema is not None and not isinstance(requested_schema, CapsuleType):
            raise TypeError("requested_schema must be an Arrow schema capsule")
        return self._reader.__arrow_c_stream__(requested_schema)

    def _iter_batches(self) -> Iterator[pa.RecordBatch]:
        decoded_since_reclaim = 0
        try:
            for row_group in self._row_groups:
                parquet_file = self._file
                if parquet_file is None:
                    return
                for batch in parquet_file.iter_batches(  # pyright: ignore[reportUnknownMemberType]
                    batch_size=self._batch_rows,
                    row_groups=[row_group],
                    use_threads=self._use_threads,
                ):
                    for index, name, target_type, unit in self._transforms:
                        integer_type = self._arrow.int32() if unit == "day" else self._arrow.int64()
                        values = cast(Any, self._compute).cast(batch.column(index), integer_type, safe=True)
                        batch = cast(
                            "pa.RecordBatch",
                            cast(Any, batch).set_column(index, name, values.view(target_type)),
                        )
                    self._rows_read += batch.num_rows
                    yield batch
                decoded_since_reclaim += parquet_file.metadata.row_group(row_group).total_byte_size
                if self._reclaim_bytes is not None and decoded_since_reclaim >= self._reclaim_bytes:
                    self._arrow.default_memory_pool().release_unused()
                    decoded_since_reclaim = 0
        finally:
            self._close_file()
            self._arrow.default_memory_pool().release_unused()


__all__ = [
    "DEFAULT_PARQUET_RECLAIM_BYTES",
    "ParquetArrowStream",
    "ParquetEpochUnit",
    "ParquetSource",
]

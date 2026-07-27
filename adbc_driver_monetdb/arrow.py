from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import pyarrow as pa

DEFAULT_ARROW_BATCH_ROWS: int = 65_536
DEFAULT_ARROW_BATCH_BYTES: int = 16 * 1024 * 1024


def recommended_arrow_batch_rows(
    schema: pa.Schema,
    *,
    max_rows: int = DEFAULT_ARROW_BATCH_ROWS,
    max_bytes: int = DEFAULT_ARROW_BATCH_BYTES,
) -> int:
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")

    try:
        import pyarrow as pa
    except ImportError as exc:
        raise ModuleNotFoundError(
            "recommended_arrow_batch_rows requires PyArrow; install 'adbc-driver-monetdb[pyarrow]'"
        ) from exc

    schema_api = cast(Any, schema)
    estimated_row_bytes = sum(
        _estimated_field_bytes(cast("pa.DataType", schema_api.field(index).type), pa) + 1
        for index in range(len(schema))
    )
    return min(max_rows, max(1, max_bytes // max(1, estimated_row_bytes)))


def _estimated_field_bytes(data_type: pa.DataType, arrow_module: object) -> int:
    pa = cast(Any, arrow_module)
    if pa.types.is_boolean(data_type):
        return 1
    if pa.types.is_fixed_size_binary(data_type) or pa.types.is_decimal(data_type):
        byte_width = getattr(data_type, "byte_width", None)
        if isinstance(byte_width, int):
            return byte_width
    if pa.types.is_primitive(data_type):
        return max(1, data_type.bit_width // 8)
    return 32


__all__ = [
    "DEFAULT_ARROW_BATCH_BYTES",
    "DEFAULT_ARROW_BATCH_ROWS",
    "recommended_arrow_batch_rows",
]

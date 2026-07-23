import json
from datetime import UTC, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, cast
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import UUID

import pyarrow as pa
import pymonetdb
import pytest

from adbc_driver_monetdb import dbapi

PROJECTION = r"""
    SELECT TRUE AS b,
           CAST(-127 AS TINYINT) AS i8_min,
           CAST(127 AS TINYINT) AS i8_max,
           CAST(-32767 AS SMALLINT) AS i16_min,
           CAST(32767 AS SMALLINT) AS i16_max,
           CAST(-2147483647 AS INT) AS i32_min,
           CAST(2147483647 AS INT) AS i32_max,
           CAST(-9 AS DECIMAL(2, 0)) AS d2,
           CAST(-1.25 AS DECIMAL(4, 2)) AS d4,
           CAST(1.23 AS DECIMAL(9, 2)) AS d9,
           CAST(123456789012.345678 AS DECIMAL(18, 6)) AS d18,
           CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38, 10)) AS d38,
           TIME '23:59:59.123456' AS time_v,
           TIMESTAMP '2100-02-28 01:02:03.123456' AS timestamp_v,
           TIMESTAMPTZ '2025-01-01 00:30:00+01:00' AS timestamptz_v,
           UUID '444fcb84-9a7d-4fe1-adfa-7eae290328c3' AS uuid_v,
           CAST('127.0.0.1' AS INET4) AS inet4_v,
           CAST('::1' AS INET6) AS inet6_v,
           JSON '{"x":"ä"}' AS json_v,
           CAST(NULL AS STRING) AS null_v
"""

EXPECTED_ROW = (
    True,
    -127,
    127,
    -32767,
    32767,
    -2_147_483_647,
    2_147_483_647,
    Decimal("-9"),
    Decimal("-1.25"),
    Decimal("1.23"),
    Decimal("123456789012.345678"),
    Decimal("1234567890123456789012345678.1234567890"),
    time(23, 59, 59, 123456),
    datetime(2100, 2, 28, 1, 2, 3, 123456),
    datetime(2024, 12, 31, 23, 30, tzinfo=UTC),
    UUID("444fcb84-9a7d-4fe1-adfa-7eae290328c3"),
    "127.0.0.1",
    "::1",
    '{"x":"ä"}',
    None,
)

EXPECTED_TYPES = [
    pa.bool_(),
    pa.int8(),
    pa.int8(),
    pa.int16(),
    pa.int16(),
    pa.int32(),
    pa.int32(),
    pa.decimal128(2, 0),
    pa.decimal128(4, 2),
    pa.decimal128(9, 2),
    pa.decimal128(18, 6),
    pa.decimal128(38, 10),
    pa.time64("us"),
    pa.timestamp("us"),
    pa.timestamp("us", tz="UTC"),
    pa.uuid(),
    pa.string(),
    pa.string(),
    pa.json_(),
    pa.string(),
]

IDENTIFIER_NAMES = [
    'double"quote',
    'double""quote',
    '"""triple_double_start',
    'triple_double_end"""',
    '"""both_double"""',
    "single'quote",
    "single''quote",
    "'''triple_single_start",
    "triple_single_end'''",
    "'''both_single'''",
    "mixed''quotes%",
    'mixed""quotes%',
    "percent%name",
    "multiple%%percent",
    "percent_at_end%",
    '"',
    '""',
    "'",
    "''",
]


class _Description(Protocol):
    name: str


def _pymonetdb_connect(uri: str, *, replysize: str = "1") -> pymonetdb.Connection:
    parsed = urlsplit(uri)
    query = parse_qs(parsed.query)
    username = unquote(parsed.username) if parsed.username is not None else query.get("user", ["monetdb"])[-1]
    password = unquote(parsed.password) if parsed.password is not None else query.get("password", ["monetdb"])[-1]
    return pymonetdb.connect(
        parsed.path.lstrip("/"),
        hostname=parsed.hostname,
        port=parsed.port,
        username=username,
        password=password,
        autocommit=True,
        replysize=replysize,
        tls=parsed.scheme == "monetdbs",
    )


def _pymonetdb_query(
    connection: pymonetdb.Connection,
    query: str,
) -> tuple[list[str], list[tuple[object, ...]]]:
    cursor = connection.cursor()
    cursor.execute(query)  # pyright: ignore[reportUnknownMemberType]
    description = cast(list[_Description] | None, cursor.description)
    assert description is not None
    names = [column.name for column in description]
    rows = cast(list[tuple[object, ...]], cursor.fetchall())
    return names, rows


def _field_metadata(schema: pa.Schema, name: str) -> dict[bytes, bytes] | None:
    return schema.field(name).metadata  # pyright: ignore[reportUnknownMemberType]


def _normalize_value(name: str, value: object) -> object:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC)
    if isinstance(value, IPv4Address | IPv6Address):
        return str(value)
    if name == "json_v":
        parsed = json.loads(value) if isinstance(value, str) else value
        return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    return value


def _normalized_rows(names: list[str], rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    return [tuple(_normalize_value(name, value) for name, value in zip(names, row, strict=True)) for row in rows]


@pytest.mark.integration
@pytest.mark.parametrize(
    ("suffix", "row_count"),
    [
        (" WHERE FALSE", 0),
        ("", 1),
        (" FROM sys.generate_series(1, 4)", 3),
    ],
)
def test_adbc_expected_matrix_then_pymonetdb_common_subset(
    monetdb_uri: str,
    suffix: str,
    row_count: int,
) -> None:
    query = PROJECTION + suffix
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        table = cursor.fetch_arrow_table()
        cursor.execute(query)
        adbc_rows = cursor.fetchall()

    assert table.schema.types == EXPECTED_TYPES
    assert _field_metadata(table.schema, "uuid_v") == {}
    assert _field_metadata(table.schema, "inet4_v") == {b"ARROW:extension:name": b"monetdb.inet4"}
    assert _field_metadata(table.schema, "inet6_v") == {b"ARROW:extension:name": b"monetdb.inet6"}
    assert _field_metadata(table.schema, "json_v") == {}
    assert adbc_rows == [EXPECTED_ROW] * row_count

    reference = _pymonetdb_connect(monetdb_uri)
    try:
        names, pymonetdb_rows = _pymonetdb_query(reference, query)
    finally:
        reference.close()

    assert names == table.schema.names
    common_indices = [index for index, name in enumerate(names) if name != "d38"]
    common_names = [names[index] for index in common_indices]
    expected_common = tuple(EXPECTED_ROW[index] for index in common_indices)
    pymonetdb_common = [tuple(row[index] for index in common_indices) for row in pymonetdb_rows]
    adbc_common = [tuple(row[index] for index in common_indices) for row in adbc_rows]
    assert _normalized_rows(common_names, pymonetdb_common) == [expected_common] * row_count
    assert _normalized_rows(common_names, adbc_common) == _normalized_rows(common_names, pymonetdb_common)


@pytest.mark.integration
@pytest.mark.xfail(
    strict=False,
    reason="pymonetdb preserves SQL quoting in double-quoted cursor.description names",
)
def test_pymonetdb_known_difference_adversarial_identifier_descriptions(
    monetdb_uri: str,
) -> None:
    expressions = ", ".join(
        f'{index} AS "{name.replace(chr(34), chr(34) * 2)}"' for index, name in enumerate(IDENTIFIER_NAMES)
    )
    reference = _pymonetdb_connect(monetdb_uri)
    try:
        names, _ = _pymonetdb_query(reference, f"SELECT {expressions}")
        assert names == IDENTIFIER_NAMES
    finally:
        reference.close()


@pytest.mark.integration
@pytest.mark.xfail(
    strict=False,
    reason="pymonetdb shifts TIMETZ values by the configured session timezone",
)
def test_pymonetdb_known_difference_timetz_normalization(monetdb_uri: str) -> None:
    reference = _pymonetdb_connect(monetdb_uri)
    try:
        _, rows = _pymonetdb_query(reference, "SELECT TIMETZ '12:34:56.123456+00:00'")
        value = rows[0][0]
        assert isinstance(value, time)
        assert value.hour == 12
        offset = value.utcoffset()
        assert offset is not None
        assert offset.total_seconds() == 0
    finally:
        reference.close()


@pytest.mark.integration
@pytest.mark.xfail(
    strict=False,
    reason="pymonetdb mixes JSON strings and decoded structures across result windows",
)
def test_pymonetdb_known_difference_json_window_representation(monetdb_uri: str) -> None:
    reference = _pymonetdb_connect(monetdb_uri)
    try:
        _, rows = _pymonetdb_query(
            reference,
            'SELECT JSON \'{"x":{"y":1}}\' FROM sys.generate_series(1, 4)',
        )
        assert rows == [
            ({"x": {"y": 1}},),
            ({"x": {"y": 1}},),
            ({"x": {"y": 1}},),
        ]
    finally:
        reference.close()


@pytest.mark.integration
@pytest.mark.xfail(
    strict=False,
    reason="materializing pymonetdb rows through Arrow widens narrow integers to int64",
)
def test_pymonetdb_known_difference_narrow_integer_widths(monetdb_uri: str) -> None:
    query = (
        "SELECT CAST(value AS TINYINT) AS i8, CAST(value AS SMALLINT) AS i16, "
        "CAST(value AS INT) AS i32 FROM sys.generate_series(1, 4)"
    )
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        expected_schema = cursor.fetch_arrow_table().schema

    reference = _pymonetdb_connect(monetdb_uri)
    try:
        names, rows = _pymonetdb_query(reference, query)
    finally:
        reference.close()
    inferred = pa.Table.from_pylist([dict(zip(names, row, strict=True)) for row in rows])

    assert inferred.schema.types == expected_schema.types


@pytest.mark.integration
@pytest.mark.xfail(
    strict=False,
    reason="pymonetdb loses the scale of DECIMAL(38, 10) values in binary result windows",
)
def test_pymonetdb_known_difference_decimal38_binary_scale(monetdb_uri: str) -> None:
    reference = _pymonetdb_connect(monetdb_uri)
    try:
        _, rows = _pymonetdb_query(
            reference,
            "SELECT CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38, 10)) FROM sys.generate_series(1, 4)",
        )
        assert rows == [(EXPECTED_ROW[11],)] * 3
    finally:
        reference.close()

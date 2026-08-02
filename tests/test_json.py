from uuid import uuid4

import adbc_driver_manager
import polars as pl
import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

JSON_DOCUMENT = '{"outer":{"inner":[1,2],"label":"ä","flag":true,"nothing":null},"items":[{"x":1},{"x":2}]}'
JSON_FUNCTIONS = f"""
SELECT json.filter(doc, '$.outer') AS nested_object,
       json.filter(doc, '$.outer.inner') AS nested_array,
       json.filter(doc, '$.outer.inner.[1]') AS nested_scalar,
       json.filter(doc, '$.outer.label') AS nested_string,
       json.filter(doc, '$.outer.flag') AS nested_boolean,
       json.filter(doc, '$.outer.nothing') AS nested_null,
       json.filter(doc, '$.items.[0]') AS indexed_object,
       json.filter(doc, 1) AS indexed_value,
       json.keyarray(doc) AS keys,
       json.valuearray(doc) AS values_v,
       json.text(json.filter(doc, '$.outer.label')) AS text_v,
       json.number(json.filter(doc, '$.outer.inner.[1]')) AS number_v,
       json."integer"(json.filter(doc, '$.outer.inner.[1]')) AS integer_v,
       json.length(doc) AS length_v,
       json.isobject(doc) AS object_v,
       json.isarray(doc) AS array_v,
       json.isvalid(CAST(doc AS STRING)) AS valid_v
FROM (SELECT JSON '{JSON_DOCUMENT}' AS doc) AS source
"""
JSON_FUNCTION_TYPES = [
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.json_(),
    pa.string(),
    pa.float64(),
    pa.int64(),
    pa.int32(),
    pa.bool_(),
    pa.bool_(),
    pa.bool_(),
]
JSON_FUNCTION_ROW = {
    "nested_object": '{"inner":[1,2],"label":"ä","flag":true,"nothing":null}',
    "nested_array": "[1,2]",
    "nested_scalar": "2",
    "nested_string": '"ä"',
    "nested_boolean": "true",
    "nested_null": "null",
    "indexed_object": '{"x":1}',
    "indexed_value": '[{"x":1},{"x":2}]',
    "keys": '["outer","items"]',
    "values_v": ('[{"inner":[1,2],"label":"ä","flag":true,"nothing":null},[{"x":1},{"x":2}]]'),
    "text_v": "ä",
    "number_v": 2.0,
    "integer_v": 2,
    "length_v": 2,
    "object_v": True,
    "array_v": False,
    "valid_v": True,
}


@pytest.mark.integration
def test_json_functions_preserve_declared_arrow_types(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(JSON_FUNCTIONS)
        table = cursor.fetch_arrow_table()
        cursor.execute(f"{JSON_FUNCTIONS} WHERE FALSE")
        empty = cursor.fetch_arrow_table()
        frame = pl.read_database(JSON_FUNCTIONS, connection)

    assert table.schema.types == JSON_FUNCTION_TYPES
    assert table.to_pylist() == [JSON_FUNCTION_ROW]
    assert empty.schema == table.schema
    assert empty.num_rows == 0
    assert frame.schema == {
        "nested_object": pl.String,
        "nested_array": pl.String,
        "nested_scalar": pl.String,
        "nested_string": pl.String,
        "nested_boolean": pl.String,
        "nested_null": pl.String,
        "indexed_object": pl.String,
        "indexed_value": pl.String,
        "keys": pl.String,
        "values_v": pl.String,
        "text_v": pl.String,
        "number_v": pl.Float64,
        "integer_v": pl.Int64,
        "length_v": pl.Int32,
        "object_v": pl.Boolean,
        "array_v": pl.Boolean,
        "valid_v": pl.Boolean,
    }


@pytest.mark.integration
def test_json_functions_remain_stable_across_small_result_windows(monetdb_uri: str) -> None:
    query = f"""
    SELECT json.filter(doc, '$.outer') AS nested_object,
           json.filter(doc, '$.outer.inner') AS nested_array,
           json.filter(doc, '$.outer.inner.[1]') AS nested_scalar,
           json.filter(doc, '$.items.[0]') AS indexed_object,
           json.filter(doc, 1) AS indexed_value,
           json.keyarray(doc) AS keys,
           json.valuearray(doc) AS values_v
    FROM sys.generate_series(1, 300),
         (SELECT JSON '{JSON_DOCUMENT}' AS doc) AS source
    """
    with (
        dbapi.connect(monetdb_uri, autocommit=True) as connection,
        connection.cursor(adbc_stmt_kwargs={StatementOptions.READ_BATCH_ROWS: "8"}) as cursor,
    ):
        cursor.execute(query)
        batches = list(cursor.fetch_record_batch())

    assert len(batches) == 38
    assert sum(batch.num_rows for batch in batches) == 299
    assert all(batch.schema == batches[0].schema for batch in batches)
    assert batches[0].schema.types == [pa.json_()] * 7


@pytest.mark.integration
def test_json_ingest_round_trips_all_shapes_and_rejects_invalid_text(
    monetdb_uri: str,
) -> None:
    table_name = f"json_round_trip_{uuid4().hex}"
    values = [
        None,
        "null",
        "true",
        "1.25",
        '"text"',
        "[]",
        '[1,"ä",false]',
        "{}",
        '{"nested":{"x":1}}',
    ]
    source = pa.table(
        {
            "id": pa.array(range(len(values)), type=pa.int64()),
            "payload": pa.array(values, type=pa.json_()),
        }
    )
    invalid = pa.table(
        {
            "id": pa.array([len(values)], type=pa.int64()),
            "payload": pa.array(["not json"], type=pa.json_()),
        }
    )

    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            assert cursor.adbc_ingest(table_name, source, mode="create") == len(values)
            schema = connection.adbc_get_table_schema(table_name)
            cursor.execute(f'SELECT id, payload FROM "{table_name}" ORDER BY id')
            result = cursor.fetch_arrow_table()
            frame = pl.read_database(
                f'SELECT payload FROM "{table_name}" ORDER BY id',
                connection,
            )
            with pytest.raises(adbc_driver_manager.OperationalError, match="JSON"):
                cursor.adbc_ingest(table_name, invalid, mode="append")
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            row_count = cursor.fetchone()
        finally:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    assert schema.types == [pa.int64(), pa.json_()]
    assert result.schema.types == [pa.int64(), pa.json_()]
    assert result.to_pydict() == source.to_pydict()
    assert frame.schema == {"payload": pl.String}
    assert frame.get_column("payload").to_list() == values
    assert row_count == (len(values),)

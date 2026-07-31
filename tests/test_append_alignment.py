import datetime as dt
import json
from typing import Any

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

INSERT_ROWS = 1
COPY_ROWS = 500


def _stats(cursor: dbapi.Cursor) -> dict[str, object]:
    return json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))


def _repeat(rows: int, **columns: pa.Array[Any]) -> pa.Table:
    names = list(columns)
    return pa.Table.from_arrays([pa.chunked_array([columns[name]] * rows) for name in names], names=names)


@pytest.fixture
def target(monetdb_uri: str) -> str:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS append_alignment")
        cursor.execute(
            'CREATE TABLE append_alignment("time" TIMESTAMP, "a" REAL, "b" DOUBLE DEFAULT 42.0, "s" VARCHAR(8))'
        )
    return monetdb_uri


@pytest.mark.integration
@pytest.mark.parametrize(("rows", "path"), [(INSERT_ROWS, "insert"), (COPY_ROWS, "copy")])
def test_append_matches_destination_columns_by_name_on_every_route(target: str, rows: int, path: str) -> None:
    reordered = _repeat(
        rows,
        s=pa.array(["x"]),
        b=pa.array([2.5], type=pa.float64()),
        time=pa.array([dt.datetime(2026, 7, 31)], type=pa.timestamp("us")),
        a=pa.array([1.5], type=pa.float32()),
    )
    with dbapi.connect(target, autocommit=True) as connection, connection.cursor() as cursor:
        assert cursor.adbc_ingest("append_alignment", reordered, mode="append") == rows
        assert _stats(cursor)["path"] == path
        cursor.execute('SELECT DISTINCT "time", "a", "b", "s" FROM append_alignment')
        assert cursor.fetchall() == [(dt.datetime(2026, 7, 31), 1.5, 2.5, "x")]


@pytest.mark.integration
@pytest.mark.parametrize("rows", [INSERT_ROWS, COPY_ROWS])
def test_append_fills_absent_columns_from_their_defaults_on_every_route(target: str, rows: int) -> None:
    subset = _repeat(
        rows,
        s=pa.array(["y"]),
        a=pa.array([1.5], type=pa.float32()),
    )
    with dbapi.connect(target, autocommit=True) as connection, connection.cursor() as cursor:
        assert cursor.adbc_ingest("append_alignment", subset, mode="append") == rows
        cursor.execute('SELECT DISTINCT "time", "a", "b", "s" FROM append_alignment')
        assert cursor.fetchall() == [(None, 1.5, 42.0, "y")]


@pytest.mark.integration
@pytest.mark.parametrize("rows", [INSERT_ROWS, COPY_ROWS])
def test_append_matches_destination_column_names_case_insensitively(target: str, rows: int) -> None:
    with dbapi.connect(target, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS append_alignment_case")
        cursor.execute('CREATE TABLE append_alignment_case("Col One" INT, "col two" INT)')
        upper = _repeat(rows, **{"COL TWO": pa.array([5], type=pa.int32())})
        assert cursor.adbc_ingest("append_alignment_case", upper, mode="append") == rows
        cursor.execute('SELECT DISTINCT "Col One", "col two" FROM append_alignment_case')
        assert cursor.fetchall() == [(None, 5)]


@pytest.mark.integration
@pytest.mark.parametrize("rows", [INSERT_ROWS, COPY_ROWS])
def test_append_rejects_unknown_duplicate_and_mistyped_columns_on_every_route(target: str, rows: int) -> None:
    with dbapi.connect(target, autocommit=True) as connection, connection.cursor() as cursor:
        unknown = _repeat(rows, nope=pa.array([1], type=pa.int32()))
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="does not exist in the destination table"):
            cursor.adbc_ingest("append_alignment", unknown, mode="append")

        duplicated = pa.Table.from_arrays(
            [pa.chunked_array([pa.array([1.5], type=pa.float32())] * rows)] * 2,
            names=["a", "A"],
        )
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="more than once"):
            cursor.adbc_ingest("append_alignment", duplicated, mode="append")

        mistyped = _repeat(rows, a=pa.array([1.5], type=pa.float64()))
        with pytest.raises(adbc_driver_manager.ProgrammingError, match="destination type is REAL"):
            cursor.adbc_ingest("append_alignment", mistyped, mode="append")


@pytest.mark.integration
@pytest.mark.parametrize("rows", [INSERT_ROWS, COPY_ROWS])
def test_append_leaves_absent_not_null_columns_to_the_server_on_every_route(monetdb_uri: str, rows: int) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS append_alignment_not_null")
        cursor.execute('CREATE TABLE append_alignment_not_null("a" INT, "b" INT NOT NULL)')
        partial = _repeat(rows, a=pa.array([1], type=pa.int32()))
        with pytest.raises(adbc_driver_manager.IntegrityError, match="NOT NULL constraint"):
            cursor.adbc_ingest("append_alignment_not_null", partial, mode="append")


@pytest.mark.integration
@pytest.mark.parametrize(("rows", "path"), [(INSERT_ROWS, "insert"), (COPY_ROWS, "staged_copy")])
def test_constrained_append_stages_a_reordered_subset(monetdb_uri: str, rows: int, path: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS append_alignment_pk")
        cursor.execute('CREATE TABLE append_alignment_pk("id" INT, "a" INT, "b" INT, PRIMARY KEY ("id"))')
        keyed = pa.table(
            {
                "b": pa.array([7] * rows, type=pa.int32()),
                "id": pa.array(range(rows), type=pa.int32()),
            }
        )
        assert cursor.adbc_ingest("append_alignment_pk", keyed, mode="append") == rows
        assert _stats(cursor)["path"] == path
        cursor.execute('SELECT COUNT(*), COUNT("a"), MIN("b") FROM append_alignment_pk')
        assert cursor.fetchall() == [(rows, 0, 7)]


@pytest.mark.integration
@pytest.mark.parametrize("rows", [INSERT_ROWS, COPY_ROWS])
def test_temporary_append_accepts_a_reordered_subset(monetdb_uri: str, rows: int) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS tmp.append_alignment_tmp")
        cursor.execute('CREATE LOCAL TEMPORARY TABLE append_alignment_tmp("x" INT, "y" INT) ON COMMIT PRESERVE ROWS')
        partial = _repeat(rows, y=pa.array([9], type=pa.int32()))
        assert cursor.adbc_ingest("append_alignment_tmp", partial, mode="append", temporary=True) == rows
        cursor.execute('SELECT COUNT(*), COUNT("x"), MIN("y") FROM tmp.append_alignment_tmp')
        assert cursor.fetchall() == [(rows, 0, 9)]


@pytest.mark.integration
@pytest.mark.parametrize(("rows", "path"), [(1, "insert"), (200, "copy")])
def test_interval_columns_round_trip_from_fetch_back_into_the_same_table(
    monetdb_uri: str, rows: int, path: str
) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS append_interval_source")
        cursor.execute("DROP TABLE IF EXISTS append_interval_target")
        for name in ("append_interval_source", "append_interval_target"):
            cursor.execute(f'CREATE TABLE {name}("s" INTERVAL SECOND, "d" INTERVAL DAY, "m" INTERVAL MONTH)')
        cursor.execute(
            "INSERT INTO append_interval_source VALUES "
            "(INTERVAL '90' SECOND, INTERVAL '2' DAY, INTERVAL '3' MONTH), "
            "(INTERVAL '90.25' SECOND, INTERVAL '-2' DAY, INTERVAL '-3' MONTH), "
            "(INTERVAL '0' SECOND, INTERVAL '0' DAY, INTERVAL '0' MONTH), "
            "(NULL, NULL, NULL)"
        )
        cursor.execute("SELECT * FROM append_interval_source")
        fetched = cursor.fetch_arrow_table()

        payload = pa.concat_tables([fetched] * rows)
        assert cursor.adbc_ingest("append_interval_target", payload, mode="append") == fetched.num_rows * rows
        assert _stats(cursor)["path"] == path

        cursor.execute('SELECT DISTINCT "s", "d", "m" FROM append_interval_target ORDER BY "s" NULLS LAST')
        assert cursor.fetchall() == [
            (dt.timedelta(0), dt.timedelta(0), 0),
            (dt.timedelta(seconds=90), dt.timedelta(days=2), 3),
            (dt.timedelta(seconds=90, milliseconds=250), dt.timedelta(days=-2), -3),
            (None, None, None),
        ]

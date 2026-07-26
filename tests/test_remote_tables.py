import datetime
from collections.abc import Iterator

import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

SOURCE_TABLE = "adbc_remote_source"
LOCAL_TABLE = "adbc_remote_local"
DIMENSION_TABLE = "adbc_remote_dimension"
MERGE_TABLE = "adbc_remote_merge"
SOURCE_ROWS = 1_000


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@pytest.fixture(scope="module")
def remote_tables(
    monetdb_uri: str,
    remote_monetdb: tuple[str, str, str, str],
) -> Iterator[tuple[str, str]]:
    source_uri, server_uri, username, password = remote_monetdb
    with (
        dbapi.connect(source_uri, autocommit=True) as source,
        dbapi.connect(monetdb_uri, autocommit=True) as master,
    ):
        for table in (MERGE_TABLE, DIMENSION_TABLE, LOCAL_TABLE, SOURCE_TABLE):
            master.execute(f"DROP TABLE IF EXISTS {table}")
        source.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
        source.execute(f"CREATE TABLE {SOURCE_TABLE}(id BIGINT, payload VARCHAR(64), observed_at TIMESTAMP)")
        source.execute(
            f"INSERT INTO {SOURCE_TABLE} "
            "SELECT value, 'remote-' || CAST(value AS VARCHAR(20)), "
            "TIMESTAMP '2024-01-01 00:00:00' + value * INTERVAL '1' SECOND "
            f"FROM sys.generate_series(0, {SOURCE_ROWS})"
        )
        master.execute(
            f"CREATE REMOTE TABLE {SOURCE_TABLE}(id BIGINT, payload VARCHAR(64), observed_at TIMESTAMP) "
            f"ON {_sql_string(server_uri)} WITH USER {_sql_string(username)} PASSWORD {_sql_string(password)}"
        )
        master.execute(f"CREATE TABLE {LOCAL_TABLE}(id BIGINT, payload VARCHAR(64), observed_at TIMESTAMP)")
        master.execute(
            f"INSERT INTO {LOCAL_TABLE} VALUES "
            f"({SOURCE_ROWS}, 'local-{SOURCE_ROWS}', TIMESTAMP '2024-01-02 00:00:00'), "
            f"({SOURCE_ROWS + 1}, 'local-{SOURCE_ROWS + 1}', TIMESTAMP '2024-01-02 00:00:01')"
        )
        master.execute(f"CREATE TABLE {DIMENSION_TABLE}(id BIGINT, label VARCHAR(64))")
        master.execute(
            f"INSERT INTO {DIMENSION_TABLE} "
            "SELECT value, 'dimension-' || CAST(value AS VARCHAR(20)) "
            f"FROM sys.generate_series(0, {SOURCE_ROWS})"
        )
        master.execute(f"CREATE MERGE TABLE {MERGE_TABLE}(id BIGINT, payload VARCHAR(64), observed_at TIMESTAMP)")
        master.execute(f"ALTER TABLE {MERGE_TABLE} ADD TABLE {SOURCE_TABLE}")
        master.execute(f"ALTER TABLE {MERGE_TABLE} ADD TABLE {LOCAL_TABLE}")

    try:
        yield source_uri, monetdb_uri
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as master:
            for table in (MERGE_TABLE, DIMENSION_TABLE, LOCAL_TABLE, SOURCE_TABLE):
                master.execute(f"DROP TABLE IF EXISTS {table}")
        with dbapi.connect(source_uri, autocommit=True) as source:
            source.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")


@pytest.mark.integration
@pytest.mark.parametrize("read_prefetch", ["false", "true"])
def test_remote_table_fetches_through_xexportbin(
    remote_tables: tuple[str, str],
    read_prefetch: str,
) -> None:
    source_uri, master_uri = remote_tables
    query = f"SELECT id, payload, observed_at FROM {SOURCE_TABLE} ORDER BY id"
    with (
        dbapi.connect(source_uri, autocommit=True) as source,
        dbapi.connect(master_uri, autocommit=True) as master,
        source.cursor() as source_cursor,
        master.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_PREFETCH: read_prefetch,
            }
        ) as remote_cursor,
    ):
        source_cursor.execute(query)
        direct = source_cursor.fetch_arrow_table()
        remote_cursor.execute(query)
        remote = remote_cursor.fetch_arrow_table()
        remote_cursor.execute(query)
        rows = remote_cursor.fetchall()

    assert remote.num_rows == SOURCE_ROWS
    assert remote.equals(direct, check_metadata=True)
    assert len(rows) == SOURCE_ROWS
    assert rows[0] == (0, "remote-0", datetime.datetime(2024, 1, 1))
    assert rows[-1] == (
        SOURCE_ROWS - 1,
        f"remote-{SOURCE_ROWS - 1}",
        datetime.datetime(2024, 1, 1, 0, 16, 39),
    )


@pytest.mark.integration
def test_merge_table_fetches_remote_and_local_members(
    remote_tables: tuple[str, str],
) -> None:
    _, master_uri = remote_tables
    with dbapi.connect(master_uri, autocommit=True) as master, master.cursor() as cursor:
        cursor.execute(f"SELECT id, payload, observed_at FROM {MERGE_TABLE} ORDER BY id")
        merged = cursor.fetch_arrow_table()
        cursor.execute(f"SELECT COUNT(*), MIN(id), MAX(id) FROM {MERGE_TABLE}")
        aggregate = cursor.fetchone()

    assert merged.num_rows == SOURCE_ROWS + 2
    assert merged.slice(SOURCE_ROWS - 1, 3).to_pylist() == [
        {
            "id": SOURCE_ROWS - 1,
            "payload": f"remote-{SOURCE_ROWS - 1}",
            "observed_at": datetime.datetime(2024, 1, 1, 0, 16, 39),
        },
        {
            "id": SOURCE_ROWS,
            "payload": f"local-{SOURCE_ROWS}",
            "observed_at": datetime.datetime(2024, 1, 2),
        },
        {
            "id": SOURCE_ROWS + 1,
            "payload": f"local-{SOURCE_ROWS + 1}",
            "observed_at": datetime.datetime(2024, 1, 2, 0, 0, 1),
        },
    ]
    assert aggregate == (SOURCE_ROWS + 2, 0, SOURCE_ROWS + 1)


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql",
    [
        (
            f"SELECT r.id, r.payload, d.label FROM {SOURCE_TABLE} AS r "
            f"JOIN {DIMENSION_TABLE} AS d ON r.id = d.id ORDER BY r.id"
        ),
        (
            f"SELECT r.id, r.payload, d.label FROM {DIMENSION_TABLE} AS d "
            f"LEFT JOIN {SOURCE_TABLE} AS r ON d.id = r.id ORDER BY d.id"
        ),
        (
            f"SELECT m.id, m.payload, d.label FROM {MERGE_TABLE} AS m "
            f"JOIN {DIMENSION_TABLE} AS d ON m.id = d.id ORDER BY m.id"
        ),
    ],
    ids=["remote-to-local", "local-to-remote", "merge-to-local"],
)
def test_remote_and_local_table_joins(
    remote_tables: tuple[str, str],
    sql: str,
) -> None:
    _, master_uri = remote_tables
    with dbapi.connect(master_uri, autocommit=True) as master, master.cursor() as cursor:
        cursor.execute(sql)
        joined = cursor.fetch_arrow_table()

    assert joined.num_rows == SOURCE_ROWS
    assert joined.slice(0, 1).to_pylist() == [{"id": 0, "payload": "remote-0", "label": "dimension-0"}]
    assert joined.slice(SOURCE_ROWS - 1, 1).to_pylist() == [
        {
            "id": SOURCE_ROWS - 1,
            "payload": f"remote-{SOURCE_ROWS - 1}",
            "label": f"dimension-{SOURCE_ROWS - 1}",
        }
    ]

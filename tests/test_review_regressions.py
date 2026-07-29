from decimal import Decimal
from typing import cast
from urllib.parse import parse_qs, urlsplit

import adbc_driver_manager
import polars as pl
import pytest

from adbc_driver_monetdb import ConnectionOptions, DatabaseOptions, StatementOptions, dbapi


@pytest.mark.integration
def test_connection_context_exit_rolls_back_uncommitted_work(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as setup:
        setup.execute("DROP TABLE IF EXISTS review_close_rollback")
        setup.execute("CREATE TABLE review_close_rollback(value INT)")
    try:
        with dbapi.connect(monetdb_uri) as connection:
            connection.execute("INSERT INTO review_close_rollback VALUES (42)")
        with dbapi.connect(monetdb_uri, autocommit=True) as audit:
            assert audit.execute("SELECT COUNT(*) FROM review_close_rollback").fetchone() == (0,)
    finally:
        with dbapi.connect(monetdb_uri, autocommit=True) as cleanup:
            cleanup.execute("DROP TABLE IF EXISTS review_close_rollback")


@pytest.mark.integration
def test_get_objects_uses_canonical_monetdb_xdbc_metadata(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS review_xdbc")
            connection.execute(
                "CREATE TABLE review_xdbc("
                "bo BOOLEAN, i INT, d DECIMAL(12,3), r REAL, f DOUBLE, h HUGEINT, "
                "t TIME(4), ts TIMESTAMP(2), ym INTERVAL YEAR TO MONTH, "
                "ds INTERVAL DAY TO SECOND, v VARCHAR(9), b BLOB, u UUID, j JSON)"
            )
            rows = cast(
                list[dict[str, object]],
                connection.adbc_get_objects(
                    depth="all",
                    db_schema_filter="sys",
                    table_name_filter="review_xdbc",
                )
                .read_all()
                .to_pylist(),
            )
        finally:
            connection.execute("DROP TABLE IF EXISTS review_xdbc")

    schemas = cast(list[dict[str, object]], rows[0]["catalog_db_schemas"])
    tables = cast(list[dict[str, object]], schemas[0]["db_schema_tables"])
    columns = cast(list[dict[str, object]], tables[0]["table_columns"])
    by_name = {cast(str, column["column_name"]): column for column in columns}
    fields = (
        "xdbc_data_type",
        "xdbc_column_size",
        "xdbc_decimal_digits",
        "xdbc_num_prec_radix",
        "xdbc_sql_data_type",
        "xdbc_datetime_sub",
        "xdbc_char_octet_length",
    )
    expected = {
        "bo": (-7, 1, None, None, -7, None, None),
        "i": (4, 31, 0, 2, 4, None, None),
        "d": (3, 12, 3, 10, 3, None, None),
        "r": (7, 24, 7, 2, 7, None, None),
        "f": (8, 53, 15, 2, 8, None, None),
        "h": (16_384, 127, 0, 2, 16_384, None, None),
        "t": (92, 12, 4, None, 9, 2, None),
        "ts": (93, 23, 2, None, 9, 3, None),
        "ym": (107, 38, 0, None, 10, 7, None),
        "ds": (110, 47, 0, None, 10, 10, None),
        "v": (-9, 9, None, None, -9, None, 36),
        "u": (None, 36, None, None, -11, None, None),
    }
    for name, values in expected.items():
        assert tuple(by_name[name][field] for field in fields) == values
    assert by_name["b"]["xdbc_data_type"] == -4
    assert by_name["b"]["xdbc_char_octet_length"] == by_name["b"]["xdbc_column_size"]
    assert all(by_name["j"][field] is None for field in fields)


@pytest.mark.integration
def test_wide_results_accept_terse_column_metadata(monetdb_uri: str) -> None:
    columns = ", ".join(f'"{chr(ord("a") + index)}" INT' for index in range(26))
    expressions = ", ".join(f'1 AS "{chr(ord("a") + index % 26)}"' for index in range(29))
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute("DROP TABLE IF EXISTS review_wide_terse")
            cursor.execute(f"CREATE TABLE review_wide_terse({columns})")
            cursor.execute("SELECT * FROM review_wide_terse")
            empty = cursor.fetch_record_batch().read_all()
            cursor.execute(f"SELECT {expressions}")
            row = cursor.fetchone()
        finally:
            cursor.execute("DROP TABLE IF EXISTS review_wide_terse")

    assert empty.num_columns == 26
    assert empty.num_rows == 0
    assert row == (1,) * 29


@pytest.mark.integration
def test_dbapi_fetchmany_arraysize_and_rowcount(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute("DROP TABLE IF EXISTS review_dbapi_accessories")
            cursor.execute("CREATE TABLE review_dbapi_accessories(value INT)")
            cursor.executemany(
                "INSERT INTO review_dbapi_accessories VALUES (?)",
                [(value,) for value in range(1, 8)],
            )
            assert cursor.rowcount == 7
            cursor.execute("SELECT value FROM review_dbapi_accessories ORDER BY value")
            cursor.arraysize = 3
            assert cursor.fetchmany() == [(1,), (2,), (3,)]
            assert cursor.fetchmany(2) == [(4,), (5,)]
            assert cursor.fetchmany() == [(6,), (7,)]
            assert cursor.fetchmany() == []
        finally:
            cursor.execute("DROP TABLE IF EXISTS review_dbapi_accessories")


@pytest.mark.integration
def test_execute_query_propagates_known_dml_row_counts(monetdb_uri: str) -> None:
    table = "review_execute_query_rowcount"
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            assert cursor.rowcount == -1
            cursor.execute(f"CREATE TABLE {table}(value INT)")
            assert cursor.rowcount == -1

            cursor.execute(f"INSERT INTO {table} VALUES (1), (2), (3)")
            assert cursor.rowcount == 3
            cursor.execute(f"UPDATE {table} SET value = value + 10 WHERE value = ?", [2])
            assert cursor.rowcount == 1
            cursor.execute(f"UPDATE {table} SET value = value + 10 WHERE value = -1")
            assert cursor.rowcount == 0
            cursor.execute(f"DELETE FROM {table} WHERE value > 2")
            assert cursor.rowcount == 2

            statement = cursor.adbc_statement
            statement.set_sql_query(f"SELECT value FROM {table}")
            stream, rows_affected = statement.execute_query()
            stream.release()
            assert rows_affected == 1
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.integration
def test_session_timezone_does_not_change_timestamptz_decode(monetdb_uri: str) -> None:
    query = "SELECT TIMESTAMPTZ '2025-01-01 00:30:00+01:00' AS value"
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        before = pl.read_database(query, connection)
        connection.execute("SET TIME ZONE INTERVAL '+02:00' HOUR TO MINUTE")
        after = pl.read_database(query, connection)

    expected = 1_735_687_800_000_000
    assert before.get_column("value").cast(pl.Int64).item() == expected
    assert after.get_column("value").cast(pl.Int64).item() == expected
    assert before.schema == after.schema == pl.Schema({"value": pl.Datetime("us", "UTC")})


@pytest.mark.integration
def test_empty_typed_ingest_all_modes_and_polars(monetdb_uri: str) -> None:
    empty = pl.DataFrame(schema={"value": pl.Int64, "name": pl.String})
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS empty_ingest")
                cursor.execute("DROP TABLE IF EXISTS empty_polars_ingest")
                assert cursor.adbc_ingest("empty_ingest", empty, mode="create") == 0
                assert cursor.adbc_ingest("empty_ingest", empty, mode="append") == 0
                assert cursor.adbc_ingest("empty_ingest", empty, mode="create_append") == 0
                assert cursor.adbc_ingest("empty_ingest", empty, mode="replace") == 0
            assert (
                empty.write_database(
                    "empty_polars_ingest",
                    connection,
                    if_table_exists="replace",
                    engine="adbc",
                )
                == 0
            )
            direct = pl.read_database("SELECT * FROM empty_ingest", connection)
            through_polars = pl.read_database("SELECT * FROM empty_polars_ingest", connection)
        finally:
            connection.execute("DROP TABLE IF EXISTS empty_ingest")
            connection.execute("DROP TABLE IF EXISTS empty_polars_ingest")

    assert direct.schema == empty.schema
    assert through_polars.schema == empty.schema
    assert direct.is_empty()
    assert through_polars.is_empty()


@pytest.mark.integration
@pytest.mark.parametrize("resume_before_second_query", [False, True])
@pytest.mark.parametrize("read_prefetch", ["false", "true"])
def test_partially_consumed_stream_can_resume_around_a_second_query(
    monetdb_uri: str,
    resume_before_second_query: bool,
    read_prefetch: str,
) -> None:
    with (
        dbapi.connect(monetdb_uri) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_BATCH_ROWS: "100000",
                StatementOptions.READ_PREFETCH: read_prefetch,
            }
        ) as first_cursor,
        connection.cursor() as second_cursor,
    ):
        assert first_cursor.adbc_statement.get_option(str(StatementOptions.READ_PREFETCH)) == read_prefetch
        first_cursor.execute("SELECT value FROM sys.generate_series(1, 300001)")
        reader = first_cursor.fetch_record_batch()
        first = reader.read_next_batch()
        if resume_before_second_query:
            resumed = reader.read_next_batch()
            second_cursor.execute("SELECT 42")
            second = second_cursor.fetchone()
        else:
            second_cursor.execute("SELECT 42")
            second = second_cursor.fetchone()
            resumed = reader.read_next_batch()
        reader.close()

    assert first.num_rows == 100_000
    assert first.column(0)[0].as_py() == 1  # pyright: ignore[reportUnknownMemberType]
    assert resumed.num_rows == 100_000
    assert resumed.column(0)[0].as_py() == 100_001  # pyright: ignore[reportUnknownMemberType]
    assert second == (42,)


@pytest.mark.integration
def test_prefetched_reader_can_be_closed_early_and_connection_reused(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        assert connection.adbc_connection.get_option(str(ConnectionOptions.READ_PREFETCH)) == "true"
        for _ in range(25):
            with connection.cursor(
                adbc_stmt_kwargs={
                    StatementOptions.READ_BATCH_ROWS: "10000",
                    StatementOptions.READ_PREFETCH: "true",
                }
            ) as cursor:
                cursor.execute("SELECT value FROM sys.generate_series(1, 100001)")
                reader = cursor.fetch_record_batch()
                assert reader.read_next_batch().num_rows == 10_000
                reader.close()
            assert connection.execute("SELECT 42").fetchone() == (42,)


@pytest.mark.integration
def test_connection_read_prefetch_false_is_inherited_by_statements(monetdb_uri: str) -> None:
    with dbapi.connect(
        monetdb_uri,
        conn_kwargs={ConnectionOptions.READ_PREFETCH: "false"},
    ) as connection:
        assert connection.adbc_connection.get_option(str(ConnectionOptions.READ_PREFETCH)) == "false"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.READ_BATCH_ROWS)) == "131072"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.WRITE_BATCH_ROWS)) == "0"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.WRITE_WINDOW_BYTES)) == "0"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.INGEST_PARTIAL)) == "block"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.INGEST_ATOMICITY)) == "transaction"
        assert connection.adbc_connection.get_option(str(ConnectionOptions.CONSTRAINED_APPEND)) == "auto"
        with connection.cursor() as cursor:
            assert cursor.adbc_statement.get_option(str(StatementOptions.READ_PREFETCH)) == "false"
            assert cursor.adbc_statement.get_option(str(StatementOptions.READ_BATCH_ROWS)) == "131072"
            assert cursor.adbc_statement.get_option(str(StatementOptions.WRITE_BATCH_ROWS)) == "0"
            assert cursor.adbc_statement.get_option(str(StatementOptions.WRITE_WINDOW_BYTES)) == "0"
            assert cursor.adbc_statement.get_option(str(StatementOptions.INGEST_PARTIAL)) == "block"
            assert cursor.adbc_statement.get_option(str(StatementOptions.INGEST_ATOMICITY)) == "transaction"
            assert cursor.adbc_statement.get_option(str(StatementOptions.CONSTRAINED_APPEND)) == "auto"


@pytest.mark.integration
def test_i64_null_sentinel_ingest_is_a_clean_error(monetdb_uri: str) -> None:
    frame = pl.DataFrame({"value": pl.Series([-(2**63)], dtype=pl.Int64)})
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            with (
                connection.cursor() as cursor,
                pytest.raises(adbc_driver_manager.DataError, match="NULL sentinel"),
            ):
                cursor.adbc_ingest("integer_sentinel_ingest", frame, mode="create")
        finally:
            connection.execute("DROP TABLE IF EXISTS integer_sentinel_ingest")


@pytest.mark.integration
def test_hugeint_outside_decimal128_range_is_a_clean_error(monetdb_uri: str) -> None:
    with (
        dbapi.connect(monetdb_uri) as connection,
        pytest.raises(adbc_driver_manager.DataError, match="HUGEINT exceeds Arrow Decimal128"),
    ):
        pl.read_database(
            "SELECT CAST(170141183460469231731687303715884105727 AS HUGEINT)",
            connection,
        )


@pytest.mark.integration
def test_astral_unicode_roundtrip_and_nul_parameter_error(monetdb_uri: str) -> None:
    value = "MonetDB 🦀🚀 𐍈"
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT ?", [value])
        assert cursor.fetchone() == (value,)
        with pytest.raises(adbc_driver_manager.DataError, match="contains a NUL byte"):
            cursor.execute("SELECT ?", ["a\x00b"])


@pytest.mark.integration
def test_bce_date_from_live_server(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as connection:
        frame = pl.read_database("SELECT DATE '-0001-12-31' AS value", connection)

    assert frame.schema == pl.Schema({"value": pl.Date})
    assert frame.get_column("value").cast(pl.Int32).item() == -719_529
    assert "-0001-12-31" in str(frame)


@pytest.mark.integration
def test_raw_client_copy_is_refused_without_desynchronizing(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute("DROP TABLE IF EXISTS review_raw_client_copy")
            cursor.execute("CREATE TABLE review_raw_client_copy(value INT)")
            for sentinel, statement in enumerate(
                [
                    "COPY INTO review_raw_client_copy FROM 'review_in.csv' ON CLIENT",
                    "COPY SELECT 1 INTO 'review_out.csv' ON CLIENT",
                ],
                start=42,
            ):
                with pytest.raises(adbc_driver_manager.DataError, match="file transfer") as refused:
                    cursor.execute(statement)
                assert refused.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_DATA
                cursor.execute(f"SELECT {sentinel}")
                assert cursor.fetchone() == (sentinel,)
        finally:
            cursor.execute("DROP TABLE IF EXISTS review_raw_client_copy")


@pytest.mark.integration
@pytest.mark.parametrize("row_count", [1, 3])
def test_quoted_column_names_roundtrip_through_arrow_and_polars(
    monetdb_uri: str,
    row_count: int,
) -> None:
    names = ['a"b', "c d", "select", "MiXeD", "ä"]
    table_name = f"review_quoted_names_{row_count}"
    values = ",".join(f"({row}, {row}, {row}, {row}, {row})" for row in range(1, row_count + 1))
    query = f'SELECT * FROM {table_name} ORDER BY "a""b"'
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(f'CREATE TABLE {table_name}("a""b" INT, "c d" INT, "select" INT, "MiXeD" INT, "ä" INT)')
            cursor.execute(f"INSERT INTO {table_name} VALUES {values}")
            cursor.execute(query)
            arrow_table = cursor.fetch_arrow_table()
            polars_frame = pl.read_database(query, connection)
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    assert arrow_table.schema.names == names
    assert arrow_table.num_rows == row_count
    assert polars_frame.columns == names
    assert polars_frame.height == row_count


@pytest.mark.integration
def test_adversarial_identifier_matrix_roundtrips_through_ingest_arrow_and_polars(
    monetdb_uri: str,
) -> None:
    names = [
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
    table_name = "review_adversarial_identifiers"
    frame = pl.DataFrame({name: [index] for index, name in enumerate(names)})
    quoted_table = f'"{table_name}"'
    query = f"SELECT * FROM {quoted_table}"

    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            assert cursor.adbc_ingest(table_name, frame.to_arrow(), mode="create") == 1
            cursor.execute(query)
            arrow_table = cursor.fetch_arrow_table()
            polars_frame = pl.read_database(query, connection)
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")

    assert arrow_table.schema.names == names
    assert arrow_table.to_pydict() == frame.to_dict(as_series=False)
    assert polars_frame.equals(frame)


@pytest.mark.integration
def test_polars_schema_qualified_write_and_cross_catalog_rejection(monetdb_uri: str) -> None:
    frame = pl.DataFrame({"value": [1, 2]})
    with dbapi.connect(monetdb_uri, autocommit=True) as connection:
        try:
            connection.execute("DROP TABLE IF EXISTS review_schema.review_table")
            connection.execute("DROP SCHEMA IF EXISTS review_schema")
            connection.execute("CREATE SCHEMA review_schema")
            assert frame.write_database("review_schema.review_table", connection, engine="adbc") == 2
            result = pl.read_database(
                "SELECT value FROM review_schema.review_table ORDER BY value",
                connection,
            )
            with pytest.raises(adbc_driver_manager.NotSupportedError) as unsupported:
                frame.write_database("test.review_schema.never", connection, engine="adbc")
            assert unsupported.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED
        finally:
            connection.execute("DROP TABLE IF EXISTS review_schema.review_table")
            connection.execute("DROP SCHEMA IF EXISTS review_schema")

    assert result.equals(frame)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("sql", "parameters", "expected"),
    [
        ("SELECT :a * :b AS value", {"a": 6, "b": 7}, [42]),
        (
            "SELECT CAST(CAST(? AS BIGINT) * CAST(? AS BIGINT) AS BIGINT) AS value",
            (6, 7),
            [42],
        ),
        (
            "SELECT CAST(:a AS DECIMAL(9,2)) * CAST(:b AS DECIMAL(9,2)) AS value",
            {"a": Decimal("1.25"), "b": Decimal("2.00")},
            [Decimal("2.5000")],
        ),
        ("SELECT :a * :b AS value", {"a": 6, "b": None}, [None]),
        (
            "SELECT value * :factor AS value FROM sys.generate_series(1, 4)",
            {"factor": 2},
            [2, 4, 6],
        ),
    ],
)
def test_parameter_expression_matrix_dbapi_and_polars(
    monetdb_uri: str,
    sql: str,
    parameters: dict[str, object] | tuple[object, ...],
    expected: list[object],
) -> None:
    with dbapi.connect(monetdb_uri) as connection, connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        dbapi_values = [row[0] for row in cursor.fetchall()]
        polars_values = (
            pl.read_database(
                sql,
                connection,
                execute_options={"parameters": parameters},
            )
            .get_column("value")
            .to_list()
        )

    assert dbapi_values == expected
    assert polars_values == expected


@pytest.mark.integration
def test_explain_results_and_explain_analyze_error_preserve_connection(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("EXPLAIN SELECT 1")
        assert cursor.description is not None
        assert cursor.description[0][0] == "rel"
        plan = [row[0] for row in cursor.fetchall()]
        assert len(plan) > 1
        assert all(isinstance(line, str) and line for line in plan)
        cursor.execute("SELECT 42")
        assert cursor.fetchone() == (42,)

        with pytest.raises(adbc_driver_manager.ProgrammingError) as unsupported:
            cursor.execute("EXPLAIN ANALYZE SELECT 1")
        assert unsupported.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
        assert unsupported.value.sqlstate == "42000"
        cursor.execute("SELECT 43")
        assert cursor.fetchone() == (43,)


@pytest.mark.integration
def test_timeout_uri_validation_precedence_and_readback(monetdb_uri: str) -> None:
    separator = "&" if "?" in monetdb_uri else "?"
    for misspelled in ["connect_timout", "read_timout", "write_timout", "operation_timout"]:
        with pytest.raises(adbc_driver_manager.ProgrammingError, match=misspelled) as rejected:
            dbapi.connect(f"{monetdb_uri}{separator}{misspelled}=1")
        assert rejected.value.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT

    configured_uri = f"{monetdb_uri}{separator}connect_timeout=9&read_timeout=8&write_timeout=7&operation_timeout=6"
    connection_values: dict[str, str] = {
        str(ConnectionOptions.READ_TIMEOUT): "5",
        str(ConnectionOptions.WRITE_TIMEOUT): "4",
        str(ConnectionOptions.OPERATION_TIMEOUT): "3",
    }
    with dbapi.connect(configured_uri, conn_kwargs=connection_values) as connection:
        returned_query = parse_qs(urlsplit(connection.adbc_database.get_option("uri")).query)
        assert "password" not in returned_query
        assert returned_query["connect_timeout"] == ["9"]
        for option, expected in {
            DatabaseOptions.CONNECT_TIMEOUT: "9",
            DatabaseOptions.READ_TIMEOUT: "8",
            DatabaseOptions.WRITE_TIMEOUT: "7",
            DatabaseOptions.OPERATION_TIMEOUT: "6",
        }.items():
            assert connection.adbc_database.get_option(option) == expected
        for option, expected in connection_values.items():
            assert connection.adbc_connection.get_option(option) == expected

        with connection.cursor() as inherited:
            for option, expected in connection_values.items():
                assert inherited.adbc_statement.get_option(str(option)) == expected

        statement_values: dict[str, object] = {
            str(StatementOptions.READ_TIMEOUT): "2",
            str(StatementOptions.WRITE_TIMEOUT): "1",
            str(StatementOptions.OPERATION_TIMEOUT): "0",
        }
        with connection.cursor(adbc_stmt_kwargs=statement_values) as overridden:
            for option, expected in statement_values.items():
                assert overridden.adbc_statement.get_option(option) == expected

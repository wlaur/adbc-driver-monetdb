import adbc_driver_manager
import polars as pl
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi


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
def test_partially_consumed_stream_can_resume_around_a_second_query(
    monetdb_uri: str,
    resume_before_second_query: bool,
) -> None:
    with (
        dbapi.connect(monetdb_uri) as connection,
        connection.cursor(adbc_stmt_kwargs={StatementOptions.READ_BATCH_ROWS: "100000"}) as first_cursor,
        connection.cursor() as second_cursor,
    ):
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

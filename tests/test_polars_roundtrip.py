import polars as pl
import pytest

from adbc_driver_monetdb import dbapi


@pytest.mark.integration
def test_read_database(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        # polars' connection union references optional deps we don't install -> partially unknown
        df = pl.read_database("SELECT 42 AS answer, 'monetdb' AS name", conn)  # pyright: ignore[reportUnknownMemberType]
    assert df.shape == (1, 2)
    assert df.get_column("answer").item() == 42
    assert df.get_column("name").item() == "monetdb"


@pytest.mark.integration
def test_empty_and_null_results(monetdb_uri: str) -> None:
    with dbapi.connect(monetdb_uri) as conn:
        empty = pl.read_database("SELECT CAST(NULL AS INT) AS value WHERE FALSE", conn)  # pyright: ignore[reportUnknownMemberType]
        values = pl.read_database(  # pyright: ignore[reportUnknownMemberType]
            "SELECT * FROM (VALUES (1, 'a'), (2, CAST(NULL AS VARCHAR(2))), (3, 'c')) AS t(i, s) ORDER BY i",
            conn,
        )
    assert empty.schema == {"value": pl.Int32}
    assert empty.height == 0
    assert values.to_dict(as_series=False) == {"i": [1, 2, 3], "s": ["a", None, "c"]}


@pytest.mark.integration
@pytest.mark.xfail(reason="driver skeleton: ingestion is not implemented yet", strict=False)
def test_write_database_roundtrip(monetdb_uri: str) -> None:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [1.5, None, 3.0],
            "name": ["a", "b", None],
        }
    )
    with dbapi.connect(monetdb_uri) as conn:
        df.write_database("roundtrip_smoke", conn, if_table_exists="replace", engine="adbc")  # pyright: ignore[reportUnknownMemberType]
        back = pl.read_database("SELECT id, value, name FROM roundtrip_smoke ORDER BY id", conn)  # pyright: ignore[reportUnknownMemberType]
    assert back.equals(df)

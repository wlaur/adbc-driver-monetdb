from pathlib import Path

from adbc_drivers_validation import model  # pyright: ignore[reportMissingTypeStubs]


class MonetdbQuirks(model.DriverQuirks):
    name = "monetdb"
    driver = "adbc_driver_monetdb"
    driver_name = "adbc-driver-monetdb"
    vendor_name = "MonetDB"
    vendor_version = "11.55.7"
    short_version = "11.55"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_set_current_schema=True,
        connection_transactions=True,
        current_catalog="test",
        current_schema="sys",
        secondary_schema="information_schema",
        get_objects=True,
        get_objects_constraints_check=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        get_objects_constraints_unique=True,
        metadata_type_name=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        statement_bulk_ingest_temporary=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        supported_xdbc_fields=[
            "xdbc_data_type",
            "xdbc_type_name",
            "xdbc_nullable",
            "xdbc_sql_data_type",
            "xdbc_is_nullable",
        ],
    )
    setup = model.DriverSetup(database={"uri": model.FromEnv("MONETDB_TEST_URI")})

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent / "queries",)

    @property
    def sample_ddl_constraints(self) -> list[str]:
        return [
            "CREATE TABLE constraint_check("
            "a INT CONSTRAINT check_a CHECK(a > 0), b INT, CONSTRAINT check_ab CHECK(a < b))",
            "CREATE TABLE constraint_unique(z INT, a INT UNIQUE, b STRING, c INT, CONSTRAINT unique_cb UNIQUE(c, b))",
            "CREATE TABLE constraint_primary(z INT, a INT, b STRING, PRIMARY KEY(a))",
            "CREATE TABLE constraint_primary_multi(z INT, a INT, b STRING, PRIMARY KEY(b, a))",
            "CREATE TABLE constraint_primary_multi2(z INT, a STRING, b INT, PRIMARY KEY(a, b))",
            "CREATE TABLE constraint_foreign(z INT, a INT, b INT, FOREIGN KEY(b) REFERENCES constraint_primary(a))",
            "CREATE TABLE constraint_foreign_multi("
            "z INT, a INT, b INT, c STRING, "
            "FOREIGN KEY(c, b) REFERENCES constraint_primary_multi2(a, b))",
        ]

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        del table_name
        message = str(error).lower()
        return "does not exist" in message or "unknown table" in message or "not found" in message

    def qualify_temp_table(self, cursor: object, name: str) -> str:
        del cursor
        return self.quote_one_identifier(name)


def get_quirks() -> MonetdbQuirks:
    return MonetdbQuirks()

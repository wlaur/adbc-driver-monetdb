import re
from pathlib import Path

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import dbapi

CORPUS = Path(__file__).with_name("query_corpus") / "sqlsmith_seed_52217.sql"
CASE_MARKER = re.compile(r"^-- query: (\d+); expected: (ok|programming_error 42000)$")


def _corpus_cases() -> list[tuple[int, str, str]]:
    cases: list[tuple[int, str, str]] = []
    query_number: int | None = None
    expected: str | None = None
    statement: list[str] = []
    for line in CORPUS.read_text().splitlines(keepends=True):
        marker = CASE_MARKER.match(line.rstrip())
        if marker:
            if query_number is not None and expected is not None:
                cases.append((query_number, expected, "".join(statement).strip()))
            query_number = int(marker.group(1))
            expected = marker.group(2)
            statement = []
        elif query_number is not None:
            statement.append(line)
    if query_number is not None and expected is not None:
        cases.append((query_number, expected, "".join(statement).strip()))
    if any(not statement.endswith(";") for _, _, statement in cases):
        raise ValueError("SQLsmith corpus contains an unterminated statement")
    return cases


@pytest.mark.integration
def test_sqlsmith_query_strings_execute_or_fail_cleanly(monetdb_uri: str) -> None:
    cases = _corpus_cases()
    assert len(cases) == 17
    assert max(len(statement) for _, _, statement in cases) > 40_000

    with dbapi.connect(monetdb_uri, autocommit=True) as connection, connection.cursor() as cursor:
        try:
            cursor.execute("DROP TABLE IF EXISTS review_sqlsmith_orders")
            cursor.execute("DROP TABLE IF EXISTS review_sqlsmith_customers")
            cursor.execute(
                "CREATE TABLE review_sqlsmith_customers("
                "customer_id INTEGER PRIMARY KEY, name VARCHAR(40), "
                "active BOOLEAN, joined DATE)"
            )
            cursor.execute(
                "CREATE TABLE review_sqlsmith_orders("
                "order_id BIGINT PRIMARY KEY, customer_id INTEGER, "
                "amount DECIMAL(12, 2), created_at TIMESTAMP, note VARCHAR(80))"
            )
            cursor.execute(
                "INSERT INTO review_sqlsmith_customers VALUES "
                "(1, 'Ada', TRUE, DATE '2024-01-02'), "
                "(2, 'Linus', FALSE, DATE '2024-03-04'), "
                "(3, NULL, TRUE, NULL)"
            )
            cursor.execute(
                "INSERT INTO review_sqlsmith_orders VALUES "
                "(10, 1, 12.50, TIMESTAMP '2025-01-01 12:00:00', 'first'), "
                "(11, 1, NULL, TIMESTAMP '2025-01-02 13:30:00', NULL), "
                "(12, 2, -3.25, NULL, 'refund')"
            )

            for query_number, expected, statement in cases:
                if expected == "ok":
                    cursor.execute(statement)
                    cursor.fetchall()
                else:
                    with pytest.raises(adbc_driver_manager.ProgrammingError) as caught:
                        cursor.execute(statement)
                    assert caught.value.sqlstate == "42000", query_number
                assert cursor.execute("SELECT 1").fetchone() == (1,), query_number
        finally:
            cursor.execute("DROP TABLE IF EXISTS review_sqlsmith_orders")
            cursor.execute("DROP TABLE IF EXISTS review_sqlsmith_customers")

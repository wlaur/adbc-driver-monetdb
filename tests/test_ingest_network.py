import http.client
import json
import os
import random

import pyarrow as pa
import pytest

from adbc_driver_monetdb import StatementOptions, dbapi

MIB = 1024 * 1024


def _set_latency(milliseconds: int) -> None:
    connection = http.client.HTTPConnection(
        os.environ.get("MONETDB_TOXIPROXY_HOST", "127.0.0.1"),
        int(os.environ.get("MONETDB_TOXIPROXY_API_PORT", "8474")),
        timeout=5,
    )
    connection.request("POST", "/reset", body=b"")
    response = connection.getresponse()
    response.read()
    if response.status >= 300:
        raise RuntimeError(f"toxiproxy reset failed with HTTP {response.status}")
    if milliseconds:
        payload = json.dumps(
            {
                "name": "latency",
                "type": "latency",
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": {"latency": milliseconds, "jitter": 0},
            }
        ).encode()
        connection.request(
            "POST",
            "/proxies/monetdb/toxics",
            body=payload,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        response.read()
        if response.status >= 300:
            raise RuntimeError(f"toxiproxy toxic creation failed with HTTP {response.status}")
    connection.close()


def _stats(cursor: dbapi.Cursor) -> dict[str, object]:
    return json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))


def _wide_random_batch(rows: int, columns: int) -> pa.RecordBatch:
    values = pa.py_buffer(random.Random(20260728).randbytes(rows * 8))
    array = pa.Int64Array.from_buffers(pa.int64(), rows, [None, values])
    return pa.record_batch({f"value_{index}": array for index in range(columns)})


@pytest.mark.integration
@pytest.mark.local_only
def test_measured_latency_adapts_insert_routing_and_copy_windows() -> None:
    if os.environ.get("MONETDB_RUN_NETWORK_BENCHMARK") != "1":
        pytest.skip("set MONETDB_RUN_NETWORK_BENCHMARK=1 to run the toxiproxy regression")

    proxy_uri = os.environ.get(
        "MONETDB_TOXIPROXY_URI",
        "monetdb://monetdb:monetdb@127.0.0.1:50003/test",
    )
    bulk = _wide_random_batch(9_000_000, 1)
    wide = _wide_random_batch(8_000, 1_000)
    tiny = _wide_random_batch(1_000, 1_000)
    latency_stats: dict[int, dict[str, object]] = {}
    wide_latency_stats: dict[int, dict[str, object]] = {}
    try:
        for milliseconds in (0, 1, 5, 20):
            _set_latency(milliseconds)
            with (
                dbapi.connect(proxy_uri, autocommit=True) as connection,
                connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: 0}) as cursor,
            ):
                assert (
                    cursor.adbc_ingest(
                        f"network_window_{milliseconds}ms",
                        bulk,
                        mode="replace",
                    )
                    == bulk.num_rows
                )
                latency_stats[milliseconds] = _stats(cursor)

        for milliseconds in (0, 5):
            _set_latency(milliseconds)
            with (
                dbapi.connect(proxy_uri, autocommit=True) as connection,
                connection.cursor(adbc_stmt_kwargs={StatementOptions.INGEST_INSERT_ROWS: 0}) as cursor,
            ):
                assert (
                    cursor.adbc_ingest(
                        f"network_wide_window_{milliseconds}ms",
                        wide,
                        mode="replace",
                    )
                    == wide.num_rows
                )
                wide_latency_stats[milliseconds] = _stats(cursor)

        _set_latency(5)
        with (
            dbapi.connect(proxy_uri, autocommit=True) as connection,
            connection.cursor() as cursor,
        ):
            assert cursor.adbc_ingest("network_insert_proxy", tiny, mode="replace") == tiny.num_rows
            routed = _stats(cursor)
    finally:
        _set_latency(0)

    measured: list[int] = []
    for latency in (0, 1, 5, 20):
        round_trip = latency_stats[latency]["measured_round_trip_us"]
        assert isinstance(round_trip, int)
        measured.append(round_trip)
    assert max(measured[:2]) < 5_000 <= min(measured[2:])
    assert measured[2] < measured[3]

    for milliseconds in (0, 1):
        stats = latency_stats[milliseconds]
        assert stats["copy_count"] == 2
        assert stats["window_budget_bytes"] == 512 * MIB
        assert stats["physical_window_budget_bytes"] == 128 * MIB
        assert stats["incompressible_window_budget_bytes"] == 64 * MIB
    for milliseconds in (5, 20):
        stats = latency_stats[milliseconds]
        assert stats["copy_count"] == 1
        assert stats["window_budget_bytes"] == 512 * MIB
        assert stats["physical_window_budget_bytes"] == 512 * MIB
        assert stats["incompressible_window_budget_bytes"] == 512 * MIB
    assert wide_latency_stats[0]["copy_count"] == 2
    assert wide_latency_stats[0]["physical_window_budget_bytes"] == 48 * MIB
    assert wide_latency_stats[0]["incompressible_window_budget_bytes"] == 48 * MIB
    assert wide_latency_stats[5]["copy_count"] == 1
    assert wide_latency_stats[5]["physical_window_budget_bytes"] == 512 * MIB
    assert wide_latency_stats[5]["incompressible_window_budget_bytes"] == 512 * MIB
    assert routed["path"] == "insert"
    assert isinstance(routed["insert_rows_threshold"], int)
    assert routed["insert_rows_threshold"] >= tiny.num_rows

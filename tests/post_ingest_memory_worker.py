import gc
import json
import os
import random
from pathlib import Path

import pyarrow as pa

from adbc_driver_monetdb import StatementOptions, dbapi

URI = os.environ["MONETDB_MEMORY_PROBE_URI"]
ROWS = 50_000
BATCHES = 40
PAYLOAD_BYTES = 512


def rss_bytes() -> int:
    with Path("/proc/self/statm").open() as status:
        resident_pages = int(status.read().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE")


payload_bytes = random.Random(52_217).randbytes(ROWS * PAYLOAD_BYTES)
payload = pa.FixedSizeBinaryArray.from_buffers(pa.binary(PAYLOAD_BYTES), ROWS, [None, pa.py_buffer(payload_bytes)])
schema = pa.schema([("id", pa.int64()), ("payload", pa.binary(PAYLOAD_BYTES))])


def batches() -> list[pa.RecordBatch]:
    return [
        pa.RecordBatch.from_arrays(  # pyright: ignore[reportUnknownMemberType]
            [pa.array(range(index * ROWS, (index + 1) * ROWS), type=pa.int64()), payload],
            schema=schema,
        )
        for index in range(BATCHES)
    ]


reader = pa.RecordBatchReader.from_batches(schema, batches())
gc.collect()
before = rss_bytes()
connection = dbapi.connect(URI, autocommit=True)
cursor = connection.cursor()
try:
    cursor.execute("DROP TABLE IF EXISTS review_post_ingest_memory")
    affected = cursor.adbc_ingest("review_post_ingest_memory", reader, mode="create")
    stats = json.loads(cursor.adbc_statement.get_option(str(StatementOptions.INGEST_STATS)))
    del reader
    cursor.close()
    gc.collect()
    pa.default_memory_pool().release_unused()
    after_cursor = rss_bytes()
finally:
    connection.execute("DROP TABLE IF EXISTS review_post_ingest_memory")
    connection.close()

gc.collect()
pa.default_memory_pool().release_unused()
after_connection = rss_bytes()
print(
    json.dumps(
        {
            "affected": affected,
            "expected": ROWS * BATCHES,
            "before": before,
            "after_cursor": after_cursor,
            "after_connection": after_connection,
            "arrow_allocated": pa.total_allocated_bytes(),
            "peak_window_physical_bytes": stats["peak_window_physical_bytes"],
            "peak_prefetch_physical_bytes": stats["peak_prefetch_physical_bytes"],
        }
    )
)

# Concurrent wide-JSON workload: memory exhaustion under investigation

**Status:** unresolved on Dec2025-SP4 (11.55.9), tested 2026-10-11 on native ARM64.
A synthetic concurrent workload loses its database process to the container OOM
killer. An independent server crash mechanism is not confirmed, and this is not
yet a report of a proven server defect.

## SP4 observations

The disposable server uses `wlaur/monetdb-container:11.55.9-1` with a 4 GiB memory
limit and no swap. Seed a table with 10,000 JSON documents containing an 8,000-byte
string:

```sql
CREATE TABLE concurrent_json (id BIGINT, content JSON);
INSERT INTO concurrent_json
SELECT value, CAST('{"parts":[{"text":"' || repeat('x', 8000) || '"}]}' AS JSON)
FROM sys.generate_series(1, 10001);
```

Readers repeatedly execute:

```sql
SELECT sum(length(json.text(json.filter(json.filter(json.filter(content, '$.parts'), 0), '$.text'))))
FROM concurrent_json;
```

| Workload | Result |
| --- | --- |
| One reader, one query, no writer | `80000000`; peak 739,119,104 bytes; no OOM |
| Four concurrent readers, one query each, no writer | Correct results; peak 2,412,253,184 bytes; no OOM |
| Four looping readers plus an ADBC writer, up to 200 appends of 100 comparable documents | Database receives SIGKILL; cgroup reports `oom_kill 1` |
| Four looping readers plus 20 appends of 100 documents, 60 seconds | 194 reader queries and all 20 appends complete |

The writer uses the same physical connection for each `adbc_ingest(..., mode="append")`
and autocommit. The table grows during the run, so each reader must process more data.
These observations do not distinguish excessive server memory use from aggregate
query demand exceeding the configured limit. They do not establish a concurrency
race, an allocator defect, or a relationship to the separate
[JSON cast](json-cast-memory-exhaustion.md) and
[binary-export disconnect](binary-export-abort-on-client-disconnect.md) defects.

## Remaining work

Hold data volume constant, measure allocations across reader counts, and obtain
a server backtrace if an exit occurs without an OOM kill. A negative stress run
is not sufficient evidence to mark this investigation resolved.

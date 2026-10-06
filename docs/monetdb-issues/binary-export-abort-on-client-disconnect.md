# Server aborts when a client disconnects during binary result export

**Status:** observed on 11.55.7 (Dec2025-SP3). **Not reproduced** in the 2026-09-22 pass
against `master` at `ee305e491c`, but the same pass also failed to reproduce it on 11.55.7,
so the negative result is inconclusive. The offending free is unchanged in the source at
`ee305e491c`.

## Environment where it was first seen

- MonetDB 11.55.7 (Dec2025-SP3), official `monetdb/monetdb:Dec2025-SP3` container
- Fresh database, 4 GiB available to the container, no memory pressure

## Reproduction

Start MonetDB with a published port:

```sh
docker run --rm --name monetdb-export-repro \
  -p 50000:50000 --memory 4g \
  monetdb/monetdb:Dec2025-SP3
```

Create a result large enough that it is still being transferred when the client is stopped,
and kill the client mid-transfer. Repeat; the timing depends on the machine:

```sh
for attempt in $(seq 1 20); do
  mclient -h localhost -p 50000 -d demo -u monetdb \
    -s "SELECT value, value * 2, value * 3 FROM generate_series(1, 20000001) AS g(value)" \
    >/dev/null &
  client_pid=$!
  sleep 0.2
  kill -KILL "$client_pid" 2>/dev/null || true
  wait "$client_pid" 2>/dev/null || true
done
```

The important condition is that the connection is closed while MonetDB is writing a binary
result chunk. A client using binary result transfer negotiates that mode on the connection;
no table or pre-existing data is required.

## Observed result

The database process aborts. Its log contains:

```text
MALException:sql.export_bin_column:42000!no error
mvc_export_bin_chunk: ERROR: MALException:sql.export_bin_column:42000!no error
free(): invalid pointer
database 'demo' has crashed with signal SIGABRT (dumped core)
```

Other connections to the same database are dropped while it restarts.

## Expected result

Closing a client connection during result transfer should cancel that transfer and clean up
the result. It should not abort the database process or affect other clients.

## Source

The abort follows a write failure in the binary export path. `mvc_export_bin_chunk()`
receives an error from `dump_binary_column()` and frees the returned message with
`GDKfree()`. The message is created through the query context's error allocator, so freeing
it directly appears to be the invalid free. The unhelpful `42000!no error` text also suggests
that the outer byte-counting stream is not retaining the error from the wrapped stream when
the write fails.

At `ee305e491c` this is `sql/backends/monet5/sql_result.c`, around line 2043, unchanged:

```c
str msg = dump_binary_column(info->type_rec, info->bat, offset, end_row - offset, false, countstream);
if (msg != MAL_SUCCEED) {
        GDKerror("%s", msg);
        GDKfree(msg);
        ret = -3;
        goto end;
}
```

## What the 2026-09-22 re-verification did, and why it is inconclusive

Thirty `kill -KILL` attempts at disconnect delays of 0.05 s to 1.5 s, run both from inside
the container and from the host against a published port, against `master` at `ee305e491c`.
No abort, no `SIGABRT`, nothing in `merovingian.log`.

The same harness, run as a control against `wlaur/monetdb-container:11.55.7-2` — the release
line where the abort was originally observed — also produced no abort. So the harness does
not reproduce the original conditions, and it cannot distinguish "fixed upstream" from
"not triggered".

Settling this needs the trigger identified rather than approximated: `dump_binary_column()`
must actually return an error, which means the write to the wrapped stream must fail at the
right moment. A client that forces an RST rather than a clean FIN, or a proxy that drops the
connection mid-chunk, is a more promising harness than killing `mclient`.

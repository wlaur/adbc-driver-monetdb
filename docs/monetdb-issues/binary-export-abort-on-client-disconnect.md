# Server aborts when a client resets a connection during binary result export

**Status:** reproduces on Dec2025-SP4 (11.55.9), verified 2026-10-11 on native ARM64.
Twenty consecutive binary-export resets produced `free(): invalid pointer` and `SIGABRT`.

## Reproduction

Start a disposable server with a published port and a memory limit:

```sh
docker run -d --name monetdb-export-repro -p 50004:50000 \
  --memory=4g --memory-swap=4g \
  -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=probes \
  wlaur/monetdb-container:11.55.9-1
```

Once the database is ready:

```sh
uv run docs/monetdb-issues/repro/binary-export-disconnect.py \
  'monetdb://monetdb:monetdb@localhost:50004/probes?replysize=1'
docker exec monetdb-export-repro sh -c \
  "grep -E 'invalid pointer|SIGABRT|has crashed' /var/monetdb5/dbfarm/merovingian.log"
docker rm -fv monetdb-export-repro
```

The script uses pymonetdb's MAPI connection directly, without ADBC. It requests
`Xexportbin` for a 20-million-row generated result, waits until it receives the
binary `&6` header and initial column data, then closes the TCP socket with
`SO_LINGER` set to zero, sending a reset. No tables or external data are needed.

## Observed result

The database subprocess aborts:

```text
free(): invalid pointer
database 'probes' was killed by signal SIGABRT
```

Other connections to that database are lost while the farm restarts it. A surviving
container does not mean the database subprocess survived.

## Expected result

A client disconnect should cancel the transfer and clean up its result without
terminating the database or other sessions.

## Source

[`mvc_export_bin_chunk()` in SP4's `sql_result.c`](https://github.com/MonetDB/MonetDB/blob/Dec2025_SP4_release/sql/backends/monet5/sql_result.c#L2024-L2030)
handles a failed `dump_binary_column()` with:

```c
if (msg != MAL_SUCCEED) {
        GDKerror("%s", msg);
        GDKfree(msg);
        ret = -3;
        goto end;
}
```

The error message comes from the query context's error allocator. Freeing it
directly with `GDKfree()` appears to cause the invalid free. This is a source-level
explanation consistent with the reproduced abort; a server patch has not been tested.

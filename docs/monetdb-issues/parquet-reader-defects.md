# Native Parquet reader: `DECIMAL` columns cannot be read, column 14 is renamed

**Status:** partly fixed on `master` at `ee305e491c` (56.0.0). `DECIMAL` is still unreadable;
the hardcoded rename of column 14 is still in the source. `TIMESTAMP` and `DATE` mapping,
broken when first examined, now works.

The reader is new in the `56.0.0` development line
(`sql/backends/monet5/vaults/parquet/`). It registers as a file loader, so
`SELECT * FROM '/path/x.parquet'` works, with glob support and `LIMIT` pushdown. It is
server-side only: the file must be on the server's filesystem.

## Enabling it

The module must be loaded at **bootstrap**. `76_parquet.sql`, which installs the
`sys.parquet_*` SQL functions, only runs on a database's first boot, so loading the module
later leaves those functions missing:

```sh
docker run -d --name mdb-parquet \
  -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=pq \
  -e MSERVER5_EXTRA_ARGS=--loadmodule=parquet <image>
```

`MDB_DB_PROPERTIES=loadmodules=parquet` is applied after database creation and is too late.
`SELECT * FROM 'file.parquet'` works with just the module loaded, because `fl_register()`
happens in the MAL prelude.

## Reproduction

[`repro/make-parquet-probe.py`](repro/make-parquet-probe.py) writes small probe files:

```sh
uv run docs/monetdb-issues/repro/make-parquet-probe.py /tmp/probes
docker cp /tmp/probes/p_dec15.parquet mdb-parquet:/tmp/p_dec15.parquet
docker cp /tmp/probes/p_int18.parquet mdb-parquet:/tmp/p_int18.parquet
```

### 1. `DECIMAL` columns cannot be read

```sql
SELECT * FROM '/tmp/p_dec15.parquet' LIMIT 2;
```

```text
ERROR = !failed to append
CODE  = HY002
```

The server log shows the decoder giving up on the precision:

```text
MSG pq[76]: later 15
```

That message comes from `sql/backends/monet5/vaults/parquet/pqc_reader.c:1420`, in the
dictionary-decode path, which handles precision ∈ {8, 16, 32, 64, 96, 128} and prints
`later %d` for anything else:

```c
} else {
        printf("later %d\n", r->pse->precision);
}
```

Earlier, this produced no output and returned `nrows` as if it had succeeded, so a consumer
waited forever on a source that never produced. At `ee305e491c` the read now fails cleanly
with `failed to append` instead of deadlocking, which is an improvement, but the data is
still unreadable.

A `DECIMAL(8, 2)` probe — a precision that *is* in the supported set, and which logs no
`later` message — fails the same way, so the failure is not limited to unsupported
precisions.

TPC-H's `DECIMAL(15,2)` columns are the common real-world case.

### 2. Column 14 is renamed to `e1`

`sql/backends/monet5/vaults/parquet/parquet.c:455` replaces the name of column index 14
unconditionally:

```c
char *name = NULL;
if (e->name) {
        if (i == 14)
                name = ma_strdup(sql->sa, "e1");
        else
                name = ma_strdup(sql->sa, e->name);
}
```

On an 18-column file whose columns are named `c00`…`c17`, `SELECT e1 FROM '…'` resolves and
returns column 14's values. On a 3-column file, `e1` is correctly unknown
(`SELECT: identifier 'e1' unknown`), confirming the name comes from the index-14 branch and
not from anywhere else. At `ee305e491c` the original name still resolves as well, so this
currently adds a bogus alias rather than discarding the real name — but the hardcode is
plainly leftover debugging and should go.

### 3. Fixed since first examined: `TIMESTAMP` and `DATE`

`TIMESTAMP` previously came back as `bigint` and `DATE` as `smallint` (an int32 epoch-day
truncated into 16 bits). Both now map correctly:

```text
0,0,2026-01-01 00:00:00.00000
0,0,2026-01-01
```

## Not re-tested

- **Full ingest of a large file SIGSEGVs.** A 100M-row, 14.8 GB ClickBench `hits.parquet`
  crashed the server about 100 s in. `count(*)` over the same file, which only reads
  metadata, returned 99,997,497 in 1.29 s. Re-testing needs the multi-gigabyte input.

## Other work-in-progress markers in the same tree

`printf("#using large fallback\n")` and `printf("#est == 0, %d\n")` in `rel_pphash.c:32,40`;
`printf("# todo needs check\n")` and similar in `rel_physical.c:899,1069,1080`; the
"is this tabular / has repetition" validation in `parquet.c` sits behind `if (0)`.

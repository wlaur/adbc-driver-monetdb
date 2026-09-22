# MonetDB server issues

Draft bug reports for the MonetDB server, collected while building and exercising this
driver and the SQLAlchemy dialect that sits on top of it. Each file is written to be
filed upstream as-is: a summary, a self-contained reproduction, expected versus observed
behaviour, and — where it was possible to find one — the source location responsible.

Nothing here is a driver defect. Every reproduction in this directory runs through
`mclient` or a plain container, with no ADBC involved, unless the file says otherwise.

## Status

Verified on 2026-09-22 against MonetDB `master` at
[`ee305e491c`](https://github.com/MonetDB/MonetDB/commit/ee305e491c), version `56.0.0`,
built for `linux/arm64` with [`repro/`](repro/README.md). The baseline column is the
release line the issue was first reported against.

| Issue | Class | First seen on | Status at `ee305e491c` |
| --- | --- | --- | --- |
| [`sessions-catalog-null-dereference`](sessions-catalog-null-dereference.md) | crash, any authenticated user | 11.55.7 | **reproduces** |
| [`window-avg-frame-wider-than-16`](window-avg-frame-wider-than-16.md) | silently wrong numbers | 11.55.7 | **reproduces** |
| [`rollup-grouping-order-by-empty-result`](rollup-grouping-order-by-empty-result.md) | silently missing rows | 11.55.7 | **reproduces** |
| [`alter-column-narrowing-keeps-overlong-values`](alter-column-narrowing-keeps-overlong-values.md) | silent type violation | 11.55.5 | **reproduces** |
| [`savepoint-resurrects-deleted-row`](savepoint-resurrects-deleted-row.md) | silently wrong rows | 11.55.7 | **reproduces** |
| [`remote-table-prepared-parameter`](remote-table-prepared-parameter.md) | error, feature unusable | 11.55.7 | **reproduces** |
| [`json-cast-memory-exhaustion`](json-cast-memory-exhaustion.md) | out-of-memory kill | 11.55.7 | **reproduces** |
| [`binary-export-abort-on-client-disconnect`](binary-export-abort-on-client-disconnect.md) | crash | 11.55.7 | not reproduced this pass; offending code unchanged |
| [`concurrent-json-workload-server-exit`](concurrent-json-workload-server-exit.md) | crash | 11.55.7 | not re-tested — no standalone reproduction exists |
| [`parquet-reader-defects`](parquet-reader-defects.md) | wrong data, failed reads | 56.0.0 | partly fixed, `DECIMAL` still unreadable |
| [`pipeline-binary-result-size-mismatch`](pipeline-binary-result-size-mismatch.md) | protocol error | 56.0.0 | not re-tested — needs a loaded TPC-DS database |
| [`pipeline-disables-mitosis`](pipeline-disables-mitosis.md) | design, large slowdown | 56.0.0 | not re-tested — measurement, not a pass/fail check |
| [`monetdbd-fresh-database-second-boot`](monetdbd-fresh-database-second-boot.md) | startup failure | 56.0.0 | **fixed** |
| [`prepared-statement-loses-temporary-table`](prepared-statement-loses-temporary-table.md) | lost DDL | 11.55.0–11.55.6 | **fixed** in 11.55.7 |
| [`tmp-schema-catalog-misbinding`](tmp-schema-catalog-misbinding.md) | wrong catalog rows | 11.55.1 | not reproduced |

Four entries were not re-verified in this pass. Each says why in its own file, and what
would be needed to settle it. They are the expensive ones: a populated TPC-DS database, a
multi-gigabyte Parquet ingest, a concurrent multi-client workload, and a whole-suite
performance comparison.

## Reproducing

[`repro/`](repro/README.md) holds everything needed to rebuild a server from a MonetDB
checkout and re-run the checks:

```sh
docs/monetdb-issues/repro/build-image.sh /path/to/MonetDB monetdb-tip:local
docs/monetdb-issues/repro/check-issues.sh monetdb-tip:local
```

## Conventions

- `monetdb/monetdb` is the official image and supplies `linux/amd64`. On Apple Silicon the
  native `linux/arm64` builds come from
  [`wlaur/monetdb-container`](https://github.com/wlaur/monetdb-container) for releases, and
  from `repro/` for arbitrary commits.
- Every reproduction creates a disposable database. None of them needs a host port, a
  mounted volume, or an existing dataset unless stated.
- A file whose status is not "reproduces" is kept rather than deleted: knowing a defect was
  fixed, and in which version, is the thing a support policy is written against.

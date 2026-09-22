# A prepared statement in the same transaction loses a later temporary-table definition

**Status: fixed in 11.55.7.** Affected 11.55.0–11.55.6. Re-checked on `master` at
`ee305e491c`: the temporary table survives and holds its rows.

## Summary

On 11.55.0 through 11.55.6, MonetDB could acknowledge a `CREATE LOCAL TEMPORARY TABLE` without
retaining it, when a prepared statement had already run in the caller's transaction. The
`CREATE` returned success; the table was not there afterwards.

## Reproduction

```sql
START TRANSACTION;
PREPARE SELECT ? + 1;
CREATE LOCAL TEMPORARY TABLE tmp_after_prepare (x INTEGER) ON COMMIT PRESERVE ROWS;
INSERT INTO tmp_after_prepare VALUES (1);
SELECT count(*) FROM tmp_after_prepare;
COMMIT;
```

Expected — and what `ee305e491c` returns:

```text
1
```

On the affected releases the `INSERT` or the `SELECT` fails because the table is not there.

## Why this driver cares

COPY-sized appends to an existing constrained table use an unconstrained staging table. From
11.55.7 onward that staging table is session-local. On 11.55.0–11.55.6 the driver instead uses
a uniquely named, transaction-scoped `UNLOGGED` table in the target schema, which is why
constrained staging needs `CREATE TABLE` rights there on those releases, and why
temporary-table targets stay on the direct path.

That workaround is what this file documents the reason for. See
[`docs/design-decisions.md`](../design-decisions.md), "Ingest routing, retention, and
topology".

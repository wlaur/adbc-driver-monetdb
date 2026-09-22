# `sys.columns` can misbind to the temporary schema after a temporary-table preflight

**Status: not reproduced.** Recorded because the driver avoids the affected view on its
account, and because the workaround would otherwise look unmotivated. Observed on 11.55.1. A
direct re-check on `master` at `ee305e491c` returned the correct rows.

## Summary

On the 11.55.1 release, a temporary-table preflight executed immediately before reading the
public `sys.columns` view could make that view's internal `_columns` reference bind to the
temporary schema, so the read returned the wrong rows.

## Re-check

```sql
CREATE TABLE misbind_probe (a INTEGER, b VARCHAR(10));
CREATE LOCAL TEMPORARY TABLE tmp_preflight (x INTEGER) ON COMMIT PRESERVE ROWS;
SELECT count(*) FROM sys.columns c JOIN sys.tables t ON c.table_id = t.id
 WHERE t.name = 'misbind_probe';
```

At `ee305e491c` this returns `2`, which is correct. The original report's sequence came from
driver internals rather than from a script, so this simplified form may not be the exact
trigger — treat the negative as weak evidence.

## Why this driver cares

Append-schema reads use `sys._columns` / `sys._tables`, or their `tmp` counterparts when the
ADBC temporary-table option is set, rather than the public `sys.columns` view. The base
catalogs provide the same metadata without that binder behaviour, and the minimum-version
integration job covers the sequence. See
[`docs/design-decisions.md`](../design-decisions.md), "Ingest routing, retention, and
topology".

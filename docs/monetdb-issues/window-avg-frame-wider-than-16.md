# Wrong `avg()` over a `ROWS` window frame wider than 16

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).

## Summary

`avg()` over a `ROWS BETWEEN n PRECEDING AND CURRENT ROW` frame returns **incorrect values**
once the frame can hold more than 16 rows. No error is raised; the query simply returns wrong
numbers.

Frames of at most 16 rows are always correct. From 17 rows upward, results are wrong starting
at row index 16 (the 17th row). The boundary is exactly 16 regardless of the data, which
suggests an internal vector or batch width.

This is the most serious of the silent-wrong-answer issues: a report, a dashboard, or any
downstream consumer of these values has no way to notice.

## Reproduction

Self-contained, integers 1..41, so every expected average is checkable by hand:

```sql
CREATE TABLE mdb_window_repro (i INTEGER, v DOUBLE);
INSERT INTO mdb_window_repro SELECT value, cast(value as double) FROM sys.generate_series(1, 41);

SELECT i, v,
       avg(v) OVER (ORDER BY i ROWS BETWEEN 16 PRECEDING AND CURRENT ROW) AS ra
FROM mdb_window_repro
ORDER BY i;
```

At `i = 17` the frame is rows 1..17, so the average must be `(1+17)/2 = 9.0`:

| i | MonetDB `ra` | expected |
| --- | --- | --- |
| 15 | 8.00 | 8.00 |
| 16 | 8.50 | 8.50 |
| **17** | **12.75** | **9.00** |
| 18 | 10.00 | 10.00 |

Verbatim output on `master` at `ee305e491c`:

```
15,8
16,8.5
17,12.75
18,10
```

## Frame width is the trigger

Same table, varying only the frame:

| Frame | Rows wrong (of 41) | First wrong |
| --- | --- | --- |
| `15 PRECEDING` (max 16 rows) | 0 | — |
| `16 PRECEDING` (max 17 rows) | 3 | i=17, got 12.75, want 9.00 |
| `59 PRECEDING` (max 60 rows) | 23 | i=17, got 12.75, want 9.00 |

Casting the column, ordering in a subquery, and omitting `ORDER BY` inside the window all
make no difference. It is not a float-precision effect: the errors are far larger than any
rounding, and the values are exactly reproducible.

## Where it was hit

A 60-row moving average over a `REAL` column:

```sql
SELECT time,
       value,
       avg(value) OVER (ORDER BY time ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM measurements
ORDER BY time
LIMIT 10000;
```

Rows 0-15 matched DuckDB exactly; row 16 onward diverged. Verified against an independent
NumPy float64 computation over the same values: DuckDB is correct, MonetDB is not.

| Row | True mean | MonetDB |
| --- | --- | --- |
| 15 | 156.5181188583 | 156.5181188583 |
| 16 | 156.5231018066 | 156.5604739189 |
| 17 | 156.5259475708 | 156.5650911331 |

## Workaround

None known at the query level. A frame of 16 rows or fewer is correct, but that changes the
question being asked.

## Note when re-testing

A `WHERE` clause on the same query level filters rows **before** the window function runs, so
the frame never widens past the filtered set and the bug does not appear. Compute the window
in a subquery and filter outside it:

```sql
SELECT i, ra FROM (
  SELECT i, avg(v) OVER (ORDER BY i ROWS BETWEEN 16 PRECEDING AND CURRENT ROW) AS ra
  FROM mdb_window_repro
) t WHERE i BETWEEN 15 AND 18 ORDER BY i;
```

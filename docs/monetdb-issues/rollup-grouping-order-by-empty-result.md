# Empty result when `ORDER BY` has a `CASE` over `grouping()`

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).

## Summary

A `GROUP BY ROLLUP` query that both

* projects a window function whose `PARTITION BY` contains a `CASE` over `grouping()`, and
* sorts by a statement-level `ORDER BY` that also contains a `CASE` over `grouping()`

returns **no rows and no result-set metadata at all**. No error is raised. Dropping either
`CASE` returns the expected six rows.

Silently returning nothing is the worst-case failure mode here: a caller cannot distinguish
it from a genuinely empty table, so it corrupts results rather than failing the query.

This is not a synthetic pattern. It is exactly the shape of **TPC-DS queries 70 and 86**,
both of which carry it verbatim, and both of which return nothing on MonetDB while returning
3 and 100 rows respectively on DuckDB.

Seen through this driver at 0.12.0, which reports it as a zero-column result, and confirmed
through `mclient`.

## Reproduction

Self-contained, three rows:

```sql
CREATE TABLE mdb_rollup_repro (a VARCHAR(10), b VARCHAR(10), v INTEGER);
INSERT INTO mdb_rollup_repro VALUES ('x','p',1), ('x','q',2), ('y','r',3);

-- returns 6 rows, as expected
SELECT sum(v) AS s, a, b, grouping(a)+grouping(b) AS lh,
       rank() OVER (PARTITION BY grouping(a)+grouping(b),
                                 CASE WHEN grouping(b)=0 THEN a END
                    ORDER BY sum(v) DESC) AS rk
FROM mdb_rollup_repro
GROUP BY rollup(a,b)
ORDER BY lh DESC, rk;

-- identical query, one extra ORDER BY term: returns NOTHING, and no error
SELECT sum(v) AS s, a, b, grouping(a)+grouping(b) AS lh,
       rank() OVER (PARTITION BY grouping(a)+grouping(b),
                                 CASE WHEN grouping(b)=0 THEN a END
                    ORDER BY sum(v) DESC) AS rk
FROM mdb_rollup_repro
GROUP BY rollup(a,b)
ORDER BY lh DESC, CASE WHEN grouping(a)+grouping(b)=0 THEN a END, rk;
```

Expected: the same six rows, ordered. Actual: `mclient` prints nothing; the driver reports a
0-row, 0-column result.

Wrapping the broken statement in `SELECT count(*) FROM (...) t` also prints nothing — not
even a `0` — which is what "no result-set metadata" means in practice.

## Which element triggers it

Both `CASE`-over-`grouping()` expressions must be present. Each alone is fine.

| Variant | Result |
| --- | --- |
| rollup, no window, `ORDER BY CASE ... grouping()` | 6 rows |
| rollup + window, no statement-level `ORDER BY` | 6 rows |
| rollup + window, `ORDER BY lh DESC, rk` | 6 rows |
| rollup + window, `ORDER BY ... CASE ... grouping() ...` | **0 rows, no error** |

`NULLS FIRST` and `LIMIT` are irrelevant — removing them does not change the outcome.

## Where it was hit

TPC-DS **q86** and **q70**, taken verbatim from the DuckDB `tpcds` extension's query set.
Both are affected because the TPC-DS reporting-hierarchy pattern puts a `CASE` over
`grouping()` in both the window partition and the final sort. q86 in full:

```sql
SELECT sum(ws_net_paid) AS total_sum, i_category, i_class,
       grouping(i_category)+grouping(i_class) AS lochierarchy,
       rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
                                  CASE WHEN grouping(i_class) = 0 THEN i_category END
                    ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM web_sales, date_dim d1, item
WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
  AND d1.d_date_sk = ws_sold_date_sk
  AND i_item_sk = ws_item_sk
GROUP BY rollup(i_category,i_class)
ORDER BY lochierarchy DESC NULLS FIRST,
         CASE WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category END NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;
```

q70 is the same pattern over `store_sales` with `rollup(s_state, s_county)`. On TPC-DS SF1:

| Query | DuckDB | MonetDB |
| --- | --- | --- |
| q70 | 3 rows | 0 rows, no error |
| q86 | 100 rows | 0 rows, no error |

Removing the `CASE` term from the final `ORDER BY` makes both return rows on MonetDB.

## Workaround

Compute the ordering key as a column in a subquery and sort on it in the outer query, which
leaves the projected columns unchanged:

```sql
SELECT s, a, b, lh, rk
FROM (
  SELECT sum(v) AS s, a, b, grouping(a)+grouping(b) AS lh,
         CASE WHEN grouping(a)+grouping(b)=0 THEN a END AS order_key,
         rank() OVER (PARTITION BY grouping(a)+grouping(b),
                                   CASE WHEN grouping(b)=0 THEN a END
                      ORDER BY sum(v) DESC) AS rk
  FROM mdb_rollup_repro
  GROUP BY rollup(a,b)
) AS grouped
ORDER BY lh DESC, order_key, rk;
```

Note `ordered` is reserved in MonetDB and cannot be used as the subquery alias.

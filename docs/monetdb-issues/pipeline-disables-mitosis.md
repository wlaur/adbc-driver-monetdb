# Enabling the pipeline engine disables mitosis for every client

**Status:** a design consequence rather than a defect, recorded because it makes the flag
unusable as a global switch. Source checked against `master`; the performance figures come
from a `56.0.0` build at `bbc2d72f02` and were **not** re-measured at `ee305e491c`.

## What happens

`sql/backends/monet5/rel_physical.c` chooses between the new pipeline planner and classic
mitosis:

```c
const ATOMIC_BASE_TYPE oahash_enabled = (1U<<19);
if (!SQLrunning || !(ATOMIC_GET(&GDKdebug) & oahash_enabled)
    || gp.complex_modify || gp.cnt[op_except] || gp.cnt[op_inter]) {
    (void)rel_partition(&v, sql, rel);    /* classic mitosis */
} else {
    (void)rel_pipeline(&v, rel, true, 0); /* new engine */
}
```

Separately, `sql/backends/monet5/sql_optimizer.c:139` sets `c->no_mitosis = 1` for **all**
clients whenever the flag is on.

So a query shape the pipeline planner declines — `EXCEPT`, `INTERSECT`, complex modifications,
and everything else the guard rejects — runs with *neither* pipeline nor mitosis parallelism.
It loses all intra-query parallelism, rather than falling back to what it had before.

## Observed effect

Across a mixed analytical workload, the declined queries clustered at almost exactly 0.3× the
throughput of the same queries on 11.55.7 — a flat ~3× penalty, which is what losing mitosis
looks like. Individual queries the pipeline *does* accept were up to 23× faster, so the engine
is doing what it claims on its target shapes; the problem is the all-or-nothing switch.

`SQLrunning` is declared in `sql_scenario.h:15` as
`// dev. debug var, 2 remove once the code is ~stable`, which is consistent with the flag not
yet being meant for general use.

## Suggested direction

Per-query-shape engine selection, so a declined query keeps mitosis. Today the chooser is one
global boolean that also disables mitosis; the
[2024 roadmap](https://www.monetdb.org/about-us/roadmap-2024/) already promises "new
optimisers to determine the best execution strategy for a given query", and this is the case
that makes it mandatory rather than nice to have.

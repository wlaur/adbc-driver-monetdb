# Enabling the pipeline engine disables mitosis for every client

**Status:** a design consequence rather than a defect, recorded because it makes the flag
unusable as a global switch. Re-measured on `master` at `ee305e491c` (56.0.0) on 2026-09-22
against a Dec2025-SP3 baseline on the same machine.

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

Warm-query totals at `ee305e491c`, common queries only, against Dec2025-SP3 on the same
machine. `off` is MonetDB's shipped default; `on` sets `-d524288`:

| suite | SP3 | pipeline off | pipeline on | off vs SP3 | on vs SP3 |
|---|---:|---:|---:|---:|---:|
| kaggle_airbnb | 145.07 s | 363.12 s | 39.95 s | 0.40× | **3.63×** |
| clickbench | 109.05 s | 95.21 s | 69.47 s | 1.15× | 1.57× |
| chat_threads | 33.60 s | 28.35 s | 41.04 s | 1.19× | **0.82×** |
| time_series | 0.93 s | 1.00 s | 1.07 s | 0.93× | 0.86× |

The engine is violently bimodal: a 3.63× win on the suite whose shapes it targets, and a loss
on two of the other three. RTABench, TPC-H and TPC-DS are absent because with the engine
enabled each hit a corrupt result — see
[pipeline-corrupt-binary-results](pipeline-corrupt-binary-results.md) — so their query sets no
longer match and the totals are not comparable.

An earlier campaign on `bbc2d72f02` measured the declined queries clustering at almost exactly
0.3× the throughput of the same queries on 11.55.7 — a flat ~3× penalty, which is what losing
mitosis looks like — while individual accepted queries ran up to 23× faster. The engine does
what it claims on its target shapes; the problem is the all-or-nothing switch.

`SQLrunning` is declared in `sql_scenario.h:15` as
`// dev. debug var, 2 remove once the code is ~stable`, which is consistent with the flag not
yet being meant for general use.

## Suggested direction

Per-query-shape engine selection, so a declined query keeps mitosis. Today the chooser is one
global boolean that also disables mitosis; the
[2024 roadmap](https://www.monetdb.org/about-us/roadmap-2024/) already promises "new
optimisers to determine the best execution strategy for a given query", and this is the case
that makes it mandatory rather than nice to have.

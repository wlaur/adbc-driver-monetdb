# Reproduction tooling

Run the self-contained SQL checks against the pinned native ARM64 SP4 image:

```sh
./check-issues.sh wlaur/monetdb-container:11.55.9-1
```

The checks cover window averages, rollup ordering, VARCHAR narrowing, savepoint
visibility, and `sys.sessions`. The sessions check deliberately crashes the database
when the defect is present, so it runs last. Remote-table and JSON memory checks
have separate recipes in their issue reports. `json-cast.sql` is the synthetic
single-statement fixture for the JSON cast report; run it with the documented
container memory limit.

To test a proposed server fix, build from a local public MonetDB checkout:

```sh
./build-image.sh /path/to/MonetDB monetdb-probe:local Dec2025_SP4_release
./check-issues.sh monetdb-probe:local
```

The build takes its source through `git archive` and leaves the checkout unchanged.
The default source ref is `Dec2025_SP4_release`. No server patches are applied.

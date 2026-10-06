# Reproduction tooling

Everything needed to build a MonetDB server from an arbitrary commit and re-run the checks
in [`../`](../README.md). Nothing here is part of the driver or its build.

## Building an image

`build-image.sh` builds a native `linux/arm64` image from a local MonetDB checkout. It takes
the source with `git archive`, so the checkout's working tree and current branch are left
alone.

```sh
git clone https://github.com/MonetDB/MonetDB ~/src/MonetDB
./build-image.sh ~/src/MonetDB monetdb-tip:local              # origin/master
./build-image.sh ~/src/MonetDB monetdb-56:local ee305e491c    # a specific commit
```

A full `master` build takes about a minute on an M4 Pro with 12 build threads.

`Dockerfile` differs from the release image in
[`wlaur/monetdb-container`](https://github.com/wlaur/monetdb-container) in four ways, all
required by the `56.0.0` development line:

| addition | why |
|---|---|
| `libxxhash-dev` / `libxxhash0` | **hard requirement** — cmake fails without it |
| `libsnappy-dev` / `libsnappy1v5` | Parquet codecs |
| `libzstd-dev` / `libzstd1` | Parquet codecs |
| `libbrotli-dev` / `libbrotli1` | Parquet codecs |

plus `-DWITH_SNAPPY=ON -DWITH_ZSTD=ON -DWITH_BROTLI=ON`, all of which default to `ON`.
`entrypoint.sh` is a copy of the one in that repository.

For released versions, prefer the published images — `monetdb/monetdb` for `linux/amd64`,
`wlaur/monetdb-container` for native `linux/arm64` — and build from source only for commits
that have no image.

## Running the checks

```sh
./check-issues.sh monetdb-tip:local
```

One container, `mclient` only. Covers the window-frame, rollup/`ORDER BY`, `VARCHAR`
narrowing, `SAVEPOINT`, prepared-statement/temporary-table and `sys.sessions` checks, and
prints what each result should be. The last check deliberately crashes the server when the
defect is present, so it runs last.

`PLATFORM=linux/amd64 ./check-issues.sh monetdb/monetdb:latest` runs the same checks against
the official image.

```sh
./boot-test.sh monetdb-tip:local 6
```

Creates a fresh dbfarm, boots it, restarts the container and greps `merovingian.log`, six
times over — the check for
[`monetdbd-fresh-database-second-boot`](../monetdbd-fresh-database-second-boot.md).

```sh
./make-parquet-probe.py /tmp/probes
```

Writes the small Parquet files used by
[`parquet-reader-defects`](../parquet-reader-defects.md). Needs `uv`; the script carries its
own dependency metadata.

`json-cast.sql` is the single-statement fixture for
[`json-cast-memory-exhaustion`](../json-cast-memory-exhaustion.md); that file has the full
recipe, including the cgroup limits the reproduction depends on.

## The pipeline engine

The `56.0.0` line carries a morsel-driven pipeline runtime that is **off by default**, behind
`GDKdebug` bit 19 (524288) — a value not listed in `gdk/gdk.h`:

```sh
docker run ... -e MSERVER5_EXTRA_ARGS=-d524288 monetdb-tip:local
./check-issues.sh monetdb-tip:local 524288
```

At runtime, `SELECT sys.debug(524288);` *sets* the mask and returns the **previous** value;
`sys.debug(0)` disables it; `sys.debugflags()` reads without modifying. Enabling it also
forces `no_mitosis = 1` for every client — see
[`pipeline-disables-mitosis`](../pipeline-disables-mitosis.md).

The native Parquet reader needs its module loaded at **bootstrap**, because `76_parquet.sql`
only runs on a database's first boot:

```sh
docker run ... -e MSERVER5_EXTRA_ARGS=--loadmodule=parquet monetdb-tip:local
```

## `apply-patch.py`

Makes the `sys.tracelog` grant in `sql_update_sep2022` tolerate SQLSTATE `01007`. It was
needed to boot images built from `master` around `bbc2d72f02`; it is not needed at
`ee305e491c`. Pass `1` as the fourth argument to `build-image.sh` to apply it when building
an older commit. See
[`monetdbd-fresh-database-second-boot`](../monetdbd-fresh-database-second-boot.md) for what
it works around and why it was never a complete fix.

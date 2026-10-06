# Native `JSON` cast exhausts memory on a small document

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).

A single 289,086-byte JSON document exhausts a disposable database container's 4 GiB memory
limit when cast to native `JSON`. Casting the identical value to `TEXT` completes in
milliseconds. No table and no insert are involved.

## Captured result

Re-verified 2026-09-22 against a `master` build, executing
[`repro/json-cast.sql`](repro/json-cast.sql) through `mclient` in a 4 GiB container:

| Operation | Result |
| --- | --- |
| `SELECT LENGTH(CAST('<document>' AS TEXT))` | `289086`, immediate |
| `SELECT LENGTH(CAST('<document>' AS JSON))` | database process killed, about one second in |
| Container `memory.peak` | `4294967296` bytes |
| Container `memory.events` | `oom 159`, `oom_kill 1` |

The client sees `ERROR = !unexpected end of file`.

The first observation, on 11.55.7, came through a SQLAlchemy bound parameter rather than a
SQL literal, establishing that the native JSON conversion triggers the failure without a
batch insert or accumulated rows. An earlier run against a database with no memory limit was
killed by the host OOM killer at approximately 59.3 GiB anonymous RSS.

## Reproduction

[`repro/json-cast.sql`](repro/json-cast.sql) is a single
`SELECT LENGTH(CAST('<document>' AS JSON));`. The document is synthetic: one message
containing 60 tool results, each holding 100 small numeric objects. It requires no Python,
no application code, no tables, and no inserts. Its SHA-256, after SQL quote-unescaping, is
`9b1722bcc0173a2ff14f7891dfb41e611a6e91145baed96602a061872defe284`.

Use a disposable database with a hard memory limit and no swap:

```sh
docker run --detach --name monetdb-json-repro \
  --memory=4g --memory-swap=4g --pids-limit=256 \
  -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=jsonrepro \
  monetdb/monetdb:Dec2025-SP3

docker exec monetdb-json-repro sh -c \
  'cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max'

docker cp docs/monetdb-issues/repro/json-cast.sql monetdb-json-repro:/tmp/json-cast.sql
docker exec monetdb-json-repro sh -c \
  'sed "s/AS JSON/AS TEXT/" /tmp/json-cast.sql > /tmp/text-cast.sql'
```

Wait for the database farm to start and confirm the limits print `4294967296` and `0`. Run
the control first:

```sh
docker exec monetdb-json-repro sh -c 'umask 077;
  printf "user=monetdb\npassword=monetdb\n" > /tmp/.m;
  DOTMONETDBFILE=/tmp/.m mclient -h127.0.0.1 -p50000 -djsonrepro -fcsv /tmp/text-cast.sql'
```

Expected control result: `289086`. Then run the native JSON cast:

```sh
docker exec monetdb-json-repro sh -c 'umask 077;
  printf "user=monetdb\npassword=monetdb\n" > /tmp/.m;
  DOTMONETDBFILE=/tmp/.m mclient -h127.0.0.1 -p50000 -djsonrepro -fcsv /tmp/json-cast.sql'
```

Inspect the cgroup counters after the query, even if the client disconnects. The farm
container survives an OOM kill of its database subprocess, so container uptime alone is not
evidence:

```sh
docker exec monetdb-json-repro sh -c \
  'cat /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events'
docker logs --tail 100 monetdb-json-repro
docker rm --force monetdb-json-repro
```

## Not explained by token density alone

This smaller control returns `100003` and peaks at about 49 MB with all OOM counters zero:

```sql
SELECT LENGTH(CAST('[' || REPEAT('0,', 50000) || '0]' AS TEXT));
```

Changing `TEXT` to `JSON` there would test token density independently of nesting; that
variant has not been executed.

## Expected behavior

A roughly 289 kB valid JSON value should not require more than 4 GiB merely to cast and
measure its length. Conversion should complete with bounded memory, or return a controlled
error, without killing the database process.

## Remaining work

Further minimization and an allocation profile are still needed before assigning a parser
root cause. What the document's shape contributes — nesting depth, object count, or total
token count — is not established.

# `SAVEPOINT` makes a deleted row visible again

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).

MonetDB returns a previously deleted row after creating a savepoint in the same transaction.
No rollback to the savepoint is involved.

## Reproduction

First verified 2026-09-15 on the official `monetdb/monetdb:Dec2025-SP3` image; re-verified
2026-09-22 on a `master` build. This starts a disposable database with no external network
access and no host ports:

```sh
docker run -d --name monetdb-savepoint-repro --network none \
  -e MDB_CREATE_DBS=scratch -e MDB_DB_ADMIN_PASS=monetdb \
  monetdb/monetdb:Dec2025-SP3
```

Once the container is ready, run the following. `mclient` uses its default autocommit mode,
so the seed row is committed before `START TRANSACTION`.

```sh
docker exec -i monetdb-savepoint-repro sh -c '
  umask 077
  printf "user=monetdb\npassword=%s\n" "$MDB_DB_ADMIN_PASS" > /tmp/mclient.conf
  export DOTMONETDBFILE=/tmp/mclient.conf
  exec mclient -h 127.0.0.1 -p 50000 -d scratch -f csv
' <<'SQL'
CREATE TABLE savepoint_probe(id INTEGER);
INSERT INTO savepoint_probe VALUES (1);
START TRANSACTION;
DELETE FROM savepoint_probe WHERE id = 1;
SAVEPOINT sp;
SELECT id FROM savepoint_probe;
ROLLBACK;
DROP TABLE savepoint_probe;
SQL
```

**Expected:** the `SELECT` returns zero rows because the only row was deleted.

**Actual:** it returns the deleted row:

```text
1
```

The result reproduced in three consecutive runs on 11.55.7 and again on `master`. Removing
`SAVEPOINT sp` makes the `SELECT` return zero rows — that control was re-run on `master` and
returned nothing, as it should. A separate check confirmed that selecting before the
savepoint and after `RELEASE SAVEPOINT sp` also returns zero rows.

The committed seed row and the `DELETE` predicate matter: creating and inserting inside the
tested transaction, or using `DELETE FROM savepoint_probe` without a `WHERE` clause, did not
reproduce the defect. No primary key is needed.

Remove the disposable container and its volumes afterwards:

```sh
docker rm -fv monetdb-savepoint-repro
```

## Source hint

In `Dec2025_SP3_release`, inspect
[`OLD_VALID_4_READ` and `SEG_IS_VALID` in `sql/storage/bat/bat_storage.c`](https://github.com/MonetDB/MonetDB/blob/d7afdba1728b74fda8fc62ed583d5c559c7a70dc/sql/storage/bat/bat_storage.c#L45-L63).
`SAVEPOINT` creates a child transaction with a new transaction ID. `OLD_VALID_4_READ`
excludes the current ID but, unlike `VALID_4_READ`, does not check parent transaction IDs.
This appears to let `SEG_IS_VALID` classify a row deleted by the parent as visible when
building the SELECT candidate rows. This is a likely cause from source inspection; a server
patch has not been tested.

# Prepared query over a remote table loses its parameter on the remote server

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).

## Summary

MonetDB accepts `PREPARE` for a parameterized query over a remote table, but the
corresponding `EXECUTE` fails on the source server because the generated remote relational
plan references parameter `A0` without defining it.

The equivalent query with a literal succeeds. The same prepared query also succeeds when it
runs directly against a local table.

This reproduces with `mclient` alone, without ADBC or SQLAlchemy.

## Reproduction

Two MonetDB instances that can reach each other. On a Docker network:

```sh
docker network create mdbnet
docker run -d --name mdb-src   --network mdbnet \
  -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=remote monetdb/monetdb:Dec2025-SP3
docker run -d --name mdb-coord --network mdbnet \
  -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=test   monetdb/monetdb:Dec2025-SP3
```

Run on the source database (`remote` on host `mdb-src`):

```sql
CREATE TABLE remote_source(id BIGINT);
INSERT INTO remote_source VALUES (1), (2), (3);
```

Run on the coordinator database (`test` on host `mdb-coord`):

```sql
CREATE REMOTE TABLE remote_source(id BIGINT)
ON 'mapi:monetdb://mdb-src:50000/remote/sys/remote_source'
WITH USER 'monetdb' PASSWORD 'monetdb';

CREATE VIEW remote_view AS
SELECT id FROM remote_source;
```

The literal query succeeds:

```sql
SELECT AVG(id) FROM remote_view WHERE id > 0;
```

```text
2
```

Prepare the equivalent parameterized query:

```sql
PREPARE SELECT AVG(id) FROM remote_view WHERE id > ?;
```

In a fresh session this creates statement id `0`. The id can also be read from
`SELECT statementid, statement FROM sys.prepared_statements;`. Execute it:

```sql
EXECUTE 0(0);
```

The coordinator reports:

```text
Exception occurred in the remote server, please check the log there
```

and the source server's `merovingian.log` reports:

```text
ERR remote[69]: #client0023: createExceptionInternal: ERROR:
                SQLException:RAstatement2:42000!Identifier A0 doesn't exist
```

## Expected result

`EXECUTE 0(0)` should return the same result as the literal query, `2`.

## Additional observation

`PREPARE` metadata is also incomplete for the remote query. It contains the `BIGINT`
parameter row but no `DOUBLE` result row:

```text
bigint,63,0,,,
```

Preparing the same query directly against the local source table returns both rows:

```text
double  53  0
bigint   2  0
```

## Source-level indication

In `sql/backends/monet5/sql_gencode.c`, prepared parameters are introduced as `A0`, `A1`, and
so on. The distributed-query path serializes a relational plan and a separate variable
signature for reconstruction by `RAstatement2`. `RAstatement2` then calls `rel_read()` in
`sql/backends/monet5/sql_execute.c`.

The observed error indicates that the serialized remote plan contains `A0`, but the signature
or reconstructed scope does not contain the corresponding prepared parameter. The missing
result metadata may be another consequence of the same prepared/distributed-plan boundary.

The separate case where `COPY (SELECT ...) INTO BINARY ... ON CLIENT` produces no client
output when the select touches a remote table is not covered by this reproduction.

# Narrowing a `VARCHAR` column silently leaves over-long data in place

**Status:** reproduces on `master` at `ee305e491c` (56.0.0), 11.55.7 and 11.55.5.
**Found:** 2026-08-05, migrating a schema to the width its ORM declares.

## Summary

`ALTER TABLE ... ALTER COLUMN <col> VARCHAR(n)` is accepted without error or warning when
the column already contains values longer than `n`. The catalog is updated to `n`, the
over-long values are left untouched, and nothing reports that the table now holds data that
violates its own declared type.

The length constraint is otherwise enforced: inserting an over-long literal or expression
into the same column is rejected with `22001`. So the value sitting in the table is one the
database would refuse to accept.

This is not merely cosmetic. The inconsistent value propagates into other tables through
ordinary `INSERT ... SELECT`, and `msqldump` produces a dump that the server cannot restore —
the affected table is missing from the restored database.

## Affected versions

| Image | `monet_version` | `revision` |
|---|---|---|
| `monetdb/monetdb:Dec2025-SP2` | 11.55.5 | `691e862e16ce` |
| `monetdb/monetdb:Dec2025-SP3` | 11.55.7 | `1d742c123cb0` |
| built from `master` | 56.0.0 | `ee305e491c` |

Reproduced through `mclient` only — no application driver involved.

## Reproduction

Run each statement separately (a failing statement aborts the transaction and would roll
back the `CREATE TABLE`):

```sql
CREATE TABLE t (c VARCHAR(1024));
INSERT INTO t VALUES (repeat('x', 900));

SELECT length(c) FROM t;                      -- 900

ALTER TABLE t ALTER COLUMN c VARCHAR(255);    -- accepted, no error, no warning

SELECT length(c) FROM t;                      -- 900   <-- still over-long
SELECT type_digits FROM sys.columns c
  JOIN sys.tables tt ON c.table_id = tt.id
 WHERE tt.name = 't' AND c.name = 'c';        -- 255   <-- catalog says 255
```

## Observed vs expected

**Observed:** the ALTER succeeds silently; the column is declared `VARCHAR(255)` while
holding a 900-character value.

**Expected:** one of

1. reject the ALTER while non-conforming rows exist (PostgreSQL does this:
   `ERROR: value too long for type character varying(255)`), or
2. truncate as part of the ALTER, or
3. at minimum, emit a warning.

Silent acceptance is the one outcome that leaves the database in a state it will not
otherwise allow to be created.

## Consequences

### 1. The stored value is one the database would reject

On the very same column, after the ALTER:

```sql
INSERT INTO t VALUES (repeat('y', 300));
-- ERROR = !value too long for type (var)char(255)
-- CODE  = 22001
```

Verified on a pristine, never-altered `VARCHAR(255)` column that both an over-long
**literal** and an over-long **expression** are rejected with `22001`. The constraint is
real; the ALTER just does not apply it to existing rows.

### 2. The violation propagates to other tables

`INSERT ... SELECT` from the altered column is **accepted** into a pristine `VARCHAR(255)`
column, apparently because the source column's declared type (255) is trusted rather than the
values inspected:

```sql
CREATE TABLE p (c VARCHAR(255));       -- pristine, never altered
INSERT INTO p SELECT c FROM t;         -- 1 affected row, no error
SELECT length(c) FROM p;               -- 900
```

So one silent ALTER seeds a value that then spreads through ordinary SQL into columns that
never had anything done to them.

### 3. The database cannot restore its own dump

`msqldump` writes the catalog's DDL (`VARCHAR(255)`) together with the 900-character record:

```sql
CREATE TABLE "sys"."t" (
        "c" VARCHAR(255)
);
COPY 1 RECORDS INTO "sys"."t" FROM stdin USING DELIMITERS E'\t',E'\n','"';
```

Restoring that dump into a fresh database fails:

```
Failed to import table 't', line 1: column 1 c: 'varchar(255)' expected in 'xxxxx...'
Current transaction is aborted (please ROLLBACK)
syntax error, unexpected IDENT in: ""xxxxx..."
```

The restored database ends up **without table `t` at all** (`SELECT: no such table 't'`). The
dump looks successful, and the loss only surfaces on restore — potentially long after the
ALTER that caused it, and with no indication of which table or ALTER was responsible.

## Why this is easy to hit

A schema-migration tool emits exactly this ALTER when a model's declared width is reduced.
The migration reports success, a subsequent schema-diff check reports the schema as correct,
and nothing is wrong until either a row is rewritten or a backup is restored. This surfaced
on a routine width change, and only because the stored data was asserted on afterwards.

## Workaround

Truncate explicitly. Ordering matters, because of a second behaviour: `ALTER COLUMN <type>`
rebuilds the column from **committed** data, discarding uncommitted changes to that column in
the same transaction. Truncating and then narrowing in one transaction silently reverts the
truncation.

Narrowing first and truncating second works, and stays within a single transaction:

```sql
ALTER TABLE t ALTER COLUMN c VARCHAR(255);
UPDATE t SET c = substring(c, 1, 255) WHERE length(c) > 255;
```

That ordering is counter-intuitive enough to be worth documenting even if the ALTER behaviour
itself is judged to be by design.

## Open questions for the maintainers

- Is the silent acceptance intentional (e.g. treating the width as metadata-only)? If so, the
  `msqldump` / restore asymmetry still looks like a defect on its own.
- Should `INSERT ... SELECT` validate values from a source column whose declared width matches
  the target, or is trusting the declared type intended?
- Is there an existing way to ask the server to validate a column against its declared type,
  so this state can be detected in databases that have already hit it?

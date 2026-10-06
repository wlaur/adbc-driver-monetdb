# Databases created through monetdbd failed to start on their second boot

**Status: fixed.** Reproduced on a `56.0.0` build from `master` at `bbc2d72f02`. Six
create-and-restart cycles against a build from `ee305e491c` were clean, with no `FATAL` and no
`SIGSEGV` in `merovingian.log`.

## What happened

Any database created through `monetdbd` — that is, the standard container flow — died on its
next start:

```text
ERR benchmark[79]: !FATAL: SQLException:sql.grant_func:01007!
                   GRANT: User/role 'public' already has this privilege
```

`sql_update_sep2022` (`sql/backends/monet5/sql_upgrades.c:2252`) probed for an EXECUTE
privilege on `sys.tracelog`, found none, and re-issued a grant that `16_tracelog.sql` had
already made during bootstrap. `SQLupgrades` routed the error to `GDKfatal()`. `monetdbd`
retried five times and gave up.

A standalone `mserver5 --dbpath=...` bootstrap and restart was **not** affected, which is
likely why CI missed it.

## Re-verification

[`repro/boot-test.sh`](repro/boot-test.sh) creates a fresh dbfarm, boots it, restarts the
container, and checks `merovingian.log`:

```sh
docs/monetdb-issues/repro/boot-test.sh monetdb-tip:local 6
```

Against `ee305e491c`:

```text
run 1: boot1=ok boot2=ok fatal=0 segv=0 -> OK
...
SUMMARY image=monetdb-tip:ee305e491c iterations=6 ok=6 bad=0
```

## Historical workaround, no longer needed

[`repro/apply-patch.py`](repro/apply-patch.py) made that one grant tolerant of SQLSTATE
`01007`. It is kept because `repro/build-image.sh` can still apply it when building an older
commit, and because it documents the failure precisely.

It was never a complete fix: the failed statement had already aborted the upgrade transaction
and `SQLupgrades` continued against it, so roughly one fresh container in three then
segfaulted at startup instead. The correct fix — which is what upstream appears to have done —
is to make the detection query find the existing privilege.

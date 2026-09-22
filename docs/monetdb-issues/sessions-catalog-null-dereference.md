# `sys.sessions` NULL dereference crashes mserver5 for any non-admin user

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) and on 11.55.7 (Dec2025-SP3).
Deterministic, 100% reproducible with two `mclient` sessions and one `SELECT`.

`sql_sessions_wrap()` dereferences `Client->sqlcontext` without checking it for `NULL`.
Any client record in `RUNCLIENT` mode that does not (yet) have a SQL context makes the
server segfault as soon as a **non-administrator** session reads `sys.sessions`.

`sys.sessions` is granted to public, so any authenticated user can crash the server and
take every other session down with it. Administrators are unaffected: the offending
expression is short-circuited for them, which is why this has gone unnoticed.

Two situations produce a `RUNCLIENT` client with `sqlcontext == NULL`:

1. **Any non-SQL session** (`mclient -l mal`). `sqlcontext` is never set for the lifetime
   of the session, so the crash is deterministic.
2. **Any SQL session that is still connecting.** `MCnewClient()` sets `mode = RUNCLIENT`,
   and `sqlcontext` is only assigned much later, in `SQLprepareClient()`, after the
   authentication handshake. Every incoming connection passes through that window, so on a
   busy server the crash happens on its own.

## Affected versions

Introduced by `ac5f521dc8de` ("Use proper sqlid to decide which sessions rows to show",
2024-07-12), which replaced a `strcmp` on `Client->username` with a dereference through
`Client->sqlcontext`. Present in every release from `Aug2024` (11.51.x) onwards, including
`Dec2025-SP3` (11.55.7), and still present on `master` at `ee305e491c`.

## Reproduction

Deterministic, on the stock image, with nothing but `mclient`:

```bash
#!/usr/bin/env bash
set -euo pipefail
IMAGE=monetdb/monetdb:latest
NAME=mdb-sessions-segv

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --platform linux/amd64 \
    -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=benchmark "$IMAGE" >/dev/null

printf 'waiting for the database'
for _ in $(seq 60); do
    if docker exec "$NAME" monetdb status 2>/dev/null | grep -q benchmark; then break; fi
    printf '.'; sleep 1
done
echo

docker exec -i "$NAME" bash -s <<'IN_CONTAINER'
set -eu
printf 'user=monetdb\npassword=monetdb\n' > /tmp/dot-admin
printf 'user=alice\npassword=alice\n'     > /tmp/dot-alice
admin() { DOTMONETDBFILE=/tmp/dot-admin mclient -d benchmark "$@"; }
alice() { DOTMONETDBFILE=/tmp/dot-alice mclient -d benchmark "$@"; }

# any ordinary user will do -- sys.sessions is granted to public
admin -s "CREATE USER \"alice\" WITH PASSWORD 'alice' NAME 'alice' SCHEMA \"sys\";" >/dev/null

# a session that never acquires a SQL context: mode is RUNCLIENT, sqlcontext stays NULL
rm -f /tmp/hold; mkfifo /tmp/hold
( exec 3>/tmp/hold; sleep 30; exec 3>&- ) &
DOTMONETDBFILE=/tmp/dot-admin mclient -d benchmark -l mal < /tmp/hold >/dev/null 2>&1 &
sleep 4

echo "--- sys.sessions as monetdb (admin: the deref is short-circuited) ---"
admin -f csv -s "SELECT sessionid, username, language FROM sys.sessions;"

echo "--- sys.sessions as alice (non-admin: dereferences the NULL sqlcontext) ---"
alice -f csv -s "SELECT COUNT(*) FROM sys.sessions;" 2>&1 | tail -3 || true

sleep 10
echo "--- merovingian.log ---"
grep -E 'SIGSEGV|has crashed' /var/monetdb5/dbfarm/merovingian.log || echo "NO CRASH"
IN_CONTAINER
```

Output, on `master` at `ee305e491c`:

```
--- sys.sessions as monetdb (admin: the deref is short-circuited) ---
0,monetdb,mal
1,monetdb,sql
--- sys.sessions as alice (non-admin: dereferences the NULL sqlcontext) ---
ACTION= read_into_cache
QUERY = SELECT COUNT(*) FROM sys.sessions;
ERROR = !unexpected end of file
--- merovingian.log ---
2026-09-22 09:25:37 MSG merovingian[1]: database 'chk' (74) was killed by signal SIGSEGV
```

The admin `SELECT` on the line above lists the MAL session (`0,monetdb,mal`) without
incident; the same catalogue read as `alice` kills the server.

### The same crash without a MAL session

The connection-setup window is enough on its own — no `-l mal`, ordinary SQL sessions only.
Four shell loops opening and closing `mclient -s "SELECT 1;"` connections, plus one
non-admin session executing `SELECT COUNT(*) FROM sys.sessions;` in a loop:

| poller identity | `sys.sessions` executions | result |
|---|---:|---|
| `monetdb` (`user_id == USER_MONETDB`) | 200,000 | no crash |
| `alice`, default role | **88** | **SIGSEGV** |
| `alice`, after `SET ROLE sysadmin` | 200,000 | no crash |

Same user, same statement, same churn in rows 2 and 3 — only the value of `admin` differs,
and `admin` is exactly what gates the dereference.

## Backtrace

Debug build (`-O0 -g`, `ASSERT=ON`), crash triggered by the deterministic recipe above:

```
Thread 24 "client0024" received signal SIGSEGV, Segmentation fault.
0x0000ffffb4196454 in sql_sessions_wrap (cntxt=0xe1b5da8, mb=..., stk=..., pci=...)
    at sql/backends/monet5/sql.c:3711
3711            bool allowed_to_see = admin || c == cntxt ||  their_be->mvc->user_id == user_id;
        their_be = 0x0
        username = 0x0
        admin = false
#1  runMALsequence (...) at monetdb5/mal/mal_interpreter.c:701
#2  runMAL (...) at monetdb5/mal/mal_interpreter.c:367
#3  SQLrun (...) at sql/backends/monet5/sql_execute.c:80
#4  SQLengine_ (c=0xe1b5da8) at sql/backends/monet5/sql_scenario.c:1744
#5  SQLengine (c=0xe1b5da8) at sql/backends/monet5/sql_scenario.c:1764
#6  runScenarioBody (c=0xe1b5da8) at monetdb5/mal/mal_scenario.c:282
#7  runScenario (c=0xe1b5da8) at monetdb5/mal/mal_scenario.c:297
#8  MSserveClient (c=0xe1b5da8) at monetdb5/modules/mal/mal_mapi.c:213
#9  MSscheduleClient (...) at monetdb5/modules/mal/mal_mapi.c:428
#10 doChallenge (data=0xffff70027c70) at monetdb5/modules/mal/mal_mapi.c:519
```

## Where it is

`sql/backends/monet5/sql.c`, in `sql_sessions_wrap()` — line 3711 in 11.55.7, line 3756 on
`master` at `ee305e491c`, unchanged:

```c
MT_lock_set(&mal_contextLock);
for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
        if (c->mode != RUNCLIENT)
                continue;

        backend *their_be = c->sqlcontext;                                  /* may be NULL */
        bool allowed_to_see = admin || c == cntxt || their_be->mvc->user_id == user_id;
```

The loop assumes `mode == RUNCLIENT` implies a usable SQL context. It does not:

- `monetdb5/mal/mal_client.c`, `MCnewClient()` sets `c->mode = RUNCLIENT` under
  `mal_contextLock`, and `MCinitClientRecord()` never touches `c->sqlcontext`.
- `sql/backends/monet5/sql_scenario.c`, `SQLprepareClient()` assigns `c->sqlcontext = be`
  only after the credential handshake, with `mal_contextLock` **not** held in between.
- Non-SQL scenarios never call `SQLprepareClient()` at all.

Holding `mal_contextLock` (which the loop does) does not close the window: the lock is
dropped between the two assignments.

## Suggested fix

Guard the dereference, the way the SQL log thread in the same tree already does at
`sql/backends/monet5/sql_scenario.c:136-141`
(`backend *be = c->sqlcontext; if (be) { mvc *sql = be->mvc; if (sql) { ... } }`).

A client with no SQL context has no `user_id` to compare against, so the natural policy is
to treat it as not visible to non-admins and `continue`, i.e. make `allowed_to_see` fall
back to `false` rather than to a dereference. Admins keep seeing such sessions, which the
row `0,monetdb,mal` above already demonstrates works.

Worth deciding at the same time whether `mode == RUNCLIENT` should imply an initialised
scenario at all. If the intent is that a client record only becomes `RUNCLIENT` once it is
fully set up, moving the transition later (or adding an intermediate state) would fix this
class of bug rather than this one instance; every other reader of `mal_clients[]` currently
carries the same assumption.

Unrelated but adjacent: `MCsetClientInfo()` (`monetdb5/mal/mal_client.c:583`) writes
`c->client_hostname` / `client_application` / `client_library` / `client_remark` without
holding `mal_contextLock`, while `sql_sessions_wrap()` reads them under it. It only ever
allocates from the client's own allocator and never frees, so today the reader can only
observe a stale-but-valid pointer, but the write side should probably take the lock.

## Note on an earlier "fixed" verdict

An earlier check against `master` reported this as fixed. That check created a non-admin
user and read `sys.sessions`, but never created a client record without a SQL context, so
it could not fire. A re-verification must include either the `-l mal` session or heavy
connection churn.

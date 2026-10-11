# `sys.sessions` NULL dereference crashes mserver5 for any non-admin user

**Status:** reproduces on Dec2025-SP4 (11.55.9), verified 2026-10-11 on native ARM64.
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

## Reproduction

Deterministic, on the stock image, with nothing but `mclient`:

```bash
#!/usr/bin/env bash
set -euo pipefail
IMAGE=wlaur/monetdb-container:11.55.9-1
NAME=mdb-sessions-segv

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --platform linux/arm64 \
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

Output on Dec2025-SP4:

```
--- sys.sessions as monetdb (admin: the deref is short-circuited) ---
0,monetdb,mal
1,monetdb,sql
--- sys.sessions as alice (non-admin: dereferences the NULL sqlcontext) ---
ACTION= read_into_cache
QUERY = SELECT COUNT(*) FROM sys.sessions;
ERROR = !unexpected end of file
--- merovingian.log ---
2026-10-11 07:02:00 MSG merovingian[1]: database 'chk' (74) was killed by signal SIGSEGV
```

The admin `SELECT` on the line above lists the MAL session (`0,monetdb,mal`) without
incident; the same catalogue read as `alice` kills the server.

## Where it is

[`sql_sessions_wrap()` in the SP4 source](https://github.com/MonetDB/MonetDB/blob/Dec2025_SP4_release/sql/backends/monet5/sql.c):

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

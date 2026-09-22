#!/usr/bin/env bash
# Re-run the reproducible checks in ../ against a given MonetDB image.
# usage: check-issues.sh IMAGE [GDK_DEBUG]
#
# Covers the checks that need nothing but one server and mclient. The remote-table,
# JSON-cast, Parquet, binary-export and concurrency checks need their own setup and
# are described in their own files.
set -uo pipefail

image="${1:?usage: check-issues.sh IMAGE [GDK_DEBUG]}"
gdk_debug="${2:-}"
name=mdb-issue-check
platform="${PLATFORM:-linux/arm64}"

env_args=()
if [[ -n "$gdk_debug" ]]; then
    env_args+=(-e "MSERVER5_EXTRA_ARGS=-d$gdk_debug")
fi

docker rm -f "$name" >/dev/null 2>&1
docker run -d --name "$name" --platform "$platform" \
    -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=chk \
    "${env_args[@]}" "$image" >/dev/null

q() {
    local user="${2:-monetdb}" pass="${3:-monetdb}"
    docker exec -i "$name" sh -c \
        "umask 077; printf 'user=%s\npassword=%s\n' '$user' '$pass' > /tmp/.m-$user;
         DOTMONETDBFILE=/tmp/.m-$user exec mclient -h127.0.0.1 -p50000 -dchk -fcsv" <<<"$1" 2>&1
}

n=0
until q "SELECT 1;" >/dev/null 2>&1 || [ $n -ge 60 ]; do n=$((n + 1)); sleep 2; done

echo "### image=$image gdk_debug=${gdk_debug:-unset}"
q "SELECT value FROM sys.env() WHERE name = 'monet_version';"

echo
echo "### window-avg-frame-wider-than-16 (i=17 must be 9.0; the window must not be filtered)"
q "CREATE TABLE w (i INTEGER, v DOUBLE);"
q "INSERT INTO w SELECT value, CAST(value AS DOUBLE) FROM sys.generate_series(1, 42);"
q "SELECT i, ra FROM (
     SELECT i, avg(v) OVER (ORDER BY i ROWS BETWEEN 16 PRECEDING AND CURRENT ROW) AS ra
     FROM w
   ) t WHERE i BETWEEN 15 AND 18 ORDER BY i;"

echo
echo "### rollup-grouping-order-by-empty-result (must print a count of 6)"
q "CREATE TABLE g (a VARCHAR(10), b VARCHAR(10), v INTEGER);"
q "INSERT INTO g VALUES ('x','p',1), ('x','q',2), ('y','r',3);"
q "SELECT count(*) AS rows_returned FROM (
     SELECT sum(v) AS s, a, b, grouping(a)+grouping(b) AS lh,
            rank() OVER (PARTITION BY grouping(a)+grouping(b),
                                      CASE WHEN grouping(b)=0 THEN a END
                         ORDER BY sum(v) DESC) AS rk
     FROM g GROUP BY rollup(a,b)
     ORDER BY lh DESC, CASE WHEN grouping(a)+grouping(b)=0 THEN a END, rk) t;"

echo
echo "### alter-column-narrowing-keeps-overlong-values (length must not exceed 255)"
q "CREATE TABLE t4 (c VARCHAR(1024));"
q "INSERT INTO t4 VALUES (repeat('x', 900));"
q "ALTER TABLE t4 ALTER COLUMN c VARCHAR(255);"
q "SELECT length(c) AS len_after_narrowing FROM t4;"

echo
echo "### savepoint-resurrects-deleted-row (SELECT must return no rows)"
q "CREATE TABLE sp_probe(id INTEGER);"
q "INSERT INTO sp_probe VALUES (1);"
q "START TRANSACTION;
   DELETE FROM sp_probe WHERE id = 1;
   SAVEPOINT sp;
   SELECT id FROM sp_probe;
   ROLLBACK;"

echo
echo "### prepared-statement-loses-temporary-table (must print 1)"
q "START TRANSACTION;
   PREPARE SELECT ? + 1;
   CREATE LOCAL TEMPORARY TABLE tmp_after_prepare (x INTEGER) ON COMMIT PRESERVE ROWS;
   INSERT INTO tmp_after_prepare VALUES (1);
   SELECT count(*) AS rows_in_tmp FROM tmp_after_prepare;
   COMMIT;"

echo
echo "### sessions-catalog-null-dereference (server must survive)"
echo "--- a RUNCLIENT with no SQL context is required, so hold a MAL session open ---"
q "CREATE USER \"probe\" WITH PASSWORD 'probe' NAME 'probe' SCHEMA \"sys\";"
docker exec -d "$name" sh -c \
    'umask 077; printf "user=monetdb\npassword=monetdb\n" > /tmp/.mal;
     rm -f /tmp/hold; mkfifo /tmp/hold;
     ( exec 3>/tmp/hold; sleep 60; exec 3>&- ) &
     DOTMONETDBFILE=/tmp/.mal mclient -h127.0.0.1 -p50000 -dchk -l mal < /tmp/hold >/dev/null 2>&1'
sleep 5
echo "--- as admin (the deref is short-circuited) ---"
q "SELECT sessionid, username, language FROM sys.sessions;"
echo "--- as probe (non-admin) ---"
q "SELECT COUNT(*) FROM sys.sessions;" probe probe
sleep 3
echo "--- server still alive? ---"
q "SELECT 'alive' AS state;"
echo "--- merovingian.log ---"
docker exec "$name" sh -c \
    "grep -E 'SIGSEGV|has crashed' /var/monetdb5/dbfarm/merovingian.log" || echo "NO CRASH"

docker rm -f "$name" >/dev/null 2>&1

#!/usr/bin/env bash
# Fresh-database second-boot test for the master image.
# usage: boot-test.sh IMAGE [ITERATIONS]
set -uo pipefail

image="${1:?usage: boot-test.sh IMAGE [ITERATIONS]}"
iterations="${2:-10}"
base="${TMPDIR:-/tmp}/monetdb-boot-test-$$"
docker rm -f $(docker ps -aq --filter "name=mdb-boot-test") >/dev/null 2>&1
rm -rf "$base"; mkdir -p "$base"

connect() {
    docker exec "$1" sh -c \
        'printf "user=monetdb\npassword=monetdb\n" > /tmp/.m; DOTMONETDBFILE=/tmp/.m mclient -h127.0.0.1 -p50000 -dbenchmark -fraw -s "select 1"' \
        >/dev/null 2>&1
}

wait_up() {
    for _ in $(seq 1 40); do
        connect "$1" && return 0
        sleep 2
    done
    return 1
}

ok=0; bad=0
for i in $(seq 1 "$iterations"); do
    farm="$base/farm-$i"
    mkdir -p "$farm"
    name="mdb-boot-test-$i"
    docker rm -f "$name" >/dev/null 2>&1

    docker run -d --name "$name" \
        -v "$farm:/var/monetdb5/dbfarm" \
        -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=benchmark \
        "$image" >/dev/null 2>&1

    first=FAIL; wait_up "$name" && first=ok
    docker restart "$name" >/dev/null 2>&1
    second=FAIL; wait_up "$name" && second=ok

    log="$farm/merovingian.log"
    nfatal=$(grep -c "FATAL" "$log" 2>/dev/null); nfatal=${nfatal:-0}
    nsegv=$(grep -c "SIGSEGV" "$log" 2>/dev/null); nsegv=${nsegv:-0}

    if [[ "$first" == ok && "$second" == ok && "$nfatal" -eq 0 && "$nsegv" -eq 0 ]]; then
        verdict=OK; ok=$((ok+1))
    else
        verdict=BAD; bad=$((bad+1))
    fi
    echo "run $i: boot1=$first boot2=$second fatal=$nfatal segv=$nsegv -> $verdict"
    if [[ "$verdict" == BAD ]]; then
        grep -E "FATAL|SIGSEGV|Exception|error" "$log" 2>/dev/null | tail -6 | sed 's/^/    /'
    fi
    docker rm -f "$name" >/dev/null 2>&1
done

echo "SUMMARY image=$image iterations=$iterations ok=$ok bad=$bad"

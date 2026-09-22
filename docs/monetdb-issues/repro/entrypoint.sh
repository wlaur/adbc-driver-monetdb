#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla
# Public License, v. 2.0. If a copy of the MPL was not
# distributed with this file, You can obtain one at
# https://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2024 MonetDB B.V.
# Modifications Copyright 2026 William Laurén

set -e -o pipefail -o nounset
set +x

farm_dir="${MDB_FARM_DIR-/var/monetdb5/dbfarm}"

if [[ -z "$farm_dir" ]]; then
    farm_dir="/var/monetdb5/dbfarm"
    echo "MDB_FARM_DIR set to an empty string. Using the default value of '$farm_dir'."
fi

configure() {
    while read -r varname; do
        case "$varname" in
            MDB_SHOW_VARS) ;;
            MDB_CREATE_DBS) ;;
            MDB_DB_ADMIN_PASS) ;;
            MDB_DB_ADMIN_PASS_FILE) ;;
            MDB_DAEMON_PASS) ;;
            MDB_DAEMON_PASS_FILE) ;;
            MDB_LOGFILE) ;;
            MDB_SNAPSHOT_DIR) ;;
            MDB_FARM_DIR) ;;
            MDB_SNAPSHOT_COMPRESSION) ;;
            MDB_FARM_PROPERTIES) ;;
            MDB_DB_PROPERTIES) ;;
            *) echo "Warning: unused MDB_ variable $varname" ;;
        esac
    done < <(env | sed -n -e 's/=.*//' -e '/^MDB_/p')

    logfile="${MDB_LOGFILE:-merovingian.log}"
    snapshotdir="${MDB_SNAPSHOT_DIR:-}"
    snapshotcompression="${MDB_SNAPSHOT_COMPRESSION:-}"
    show_vars="${MDB_SHOW_VARS:-}"

    if [[ -n "${MDB_DAEMON_PASS_FILE:-}" ]]; then
        passphrase="$(head -1 "$MDB_DAEMON_PASS_FILE")"
    else
        passphrase="${MDB_DAEMON_PASS:-}"
    fi
    if [[ -n "${MDB_DB_ADMIN_PASS_FILE:-}" ]]; then
        admin_pass="$(head -1 "$MDB_DB_ADMIN_PASS_FILE")"
    else
        admin_pass="${MDB_DB_ADMIN_PASS:-}"
    fi

    tmp_dbs="${MDB_CREATE_DBS-monetdb}"
    if [[ -n "$tmp_dbs" && -z "$admin_pass" ]]; then
        echo "Please use MDB_DB_ADMIN_PASS or MDB_DB_ADMIN_PASS_FILE to set a database admin"
        echo "password for '$tmp_dbs'. Alternatively, set MDB_CREATE_DBS to ''."
        exit 1
    fi

    IFS=',' read -ra create_dbs <<< "$tmp_dbs"
    IFS=',' read -ra farm_props <<< "${MDB_FARM_PROPERTIES:-}"
    IFS=',' read -ra db_props <<< "${MDB_DB_PROPERTIES:-}"
}

create_dbfarm() {
    if [[ -f "$farm_dir/.merovingian_properties" ]]; then
        return
    fi
    echo "Creating db farm at $farm_dir"
    mkdir -p "$farm_dir"
    monetdbd create "$farm_dir"
}

set_properties() {
    monetdbd set "logfile=$logfile" "$farm_dir"
    monetdbd set "snapshotdir=$snapshotdir" "$farm_dir"
    monetdbd set "snapshotcompression=$snapshotcompression" "$farm_dir"
    for prop in "${farm_props[@]}"; do
        case "$prop" in
            listenaddr=* | control=* | passphrase=*) ;;
            *) monetdbd set "$prop" "$farm_dir" ;;
        esac
    done
}

create_databases() {
    monetdbd start "$farm_dir"
    for db in "${create_dbs[@]}"; do
        echo "Creating database '$db'"
        monetdb create -p "$admin_pass" "$db"
        for prop in "${db_props[@]}"; do
            monetdb set "$prop" "$db"
        done
    done
    monetdbd stop "$farm_dir"
}

enable_control() {
    monetdbd set "passphrase=$passphrase" "$farm_dir"
    monetdbd set control=true "$farm_dir"
}

configure

if [[ ! -f "$farm_dir/.container_initialized" ]]; then
    if [[ ! -d "$farm_dir" ]] || [[ ! -f "$farm_dir/.merovingian_properties" ]]; then
        create_dbfarm
    fi

    set_properties

    if [[ -n "$admin_pass" ]]; then
        create_databases
    fi

    if [[ -n "$passphrase" ]]; then
        enable_control
    fi

    monetdbd set listenaddr=all "$farm_dir"
    touch "$farm_dir/.container_initialized"
fi

if [[ -n "$show_vars" ]]; then
    monetdbd get all "$farm_dir"
fi

echo "Starting MonetDB daemon"
exec monetdbd start -n "$farm_dir"

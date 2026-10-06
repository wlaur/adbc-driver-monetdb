#!/usr/bin/env python3
"""Make the sep2022 sys.tracelog grant tolerant of an already-present privilege.

On MonetDB master (bbc2d72) a database bootstrapped through monetdbd fails to
start on its next boot: the upgrade block's detection query misses the EXECUTE
privilege that 16_tracelog.sql already granted, re-issues the grant, and the
resulting SQLSTATE 01007 is fatal. Only startup-time upgrade code is touched.
"""

import sys
from pathlib import Path

PATH = "sql/backends/monet5/sql_upgrades.c"
ANCHOR = '"grant select on sys.tracelog to public;\\n");'
CALL = 'err = SQLstatementIntern(c, buf, "update", true, false, NULL);'
TOLERATE = """err = SQLstatementIntern(c, buf, "update", true, false, NULL);
\t\tif (err != MAL_SUCCEED && strstr(err, "already has this privilege") != NULL) {
\t\t\terr = MAL_SUCCEED;
\t\t\tsql->session->status = 0;
\t\t\tsql->errstr[0] = '\\0';
\t\t}"""

source = Path(PATH).read_text(encoding="utf-8")

anchor_at = source.find(ANCHOR)
if anchor_at < 0:
    sys.exit(f"anchor not found in {PATH}; upstream code changed")

call_at = source.find(CALL, anchor_at)
if call_at < 0:
    sys.exit(f"grant statement call not found after anchor in {PATH}")

patched = source[:call_at] + TOLERATE + source[call_at + len(CALL) :]
Path(PATH).write_text(patched, encoding="utf-8")
print(f"patched {PATH} at offset {call_at}")

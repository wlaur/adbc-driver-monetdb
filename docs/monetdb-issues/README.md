# MonetDB server issues

Current server defects and unresolved investigations for MonetDB Dec2025-SP4
(11.55.9), verified 2026-10-11 on native ARM64 with
`wlaur/monetdb-container:11.55.9-1` (digest
`sha256:b7238498164cd26a0b7bd9391d51948dca5c69f490f888186d3204320abaecf7`).
These are server reports, not driver defects.

| Issue | SP4 result |
| --- | --- |
| [Sessions catalog NULL dereference](sessions-catalog-null-dereference.md) | Non-admin catalog read with an open MAL session causes SIGSEGV |
| [Window average](window-avg-frame-wider-than-16.md) | Row 17 returns 12.75 instead of 9.0 |
| [Rollup ordering](rollup-grouping-order-by-empty-result.md) | No result instead of six rows |
| [VARCHAR narrowing](alter-column-narrowing-keeps-overlong-values.md) | 900 characters retained in VARCHAR(255) |
| [Savepoint visibility](savepoint-resurrects-deleted-row.md) | Deleted committed row becomes visible |
| [Remote prepared parameter](remote-table-prepared-parameter.md) | EXECUTE fails; literal query succeeds |
| [JSON cast memory exhaustion](json-cast-memory-exhaustion.md) | 289,086-byte document exhausts 4 GiB |
| [Binary export disconnect](binary-export-abort-on-client-disconnect.md) | TCP reset during export causes invalid free and SIGABRT |
| [Concurrent JSON workload](concurrent-json-workload-server-exit.md) | Memory exhaustion observed; independent crash mechanism remains unconfirmed |

Each confirmed report includes a reproduction using `mclient` or a plain MAPI
connection. [`repro/`](repro/README.md) contains the shared SQL checks, synthetic
fixtures, and source-build tooling for testing proposed server fixes. Run crash
and memory-exhaustion probes only against their disposable databases.

Resolved reports and reports for features outside SP4 are removed. Unconfirmed
investigations state the evidence and limits of their current reproduction.

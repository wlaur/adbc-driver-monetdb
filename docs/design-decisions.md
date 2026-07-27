# Design decisions

This file records durable choices that are easy to mistake for unfinished work. It is not a
backlog or a release checklist. User-visible behavior belongs in the README. Revisit these
decisions only when new protocol evidence, reproducible workload measurements, or user
requirements change their premises.

## Component and release boundaries

- Generic MAPI transport, framing, authentication, timeout, and client-information behavior
  belongs in the `monetdb-rust` fork under MPL-2.0. Arrow conversion and ADBC scheduling belong in
  this repository under MIT. Performance work must not move Arrow or ADBC policy into the fork.
- `adbc_driver_monetdbs` remains a re-export of `adbc_driver_monetdb` solely because Polars derives
  the Python module name `adbc_driver_<scheme>` from a URI. It keeps the standard
  `monetdbs://` TLS URI usable through Polars without creating a second driver identity. ADBC
  standardizes `uri` as an option and driver loading separately; there is one native driver, one
  wheel distribution, and one DBC manifest named `monetdb`.
- MonetDB exposes one sequential result channel per MAPI connection. Partitioned and incremental
  results remain explicitly unsupported. Multi-connection range-partitioned reads would be a new
  feature with transaction-consistency semantics, not an implementation detail of the current
  reader.
- Results that fit the negotiated inline reply prefix are decoded from that initial MAPI response.
  Server-resident results use `Xexportbin`; the driver does not issue a second text query as a
  fallback when a binary representation is unsupported.
- Optional XDBC fields are not advertised without backend semantics to populate them.
- Statement destruction remains nonblocking. Explicit cancellation interrupts network work and
  closes the session. Closing a prefetched reader drops its receiver, waits for a bounded grace
  period, and then detaches a still-running fetch so abandoning a result does not implicitly
  destroy the session.
- The driver repository does not maintain a separate changelog. Pull requests, Git history, and
  generated GitHub release notes are the change record.
- DBC libraries are built directly for each target rather than extracted from Python wheels.
- Release jobs use the current stable Rust toolchain and rerun `cargo audit`; tag-time toolchain
  pins would hide rather than resolve a newly published advisory.
- The upstream-shaped fork retains the upstream `homepage` and `repository` package metadata.
  Revisit that only if the fork is published as an independently branded crate.

## Client identity

- `monetdb-rust` exposes `client_prefix` as a normal connection parameter so the generic protocol
  library can support branded clients. The ADBC driver does not accept `client_prefix` in its URI;
  it always supplies `adbc_driver_monetdb <version>` itself so callers cannot impersonate another
  driver.
- Applications may customize `client_application` and `client_remark`, or disable all client
  information with `client_info=false`.
- Client-information values have no driver-specific length cap. Newlines are rejected because
  `Xclientinfo` uses newline-delimited key/value records.
- The Python shim supplies the basename of `sys.argv[0]` only when neither an ADBC option nor the
  URI supplies an application name. The precedence is explicit option, URI value, Python default,
  then the protocol library default.

## Result scheduling and memory

- The read default remains 131,072 rows. Write scheduling is byte-based: a 512 MiB encoded budget,
  with a 4,096-row minimum and no independent maximum-row cap, reduced under constrained Linux
  cgroups. The first 4,096 rows establish the wire-byte estimate; later windows use a 70/30
  actual/prior update. Full-suite calibration matters here: on Dec2025, repeatedly extending a
  multi-million-row table can dominate ingest time even when each COPY is fast in smaller
  microbenchmarks. The byte budget keeps memory bounded while allowing narrow inputs to remain one
  COPY. Boundaries stay independent of producer batching and an exact-row diagnostic override
  remains available.
- The protocol fork exposes a generic streaming upload sink. The driver serves each requested
  column in bounded 16 MiB pieces, so an encoded window is never materialized as one `Vec<u8>`.
  Null-free signed integer and float layouts borrow validated Arrow buffers directly; other types
  use one bounded scratch piece while the protocol layer owns one framed piece. A 2M-row
  single-column replay improved from 52.3 to 23.8 ms for BIGINT, 127.6 to 53.0 ms for VARCHAR, and
  105.7 to 42.5 ms for TIMESTAMP on the documented local Dec2025 harness because adaptive
  coalescing also removed repeated COPY round trips.
- Appends to an existing table inside a caller-managed transaction execute directly for every
  COPY window. Operation savepoints caused MonetDB to retain and materialize disproportionate
  storage for large, wide streams even after the savepoint was released. A server error aborts the
  caller transaction until rollback. A client-side error after a completed window marks the
  connection rollback-only: reads remain available, but commit is blocked until rollback. This
  preserves caller ownership without silently discarding earlier work. Explicit savepoint and
  partial-commit modes remain advanced opt-ins. Autocommit still wraps the complete stream in an
  internal transaction; create and replace modes retain their operation savepoint because their
  DDL must be recovered without discarding unrelated caller work.
- The prefetch worker fetches complete raw `Xexportbin` frames while the caller decodes the
  previous frame. A worker that both fetches and decodes would serialize those phases and lose the
  overlap.
- Prefetch can hold about three windows: one decoding, one buffered, and one in flight. Early
  abandonment can therefore waste up to two fetched windows. A detached final fetch temporarily
  retains the connection's protocol operation lock; the next statement waits for that bounded
  server response or the configured read/operation timeout.
- MonetDB currently emits plain NUL-terminated strings for `Xexportbin`, not string backreferences.
  The decoder keeps its tested backreference path for forward compatibility, but the current
  two-phase string decoder is optimized for literal output.
- Write-side string deduplication remains enabled. Removing it reduced encoding work but increased
  repeated-data wire volume by 2–5× and also removed server-side savings.
- The adaptive dedup map samples 4,096 rows. Unconditional pre-sizing raised memory use, while a
  2,048-row sample was unstable.

## Semantics and test strategy

- The canonical local Dec2025-SP3 test service is the pinned native ARM64
  `wlaur/monetdb-container` image built from its public recipe. CI retains the official amd64
  images only for the x86_64 wheel job and the 11.55.1 minimum-server gate: an ARM64-only image
  cannot run those architecture/version checks, and no 11.55.1 tag exists in the ARM64 image
  repository.
- MonetDB `JSON` maps to the canonical Arrow `arrow.json` extension backed by UTF-8, never to an
  Arrow `Struct`. MonetDB defines JSON as a validated string subtype, and a column may contain
  objects, arrays, scalars, and JSON `null` with different shapes in different rows. Functions
  declared to return JSON preserve `arrow.json`; functions declared to return strings, numbers,
  integers, or booleans use those scalar Arrow types. Polars may explicitly decode the resulting
  `String` storage into a known struct schema when an application wants that narrower view.
- `TIMESTAMPTZ` decoding under `SET TIME ZONE` is correct and is pinned by a live regression test.
- TLS configuration errors are argument-shaped, but handshake and certificate-verification
  failures are I/O failures and map to `OperationalError`.
- Certificate hashes shorter than 16 hexadecimal digits are rejected deliberately.
- TLS integration tests generate certificates and proxies in-process. A persistent Compose TLS
  endpoint would add maintenance without increasing coverage.
- `pymonetdb` is a differential reference, not the oracle. Tests first assert the ADBC schema and
  values against independent expectations, then compare the common subset after documented
  normalization. Known pymonetdb differences remain explicit nonblocking xfails so upstream fixes
  become visible.

## Prepared-statement lifetime

- Positional prepared statements are cached per connection under SQL normalized by trimming
  surrounding whitespace and trailing semicolons. A 128-entry least-recently-used cache bounds
  session memory; lookups update a monotonic use counter in constant time, while eviction scans
  only when inserting a new entry. Shared entry leases prevent eviction from deallocating a plan
  still used by a live statement. Statement destruction remains nonblocking; the cache or session
  owns server cleanup.
- Commit and rollback preserve MonetDB prepared statements. Schema-changing SQL observed on the
  same connection clears the cache before execution. MonetDB can also silently discard a plan
  after DDL from another session; an `EXEC: PREPARED Statement missing` response evicts that exact
  entry and retries once when doing so cannot roll back user work.
- MonetDB aborts an explicit transaction when `EXECUTE` reports a missing prepared statement, so
  the driver retries only in autocommit or inside its own multi-row savepoint. A one-row bound DML
  statement executes directly to avoid three transaction-control round trips; a rare stale-plan
  error in an explicit transaction follows the normal database-error contract and requires the
  caller to roll back.
- `PREPARE` can narrow declared decimal widths from column statistics. The driver restores
  declared catalog types when MonetDB supplies an unambiguous table/column origin. Current server
  metadata omits the origin schema, so identical table and column names in multiple schemas are
  deliberately left at the `PREPARE` type instead of guessing. Exact restoration in that case
  would require server metadata or a SQL parser, and the catalog lookup is the necessary extra
  round trip for accurate unambiguous declarations.
- On the documented 200-query workload, new-cursor execution fell from 1.944 to 0.342 ms per
  statement. Cache lookup, LRU maintenance, and invalidation bookkeeping added about 0.010 ms over
  the cache-only path and kept the result below the 0.50 ms acceptance threshold.
- The review's remaining repeated-DML regression was three wire round trips caused by wrapping
  every parameter batch in a savepoint. One-row DML now executes directly, because there is no
  partial batch to recover; multi-row batches retain the atomic scope. The other structural
  regression was a one-row login reply window, which forced every result of at least two rows into
  an extra fetch. The driver now keeps MonetDB's normal 100-row inline window.
- Multi-row ExecuteUpdate keeps its atomic transaction/savepoint but sends rendered `EXECUTE`
  statements in bounded batches of at most 1,024 rows or 8 MiB. The first row remains an
  individual stale-plan probe so a missing prepared statement can be retried without buffering
  an arbitrary Arrow stream. On the local 4,096-row benchmark, batching reduced median time from
  2,480.9 ms for repeated ExecuteQuery calls to 224.2 ms for ExecuteUpdate (11.1×); the public
  `test_local_executemany_batching` benchmark reproduces the workload.
- A short row-oriented SELECT may still trail pymonetdb after those protocol round trips are
  removed. pymonetdb constructs Python tuples directly; ADBC constructs a canonical Arrow stream
  across the native boundary and the driver manager then converts it to tuples. That fixed
  schema/buffer/FFI cost cannot be removed without making the DB-API path semantically different
  from the Arrow-native result. `tests/test_local_benchmark.py` keeps the comparison reproducible,
  while performance claims for columnar consumers continue to use Arrow-native fetches.
- The local point-query benchmark now records the protocol floor and parameterized lookup
  separately. A five-round, 500-call run measured 110.0 µs versus 107.4 µs for `SELECT 1` and
  253.2 µs versus 242.6 µs for the parameterized indexed lookup (ADBC versus pymonetdb, a 1.04
  ratio). This is within the review's 1.05 threshold for recoverable fixed overhead.
- Wide DECIMAL and HUGEINT result buffers use Arrow's byte-identical little-endian `i128` layout
  directly after a precision/sentinel validation scan. Unaligned or big-endian buffers retain the
  checked copying path.

## Measured optimization rejections

These conclusions record comparative measurements completed on 2026-07-23. A proposal to reverse
one must provide its commands, workload, environment, and repeated results so the new evidence can
be reviewed and reproduced.

| Proposal | Decision |
|---|---|
| `simdutf8` for real 9–21-byte strings | Rejected; validation was 4–8% slower because call overhead dominated. |
| Unified single-pass string decoding | Rejected; it did not improve literal `Xexportbin` output, while the two-phase path preserves backreference support. |
| UUID or BLOB decoder rewrites | Rejected; prototypes were at parity or slower. |
| Nightly `std::simd`, NEON intrinsics, or new unsafe copy paths | Rejected; safe Rust already reaches memcpy or auto-vectorized sentinel-scan speeds. |
| Intra-column Rayon chunking | Rejected; the small isolated gain disappears when prefetch overlaps decode with fetch. |
| Removing string deduplication | Rejected because the wire-size and server-CPU regressions outweigh cheaper client encoding. |
| Unconditional dedup-map pre-sizing or a 2,048-row sample | Rejected in favor of the adaptive 4,096-row sample. |
| Disabling dedup adaptively | Rejected; the best case was about 2%, with a 2–5× downside on a wrong guess. |
| Temporal, decimal, or primitive encode rewrites | Rejected; they were at parity or slower. The chrono removal helped decode because decode had validation and intermediate-allocation costs that encode does not have. |
| Cross-window string backreferences | Deferred; keeping windows independent bounds state, while adaptive coalescing already avoids upstream-batch resets. |
| Asynchronous COPY double-buffering | Rejected for the sequential ON CLIENT protocol; synchronous bounded encode/send already holds at most one scratch and one framed chunk. |
| A worker that fetches and decodes | Rejected because it cannot overlap the two phases. |
| Performance-only protocol-fork primitives | Rejected unless a genuinely generic MAPI operation is missing. |

The dominant remaining read costs are server execution, serialization, and transfer; writes are
even more server-bound. Unix-domain sockets are already available through `sock=` for colocated
workloads. Server-emitted string backreferences need no new client work if MonetDB begins using
them, because the decoder path already exists.

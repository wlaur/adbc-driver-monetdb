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
- Client transport timeout, cancellation, I/O, framing, and closed-session errors carry the ADBC
  detail `adbc.monetdb.connection_terminal=true`. Server SQLSTATE timeout and cancellation errors
  do not. This gives pools a structured invalidation contract without treating every timeout as
  fatal or matching human-readable diagnostics.
- Named Arrow binding is exposed as `adbc.monetdb.bind_by_name`. The driver also accepts
  `adbc.statement.bind_by_name` because the Python ADBC driver manager emits that spelling
  internally for DB-API dictionaries; it is an interoperability input rather than the driver's
  documented public option.
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

- Automatic reads start at 64 MiB below a measured 5 ms round trip and 128 MiB at or above it,
  capped at one eighth of the lower host/cgroup memory limit. When a fixed-width schema's exact
  estimate shows that one 131,072-row export granule fits within that memory guard and a 512 MiB
  ceiling, the automatic budget expands to that granule. Adaptive window endpoints use complete
  export granules when possible and power-of-two sub-granules otherwise. Variable-width schemas
  start with a conservative allowance, update it from returned frames, and grow at most fourfold
  per window. `read_batch_rows` is a diagnostic row override; non-zero values are normalized to
  the nearest export granule or sub-granule and the effective value remains exact. Zero selects
  byte scheduling. Copy-decoded frames reuse a small buffer pool,
  while adopted fixed-width frames shed capacity slack above one eighth before Arrow owns them.
  `read_stats` exposes the budget, estimates, observed windows, adoption, prefetch, and recycling.
  On a public ClickBench Parquet subset, the old 131,072-row scan took 19.569 seconds and 4,323.7
  MiB peak client RSS. A 32 MiB target took 19.901 seconds and 428.3 MiB; the selected 64 MiB target
  with the final variable-width guard took a 19.744-second median and at most 741.4 MiB across three
  measured repeats. On a TPC-H Parquet subset, 32 MiB changed a lineitem scan from 1.564
  seconds/917.0 MiB to 1.542 seconds/320.8 MiB. These are scheduler calibrations, not authoritative
  workload scores.
- Server-aware follow-up measurements retained the granule scheduling for repeatable client and
  latency results, not as a server-memory mitigation. On a self-contained 218,880 by 787
  fixed-width probe, reducing eleven round trips to two improved warm-cache latency by 23–28% and
  reduced peak client RSS by about 5.5 MiB. On one-million-row synthetic ClickBench and TPC-H
  schemas, the trade was respectively 28.4% lower client RSS for 4.2% higher latency, and 7.8%
  lower latency for 0.6% higher client RSS; peak server memory and disk were effectively flat.
  Fresh-container controls also showed that an apparent roughly 695 MiB aligned-versus-unaligned
  cgroup-memory difference was named-volume page-cache attribution: the MonetDB process mapped the
  same pages under both schedules, and identical schedules changed cgroup charge across fresh
  containers. Consequently server cgroup memory must be measured and reported, but must not be
  used as evidence for an alignment-specific memory win without controlling page ownership.
- Write scheduling separates a 512 MiB logical encoded-byte target from physical retained storage.
  Below a measured 5 ms round trip, ordinary schemas have
  a 128 MiB physical limit and incompressible windows close at 64 MiB. Schemas with at least 512
  columns use 48 MiB for both local limits; above 5 ms, both physical limits rise to the logical
  target. Logical targets are capped at one eighth of the lower of host physical memory and a
  finite cgroup limit. Every producer
  batch is measured from its Arrow buffers before it
  enters the pending queue. Windows consume only batches and zero-copy slices whose measured wire
  size fits the remaining budget; a binary search finds the largest fitting slice of an oversized
  batch. A single encoded row larger than the budget is the only permitted overrun.
  Thirty-two equal-level pending batches are compacted at a time, bounding metadata while copying
  each row at most logarithmically even for streams of one-row batches. Full-suite calibration
  matters here: on Dec2025, repeatedly extending a multi-million-row table can dominate ingest
  time because MonetDB maintains constraint indexes for each append. COPY-sized appends to an
  existing constrained target therefore stream into a session-local unconstrained table and use
  one final target `INSERT … SELECT`. This keeps ordinary client bounds, avoids recognizing
  workload-specific schemas, and validates the growing target index once. Unconstrained targets
  and tiny prepared INSERTs remain direct. An explicit `direct` option exists for diagnostic and
  measured server-specific exceptions. Boundaries stay independent of producer batching and an
  exact-row diagnostic override remains available.
- The earlier unconditional 512 MiB physical default was rejected after the 764,331 × 786
  time-series chain showed 1,585 MiB driver-only RSS and 3,271 MiB end-to-end client RSS. A
  128 MiB retained-storage run used 505 MiB and added 1.5–2.2 seconds per 2.2 GiB locally. The
  final 48 MiB wide-schema limit brought the paired time-series RSS below staged +20% while
  leaving the ordinary 128 MiB path intact for TPC-DS and ClickBench. Keeping the 512 MiB logical
  target preserves coalescing for compressible data. The RTT branch raises physical limits only
  where repeated network exchanges dominate.
- Logical ingest windows are decoupled from physical staging memory. Each encoded column starts by
  applying LZ4 compression in bounded 1 MiB pieces. Shuffle and compression reuse one workspace
  on the prefetch thread, while finished pieces are packed into 16 MiB anonymous arena slabs owned
  by the window. Mapping every small piece was rejected after a 1,000-column compressible run
  retained roughly 500 MiB of page padding for 12 MiB of payload. Keeping separate heap
  workspaces or finished allocations per piece was also rejected after paced ClickBench runs
  accumulated allocator arenas. The reusable workspace plus window arena bounds both scratch and
  allocation count. Client-only retention uses direct blocks; wire-eligible compression uses frames.
  Compression remains enabled only when it saves at least 12.5%; this conservative threshold
  covers both the temporary compression workspace and the CPU cost. A compressible window can
  release its Arrow batches and continue toward the logical budget while retaining only
  compressed chunks. Upload reuses one mapping to decode at most 1 MiB immediately before the
  ordinary binary protocol consumes it. The compressed representation is never a disk format.
- A finite Linux cgroup limit is enforced before starting an ingest prefetch worker. The
  process-wide reservation covers two physical windows, two 16 MiB staging groups, and 64 MiB of
  producer headroom, while preserving 10% of the cgroup for unrelated work. This deliberately
  conservative model prevents simultaneous connections in one client process from racing the
  cgroup OOM handler. Rejected work receives SQLSTATE `HY001`. The reservation is owned by the
  prefetch worker and is released when that worker exits. A worker detached after an operation
  timeout retains its reservation until it actually stops, because releasing admission capacity
  while the worker can still allocate would make the limit unsound.
- The bounded Parquet producer reclaims unused PyArrow allocator pages after each 2 GiB of decoded
  row-group data and at stream close. Reclaiming every row group was rejected because many small
  TPC-DS files regressed sharply; waiting until the end allowed a paced 36.4 GiB ClickBench decode
  to retain more than 20 GiB. The byte interval avoids both failure modes and can be disabled per
  stream. Path inputs are opened through an owned PyArrow `OSFile`, avoiding whole-file mapping.
  Column-parallel decoding remains opt-in because it improved isolated scans but slightly regressed
  full ingest while increasing client memory. Nullable integer epoch-to-timestamp/date transforms
  run batch by batch in the same bounded stream.
- Input staging, including the first per-column probe, is fixed at 16 MiB, preventing a full
  logical window of Arrow batches from coexisting with its encoded copy. If the first staging
  group is not materially compressible, further compression attempts stop and automatic
  scheduling closes the window at 64 MiB. Null-free batches can remain the canonical Arrow
  representation; batches containing nulls stay as raw encoded chunks so null-sentinel conversion
  is not deferred onto the upload path. This bounds duplicated probe data for random floats,
  pre-compressed binary values, and other high-entropy inputs while preserving large COPY windows
  for repetitive strings, null-heavy columns, and regular numeric data. The decision is based
  only on observed bytes and null presence, not table or schema identity. Explicit row scheduling
  bypasses the cap because it is an exact diagnostic override.
- Very wide, incompressible input exposes a fundamental three-way tradeoff. Smaller COPY windows
  bound client RSS and avoid staging files, but MonetDB may temporarily retain more append segments
  during consolidation or restart. A single large window would move the encoded table into client
  RAM, while file-backed chunks would recreate the disk-staging implementation. The automatic
  64 MiB branch chooses bounded client memory and no temporary disk. This can lose server peak
  memory to disk staging for that shape even though final farm size is identical; the behavior is
  a format-driven consequence, not a reason to recognize particular tables or schemas.
- The protocol fork exposes a generic streaming upload sink. The driver serves each requested
  column in bounded 1 MiB encoder pieces that the protocol coalesces into 16 MiB messages, so an
  encoded window is never materialized as one `Vec<u8>`. Null-free signed integer and float
  layouts borrow validated Arrow buffers directly; other types use one bounded scratch piece.
  During a flush the protocol scatter-writes MAPI headers with the pending 16 MiB payload instead
  of copying it into a similarly sized framed message, so the conservative streaming-buffer bound
  is about 17 MiB including the 1 MiB encoder chunk and framing headers.
  `peak_in_flight_bytes` reports that bound from the protocol constants rather than inferring it
  from producer chunk size. A 2M-row
  single-column replay improved from 52.3 to 23.8 ms for BIGINT, 127.6 to 53.0 ms for VARCHAR, and
  105.7 to 42.5 ms for TIMESTAMP on the documented local Dec2025 harness because adaptive
  coalescing also removed repeated COPY round trips. On a 160 MB, 20-column random-REAL upload,
  replacing copied framing with scatter framing reduced the median of five warmed runs from
  168.6 to 161.2 ms (4.4%) and removed one message-sized allocation.
- Unconstrained appends to an existing table inside a caller-managed transaction execute directly
  for every COPY window. Operation savepoints caused MonetDB to retain and materialize
  disproportionate storage for large, wide streams even after release. A client-side error after
  a completed target window marks the connection rollback-only: reads remain available, but
  connection APIs and raw SQL both block commit until a successful rollback clears the state.
  Staged constrained appends write no target rows before the final move and retain an operation
  savepoint, so producer and constraint failures can preserve earlier caller work without risking
  a partial target. Explicit savepoint and partial-commit modes remain advanced opt-ins for the
  direct path. Autocommit wraps the complete stream in an internal transaction; create and replace
  modes retain their operation savepoint because their DDL must be recovered without discarding
  unrelated caller work.
- Constrained staging is intentionally a speed/server-storage trade. On a generic BIGINT-primary-key
  plus 512-byte-BLOB stream at 100,000, 400,000, and 1,600,000 rows, staged caller-transaction server
  RSS peaked at 186.0, 564.4, and 2,884.5 MiB, versus 145.4, 255.0, and 883.6 MiB for direct COPY.
  At 1,600,000 rows, staged peak disk was 3,043.3 MiB versus 1,088.0 MiB direct, while ingest was
  2.422 versus 1.749 seconds. Autocommit did not remove the staged duplication. Moving and
  truncating 512 MiB generations inside the same atomic transaction was rejected: it measured
  2,934.8 MiB RSS, 3,027.5 MiB disk, and 2.548 seconds because MonetDB retains rollback state until
  commit. `constrained_append=direct` remains the honest memory escape hatch; create/replace in a
  caller transaction retains its savepoint to preserve earlier work.
- Post-ingest RSS is not live driver or Arrow state in the measured macOS case. Arrow accounting
  returned to zero, while the retained pages appeared as empty large malloc regions and were
  released gradually by the platform allocator. A PyArrow-only replay retained the same class of
  pages. In a fresh Linux process, a 1 GiB synthetic ingest returned to 28.6 MiB above its baseline
  after cursor close, with zero Arrow-allocated bytes and a 92.1 MiB measured driver prefetch peak.
  The Linux integration suite gates the post-close delta at 256 MiB. Allocator replacement and
  explicit trimming remain rejected because they would treat platform policy as live driver state.
- The prefetch worker fetches complete raw `Xexportbin` frames while the caller decodes the
  previous frame. A worker that both fetches and decodes would serialize those phases and lose the
  overlap.
- Prefetch can hold about three byte-budgeted windows: one decoding, one buffered, and one in
  flight. Copy-decoded response allocations are returned to the fetch worker for reuse. Early
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
- Savepoint-scoped temporary-table ingest needs both the target's commit action and whether any
  transaction-scoped temporary table exists. One catalog query returns both facts and is reused by
  atomic-scope selection, avoiding a duplicate round trip without weakening temporary-table
  preservation.
- A reduced deterministic SQLsmith corpus generated with seed 52217 is committed as an integration
  fixture. CI replays ordinary, nested, very long, and expected-server-error query strings through
  ADBC and verifies connection recovery. SQLsmith is not built in CI; generation is an offline way
  to refresh the bounded fixture, not a second database execution harness.
- MonetDB `JSON` maps to the canonical Arrow `arrow.json` extension backed by UTF-8, never to an
  Arrow `Struct`. MonetDB defines JSON as a validated string subtype, and a column may contain
  objects, arrays, scalars, and JSON `null` with different shapes in different rows. Functions
  declared to return JSON preserve `arrow.json`; functions declared to return strings, numbers,
  integers, or booleans use those scalar Arrow types. Polars may explicitly decode the resulting
  `String` storage into a known struct schema when an application wants that narrower view.
- `TIMESTAMPTZ` decoding under `SET TIME ZONE` is correct and is pinned by a live regression test.
- Interval parameters render their fractional seconds only when non-zero. MonetDB's
  `parse_interval` advances past the fraction only for a non-zero value, so `INTERVAL '90.000'
  SECOND` is rejected as trailing input while `INTERVAL '90' SECOND` and `INTERVAL '90.250' SECOND`
  parse. Whole-second durations are the common case, so before this the prepared-INSERT route could
  not re-ingest what a fetch of the same column produced. A fetch → append round trip for
  `INTERVAL SECOND`, `INTERVAL DAY`, and `INTERVAL MONTH` is pinned on both routes.
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
  surrounding whitespace and trailing semicolons. A configurable 512-entry least-recently-used
  cache bounds
  session memory; lookups update a monotonic use counter in constant time, while eviction scans
  only when inserting a new entry. Shared entry leases prevent eviction from deallocating a plan
  still used by a live statement. Statement destruction remains nonblocking; the cache or session
  owns server cleanup.
- Commit and rollback preserve MonetDB prepared statements. Schema-changing SQL observed on the
  same connection clears the cache before execution. MonetDB can also silently discard a plan
  after DDL from another session; an `EXEC: PREPARED Statement missing` response evicts that exact
  entry and retries once when doing so cannot roll back user work.
- Constrained-append staging invalidates the complete prepared cache because MonetDB clears every
  server-side prepared handle when the staging DDL runs, including for a session-local table.
  Scoped client invalidation would retain IDs that no longer exist; there are no surviving server
  handles to deallocate after the generation changes.
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
- MonetDB rejects parameters used as aggregate or window-function arguments during `PREPARE`.
  The driver treats the server's general parameter-argument diagnostic like an indeterminate
  parameter type and uses its existing typed literal renderer for that statement. Recognizing the
  diagnostic instead of individual SQL functions keeps the fallback independent of query
  generators, and a server version that accepts the prepared form automatically returns to the
  cached path.
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

## Ingest routing, retention, and topology

- A complete one-batch ingest of at most 100 rows uses the cached prepared INSERT path when a
  conservative expansion-aware input gate passes and incrementally rendered SQL remains at most
  8 MiB.
  At 1,001 columns, binary COPY costs about 210 ms regardless of one or ten rows because the
  server requests one client file per column; a prepared one-row INSERT costs 3–5 ms. The row
  threshold is configurable, zero disables the route, and multi-batch readers always stay on
  COPY. The 8 MiB cap bounds literal rendering and the single MAPI script.
- Connection initialization times existing metadata queries and retains the lower observed round
  trip without adding a probe. At 0.5 ms, the INSERT crossover starts growing from the measured
  round trip. At 5 ms, the COPY window and incompressible floor grow to the remote-link target.
  At 1,000 columns, a local Toxiproxy
  calibration measured one-row COPY at 0.23 seconds direct, 3.92 seconds with 1 ms downstream
  latency, and 12.59 seconds with 5 ms; a prepared 1,000-row INSERT remained 1.57–1.78 seconds.
  The manual `test_measured_latency_adapts_insert_routing_and_copy_windows` regression reproduces
  the topology change with the pinned Compose proxy.
- The earlier 512-column rule that expanded wide schemas to the largest physical window was
  removed. Width now only tightens the local physical bound to 48 MiB, because per-column state
  makes those schemas the most expensive. Network topology remains the only automatic expansion
  input: at or above 5 ms the driver restores the logical target for every schema.
- Each new window samples at most 256 KiB per column. For
  fixed-width values it compares plain LZ4 with byte-plane shuffle plus LZ4 and selects only a
  representation saving at least 12.5%. `auto` and `none` both permit shuffled client storage;
  `auto` independently sends wire frames only for an all-plain-LZ4 window, and `lz4` forces
  frames. Selection is per column and byte-observed. A
  full-chunk savings check can still demote a misleading sample to raw retention without affecting
  correctness. Retained Arrow batches are encoded through a capacity-one worker channel, so
  encoding of a bounded piece overlaps upload of the preceding piece.
- A zero-capacity window handoff builds one logical ingest window while the preceding COPY is
  uploading. The bound prevents unbounded producer read-ahead. Reported reader errors and worker
  panics are surfaced before the statement completes; after an ingest error, a worker that has not
  stopped is detached so cleanup cannot block indefinitely. On the 764,331 × 786 time-series
  input, this reduced the large-table ingest query from 8.21 to 6.52 seconds.
- Append matches stream columns to destination columns by name on every route. The prepared-INSERT
  route was by-name from the start because it names the columns it inserts, while the COPY route
  validated positionally with exact arity, so an identical frame succeeded below the routing
  threshold and failed above it. The contract is now the prepared route's, implemented by naming
  the destination columns in the COPY statement (`COPY … BINARY INTO t (…) FROM 'c0', …`) in the
  stream's order: `bincopyfrom` runs the same `rel_inserts` reorder-and-default path as
  `INSERT … (column list)`, so absent columns take their `DEFAULT` and no client-side projection or
  null-fill is needed. Tightening the small route to positional exact-arity instead was rejected:
  it breaks working callers and discards server-side `DEFAULT` handling the small route already
  provided. Name matching prefers an exact spelling and then falls back to case-insensitive
  matching, so quoted destination columns that differ only by case remain distinguishable. The
  insert route re-prepares once with the catalog's spelling when fallback matching is required.
- Append-schema reads use `sys._columns`/`sys._tables`, or their `tmp` counterparts when the ADBC
  temporary-table option is set. On the supported 11.55.1 release, a temporary-table preflight
  immediately before the public `sys.columns` view can misbind that view's internal `_columns`
  reference to the temporary schema. The base catalogs provide the same metadata without that
  server-side binder defect; the minimum-version integration job covers the sequence.
- MonetDB 11.55.0–11.55.6 can acknowledge a local temporary-table definition without retaining it
  when a prepared statement has already run in the caller transaction. Constrained append staging
  therefore uses a transaction-scoped, uniquely named `UNLOGGED` table in the target schema on
  those versions and a session-local table from 11.55.7 onward. Both are dropped after a successful
  final move and rolled back with the ingest savepoint on failure. Temporary-table targets stay on
  the direct path on the affected versions because persistent tables cannot be created in `tmp`.
- Server-side LZ4 is selected from observed bytes, not from another latency heuristic. The `auto`
  default sends plain frames only when every column in a window cleared the savings threshold; an
  incompressible or shuffled column keeps the whole window on the ordinary upload path. `none`
  disables the wire path, while `lz4` forces bounded plain frames for links where reduced bytes
  outweigh server decompression. In the final cross-suite validation, `auto` improved time-series
  by 5%, rtabench by 22%, and ClickBench by 28% versus the preceding ADBC state. Per-window
  selection prevents one high-entropy window from disabling compression for later repetitive
  data, and `window_wire_compression` exposes the decision.
- The stats contract exposes every adaptive decision: `path`, `measured_round_trip_us`,
  `insert_rows_threshold`, `window_budget_bytes`, `incompressible_window_budget_bytes`,
  `stored_bytes`, `window_storage`, `window_wire_compression`, and `prepared_cache_hits`.
  Physical attribution reports per-window stored bytes, staging bytes, retained Arrow buffer
  capacities, scratch, and single-window/prefetch high-water estimates. Tests assert these fields
  instead of inferring routes from timing.

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
| Demoting fixed-width Arrow validation | Rejected; a paired time-series check improved the 786-column ingest by only 0.4%, inside run noise, and a 200,000-row primitive control was neutral. Full validation keeps one contract for all foreign Arrow inputs. |
| Cross-window string backreferences | Deferred; keeping windows independent bounds state, while adaptive coalescing already avoids upstream-batch resets. |
| Asynchronous COPY double-buffering | Rejected for the sequential ON CLIENT protocol; synchronous bounded encode/send already holds at most one scratch and one framed chunk. |
| A worker that fetches and decodes | Rejected because it cannot overlap the two phases. |
| Performance-only protocol-fork primitives | Rejected unless a genuinely generic MAPI operation is missing. |
| Mid-transaction staged move plus `TRUNCATE` generations | Rejected; MonetDB retained rollback state, leaving RSS/disk unchanged while latency regressed about 5%. |
| Allocator swaps or explicit post-ingest trimming | Rejected; measured retained macOS pages were empty allocator regions, while Linux returned to 28.6 MiB above baseline without intervention. |

The dominant remaining read costs are server execution, serialization, and transfer; writes are
even more server-bound. Unix-domain sockets are already available through `sock=` for colocated
workloads. Server-emitted string backreferences need no new client work if MonetDB begins using
them, because the decoder path already exists.

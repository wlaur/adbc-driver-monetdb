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
- The driver does not fall back to text-protocol result sets. Unsupported binary representations
  return bounded errors.
- Optional XDBC fields are not advertised without backend semantics to populate them.
- Statement destruction remains nonblocking. Callers use cancellation or close the result reader
  to interrupt network work; prefetched-reader teardown performs its own bounded grace period and
  cancellation.
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

- The default read window and maximum parallel encode/COPY window are both 131,072 rows. A sweep
  from 32,768 through 262,144 rows found no better general default. Smaller write windows remain
  configurable.
- The prefetch worker fetches complete raw `Xexportbin` frames while the caller decodes the
  previous frame. A worker that both fetches and decodes would serialize those phases and lose the
  overlap.
- Prefetch can hold about three windows: one decoding, one buffered, and one in flight. Early
  abandonment can therefore waste up to two fetched windows.
- MonetDB currently emits plain NUL-terminated strings for `Xexportbin`, not string backreferences.
  The decoder keeps its tested backreference path for forward compatibility, but the current
  two-phase string decoder is optimized for literal output.
- Write-side string deduplication remains enabled. Removing it reduced encoding work but increased
  repeated-data wire volume by 2–5× and also removed server-side savings.
- The adaptive dedup map samples 4,096 rows. Unconditional pre-sizing raised memory use, while a
  2,048-row sample was unstable.

## Semantics and test strategy

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
| Cross-batch backreferences, COPY double-buffering, or write-side pipelining | Deferred unless a measured workload puts encoding back on the critical path; server COPY time currently dominates. |
| A worker that fetches and decodes | Rejected because it cannot overlap the two phases. |
| Performance-only protocol-fork primitives | Rejected unless a genuinely generic MAPI operation is missing. |

The dominant remaining read costs are server execution, serialization, and transfer; writes are
even more server-bound. Unix-domain sockets are already available through `sock=` for colocated
workloads. Server-emitted string backreferences need no new client work if MonetDB begins using
them, because the decoder path already exists.

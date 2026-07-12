# Implementation plan

An ADBC driver for MonetDB in Rust: reads decode MonetDB's binary result-set protocol
(`Xexportbin`) directly into Arrow record batches; writes stream Arrow columns through
`COPY BINARY ... ON CLIENT`. Consumers get MonetDB through the standard ADBC surface —
polars, pandas ≥ 2.2, R (`adbi`), and anything else that speaks ADBC — instead of
per-value Python object materialization.

Context: [MonetDB/MonetDB#7464](https://github.com/MonetDB/MonetDB/issues/7464) asked for
exactly this and MonetDB's maintainers invited an external implementation. pymonetdb's own
docs put the binary result-set format at "a factor 3 or more" faster to parse than the text
protocol — and it still builds Python objects; decoding to Arrow in native code removes
that ceiling entirely, and bulk ingest replaces `executemany` row loops.

## Support policy

- **MonetDB Dec2025 (11.55)+, little-endian servers only.** Verified at connect (the login
  challenge must advertise `BINARY=1`; `monet_version` checked via `sys.env()`); the driver
  fails fast otherwise. No text-protocol result decoding, no legacy fallbacks.
- **Current-stable consumers only:** Python ≥ 3.13, polars ≥ 1.42, adbc-driver-manager ≥ 1.11
  (ADBC spec 1.1). Rust: edition 2024, `adbc_core`/`adbc_ffi` 0.23, arrow 58 (one arrow major
  per workspace, moved forward in lockstep with ADBC releases).
- **Platforms:** Linux x86_64 + aarch64 (manylinux), macOS arm64, Windows x86_64; one abi3
  (cp313) wheel per platform.

## Architecture

```
adbc_driver_monetdb           thin shim: dbapi.connect() -> adbc_driver_manager
                              (module name is load-bearing: polars resolves
                              adbc_driver_{scheme} for monetdb:// URIs)
crates/adbc-monetdb           ADBC object model (adbc_core traits) exported as a
                              C-ABI cdylib via adbc_ffi::export_driver!
                              (AdbcDriverMonetdbInit + AdbcDriverInit fallback)
crates/monetdb-arrow          wire format <-> Arrow: Xexportbin frame parsing (done),
                              per-type decoders/encoders (M2)
monetdb-rust                  fork of the official MAPI client crate (submodule):
                              framing, auth, TLS, URL parsing exist; the pieces the
                              driver needs are added here (M1)
```

The fork is consumed as a path dependency and kept public; protocol additions are written
as upstream-shaped MPL-2.0 changes so they can be offered back to
[MonetDB/monetdb-rust](https://github.com/MonetDB/monetdb-rust).

## Milestones

### M0 — spike: validate the read path end to end

- [x] `Xexportbin` issued over a raw MAPI connection against a live Dec2025 server
- [x] decode int/double/varchar columns to Arrow, hand to polars via the C stream interface
- [ ] benchmark a large (multi-million row, mixed numeric/string) fetch against pymonetdb
      (text and binary modes) to quantify the win before building everything else
- [ ] pin down the error-offset semantics of the trailing negative `toc_pos`
      (docs say offset-from-start; pymonetdb reads it relative to the end — see the
      TODO in `crates/monetdb-arrow/src/exportbin.rs`)
- [ ] measure where bulk-insert time goes (client serialization vs server ingest) to size
      the write-path win

### M1 — protocol layer (the monetdb-rust fork)

- [x] `Xexportbin <resid> <start> <count>` command + binary response framing
- [x] `Xreply_size` control (small text prefix, bulk via binary windows)
- [x] transactions: autocommit handshake option, `Xauto_commit`, commit/rollback
- [ ] `PREPARE` / `EXECUTE` (text descriptions; `Q_PREPARE` result sets are text-only
      in the MAPI protocol)
- [ ] file-transfer uploads (the `rb` subprotocol) for `COPY BINARY ... ON CLIENT`,
      serving named "files" from in-memory column buffers
- [x] expose the server fingerprint: endianness (challenge field 5), `BINARY` level,
      `monet_version`
- [x] result-set header parsing that carries decimal digits/scale and column types
      through to the consumer

### M2 — Arrow conversion (crates/monetdb-arrow)

- [x] `Xexportbin` frame parsing (header, 32-byte-aligned columns, TOC, in-frame errors)
- [x] fixed-width decoders: ints (sentinel `INT_MIN` per width), floats (NaN = NULL),
      bool (`0x80` = NULL), decimal as scaled int8/16/32/64/128 → `decimal128(p, s)`
- [x] temporal decoders: date (4 B struct), time (8 B), timestamp (12 B) → `date32` /
      `time64[us]` / `timestamp[us]`; **the wire field named `ms` holds microseconds**
- [x] string decoder: NUL-terminated UTF-8 → offsets + data buffer; `80 00` = NULL;
      back-reference decoding (defensive on read — servers currently emit backrefs only
      on ingest, but the format allows them)
- [x] blob (i64 length prefix, `~0` = NULL), uuid (16 B, all-zero = NULL), inet4/inet6
- [ ] encoders for every type above (COPY BINARY little-endian), including
      dictionary/categorical strings → back-reference encoding
- [ ] golden-fixture tests for every type (bytes captured from a real server), plus
      property tests for the string/backref codec

### M3 — ADBC surface (crates/adbc-monetdb)

- [x] connection lifecycle: URI/username/password options, MAPI connect via the protocol
      crate, Dec2025+ / little-endian gate with clear errors
- [x] `ExecuteQuery` → `RecordBatchReader`: one batch per `Xexportbin` window; window size
      as a statement option (`adbc.monetdb.batch_rows`, default ~128k rows)
- [x] `ExecuteUpdate` for DML/DDL (affected-row counts from the text header)
- [ ] bulk ingest: modes `create` / `append` / `replace` / `create_append` +
      `adbc.ingest.temporary`; DDL generated from the Arrow schema; multi-batch streams
      chunked into successive `COPY BINARY` statements inside one transaction
- [ ] `GetInfo` (vendor_name = "MonetDB" — polars introspects it), `GetTableTypes`
- [ ] `GetObjects` / `GetTableSchema` via `sys.tables` / `sys.columns` / `sys.keys`
      (and `PREPARE SELECT * FROM t` for schemas)
- [ ] prepared statements with positional (qmark) parameters rendered as SQL literals
      (MonetDB has no wire-level binary bind; bulk data goes through ingest)
- [x] error mapping: MAPI error strings → ADBC status + SQLSTATE (MonetDB prefixes
      errors with a 5-character SQLSTATE)
- [ ] geometry/xml columns: fail with guidance to cast to text in SQL

### M4 — packaging and release

- [x] CI: lint (ruff + pyright strict), rust (fmt/clippy/test), abi3 wheels for all four
      platforms (cibuildwheel + maturin), wheel smoke tests on 3.13/3.14, integration job
      against a dockerized MonetDB
- [ ] sdist build + PyPI publish via trusted publishing (tag-driven, already scaffolded)
- [ ] TOML driver manifest so non-Python driver managers (and `dbc install`) can resolve
      the driver by name
- [ ] register `monetdb` in
      [adbc-drivers/name-mappings](https://github.com/adbc-drivers/name-mappings)
- [ ] run the [adbc-drivers/validation](https://github.com/adbc-drivers/validation)
      pytest suite via a `DriverQuirks` class; publish the feature matrix in the README
- [ ] announce on MonetDB/MonetDB#7464 and offer the protocol work upstream

## ADBC feature mapping

| ADBC | MonetDB mechanism |
|---|---|
| `ExecuteQuery` → Arrow stream | execute + small `reply_size`, then `Xexportbin` row windows |
| bulk ingest (all four modes) | DDL from Arrow schema + `COPY BINARY INTO ... ON CLIENT` |
| `adbc.ingest.temporary` | `CREATE LOCAL TEMPORARY TABLE` |
| prepared statements / bind | `PREPARE`/`EXECUTE`, literal parameter rendering |
| `GetTableSchema` | `PREPARE SELECT * FROM t` (no execution) |
| `GetObjects` | SQL over `sys.*` catalogs |
| transactions / autocommit | handshake option + `Xauto_commit` + SQL |
| partitioned results | unsupported (MAPI is a single sequential channel) |

## Type mapping (read direction; write is the inverse)

| MonetDB | wire | NULL sentinel | Arrow | polars |
|---|---|---|---|---|
| BOOLEAN | 1 B | `0x80` | `bool` | `Boolean` |
| TINYINT/SMALLINT/INT/BIGINT | i8/i16/i32/i64 | `INT_MIN` of width | `int8..int64` | `Int8..Int64` |
| HUGEINT | i128 | `1<<127` | `decimal128(38,0)` | `Decimal(38,0)` |
| REAL / DOUBLE | f32 / f64 | NaN | `float32/64` | `Float32/64` |
| DECIMAL(p,s) | scaled int (width by p) | width's `INT_MIN` | `decimal128(p,s)` | `Decimal(p,s)` |
| VARCHAR/CHAR/CLOB | NUL-terminated (+backrefs) | `80 00` | `utf8` | `String` |
| JSON / URL | as string | `80 00` | `utf8` (+ `arrow.json` metadata) | `String` |
| BLOB | i64 len + bytes | len = `~0` | `binary` | `Binary` |
| DATE | `{u8 day,u8 month,i16 year}` | month `0xFF` | `date32` | `Date` |
| TIME | `{u32 µs,u8 s,u8 m,u8 h,pad}` | all `0xFF` | `time64[us]` | `Time` |
| TIMESTAMP | time+date structs (12 B) | all `0xFF` | `timestamp[us]` | `Datetime("us")` |
| TIMESTAMPTZ | 12 B, components in UTC | all `0xFF` | `timestamp[us,"UTC"]` | `Datetime` (UTC) |
| INTERVAL SECOND/DAY | i64 ms | i64 min | `duration[ms]` | `Duration("ms")` |
| INTERVAL MONTH | i32 months | i32 min | `int32` + metadata | `Int32` |
| UUID | 16 B | all-zero | `arrow.uuid` (FSB16); option for `utf8` | `Binary(16)`/`String` |
| INET4/INET6 | 4/16 B | zero | `utf8` (rendered) | `String` |
| GEOMETRY / XML | not in the binary format | — | error (cast to text in SQL) | — |

Write direction extras: unsigned ints widen (`u8→SMALLINT`, `u16→INT`, `u32→BIGINT`,
`u64→HUGEINT`); `Categorical`/`Enum` → VARCHAR with backref encoding; tz-aware datetimes
convert to UTC → TIMESTAMPTZ; float NaN and null both map to NULL (MonetDB semantics);
`Struct`/`List` are unsupported — JSON-encode first (matches MonetDB's type system).

## Protocol notes (the sharp edges)

- `Xexportbin` frames: text `&6` header line, then each column 32-byte aligned (use the
  TOC, never assume contiguity), then TOC + trailing offset. TOC integers are in the
  *client-requested* byte order; column data is in the *server's native* order.
- The first `reply_size` rows of every result set arrive as text inside the execute
  response — negotiate a small reply size and fetch the bulk via `Xexportbin`.
- `Q_PREPARE` result sets cannot be fetched with `Xexport*` (protocol limitation);
  prepared-statement metadata is text-only. `EXECUTE` results are ordinary `Q_TABLE`s.
- String backrefs are ingest-oriented; current servers do not emit them on export, but the
  decoder handles them anyway.
- MonetDB stores no real float NaNs (NaN is the NULL sentinel) — the mapping is lossless.
- An all-zero UUID is indistinguishable from NULL by design (server semantics).

## Testing

- **Unit (no server):** frame/codec tests with synthetic and golden fixtures in
  `monetdb-arrow`; the Python package chain is smoke-tested by asserting the skeleton's
  `NotImplemented` error surfaces through driver-manager dlopen.
- **Integration (dockerized `monetdb/monetdb:Dec2025-SP3`):** polars round-trips over the
  full dtype matrix — nulls in every type, empty vs NULL strings, decimal extremes at each
  backing width, temporal edge values, 0-row and multi-window results, wide tables.
  Tests are written first and marked `xfail(strict=False)` until their milestone lands.
- **Conformance:** the ADBC validation suite (M4) drives the driver through the Python
  driver manager, so it exercises the same artifact users install.

## References

- MonetDB binary result sets: `documentation/source/binary-resultset.rst` (MonetDB repo);
  server implementation `sql/backends/monet5/sql_result.c` (`mvc_export_bin_chunk`)
- COPY BINARY formats: `sql/backends/monet5/sql_bincopyconvert.c`,
  `common/utils/copybinary.h`, `documentation/source/bincopy-backref.rst`
- pymonetdb (reference client, incl. binary decoding): https://github.com/MonetDB/pymonetdb
- ADBC spec + Rust crates: https://arrow.apache.org/adbc/ ,
  https://github.com/apache/arrow-adbc/tree/main/rust
- Driver Foundry (driverbase-rs, template-rs, validation): https://github.com/adbc-drivers
- polars ADBC integration: `pl.read_database`, `pl.read_database_uri(engine="adbc")`,
  `DataFrame.write_database(engine="adbc")`

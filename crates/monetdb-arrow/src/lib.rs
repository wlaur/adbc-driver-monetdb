//! Conversion between MonetDB's binary wire formats and Apache Arrow.
//!
//! Scope:
//! - decode `Xexportbin` binary result-set frames into Arrow `RecordBatch`es
//!   (one batch per fetched row window),
//! - encode Arrow columns into the `COPY BINARY` per-column format for bulk
//!   ingest via `COPY LITTLE ENDIAN BINARY INTO ... ON CLIENT`.
//!
//! The column byte layout is identical in both directions and is defined by the
//! MonetDB server sources (`sql/backends/monet5/sql_bincopyconvert.c`,
//! `common/utils/copybinary.h`) and documented in
//! `documentation/source/binary-resultset.rst` / `bincopy-backref.rst`.
//!
//! Only little-endian servers and MonetDB Dec2025 (11.55) or newer are
//! supported.

pub mod exportbin;

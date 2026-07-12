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

mod decode;
mod encode;
pub mod exportbin;

pub use decode::{
    DecodeError, data_type_for_monet_type, decode_column, decode_frame, decode_inline_row,
    field_for_column, field_for_monet_type,
};
pub use encode::{EncodeError, encode_column, monet_type_for_field, sql_type_for_field};

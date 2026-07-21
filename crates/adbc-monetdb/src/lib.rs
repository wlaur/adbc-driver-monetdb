//! ADBC driver for MonetDB.
//!
//! The driver speaks MAPI (via the forked `monetdb` submodule) and converts
//! between MonetDB's binary wire formats and Arrow via `monetdb-arrow`.
//! Supported servers: MonetDB Dec2025 (11.55) and newer, little-endian only.

mod driver;

pub use driver::{MonetdbConnection, MonetdbDatabase, MonetdbDriver, MonetdbStatement};

// Exports `AdbcDriverMonetdbInit` (and the generic `AdbcDriverInit` fallback)
// from the cdylib for the ADBC driver manager.
adbc_ffi::export_driver!(AdbcDriverMonetdbInit, MonetdbDriver);

#[cfg(feature = "python")]
mod python;

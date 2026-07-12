//! Stub Python extension module.
//!
//! Exists so maturin ships the driver cdylib inside the Python package as
//! `adbc_driver_monetdb._native`. The file is located via `_native.__file__`
//! and dlopened by the ADBC driver manager; it exposes no Python API.

use pyo3::prelude::*;
use pyo3::types::PyModule;

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__adbc_entrypoint__", "AdbcDriverMonetdbInit")?;
    Ok(())
}

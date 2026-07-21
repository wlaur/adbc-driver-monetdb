use std::{env, ffi::c_void, path::Path};

use adbc_core::constants::{ADBC_STATUS_OK, ADBC_VERSION_1_1_0};
use adbc_ffi::{FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcDriverInitFunc, FFI_AdbcError};
use libloading::{Library, Symbol};

fn main() -> Result<(), String> {
    let path = env::args_os()
        .nth(1)
        .ok_or_else(|| "usage: dbc_smoke <standalone-driver-library>".to_owned())?;
    let path = Path::new(&path);
    // SAFETY: The path comes from the CI-built standalone driver artifact. The
    // library stays alive until every resolved function pointer has been used.
    let library = unsafe { Library::new(path) }.map_err(|error| error.to_string())?;
    // SAFETY: The build exports this symbol with the ADBC initializer ABI.
    let initialize: Symbol<FFI_AdbcDriverInitFunc> =
        unsafe { library.get(b"AdbcDriverMonetdbInit\0") }.map_err(|error| error.to_string())?;
    let mut driver = FFI_AdbcDriver::default();
    let mut error = FFI_AdbcError::default();
    // SAFETY: Both pointers reference initialized, correctly aligned FFI
    // structs and remain valid for the duration of the call.
    let status = unsafe {
        initialize(
            ADBC_VERSION_1_1_0,
            (&mut driver as *mut FFI_AdbcDriver).cast::<c_void>(),
            &mut error,
        )
    };
    if status != ADBC_STATUS_OK {
        return Err(format!(
            "driver initialization returned ADBC status {status}"
        ));
    }

    let database_new = driver
        .DatabaseNew
        .ok_or_else(|| "initialized driver has no DatabaseNew function".to_owned())?;
    let database_release = driver
        .DatabaseRelease
        .ok_or_else(|| "initialized driver has no DatabaseRelease function".to_owned())?;
    let mut database = FFI_AdbcDatabase::default();
    // SAFETY: The function pointer was populated by the initialized driver and
    // receives valid database and error pointers.
    let status = unsafe { database_new(&mut database, &mut error) };
    if status != ADBC_STATUS_OK {
        return Err(format!("DatabaseNew returned ADBC status {status}"));
    }
    // SAFETY: DatabaseNew initialized this database with the same live driver.
    let status = unsafe { database_release(&mut database, &mut error) };
    if status != ADBC_STATUS_OK {
        return Err(format!("DatabaseRelease returned ADBC status {status}"));
    }
    Ok(())
}

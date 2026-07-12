use std::{env, path::PathBuf};

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let lock_path = manifest.join("../..").join("Cargo.lock");
    println!("cargo::rerun-if-changed={}", lock_path.display());
    let lockfile = cargo_lock::Lockfile::load(&lock_path).expect("read workspace Cargo.lock");
    let version = lockfile
        .packages
        .iter()
        .find(|package| package.name.as_str() == "arrow-array")
        .map(|package| &package.version)
        .expect("arrow-array is present in Cargo.lock");
    println!("cargo::rustc-env=ADBC_MONETDB_ARROW_VERSION=v{version}");
}

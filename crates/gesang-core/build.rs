//! Tracks schema and query inputs for reproducible compile-time validation.

fn main() {
    println!("cargo::rerun-if-changed=queries");
    println!("cargo::rerun-if-changed=migrations");
    println!("cargo::rerun-if-changed=migrations-sqlite");
    println!("cargo::rerun-if-env-changed=GESANG_SQL_CHECK");
}

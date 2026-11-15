//! Tracks schema changes and explicit database-validation runs.

fn main() {
    println!("cargo::rerun-if-changed=migrations");
    println!("cargo::rerun-if-env-changed=GESANG_SQL_CHECK");
}

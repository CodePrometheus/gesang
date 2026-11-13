//! One cross-platform entry point for repository checks and native build steps.

use std::{
    env,
    error::Error,
    ffi::OsString,
    io,
    path::Path,
    process::{Command, ExitCode},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut arguments = env::args_os().skip(1);
    let task = arguments.next().unwrap_or_else(|| OsString::from("help"));
    if arguments.next().is_some() {
        return Err(io::Error::other("unexpected argument; run cargo xtask help").into());
    }
    match task.to_str() {
        Some("help" | "--help" | "-h") => {
            println!(
                "Usage: cargo xtask <task>\n\nTasks:\n  check                Formatting, strict lint, unit tests, and API documentation\n  fmt                  Check repository formatting without changing files\n  lint                 Strict static checks for every workspace target\n  test                 Workspace tests, excluding explicitly ignored integration tests\n  test-store           Real transactional store tests; requires GESANG_TEST_DATABASE_URL\n  test-storage         Real native execution storage recovery; requires explicit integration settings\n  test-initialization  Real native first-store creation and reconciliation through shared custody\n  doc                  Check API documentation with warnings treated as errors\n  android-setup        Install the pinned official native toolchain\n  android-native       Build native libraries and generated Kotlin bindings"
            );
            Ok(())
        }
        Some("check") => {
            formatting()?;
            lint()?;
            tests()?;
            documentation()
        }
        Some("fmt") => formatting(),
        Some("lint") => lint(),
        Some("test") => tests(),
        Some("doc") => documentation(),
        Some("test-store") => {
            if env::var_os("GESANG_TEST_DATABASE_URL").is_none() {
                return Err(io::Error::other(
                    "GESANG_TEST_DATABASE_URL must identify a dedicated integration database",
                )
                .into());
            }
            cargo(&[
                "test",
                "--locked",
                "-p",
                "gesang-core",
                "--test",
                "transactional_store",
                "--test",
                "configuration",
                "--test",
                "custody",
                "--test",
                "lifecycle",
                "--",
                "--ignored",
                "--nocapture",
            ])?;
            cargo(&[
                "test",
                "--locked",
                "-p",
                "gesang-host",
                "--test",
                "control",
                "--test",
                "jobs",
                "--test",
                "initialization",
                "--",
                "--ignored",
                "--nocapture",
            ])
        }
        Some("test-storage") => {
            for name in [
                "GESANG_TEST_DATABASE_URL",
                "GESANG_TEST_STORAGE_PROBE",
                "CHOPIN_STORAGE_NATIVE",
                "GESANG_TEST_STORAGE_TIMEOUT_MS",
            ] {
                if env::var_os(name).is_none_or(|value| value.is_empty()) {
                    return Err(io::Error::other(format!(
                        "{name} is required for real execution storage tests"
                    ))
                    .into());
                }
            }
            cargo(&[
                "test",
                "--locked",
                "-p",
                "gesang-host",
                "--test",
                "storage",
                "--",
                "--ignored",
                "--nocapture",
            ])
        }
        Some("test-initialization") => {
            for name in [
                "GESANG_TEST_DATABASE_URL",
                "GESANG_TEST_INITIALIZATION_PROBE",
                "CHOPIN_STORAGE_NATIVE",
                "GESANG_TEST_STORAGE_TIMEOUT_MS",
            ] {
                if env::var_os(name).is_none_or(|value| value.is_empty()) {
                    return Err(io::Error::other(format!(
                        "{name} is required for real initialization tests"
                    ))
                    .into());
                }
            }
            cargo(&[
                "test",
                "--locked",
                "-p",
                "gesang-host",
                "--test",
                "initialization_storage",
                "--",
                "--ignored",
                "--nocapture",
            ])
        }
        Some("android-setup") => shell_step("scripts/android-setup.sh"),
        Some("android-native") => shell_step("scripts/android-native.sh"),
        _ => Err(io::Error::other("unknown task; run cargo xtask help").into()),
    }
}

fn formatting() -> Result<()> {
    cargo(&["fmt", "--all", "--check"])
}

fn lint() -> Result<()> {
    cargo(&[
        "clippy",
        "--locked",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
    ])
}

fn tests() -> Result<()> {
    cargo(&[
        "test",
        "--locked",
        "--workspace",
        "--all-features",
        "--no-fail-fast",
    ])
}

fn documentation() -> Result<()> {
    let mut command = cargo_command();
    command.args([
        "doc",
        "--locked",
        "--workspace",
        "--no-deps",
        "--all-features",
    ]);
    let mut flags = env::var_os("RUSTDOCFLAGS").unwrap_or_default();
    flags.push(" -D warnings");
    command.env("RUSTDOCFLAGS", flags);
    execute(command)
}

fn cargo(arguments: &[&str]) -> Result<()> {
    let mut command = cargo_command();
    command.args(arguments);
    execute(command)
}

fn cargo_command() -> Command {
    Command::new(env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo")))
}

fn shell_step(script: &str) -> Result<()> {
    if cfg!(windows) {
        return Err(io::Error::other(
            "this native build step currently requires a supported Unix build host",
        )
        .into());
    }
    let mut command = Command::new("bash");
    command.arg(script);
    execute(command)
}

fn execute(mut command: Command) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| io::Error::other("repository root is unavailable"))?;
    command.current_dir(root);
    let status = command.status()?;
    if !status.success() {
        return Err(io::Error::other(format!("task step failed with {status}")).into());
    }
    Ok(())
}

//! One cross-platform entry point for repository checks and native build steps.

mod sql;

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
                "Usage: cargo xtask <task>\n\nTasks:\n  check                Formatting, strict lint, unit tests, and API documentation\n  fmt                  Check repository formatting without changing files\n  lint                 Strict static checks for every workspace target\n  test                 Workspace tests, excluding explicitly ignored integration tests\n  test-engine          Real service and host protocol integration; requires explicit integration settings\n  test-store           Real transactional store tests; requires GESANG_TEST_DATABASE_URL\n  test-jobs            Real Node command client, broker admission and original-operation recovery\n  test-command-goals   Real encrypted goal recovery through shared authority and command broker\n  test-native-command-goals  Real encrypted goal recovery with an isolated Linux worker\n  test-native-chat     Browser and file delivery with a real local model and Linux worker\n  test-sandbox         Real resource API and process recovery; requires explicit local runtime settings\n  prepare-sql          Generate offline query metadata from source migrations in an isolated test schema\n  check-sql            Verify query metadata against source migrations in an isolated test schema\n  test-storage         Real native execution storage recovery; requires explicit integration settings\n  test-initialization  Real native first-store creation and reconciliation through shared custody\n  doc                  Check API documentation with warnings treated as errors\n  android-setup        Install the pinned official native toolchain\n  android-native       Build native libraries and generated Kotlin bindings"
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
        Some("prepare-sql") => sql::run(sql::Action::Prepare),
        Some("check-sql") => sql::run(sql::Action::Check),
        Some("test-sandbox") => sandbox_tests(),
        Some("test-engine") => {
            let mut command = Command::new("python3");
            command.arg("scripts/test-engine.py");
            execute(command)
        }
        Some("test-command-goals") => protected_jobs_probe(
            "node_client::goal_restart",
            &["GESANG_TEST_COMMAND_GOAL_PROBE"],
        ),
        Some("test-native-command-goals") => protected_jobs_probe(
            "node_client::goal_restart_with_native_worker",
            &[
                "GESANG_TEST_COMMAND_GOAL_PROBE",
                "GESANG_TEST_NATIVE_WORKER_RUNNER",
            ],
        ),
        Some("test-native-chat") => protected_jobs_probe(
            "native_chat::publish_from_chat",
            &[
                "GESANG_TEST_NATIVE_WORKER_RUNNER",
                "GESANG_TEST_NATIVE_CHAT_DIR",
                "GESANG_TEST_NATIVE_CHAT_CHROME_URL",
                "GESANG_TEST_CHOPIN_SOURCE",
                "GESANG_TEST_MODEL_URL",
                "GESANG_TEST_MODEL_ID",
            ],
        ),
        Some("test-jobs") => {
            for name in [
                "GESANG_TEST_DATABASE_URL",
                "GESANG_TEST_JOB_PROBE",
                "GESANG_TEST_OPERATION_TIMEOUT_MS",
            ] {
                if env::var_os(name).is_none_or(|value| value.is_empty()) {
                    return Err(io::Error::other(format!(
                        "{name} is required for command client interoperability tests"
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
                "jobs",
                "node_client::interrupted_submit",
                "--",
                "--ignored",
                "--nocapture",
            ])
        }
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
                "--test",
                "services",
                "--test",
                "catalog",
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
                "--skip",
                "node_client::",
                "--skip",
                "native_chat::",
            ])?;
            cargo(&[
                "test",
                "--locked",
                "-p",
                "gesang-services",
                "--test",
                "access",
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

fn protected_jobs_probe(test: &str, extra_settings: &[&str]) -> Result<()> {
    let settings = [
        "GESANG_TEST_DATABASE_URL",
        "CHOPIN_STORAGE_NATIVE",
        "GESANG_TEST_OPERATION_TIMEOUT_MS",
    ];
    for name in settings.iter().chain(extra_settings) {
        if env::var_os(name).is_none_or(|value| value.is_empty()) {
            return Err(io::Error::other(format!(
                "{name} is required for the protected execution probe"
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
        "jobs",
        test,
        "--",
        "--ignored",
        "--exact",
        "--nocapture",
    ])
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

fn sandbox_tests() -> Result<()> {
    for variable in [
        "GESANG_TEST_DATABASE_URL",
        "GESANG_TEST_CONTAINER_CONFIG",
        "GESANG_TEST_ARTIFACT_DIR",
    ] {
        if env::var_os(variable).is_none_or(|value| value.is_empty()) {
            return Err(
                io::Error::other(format!("{variable} is required for real sandbox tests")).into(),
            );
        }
    }
    cargo(&[
        "build",
        "--locked",
        "-p",
        "gesang-services",
        "--example",
        "sandbox-driver",
    ])?;
    let output = cargo_command()
        .args(["metadata", "--locked", "--format-version", "1", "--no-deps"])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other("could not locate the built sandbox driver").into());
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let target = metadata["target_directory"]
        .as_str()
        .ok_or_else(|| io::Error::other("build target directory is missing"))?;
    let driver = Path::new(target).join("debug/examples/sandbox-driver");
    if !driver.is_file() {
        return Err(io::Error::other("the sandbox tests require a native host build").into());
    }
    let mut command = cargo_command();
    command.args([
        "test",
        "--locked",
        "-p",
        "gesang-services",
        "--test",
        "container",
        "-p",
        "gesang-host",
        "--test",
        "sandbox",
        "--",
        "--ignored",
        "--nocapture",
    ]);
    command.env("GESANG_TEST_SANDBOX_DRIVER", driver);
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

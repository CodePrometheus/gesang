use std::{env, fs, io, path::Path, str::FromStr};

use sqlx::{
    ConnectOptions, Executor,
    migrate::Migrator,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use crate::{Result, cargo_command, execute};

pub(crate) enum Action {
    Prepare,
    Check,
}

pub(crate) fn run(action: Action) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| io::Error::other("repository root is unavailable"))?;
    let url = env::var("GESANG_TEST_DATABASE_URL").map_err(|_| {
        io::Error::other("GESANG_TEST_DATABASE_URL must identify a dedicated test database")
    })?;
    let run_id = Uuid::new_v4();
    let schema = format!("sqlx_{}", run_id.simple());
    let artifacts = root
        .join(".agents/artifacts/sql-check")
        .join(run_id.to_string());
    let generated = artifacts.join("queries");
    fs::create_dir_all(&generated)?;

    let database = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(prepare_database(root, &url, &schema))?;

    let mut command = cargo_command();
    command.args([
        "check",
        "--locked",
        "--workspace",
        "--all-targets",
        "--all-features",
    ]);
    command.env("DATABASE_URL", database);
    command.env("SQLX_OFFLINE", "false");
    command.env("SQLX_OFFLINE_DIR", &generated);
    command.env("GESANG_SQL_CHECK", run_id.to_string());
    execute(command)?;

    let cache = root.join(".sqlx");
    fs::create_dir_all(&cache)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&generated)? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "json")
        {
            continue;
        }
        let source = fs::read(entry.path())?;
        let target = cache.join(entry.file_name());
        match action {
            Action::Prepare => {
                if target.is_symlink() {
                    return Err(
                        io::Error::other("query metadata must not be a symbolic link").into(),
                    );
                }
                fs::write(&target, &source)?;
            }
            Action::Check => {
                let expected = fs::read(&target).map_err(|_| {
                    io::Error::other("query metadata is missing; run cargo xtask prepare-sql")
                })?;
                if serde_json::from_slice::<serde_json::Value>(&source)?
                    != serde_json::from_slice::<serde_json::Value>(&expected)?
                {
                    return Err(io::Error::other(
                        "query metadata differs from the migration schema; run cargo xtask prepare-sql",
                    ).into());
                }
            }
        }
        files.push(entry.file_name());
    }
    if files.is_empty() {
        return Err(io::Error::other(
            "no query metadata was generated; SQL validation did not run",
        )
        .into());
    }
    let mut retired = Vec::new();
    for entry in fs::read_dir(&cache)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(text) = name.to_str() else { continue };
        if !text.starts_with("query-") || !text.ends_with(".json") || files.contains(&name) {
            continue;
        }
        match action {
            Action::Check => {
                return Err(io::Error::other(
                    "unused query metadata remains; run cargo xtask prepare-sql",
                )
                .into());
            }
            Action::Prepare => {
                let archive = artifacts.join("retired");
                fs::create_dir_all(&archive)?;
                fs::rename(entry.path(), archive.join(&name))?;
                retired.push(name);
            }
        }
    }
    fs::write(
        artifacts.join("result.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema": schema,
            "queries": files,
            "retired_metadata": retired,
            "matched": true,
            "database_retained": true
        }))?,
    )?;
    println!(
        "Validated {} queries against the source migrations.",
        files.len()
    );
    Ok(())
}

async fn prepare_database(root: &Path, url: &str, schema: &str) -> Result<String> {
    let bootstrap = PgPoolOptions::new().connect(url).await?;
    // The identifier contains only a fixed prefix and a generated UUID.
    bootstrap
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await?;
    bootstrap.close().await;
    let options = PgConnectOptions::from_str(url)?.options([("search_path", schema)]);
    let mut database = options.to_url_lossy();
    database
        .query_pairs_mut()
        .append_pair("options[search_path]", schema);
    let database = database.to_string();
    let pool = PgPoolOptions::new().connect_with(options).await?;
    Migrator::new(root.join("crates/gesang-core/migrations").as_path())
        .await?
        .run(&pool)
        .await?;
    let directory = root.join("crates/gesang-services/migrations");
    let table = fs::read_to_string(directory.join("table-name"))?;
    let mut services = Migrator::new(directory.as_path()).await?;
    services.dangerous_set_table_name(table.trim().to_owned());
    services.run(&pool).await?;
    pool.close().await;
    let probe = PgPoolOptions::new().connect(&database).await?;
    let actual = sqlx::query_scalar::<_, String>("SELECT current_schema()")
        .fetch_one(&probe)
        .await?;
    probe.close().await;
    if actual != schema {
        return Err(io::Error::other("query validation did not select the isolated schema").into());
    }
    Ok(database)
}

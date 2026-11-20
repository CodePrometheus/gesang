use std::{marker::PhantomData, path::Path};

use jiff::Timestamp;
use sqlx::{
    ColumnIndex, Decode, Encode, PgPool, Postgres, Row as _, Sqlite, SqlitePool, Type,
    postgres::{PgArguments, PgRow},
    query::Query as SqlxQuery,
    sqlite::{
        SqliteArguments, SqliteConnectOptions, SqliteJournalMode, SqliteRow, SqliteSynchronous,
    },
};

use crate::{Error, PostgresTimestamp, Result};

#[derive(Clone, Debug)]
pub(crate) enum Database {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

/// An active host transaction used by coordination checks and by the host's own writes.
#[derive(Debug)]
#[non_exhaustive]
pub enum CoordinationTransaction<'a, 'db> {
    /// A read-committed transaction against the configured platform database.
    Postgres(&'a mut sqlx::Transaction<'db, Postgres>),
    /// An immediate write transaction against the configured coordination file.
    Sqlite(&'a mut sqlx::Transaction<'db, Sqlite>),
}

impl<'a, 'db> From<&'a mut sqlx::Transaction<'db, Postgres>> for CoordinationTransaction<'a, 'db> {
    fn from(tx: &'a mut sqlx::Transaction<'db, Postgres>) -> Self {
        Self::Postgres(tx)
    }
}

impl<'a, 'db> From<&'a mut sqlx::Transaction<'db, Sqlite>> for CoordinationTransaction<'a, 'db> {
    fn from(tx: &'a mut sqlx::Transaction<'db, Sqlite>) -> Self {
        Self::Sqlite(tx)
    }
}

pub(crate) enum Transaction {
    Postgres(sqlx::Transaction<'static, Postgres>),
    Sqlite(sqlx::Transaction<'static, Sqlite>),
}

impl Transaction {
    pub(crate) fn coordination(&mut self) -> CoordinationTransaction<'_, 'static> {
        match self {
            Self::Postgres(tx) => tx.into(),
            Self::Sqlite(tx) => tx.into(),
        }
    }

    pub(crate) fn executor(&mut self) -> Executor<'_> {
        match self {
            Self::Postgres(tx) => Executor::Postgres(tx),
            Self::Sqlite(tx) => Executor::Sqlite(tx),
        }
    }

    pub(crate) async fn commit(self) -> Result<()> {
        match self {
            Self::Postgres(tx) => tx.commit().await?,
            Self::Sqlite(tx) => tx.commit().await?,
        }
        Ok(())
    }
}

impl CoordinationTransaction<'_, '_> {
    pub(crate) fn executor(&mut self) -> Executor<'_> {
        match self {
            Self::Postgres(connection) => Executor::Postgres(connection),
            Self::Sqlite(connection) => Executor::Sqlite(connection),
        }
    }

    pub(crate) async fn verify(&mut self) -> Result<()> {
        let safe = match self {
            Self::Postgres(connection) => {
                sqlx::query_scalar::<_, String>("SHOW transaction_isolation")
                    .fetch_one(&mut ***connection)
                    .await?
                    == "read committed"
            }
            Self::Sqlite(connection) => {
                sqlx::query_scalar::<_, i64>("PRAGMA read_uncommitted")
                    .fetch_one(&mut ***connection)
                    .await?
                    == 0
            }
        };
        if !safe {
            return Err(Error::DurabilityRequired);
        }
        Ok(())
    }
}

impl Database {
    pub(crate) async fn open_sqlite(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::{fs::OpenOptions, io::ErrorKind, os::unix::fs::OpenOptionsExt};

            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
            {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(sqlx::Error::Io(error).into()),
            }
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true);
        Ok(Self::Sqlite(SqlitePool::connect_with(options).await?))
    }

    pub(crate) fn executor(&self) -> Executor<'_> {
        match self {
            Self::Postgres(pool) => Executor::PostgresPool(pool),
            Self::Sqlite(pool) => Executor::SqlitePool(pool),
        }
    }

    pub(crate) async fn verify_transaction(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
    ) -> Result<()> {
        match (&self, &tx) {
            (Self::Postgres(_), CoordinationTransaction::Postgres(_))
            | (Self::Sqlite(_), CoordinationTransaction::Sqlite(_)) => tx.verify().await,
            _ => Err(Error::StorageBackend),
        }
    }

    pub(crate) async fn begin(&self) -> Result<Transaction> {
        Ok(match self {
            Self::Postgres(pool) => Transaction::Postgres(begin_postgres(pool).await?),
            Self::Sqlite(pool) => Transaction::Sqlite(pool.begin_with("BEGIN IMMEDIATE").await?),
        })
    }

    pub(crate) async fn begin_read(&self) -> Result<Transaction> {
        Ok(match self {
            Self::Postgres(pool) => Transaction::Postgres(pool.begin().await?),
            Self::Sqlite(pool) => Transaction::Sqlite(pool.begin().await?),
        })
    }

    pub(crate) async fn migrate(&self) -> Result<()> {
        self.validate_durability().await?;
        match self {
            Self::Postgres(pool) => sqlx::migrate!("./migrations").run(pool).await?,
            Self::Sqlite(pool) => sqlx::migrate!("./migrations-sqlite").run(pool).await?,
        }
        Ok(())
    }

    pub(crate) async fn validate_durability(&self) -> Result<()> {
        let safe = match self {
            Self::Postgres(pool) => {
                let (fsync, synchronous): (String, String) = sqlx::query_as(
                    "SELECT current_setting('fsync'), current_setting('synchronous_commit')",
                )
                .fetch_one(pool)
                .await?;
                fsync == "on" && matches!(synchronous.as_str(), "on" | "remote_apply")
            }
            Self::Sqlite(pool) => {
                let mut conn = pool.acquire().await?;
                let (journal, synchronous, foreign_keys): (String, i64, i64) = sqlx::query_as(
                    "SELECT journal_mode, synchronous, foreign_keys FROM                      pragma_journal_mode(), pragma_synchronous(), pragma_foreign_keys()",
                )
                .fetch_one(&mut *conn)
                .await?;
                journal.eq_ignore_ascii_case("wal") && synchronous >= 2 && foreign_keys == 1
            }
        };
        if !safe {
            return Err(Error::DurabilityRequired);
        }
        Ok(())
    }
}

pub(crate) async fn begin_postgres(pool: &PgPool) -> Result<sqlx::Transaction<'static, Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL synchronous_commit='on'")
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

pub(crate) enum Executor<'a> {
    Postgres(&'a mut sqlx::PgConnection),
    PostgresPool(&'a PgPool),
    Sqlite(&'a mut sqlx::SqliteConnection),
    SqlitePool(&'a SqlitePool),
}

#[derive(Clone, Copy)]
pub(crate) struct Statement {
    postgres: &'static str,
    sqlite: &'static str,
}

impl Statement {
    pub(crate) const fn new(postgres: &'static str, sqlite: &'static str) -> Self {
        Self { postgres, sqlite }
    }
}

impl From<&'static str> for Statement {
    fn from(sql: &'static str) -> Self {
        Self::new(sql, sql)
    }
}

// The shared spelling keeps authorization predicates in one place. Compound
// statements retain native row locks in Postgres and run under SQLite's writer lock.
macro_rules! statement {
    ($($part:tt),+ $(,)?) => {
        $crate::store::database::Statement::new(
            concat!($($crate::store::database::fragment!(postgres, $part)),+),
            concat!($($crate::store::database::fragment!(sqlite, $part)),+),
        )
    };
}

macro_rules! fragment {
    (postgres, clock) => {
        "clock_timestamp()"
    };
    (sqlite, clock) => {
        "(CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000)"
    };
    (postgres, (lease $parameter:literal)) => {
        concat!(
            "clock_timestamp()+$",
            stringify!($parameter),
            "*interval '1 millisecond'"
        )
    };
    (sqlite, (lease $parameter:literal)) => {
        concat!(
            "(",
            $crate::store::database::fragment!(sqlite, clock),
            " + $",
            stringify!($parameter),
            " * 1000)",
        )
    };
    (postgres, [$postgres:literal, $sqlite:literal]) => {
        $postgres
    };
    (sqlite, [$postgres:literal, $sqlite:literal]) => {
        $sqlite
    };
    ($backend:ident, $sql:literal) => {
        $sql
    };
}

pub(crate) use {fragment, statement};

pub(crate) enum Target<'e, DB: sqlx::Database> {
    Connection(&'e mut DB::Connection),
    Pool(&'e sqlx::Pool<DB>),
}

pub(crate) enum Query<'e, 'q> {
    Postgres(Target<'e, Postgres>, SqlxQuery<'q, Postgres, PgArguments>),
    Sqlite(Target<'e, Sqlite>, SqlxQuery<'q, Sqlite, SqliteArguments>),
}

pub(crate) enum Row {
    Postgres(PgRow),
    Sqlite(SqliteRow),
}

impl Row {
    pub(crate) fn try_get<'r, T, I>(&'r self, index: I) -> sqlx::Result<T>
    where
        T: Type<Postgres> + Type<Sqlite> + Decode<'r, Postgres> + Decode<'r, Sqlite>,
        I: ColumnIndex<PgRow> + ColumnIndex<SqliteRow>,
    {
        match self {
            Self::Postgres(row) => row.try_get(index),
            Self::Sqlite(row) => row.try_get(index),
        }
    }
    pub(crate) fn timestamp(&self, column: &str) -> sqlx::Result<Timestamp> {
        match self {
            Self::Postgres(row) => row.try_get(column).map(|PostgresTimestamp(value)| value),
            Self::Sqlite(row) => sqlite_timestamp(column, row.try_get(column)?),
        }
    }

    pub(crate) fn optional_timestamp(&self, column: &str) -> sqlx::Result<Option<Timestamp>> {
        match self {
            Self::Postgres(row) => row
                .try_get::<Option<PostgresTimestamp>, _>(column)
                .map(|value| value.map(|PostgresTimestamp(time)| time)),
            Self::Sqlite(row) => row
                .try_get::<Option<i64>, _>(column)?
                .map(|value| sqlite_timestamp(column, value))
                .transpose(),
        }
    }
}

fn sqlite_timestamp(column: &str, micros: i64) -> sqlx::Result<Timestamp> {
    Timestamp::from_microsecond(micros).map_err(|source| sqlx::Error::ColumnDecode {
        index: column.into(),
        source: source.into(),
    })
}

pub(crate) fn query<'e, 'q>(
    executor: Executor<'e>,
    statement: impl Into<Statement>,
) -> Query<'e, 'q> {
    let statement = statement.into();
    match executor {
        Executor::Postgres(conn) => {
            Query::Postgres(Target::Connection(conn), sqlx::query(statement.postgres))
        }
        Executor::PostgresPool(pool) => {
            Query::Postgres(Target::Pool(pool), sqlx::query(statement.postgres))
        }
        Executor::Sqlite(conn) => {
            Query::Sqlite(Target::Connection(conn), sqlx::query(statement.sqlite))
        }
        Executor::SqlitePool(pool) => {
            Query::Sqlite(Target::Pool(pool), sqlx::query(statement.sqlite))
        }
    }
}

macro_rules! run {
    ($query:expr, $method:ident, $wrap:expr) => {
        match $query {
            Query::Postgres(Target::Connection(conn), query) => {
                query.$method(conn).await.map($wrap)
            }
            Query::Postgres(Target::Pool(pool), query) => query.$method(pool).await.map($wrap),
            Query::Sqlite(Target::Connection(conn), query) => query.$method(conn).await.map($wrap),
            Query::Sqlite(Target::Pool(pool), query) => query.$method(pool).await.map($wrap),
        }
    };
    ($query:expr, $method:ident, $pg:expr, $sqlite:expr) => {
        match $query {
            Query::Postgres(Target::Connection(conn), query) => query.$method(conn).await.map($pg),
            Query::Postgres(Target::Pool(pool), query) => query.$method(pool).await.map($pg),
            Query::Sqlite(Target::Connection(conn), query) => {
                query.$method(conn).await.map($sqlite)
            }
            Query::Sqlite(Target::Pool(pool), query) => query.$method(pool).await.map($sqlite),
        }
    };
}

impl Query<'_, '_> {
    pub(crate) fn bind<'v, T>(self, value: T) -> Self
    where
        T: Type<Postgres> + Type<Sqlite> + Encode<'v, Postgres> + Encode<'v, Sqlite>,
    {
        match self {
            Self::Postgres(target, query) => Self::Postgres(target, query.bind(value)),
            Self::Sqlite(target, query) => Self::Sqlite(target, query.bind(value)),
        }
    }

    pub(crate) fn bind_timestamp(self, value: Timestamp) -> Self {
        self.bind_optional_timestamp(Some(value))
    }

    pub(crate) fn bind_optional_timestamp(self, value: Option<Timestamp>) -> Self {
        match self {
            Self::Postgres(target, query) => {
                Self::Postgres(target, query.bind(value.map(PostgresTimestamp)))
            }
            Self::Sqlite(target, query) => Self::Sqlite(
                target,
                // SQLite stores Unix microseconds, flooring negative fractional values.
                query.bind(value.map(|time| time.as_nanosecond().div_euclid(1_000) as i64)),
            ),
        }
    }

    pub(crate) async fn execute(self) -> sqlx::Result<u64> {
        run!(self, execute, |r| r.rows_affected())
    }

    pub(crate) async fn fetch_one(self) -> sqlx::Result<Row> {
        run!(self, fetch_one, Row::Postgres, Row::Sqlite)
    }

    pub(crate) async fn fetch_optional(self) -> sqlx::Result<Option<Row>> {
        run!(self, fetch_optional, |r| r.map(Row::Postgres), |r| r
            .map(Row::Sqlite))
    }

    pub(crate) async fn fetch_all(self) -> sqlx::Result<Vec<Row>> {
        run!(
            self,
            fetch_all,
            |r| r.into_iter().map(Row::Postgres).collect(),
            |r| r.into_iter().map(Row::Sqlite).collect()
        )
    }
}

pub(crate) struct ScalarQuery<'e, 'q, T>(Query<'e, 'q>, PhantomData<T>);

pub(crate) fn query_scalar<'e, 'q, T>(
    executor: Executor<'e>,
    statement: impl Into<Statement>,
) -> ScalarQuery<'e, 'q, T> {
    ScalarQuery(query(executor, statement), PhantomData)
}

impl<T> ScalarQuery<'_, '_, T>
where
    T: Type<Postgres> + Type<Sqlite> + for<'r> Decode<'r, Postgres> + for<'r> Decode<'r, Sqlite>,
{
    pub(crate) fn bind<'v, V>(self, value: V) -> Self
    where
        V: Type<Postgres> + Type<Sqlite> + Encode<'v, Postgres> + Encode<'v, Sqlite>,
    {
        Self(self.0.bind(value), self.1)
    }

    pub(crate) async fn fetch_one(self) -> sqlx::Result<T> {
        self.0.fetch_one().await?.try_get(0)
    }

    pub(crate) async fn fetch_optional(self) -> sqlx::Result<Option<T>> {
        self.0
            .fetch_optional()
            .await?
            .map(|row| row.try_get(0))
            .transpose()
    }

    pub(crate) async fn fetch_all(self) -> sqlx::Result<Vec<T>> {
        self.0
            .fetch_all()
            .await?
            .iter()
            .map(|row| row.try_get(0))
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod timestamp_tests {
    use super::*;

    // Captured with the previous SQLite mapping and SQLx's Chrono PostgreSQL codec.
    const LEGACY_TIMESTAMPS: [(&str, i64, &str); 4] = [
        (
            "1969-12-31T23:59:58.123456789Z",
            -1_876_544,
            "1969-12-31T23:59:58.123457Z",
        ),
        ("1969-12-31T23:59:59.999999999Z", -1, "1970-01-01T00:00:00Z"),
        (
            "1999-12-31T23:59:59.999999999Z",
            946_684_799_999_999,
            "2000-01-01T00:00:00Z",
        ),
        (
            "2026-11-20T16:00:00.120300400+08:00",
            1_795_161_600_120_300,
            "2026-11-20T08:00:00.120300Z",
        ),
    ];

    #[tokio::test]
    async fn sqlite_timestamps_match_legacy_microseconds() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        for (input, micros, _) in LEGACY_TIMESTAMPS {
            let row = query(
                Executor::SqlitePool(&pool),
                "SELECT $1 AS actual, $2 AS legacy, $3 AS absent",
            )
            .bind_timestamp(input.parse().unwrap())
            .bind(micros)
            .bind_optional_timestamp(None)
            .fetch_one()
            .await
            .unwrap();
            assert_eq!(row.try_get::<i64, _>("actual").unwrap(), micros, "{input}");
            let legacy = row.timestamp("legacy").unwrap();
            assert_eq!(legacy, Timestamp::from_microsecond(micros).unwrap());
            assert_eq!(row.optional_timestamp("actual").unwrap(), Some(legacy));
            assert_eq!(row.optional_timestamp("absent").unwrap(), None);
        }
    }

    #[tokio::test]
    #[ignore = "requires a real isolated transactional database"]
    async fn postgres_timestamps_match_legacy_microseconds() {
        let url = std::env::var("GESANG_TEST_DATABASE_URL").unwrap();
        let pool = PgPool::connect(&url).await.unwrap();
        for (input, _, expected) in LEGACY_TIMESTAMPS {
            let row = query(
                Executor::PostgresPool(&pool),
                "SELECT $1 AS actual, CAST($2::text AS TIMESTAMPTZ) AS legacy, $3 AS absent",
            )
            .bind_timestamp(input.parse().unwrap())
            .bind(expected)
            .bind_optional_timestamp(None)
            .fetch_one()
            .await
            .unwrap();
            let legacy = row.timestamp("legacy").unwrap();
            assert_eq!(legacy, expected.parse().unwrap());
            assert_eq!(
                row.optional_timestamp("actual").unwrap(),
                Some(legacy),
                "{input}"
            );
            assert_eq!(row.optional_timestamp("absent").unwrap(), None);
        }
    }
}

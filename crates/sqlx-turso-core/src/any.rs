use std::{str::FromStr, sync::OnceLock};

use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};
use sqlx_core::{
    any::{
        Any, AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult,
        AnyRow, AnyStatement, AnyTypeInfo, AnyTypeInfoKind,
        driver::{AnyDriver, install_drivers},
    },
    column::Column,
    connection::Connection,
    database::Database,
    error::Error,
    executor::Executor,
    ext::ustr::UStr,
    sql_str::SqlStr,
    transaction::TransactionManager,
    type_info::TypeInfo,
};

use crate::{
    Turso, TursoArguments, TursoColumn, TursoConnectOptions, TursoConnection, TursoQueryResult,
    TursoRow, TursoStatement, TursoTypeInfo,
};

/// SQLx `Any` driver descriptor for Turso
pub const TURSO_ANY_DRIVER: AnyDriver = any_driver();

static TURSO_ANY_DRIVER_INSTALL: OnceLock<Result<(), String>> = OnceLock::new();

#[cfg(feature = "migrate")]
const fn any_driver() -> AnyDriver {
    AnyDriver::with_migrate::<Turso>()
}

#[cfg(not(feature = "migrate"))]
const fn any_driver() -> AnyDriver {
    AnyDriver::without_migrate::<Turso>()
}

/// Installs Turso as the only SQLx `Any` driver
pub fn install_turso_any_driver() -> sqlx_core::Result<()> {
    let result = TURSO_ANY_DRIVER_INSTALL
        .get_or_init(|| install_drivers(&[TURSO_ANY_DRIVER]).map_err(|error| error.to_string()));

    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(Error::Configuration(
            format!(
                "failed to install Turso as the SQLx Any driver: {error}; \
             install mixed Any drivers once with `sqlx::any::install_drivers()` and \
             `sqlx_turso::TURSO_ANY_DRIVER`"
            )
            .into(),
        )),
    }
}

impl TryFrom<&AnyConnectOptions> for TursoConnectOptions {
    type Error = Error;

    fn try_from(options: &AnyConnectOptions) -> Result<Self, Self::Error> {
        TursoConnectOptions::from_str(options.database_url.as_str())
    }
}

impl AnyConnectionBackend for TursoConnection {
    fn name(&self) -> &str {
        Turso::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        async move { Connection::close(*self).await }.boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        async move { Connection::close_hard(*self).await }.boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn begin(&mut self, statement: Option<SqlStr>) -> BoxFuture<'_, sqlx_core::Result<()>> {
        <Turso as Database>::TransactionManager::begin(self, statement).boxed()
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        <Turso as Database>::TransactionManager::commit(self).boxed()
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        <Turso as Database>::TransactionManager::rollback(self).boxed()
    }

    fn start_rollback(&mut self) {
        <Turso as Database>::TransactionManager::start_rollback(self);
    }

    fn get_transaction_depth(&self) -> usize {
        <Turso as Database>::TransactionManager::get_transaction_depth(self)
    }

    fn cached_statements_size(&self) -> usize {
        Connection::cached_statements_size(self)
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::clear_cached_statements(self).boxed()
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self);
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxStream<'_, sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        let arguments = match arguments
            .map(AnyArguments::convert_into::<TursoArguments>)
            .transpose()
            .map_err(Error::Encode)
        {
            Ok(arguments) => arguments,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };

        stream::once(async move { self.fetch_many_sql(query, persistent, arguments).await })
            .try_flatten()
            .and_then(|item| async move {
                match item {
                    Either::Left(result) => Ok(Either::Left(AnyQueryResult::from(result))),
                    Either::Right(row) => Ok(Either::Right(AnyRow::try_from(row)?)),
                }
            })
            .boxed()
    }

    fn fetch_optional(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxFuture<'_, sqlx_core::Result<Option<AnyRow>>> {
        let arguments = arguments
            .map(AnyArguments::convert_into::<TursoArguments>)
            .transpose()
            .map_err(Error::Encode);

        async move {
            let mut stream = self.fetch_many_sql(query, persistent, arguments?).await?;

            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return AnyRow::try_from(row).map(Some);
                }
            }

            Ok(None)
        }
        .boxed()
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: SqlStr,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement>> {
        async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            AnyStatement::try_from(statement)
        }
        .boxed()
    }

    fn describe(
        &mut self,
        sql: SqlStr,
    ) -> BoxFuture<'_, sqlx_core::Result<sqlx_core::describe::Describe<Any>>> {
        async move { Executor::describe(self, sql).await?.try_into_any() }.boxed()
    }
}

impl TryFrom<TursoRow> for AnyRow {
    type Error = Error;

    fn try_from(row: TursoRow) -> Result<Self, Self::Error> {
        let column_names = row.column_names();
        AnyRow::map_from(&row, column_names)
    }
}

impl TryFrom<TursoStatement> for AnyStatement {
    type Error = Error;

    fn try_from(statement: TursoStatement) -> Result<Self, Self::Error> {
        let column_names = statement.column_names();
        AnyStatement::try_from_statement(statement, column_names)
    }
}

impl TryFrom<&TursoColumn> for AnyColumn {
    type Error = Error;

    fn try_from(column: &TursoColumn) -> Result<Self, Self::Error> {
        Ok(Self {
            ordinal: column.ordinal(),
            name: UStr::new(column.name()),
            type_info: AnyTypeInfo::try_from(column.type_info())?,
        })
    }
}

impl TryFrom<&TursoTypeInfo> for AnyTypeInfo {
    type Error = Error;

    fn try_from(type_info: &TursoTypeInfo) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: any_type_info_kind(type_info),
        })
    }
}

impl From<TursoQueryResult> for AnyQueryResult {
    fn from(result: TursoQueryResult) -> Self {
        Self {
            rows_affected: result.rows_affected(),
            last_insert_id: None,
        }
    }
}

fn any_type_info_kind(type_info: &TursoTypeInfo) -> AnyTypeInfoKind {
    if type_info.is_null() {
        AnyTypeInfoKind::Null
    } else if type_info.has_bool_affinity() {
        AnyTypeInfoKind::Bool
    } else if type_info.has_integer_affinity() {
        AnyTypeInfoKind::BigInt
    } else if type_info.has_real_affinity() {
        AnyTypeInfoKind::Double
    } else if type_info.has_blob_affinity() {
        AnyTypeInfoKind::Blob
    } else {
        AnyTypeInfoKind::Text
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, process, str::FromStr};

    use sqlx_core::{
        any::{AnyConnectOptions, AnyPoolOptions},
        connection::{ConnectOptions, Connection},
        executor::Executor,
        row::Row,
    };

    use crate::install_turso_any_driver;

    #[tokio::test]
    async fn any_connection_executes_queries() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-any-{}.db", process::id()));
        remove_database_files(&path);
        install_turso_any_driver()?;

        let url = format!("turso://{}?mode=rwc", path.display());
        let mut connection = AnyConnectOptions::from_str(&url)?.connect().await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .await?;
        sqlx_core::query::query::<sqlx_core::any::Any>("INSERT INTO test (name) VALUES (?)")
            .bind("alice")
            .execute(&mut connection)
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT name FROM test WHERE id = 1")
            .await?;
        assert_eq!(row.try_get::<String, _>("name")?, "alice");

        remove_database_files(&path);
        Ok(())
    }

    #[tokio::test]
    async fn any_driver_install_is_idempotent() -> sqlx_core::Result<()> {
        install_turso_any_driver()?;
        install_turso_any_driver()?;

        Ok(())
    }

    #[tokio::test]
    async fn any_pool_executes_and_recovers_after_error() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-any-pool-{}.db", process::id()));
        remove_database_files(&path);
        install_turso_any_driver()?;

        let url = format!("turso://{}?mode=rwc", path.display());
        let pool = AnyPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;

        pool.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let error = pool
            .execute("INSERT INTO missing_table (id) VALUES (1)")
            .await
            .expect_err("missing table insert should fail");
        assert!(error.as_database_error().is_some());

        pool.execute("INSERT INTO test (id) VALUES (1)").await?;
        let row = pool.fetch_one("SELECT COUNT(*) AS count FROM test").await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        pool.close().await;
        remove_database_files(&path);
        Ok(())
    }

    #[tokio::test]
    async fn any_transaction_commits_and_rolls_back() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-any-tx-{}.db", process::id()));
        remove_database_files(&path);
        install_turso_any_driver()?;

        let url = format!("turso://{}?mode=rwc", path.display());
        let mut connection = AnyConnectOptions::from_str(&url)?.connect().await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        transaction.commit().await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (2)")
            .await?;
        transaction.rollback().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        remove_database_files(&path);
        Ok(())
    }

    #[cfg(feature = "migrate")]
    #[tokio::test]
    async fn any_migrate_database_creates_and_drops_files() -> sqlx_core::Result<()> {
        use sqlx_core::migrate::MigrateDatabase;

        let path =
            std::env::temp_dir().join(format!("sqlx-turso-any-migrate-{}.db", process::id()));
        remove_database_files(&path);
        install_turso_any_driver()?;

        let url = format!("turso://{}?mode=rwc", path.display());
        assert!(!sqlx_core::any::Any::database_exists(&url).await?);

        sqlx_core::any::Any::create_database(&url).await?;
        assert!(sqlx_core::any::Any::database_exists(&url).await?);

        sqlx_core::any::Any::drop_database(&url).await?;
        assert!(!sqlx_core::any::Any::database_exists(&url).await?);

        remove_database_files(&path);
        Ok(())
    }

    fn remove_database_files(path: &Path) {
        let base = path.as_os_str().to_string_lossy();
        for suffix in ["", "-wal", "-shm", "-wal-tshm"] {
            let _ = std::fs::remove_file(format!("{base}{suffix}"));
        }
    }
}

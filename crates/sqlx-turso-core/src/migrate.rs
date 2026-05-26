use std::{
    fmt,
    path::Path,
    str::FromStr,
    time::{Duration, Instant},
};

use futures_core::future::BoxFuture;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    error::Error,
    executor::Executor,
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError, Migration},
    query::query,
    query_as::query_as,
    query_scalar::query_scalar,
    sql_str::AssertSqlSafe,
};

use crate::{
    Turso, TursoConnectOptions, TursoConnection, TursoDatabaseTarget,
    lifecycle::known_database_files,
};

impl MigrateDatabase for Turso {
    async fn create_database(url: &str) -> Result<(), Error> {
        let options = TursoConnectOptions::from_str(url)?.create_if_missing(true);

        if options.is_in_memory() {
            return Ok(());
        }

        options.connect().await?.close().await
    }

    async fn database_exists(url: &str) -> Result<bool, Error> {
        let options = TursoConnectOptions::from_str(url)?;

        match options.target() {
            TursoDatabaseTarget::Memory { .. } => Ok(true),
            TursoDatabaseTarget::File(path) => Ok(path.exists()),
        }
    }

    async fn drop_database(url: &str) -> Result<(), Error> {
        let options = TursoConnectOptions::from_str(url)?;

        match options.target() {
            TursoDatabaseTarget::Memory { .. } => Ok(()),
            TursoDatabaseTarget::File(path) => remove_known_database_files(path).await,
        }
    }
}

impl Migrate for TursoConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let schema_name = SqliteIdentifier::parse(schema_name)?;
            let schema_version: Option<i64> = query_scalar(AssertSqlSafe(format!(
                "PRAGMA {schema_name}.schema_version"
            )))
            .fetch_optional(&mut *self)
            .await?;

            if schema_version.is_some() {
                return Ok(());
            }

            Err(MigrateError::CreateSchemasNotSupported(format!(
                "cannot create new schema {schema_name}; creation of additional schemas in Turso requires attaching extra database files"
            )))
        })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            self.execute(AssertSqlSafe(format!(
                r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
                "#
            )))
            .await?;

            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            let row: Option<(i64,)> = query_as(AssertSqlSafe(format!(
                "SELECT version FROM {table_name} WHERE success = false ORDER BY version LIMIT 1"
            )))
            .fetch_optional(self)
            .await?;

            Ok(row.map(|row| row.0))
        })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            let rows: Vec<(i64, Vec<u8>)> = query_as(AssertSqlSafe(format!(
                "SELECT version, checksum FROM {table_name} ORDER BY version"
            )))
            .fetch_all(self)
            .await?;

            Ok(rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect())
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn apply<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            let start = Instant::now();

            let result = if migration.no_tx {
                execute_migration(self, &table_name, migration).await
            } else {
                let mut transaction = self.begin().await?;
                let result = execute_migration(&mut transaction, &table_name, migration).await;
                if result.is_ok() {
                    transaction.commit().await?;
                }

                result
            };

            if let Err(error) = result {
                insert_migration_row(self, &table_name, migration, false).await?;
                return Err(error);
            }

            let elapsed = start.elapsed();

            #[allow(clippy::cast_possible_truncation)]
            let _ = query(AssertSqlSafe(format!(
                r#"
UPDATE {table_name}
SET execution_time = ?1
WHERE version = ?2
                "#
            )))
            .bind(elapsed.as_nanos() as i64)
            .bind(migration.version)
            .execute(self)
            .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            let start = Instant::now();

            if migration.no_tx {
                revert_migration(self, &table_name, migration).await?;
            } else {
                let mut transaction = self.begin().await?;
                revert_migration(&mut transaction, &table_name, migration).await?;
                transaction.commit().await?;
            }

            Ok(start.elapsed())
        })
    }

    fn skip<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let table_name = SqliteIdentifierPath::parse(table_name)?;
            let _ = query(AssertSqlSafe(format!(
                r#"
INSERT INTO {table_name} ( version, description, success, checksum, execution_time )
VALUES ( ?1, ?2, TRUE, ?3, -1 )
                "#
            )))
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(&*migration.checksum)
            .execute(self)
            .await?;

            Ok(())
        })
    }
}

async fn remove_known_database_files(path: &Path) -> Result<(), Error> {
    for file in known_database_files(path) {
        match tokio::fs::remove_file(file).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }

    Ok(())
}

async fn execute_migration(
    conn: &mut TursoConnection,
    table_name: &SqliteIdentifierPath,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(migration.sql.clone())
        .await
        .map_err(|error| MigrateError::ExecuteMigration(error, migration.version))?;

    insert_migration_row(conn, table_name, migration, true).await?;

    Ok(())
}

async fn insert_migration_row(
    conn: &mut TursoConnection,
    table_name: &SqliteIdentifierPath,
    migration: &Migration,
    success: bool,
) -> Result<(), MigrateError> {
    let _ = query(AssertSqlSafe(format!(
        r#"
INSERT OR REPLACE INTO {table_name} ( version, description, success, checksum, execution_time )
VALUES ( ?1, ?2, ?3, ?4, -1 )
        "#
    )))
    .bind(migration.version)
    .bind(&*migration.description)
    .bind(success)
    .bind(&*migration.checksum)
    .execute(conn)
    .await?;

    Ok(())
}

async fn revert_migration(
    conn: &mut TursoConnection,
    table_name: &SqliteIdentifierPath,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(migration.sql.clone())
        .await
        .map_err(|error| MigrateError::ExecuteMigration(error, migration.version))?;

    let _ = query(AssertSqlSafe(format!(
        r#"
DELETE FROM {table_name}
WHERE version = ?1
        "#
    )))
    .bind(migration.version)
    .execute(conn)
    .await?;

    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct SqliteIdentifier(String);

impl SqliteIdentifier {
    fn parse(value: &str) -> Result<Self, MigrateError> {
        let (identifier, rest) = Self::parse_prefix(value, value)?;
        if !rest.is_empty() {
            return Err(invalid_identifier(value));
        }

        Ok(identifier)
    }

    fn parse_prefix<'a>(value: &'a str, original: &str) -> Result<(Self, &'a str), MigrateError> {
        if let Some(quoted) = value.strip_prefix('"') {
            return Self::parse_quoted_prefix(quoted, original);
        }

        Self::parse_bare_prefix(value, original)
    }

    fn parse_bare_prefix<'a>(
        value: &'a str,
        original: &str,
    ) -> Result<(Self, &'a str), MigrateError> {
        let mut chars = value.char_indices().peekable();
        let Some((_, first)) = chars.next() else {
            return Err(invalid_identifier(original));
        };

        if !is_identifier_start(first) {
            return Err(invalid_identifier(original));
        }

        let mut end = first.len_utf8();
        while let Some((index, ch)) = chars.peek().copied() {
            if !is_identifier_continue(ch) {
                break;
            }

            end = index + ch.len_utf8();
            let _ = chars.next();
        }

        Ok((Self(value[..end].to_owned()), &value[end..]))
    }

    fn parse_quoted_prefix<'a>(
        value: &'a str,
        original: &str,
    ) -> Result<(Self, &'a str), MigrateError> {
        let mut identifier = String::new();
        let mut chars = value.char_indices().peekable();

        while let Some((index, ch)) = chars.next() {
            if ch != '"' {
                identifier.push(ch);
                continue;
            }

            if chars.peek().is_some_and(|(_, next)| *next == '"') {
                identifier.push('"');
                let _ = chars.next();
                continue;
            }

            if identifier.is_empty() {
                return Err(invalid_identifier(original));
            }

            let rest_start = index + ch.len_utf8();
            return Ok((Self(identifier), &value[rest_start..]));
        }

        Err(invalid_identifier(original))
    }

    fn escaped(&self) -> String {
        self.0.replace('"', "\"\"")
    }
}

impl fmt::Display for SqliteIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.escaped())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct SqliteIdentifierPath(Vec<SqliteIdentifier>);

impl SqliteIdentifierPath {
    fn parse(value: &str) -> Result<Self, MigrateError> {
        let (first, mut rest) = SqliteIdentifier::parse_prefix(value, value)?;
        let mut parts = vec![first];

        while let Some(next) = rest.strip_prefix('.') {
            if parts.len() == 2 {
                return Err(invalid_identifier(value));
            }

            let (part, remaining) = SqliteIdentifier::parse_prefix(next, value)?;
            parts.push(part);
            rest = remaining;
        }

        if !rest.is_empty() {
            return Err(invalid_identifier(value));
        }

        Ok(Self(parts))
    }
}

impl fmt::Display for SqliteIdentifierPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = self.0.iter();
        if let Some(first) = parts.next() {
            write!(f, "{first}")?;
        }

        for part in parts {
            write!(f, ".{part}")?;
        }

        Ok(())
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_identifier_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn invalid_identifier(value: &str) -> MigrateError {
    MigrateError::Execute(Error::Configuration(
        format!("invalid SQLite identifier `{value}`").into(),
    ))
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, process};

    use sqlx_core::{
        connection::ConnectOptions,
        migrate::{Migrate, MigrateDatabase, Migration, MigrationType},
        query_scalar::query_scalar,
        sql_str::{AssertSqlSafe, SqlSafeStr},
    };

    use crate::{
        Turso, TursoConnectOptions, lifecycle::known_database_files,
        migrate::remove_known_database_files,
    };

    const MIGRATIONS_TABLE: &str = "_sqlx_migrations";

    #[tokio::test]
    async fn creates_checks_and_drops_file_database_with_known_sidecars() -> sqlx_core::Result<()> {
        let path = temp_database_path("lifecycle");
        remove_known_database_files(&path).await?;

        let url = database_url(&path);
        assert!(!Turso::database_exists(&url).await?);

        Turso::create_database(&url).await?;
        assert!(Turso::database_exists(&url).await?);

        for file in known_database_files(&path) {
            tokio::fs::write(file, b"sidecar").await?;
        }

        let unrelated = path.with_file_name(format!(
            "{}-not-known",
            path.file_name().unwrap().to_string_lossy()
        ));
        tokio::fs::write(&unrelated, b"keep").await?;

        Turso::drop_database(&url).await?;

        for file in known_database_files(&path) {
            assert!(!file.exists());
        }
        assert!(unrelated.exists());

        tokio::fs::remove_file(unrelated).await?;
        Ok(())
    }

    #[tokio::test]
    async fn applies_lists_and_reverts_migrations() -> sqlx_core::Result<()> {
        let mut conn = TursoConnectOptions::new().connect().await?;
        conn.ensure_migrations_table(MIGRATIONS_TABLE).await?;

        let up = migration(
            1,
            "create widgets",
            MigrationType::ReversibleUp,
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL); INSERT INTO widgets(name) VALUES ('one')",
        );
        let down = migration(
            1,
            "create widgets",
            MigrationType::ReversibleDown,
            "DROP TABLE widgets",
        );

        conn.apply(MIGRATIONS_TABLE, &up).await?;

        let applied = conn.list_applied_migrations(MIGRATIONS_TABLE).await?;
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].version, 1);

        let count: i64 = query_scalar("SELECT COUNT(*) FROM widgets")
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(count, 1);

        conn.revert(MIGRATIONS_TABLE, &down).await?;
        assert!(
            conn.list_applied_migrations(MIGRATIONS_TABLE)
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn supports_quoted_schema_qualified_migration_table_name() -> sqlx_core::Result<()> {
        let mut conn = TursoConnectOptions::new().connect().await?;
        let migrations_table = r#""main"."sqlx migrations""#;
        conn.ensure_migrations_table(migrations_table).await?;

        let up = migration(
            10,
            "create quoted widgets",
            MigrationType::ReversibleUp,
            "CREATE TABLE quoted_widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL); INSERT INTO quoted_widgets(name) VALUES ('one')",
        );
        let down = migration(
            10,
            "create quoted widgets",
            MigrationType::ReversibleDown,
            "DROP TABLE quoted_widgets",
        );

        conn.apply(migrations_table, &up).await?;

        let applied = conn.list_applied_migrations(migrations_table).await?;
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].version, 10);

        let count: i64 = query_scalar("SELECT COUNT(*) FROM quoted_widgets")
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(count, 1);

        conn.revert(migrations_table, &down).await?;
        assert!(
            conn.list_applied_migrations(migrations_table)
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn records_dirty_migration_on_failure() -> sqlx_core::Result<()> {
        let mut conn = TursoConnectOptions::new().connect().await?;
        conn.ensure_migrations_table(MIGRATIONS_TABLE).await?;

        let bad = migration(
            2,
            "bad migration",
            MigrationType::Simple,
            "CREATE TABLE broken(id INTEGER PRIMARY KEY); SELECT missing FROM broken",
        );

        let error = conn.apply(MIGRATIONS_TABLE, &bad).await.unwrap_err();
        assert!(matches!(
            error,
            sqlx_core::migrate::MigrateError::ExecuteMigration(_, 2)
        ));
        assert_eq!(conn.dirty_version(MIGRATIONS_TABLE).await?, Some(2));

        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_migration_table_identifier() -> sqlx_core::Result<()> {
        let mut conn = TursoConnectOptions::new().connect().await?;

        let error = conn
            .ensure_migrations_table("_sqlx_migrations; DROP TABLE users")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("invalid SQLite identifier"));

        Ok(())
    }

    #[test]
    fn parses_and_escapes_quoted_identifier_paths() {
        let path = super::SqliteIdentifierPath::parse(r#""main"."migration ""audit""""#)
            .expect("quoted identifier path should parse");

        assert_eq!(path.to_string(), r#""main"."migration ""audit""""#);
    }

    fn migration(
        version: i64,
        description: &'static str,
        migration_type: MigrationType,
        sql: &'static str,
    ) -> Migration {
        Migration::new(
            version,
            Cow::Borrowed(description),
            migration_type,
            AssertSqlSafe(sql).into_sql_str(),
            false,
        )
    }

    fn temp_database_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("sqlx-turso-migrate-{name}-{}.db", process::id()))
    }

    fn database_url(path: &std::path::Path) -> String {
        format!("turso://{}?mode=rwc", path.display())
    }
}

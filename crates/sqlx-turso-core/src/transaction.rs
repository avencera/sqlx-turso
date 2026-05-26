use sqlx_core::{
    error::Error,
    sql_str::SqlStr,
    transaction::{
        TransactionManager, begin_ansi_transaction_sql, commit_ansi_transaction_sql,
        rollback_ansi_transaction_sql,
    },
};

use crate::{Turso, connection::TursoConnection, executor::map_turso_error};

/// SQLx-compatible transaction handle for Turso connections
pub type TursoTransaction<'c> = sqlx_core::transaction::Transaction<'c, Turso>;

/// SQLx transaction manager for Turso connections
#[derive(Debug)]
pub struct TursoTransactionManager;

impl TransactionManager for TursoTransactionManager {
    type Database = Turso;

    async fn begin(conn: &mut TursoConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        conn.clear_pending_rollback().await?;

        if statement.is_some() && conn.transaction_depth() > 0 {
            return Err(Error::InvalidSavePointStatement);
        }

        let sql = statement.unwrap_or_else(|| begin_ansi_transaction_sql(conn.transaction_depth()));
        conn.raw()
            .execute(sql.as_str(), ())
            .await
            .map_err(map_turso_error)?;
        conn.increment_transaction_depth();
        Ok(())
    }

    async fn commit(conn: &mut TursoConnection) -> Result<(), Error> {
        conn.clear_pending_rollback().await?;

        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }

        let sql = commit_ansi_transaction_sql(depth);
        conn.raw()
            .execute(sql.as_str(), ())
            .await
            .map_err(map_turso_error)?;
        conn.decrement_transaction_depth();
        Ok(())
    }

    async fn rollback(conn: &mut TursoConnection) -> Result<(), Error> {
        conn.clear_pending_rollback().await?;

        let depth = conn.transaction_depth();
        if depth == 0 {
            return Ok(());
        }

        let sql = rollback_ansi_transaction_sql(depth);
        conn.raw()
            .execute(sql.as_str(), ())
            .await
            .map_err(map_turso_error)?;
        conn.decrement_transaction_depth();
        Ok(())
    }

    fn start_rollback(conn: &mut TursoConnection) {
        conn.mark_rollback_needed();
    }

    fn get_transaction_depth(conn: &TursoConnection) -> usize {
        conn.transaction_depth()
    }
}

#[cfg(test)]
mod tests {
    use sqlx_core::{
        connection::{ConnectOptions, Connection},
        executor::Executor,
        row::Row,
    };

    use crate::TursoConnectOptions;

    #[tokio::test]
    async fn commits_top_level_transaction() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        transaction.commit().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn rolls_back_top_level_transaction() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        transaction.rollback().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn supports_nested_savepoint_rollback() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let mut nested = transaction.begin().await?;
        (&mut *nested)
            .execute("INSERT INTO test (id) VALUES (2)")
            .await?;
        nested.rollback().await?;

        transaction.commit().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn supports_nested_savepoint_commit() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let mut nested = transaction.begin().await?;
        (&mut *nested)
            .execute("INSERT INTO test (id) VALUES (2)")
            .await?;
        nested.commit().await?;

        transaction.commit().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn supports_custom_top_level_begin() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        transaction.commit().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_custom_begin_inside_nested_transaction() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        let mut transaction = connection.begin().await?;

        let error = transaction
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect_err("custom nested begin should be rejected");
        assert!(matches!(
            error,
            sqlx_core::error::Error::InvalidSavePointStatement
        ));

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn dropped_transaction_rolls_back_on_next_use() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        {
            let mut transaction = connection.begin().await?;
            (&mut *transaction)
                .execute("INSERT INTO test (id) VALUES (1)")
                .await?;
        }

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn dropped_nested_transaction_rolls_back_to_savepoint() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin().await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        {
            let mut nested = transaction.begin().await?;
            (&mut *nested)
                .execute("INSERT INTO test (id) VALUES (2)")
                .await?;
        }

        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (3)")
            .await?;
        transaction.commit().await?;

        let rows = (&mut connection)
            .fetch_all("SELECT id FROM test ORDER BY id")
            .await?;
        let ids = rows
            .iter()
            .map(|row| row.try_get::<i64, _>("id"))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ids, [1, 3]);

        Ok(())
    }

    #[tokio::test]
    async fn dropped_nested_and_outer_transactions_roll_back_all() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        {
            let mut transaction = connection.begin().await?;
            (&mut *transaction)
                .execute("INSERT INTO test (id) VALUES (1)")
                .await?;

            {
                let mut nested = transaction.begin().await?;
                (&mut *nested)
                    .execute("INSERT INTO test (id) VALUES (2)")
                    .await?;
            }
        }

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn supports_begin_concurrent_transaction() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "sqlx-turso-begin-concurrent-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .mvcc(true)
            .connect()
            .await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut transaction = connection.begin_with("BEGIN CONCURRENT").await?;
        (&mut *transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        transaction.commit().await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        drop(connection);
        let _ = std::fs::remove_file(&path);

        Ok(())
    }

    #[tokio::test]
    async fn begin_concurrent_conflict_rolls_back_and_keeps_connection_usable()
    -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "sqlx-turso-begin-concurrent-conflict-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let options = TursoConnectOptions::new().filename(&path).mvcc(true);
        let mut first = options.clone().connect().await?;
        let mut second = options.connect().await?;

        (&mut first)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        let mut first_transaction = first.begin_with("BEGIN CONCURRENT").await?;
        (&mut *first_transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let mut second_transaction = second.begin_with("BEGIN CONCURRENT").await?;
        (&mut *second_transaction)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        first_transaction.commit().await?;
        let _error = second_transaction
            .commit()
            .await
            .expect_err("second concurrent primary-key commit should conflict");

        let row = (&mut second)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        drop(first);
        drop(second);
        let _ = std::fs::remove_file(&path);

        Ok(())
    }

    #[tokio::test]
    async fn rollback_cleanup_failure_marks_connection_unusable() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;
        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;

        {
            let mut transaction = connection.begin().await?;
            let mut nested = transaction.begin().await?;
            (&mut *nested)
                .execute("INSERT INTO test (id) VALUES (1)")
                .await?;
        }

        connection
            .raw()
            .execute("ROLLBACK", ())
            .await
            .map_err(sqlx_core::error::Error::config)?;

        let error = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await
            .expect_err("scheduled nested rollback should fail after raw rollback");
        assert!(matches!(error, sqlx_core::error::Error::Configuration(_)));

        let error = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await
            .expect_err("connection should be marked unusable");
        assert!(matches!(error, sqlx_core::error::Error::WorkerCrashed));

        Ok(())
    }
}

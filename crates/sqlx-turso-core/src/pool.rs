use sqlx_core::{executor::Executor, pool::Pool};

/// SQLx-compatible pool options for Turso connections
pub type TursoPoolOptions = sqlx_core::pool::PoolOptions<crate::Turso>;

/// SQLx-compatible pool handle for Turso connections
pub type TursoPool = Pool<crate::Turso>;

/// Executor facade for Turso SQLx operations
pub trait TursoExecutor<'c>: Executor<'c, Database = crate::Turso> {}
impl<'c, T> TursoExecutor<'c> for T where T: Executor<'c, Database = crate::Turso> {}

#[cfg(test)]
mod tests {
    use sqlx_core::{executor::Executor, row::Row};

    use crate::{TursoConnectOptions, TursoEncryptionOptions, TursoPoolOptions};

    #[tokio::test]
    async fn pool_acquires_and_releases_file_connections() -> sqlx_core::Result<()> {
        let path = temp_database_path("pool-file");
        remove_database_sidecars(&path);

        let pool = TursoPoolOptions::new()
            .max_connections(2)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .busy_timeout(std::time::Duration::from_millis(50)),
            )
            .await?;

        let mut connection = pool.acquire().await?;
        (&mut *connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        drop(connection);

        let mut connection = pool.acquire().await?;
        (&mut *connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;
        let row = (&mut *connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);
        drop(connection);

        pool.close().await;
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn pool_shares_in_memory_state() -> sqlx_core::Result<()> {
        let pool = TursoPoolOptions::new()
            .max_connections(2)
            .connect_with(TursoConnectOptions::new())
            .await?;

        let mut first = pool.acquire().await?;
        let mut second = pool.acquire().await?;

        (&mut *first)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut *first)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let row = (&mut *second)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);
        drop(first);
        drop(second);

        pool.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn dropped_transaction_rolls_back_before_pool_reuse() -> sqlx_core::Result<()> {
        let pool = TursoPoolOptions::new()
            .max_connections(1)
            .connect_with(TursoConnectOptions::new())
            .await?;

        {
            let mut connection = pool.acquire().await?;
            (&mut *connection)
                .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
                .await?;
        }

        {
            let mut transaction = pool.begin().await?;
            (&mut *transaction)
                .execute("INSERT INTO test (id) VALUES (1)")
                .await?;
        }

        let mut connection = pool.acquire().await?;
        let row = (&mut *connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 0);
        drop(connection);

        pool.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn pool_supports_concurrent_readers() -> sqlx_core::Result<()> {
        let path = temp_database_path("pool-readers");
        remove_database_sidecars(&path);

        let pool = TursoPoolOptions::new()
            .max_connections(4)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;

        let mut connection = pool.acquire().await?;
        (&mut *connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut *connection)
            .execute("INSERT INTO test (id) VALUES (1), (2), (3)")
            .await?;
        drop(connection);

        let first_pool = pool.clone();
        let second_pool = pool.clone();
        let first = tokio::spawn(async move { count_rows(first_pool).await });
        let second = tokio::spawn(async move { count_rows(second_pool).await });

        assert_eq!(first.await.expect("reader task panicked")?, 3);
        assert_eq!(second.await.expect("reader task panicked")?, 3);

        pool.close().await;
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn pool_surfaces_native_write_contention() -> sqlx_core::Result<()> {
        let path = temp_database_path("pool-contention");
        remove_database_sidecars(&path);

        let pool = TursoPoolOptions::new()
            .max_connections(2)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;

        let mut first = pool.acquire().await?;
        let mut second = pool.acquire().await?;

        (&mut *first)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut *first).execute("BEGIN IMMEDIATE").await?;
        (&mut *first)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let error = (&mut *second)
            .execute("INSERT INTO test (id) VALUES (2)")
            .await
            .expect_err("second writer should see native contention");
        assert!(
            error.to_string().contains("database is locked")
                || error.to_string().contains("busy")
                || error.to_string().contains("locked")
        );

        (&mut *first).execute("ROLLBACK").await?;
        drop(first);
        drop(second);

        pool.close().await;
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn pool_supports_encrypted_files() -> sqlx_core::Result<()> {
        let path = temp_database_path("pool-encrypted");
        remove_database_sidecars(&path);

        let pool = TursoPoolOptions::new()
            .max_connections(2)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .encryption_options(TursoEncryptionOptions::new(
                        "aegis256",
                        "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
                    )?),
            )
            .await?;

        let mut connection = pool.acquire().await?;
        (&mut *connection)
            .execute("CREATE TABLE test (value TEXT)")
            .await?;
        (&mut *connection)
            .execute("INSERT INTO test (value) VALUES ('secret')")
            .await?;
        let row = (&mut *connection)
            .fetch_one("SELECT value FROM test")
            .await?;
        assert_eq!(row.try_get::<String, _>("value")?, "secret");
        drop(connection);

        pool.close().await;
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn pool_supports_mvcc_connections() -> sqlx_core::Result<()> {
        let pool = TursoPoolOptions::new()
            .max_connections(2)
            .connect_with(TursoConnectOptions::new().mvcc(true))
            .await?;

        let mut connection = pool.acquire().await?;
        let row = (&mut *connection).fetch_one("PRAGMA journal_mode").await?;
        assert_eq!(row.try_get::<String, _>(0)?.to_ascii_lowercase(), "mvcc");
        drop(connection);

        pool.close().await;

        Ok(())
    }

    #[cfg(feature = "sync")]
    #[tokio::test]
    async fn pool_supports_sync_backed_connections_without_bootstrap() -> sqlx_core::Result<()> {
        let path = temp_database_path("pool-sync");
        remove_database_sidecars(&path);

        let pool = TursoPoolOptions::new()
            .max_connections(1)
            .connect_with(
                TursoConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .with_sync_options(
                        crate::TursoSyncOptions::new("http://127.0.0.1:9")
                            .with_bootstrap_if_empty(false),
                    ),
            )
            .await?;

        let mut connection = pool.acquire().await?;
        (&mut *connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        let _stats = connection.sync_stats().await?;
        drop(connection);

        pool.close().await;
        remove_database_sidecars(&path);

        Ok(())
    }

    async fn count_rows(pool: crate::TursoPool) -> sqlx_core::Result<i64> {
        let mut connection = pool.acquire().await?;
        let row = (&mut *connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        row.try_get::<i64, _>("count")
    }

    fn temp_database_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{name}-{}.db", std::process::id()))
    }

    fn remove_database_sidecars(path: &std::path::Path) {
        let path = path.to_string_lossy();
        for suffix in ["", "-wal", "-shm", "-wal-tshm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
}

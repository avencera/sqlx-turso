#[cfg(test)]
mod tests {
    use sqlx_core::{
        connection::ConnectOptions, executor::Executor, query::query, row::Row,
        sql_str::AssertSqlSafe,
    };

    #[cfg(feature = "sync")]
    use crate::TursoSyncOptions;
    use crate::{Turso, TursoConnectOptions, TursoExperimentalFeature};

    #[tokio::test]
    async fn supports_generated_columns_when_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::GeneratedColumns, true)
            .connect()
            .await?;

        (&mut connection)
            .execute(
                "CREATE TABLE test (\
                 value INTEGER, \
                 doubled INTEGER GENERATED ALWAYS AS (value * 2) VIRTUAL\
                 )",
            )
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (value) VALUES (21)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT doubled FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("doubled")?, 42);

        Ok(())
    }

    #[tokio::test]
    async fn supports_materialized_views_when_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::MaterializedViews, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (id, value) VALUES (1, 'alpha')")
            .await?;
        (&mut connection)
            .execute("CREATE MATERIALIZED VIEW test_view AS SELECT id, value FROM test")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT value FROM test_view WHERE id = 1")
            .await?;
        assert_eq!(row.try_get::<String, _>("value")?, "alpha");

        Ok(())
    }

    #[tokio::test]
    async fn supports_attach_when_enabled() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-attached-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::Attach, true)
            .connect()
            .await?;

        let attach_sql = format!("ATTACH DATABASE '{}' AS attached", path.display());
        query::<Turso>(AssertSqlSafe(attach_sql))
            .execute(&mut connection)
            .await?;
        (&mut connection)
            .execute("CREATE TABLE attached.test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO attached.test (id) VALUES (1)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM attached.test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        let _ = (&mut connection).execute("DETACH DATABASE attached").await;
        drop(connection);
        let _ = std::fs::remove_file(&path);

        Ok(())
    }

    #[tokio::test]
    async fn supports_custom_types_when_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::CustomTypes, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TYPE user_id BASE integer")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT name FROM sqlite_turso_types WHERE name = 'user_id'")
            .await?;
        assert_eq!(row.try_get::<String, _>("name")?, "user_id");

        Ok(())
    }

    #[tokio::test]
    async fn supports_domains_when_custom_types_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::CustomTypes, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT name FROM sqlite_turso_types WHERE name = 'positive_int'")
            .await?;
        assert_eq!(row.try_get::<String, _>("name")?, "positive_int");

        Ok(())
    }

    #[tokio::test]
    async fn supports_without_rowid_when_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::WithoutRowid, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (code TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (code, value) VALUES ('a', 'alpha')")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT value FROM test WHERE code = 'a'")
            .await?;
        assert_eq!(row.try_get::<String, _>("value")?, "alpha");

        Ok(())
    }

    #[tokio::test]
    async fn supports_vacuum_when_enabled() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-vacuum-{}.db", std::process::id()));
        remove_database_sidecars(&path);

        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .experimental_feature(TursoExperimentalFeature::Vacuum, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection).execute("VACUUM").await?;

        drop(connection);
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn rejects_autovacuum_until_builder_exposes_flag() {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-autovacuum-{}.db", std::process::id()));
        remove_database_sidecars(&path);

        let error = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .pragma("auto_vacuum", Some("FULL".to_owned()))
            .experimental_feature(TursoExperimentalFeature::Vacuum, true)
            .connect()
            .await
            .expect_err("autovacuum should stay blocked until pinned Turso exposes the flag");
        assert!(
            error
                .to_string()
                .contains("Autovacuum is not enabled. Use --experimental-autovacuum flag")
        );

        remove_database_sidecars(&path);
    }

    #[tokio::test]
    async fn supports_named_vfs_when_configured() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-vfs-{}.db", std::process::id()));
        remove_database_sidecars(&path);

        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .vfs("memory")
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        drop(connection);
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn supports_custom_io_when_configured() -> sqlx_core::Result<()> {
        let path =
            std::env::temp_dir().join(format!("sqlx-turso-custom-io-{}.db", std::process::id()));
        remove_database_sidecars(&path);

        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .custom_io(std::sync::Arc::new(turso::core::MemoryIO::new()))
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        drop(connection);
        remove_database_sidecars(&path);

        Ok(())
    }

    #[tokio::test]
    async fn supports_multiprocess_wal_when_enabled() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "sqlx-turso-multiprocess-wal-{}.db",
            std::process::id()
        ));
        remove_database_sidecars(&path);

        let options = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .experimental_feature(TursoExperimentalFeature::MultiprocessWal, true);
        let mut first = options.clone().connect().await?;
        let mut second = options.connect().await?;

        (&mut first)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut first)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let row = (&mut second)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);

        drop(first);
        drop(second);
        remove_database_sidecars(&path);

        Ok(())
    }

    #[cfg(feature = "sync")]
    #[tokio::test]
    async fn supports_sync_backed_local_execution_without_bootstrap() -> sqlx_core::Result<()> {
        let path = std::env::temp_dir().join(format!("sqlx-turso-sync-{}.db", std::process::id()));
        remove_database_sidecars(&path);

        let mut connection = TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .with_sync_options(
                TursoSyncOptions::new("http://127.0.0.1:9")
                    .with_bootstrap_if_empty(false)
                    .with_client_name("sqlx-turso-test"),
            )
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO test (id) VALUES (1)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT COUNT(*) AS count FROM test")
            .await?;
        assert_eq!(row.try_get::<i64, _>("count")?, 1);
        let _stats = connection.sync_stats().await?;
        connection.sync_checkpoint().await?;

        drop(connection);
        remove_database_sidecars(&path);

        Ok(())
    }

    #[cfg(feature = "sync")]
    #[tokio::test]
    async fn rejects_sync_custom_io_until_builder_exposes_hook() {
        let path = std::env::temp_dir().join(format!(
            "sqlx-turso-sync-custom-io-{}.db",
            std::process::id()
        ));
        remove_database_sidecars(&path);

        let error = TursoConnectOptions::new()
            .filename(&path)
            .custom_io(std::sync::Arc::new(turso::core::MemoryIO::new()))
            .with_sync_options(
                TursoSyncOptions::new("http://127.0.0.1:9").with_bootstrap_if_empty(false),
            )
            .connect()
            .await
            .expect_err("sync builder should reject custom IO until pinned Turso exposes a hook");
        assert!(
            error
                .to_string()
                .contains("custom IO Turso sync connections")
        );

        remove_database_sidecars(&path);
    }

    #[tokio::test]
    async fn supports_vector_functions_through_blob_values() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new().connect().await?;

        let row = (&mut connection)
            .fetch_one("SELECT vector_extract(vector32('[1.0, 2.0]')) AS vector")
            .await?;
        let value = row.try_get::<String, _>("vector")?;
        assert!(value.contains("1"));
        assert!(value.contains("2"));

        Ok(())
    }

    #[cfg(feature = "fts")]
    #[tokio::test]
    async fn supports_fts_index_methods_when_feature_enabled() -> sqlx_core::Result<()> {
        let mut connection = TursoConnectOptions::new()
            .experimental_feature(TursoExperimentalFeature::IndexMethod, true)
            .connect()
            .await?;

        (&mut connection)
            .execute("CREATE TABLE users (email TEXT, age INTEGER)")
            .await?;
        (&mut connection)
            .execute("CREATE INDEX fts_users_email ON users USING fts (email)")
            .await?;
        (&mut connection)
            .execute("INSERT INTO users (email, age) VALUES ('foo@example.com', 21)")
            .await?;

        let row = (&mut connection)
            .fetch_one("SELECT age FROM users WHERE email MATCH 'foo@example.com'")
            .await?;
        assert_eq!(row.try_get::<i64, _>("age")?, 21);

        Ok(())
    }

    fn remove_database_sidecars(path: &std::path::Path) {
        let path = path.to_string_lossy();
        for suffix in ["", "-wal", "-shm", "-wal-tshm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }
}

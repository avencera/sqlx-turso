use sqlx_turso::{
    TursoConnectOptions, TursoSyncOptions,
    sqlx::{ConnectOptions, Executor},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let sync = TursoSyncOptions::new("http://127.0.0.1:1")
        .with_client_name("sqlx-turso-example")
        .with_bootstrap_if_empty(false);

    let mut conn = TursoConnectOptions::new()
        .filename("/tmp/sqlx-turso-sync-example.db")
        .with_sync_options(sync)
        .connect()
        .await?;

    conn.execute("CREATE TABLE IF NOT EXISTS local_items (id INTEGER PRIMARY KEY)")
        .await?;
    let _stats = conn.sync_stats().await?;

    Ok(())
}

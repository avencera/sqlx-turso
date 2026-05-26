use sqlx_turso::{
    TursoConnectOptions, TursoEncryptionOptions,
    sqlx::{ConnectOptions, Executor},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let path = std::env::temp_dir().join(format!(
        "sqlx-turso-encrypted-example-{}.db",
        std::process::id()
    ));

    let mut conn = TursoConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .encryption_options(TursoEncryptionOptions::new("aegis256", "0011223344556677")?)
        .mvcc(true)
        .connect()
        .await?;

    conn.execute("CREATE TABLE IF NOT EXISTS secrets (id INTEGER PRIMARY KEY, value TEXT)")
        .await?;

    Ok(())
}

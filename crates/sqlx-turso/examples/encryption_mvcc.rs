use sqlx_turso::{
    TursoConnectOptions, TursoEncryptionOptions,
    sqlx::{ConnectOptions, Executor},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let mut conn = TursoConnectOptions::new()
        .filename("/tmp/sqlx-turso-encrypted-example.db")
        .encryption_options(TursoEncryptionOptions::new("aegis256", "0011223344556677")?)
        .mvcc(true)
        .connect()
        .await?;

    conn.execute("CREATE TABLE IF NOT EXISTS secrets (id INTEGER PRIMARY KEY, value TEXT)")
        .await?;

    Ok(())
}

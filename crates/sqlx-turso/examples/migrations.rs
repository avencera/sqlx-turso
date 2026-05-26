use sqlx_turso::{MigrateDatabase, Turso, TursoConnection, sqlx::Connection};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let url = "turso:///tmp/sqlx-turso-migrations-example.db";

    if !Turso::database_exists(url).await.unwrap_or(false) {
        Turso::create_database(url).await?;
    }

    let _conn = TursoConnection::connect(url).await?;

    Ok(())
}

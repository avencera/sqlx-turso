use sqlx_turso::{MigrateDatabase, Turso, TursoConnection, sqlx::Connection};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let path = std::env::temp_dir().join(format!(
        "sqlx-turso-migrations-example-{}.db",
        std::process::id()
    ));
    let url = format!("turso://{}?mode=rwc", path.display());

    if !Turso::database_exists(&url).await? {
        Turso::create_database(&url).await?;
    }

    let _conn = TursoConnection::connect(&url).await?;

    Ok(())
}

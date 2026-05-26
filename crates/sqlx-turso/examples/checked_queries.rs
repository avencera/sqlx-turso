use sqlx_turso::{TursoConnection, sqlx::Connection};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let mut conn = TursoConnection::connect("turso::memory:").await?;

    let row = sqlx_turso::query!("SELECT 1 AS \"id!: i64\"")
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(row.id, 1);

    Ok(())
}

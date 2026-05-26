use sqlx_turso::{TursoConnection, sqlx::Connection};

#[tokio::test]
async fn online_literal_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = TursoConnection::connect("turso::memory:").await?;

    let row = sqlx_turso::query!("SELECT 1 AS \"id!: i64\"")
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(row.id, 1);

    Ok(())
}

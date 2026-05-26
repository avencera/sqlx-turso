use sqlx_turso::{TursoPoolOptions, sqlx::Row};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let pool = TursoPoolOptions::new()
        .max_connections(4)
        .connect("turso::memory:")
        .await?;

    sqlx_turso::sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .execute(&pool)
        .await?;
    sqlx_turso::sqlx::query("INSERT INTO users (id, name) VALUES (?, ?)")
        .bind(1_i64)
        .bind("alice")
        .execute(&pool)
        .await?;

    let row = sqlx_turso::sqlx::query("SELECT name FROM users WHERE id = ?")
        .bind(1_i64)
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.try_get::<String, _>("name")?, "alice");

    Ok(())
}

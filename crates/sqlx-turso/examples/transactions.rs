use sqlx_turso::{
    TursoConnection,
    sqlx::{Connection, Executor, Row},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let mut conn = TursoConnection::connect("turso::memory:").await?;

    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await?;

    let mut tx = conn.begin().await?;
    sqlx_turso::sqlx::query("INSERT INTO users (id, name) VALUES (?, ?)")
        .bind(1_i64)
        .bind("alice")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    let row = conn
        .fetch_one("SELECT COUNT(*) AS count FROM users")
        .await?;
    assert_eq!(row.try_get::<i64, _>("count")?, 1);

    Ok(())
}

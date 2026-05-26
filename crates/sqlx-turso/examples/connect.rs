use sqlx_turso::{
    TursoConnection,
    sqlx::{Connection, Executor, Row},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> sqlx_turso::sqlx::Result<()> {
    let mut conn = TursoConnection::connect("turso::memory:").await?;

    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await?;
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
        .await?;

    let row = conn
        .fetch_one("SELECT name FROM users WHERE id = 1")
        .await?;
    assert_eq!(row.try_get::<String, _>("name")?, "alice");

    Ok(())
}

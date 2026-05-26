use sqlx_turso::{
    TursoConnection,
    sqlx::{Connection, Executor},
};

#[derive(Debug)]
struct MacroUser {
    id: i64,
    name: String,
}

async fn setup() -> sqlx_turso::sqlx::Result<TursoConnection> {
    let mut connection = TursoConnection::connect("turso::memory:").await?;

    connection
        .execute("CREATE TABLE macro_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .await?;
    connection
        .execute("INSERT INTO macro_users (id, name) VALUES (1, 'alice')")
        .await?;

    Ok(connection)
}

#[tokio::test]
async fn query_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let row = sqlx_turso::query!(
        "SELECT id AS \"id!: i64\", name AS \"name!\" FROM macro_users WHERE id = ?",
        1_i64
    )
    .fetch_one(&mut connection)
    .await?;

    assert_eq!(row.id, 1);
    assert_eq!(row.name, "alice");

    Ok(())
}

#[tokio::test]
async fn query_as_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let row = sqlx_turso::query_as!(
        MacroUser,
        "SELECT id AS \"id!: i64\", name AS \"name!\" FROM macro_users WHERE id = ?",
        1_i64
    )
    .fetch_one(&mut connection)
    .await?;

    assert_eq!(row.id, 1);
    assert_eq!(row.name, "alice");

    Ok(())
}

#[tokio::test]
async fn query_scalar_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let name = sqlx_turso::query_scalar!(
        "SELECT name AS \"name!\" FROM macro_users WHERE id = ?",
        1_i64
    )
    .fetch_one(&mut connection)
    .await?;

    assert_eq!(name, "alice");

    Ok(())
}

#[tokio::test]
async fn query_file_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let row = sqlx_turso::query_file!("tests/macro_user.sql", 1_i64)
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(row.id, 1);
    assert_eq!(row.name, "alice");

    Ok(())
}

#[tokio::test]
async fn query_file_as_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let row = sqlx_turso::query_file_as!(MacroUser, "tests/macro_user.sql", 1_i64)
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(row.id, 1);
    assert_eq!(row.name, "alice");

    Ok(())
}

#[tokio::test]
async fn online_literal_macro_checks_and_fetches_rows() -> sqlx_turso::sqlx::Result<()> {
    let mut connection = setup().await?;

    let row = sqlx_turso::query!("SELECT 1 AS \"id!: i64\"")
        .fetch_one(&mut connection)
        .await?;

    assert_eq!(row.id, 1);

    Ok(())
}

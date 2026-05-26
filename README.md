# sqlx-turso

`sqlx-turso` is a SQLx adapter for the Rust `turso` database engine. It exposes a distinct `Turso` SQLx database type, a small public facade crate, first-party checked query macros, migration support, pool aliases, and honest feature gates for Turso-specific behavior.

## Crates

- `sqlx-turso`: public facade and application-facing imports
- `sqlx-turso-core`: SQLx runtime implementation, options, values, rows, pools, transactions, migrations, and macro metadata hooks
- `sqlx-turso-macros`: `sqlx_turso::query!` macro family
- `sqlx-turso-cli`: metadata preparation helper for Turso checked query macros

## Basic Use

```rust
use sqlx_turso::{
    TursoConnection,
    sqlx::{Connection, Executor, Row},
};

async fn example() -> sqlx_turso::sqlx::Result<()> {
  let mut conn = TursoConnection::connect("turso::memory:").await?;
  conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").await?;
  conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice')").await?;

  let row = conn.fetch_one("SELECT name FROM users WHERE id = 1").await?;
  assert_eq!(row.try_get::<String, _>("name")?, "alice");
  Ok(())
}
```

## Features

- `runtime-tokio`: enabled by default
- `macros`: first-party checked query macros
- `migrate`: SQLx migration traits and local database create/exists/drop helpers
- `any`: SQLx `Any` backend registration
- `offline`: serializable describe metadata for checked query macros
- `chrono`, `time`, `uuid`, `json`: optional value integrations
- `fts`: forwards Turso FTS support
- `sync`: local sync-backed connections and sync checkpoint/stat APIs

## Checked Queries

Use `sqlx_turso::query!` with the `macros` feature. Offline metadata is stored in `.sqlx/query-*.json`.

```rust
let row = sqlx_turso::query!("SELECT 1 AS \"id!: i64\"")
    .fetch_one(&mut conn)
    .await?;
assert_eq!(row.id, 1);
```

Stock `cargo sqlx prepare` does not know the `turso:` URL scheme. Use the wrapper instead:

```sh
sqlx-turso prepare --database-url turso:///path/to/app.db -- -p your-crate --features macros
```

## Unsupported Or Precisely Blocked Surfaces

- true read-only and immutable open modes are rejected because pinned `turso =0.7.0-pre.3` exposes no public builder mapping
- stored generated columns are not claimed; current coverage verifies virtual generated columns
- autovacuum remains blocked because the pinned public Turso builder exposes VACUUM but not the required autovacuum flag
- full remote sync push/pull tests require a committed sync-server fixture; default tests do not require public network, Turso Cloud credentials, or a local server
- sync custom IO is blocked because the pinned sync builder does not expose the local `with_io_impl` hook
- SQLx SQLite features such as `deserialize`, `load-extension`, `preupdate-hook`, and `unlock-notify` are not exposed without tested Turso-safe hooks
- decimal precision integrations are deferred until an explicit storage contract exists

See `examples/` for connect, pool, transaction, migration, checked-query, encryption/MVCC, and sync snippets.

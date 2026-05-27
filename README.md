# sqlx-turso

`sqlx-turso` is a SQLx adapter for the Rust `turso` database engine. It exposes a distinct `Turso` SQLx database type, a small public facade crate, first-party checked query macros, migration support, pool aliases, and honest feature gates for Turso-specific behavior.

This is a pre-release Turso-backed SQLx driver, not a drop-in replacement for
`sqlx-sqlite`. The supported and limited surfaces are documented below so
applications can decide whether the current crate is appropriate for production
use.

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

## Installation

Install the `sqlx-turso` CLI with Cargo:

```sh
cargo install sqlx-turso-cli --version 0.1.0-alpha.1
```

## Compile-Time Checked Queries

`sqlx-turso` provides SQLx-style compile-time checked query macros through the
`sqlx_turso::query!` macro family when the `macros` feature is enabled. These
are Turso-specific macros exported by this crate, not the stock `sqlx::query!`
macros.

```toml
sqlx-turso = { version = "0.1.0-alpha.1", features = ["macros"] }
```

```rust
let row = sqlx_turso::query!("SELECT 1 AS \"id!: i64\"")
    .fetch_one(&mut conn)
    .await?;

assert_eq!(row.id, 1);
```

Supported checked macro entry points:

- `sqlx_turso::query!`
- `sqlx_turso::query_as!`
- `sqlx_turso::query_scalar!`
- `sqlx_turso::query_file!`
- `sqlx_turso::query_file_as!`
- `sqlx_turso::query_file_scalar!`

Output column typing is checked from describe metadata, and SQLx-style column
overrides such as `"id!: i64"` are supported. Bind parameter checking is weak
because Turso describe metadata does not expose public parameter metadata.

Offline metadata is stored in `.sqlx/query-*.json`.
Stock `cargo sqlx prepare` does not know the `turso:` URL scheme. Use the wrapper instead:

```sh
sqlx-turso prepare --database-url turso:///path/to/app.db -- -p your-crate --features macros
```

## Status

Supported today:

- local file and in-memory connections
- SQLx executor, row, value, statement, pool, and transaction APIs
- nested transactions through savepoints
- local migrations and database lifecycle helpers
- checked query output typing and offline metadata
- optional `chrono`, `time`, `uuid`, and `json` value integrations
- forwarded Turso FTS support
- local sync-backed execution, checkpoint, and stats APIs

Current crate limitations:

- this crate pins `turso = "=0.7.0-pre.3"`
- only virtual generated columns are covered; stored generated columns are not supported
- remote sync push/pull is not tested yet; default sync tests run without a server

Blocked by Turso support:

- bind parameter checking is weak until Turso exposes public parameter metadata
- read-only opens are rejected until Turso exposes `OpenFlags::ReadOnly` through the Rust builder and SDK config; emulating read-only after opening read-write would not provide correct locking or file-access semantics
- immutable opens are rejected until Turso exposes and documents SQLite-style immutable open semantics through the Rust builder
- autovacuum is not supported yet. Turso keeps autovacuum behind an experimental opt-in because there are still open correctness issues in that code path, and the pinned Rust builder does not expose an autovacuum opt-in. Regular `VACUUM` is still supported behind `TursoExperimentalFeature::Vacuum`
- sync connections cannot use custom IO until Turso exposes that hook

See `examples/` for connect, pool, transaction, migration, checked-query, encryption/MVCC, and sync snippets.

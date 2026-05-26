//! SQLx adapter for the Rust Turso database engine
//!
//! `sqlx-turso` exposes a distinct SQLx [`Database`](sqlx::Database) implementation backed by
//! the Rust `turso` crate. The facade re-exports the runtime types from `sqlx-turso-core` and,
//! with the `macros` feature, first-party checked query macros such as [`query!`].
//!
//! # Example
//!
//! ```no_run
//! use sqlx_turso::{
//!     TursoConnection,
//!     sqlx::{Connection, Executor, Row},
//! };
//!
//! # async fn example() -> sqlx_turso::sqlx::Result<()> {
//! let mut conn = TursoConnection::connect("turso::memory:").await?;
//! conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").await?;
//! conn.execute("INSERT INTO users (id, name) VALUES (1, 'alice')").await?;
//!
//! let row = conn.fetch_one("SELECT name FROM users WHERE id = 1").await?;
//! assert_eq!(row.try_get::<String, _>("name")?, "alice");
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `runtime-tokio` is enabled by default and is the supported production runtime
//! - `macros` enables `sqlx_turso::query!` and related checked query macros
//! - `migrate` enables SQLx migration traits and local database lifecycle helpers
//! - `any` enables SQLx `Any` driver registration for Turso
//! - `offline` enables serializable query metadata
//! - `chrono`, `time`, `uuid`, and `json` enable matching value integrations
//! - `fts` forwards Turso FTS support
//! - `sync` enables local sync-backed connections and checkpoint/stat APIs
//!
//! Unsupported SQLite/Turso surfaces are documented in the repository README and remain disabled
//! unless the pinned SQLx and Turso APIs expose enough behavior to test them honestly

#![warn(missing_docs)]

extern crate self as sqlx_turso;

pub use sqlx_turso_core::{
    Turso, TursoAdapterError, TursoConnectOptions, TursoConnection, TursoDescribeExt,
    TursoEncryptionOptions, TursoExecutor, TursoExperimentalFeature, TursoExperimentalFeatures,
    TursoIo, TursoPool, TursoPoolOptions, TursoQueryResult, TursoRow, TursoStatement,
    TursoSyncOptions, TursoTransaction, TursoTypeChecking, TursoTypeInfo,
};

#[cfg(feature = "migrate")]
pub use sqlx_turso_core::{Migrate, MigrateDatabase, Migration, MigrationType};

#[doc(hidden)]
pub use sqlx;

#[cfg(feature = "any")]
pub use sqlx_turso_core::{TURSO_ANY_DRIVER, install_turso_any_driver};

#[cfg(feature = "macros")]
pub use sqlx_turso_macros::{
    query, query_as, query_file, query_file_as, query_file_scalar, query_scalar,
};

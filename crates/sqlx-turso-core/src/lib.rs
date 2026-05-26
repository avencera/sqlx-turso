//! Core SQLx adapter types for the Rust Turso database engine
//!
//! This crate owns the [`Turso`] SQLx database marker, connection options, runtime trait
//! implementations, value/row/type metadata, transactions, pools, migrations, optional `Any`
//! support, and macro metadata hooks. Most applications should depend on the `sqlx-turso`
//! facade instead of this crate directly.

#![warn(missing_docs)]

#[cfg(feature = "any")]
mod any;
mod arguments;
mod column;
mod connection;
mod database;
mod driver;
mod error;
mod executor;
mod features;
#[cfg(feature = "migrate")]
mod lifecycle;
mod macros;
#[cfg(feature = "migrate")]
mod migrate;
mod options;
mod pool;
mod query_result;
mod row;
mod statement;
mod transaction;
mod type_info;
mod value;

#[cfg(feature = "any")]
pub use any::{TURSO_ANY_DRIVER, install_turso_any_driver};
pub use arguments::TursoArguments;
pub use column::TursoColumn;
pub use connection::TursoConnection;
pub use database::Turso;
pub use error::TursoAdapterError;
pub use macros::{TursoDescribeExt, TursoTypeChecking};
pub use options::{
    TursoConnectOptions, TursoDatabaseTarget, TursoEncryptionOptions, TursoExperimentalFeature,
    TursoExperimentalFeatures, TursoIo, TursoSyncOptions,
};
pub use pool::{TursoExecutor, TursoPool, TursoPoolOptions};
pub use query_result::TursoQueryResult;
pub use row::TursoRow;
pub use statement::TursoStatement;
pub use transaction::{TursoTransaction, TursoTransactionManager};
pub use type_info::TursoTypeInfo;
pub use value::{TursoValue, TursoValueRef};

#[cfg(feature = "migrate")]
pub use sqlx_core::migrate::{Migrate, MigrateDatabase, Migration, MigrationType};

sqlx_core::impl_acquire!(Turso, TursoConnection);

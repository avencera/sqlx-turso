use sqlx_core::database::{Database, HasStatementCache};

use crate::{
    TursoArguments, TursoColumn, TursoConnection, TursoQueryResult, TursoRow, TursoStatement,
    TursoTransactionManager, TursoTypeInfo, TursoValue, TursoValueRef,
};

/// SQLx database marker for the Rust Turso engine
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Turso;

impl Turso {
    /// Database driver name reported by this adapter
    pub const NAME: &'static str = "Turso";
}

impl Database for Turso {
    type Connection = TursoConnection;
    type TransactionManager = TursoTransactionManager;
    type Row = TursoRow;
    type QueryResult = TursoQueryResult;
    type Column = TursoColumn;
    type TypeInfo = TursoTypeInfo;
    type Value = TursoValue;
    type ValueRef<'r> = TursoValueRef<'r>;
    type Arguments = TursoArguments;
    type ArgumentBuffer = Vec<TursoValue>;
    type Statement = TursoStatement;

    const NAME: &'static str = "Turso";
    const URL_SCHEMES: &'static [&'static str] = &["turso"];
}

impl HasStatementCache for Turso {}

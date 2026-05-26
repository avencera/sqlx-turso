use std::fmt;

use either::Either;
use sqlx_core::{
    impl_statement_query,
    sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr},
    statement::Statement,
};

use crate::{Turso, TursoArguments, TursoColumn, TursoTypeInfo};

/// Prepared Turso statement metadata
#[derive(Clone)]
pub struct TursoStatement {
    sql: SqlStr,
    parameters: Option<usize>,
    columns: Vec<TursoColumn>,
    raw: Option<turso::Statement>,
}

impl TursoStatement {
    /// Creates statement metadata for a SQL string
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns: Vec::new(),
            raw: None,
        }
    }

    pub(crate) fn with_raw(
        sql: impl Into<String>,
        columns: Vec<TursoColumn>,
        raw: turso::Statement,
    ) -> Self {
        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns,
            raw: Some(raw),
        }
    }

    pub(crate) fn raw(&self) -> Option<turso::Statement> {
        self.raw.clone()
    }
}

impl fmt::Debug for TursoStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoStatement")
            .field("sql", &self.sql)
            .field("parameters", &self.parameters)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl Statement for TursoStatement {
    type Database = Turso;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[TursoTypeInfo], usize>> {
        self.parameters.map(Either::Right)
    }

    fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }

    impl_statement_query!(TursoArguments);
}

impl sqlx_core::column::ColumnIndex<TursoStatement> for str {
    fn index(&self, statement: &TursoStatement) -> Result<usize, sqlx_core::error::Error> {
        statement
            .columns()
            .iter()
            .position(|column| sqlx_core::column::Column::name(column) == self)
            .ok_or_else(|| sqlx_core::error::Error::ColumnNotFound(self.to_owned()))
    }
}

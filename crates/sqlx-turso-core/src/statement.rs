use std::{fmt, sync::Arc};

use either::Either;
use sqlx_core::{
    HashMap,
    column::ColumnIndex,
    ext::ustr::UStr,
    impl_statement_query,
    sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr},
    statement::Statement,
};

use crate::{Turso, TursoArguments, TursoColumn, TursoTypeInfo, column::collect_column_names};

/// Prepared Turso statement metadata
#[derive(Clone)]
pub struct TursoStatement {
    sql: SqlStr,
    parameters: Option<usize>,
    columns: Arc<[TursoColumn]>,
    column_names: Arc<HashMap<UStr, usize>>,
    raw: Option<turso::Statement>,
}

impl TursoStatement {
    /// Creates statement metadata for a SQL string
    pub fn new(sql: impl Into<String>) -> Self {
        let columns: Arc<[TursoColumn]> = Vec::new().into();
        let column_names = collect_column_names(&columns);

        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns,
            column_names,
            raw: None,
        }
    }

    pub(crate) fn with_raw(
        sql: impl Into<String>,
        columns: Vec<TursoColumn>,
        raw: turso::Statement,
    ) -> Self {
        let columns: Arc<[TursoColumn]> = columns.into();
        let column_names = collect_column_names(&columns);

        Self {
            sql: AssertSqlSafe(sql.into()).into_sql_str(),
            parameters: None,
            columns,
            column_names,
            raw: Some(raw),
        }
    }

    pub(crate) fn raw(&self) -> Option<turso::Statement> {
        self.raw.clone()
    }

    #[cfg(feature = "any")]
    pub(crate) fn column_names(&self) -> Arc<HashMap<UStr, usize>> {
        self.column_names.clone()
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

impl ColumnIndex<TursoStatement> for str {
    fn index(&self, statement: &TursoStatement) -> Result<usize, sqlx_core::error::Error> {
        statement
            .column_names
            .get(self)
            .copied()
            .ok_or_else(|| sqlx_core::error::Error::ColumnNotFound(self.to_owned()))
    }
}

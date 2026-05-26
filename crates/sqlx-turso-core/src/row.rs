use sqlx_core::{column::ColumnIndex, error::Error, impl_column_index_for_row, row::Row};

use crate::{Turso, TursoColumn, TursoValue, TursoValueRef};

/// Row returned from a Turso query
#[derive(Clone, Debug, Default)]
pub struct TursoRow {
    columns: Vec<TursoColumn>,
    values: Vec<TursoValue>,
}

impl TursoRow {
    /// Creates a row from column metadata and values
    pub fn new(columns: Vec<TursoColumn>, values: Vec<TursoValue>) -> Self {
        Self { columns, values }
    }
}

impl Row for TursoRow {
    type Database = Turso;

    fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<TursoValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        self.values
            .get(index)
            .map(TursoValueRef::new)
            .ok_or(Error::ColumnIndexOutOfBounds {
                len: self.values.len(),
                index,
            })
    }
}

impl_column_index_for_row!(TursoRow);

impl ColumnIndex<TursoRow> for str {
    fn index(&self, row: &TursoRow) -> Result<usize, Error> {
        row.columns()
            .iter()
            .position(|column| sqlx_core::column::Column::name(column) == self)
            .ok_or_else(|| Error::ColumnNotFound(self.to_owned()))
    }
}

use std::sync::Arc;

use sqlx_core::{HashMap, column::Column, ext::ustr::UStr, impl_column_index_for_statement};

use crate::{Turso, TursoTypeInfo};

/// Column metadata for a Turso result set
#[derive(Clone, Debug)]
#[cfg_attr(feature = "offline", derive(serde::Deserialize, serde::Serialize))]
pub struct TursoColumn {
    ordinal: usize,
    name: String,
    type_info: TursoTypeInfo,
}

impl TursoColumn {
    /// Creates column metadata
    pub fn new(ordinal: usize, name: impl Into<String>, type_info: TursoTypeInfo) -> Self {
        Self {
            ordinal,
            name: name.into(),
            type_info,
        }
    }

    pub(crate) fn from_turso(ordinal: usize, column: &turso::Column) -> Self {
        let type_info = column
            .decl_type()
            .map(TursoTypeInfo::from_name)
            .unwrap_or(TursoTypeInfo::NULL);

        Self::new(ordinal, column.name().to_owned(), type_info)
    }
}

impl Column for TursoColumn {
    type Database = Turso;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &TursoTypeInfo {
        &self.type_info
    }
}

pub(crate) fn collect_column_names(columns: &[TursoColumn]) -> Arc<HashMap<UStr, usize>> {
    let mut column_names = HashMap::with_capacity(columns.len());

    for column in columns {
        column_names.insert(UStr::new(column.name()), column.ordinal());
    }

    Arc::new(column_names)
}

impl_column_index_for_statement!(TursoStatement);

use crate::statement::TursoStatement;

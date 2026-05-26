use std::fmt::{self, Write};

use sqlx_core::{
    arguments::Arguments,
    encode::{Encode, IsNull},
    error::BoxDynError,
    impl_into_arguments_for_arguments,
    types::Type,
};

use crate::{Turso, TursoValue};

/// SQLx argument buffer for Turso queries
#[derive(Debug, Default)]
pub struct TursoArguments {
    values: Vec<TursoValue>,
}

impl TursoArguments {
    /// Returns the buffered argument values
    pub fn values(&self) -> &[TursoValue] {
        &self.values
    }

    pub(crate) fn into_turso_values(self) -> Vec<turso::Value> {
        self.values
            .into_iter()
            .map(TursoValue::into_turso)
            .collect()
    }
}

impl Arguments for TursoArguments {
    type Database = Turso;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Self::Database> + Type<Self::Database>,
    {
        let len = self.values.len();

        match value.encode(&mut self.values) {
            Ok(IsNull::Yes) => self.values.push(TursoValue::null()),
            Ok(IsNull::No) => {}
            Err(error) => {
                self.values.truncate(len);
                return Err(error);
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn format_placeholder<W: Write>(&self, writer: &mut W) -> fmt::Result {
        write!(writer, "?{}", self.values.len() + 1)
    }
}

impl_into_arguments_for_arguments!(TursoArguments);
